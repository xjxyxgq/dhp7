"""异构复制监控检查器与问题生命周期。"""
from __future__ import annotations

import asyncio
import ipaddress
import json
import re
import socket
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

import aiomysql
from sqlalchemy import and_, delete, or_, select, update
from sqlalchemy.exc import IntegrityError

from app.core.config import settings
from app.core.database import async_session_maker
from app.core.diagnostics import (
    diagnostic_context,
    diagnostic_event,
    execution_context,
    format_sanitized_exception,
    new_operation_id,
)
from app.core.logging import get_general_logger
from app.core.mysql_cluster_access import connect_mysql_endpoint, execute_fetchall_mysql_read, execute_fetchone_mysql_read, parse_vip_port
from app.core.service_tracing import trace_skip, traced_service
from app.goldendb.services.goldendb_insight_client import GoldenDBInsightClient
from app.heterogeneous.audit import record_audit
from app.heterogeneous.schemas import normalize_check_config
from app.platform.scheduler.singleton_lease import try_acquire as try_acquire_lease
from app.heterogeneous.models import (
    HeterogeneousReplicationCheckResult,
    HeterogeneousReplicationCheckRun,
    HeterogeneousReplicationIssue,
    HeterogeneousReplicationRelation,
)


DEFAULT_GTID_THRESHOLD = 1000
DELAYED_UNKNOWN_CODES = {
    "CHECK_EXECUTION_FAILED",
    "GOLDENDB_TENANCY_API_CONNECTION",
    "GOLDENDB_TOPOLOGY_API_CONNECTION",
    "GOLDENDB_CONNECTION",
    "DISCOVERED_PRIMARY_CONNECTION",
    "MYSQL_CONNECTION",
    "ROLE_UNKNOWN",
}
_locks: dict[int, asyncio.Lock] = {}
_monitor_task: asyncio.Task | None = None
_run_semaphore = asyncio.Semaphore(max(1, settings.HETEROGENEOUS_MONITOR_MAX_CONCURRENCY))
_last_cleanup_date: date | None = None
_MONITOR_LEASE_NAME = "heterogeneous_monitor"
_DOCKER_HOST_ALIASES = {"host.docker.internal", "gateway.docker.internal"}
logger = get_general_logger()


def _lock_for(relation_id: int) -> asyncio.Lock:
    return _locks.setdefault(relation_id, asyncio.Lock())


def _result(code: str, status: str, severity: str = "info", *, expected=None, observed=None, message="", context=None) -> dict[str, Any]:
    return {
        "check_code": code,
        "status": status,
        "severity": severity,
        "expected_value": expected,
        "observed_value": observed,
        "message": message,
        "context_json": context,
    }


def parse_gtid_set(value: Any) -> dict[str, list[tuple[int, int]]]:
    """解析 MySQL GTID set，返回 UUID -> 合并后的闭区间列表。"""
    text = str(value or "").strip()
    if not text:
        return {}
    result: dict[str, list[tuple[int, int]]] = {}
    for item in text.split(","):
        item = item.strip()
        if not item or ":" not in item:
            raise ValueError(f"invalid GTID item: {item}")
        uuid, *parts = item.split(":")
        uuid = uuid.strip().lower()
        if not re.fullmatch(r"[0-9a-f-]{8,64}", uuid):
            raise ValueError(f"invalid GTID UUID: {uuid}")
        intervals: list[tuple[int, int]] = []
        for part in parts:
            if "-" in part:
                start, end = part.split("-", 1)
            else:
                start = end = part
            left, right = int(start), int(end)
            if left <= 0 or right < left:
                raise ValueError(f"invalid GTID interval: {part}")
            intervals.append((left, right))
        result.setdefault(uuid, []).extend(intervals)
    for uuid, intervals in result.items():
        intervals.sort()
        merged: list[tuple[int, int]] = []
        for left, right in intervals:
            if merged and left <= merged[-1][1] + 1:
                merged[-1] = (merged[-1][0], max(merged[-1][1], right))
            else:
                merged.append((left, right))
        result[uuid] = merged
    return result


def subtract_gtid_set(minuend: dict[str, list[tuple[int, int]]],
                      subtrahend: dict[str, list[tuple[int, int]]]) -> dict[str, list[tuple[int, int]]]:
    """返回 minuend - subtrahend 的 GTID 区间集合。"""
    result: dict[str, list[tuple[int, int]]] = {}
    for uuid, minuend_intervals in minuend.items():
        subtrahend_intervals = subtrahend.get(uuid, [])
        remaining_all: list[tuple[int, int]] = []
        for left, right in minuend_intervals:
            remaining = [(left, right)]
            for tleft, tright in subtrahend_intervals:
                next_remaining = []
                for rleft, rright in remaining:
                    if tright < rleft or tleft > rright:
                        next_remaining.append((rleft, rright))
                        continue
                    if rleft < tleft:
                        next_remaining.append((rleft, tleft - 1))
                    if tright < rright:
                        next_remaining.append((tright + 1, rright))
                remaining = next_remaining
            remaining_all.extend(remaining)
        if remaining_all:
            result[uuid] = remaining_all
    return result


def gtid_transaction_count(gtid_set: dict[str, list[tuple[int, int]]]) -> int:
    return sum(right - left + 1 for intervals in gtid_set.values() for left, right in intervals)


def format_gtid_set(gtid_set: dict[str, list[tuple[int, int]]]) -> str:
    """按 MySQL GTID 文本格式输出区间集合，便于在观测值中展示差集。"""
    return ",".join(
        f"{uuid}:{left}-{right}" if left != right else f"{uuid}:{left}"
        for uuid in sorted(gtid_set) for left, right in gtid_set[uuid]
    )


def gtid_difference(source: dict[str, list[tuple[int, int]]], target: dict[str, list[tuple[int, int]]]) -> tuple[int, bool]:
    """返回 source-target 事务数和 target 是否存在 source 之外的差集。"""
    missing = subtract_gtid_set(source, target)
    extra = bool(subtract_gtid_set(target, source))
    return gtid_transaction_count(missing), extra


def _normalize_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "on", "true", "yes"}:
        return True
    if text in {"0", "off", "false", "no"}:
        return False
    return None


def _row_value(row: Any, key: str, default=None):
    if isinstance(row, dict):
        return row.get(key, default)
    return getattr(row, key, default)


def _json_snapshot_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _node_snapshot(host: str, port: int, status: dict[str, Any], variables: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps({
        "endpoint": f"{host}:{port}",
        "read_only": _normalize_bool(status.get("read_only")),
        "super_read_only": _normalize_bool(status.get("super_read_only")),
        "variables": variables,
        "slave_status": status.get("slave_status") or [],
    }, default=str))


def _peer_replication_channels(replica: dict, source: dict) -> list[dict]:
    """返回 replica 上指向 source 的复制通道。"""
    host, _, port = str(source.get("endpoint", "")).rpartition(":")
    if not host or not port:
        return []
    return [row for row in replica.get("slave_status") or []
            if str(row.get("Master_Port", "")) == port
            and _hosts_match(str(row.get("Master_Host", "")), host) is True]


def _channels_running(rows: list[dict]) -> bool:
    return any(str(row.get("Slave_IO_Running", "")).lower() == "yes"
               or str(row.get("Slave_SQL_Running", "")).lower() == "yes" for row in rows)


def replication_roles(g_snapshot: dict, m_snapshot: dict) -> tuple[str, str]:
    """按两端 SHOW SLAVE STATUS 的指向判定主备：被指向的一端为主，指向对端的一端为备。

    主节点自身复制其他节点不影响判定，只有指向当前对端的通道才算证据。
    """
    if not g_snapshot or not m_snapshot:
        return "unknown", "unknown"
    g_channels = _peer_replication_channels(g_snapshot, m_snapshot)
    m_channels = _peer_replication_channels(m_snapshot, g_snapshot)
    if g_channels and m_channels:
        # 双向复制或切换后保留的旧通道：仅有单侧运行时以运行侧为备，否则不能认定方向。
        g_running, m_running = _channels_running(g_channels), _channels_running(m_channels)
        if g_running == m_running:
            return "unknown", "unknown"
        return ("replica", "primary") if g_running else ("primary", "replica")
    if g_channels:
        return "replica", "primary"
    if m_channels:
        return "primary", "replica"
    return "unknown", "unknown"


def _check_roles(goldendb_read_only: bool | None, mysql_read_only: bool | None,
                 roles: tuple[str, str]) -> dict[str, Any]:
    observed = {"goldendb_read_only": goldendb_read_only, "mysql_read_only": mysql_read_only}
    expected = {f"{side}_read_only": None if role == "unknown" else role == "replica"
                for side, role in zip(("goldendb", "mysql"), roles)}
    if goldendb_read_only is None or mysql_read_only is None:
        return _result("ROLE_UNKNOWN", "unknown", "unknown", expected=expected, observed=observed, message="无法获取两端 read_only")
    if goldendb_read_only == mysql_read_only:
        code = "BOTH_READ_ONLY" if goldendb_read_only else "BOTH_WRITABLE"
        return _result(code, "error", "error", expected=expected, observed=observed, message="两端应形成主节点可读写、备节点只读")
    if "unknown" in roles:
        return _result("ROLE_UNKNOWN", "unknown", "unknown", expected=expected, observed=observed, message="无法从当前复制关系唯一确定主备方向")
    if observed == expected:
        return _result("ROLE_PAIR", "pass", "info", expected=expected, observed=observed, message="主节点可读写、备节点只读，符合当前复制方向")
    return _result("ROLE_DIRECTION_MISMATCH", "error", "error", expected=expected, observed=observed, message="只读状态与当前复制方向不符：主节点应可读写，备节点应只读")


async def _query_variables(connection, names: list[str]) -> dict[str, Any]:
    placeholders = ",".join(["%s"] * len(names))
    async with connection.cursor(aiomysql.DictCursor) as cursor:
        rows = await execute_fetchall_mysql_read(cursor, f"SHOW VARIABLES WHERE Variable_name IN ({placeholders})", names)
        variables = {str(_row_value(row, "Variable_name", "")).lower(): _row_value(row, "Value") for row in rows}
        # MySQL 5.7 exposes gtid_executed through @@GLOBAL but may omit it from
        # SHOW VARIABLES, while GoldenDB/MySQL 8 returns it in both forms.
        if "gtid_executed" in names and "gtid_executed" not in variables:
            row = await execute_fetchone_mysql_read(cursor, "SELECT @@global.gtid_executed AS gtid_executed")
            value = _row_value(row, "gtid_executed")
            if value is not None:
                variables["gtid_executed"] = value
    return variables


async def _query_status(connection) -> dict[str, Any]:
    async with connection.cursor(aiomysql.DictCursor) as cursor:
        row = await execute_fetchone_mysql_read(cursor, "SELECT @@global.read_only AS read_only, @@global.super_read_only AS super_read_only")
        try:
            slave = await execute_fetchall_mysql_read(cursor, "SHOW SLAVE STATUS")
        except Exception:
            slave = []
    return {"read_only": _row_value(row, "read_only"), "super_read_only": _row_value(row, "super_read_only"), "slave_status": slave}


async def _inspect_endpoint(host: str, port: int, username: str | None, password: str | None) -> tuple[dict[str, Any], dict[str, Any]]:
    connection = await connect_mysql_endpoint(host=host, port=port, username=username, password=password)
    try:
        status = await _query_status(connection)
        variables = await _query_variables(connection, [
            "collation_server", "default_collation_for_utf8mb4", "character_set_server",
            "rpl_semi_sync_master_wait_no_slave", "rpl_semi_sync_slave_enabled",
            "slave_parallel_workers", "gtid_mode", "gtid_executed",
        ])
        return status, variables
    finally:
        connection.close()


def _extract_dn_list(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict):
        data = data.get("list") or data.get("DNList") or data.get("data") or []
        if isinstance(data, dict):
            data = data.get("list") or data.get("DNList") or []
    flattened: list[dict[str, Any]] = []
    for item in data or []:
        if not isinstance(item, dict):
            continue
        if isinstance(item.get("DNList"), list):
            for child in item["DNList"]:
                if isinstance(child, dict):
                    flattened.append({**child, "groupId": child.get("groupId", item.get("groupId"))})
        else:
            flattened.append(item)
    return flattened


def _dn_id(dn: dict[str, Any]) -> str:
    return str(dn.get("dbId") or dn.get("id") or dn.get("db_id") or "")


def _dn_is_primary(dn: dict[str, Any]) -> bool | None:
    backup = dn.get("backup")
    return None if backup is None or str(backup) not in {"0", "1", "2"} else str(backup) == "1"


@traced_service
class HeterogeneousReplicationService:
    """执行关系检查、持久化结果并维护问题生命周期。"""

    @classmethod
    async def create_run(cls, relation_id: int, trigger_type: str = "manual") -> HeterogeneousReplicationCheckRun:
        logger.info("创建异构复制检查运行: relation_id={} trigger_type={}", relation_id, trigger_type)
        async with async_session_maker() as session:
            relation = await session.get(HeterogeneousReplicationRelation, relation_id)
            if relation is None or relation.deleted_at is not None:
                raise ValueError("复制关系不存在")
            if not relation.enabled and trigger_type != "manual":
                raise ValueError("复制关系未启用")
            running = await session.scalar(select(HeterogeneousReplicationCheckRun).where(
                HeterogeneousReplicationCheckRun.relation_id == relation_id,
                HeterogeneousReplicationCheckRun.status.in_(["pending", "running"]),
            ).order_by(HeterogeneousReplicationCheckRun.id.desc()))
            if running:
                logger.debug("复用已有异构复制检查运行: relation_id={} run_id={} status={}", relation_id, running.id, running.status)
                return running
            run = HeterogeneousReplicationCheckRun(
                relation_id=relation.id, active_relation_id=relation.id, config_version=relation.config_version,
                trigger_type=trigger_type, status="pending",
                config_snapshot=cls._snapshot(relation),
            )
            session.add(run)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                existing = await session.scalar(select(HeterogeneousReplicationCheckRun).where(
                    HeterogeneousReplicationCheckRun.active_relation_id == relation_id,
                ).order_by(HeterogeneousReplicationCheckRun.id.desc()))
                if existing:
                    return existing
                raise
            await session.refresh(run)
            record_audit(
                "check_run_created",
                operator="scheduler" if trigger_type == "scheduled" else "manual",
                relation_id=relation.id,
                run_id=run.id,
                details={"trigger_type": trigger_type, "config_version": relation.config_version},
            )
            logger.info("异构复制检查运行已创建: relation_id={} run_id={} trigger_type={}", relation.id, run.id, trigger_type)
            return run

    @classmethod
    async def start_monitor(cls) -> None:
        global _monitor_task
        if _monitor_task is None or _monitor_task.done():
            _monitor_task = asyncio.create_task(cls._monitor_loop(), name="heterogeneous-replication-monitor")
            logger.info("异构复制监控循环已启动")

    @classmethod
    async def stop_monitor(cls) -> None:
        global _monitor_task
        if _monitor_task is not None:
            _monitor_task.cancel()
            try:
                await _monitor_task
            except asyncio.CancelledError:
                pass
            _monitor_task = None
            logger.info("异构复制监控循环已停止")

    @classmethod
    async def _monitor_loop(cls) -> None:
        global _last_cleanup_date
        while True:
            try:
                scan_operation_id = new_operation_id()
                with execution_context(
                    operation_id=scan_operation_id,
                    origin="worker",
                    worker_id="heterogeneous-monitor",
                    job_id="heterogeneous_monitor_scan",
                    phase="scan",
                    event="monitor_scan",
                ):
                    now = datetime.utcnow()
                    # 回收卡死 run（H-1）：幂等 CAS，所有实例都跑，提供 leader 切换期的兜底。
                    await cls.recover_stale_runs()
                    # H-2：leader 门控——仅持租约的单个实例执行 scan/调度/cleanup。
                    is_leader = await try_acquire_lease(_MONITOR_LEASE_NAME, ttl_seconds=90)
                    if not is_leader:
                        await asyncio.sleep(30)
                        continue
                    async with async_session_maker() as session:
                        result = await session.execute(select(HeterogeneousReplicationRelation).where(
                            HeterogeneousReplicationRelation.enabled.is_(True),
                            HeterogeneousReplicationRelation.deleted_at.is_(None),
                        ))
                        relations = list(result.scalars().all())
                    logger.debug("异构复制监控扫描启用关系: count={}", len(relations))
                    scheduled_count = 0
                    for relation in relations:
                        if relation.last_checked_at is None or (now - relation.last_checked_at).total_seconds() >= relation.check_interval_minutes * 60:
                            try:
                                run = await cls.create_run(relation.id, "scheduled")
                                asyncio.create_task(cls.run(run.id, parent_operation_id=scan_operation_id))
                                scheduled_count += 1
                                logger.info("异构复制检查已调度: relation_id={} run_id={}", relation.id, run.id)
                            except ValueError:
                                logger.warning("跳过不可检查的异构复制关系: relation_id={}", relation.id)
                                continue
                    if scheduled_count:
                        logger.info("异构复制监控扫描完成: relation_count={} scheduled_count={}", len(relations), scheduled_count)
                    from app.heterogeneous.alert_service import scan_due_alerts
                    await scan_due_alerts(session)
                    if _last_cleanup_date != now.date():
                        await cls.cleanup_history()
                        _last_cleanup_date = now.date()
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                with execution_context(
                    operation_id=scan_operation_id,
                    origin="worker",
                    worker_id="heterogeneous-monitor",
                    job_id="heterogeneous_monitor_scan",
                    phase="failed",
                    event="monitor_scan_failed",
                ):
                    logger.error(
                        "异构复制监控循环异常，将在下个周期重试: error={}",
                        format_sanitized_exception(exc),
                    )
                await asyncio.sleep(30)

    @staticmethod
    def _snapshot(relation: HeterogeneousReplicationRelation) -> dict[str, Any]:
        data = {column.name: getattr(relation, column.name) for column in relation.__table__.columns}
        for key in ("goldendb_db_password", "goldendb_api_password", "mysql_db_password"):
            data.pop(key, None)
        return json.loads(json.dumps(data, default=_json_snapshot_value))

    @classmethod
    async def recover_stale_runs(cls) -> int:
        """回收卡死的检查 run（多实例正确性 A1 修复）。

        run 无租约/心跳：执行 run 的实例若在 _run_locked 完成前崩溃，run 会永久停在
        running（或 pending 从未被认领），且 active_relation_id 残留触发唯一约束，导致
        create_run 一直复用这条死 run，该 relation 监控永久停摆。

        本方法把超过 stale 阈值仍处于 pending/running 的 run CAS 收敛为 failed 并清空
        active_relation_id，使下一轮 create_run 能新建、监控恢复。存活 owner 正常在
        RUN_TIMEOUT 内完成，阈值取 RUN_TIMEOUT + 60s 余量，避免误回收在途 run。
        每个实例的监控循环都会调用，CAS 幂等，多实例并发安全。
        """
        run_timeout = max(1, int(settings.HETEROGENEOUS_MONITOR_RUN_TIMEOUT_SECONDS))
        now = datetime.utcnow()
        # running 按 started_at 判活；pending（未认领）按 created_at 判活。
        running_cutoff = now - timedelta(seconds=run_timeout + 60)
        pending_cutoff = now - timedelta(seconds=run_timeout + 60)
        async with async_session_maker() as session:
            result = await session.execute(
                update(HeterogeneousReplicationCheckRun)
                .where(
                    or_(
                        and_(
                            HeterogeneousReplicationCheckRun.status == "running",
                            HeterogeneousReplicationCheckRun.started_at.is_not(None),
                            HeterogeneousReplicationCheckRun.started_at < running_cutoff,
                        ),
                        and_(
                            HeterogeneousReplicationCheckRun.status == "pending",
                            HeterogeneousReplicationCheckRun.created_at < pending_cutoff,
                        ),
                    )
                )
                .values(
                    status="failed",
                    active_relation_id=None,
                    error_code="STALE_RECOVERED",
                    error_message="检查运行超时未完成（执行实例可能已崩溃），已回收以恢复监控",
                    finished_at=now,
                )
                .execution_options(synchronize_session=False)
            )
            await session.commit()
            recovered = int(getattr(result, "rowcount", 0) or 0)
        if recovered:
            logger.warning("异构复制回收卡死检查 run: count={}", recovered)
        return recovered

    @classmethod
    @trace_skip
    async def run(cls, run_id: int, parent_operation_id: str | None = None) -> None:
        operation_id = new_operation_id()
        with execution_context(
            operation_id=operation_id,
            origin="worker",
            worker_id="heterogeneous-monitor",
            job_id="heterogeneous_check_run",
            parent_operation_id=parent_operation_id,
            phase="run",
            event="run_started",
        ):
            await cls._run_in_context(run_id)

    @classmethod
    async def _run_in_context(cls, run_id: int) -> None:
        logger.info("开始执行异构复制检查: run_id={}", run_id)
        async with async_session_maker() as session:
            run = await session.get(HeterogeneousReplicationCheckRun, run_id)
            if not run:
                logger.warning("异构复制检查运行不存在: run_id={}", run_id)
                return
            relation = await session.get(HeterogeneousReplicationRelation, run.relation_id)
            if not relation:
                logger.error("异构复制检查关联关系不存在: run_id={} relation_id={}", run_id, run.relation_id)
                return
            with diagnostic_context(heterogeneous_run_id=run.id, relation_id=relation.id):
                await cls._claim_and_execute(session, run, relation)

    @classmethod
    async def _claim_and_execute(cls, session, run, relation) -> None:
        claimed = await session.execute(update(HeterogeneousReplicationCheckRun).where(
            HeterogeneousReplicationCheckRun.id == run.id,
            HeterogeneousReplicationCheckRun.status == "pending",
        ).values(status="running", started_at=datetime.utcnow()))
        await session.commit()
        if claimed.rowcount != 1:
            logger.debug("异构复制检查运行未抢占成功: run_id={}", run.id)
            return
        logger.info("异构复制检查状态流转: run_id={} relation_id={} pending->running", run.id, relation.id)
        diagnostic_event(
            "heterogeneous_run_state_changed",
            message="异构复制检查已认领",
            heterogeneous_run_id=run.id,
            relation_id=relation.id,
            status_from="pending",
            status_to="running",
            phase="claimed",
        )
        if not _lock_for(relation.id).locked():
            await _lock_for(relation.id).acquire()
        else:
            logger.debug("异构复制关系已有检查在执行，跳过重复运行: relation_id={} run_id={}", relation.id, run.id)
            return
        try:
            async with _run_semaphore:
                await cls._run_locked(session, run, relation)
        finally:
            _lock_for(relation.id).release()

    @classmethod
    async def _run_locked(cls, session, run, relation) -> None:
        await session.refresh(run)
        results: list[dict[str, Any]] = []
        issue_audits: list[tuple[str, dict[str, Any]]] = []
        logger.info("异构复制检查进入采集阶段: run_id={} relation_id={} config_version={}", run.id, relation.id, run.config_version)
        try:
            results.extend(await asyncio.wait_for(
                cls._collect(relation),
                timeout=max(1, settings.HETEROGENEOUS_MONITOR_RUN_TIMEOUT_SECONDS),
            ))
            run.status = "completed"
            run.summary = cls._summary(results)
            logger.info("异构复制检查状态流转: run_id={} relation_id={} running->completed result_count={}", run.id, relation.id, len(results))
        except asyncio.TimeoutError:
            exc = RuntimeError(f"整组检查超过 {settings.HETEROGENEOUS_MONITOR_RUN_TIMEOUT_SECONDS} 秒")
            results.append(_result("CHECK_EXECUTION_FAILED", "unknown", "unknown", message=str(exc)))
            run.status = "failed"
            run.error_code = "CHECK_EXECUTION_FAILED"
            run.error_message = str(exc)
            run.summary = "检查执行超时"
            logger.error("异构复制检查状态流转: run_id={} relation_id={} running->failed reason=timeout", run.id, relation.id)
        except Exception as exc:
            results.append(_result("CHECK_EXECUTION_FAILED", "unknown", "unknown", message=str(exc)))
            run.status = "failed"
            run.error_code = "CHECK_EXECUTION_FAILED"
            run.error_message = str(exc)
            run.summary = "检查执行失败"
            logger.error(
                "异构复制检查状态流转: run_id={} relation_id={} running->failed error={}",
                run.id,
                relation.id,
                format_sanitized_exception(exc),
            )
        run.finished_at = datetime.utcnow()
        run.active_relation_id = None
        run.result_json = {"total": len(results), "items": results}
        update_current = relation.config_version == run.config_version and relation.deleted_at is None and relation.enabled
        for item in results:
            session.add(HeterogeneousReplicationCheckResult(run_id=run.id, relation_id=relation.id, **item))
            if update_current:
                await cls._update_issue(session, relation, item, issue_audits)
                from app.heterogeneous.alert_service import observe_result
                await observe_result(session, relation, item, config_version=run.config_version)
        if update_current and run.status == "completed":
            await cls._resolve_absent_issues(session, relation, results, issue_audits)
        if relation.config_version == run.config_version and relation.deleted_at is None:
            current = [item for item in results if item["status"] in {"warning", "error", "unknown"}]
            relation.latest_status = cls._overall_status(current) if relation.enabled else "disabled"
            snapshots = {
                item["check_code"]: item.get("observed_value") or {}
                for item in results if item["check_code"].endswith("_NODE_SNAPSHOT")
            }
            relation.latest_goldendb_role = snapshots.get("GOLDENDB_NODE_SNAPSHOT", {}).get("role", "unknown")
            relation.latest_mysql_role = snapshots.get("MYSQL_NODE_SNAPSHOT", {}).get("role", "unknown")
            relation.last_checked_at = run.finished_at
        await session.commit()
        diagnostic_event(
            "heterogeneous_run_persisted",
            level="INFO" if run.status == "completed" else "ERROR",
            message="异构复制检查结果已持久化",
            heterogeneous_run_id=run.id,
            relation_id=relation.id,
            status=run.status,
            result_count=len(results),
            latest_status=relation.latest_status,
            error_code=run.error_code,
            phase="persisted",
        )
        run_audit_action = "check_run_completed" if run.status == "completed" else "check_run_failed"
        record_audit(
            run_audit_action,
            operator="scheduler",
            relation_id=relation.id,
            run_id=run.id,
            outcome="success" if run.status == "completed" else "failure",
            details={"result_count": len(results), "error_code": run.error_code, "summary": run.summary},
        )
        for action, details in issue_audits:
            record_audit(action, operator="scheduler", relation_id=relation.id, details=details)
        logger.info("异构复制检查结果已持久化: run_id={} relation_id={} latest_status={}", run.id, relation.id, relation.latest_status)

    @classmethod
    async def cleanup_history(cls) -> None:
        cutoff = datetime.utcnow() - timedelta(days=max(1, settings.HETEROGENEOUS_MONITOR_RETENTION_DAYS))
        async with async_session_maker() as session:
            old_run_ids = list((await session.execute(select(HeterogeneousReplicationCheckRun.id).where(
                HeterogeneousReplicationCheckRun.created_at < cutoff,
            ))).scalars().all())
            if old_run_ids:
                await session.execute(delete(HeterogeneousReplicationCheckResult).where(
                    HeterogeneousReplicationCheckResult.run_id.in_(old_run_ids),
                ))
                await session.execute(delete(HeterogeneousReplicationCheckRun).where(
                    HeterogeneousReplicationCheckRun.id.in_(old_run_ids),
                ))
            issue_delete_result = await session.execute(delete(HeterogeneousReplicationIssue).where(
                HeterogeneousReplicationIssue.status == "resolved",
                HeterogeneousReplicationIssue.resolved_at < cutoff,
            ))
            await session.commit()
        details = {
            "cutoff": cutoff.isoformat(),
            "deleted_run_count": len(old_run_ids),
            "deleted_issue_count": max(0, issue_delete_result.rowcount or 0),
        }
        record_audit("history_cleanup_completed", operator="scheduler", details=details)
        logger.info(
            "异构复制历史清理完成: cutoff={} deleted_runs={} deleted_issues={}",
            details["cutoff"],
            details["deleted_run_count"],
            details["deleted_issue_count"],
        )

    @staticmethod
    def _summary(results: list[dict[str, Any]]) -> str:
        counts = defaultdict(int)
        for item in results:
            counts[item["status"]] += 1
        return "检查完成：" + ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))

    @staticmethod
    def _overall_status(items: list[dict[str, Any]]) -> str:
        statuses = {item.get("severity") or item.get("status") for item in items}
        for value in ("error", "unknown", "warning"):
            if value in statuses:
                return value
        return "healthy"

    @classmethod
    async def _collect(cls, relation) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        checks = normalize_check_config(getattr(relation, "check_config", None))
        logger.debug("异构复制检查开关: relation_id={} checks={}", relation.id, checks)
        drifted_primary: tuple[str, int] | None = None
        discovered_status = None
        client = GoldenDBInsightClient(
            relation.goldendb_api_address,
            username=relation.goldendb_api_username,
            password=relation.goldendb_api_password,
            encode_password=True,
        )
        try:
            started = time.perf_counter()
            logger.debug("调用 GoldenDB Insight 租户目录: relation_id={} cluster_id={}", relation.id, relation.goldendb_cluster_id)
            tenancy_data = await client.get_tenancy_list()
            tenancy_items = tenancy_data if isinstance(tenancy_data, list) else (tenancy_data or {}).get("list", [])
            selected = [item for item in tenancy_items if str(item.get("clusterId")) == str(relation.goldendb_cluster_id)]
            logger.info("GoldenDB Insight 租户目录完成: relation_id={} count={} selected={} elapsed_ms={:.1f}", relation.id, len(tenancy_items), len(selected), (time.perf_counter() - started) * 1000)
            if not selected:
                results.append(_result("GOLDENDB_TENANCY_NOT_FOUND", "unknown", "unknown", message="GoldenDB 租户不存在"))
        except Exception as exc:
            logger.error(
                "GoldenDB Insight 租户目录失败: relation_id={} cluster_id={} error={}",
                relation.id,
                relation.goldendb_cluster_id,
                format_sanitized_exception(exc),
            )
            results.append(_result("GOLDENDB_TENANCY_API_CONNECTION", "unknown", "unknown", message=f"GoldenDB 租户 API 查询失败: {exc}"))
        try:
            started = time.perf_counter()
            logger.debug("调用 GoldenDB Insight DN 目录: relation_id={} cluster_id={}", relation.id, relation.goldendb_cluster_id)
            dn_data = await client.search_dn(cluster_id=relation.goldendb_cluster_id)
            dns = _extract_dn_list(dn_data)
            group_dns = [dn for dn in dns if str(dn.get("groupId") or dn.get("RGId")) == str(relation.goldendb_group_id)]
            primaries = [dn for dn in group_dns if _dn_is_primary(dn) is True]
            logger.info("GoldenDB Insight DN 目录完成: relation_id={} dn_count={} group_count={} primary_count={} elapsed_ms={:.1f}", relation.id, len(dns), len(group_dns), len(primaries), (time.perf_counter() - started) * 1000)
            if len(primaries) != 1:
                results.append(_result("GOLDENDB_PRIMARY_UNDETERMINED", "unknown", "unknown", message="GoldenDB 当前主 DN 无法唯一判定", observed={"count": len(primaries)}))
            else:
                discovered = primaries[0]
                discovered_id = _dn_id(discovered)
                if discovered_id != str(relation.goldendb_db_id):
                    discovered_host = discovered.get("ip") or discovered.get("dbIp")
                    discovered_port = discovered.get("port") or discovered.get("dbPort")
                    results.append(_result("PRIMARY_DN_DRIFT", "warning", "warning", expected={"db_id": relation.goldendb_db_id}, observed={"db_id": discovered_id, "ip": discovered_host, "port": discovered_port}, message="当前主 DN 与绑定 DN 不一致"))
                    if discovered_host and discovered_port:
                        drifted_primary = (str(discovered_host), int(discovered_port))
        except Exception as exc:
            logger.error(
                "GoldenDB Insight DN 目录失败: relation_id={} cluster_id={} error={}",
                relation.id,
                relation.goldendb_cluster_id,
                format_sanitized_exception(exc),
            )
            results.append(_result("GOLDENDB_TOPOLOGY_API_CONNECTION", "unknown", "unknown", message=f"GoldenDB DN 拓扑 API 查询失败: {exc}"))

        g_host, g_port = relation.goldendb_dn_ip, relation.goldendb_dn_port
        try:
            m_host, m_port = parse_vip_port(relation.cluster_vip_port)
        except Exception as exc:
            logger.warning("MySQL 节点地址无效: relation_id={} endpoint={}", relation.id, relation.cluster_vip_port)
            results.append(_result("MYSQL_ENDPOINT_INVALID", "unknown", "unknown", message=f"MySQL 节点地址无效: {exc}"))
            m_host, m_port = relation.cluster_vip_port, 3306
        try:
            logger.debug("探测 GoldenDB DN: relation_id={} endpoint={}:{}", relation.id, g_host, g_port)
            g_status, g_vars = await _inspect_endpoint(g_host, g_port, relation.goldendb_db_username, relation.goldendb_db_password)
            logger.info("GoldenDB DN 探测完成: relation_id={} endpoint={}:{} read_only={}", relation.id, g_host, g_port, _normalize_bool(g_status.get("read_only")))
        except Exception as exc:
            logger.error(
                "GoldenDB DN 探测失败: relation_id={} endpoint={}:{} error={}",
                relation.id,
                g_host,
                g_port,
                format_sanitized_exception(exc),
            )
            results.append(_result("GOLDENDB_CONNECTION", "unknown", "unknown", message=f"GoldenDB DN 连接失败: {exc}"))
            g_status, g_vars = {}, {}
        if drifted_primary and drifted_primary != (g_host, g_port):
            try:
                discovered_status, _ = await _inspect_endpoint(
                    drifted_primary[0], drifted_primary[1], relation.goldendb_db_username, relation.goldendb_db_password,
                )
            except Exception as exc:
                logger.error(
                    "GoldenDB 新主 DN 探测失败: relation_id={} endpoint={}:{} error={}",
                    relation.id,
                    drifted_primary[0],
                    drifted_primary[1],
                    format_sanitized_exception(exc),
                )
                results.append(_result("DISCOVERED_PRIMARY_CONNECTION", "unknown", "unknown", message=f"GoldenDB 新主 DN 连接失败: {exc}"))
        try:
            logger.debug("探测 MySQL 节点: relation_id={} endpoint={}:{}", relation.id, m_host, m_port)
            m_status, m_vars = await _inspect_endpoint(m_host, m_port, relation.mysql_db_username, relation.mysql_db_password)
            logger.info("MySQL 节点探测完成: relation_id={} endpoint={}:{} read_only={} slave_rows={}", relation.id, m_host, m_port, _normalize_bool(m_status.get("read_only")), len(m_status.get("slave_status") or []))
        except Exception as exc:
            logger.error(
                "MySQL 节点探测失败: relation_id={} endpoint={}:{} error={}",
                relation.id,
                m_host,
                m_port,
                format_sanitized_exception(exc),
            )
            results.append(_result("MYSQL_CONNECTION", "unknown", "unknown", message=f"MySQL 5.7 连接失败: {exc}"))
            m_status, m_vars = {}, {}

        g_snapshot = _node_snapshot(g_host, g_port, g_status, g_vars)
        m_snapshot = _node_snapshot(m_host, m_port, m_status, m_vars)
        roles = replication_roles(g_snapshot if g_status else {}, m_snapshot if m_status else {})
        g_snapshot["role"], m_snapshot["role"] = roles
        logger.debug("异构复制方向判定: relation_id={} goldendb_role={} mysql_role={}", relation.id, *roles)
        results.append(_result(
            "GOLDENDB_NODE_SNAPSHOT", "pass" if g_status else "not_applicable", "info",
            observed=g_snapshot, message="GoldenDB DN 最近观测快照",
        ))
        results.append(_result(
            "MYSQL_NODE_SNAPSHOT", "pass" if m_status else "not_applicable", "info",
            observed=m_snapshot, message="MySQL 节点最近观测快照",
        ))
        if discovered_status is not None:
            discovered_ro = _normalize_bool(discovered_status.get("read_only"))
            expected_ro = None if roles[0] == "unknown" else roles[0] == "replica"
            status = "unknown" if expected_ro is None or discovered_ro is None else "pass" if discovered_ro == expected_ro else "warning"
            results.append(_result(
                "DISCOVERED_PRIMARY_ROLE", status, "info" if status == "pass" else status,
                expected={"read_only": expected_ro},
                observed={"host": drifted_primary[0], "port": drifted_primary[1], "read_only": discovered_ro},
                message="GoldenDB 新主 DN 按异构复制方向检查只读状态",
            ))

        g_ro, m_ro = g_snapshot["read_only"], m_snapshot["read_only"]
        if checks["check_role_pair"]:
            results.append(_check_roles(g_ro, m_ro, roles))
        if g_vars and m_vars:
            results.extend(cls._compare_charset(g_vars, m_vars, checks))
        if "unknown" in roles:
            if any(checks[key] for key in ("check_replication_status", "check_replication_source", "check_gtid", "check_semi_sync", "check_slave_parallel_workers")):
                results.append(_result("REPLICATION_DIRECTION_UNKNOWN", "unknown", "unknown", message="主备方向无法确定，无法执行依赖方向的复制检查"))
            logger.debug("跳过依赖方向的复制检查: relation_id={} reason=unknown_direction", relation.id)
            return results
        goldendb_endpoint = (g_status, g_vars, g_host, g_port)
        mysql_endpoint = (m_status, m_vars, m_host, m_port)
        source, replica = (goldendb_endpoint, mysql_endpoint) if roles[0] == "primary" else (mysql_endpoint, goldendb_endpoint)
        results.extend(cls._check_replica(
            replica[0], replica[1], source, replica[2], replica[3], checks,
            node_type="mysql" if roles[0] == "primary" else "goldendb",
        ))
        if checks["check_gtid"] and source[1] and replica[1]:
            results.extend(cls._check_gtid(source[1], replica[1], relation.gtid_lag_threshold or DEFAULT_GTID_THRESHOLD))
        return results

    @staticmethod
    def _compare_charset(g_vars, m_vars, check_config: dict[str, bool] | None = None):
        checks = normalize_check_config(check_config)
        results = []
        variable_checks = []
        if checks["check_collation_server"]:
            variable_checks.append("collation_server")
        if checks["check_character_set_server"]:
            variable_checks.append("character_set_server")

        allowed = {"utf8mb4_general_ci", "utf8mb4_bin"}
        for key in variable_checks:
            expected = allowed if key == "collation_server" else {"utf8mb4"}
            g_value, m_value = str(g_vars.get(key, "")).lower(), str(m_vars.get(key, "")).lower()
            if not g_value or not m_value:
                results.append(_result(f"{key.upper()}_UNAVAILABLE", "unknown", "unknown", message=f"无法获取 {key}"))
                continue
            if g_value not in expected or m_value not in expected:
                results.append(_result(f"{key.upper()}_INVALID", "error", "error", expected={"allowed": sorted(expected)}, observed={"goldendb": g_value, "mysql": m_value}, message=f"{key} 存在非法值"))
            elif g_value != m_value:
                results.append(_result(f"{key.upper()}_MISMATCH", "error", "error", observed={"goldendb": g_value, "mysql": m_value}, message=f"{key} 两端不一致"))
            else:
                results.append(_result(f"{key.upper()}_OK", "pass", "info", observed={"value": g_value}))

        if not checks["check_default_collation"]:
            return results
        default = str(g_vars.get("default_collation_for_utf8mb4", "")).lower()
        if not default:
            results.append(_result("DEFAULT_COLLATION_UNAVAILABLE", "unknown", "unknown", message="GoldenDB default_collation_for_utf8mb4 不可用"))
        elif default != "utf8mb4_general_ci":
            results.append(_result("DEFAULT_COLLATION_INVALID", "error", "error", expected={"value": "utf8mb4_general_ci"}, observed={"value": default}, message="GoldenDB 默认 utf8mb4 排序规则不符合要求"))
        else:
            results.append(_result("DEFAULT_COLLATION_OK", "pass", "info", observed={"value": default}))
        return results

    @staticmethod
    def _check_replica(status, vars_, writable_endpoint, replica_host, replica_port, check_config: dict[str, bool] | None = None, *, node_type: str = "mysql"):
        checks = normalize_check_config(check_config)
        results = []
        if node_type == "goldendb":
            logger.debug("GoldenDB 节点跳过 master_wait_no_slave 和 slave_parallel_workers 参数检查")
        slave_rows = status.get("slave_status") or []
        if not slave_rows and checks["check_replication_status"]:
            results.append(_result("REPLICATION_STATUS_MISSING", "error", "error", message="备节点没有复制状态"))

        if checks["check_semi_sync"]:
            for name, code in (
                ("rpl_semi_sync_master_wait_no_slave", "SEMI_SYNC_MASTER_WAIT_NO_SLAVE"),
                ("rpl_semi_sync_slave_enabled", "SEMI_SYNC_SLAVE_ENABLED"),
            ):
                if node_type == "goldendb" and name == "rpl_semi_sync_master_wait_no_slave":
                    continue
                if name not in vars_:
                    results.append(_result(code, "not_applicable", "info", message=f"{name} 未安装或不可用"))
                    continue
                enabled = _normalize_bool(vars_.get(name))
                if enabled is None:
                    results.append(_result(code, "unknown", "unknown", observed={name: vars_.get(name)}, message=f"{name} 无法解析"))
                elif enabled:
                    results.append(_result(code, "error", "error", expected={name: False}, observed={name: vars_.get(name)}, message=f"{name} 应关闭"))
                else:
                    results.append(_result(code, "pass", "info", observed={name: vars_.get(name)}))

        if checks["check_slave_parallel_workers"] and node_type != "goldendb":
            if "slave_parallel_workers" not in vars_:
                results.append(_result("SLAVE_PARALLEL_WORKERS", "unknown", "unknown", message="无法获取 slave_parallel_workers"))
            else:
                try:
                    workers = int(vars_["slave_parallel_workers"])
                except (TypeError, ValueError):
                    workers = None
                if workers is None:
                    results.append(_result("SLAVE_PARALLEL_WORKERS", "unknown", "unknown", observed={"value": vars_.get("slave_parallel_workers")}, message="slave_parallel_workers 无法解析"))
                elif workers != 0:
                    results.append(_result("SLAVE_PARALLEL_WORKERS", "error", "error", expected={"value": 0}, observed={"value": workers}, message="备节点不应启用并行复制"))
                else:
                    results.append(_result("SLAVE_PARALLEL_WORKERS", "pass", "info", observed={"value": workers}))

        if not slave_rows:
            return results

        matching: list[Any] = []
        uncertain = False
        for candidate in slave_rows:
            master_host = str(_row_value(candidate, "Master_Host", "") or "")
            master_port = int(_row_value(candidate, "Master_Port", 0) or 0)
            if not writable_endpoint or master_port != writable_endpoint[3]:
                continue
            host_match = _hosts_match(master_host, writable_endpoint[2])
            if host_match is True:
                matching.append(candidate)
            elif host_match is None:
                uncertain = True

        if checks["check_replication_source"] and writable_endpoint and not matching:
            if uncertain:
                results.append(_result("REPLICATION_SOURCE_UNKNOWN", "unknown", "unknown", message="复制源主机 DNS 无法解析，无法确认是否指向当前主节点"))
            else:
                results.append(_result("REPLICATION_SOURCE_MISMATCH", "error", "error", expected={"host": writable_endpoint[2], "port": writable_endpoint[3]}, message="复制源未指向当前主节点"))
        elif checks["check_replication_source"] and len(matching) > 1:
            results.append(_result("REPLICATION_SOURCE_AMBIGUOUS", "unknown", "unknown", observed={"matches": len(matching)}, message="存在多个匹配的复制通道"))
        row = matching[0] if matching else slave_rows[0]

        if checks["check_replication_status"]:
            active_channels = [candidate for candidate in slave_rows if str(_row_value(candidate, "Slave_IO_Running", "")).lower() == "yes" or str(_row_value(candidate, "Slave_SQL_Running", "")).lower() == "yes"]
            extras = [candidate for candidate in active_channels if candidate is not row]
            if extras:
                results.append(_result("UNEXPECTED_REPLICATION_CHANNEL", "warning", "warning", observed={"count": len(extras)}, message="存在额外运行中的复制通道"))
            for field, code in (("Slave_IO_Running", "REPLICATION_IO_STOPPED"), ("Slave_SQL_Running", "REPLICATION_SQL_STOPPED")):
                value = str(_row_value(row, field, ""))
                if value.lower() != "yes":
                    results.append(_result(code, "error", "error", expected={field: "Yes"}, observed={field: value}, message=f"{field} 未运行"))
            for field, code in (("Last_IO_Error", "REPLICATION_IO_ERROR"), ("Last_SQL_Error", "REPLICATION_SQL_ERROR")):
                value = str(_row_value(row, field, "") or "")
                if value:
                    results.append(_result(code, "error", "error", observed={field: value[:500]}, message=f"{field} 非空"))

        if checks["check_replication_source"] and writable_endpoint:
            expected_host, expected_port = writable_endpoint[2], writable_endpoint[3]
            master_host, master_port = str(_row_value(row, "Master_Host", "")), int(_row_value(row, "Master_Port", 0) or 0)
            if master_port == expected_port and _hosts_match(master_host, expected_host) is True:
                results.append(_result("REPLICATION_SOURCE_OK", "pass", "info"))
        return results

    @staticmethod
    def _check_gtid(primary_vars, replica_vars, threshold):
        results = []
        if str(primary_vars.get("gtid_mode", "")).upper() != "ON" or str(replica_vars.get("gtid_mode", "")).upper() != "ON":
            return [_result("GTID_DISABLED", "error", "error", message="任一端 gtid_mode 未开启")]
        if not str(primary_vars.get("gtid_executed", "")).strip() or not str(replica_vars.get("gtid_executed", "")).strip():
            return [_result("GTID_UNAVAILABLE", "unknown", "unknown", message="任一端 gtid_executed 不可用")]
        try:
            primary, replica = parse_gtid_set(primary_vars.get("gtid_executed")), parse_gtid_set(replica_vars.get("gtid_executed"))
        except Exception as exc:
            return [_result("GTID_INVALID", "unknown", "unknown", message=f"GTID 集合无法解析: {exc}")]
        lag, extra = gtid_difference(primary, replica)
        if extra:
            extra_set = subtract_gtid_set(replica, primary)
            results.append(_result(
                "GTID_EXTRA_ON_REPLICA", "error", "error",
                observed={"count": gtid_transaction_count(extra_set), "gtid_set": format_gtid_set(extra_set)},
                message="备节点存在主节点没有的 GTID",
            ))
        if lag == 0:
            results.append(_result("GTID_LAG", "pass", "info", observed={"count": 0, "threshold": threshold}))
        elif lag <= threshold:
            results.append(_result("GTID_LAG", "warning", "warning", observed={"count": lag, "threshold": threshold}, message="GTID 落后量在阈值内"))
        else:
            results.append(_result("GTID_LAG", "error", "error", observed={"count": lag, "threshold": threshold}, message="GTID 落后量超过阈值"))
        return results

    @classmethod
    async def _update_issue(cls, session, relation, item, audit_events=None):
        audit_events = audit_events if audit_events is not None else []
        code, status, severity = item["check_code"], item["status"], item["severity"]
        open_issue = await session.scalar(select(HeterogeneousReplicationIssue).where(
            HeterogeneousReplicationIssue.relation_id == relation.id,
            HeterogeneousReplicationIssue.check_code == code,
            HeterogeneousReplicationIssue.status == "open",
        ).order_by(HeterogeneousReplicationIssue.id.desc()))
        if status in {"warning", "error", "unknown"}:
            now = datetime.utcnow()
            if severity == "unknown" and code in DELAYED_UNKNOWN_CODES and open_issue is None:
                latest = await session.scalar(select(HeterogeneousReplicationIssue).where(
                    HeterogeneousReplicationIssue.relation_id == relation.id,
                    HeterogeneousReplicationIssue.check_code == code,
                ).order_by(HeterogeneousReplicationIssue.id.desc()))
                if latest and latest.status == "observing":
                    latest.consecutive_failure_count += 1
                    latest.last_seen_at = now
                    latest.updated_at = now
                    latest.latest_observed = item.get("observed_value")
                    latest.summary = item.get("message") or code
                    if latest.consecutive_failure_count >= 2:
                        latest.status = "open"
                        latest.occurrence_count = max(1, latest.occurrence_count)
                        audit_events.append(("issue_opened", {"check_code": code, "severity": severity}))
                    return
                session.add(HeterogeneousReplicationIssue(
                    relation_id=relation.id, check_code=code, status="observing", severity=severity,
                    summary=item.get("message") or code, latest_observed=item.get("observed_value"),
                    first_seen_at=now, last_seen_at=now, occurrence_count=0, consecutive_failure_count=1,
                ))
                audit_events.append(("issue_observing", {"check_code": code, "severity": severity}))
                return
            if open_issue:
                open_issue.severity = severity
                open_issue.summary = item.get("message") or code
                open_issue.latest_observed = item.get("observed_value")
                open_issue.last_seen_at = now
                open_issue.updated_at = now
                open_issue.occurrence_count += 1
                if severity == "unknown" and code in DELAYED_UNKNOWN_CODES:
                    open_issue.consecutive_failure_count += 1
            else:
                session.add(HeterogeneousReplicationIssue(
                    relation_id=relation.id, check_code=code, status="open", severity=severity,
                    summary=item.get("message") or code, latest_observed=item.get("observed_value"),
                    first_seen_at=now, last_seen_at=now, occurrence_count=1,
                ))
                audit_events.append(("issue_opened", {"check_code": code, "severity": severity}))
        elif status in {"pass", "not_applicable"}:
            latest = await session.scalar(select(HeterogeneousReplicationIssue).where(
                HeterogeneousReplicationIssue.relation_id == relation.id,
                HeterogeneousReplicationIssue.check_code == code,
                HeterogeneousReplicationIssue.status.in_(["open", "observing"]),
            ).order_by(HeterogeneousReplicationIssue.id.desc()))
            if latest:
                latest.status = "resolved"
                latest.resolved_at = datetime.utcnow()
                latest.updated_at = latest.resolved_at
                audit_events.append(("issue_resolved", {"check_code": code}))

    @classmethod
    async def _resolve_absent_issues(cls, session, relation, results, audit_events=None):
        audit_events = audit_events if audit_events is not None else []
        failing_codes = {
            item["check_code"] for item in results
            if item["status"] in {"warning", "error", "unknown"}
        }
        active = await session.execute(select(HeterogeneousReplicationIssue).where(
            HeterogeneousReplicationIssue.relation_id == relation.id,
            HeterogeneousReplicationIssue.status.in_(["open", "observing"]),
        ))
        now = datetime.utcnow()
        for issue in active.scalars().all():
            if issue.check_code not in failing_codes:
                issue.status = "resolved"
                issue.resolved_at = now
                issue.updated_at = now
                audit_events.append(("issue_resolved", {"check_code": issue.check_code}))


def _hosts_match(left: str, right: str) -> bool | None:
    left_host = left.strip().lower()
    right_host = right.strip().lower()
    if not left_host or not right_host:
        return False
    if left_host == right_host:
        return True
    # A replica inside Docker reaches a host-published source through Docker's
    # host alias, while the host-side monitor reaches the same published port
    # through loopback. Callers also require the source ports to be identical.
    alias_host, other_host = (
        (left_host, right_host) if left_host in _DOCKER_HOST_ALIASES else (right_host, left_host)
    )
    if alias_host in _DOCKER_HOST_ALIASES:
        try:
            if ipaddress.ip_address(other_host).is_loopback:
                return True
        except ValueError:
            pass
    try:
        return ipaddress.ip_address(left_host).compressed == ipaddress.ip_address(right_host).compressed
    except ValueError:
        try:
            left_ips = {item[4][0] for item in socket.getaddrinfo(left_host, None, type=socket.SOCK_STREAM)}
            right_ips = {item[4][0] for item in socket.getaddrinfo(right_host, None, type=socket.SOCK_STREAM)}
        except OSError:
            return None
        return bool(left_ips & right_ips)
