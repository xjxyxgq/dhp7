# GTID 空事务注入工具

将指定的 GTID 集合补充到目标 MySQL。支持 Oracle MySQL 5.7 和 8.0。来源可以是命令行中的 GTID 集合，也可以是多个 MySQL 服务在程序启动时各读取一次的 `@@GLOBAL.gtid_executed` 快照。

## 构建

```sh
cd gtid_inject
go build -o gtid_inject .
```

需要 Go 1.24 或更新版本。程序依赖 `github.com/go-sql-driver/mysql`，构建时按 `go.mod` 下载。直接运行源码请在此目录执行 `go run .`，因为程序还包含 `gtid.go`，不能只运行 `main.go`。`go` 命令与 `GOROOT` 必须指向同一套 Go 工具链。

## 凭据配置

将 JSON 配置文件设为仅自己可读，例如 `chmod 600 credentials.json`：

```json
{
  "default": {"user": "operator", "password": "default-password"},
  "192.168.1.10_3306": {"user": "target-user", "password": "target-password"},
  "192.168.1.20_3306": {"user": "source-user", "password": "source-password"}
}
```

程序以命令中的主机地址和端口拼成 `host_port` 查找凭据，找不到时使用 `default`。IPv6 地址用 `[::1]:3306` 写在命令中，其配置键是 `::1_3306`。同一服务如果有 IP 和域名两种写法，应分别配置相应的键。

## 使用

直接提供目标 GTID 集合：

```sh
./gtid_inject --target 192.168.1.10:3306 --config credentials.json \
  --gtid-set 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100:105-110'
```

合并多个来源库的快照：

```sh
./gtid_inject --target 192.168.1.10:3306 --config credentials.json \
  --source 192.168.1.20:3306 --source 192.168.1.21:3306
```

默认先展示来源、目标、缺少和额外的 GTID，并在注入前要求输入 `yes`。默认每个任务包含同一 UUID 的最多 500 个连续 GTID，使用 10 条并发目标库连接。每个 GTID 仍单独执行 `SET GTID_NEXT; BEGIN; COMMIT`，但每次请求最多发送 100 个 GTID 的语句，减少客户端与 MySQL 的往返。可用 `--batch-size` 调整任务大小，用 `--concurrency` 调整连接数。注入期间默认每 5 秒输出一次已完成数量、比例、平均速度、预计剩余时长和预计完成时间；可用 `--progress-interval=10s` 调整间隔，或用 `--progress-interval=0` 关闭进度输出。`--non-interactive` 会打印确认信息并自动执行。

如果目标库原本已有目标集合之外的 GTID，默认退出并提示使用 `--force`。指定 `--force` 后会展示额外集合，再按交互或非交互模式确认执行。注入期间可以有其他业务写入；最终只要求目标库包含来源快照的全部 GTID，额外 GTID 会单独列出。

目标库必须启用 `gtid_mode=ON` 和 binary log，注入连接的 `sql_log_bin` 必须开启。账号需要读取 GTID 状态、设置会话 `GTID_NEXT` 并提交事务的权限。任何任务出错后，程序停止分配新任务，等待已开始的任务结束，重新读取目标库并报告剩余缺口，以非零状态退出。再次运行会从当前集合重新计算差集。

## 速度排查

多语句请求主要减少网络往返；每个 GTID 仍需 MySQL 提交一次事务并记录 binary log。若速度仍低，可在目标库只读检查 `SHOW VARIABLES LIKE 'sync_binlog'`、`SHOW VARIABLES LIKE 'innodb_flush_log_at_trx_commit'`，并观察磁盘延迟和 CPU。`--concurrency` 可以在 10、20、40 等值下对比，过高可能增加锁竞争和磁盘压力。调整持久化设置会改变故障后的数据安全保证，不能仅为提速而直接修改。出错时，一次请求中已执行的部分 GTID 可能未计入“客户端确认提交”数，最终以重新读取的 GTID 集合和剩余缺口为准。
