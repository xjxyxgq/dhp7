package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
)

type credentials struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

type address struct{ host, port string }

func (a address) String() string { return net.JoinHostPort(a.host, a.port) }
func (a address) key() string    { return a.host + "_" + a.port }

func parseAddress(raw string) (address, error) {
	host, port, err := net.SplitHostPort(raw)
	if err != nil || host == "" {
		return address{}, fmt.Errorf("数据库地址格式无效：%q，应为 host:port", raw)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return address{}, fmt.Errorf("数据库地址 %q 中的端口无效", raw)
	}
	return address{host, strconv.Itoa(n)}, nil
}

type stringsFlag []string

func (s *stringsFlag) String() string { return strings.Join(*s, ",") }
func (s *stringsFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func loadCredentials(path string) (map[string]credentials, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取凭据配置文件 %q 失败：%w", path, err)
	}
	var entries map[string]credentials
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("解析凭据配置文件失败：%w", err)
	}
	if entries["default"].User == "" {
		return nil, errors.New("凭据配置文件必须包含 default.user")
	}
	return entries, nil
}

func openDB(a address, entries map[string]credentials, maxOpen int, multiStatements bool) (*sql.DB, error) {
	login, ok := entries[a.key()]
	if !ok {
		login = entries["default"]
	}
	if login.User == "" {
		return nil, fmt.Errorf("数据库 %s 的用户名为空", a)
	}
	config := mysql.NewConfig()
	config.User = login.User
	config.Passwd = login.Password
	config.Net = "tcp"
	config.Addr = a.String()
	config.Timeout = 10 * time.Second
	config.ReadTimeout = 30 * time.Second
	config.WriteTimeout = 30 * time.Second
	config.MultiStatements = multiStatements
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(0) // Worker sessions with GTID_NEXT must never return to the pool.
	return db, nil
}

func readSnapshot(ctx context.Context, db *sql.DB, target bool) (gtidSet, error) {
	var mode, binlog, executed string
	err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode, @@GLOBAL.log_bin, @@GLOBAL.gtid_executed").Scan(&mode, &binlog, &executed)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(mode, "ON") {
		return nil, fmt.Errorf("GTID 模式为 %s，必须为 ON", mode)
	}
	if target && binlog != "1" && !strings.EqualFold(binlog, "ON") {
		return nil, errors.New("目标数据库未启用二进制日志")
	}
	return parseGTIDSet(executed)
}

func confirm(in io.Reader, out io.Writer) error {
	fmt.Fprint(out, "确认注入缺失的 GTID？请输入 yes 继续：")
	line, err := bufio.NewReader(in).ReadString('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("读取确认输入失败：%w", err)
	}
	if strings.TrimSpace(line) != "yes" {
		return errors.New("已取消注入")
	}
	return nil
}

func chineseFlagError(err error) error {
	message := err.Error()
	if strings.HasPrefix(message, "flag provided but not defined: -") {
		return fmt.Errorf("未知参数：--%s", strings.TrimPrefix(message, "flag provided but not defined: -"))
	}
	if strings.HasPrefix(message, "flag needs an argument: -") {
		return fmt.Errorf("参数 --%s 缺少值", strings.TrimPrefix(message, "flag needs an argument: -"))
	}
	if strings.HasPrefix(message, "bad flag syntax: ") {
		return fmt.Errorf("参数格式无效：%s", strings.TrimPrefix(message, "bad flag syntax: "))
	}
	for _, pattern := range []struct{ prefix, separator string }{
		{"invalid value ", " for flag -"},
		{"invalid boolean value ", " for -"},
	} {
		if strings.HasPrefix(message, pattern.prefix) {
			value := strings.TrimPrefix(message, pattern.prefix)
			if value, nameAndReason, ok := strings.Cut(value, pattern.separator); ok {
				name, _, _ := strings.Cut(nameAndReason, ":")
				return fmt.Errorf("参数 --%s 的值 %s 无效", name, value)
			}
		}
	}
	return fmt.Errorf("命令行参数无效：%w", err)
}

func run(args []string, in io.Reader, out io.Writer) error {
	flags := flag.NewFlagSet("gtid_inject", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.Usage = func() {
		fmt.Fprintln(out, "用法：gtid_inject [选项]")
		flags.VisitAll(func(option *flag.Flag) {
			fmt.Fprintf(out, "  --%s：%s", option.Name, option.Usage)
			if option.DefValue != "" && option.DefValue != "false" {
				fmt.Fprintf(out, "（默认：%s）", option.DefValue)
			}
			fmt.Fprintln(out)
		})
	}
	var targetRaw, explicitGTID, configPath string
	var sources stringsFlag
	var batchSize, concurrency int
	var progressInterval time.Duration
	var force, nonInteractive bool
	flags.StringVar(&targetRaw, "target", "", "目标 MySQL 地址，格式为 host:port")
	flags.StringVar(&explicitGTID, "gtid-set", "", "最终目标 GTID 集合，不能与 --source 同时使用")
	flags.Var(&sources, "source", "来源 MySQL 地址，格式为 host:port，可重复指定")
	flags.StringVar(&configPath, "config", "", "JSON 凭据配置文件路径")
	flags.IntVar(&batchSize, "batch-size", 500, "每个任务处理的同一 UUID 的最多 GTID 数量")
	flags.IntVar(&concurrency, "concurrency", 10, "目标数据库的并发连接数")
	flags.DurationVar(&progressInterval, "progress-interval", 5*time.Second, "进度输出间隔（如 5s、1m）；0 表示不输出进度")
	flags.BoolVar(&force, "force", false, "目标库有额外 GTID 时仍继续注入")
	flags.BoolVar(&nonInteractive, "non-interactive", false, "显示确认信息并自动继续")
	if err := flags.Parse(args); err != nil {
		return chineseFlagError(err)
	}
	if flags.NArg() != 0 || targetRaw == "" || configPath == "" || (explicitGTID == "") == (len(sources) == 0) || batchSize < 1 || concurrency < 1 {
		return errors.New("必须指定 --target、--config，并且在 --gtid-set 与 --source 中选择一项；--batch-size 和 --concurrency 必须为正数，不能有多余参数")
	}
	if progressInterval < 0 {
		return errors.New("--progress-interval 不能为负数")
	}
	target, err := parseAddress(targetRaw)
	if err != nil {
		return err
	}
	entries, err := loadCredentials(configPath)
	if err != nil {
		return err
	}
	ctx := context.Background()
	desired := make(gtidSet)
	if explicitGTID != "" {
		desired, err = parseGTIDSet(explicitGTID)
		if err != nil {
			return fmt.Errorf("解析 --gtid-set 失败：%w", err)
		}
		fmt.Fprintf(out, "提供的目标 GTID 集合：\n%s\n", desired)
	} else {
		for _, raw := range sources {
			source, err := parseAddress(raw)
			if err != nil {
				return err
			}
			db, err := openDB(source, entries, 1, false)
			if err != nil {
				return fmt.Errorf("准备来源数据库 %s 的连接失败：%w", source, err)
			}
			set, readErr := readSnapshot(ctx, db, false)
			db.Close()
			if readErr != nil {
				return fmt.Errorf("读取来源数据库 %s 的 GTID 失败：%w", source, readErr)
			}
			fmt.Fprintf(out, "来源数据库 %s 的 GTID 集合：\n%s\n", source, set)
			desired.add(set)
		}
		fmt.Fprintf(out, "来源数据库合并后的目标 GTID 集合：\n%s\n", desired)
	}
	targetDB, err := openDB(target, entries, concurrency, true)
	if err != nil {
		return fmt.Errorf("准备目标数据库 %s 的连接失败：%w", target, err)
	}
	defer targetDB.Close()
	before, err := readSnapshot(ctx, targetDB, true)
	if err != nil {
		return fmt.Errorf("读取目标数据库 %s 的 GTID 失败：%w", target, err)
	}
	missing, extra := desired.subtract(before), before.subtract(desired)
	fmt.Fprintf(out, "目标数据库 %s 注入前的 GTID 集合：\n%s\n待补充的 GTID：\n%s\n注入前额外的 GTID：\n%s\n", target, before, missing, extra)
	if !extra.empty() && !force {
		return errors.New("目标数据库存在额外 GTID；请检查上述差异，确认后可用 --force 继续")
	}
	if !extra.empty() {
		fmt.Fprintln(out, "已指定 --force，将在保留上述额外 GTID 的情况下继续。")
	}
	var injected uint64
	var injectionErr error
	if !missing.empty() {
		if nonInteractive {
			fmt.Fprintln(out, "已通过 --non-interactive 自动确认注入。")
		} else if err := confirm(in, out); err != nil {
			return err
		}
		injected, injectionErr = inject(targetDB, missing, batchSize, concurrency, progressInterval, out)
		fmt.Fprintf(out, "客户端确认提交的空事务数：%s\n", formatCount(injected))
	}
	after, err := readSnapshot(ctx, targetDB, true)
	if err != nil {
		if injectionErr != nil {
			return fmt.Errorf("注入后读取目标数据库的 GTID 失败：%w；注入过程还发生错误：%v", err, injectionErr)
		}
		return fmt.Errorf("注入后读取目标数据库的 GTID 失败：%w", err)
	}
	remaining, finalExtra := desired.subtract(after), after.subtract(desired)
	fmt.Fprintf(out, "目标数据库注入后的 GTID 集合：\n%s\n仍缺失的 GTID：\n%s\n注入后额外的 GTID：\n%s\n", after, remaining, finalExtra)
	if injectionErr != nil {
		return fmt.Errorf("注入中断：%w", injectionErr)
	}
	if !remaining.empty() {
		return errors.New("完整性检查失败：目标数据库未包含全部目标 GTID")
	}
	fmt.Fprintln(out, "完整性检查通过：目标数据库已包含全部目标 GTID。")
	return nil
}

type job struct {
	uuid        string
	first, last uint64
}

func formatCount(count uint64) string {
	text := strconv.FormatUint(count, 10)
	for position := len(text) - 3; position > 0; position -= 3 {
		text = text[:position] + "," + text[position:]
	}
	return text
}

func formatDuration(duration time.Duration) string {
	seconds := int64(duration / time.Second)
	if seconds < 60 {
		return fmt.Sprintf("%d秒", seconds)
	}
	if seconds < 3600 {
		return fmt.Sprintf("%d分%02d秒", seconds/60, seconds%60)
	}
	return fmt.Sprintf("%d小时%02d分%02d秒", seconds/3600, seconds%3600/60, seconds%60)
}

func progressLine(completed, total uint64, elapsed time.Duration, now time.Time) string {
	percent := 100.0
	if total > 0 {
		percent = 100 * float64(completed) / float64(total)
	}
	line := fmt.Sprintf("注入进度：%s/%s（%.1f%%）｜已用时 %s", formatCount(completed), formatCount(total), percent, formatDuration(elapsed))
	if completed >= total {
		return line + "｜已完成"
	}
	if completed == 0 || elapsed < time.Second {
		return line + "｜预计完成时间：计算中"
	}
	rate := float64(completed) / elapsed.Seconds()
	secondsLeft := math.Ceil(float64(total-completed) / rate)
	if secondsLeft > float64(math.MaxInt64/int64(time.Second)) {
		return fmt.Sprintf("%s｜平均速度 %.1f GTID/秒｜预计完成时间：暂无法估算", line, rate)
	}
	remaining := time.Duration(secondsLeft) * time.Second
	return fmt.Sprintf("%s｜平均速度 %.1f GTID/秒｜预计剩余 %s｜预计完成 %s", line, rate, formatDuration(remaining), now.Add(remaining).Format("2006-01-02 15:04:05"))
}

func inject(db *sql.DB, missing gtidSet, batchSize, concurrency int, progressInterval time.Duration, out io.Writer) (uint64, error) {
	jobs := make(chan job, concurrency*2)
	stop := make(chan struct{})
	var once sync.Once
	var firstErr error
	var completed uint64
	var workers sync.WaitGroup
	var progressDone, progressStopped chan struct{}
	var total uint64
	var started time.Time
	if progressInterval > 0 {
		for _, ranges := range missing {
			for _, current := range ranges {
				total += current.last - current.first + 1
			}
		}
		intervalText := progressInterval.String()
		if progressInterval >= time.Second && progressInterval%time.Second == 0 {
			intervalText = formatDuration(progressInterval)
		}
		fmt.Fprintf(out, "开始注入：共 %s 个 GTID，每 %s 更新进度。\n", formatCount(total), intervalText)
		progressDone, progressStopped = make(chan struct{}), make(chan struct{})
		started = time.Now()
		go func() {
			defer close(progressStopped)
			ticker := time.NewTicker(progressInterval)
			defer ticker.Stop()
			for {
				select {
				case now := <-ticker.C:
					fmt.Fprintln(out, progressLine(atomic.LoadUint64(&completed), total, now.Sub(started), now))
				case <-progressDone:
					return
				}
			}
		}()
	}
	fail := func(err error) {
		once.Do(func() { firstErr = err; close(stop) })
	}
	for i := 0; i < concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			var conn *sql.Conn
			defer func() {
				if conn != nil {
					conn.Close()
				}
			}()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var task job
				var ok bool
				select {
				case <-stop:
					return
				case task, ok = <-jobs:
					if !ok {
						return
					}
				}
				select {
				case <-stop:
					return
				default:
				}
				if conn == nil {
					var err error
					conn, err = db.Conn(context.Background())
					if err != nil {
						fail(fmt.Errorf("建立目标数据库工作连接失败：%w", err))
						return
					}
					var binlog string
					if err := conn.QueryRowContext(context.Background(), "SELECT @@SESSION.sql_log_bin").Scan(&binlog); err != nil {
						fail(fmt.Errorf("读取工作连接的二进制日志设置失败：%w", err))
						return
					}
					if binlog != "1" && !strings.EqualFold(binlog, "ON") {
						fail(errors.New("工作连接未启用二进制日志"))
						return
					}
				}
				if err := injectJob(conn, task, &completed); err != nil {
					fail(fmt.Errorf("注入 GTID %s:%d-%d 失败：%w", task.uuid, task.first, task.last, err))
					return
				}
			}
		}()
	}
	uuids := make([]string, 0, len(missing))
	for uuid := range missing {
		uuids = append(uuids, uuid)
	}
	sort.Strings(uuids)
send:
	for _, uuid := range uuids {
		for _, current := range missing[uuid] {
			for first := current.first; first <= current.last; {
				last := first + uint64(batchSize) - 1
				if last > current.last || last < first {
					last = current.last
				}
				select {
				case <-stop:
					break send
				case jobs <- job{uuid, first, last}:
				}
				if last == current.last {
					break
				}
				first = last + 1
			}
		}
	}
	close(jobs)
	workers.Wait()
	if progressInterval > 0 {
		close(progressDone)
		<-progressStopped
		if firstErr == nil {
			now := time.Now()
			fmt.Fprintln(out, progressLine(atomic.LoadUint64(&completed), total, now.Sub(started), now))
		}
	}
	return atomic.LoadUint64(&completed), firstErr
}

func injectJob(conn *sql.Conn, task job, completed *uint64) error {
	const perQuery = 100
	for first := task.first; ; {
		last := first + perQuery - 1
		if last < first || last > task.last {
			last = task.last
		}
		var statement strings.Builder
		statement.Grow(int(last-first+1) * (len(task.uuid) + 50))
		for sequence := first; ; sequence++ {
			if sequence != first {
				statement.WriteByte(';')
			}
			// UUID and sequence are validated before reaching this SQL statement.
			fmt.Fprintf(&statement, "SET SESSION GTID_NEXT = '%s:%d'; BEGIN; COMMIT", task.uuid, sequence)
			if sequence == last {
				break
			}
		}
		if _, err := conn.ExecContext(context.Background(), statement.String()); err != nil {
			return err
		}
		atomic.AddUint64(completed, last-first+1)
		if last == task.last {
			return nil
		}
		first = last + 1
	}
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintf(os.Stderr, "错误：%v\n", err)
		os.Exit(1)
	}
}
