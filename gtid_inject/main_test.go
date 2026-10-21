package main

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"
)

type injectTestConnector struct{ conn *injectTestConn }

func (c injectTestConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (injectTestConnector) Driver() driver.Driver                          { return injectTestDriver{} }

type injectTestDriver struct{}

func (injectTestDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unexpected Open") }

type injectTestConn struct {
	queries []string
	failAt  int
}

func (*injectTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}
func (*injectTestConn) Close() error              { return nil }
func (*injectTestConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected Begin") }
func (c *injectTestConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.queries = append(c.queries, query)
	if len(c.queries) == c.failAt {
		return nil, errors.New("injected failure")
	}
	return driver.RowsAffected(0), nil
}

func TestInjectJobBatchesAndCountsOnlySuccessfulQueries(t *testing.T) {
	for _, tc := range []struct {
		name      string
		failAt    int
		wantCount uint64
	}{
		{"success", 0, 250},
		{"second query fails", 2, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &injectTestConn{failAt: tc.failAt}
			db := sql.OpenDB(injectTestConnector{fake})
			defer db.Close()
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			var completed uint64
			err = injectJob(conn, job{testUUID, 1, 250}, &completed)
			if (err != nil) != (tc.failAt != 0) || completed != tc.wantCount {
				t.Fatalf("injectJob error = %v, completed = %d; want %d", err, completed, tc.wantCount)
			}
			wantSizes := []int{100, 100, 50}
			if tc.failAt != 0 {
				wantSizes = wantSizes[:tc.failAt]
			}
			if len(fake.queries) != len(wantSizes) {
				t.Fatalf("got %d queries, want %d", len(fake.queries), len(wantSizes))
			}
			for i, query := range fake.queries {
				if got := strings.Count(query, "SET SESSION GTID_NEXT = '"); got != wantSizes[i] {
					t.Errorf("query %d contains %d GTIDs, want %d", i, got, wantSizes[i])
				}
				if got := strings.Count(query, "; BEGIN; COMMIT"); got != wantSizes[i] {
					t.Errorf("query %d contains %d commits, want %d", i, got, wantSizes[i])
				}
				first, last := i*100+1, i*100+wantSizes[i]
				if !strings.HasPrefix(query, fmt.Sprintf("SET SESSION GTID_NEXT = '%s:%d'; BEGIN; COMMIT", testUUID, first)) ||
					!strings.HasSuffix(query, fmt.Sprintf("SET SESSION GTID_NEXT = '%s:%d'; BEGIN; COMMIT", testUUID, last)) {
					t.Errorf("query %d has wrong GTID boundaries", i)
				}
			}
		})
	}
}

func TestChineseCommandLineMessages(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"--unknown"}, "未知参数：--unknown"},
		{[]string{"--target"}, "参数 --target 缺少值"},
		{[]string{"--batch-size=abc"}, "参数 --batch-size 的值 \"abc\" 无效"},
		{[]string{"---bad"}, "参数格式无效：---bad"},
	} {
		var output bytes.Buffer
		err := run(tc.args, strings.NewReader(""), &output)
		if err == nil || err.Error() != tc.want {
			t.Errorf("run(%q) error = %v, want %q", tc.args, err, tc.want)
		}
		if !strings.Contains(output.String(), "用法：gtid_inject [选项]") || strings.Contains(output.String(), "default") {
			t.Errorf("run(%q) printed untranslated usage: %q", tc.args, output.String())
		}
	}
	var output bytes.Buffer
	if err := run([]string{"--help"}, strings.NewReader(""), &output); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help error = %v", err)
	}
	if !strings.Contains(output.String(), "用法：gtid_inject [选项]") || !strings.Contains(output.String(), "目标 MySQL 地址") {
		t.Fatalf("help output is not localized: %q", output.String())
	}
	if !strings.Contains(output.String(), "--progress-interval") || !strings.Contains(output.String(), "默认：5s") {
		t.Fatalf("help output lacks progress interval: %q", output.String())
	}
	if err := run([]string{"--target=127.0.0.1:3306", "--config=unused.json", "--gtid-set=" + testUUID + ":1", "--progress-interval=-1s"}, strings.NewReader(""), &output); err == nil || err.Error() != "--progress-interval 不能为负数" {
		t.Fatalf("negative progress interval error = %v", err)
	}
}

func TestProgressLine(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.FixedZone("CST", 8*3600))
	line := progressLine(250, 1000, 10*time.Second, now)
	for _, want := range []string{"250/1,000（25.0%）", "已用时 10秒", "平均速度 25.0 GTID/秒", "预计剩余 30秒", "预计完成 2026-10-16 12:00:30"} {
		if !strings.Contains(line, want) {
			t.Errorf("progress line %q does not contain %q", line, want)
		}
	}
	if line := progressLine(0, 1000, 5*time.Second, now); !strings.Contains(line, "计算中") {
		t.Errorf("zero-progress line = %q", line)
	}
	if line := progressLine(1000, 1000, time.Hour, now); !strings.Contains(line, "已完成") {
		t.Errorf("completed-progress line = %q", line)
	}
	if got := formatDuration(time.Hour + 2*time.Minute + 3*time.Second); got != "1小时02分03秒" {
		t.Errorf("duration = %q", got)
	}
}
