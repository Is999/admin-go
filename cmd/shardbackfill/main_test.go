package main

import (
	"bytes"
	"crypto/tls"
	"hash/crc32"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

// TestValidateOptionsAcceptsBoundedRun 验证生产回填参数允许 user 使用 id 计算固定桶。
func TestValidateOptionsAcceptsBoundedRun(t *testing.T) {
	err := validateOptions(options{
		Action:        actionRun,
		Job:           "user_20260722",
		Table:         "user",
		PrimaryKey:    "id",
		UIDColumn:     "id",
		InsertTrigger: "user_insert_guard",
		UpdateTrigger: "user_update_guard",
		RangeEnd:      100,
		BatchSize:     100,
		Pause:         time.Second,
		BatchTimeout:  time.Minute,
	})
	if err != nil {
		t.Fatalf("validateOptions() error = %v", err)
	}
}

// TestValidateOptionsRejectsUnsafeInputs 验证 SQL 标识符、无界批次和非法范围会被拒绝。
func TestValidateOptionsRejectsUnsafeInputs(t *testing.T) {
	base := options{
		Action:        actionRun,
		Job:           "job_1",
		Table:         "user",
		PrimaryKey:    "id",
		UIDColumn:     "id",
		InsertTrigger: "user_insert_guard",
		UpdateTrigger: "user_update_guard",
		RangeEnd:      100,
		BatchSize:     100,
		BatchTimeout:  time.Minute,
	}
	tests := []struct {
		name string         // name 表示非法参数场景。
		edit func(*options) // edit 注入单个非法参数。
		want string         // want 表示预期错误片段。
	}{
		{name: "unsafe table", edit: func(value *options) { value.Table = "user;DROP_TABLE" }, want: "非法标识符"},
		{name: "unbounded batch", edit: func(value *options) { value.BatchSize = maxBatchSize + 1 }, want: "batch-size"},
		{name: "empty range", edit: func(value *options) { value.RangeEnd = value.RangeStart }, want: "range-end"},
		{name: "long timeout", edit: func(value *options) { value.BatchTimeout = 11 * time.Minute }, want: "batch-timeout"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			test.edit(&value)
			if err := validateOptions(value); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateOptions() error = %v, want %q", err, test.want)
			}
		})
	}
}

// TestParseOptionsRejectsInsecureBypass 验证生产命令不再暴露关闭 TLS 校验的参数。
func TestParseOptionsRejectsInsecureBypass(t *testing.T) {
	if _, err := parseOptions([]string{"-allow-insecure"}); err == nil {
		t.Fatal("parseOptions() should reject removed allow-insecure flag")
	}
}

// TestSecureTLSConfigRejectsDowngrade 验证明文、跳过校验和可降级 TLS 都不能用于生产。
func TestSecureTLSConfigRejectsDowngrade(t *testing.T) {
	for _, config := range []*mysql.Config{
		nil,
		{},
		{TLS: &tls.Config{InsecureSkipVerify: true}},
		{TLS: &tls.Config{}, AllowFallbackToPlaintext: true},
	} {
		if secureTLSConfig(config) {
			t.Fatalf("secureTLSConfig(%+v) = true, want false", config)
		}
	}
	config := &mysql.Config{TLS: &tls.Config{}}
	if !secureTLSConfig(config) {
		t.Fatalf("secureTLSConfig(%+v) = false, want true", config)
	}
	parsed, err := mysql.ParseDSN("user:password@tcp(mysql.example:3306)/app?tls=true")
	if err != nil {
		t.Fatalf("mysql.ParseDSN() error = %v", err)
	}
	if !secureTLSConfig(parsed) {
		t.Fatalf("secureTLSConfig(tls=true) = false, config=%+v", parsed)
	}
	for _, dsn := range []string{
		"user:password@tcp(mysql.example:3306)/app?tls=skip-verify",
		"user:password@tcp(mysql.example:3306)/app?tls=preferred",
	} {
		parsed, err = mysql.ParseDSN(dsn)
		if err != nil {
			t.Fatalf("mysql.ParseDSN(%q) error = %v", dsn, err)
		}
		if secureTLSConfig(parsed) {
			t.Fatalf("secureTLSConfig(%q) = true, want false", dsn)
		}
	}
}

// TestFixedBucketMatchesMySQLCRC32 验证 Go 全量校验使用与 MySQL CRC32 相同的已知桶值。
func TestFixedBucketMatchesMySQLCRC32(t *testing.T) {
	if got := crc32.ChecksumIEEE([]byte("1")) % 1024; got != 951 {
		t.Fatalf("bucket(1) = %d, want 951", got)
	}
}

// TestPrintCheckpointOmitsSensitiveValues 验证状态日志稳定且不会输出 DSN 或业务 UID。
func TestPrintCheckpointOmitsSensitiveValues(t *testing.T) {
	var output bytes.Buffer
	err := printCheckpoint(&output, actionRun, checkpoint{
		Job:          "job_1",
		Table:        "user",
		RangeEnd:     100,
		Cursor:       50,
		VerifyCursor: 10,
		UpdatedRows:  5,
		Status:       statusRunning,
	}, 10, 5)
	if err != nil {
		t.Fatalf("printCheckpoint() error = %v", err)
	}
	for _, want := range []string{"job=job_1", "status=running", "cursor=50", "batch_rows=10", "updated_rows=5"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("printCheckpoint() missing %q: %s", want, output.String())
		}
	}
}

// TestIntegerDataType 验证游标和 UID 只接受离散整数类型。
func TestIntegerDataType(t *testing.T) {
	for _, value := range []string{"tinyint", "smallint", "mediumint", "int", "bigint", "BIGINT"} {
		if !integerDataType(value) {
			t.Fatalf("integerDataType(%q) = false, want true", value)
		}
	}
	for _, value := range []string{"decimal", "varchar", "double"} {
		if integerDataType(value) {
			t.Fatalf("integerDataType(%q) = true, want false", value)
		}
	}
}

// TestShardNoDataTypeRejectsTinyint 验证固定桶列必须完整容纳 0..1023。
func TestShardNoDataTypeRejectsTinyint(t *testing.T) {
	if shardNoDataType("tinyint") {
		t.Fatal("shardNoDataType(tinyint) = true, want false")
	}
	for _, value := range []string{"smallint", "mediumint", "int", "bigint", "INT"} {
		if !shardNoDataType(value) {
			t.Fatalf("shardNoDataType(%q) = false, want true", value)
		}
	}
}

// TestValidateCheckpointColumnsRejectsStructuralDrift 验证长度、默认值或列顺序漂移都会失败关闭。
func TestValidateCheckpointColumnsRejectsStructuralDrift(t *testing.T) {
	valid := expectedCheckpointColumns()
	if err := validateCheckpointColumns(valid); err != nil {
		t.Fatalf("validateCheckpointColumns() error = %v", err)
	}
	for _, edit := range []func([]checkpointColumn){
		func(columns []checkpointColumn) { columns[0].ColumnType = "varchar(63)" },
		func(columns []checkpointColumn) { columns[10].Default.Valid = false },
		func(columns []checkpointColumn) { columns[0], columns[1] = columns[1], columns[0] },
	} {
		columns := expectedCheckpointColumns()
		edit(columns)
		if err := validateCheckpointColumns(columns); err == nil {
			t.Fatal("validateCheckpointColumns() should reject structural drift")
		}
	}
}

// TestValidateCheckpointIndexesRejectsExtras 验证 checkpoint 不能用额外索引掩盖错误结构。
func TestValidateCheckpointIndexesRejectsExtras(t *testing.T) {
	indexes := []checkpointIndex{
		{Name: "PRIMARY", NonUnique: 0, Sequence: 1, Column: "job_name", Type: "BTREE", Visible: "YES"},
		{Name: "idx_shard_backfill_table_status", NonUnique: 1, Sequence: 1, Column: "table_name", Type: "BTREE", Visible: "YES"},
		{Name: "idx_shard_backfill_table_status", NonUnique: 1, Sequence: 2, Column: "status", Type: "BTREE", Visible: "YES"},
	}
	if err := validateCheckpointIndexes(indexes); err != nil {
		t.Fatalf("validateCheckpointIndexes() error = %v", err)
	}
	indexes = append(indexes, checkpointIndex{Name: "unexpected", NonUnique: 1, Sequence: 1, Column: "status", Type: "BTREE", Visible: "YES"})
	if err := validateCheckpointIndexes(indexes); err == nil {
		t.Fatal("validateCheckpointIndexes() should reject extra index")
	}
}

// TestValidateCheckpointChecksRejectsWeakening 验证 checkpoint 状态和游标约束不能被弱化。
func TestValidateCheckpointChecksRejectsWeakening(t *testing.T) {
	checks := []checkpointCheck{
		{Name: "chk_shard_backfill_cursor", Clause: "(`cursor_value` BETWEEN `range_start` AND `range_end`)", Enforced: "YES"},
		{Name: "chk_shard_backfill_range", Clause: "(`range_start` < `range_end`)", Enforced: "YES"},
		{Name: "chk_shard_backfill_status", Clause: "(`status` IN (_utf8mb4\\'running\\',_utf8mb4\\'backfilled\\',_utf8mb4\\'verifying\\',_utf8mb4\\'verified\\',_utf8mb4\\'mismatch\\'))", Enforced: "YES"},
		{Name: "chk_shard_verify_cursor", Clause: "(`verify_cursor` BETWEEN `range_start` AND `range_end`)", Enforced: "YES"},
	}
	if err := validateCheckpointChecks(checks); err != nil {
		t.Fatalf("validateCheckpointChecks() error = %v", err)
	}
	checks[2].Clause = "status <> ''"
	if err := validateCheckpointChecks(checks); err == nil {
		t.Fatal("validateCheckpointChecks() should reject weakened status constraint")
	}
}

// TestNormalizeCheckClauseAcceptsConnectionCharsets 验证连接字符集变化不会把同一条 CHECK 约束误判为结构漂移。
func TestNormalizeCheckClauseAcceptsConnectionCharsets(t *testing.T) {
	want := normalizeCheckClause("status IN ('running','backfilled')")
	for _, clause := range []string{
		"(`status` IN (_utf8mb4\\'running\\',_utf8mb4\\'backfilled\\'))",
		"(`status` IN (_latin1\\'running\\',_latin1\\'backfilled\\'))",
	} {
		if got := normalizeCheckClause(clause); got != want {
			t.Fatalf("normalizeCheckClause(%q) = %q, want %q", clause, got, want)
		}
	}
	if got := normalizeCheckClause("`cursor_value` BETWEEN `range_start` AND `range_end`"); got != "cursor_valuebetweenrange_startandrange_end" {
		t.Fatalf("标识符下划线被错误改写: %q", got)
	}
	if got := normalizeCheckClause("status IN ('running')"); got != "statusin('running')" {
		t.Fatalf("具有语义的函数括号被错误改写: %q", got)
	}
}

// TestValidGuardStatement 验证保护触发器必须同时覆盖固定桶、UID 和游标不可变语义。
func TestValidGuardStatement(t *testing.T) {
	opts := options{PrimaryKey: "id", UIDColumn: "uid"}
	insert := "BEGIN IF NEW.`id` IS NULL OR NEW.`id` <= 0 OR NEW.`shard_no` IS NULL OR NEW.`uid` IS NULL OR NEW.`uid` <= 0 OR NEW.`shard_no` <> MOD(CRC32(CAST(NEW.`uid` AS CHAR)), 1024) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'shard_no does not match fixed user bucket'; END IF; END"
	if !validGuardStatement(insert, opts, "INSERT") {
		t.Fatal("validGuardStatement() rejected valid insert guard")
	}
	update := "BEGIN IF NOT (NEW.`id` <=> OLD.`id`) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'backfill cursor primary key is immutable'; END IF; IF (NOT (NEW.`uid` <=> OLD.`uid`) OR NOT (NEW.`shard_no` <=> OLD.`shard_no`)) AND (NEW.`shard_no` IS NULL OR NEW.`uid` IS NULL OR NEW.`uid` <= 0 OR NEW.`shard_no` <> MOD(CRC32(CAST(NEW.`uid` AS CHAR)), 1024)) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'shard_no does not match fixed user bucket'; END IF; END"
	if !validGuardStatement(update, opts, "UPDATE") {
		t.Fatal("validGuardStatement() rejected valid update guard")
	}
	if validGuardStatement(strings.ReplaceAll(update, "NEW.`id` <=> OLD.`id`", "TRUE"), opts, "UPDATE") {
		t.Fatal("validGuardStatement() accepted update guard without immutable primary key")
	}
}
