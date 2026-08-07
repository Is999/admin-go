package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"admin/internal/model"

	"github.com/Is999/go-utils/errors"
	mysql "github.com/go-sql-driver/mysql"
)

const (
	// checkpointTable 是运维回填任务的数据库内持久化进度表。
	checkpointTable = model.TableNameShardBackfillCheckpoint
	// backfillDSNEnv 是仅通过环境或密钥注入的源库 DSN，避免口令进入命令行。
	backfillDSNEnv = "SHARD_BACKFILL_DSN"
	// maxBatchSize 限制单事务最多扫描和锁定的业务行数。
	maxBatchSize = 10000
)

const (
	// actionInit 显式创建 checkpoint 表。
	actionInit = "init"
	// actionRun 分批修正 shard_no 并推进回填游标。
	actionRun = "run"
	// actionVerify 分批全量复核固定桶公式。
	actionVerify = "verify"
	// actionStatus 只读输出 checkpoint 状态。
	actionStatus = "status"
)

const (
	// statusRunning 表示回填尚未完成。
	statusRunning = "running"
	// statusBackfilled 表示回填扫描完成但尚未通过全量校验。
	statusBackfilled = "backfilled"
	// statusVerifying 表示全量校验尚未完成。
	statusVerifying = "verifying"
	// statusVerified 表示全量校验完成且没有公式不一致。
	statusVerified = "verified"
	// statusMismatch 表示全量校验发现公式不一致。
	statusMismatch = "mismatch"
)

// options 表示一次回填或校验的有界执行参数。
type options struct {
	Action        string        // 运维动作
	Job           string        // checkpoint 唯一任务名
	Table         string        // 已审核业务表
	PrimaryKey    string        // 单调唯一主键列
	UIDColumn     string        // user.id 或 uid 列
	InsertTrigger string        // 源表插入保护触发器
	UpdateTrigger string        // 源表更新保护触发器
	RangeStart    uint64        // 起始游标，不包含
	RangeEnd      uint64        // 结束游标，包含
	BatchSize     int           // 单批最大行数
	Pause         time.Duration // 已提交批次间限速
	BatchTimeout  time.Duration // 单批事务超时
	Restart       bool          // 显式从范围起点重新扫描当前阶段
}

// checkpoint 表示源库内持久化的回填和校验进度。
type checkpoint struct {
	Job           string // 任务名
	Table         string // 表名
	PrimaryKey    string // 游标列
	UIDColumn     string // UID 列
	InsertTrigger string // 源表插入保护触发器
	UpdateTrigger string // 源表更新保护触发器
	RangeStart    uint64 // 范围起点
	RangeEnd      uint64 // 范围终点
	Cursor        uint64 // 回填游标
	VerifyCursor  uint64 // 校验游标
	UpdatedRows   uint64 // 累计修正行数
	VerifiedRows  uint64 // 累计校验行数
	MismatchRows  uint64 // 累计错误行数
	Status        string // 当前阶段
}

// batchResult 表示一次已提交批次的结果。
type batchResult struct {
	Checkpoint checkpoint // 提交后的进度
	Rows       int        // 本批扫描行数
	Changed    uint64     // 本批修正或发现不一致的行数
	Done       bool       // 当前阶段是否结束
}

// columnInfo 表示回填前需要核对的业务列元数据。
type columnInfo struct {
	Name       string // 列名
	DataType   string // MySQL 基础类型
	ColumnType string // 包含 unsigned 等修饰的完整类型
	Nullable   string // 是否允许 NULL，YES/NO
}

// checkpointColumn 表示 checkpoint 表的一列完整结构契约。
type checkpointColumn struct {
	Ordinal           int            // 列顺序
	Name              string         // 列名
	ColumnType        string         // 完整列类型
	Nullable          string         // 是否允许 NULL
	Default           sql.NullString // 默认值
	Extra             string         // 自动更新时间等扩展属性
	CharacterSet      sql.NullString // 字符集
	Collation         sql.NullString // 排序规则
	DatetimePrecision sql.NullInt64  // 时间小数秒精度
	Comment           string         // 列注释
}

// checkpointIndex 表示 checkpoint 表的一列索引定义。
type checkpointIndex struct {
	Name       string         // 索引名
	NonUnique  int            // 是否非唯一索引
	Sequence   int            // 索引列顺序
	Column     string         // 索引列名
	Prefix     sql.NullInt64  // 索引前缀长度
	Type       string         // 索引类型
	Visible    string         // 索引可见性
	Expression sql.NullString // 函数索引表达式
}

// checkpointCheck 表示 checkpoint 表的一条 CHECK 约束。
type checkpointCheck struct {
	Name     string // 约束名
	Clause   string // 约束表达式
	Enforced string // 是否强制执行
}

// main 使用信号上下文运行独立运维命令，不接入在线服务生命周期。
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runCommand(ctx, os.Args[1:], os.Getenv, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// runCommand 解析参数、建立单连接并执行指定动作。
func runCommand(ctx context.Context, args []string, getenv func(string) string, output io.Writer) error {
	if output == nil {
		return errors.New("输出目标不能为空")
	}
	opts, err := parseOptions(args)
	if err != nil {
		return err
	}
	if err = validateOptions(opts); err != nil {
		return err
	}
	dsn := strings.TrimSpace(getenv(backfillDSNEnv))
	if dsn == "" {
		return errors.Errorf("缺少环境变量 %s", backfillDSNEnv)
	}
	db, err := openDatabase(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	switch opts.Action {
	case actionInit:
		return initCheckpointTable(ctx, db, output)
	case actionStatus:
		if err = validateCheckpointSchema(ctx, db); err != nil {
			return err
		}
		return printStatus(ctx, db, opts.Job, output)
	default:
		if err = validateCheckpointSchema(ctx, db); err != nil {
			return err
		}
		return runBatches(ctx, db, opts, output)
	}
}

// parseOptions 使用独立 FlagSet，便于测试且不污染其他命令参数。
func parseOptions(args []string) (options, error) {
	opts := options{}
	flags := flag.NewFlagSet("shardbackfill", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&opts.Action, "action", "", "动作：init/run/verify/status")
	flags.StringVar(&opts.Job, "job", "", "checkpoint 唯一任务名")
	flags.StringVar(&opts.Table, "table", "", "业务表名")
	flags.StringVar(&opts.PrimaryKey, "primary-key", "id", "单调唯一主键列")
	flags.StringVar(&opts.UIDColumn, "uid-column", "uid", "业务用户 ID 列；user 表使用 id")
	flags.StringVar(&opts.InsertTrigger, "insert-trigger", "", "源表插入保护触发器名")
	flags.StringVar(&opts.UpdateTrigger, "update-trigger", "", "源表更新保护触发器名")
	flags.Uint64Var(&opts.RangeStart, "range-start", 0, "扫描起点，不包含")
	flags.Uint64Var(&opts.RangeEnd, "range-end", 0, "扫描终点，包含")
	flags.IntVar(&opts.BatchSize, "batch-size", 1000, "单批最大扫描行数，1..10000")
	flags.DurationVar(&opts.Pause, "pause", 100*time.Millisecond, "已提交批次之间的限速")
	flags.DurationVar(&opts.BatchTimeout, "batch-timeout", 30*time.Second, "单批事务超时")
	flags.BoolVar(&opts.Restart, "restart", false, "从范围起点重新扫描当前阶段")
	if err := flags.Parse(args); err != nil {
		return options{}, errors.Wrap(err, "解析参数失败")
	}
	if flags.NArg() != 0 {
		return options{}, errors.Errorf("存在未识别参数 %q", flags.Arg(0))
	}
	return opts, nil
}

// validateOptions 按动作校验标识符、范围和单批资源上限。
func validateOptions(opts options) error {
	switch opts.Action {
	case actionInit:
		if opts.Restart {
			return errors.New("init 不支持 restart")
		}
		return nil
	case actionStatus:
		_, err := validateName(opts.Job, "任务名")
		return err
	case actionRun, actionVerify:
	default:
		return errors.New("action 必须是 init/run/verify/status")
	}
	if _, err := validateName(opts.Job, "任务名"); err != nil {
		return err
	}
	if _, err := validateName(opts.Table, "表名"); err != nil {
		return err
	}
	if _, err := validateName(opts.PrimaryKey, "主键列"); err != nil {
		return err
	}
	if _, err := validateName(opts.UIDColumn, "UID列"); err != nil {
		return err
	}
	if _, err := validateName(opts.InsertTrigger, "插入触发器"); err != nil {
		return err
	}
	if _, err := validateName(opts.UpdateTrigger, "更新触发器"); err != nil {
		return err
	}
	if opts.InsertTrigger == opts.UpdateTrigger {
		return errors.New("插入和更新保护触发器不能同名")
	}
	if opts.RangeEnd <= opts.RangeStart {
		return errors.New("range-end 必须大于 range-start")
	}
	if opts.BatchSize < 1 || opts.BatchSize > maxBatchSize {
		return errors.Errorf("batch-size 必须在 1..%d 之间", maxBatchSize)
	}
	if opts.Pause < 0 || opts.Pause > time.Minute {
		return errors.New("pause 必须在 0..1m 之间")
	}
	if opts.BatchTimeout < time.Second || opts.BatchTimeout > 10*time.Minute {
		return errors.New("batch-timeout 必须在 1s..10m 之间")
	}
	return nil
}

// validateName 校验数据库标识符和任务名，禁止外部文本进入 SQL 结构。
func validateName(value string, field string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 64 {
		return "", errors.Errorf("%s不能为空且长度不能超过64", field)
	}
	for index, char := range value {
		if char == '_' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || index > 0 && char >= '0' && char <= '9' {
			continue
		}
		return "", errors.Errorf("%s包含非法标识符 %q", field, value)
	}
	return value, nil
}

// openDatabase 校验 DSN 安全边界并创建单连接数据库句柄。
func openDatabase(ctx context.Context, dsn string) (*sql.DB, error) {
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, errors.Errorf("解析 %s 失败", backfillDSNEnv)
	}
	if strings.TrimSpace(config.DBName) == "" {
		return nil, errors.Errorf("%s 必须指定源数据库", backfillDSNEnv)
	}
	if config.MultiStatements {
		return nil, errors.Errorf("%s 禁止 multiStatements", backfillDSNEnv)
	}
	if !secureTLSConfig(config) {
		return nil, errors.Errorf("%s 必须启用校验证书的 TLS，禁止跳过证书校验或降级明文", backfillDSNEnv)
	}
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Second
	}
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, errors.Wrap(err, "创建源库连接失败")
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err = db.PingContext(ctx); err != nil {
		db.Close()
		return nil, errors.Wrap(err, "连接源库失败")
	}
	return db, nil
}

// secureTLSConfig 判断 MySQL DSN 是否启用证书校验且禁止降级明文。
func secureTLSConfig(config *mysql.Config) bool {
	if config == nil || config.AllowFallbackToPlaintext {
		return false
	}
	if config.TLS != nil {
		return !config.TLS.InsecureSkipVerify
	}
	return strings.EqualFold(strings.TrimSpace(config.TLSConfig), "true")
}

// initCheckpointTable 显式创建独立 checkpoint 表。
func initCheckpointTable(ctx context.Context, db *sql.DB, output io.Writer) error {
	ddl, err := sqlAsset("checkpoint.sql.tmpl")
	if err != nil {
		return errors.Wrap(err, "读取 checkpoint DDL 失败")
	}
	if _, err = db.ExecContext(ctx, ddl); err != nil {
		return errors.Wrap(err, "创建 checkpoint 表失败")
	}
	if err = validateCheckpointSchema(ctx, db); err != nil {
		return err
	}
	if _, err = fmt.Fprintf(output, "action=init table=%s status=ready\n", checkpointTable); err != nil {
		return errors.Wrap(err, "输出 checkpoint 初始化状态失败")
	}
	return nil
}

// validateCheckpointSchema 确认既有 checkpoint 表与当前命令版本完全兼容。
func validateCheckpointSchema(ctx context.Context, db *sql.DB) error {
	tableQuery, err := sqlAsset("checkpoint-table.sql.tmpl")
	if err != nil {
		return errors.Tag(err)
	}
	var engine sql.NullString
	var collation sql.NullString
	var tableComment string
	var createOptions string
	if err = db.QueryRowContext(ctx, tableQuery).Scan(&engine, &collation, &tableComment, &createOptions); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.New("checkpoint 表不存在，请先执行 init")
		}
		return errors.Wrap(err, "读取 checkpoint 表定义失败")
	}
	if !engine.Valid || !strings.EqualFold(engine.String, "InnoDB") || !collation.Valid || !strings.EqualFold(collation.String, "utf8mb4_0900_ai_ci") || tableComment != "shard_no回填进度" || strings.TrimSpace(createOptions) != "" {
		return errors.Errorf("checkpoint 表属性错误 engine=%s collation=%s comment=%q options=%q", engine.String, collation.String, tableComment, createOptions)
	}

	columnQuery, err := sqlAsset("checkpoint-columns.sql.tmpl")
	if err != nil {
		return errors.Tag(err)
	}
	rows, err := db.QueryContext(ctx, columnQuery)
	if err != nil {
		return errors.Wrap(err, "读取 checkpoint 列元数据失败")
	}
	columns := make([]checkpointColumn, 0, 16)
	for rows.Next() {
		var item checkpointColumn
		if err = rows.Scan(&item.Ordinal, &item.Name, &item.ColumnType, &item.Nullable, &item.Default, &item.Extra, &item.CharacterSet, &item.Collation, &item.DatetimePrecision, &item.Comment); err != nil {
			rows.Close()
			return errors.Wrap(err, "读取 checkpoint 列定义失败")
		}
		columns = append(columns, item)
	}
	if err = rows.Close(); err != nil {
		return errors.Wrap(err, "关闭 checkpoint 列元数据结果失败")
	}
	if err = rows.Err(); err != nil {
		return errors.Wrap(err, "遍历 checkpoint 列元数据失败")
	}
	if err = validateCheckpointColumns(columns); err != nil {
		return err
	}

	indexQuery, err := sqlAsset("checkpoint-indexes.sql.tmpl")
	if err != nil {
		return errors.Tag(err)
	}
	indexRows, err := db.QueryContext(ctx, indexQuery)
	if err != nil {
		return errors.Wrap(err, "读取 checkpoint 索引元数据失败")
	}
	indexes := make([]checkpointIndex, 0, 3)
	for indexRows.Next() {
		var item checkpointIndex
		if err = indexRows.Scan(&item.Name, &item.NonUnique, &item.Sequence, &item.Column, &item.Prefix, &item.Type, &item.Visible, &item.Expression); err != nil {
			indexRows.Close()
			return errors.Wrap(err, "读取 checkpoint 索引定义失败")
		}
		indexes = append(indexes, item)
	}
	if err = indexRows.Close(); err != nil {
		return errors.Wrap(err, "关闭 checkpoint 索引元数据结果失败")
	}
	if err = indexRows.Err(); err != nil {
		return errors.Wrap(err, "遍历 checkpoint 索引元数据失败")
	}
	if err = validateCheckpointIndexes(indexes); err != nil {
		return err
	}

	checkQuery, err := sqlAsset("checkpoint-checks.sql.tmpl")
	if err != nil {
		return errors.Tag(err)
	}
	checkRows, err := db.QueryContext(ctx, checkQuery)
	if err != nil {
		return errors.Wrap(err, "读取 checkpoint CHECK 元数据失败")
	}
	checks := make([]checkpointCheck, 0, 4)
	for checkRows.Next() {
		var item checkpointCheck
		if err = checkRows.Scan(&item.Name, &item.Clause, &item.Enforced); err != nil {
			checkRows.Close()
			return errors.Wrap(err, "读取 checkpoint CHECK 定义失败")
		}
		checks = append(checks, item)
	}
	if err = checkRows.Close(); err != nil {
		return errors.Wrap(err, "关闭 checkpoint CHECK 元数据结果失败")
	}
	if err = checkRows.Err(); err != nil {
		return errors.Wrap(err, "遍历 checkpoint CHECK 元数据失败")
	}
	if err = validateCheckpointChecks(checks); err != nil {
		return err
	}
	constraintQuery, err := sqlAsset("checkpoint-constraint-count.sql.tmpl")
	if err != nil {
		return errors.Tag(err)
	}
	var constraintCount int
	if err = db.QueryRowContext(ctx, constraintQuery).Scan(&constraintCount); err != nil {
		return errors.Wrap(err, "读取 checkpoint 约束数量失败")
	}
	if constraintCount != 5 {
		return errors.Errorf("checkpoint 表约束数量错误 actual=%d expected=5", constraintCount)
	}
	return nil
}

// validateCheckpointColumns 精确校验列顺序、长度、默认值、精度、字符集和注释。
func validateCheckpointColumns(actual []checkpointColumn) error {
	expected := expectedCheckpointColumns()
	if len(actual) != len(expected) {
		return errors.Errorf("checkpoint 表列数量错误 actual=%d expected=%d", len(actual), len(expected))
	}
	for index := range expected {
		item := normalizeCheckpointColumn(actual[index])
		if item != expected[index] {
			return errors.Errorf("checkpoint 表列定义错误 position=%d column=%s", index+1, item.Name)
		}
	}
	return nil
}

// expectedCheckpointColumns 返回当前命令唯一接受的 checkpoint 列结构。
func expectedCheckpointColumns() []checkpointColumn {
	return []checkpointColumn{
		checkpointTextColumn(1, "job_name", 64, "回填任务唯一名称"),
		checkpointTextColumn(2, "table_name", 64, "业务表名"),
		checkpointTextColumn(3, "primary_key", 64, "单调唯一游标列"),
		checkpointTextColumn(4, "uid_column", 64, "业务用户ID列"),
		checkpointTextColumn(5, "insert_trigger", 64, "源表插入保护触发器"),
		checkpointTextColumn(6, "update_trigger", 64, "源表更新保护触发器"),
		checkpointUintColumn(7, "range_start", sql.NullString{}, "回填起点，不包含"),
		checkpointUintColumn(8, "range_end", sql.NullString{}, "回填终点，包含"),
		checkpointUintColumn(9, "cursor_value", sql.NullString{}, "已提交回填游标"),
		checkpointUintColumn(10, "verify_cursor", sql.NullString{}, "已提交校验游标"),
		checkpointUintColumn(11, "updated_rows", sql.NullString{String: "0", Valid: true}, "累计修正行数"),
		checkpointUintColumn(12, "verified_rows", sql.NullString{String: "0", Valid: true}, "累计校验行数"),
		checkpointUintColumn(13, "mismatch_rows", sql.NullString{String: "0", Valid: true}, "校验不一致行数"),
		checkpointTextColumn(14, "status", 16, "running/backfilled/verifying/verified/mismatch"),
		checkpointTimeColumn(15, "created_at", "default_generated", "创建时间"),
		checkpointTimeColumn(16, "updated_at", "default_generated on update current_timestamp(3)", "更新时间"),
	}
}

// checkpointTextColumn 构造指定长度的非空文本列契约。
func checkpointTextColumn(ordinal int, name string, size int, comment string) checkpointColumn {
	return checkpointColumn{
		Ordinal: ordinal, Name: name, ColumnType: fmt.Sprintf("varchar(%d)", size), Nullable: "NO",
		CharacterSet: sql.NullString{String: "utf8mb4", Valid: true},
		Collation:    sql.NullString{String: "utf8mb4_0900_ai_ci", Valid: true},
		Comment:      comment,
	}
}

// checkpointUintColumn 构造非空 bigint unsigned checkpoint 列契约。
func checkpointUintColumn(ordinal int, name string, defaultValue sql.NullString, comment string) checkpointColumn {
	return checkpointColumn{Ordinal: ordinal, Name: name, ColumnType: "bigint unsigned", Nullable: "NO", Default: defaultValue, Comment: comment}
}

// checkpointTimeColumn 构造 datetime(3) 自动时间列契约。
func checkpointTimeColumn(ordinal int, name string, extra string, comment string) checkpointColumn {
	return checkpointColumn{
		Ordinal: ordinal, Name: name, ColumnType: "datetime(3)", Nullable: "NO",
		Default: sql.NullString{String: "current_timestamp(3)", Valid: true}, Extra: extra,
		DatetimePrecision: sql.NullInt64{Int64: 3, Valid: true}, Comment: comment,
	}
}

// normalizeCheckpointColumn 消除 information_schema 仅有的大小写和连续空白差异。
func normalizeCheckpointColumn(item checkpointColumn) checkpointColumn {
	item.ColumnType = strings.ToLower(strings.TrimSpace(item.ColumnType))
	item.Nullable = strings.ToUpper(strings.TrimSpace(item.Nullable))
	item.Extra = strings.ToLower(strings.Join(strings.Fields(item.Extra), " "))
	if item.Default.Valid {
		item.Default.String = strings.ToLower(strings.TrimSpace(item.Default.String))
	}
	if item.CharacterSet.Valid {
		item.CharacterSet.String = strings.ToLower(strings.TrimSpace(item.CharacterSet.String))
	}
	if item.Collation.Valid {
		item.Collation.String = strings.ToLower(strings.TrimSpace(item.Collation.String))
	}
	return item
}

// validateCheckpointIndexes 精确校验主键与状态查询索引，拒绝缺失、改序或额外索引。
func validateCheckpointIndexes(actual []checkpointIndex) error {
	expected := []checkpointIndex{
		{Name: "PRIMARY", NonUnique: 0, Sequence: 1, Column: "job_name", Type: "BTREE", Visible: "YES"},
		{Name: "idx_shard_backfill_table_status", NonUnique: 1, Sequence: 1, Column: "table_name", Type: "BTREE", Visible: "YES"},
		{Name: "idx_shard_backfill_table_status", NonUnique: 1, Sequence: 2, Column: "status", Type: "BTREE", Visible: "YES"},
	}
	if len(actual) != len(expected) {
		return errors.Errorf("checkpoint 表索引数量错误 actual=%d expected=%d", len(actual), len(expected))
	}
	for index := range expected {
		actual[index].Type = strings.ToUpper(strings.TrimSpace(actual[index].Type))
		actual[index].Visible = strings.ToUpper(strings.TrimSpace(actual[index].Visible))
		if actual[index] != expected[index] {
			return errors.Errorf("checkpoint 表索引定义错误 index=%s sequence=%d", actual[index].Name, actual[index].Sequence)
		}
	}
	return nil
}

// validateCheckpointChecks 精确校验全部强制 CHECK 约束，拒绝缺失、弱化或额外约束。
func validateCheckpointChecks(actual []checkpointCheck) error {
	expected := []checkpointCheck{
		{Name: "chk_shard_backfill_cursor", Clause: "cursor_value BETWEEN range_start AND range_end", Enforced: "YES"},
		{Name: "chk_shard_backfill_range", Clause: "range_start < range_end", Enforced: "YES"},
		{Name: "chk_shard_backfill_status", Clause: "status IN ('running','backfilled','verifying','verified','mismatch')", Enforced: "YES"},
		{Name: "chk_shard_verify_cursor", Clause: "verify_cursor BETWEEN range_start AND range_end", Enforced: "YES"},
	}
	if len(actual) != len(expected) {
		return errors.Errorf("checkpoint 表 CHECK 数量错误 actual=%d expected=%d", len(actual), len(expected))
	}
	for index := range expected {
		if actual[index].Name != expected[index].Name || normalizeCheckClause(actual[index].Clause) != normalizeCheckClause(expected[index].Clause) || !strings.EqualFold(actual[index].Enforced, expected[index].Enforced) {
			return errors.Errorf("checkpoint 表 CHECK 定义错误 constraint=%s", actual[index].Name)
		}
	}
	return nil
}

// normalizeCheckClause 去除 MySQL 元数据输出的转义引号、字符集引导符、标识符引号、外层括号和空白差异。
func normalizeCheckClause(value string) string {
	value = strings.ToLower(value)
	// CHECK_CLAUSE 会把字符串边界返回为 \'，先还原引号再识别 _utf8mb4/_latin1 等连接字符集引导符。
	value = strings.ReplaceAll(value, `\'`, `'`)
	value = stripCheckCharsetIntroducers(value)
	value = strings.TrimSpace(value)
	for hasOuterCheckParentheses(value) {
		value = strings.TrimSpace(value[1 : len(value)-1])
	}
	for _, old := range []string{"`", " ", "\t", "\r", "\n"} {
		value = strings.ReplaceAll(value, old, "")
	}
	return value
}

// hasOuterCheckParentheses 判断首尾括号是否完整包裹表达式，避免删除函数调用等具有语义的内部括号。
func hasOuterCheckParentheses(value string) bool {
	if len(value) < 2 || value[0] != '(' || value[len(value)-1] != ')' {
		return false
	}
	depth := 0
	inString := false
	for index := 0; index < len(value); index++ {
		switch value[index] {
		case '\'':
			if inString && index+1 < len(value) && value[index+1] == '\'' {
				index++
				continue
			}
			inString = !inString
		case '(':
			if !inString {
				depth++
			}
		case ')':
			if !inString {
				depth--
				if depth == 0 && index != len(value)-1 {
					return false
				}
			}
		}
	}
	return depth == 0 && !inString
}

// stripCheckCharsetIntroducers 删除紧邻字符串字面量的 MySQL `_charset` 引导符，不改写标识符中的下划线。
func stripCheckCharsetIntroducers(value string) string {
	var normalized strings.Builder
	normalized.Grow(len(value))
	for index := 0; index < len(value); {
		if value[index] == '_' {
			quoteIndex := index + 1
			for quoteIndex < len(value) && isCheckCharsetNameByte(value[quoteIndex]) {
				quoteIndex++
			}
			if quoteIndex > index+1 && quoteIndex < len(value) && value[quoteIndex] == '\'' {
				index = quoteIndex
				continue
			}
		}
		normalized.WriteByte(value[index])
		index++
	}
	return normalized.String()
}

// isCheckCharsetNameByte 限定 MySQL 字符集名称允许的 ASCII 字母、数字和下划线。
func isCheckCharsetNameByte(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= '0' && value <= '9' || value == '_'
}

// runBatches 在一条专用连接上持有表级命名锁，直到当前阶段结束或进程退出。
func runBatches(ctx context.Context, db *sql.DB, opts options, output io.Writer) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return errors.Wrap(err, "获取源库专用连接失败")
	}
	defer conn.Close()
	lockName, err := acquireTableLock(ctx, conn, opts.Table)
	if err != nil {
		return err
	}
	defer releaseTableLock(conn, lockName)
	if err = validateTableSchema(ctx, conn, opts); err != nil {
		return err
	}
	if err = validateSourceGuards(ctx, conn, opts); err != nil {
		return err
	}
	if err = ensureCheckpoint(ctx, conn, opts); err != nil {
		return err
	}
	if opts.Restart {
		if err = restartStage(ctx, conn, opts); err != nil {
			return err
		}
	}

	for {
		if err = ctx.Err(); err != nil {
			return errors.Tag(err)
		}
		batchCtx, cancel := context.WithTimeout(ctx, opts.BatchTimeout)
		var result batchResult
		if opts.Action == actionRun {
			result, err = backfillBatch(batchCtx, conn, opts)
		} else {
			result, err = verifyBatch(batchCtx, conn, opts)
		}
		cancel()
		if err != nil {
			return err
		}
		if err = printBatch(output, opts.Action, result); err != nil {
			return err
		}
		if result.Done {
			if opts.Action == actionVerify && result.Checkpoint.Status == statusMismatch {
				return errors.Errorf("全量校验发现 %d 行 shard_no 不符合固定桶公式", result.Checkpoint.MismatchRows)
			}
			return nil
		}
		if opts.Pause > 0 {
			timer := time.NewTimer(opts.Pause)
			select {
			case <-ctx.Done():
				timer.Stop()
				return errors.Tag(ctx.Err())
			case <-timer.C:
			}
		}
	}
}

// validateTableSchema 确保游标扫描和 checkpoint 事务具备可证明的数据边界。
func validateTableSchema(ctx context.Context, conn *sql.Conn, opts options) error {
	engineQuery, err := sqlAsset("table-engine.sql.tmpl")
	if err != nil {
		return errors.Tag(err)
	}
	var engine sql.NullString
	if err = conn.QueryRowContext(ctx, engineQuery, opts.Table).Scan(&engine); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.Errorf("业务表 %s 不存在", opts.Table)
		}
		return errors.Wrap(err, "读取业务表引擎失败")
	}
	if !engine.Valid || !strings.EqualFold(engine.String, "InnoDB") {
		return errors.Errorf("业务表 %s 必须使用 InnoDB，当前引擎=%s", opts.Table, engine.String)
	}

	columnQuery, err := sqlAsset("table-columns.sql.tmpl")
	if err != nil {
		return errors.Tag(err)
	}
	rows, err := conn.QueryContext(ctx, columnQuery, opts.Table, opts.PrimaryKey, opts.UIDColumn, "shard_no")
	if err != nil {
		return errors.Wrap(err, "读取业务列元数据失败")
	}
	columns := make(map[string]columnInfo, 3)
	for rows.Next() {
		var item columnInfo
		if err = rows.Scan(&item.Name, &item.DataType, &item.ColumnType, &item.Nullable); err != nil {
			rows.Close()
			return errors.Wrap(err, "读取业务列定义失败")
		}
		columns[item.Name] = item
	}
	if err = rows.Close(); err != nil {
		return errors.Wrap(err, "关闭业务列元数据结果失败")
	}
	if err = rows.Err(); err != nil {
		return errors.Wrap(err, "遍历业务列元数据失败")
	}
	for _, name := range []string{opts.PrimaryKey, opts.UIDColumn, "shard_no"} {
		item, exists := columns[name]
		if !exists {
			return errors.Errorf("业务表 %s 缺少列 %s", opts.Table, name)
		}
		if !integerDataType(item.DataType) {
			return errors.Errorf("业务表 %s 的列 %s 必须是整数类型，当前=%s", opts.Table, name, item.ColumnType)
		}
	}
	if !strings.EqualFold(columns[opts.PrimaryKey].Nullable, "NO") || !strings.EqualFold(columns[opts.UIDColumn].Nullable, "NO") || !strings.EqualFold(columns["shard_no"].Nullable, "NO") {
		return errors.Errorf("业务表 %s 的主键列、UID 列和 shard_no 必须全部 NOT NULL", opts.Table)
	}
	if !shardNoDataType(columns["shard_no"].DataType) {
		return errors.Errorf("业务表 %s 的 shard_no 必须至少是 smallint，当前=%s", opts.Table, columns["shard_no"].ColumnType)
	}

	uniqueQuery, err := sqlAsset("unique-cursor-index.sql.tmpl")
	if err != nil {
		return errors.Tag(err)
	}
	var uniqueIndexCount int
	if err = conn.QueryRowContext(ctx, uniqueQuery, opts.Table, opts.PrimaryKey).Scan(&uniqueIndexCount); err != nil {
		return errors.Wrap(err, "校验主键唯一索引失败")
	}
	if uniqueIndexCount < 1 {
		return errors.Errorf("业务表 %s 的游标列 %s 必须有单列唯一索引", opts.Table, opts.PrimaryKey)
	}
	positiveQuery, err := renderSQL("positive-primary-key.sql.tmpl", map[string]string{
		"__TABLE__":       opts.Table,
		"__PRIMARY_KEY__": opts.PrimaryKey,
	})
	if err != nil {
		return errors.Tag(err)
	}
	var hasNonPositiveKey bool
	if err = conn.QueryRowContext(ctx, positiveQuery).Scan(&hasNonPositiveKey); err != nil {
		return errors.Wrap(err, "校验主键正整数边界失败")
	}
	if hasNonPositiveKey {
		return errors.Errorf("业务表 %s 的游标列 %s 必须全部为正整数", opts.Table, opts.PrimaryKey)
	}
	return nil
}

// shardNoDataType 确认固定逻辑桶列能完整容纳 0..1023。
func shardNoDataType(value string) bool {
	switch strings.ToLower(value) {
	case "smallint", "mediumint", "int", "bigint":
		return true
	default:
		return false
	}
}

// queryRower 是连接和事务读取单行元数据的共同能力。
type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// validateSourceGuards 确认源表插入和更新保护触发器仍按固定桶契约生效。
func validateSourceGuards(ctx context.Context, queryer queryRower, opts options) error {
	if err := validateSourceGuard(ctx, queryer, opts, opts.InsertTrigger, "AFTER", "INSERT"); err != nil {
		return errors.Tag(err)
	}
	if err := validateSourceGuard(ctx, queryer, opts, opts.UpdateTrigger, "BEFORE", "UPDATE"); err != nil {
		return errors.Tag(err)
	}
	return nil
}

// validateSourceGuard 校验单个触发器的时机、事件和关键拒绝语义。
func validateSourceGuard(ctx context.Context, queryer queryRower, opts options, trigger string, timing string, event string) error {
	query, err := sqlAsset("trigger-definition.sql.tmpl")
	if err != nil {
		return errors.Tag(err)
	}
	var actualTiming string
	var actualEvent string
	var statement string
	err = queryer.QueryRowContext(ctx, query, opts.Table, trigger).Scan(&actualTiming, &actualEvent, &statement)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.Errorf("源表保护触发器不存在 table=%s trigger=%s", opts.Table, trigger)
		}
		return errors.Wrapf(err, "读取源表保护触发器失败 table=%s trigger=%s", opts.Table, trigger)
	}
	if !strings.EqualFold(actualTiming, timing) || !strings.EqualFold(actualEvent, event) {
		return errors.Errorf("源表保护触发器时机错误 table=%s trigger=%s actual=%s/%s", opts.Table, trigger, actualTiming, actualEvent)
	}
	if !validGuardStatement(statement, opts, event) {
		return errors.Errorf("源表保护触发器语义不符合固定桶契约 table=%s trigger=%s", opts.Table, trigger)
	}
	return nil
}

// validGuardStatement 校验触发器拒绝错误桶、UID 变更和游标主键变更的关键语义。
func validGuardStatement(statement string, opts options, event string) bool {
	asset := "expected-insert-guard.sql.tmpl"
	identifiers := map[string]string{"__PRIMARY_KEY__": opts.PrimaryKey, "__UID_COLUMN__": opts.UIDColumn}
	if strings.EqualFold(event, "INSERT") {
		expected, err := renderSQL(asset, identifiers)
		return err == nil && normalizeGuardStatement(statement) == normalizeGuardStatement(expected)
	}
	asset = "expected-update-guard.sql.tmpl"
	identifiers["__PRIMARY_KEY__"] = opts.PrimaryKey
	expected, err := renderSQL(asset, identifiers)
	return err == nil && normalizeGuardStatement(statement) == normalizeGuardStatement(expected)
}

// normalizeGuardStatement 去除 MySQL 元数据格式差异，保留运算符和字面量语义。
func normalizeGuardStatement(statement string) string {
	return strings.Map(func(value rune) rune {
		if value == '`' || value == '\ufeff' || value == ' ' || value == '\t' || value == '\r' || value == '\n' {
			return -1
		}
		if value >= 'A' && value <= 'Z' {
			return value + ('a' - 'A')
		}
		return value
	}, statement)
}

// integerDataType 判断列是否为 MySQL 整数类型。
func integerDataType(value string) bool {
	switch strings.ToLower(value) {
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		return true
	default:
		return false
	}
}

// acquireTableLock 用数据库名和表名生成固定长度锁，禁止同表并发回填或校验。
func acquireTableLock(ctx context.Context, conn *sql.Conn, table string) (string, error) {
	databaseQuery, err := sqlAsset("database-name.sql.tmpl")
	if err != nil {
		return "", errors.Tag(err)
	}
	var databaseName string
	if err = conn.QueryRowContext(ctx, databaseQuery).Scan(&databaseName); err != nil {
		return "", errors.Wrap(err, "读取源数据库名失败")
	}
	digest := sha256.Sum256([]byte(databaseName + "\x00" + table))
	lockName := fmt.Sprintf("shardbackfill:%x", digest[:16])
	lockQuery, err := sqlAsset("acquire-lock.sql.tmpl")
	if err != nil {
		return "", errors.Tag(err)
	}
	var acquired sql.NullInt64
	if err = conn.QueryRowContext(ctx, lockQuery, lockName).Scan(&acquired); err != nil {
		return "", errors.Wrap(err, "获取表级回填锁失败")
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		return "", errors.Errorf("表 %s 已有回填或校验任务运行", table)
	}
	return lockName, nil
}

// releaseTableLock 尽力释放命名锁；连接关闭仍是最终释放边界。
func releaseTableLock(conn *sql.Conn, lockName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	query, err := sqlAsset("release-lock.sql.tmpl")
	if err != nil {
		return
	}
	var released sql.NullInt64
	_ = conn.QueryRowContext(ctx, query, lockName).Scan(&released)
}

// ensureCheckpoint 幂等创建任务行，已存在任务由后续事务校验不可变参数。
func ensureCheckpoint(ctx context.Context, conn *sql.Conn, opts options) error {
	query, err := sqlAsset("insert-checkpoint.sql.tmpl")
	if err != nil {
		return errors.Tag(err)
	}
	if _, err = conn.ExecContext(ctx, query, opts.Job, opts.Table, opts.PrimaryKey, opts.UIDColumn, opts.InsertTrigger, opts.UpdateTrigger, opts.RangeStart, opts.RangeEnd, opts.RangeStart, opts.RangeStart, statusRunning); err != nil {
		return errors.Wrap(err, "创建回填 checkpoint 失败")
	}
	return nil
}

// restartStage 在显式授权下重置当前阶段，重复扫描仍保持幂等。
func restartStage(ctx context.Context, conn *sql.Conn, opts options) error {
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return errors.Wrap(err, "开始重置事务失败")
	}
	defer tx.Rollback()
	current, err := loadCheckpoint(ctx, tx, opts.Job, true)
	if err != nil {
		return err
	}
	if err = validateCheckpoint(current, opts); err != nil {
		return err
	}
	var result sql.Result
	if opts.Action == actionRun {
		query, queryErr := sqlAsset("restart-backfill.sql.tmpl")
		if queryErr != nil {
			return errors.Tag(queryErr)
		}
		result, err = tx.ExecContext(ctx, query, statusRunning, opts.Job)
	} else {
		if current.Status == statusRunning {
			return errors.Errorf("任务 %s 尚未完成回填，不能开始校验", opts.Job)
		}
		query, queryErr := sqlAsset("restart-verify.sql.tmpl")
		if queryErr != nil {
			return errors.Tag(queryErr)
		}
		result, err = tx.ExecContext(ctx, query, statusBackfilled, opts.Job)
	}
	if err != nil {
		return errors.Wrap(err, "重置任务阶段失败")
	}
	if err = requireExistingRow(result, "重置任务阶段"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return errors.Wrap(err, "提交重置事务失败")
	}
	return nil
}

// backfillBatch 扫描固定数量主键、修正同一范围并原子推进 checkpoint。
func backfillBatch(ctx context.Context, conn *sql.Conn, opts options) (batchResult, error) {
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return batchResult{}, errors.Wrap(err, "开始回填事务失败")
	}
	defer tx.Rollback()
	current, err := loadCheckpoint(ctx, tx, opts.Job, true)
	if err != nil {
		return batchResult{}, err
	}
	if err = validateCheckpoint(current, opts); err != nil {
		return batchResult{}, err
	}
	if current.Status != statusRunning {
		if current.Status == statusBackfilled || current.Status == statusVerifying || current.Status == statusVerified {
			return batchResult{Checkpoint: current, Done: true}, nil
		}
		return batchResult{}, errors.Errorf("任务状态为 %s；修复后使用 -restart 重新回填", current.Status)
	}
	keys, err := selectBackfillPage(ctx, tx, opts, current.Cursor)
	if err != nil {
		return batchResult{}, err
	}
	if len(keys) == 0 {
		if err = validateSourceGuards(ctx, tx, opts); err != nil {
			return batchResult{}, err
		}
		current.Cursor = current.RangeEnd
		current.Status = statusBackfilled
		if err = updateBackfillProgress(ctx, tx, current, 0); err != nil {
			return batchResult{}, err
		}
		if err = tx.Commit(); err != nil {
			return batchResult{}, errors.Wrap(err, "提交回填完成状态失败")
		}
		return batchResult{Checkpoint: current, Done: true}, nil
	}
	lastKey := keys[len(keys)-1]
	changed, err := updateShardNo(ctx, tx, opts, current.Cursor, lastKey)
	if err != nil {
		return batchResult{}, err
	}
	if changed > uint64(len(keys)) {
		return batchResult{}, errors.Errorf("单批修正 %d 行超过已锁定的 %d 行，请检查主键单调性和并发写入", changed, len(keys))
	}
	current.Cursor = lastKey
	current.UpdatedRows += changed
	if err = updateBackfillProgress(ctx, tx, current, changed); err != nil {
		return batchResult{}, err
	}
	if err = tx.Commit(); err != nil {
		return batchResult{}, errors.Wrap(err, "提交回填批次失败")
	}
	return batchResult{Checkpoint: current, Rows: len(keys), Changed: changed}, nil
}

// selectBackfillPage 按唯一游标读取并锁定一页数据，在写入前拒绝非法 UID。
func selectBackfillPage(ctx context.Context, tx *sql.Tx, opts options, cursor uint64) ([]uint64, error) {
	query, err := renderSQL("select-backfill-page.sql.tmpl", map[string]string{
		"__TABLE__":       opts.Table,
		"__PRIMARY_KEY__": opts.PrimaryKey,
		"__UID_COLUMN__":  opts.UIDColumn,
	})
	if err != nil {
		return nil, errors.Tag(err)
	}
	rows, err := tx.QueryContext(ctx, query, cursor, opts.RangeEnd, opts.BatchSize)
	if err != nil {
		return nil, errors.Wrap(err, "读取回填批次失败")
	}
	defer rows.Close()
	keys := make([]uint64, 0, opts.BatchSize)
	previous := cursor
	for rows.Next() {
		var keyText string
		var uidText string
		if err = rows.Scan(&keyText, &uidText); err != nil {
			return nil, errors.Wrap(err, "读取回填行失败")
		}
		value, parseErr := strconv.ParseUint(keyText, 10, 64)
		if parseErr != nil || value <= previous || value > opts.RangeEnd {
			return nil, errors.Errorf("主键列 %s 返回非法或非递增值 %q", opts.PrimaryKey, keyText)
		}
		uid, parseErr := strconv.ParseUint(uidText, 10, 64)
		if parseErr != nil || uid == 0 {
			return nil, errors.Errorf("UID列 %s 返回非法正整数 %q", opts.UIDColumn, uidText)
		}
		keys = append(keys, value)
		previous = value
	}
	if err = rows.Err(); err != nil {
		return nil, errors.Wrap(err, "遍历回填批次失败")
	}
	return keys, nil
}

// updateShardNo 使用数据库 CRC32 公式修正已锁定主键范围。
func updateShardNo(ctx context.Context, tx *sql.Tx, opts options, cursor uint64, end uint64) (uint64, error) {
	query, err := renderSQL("update-shard-no.sql.tmpl", map[string]string{
		"__TABLE__":       opts.Table,
		"__PRIMARY_KEY__": opts.PrimaryKey,
		"__UID_COLUMN__":  opts.UIDColumn,
	})
	if err != nil {
		return 0, errors.Tag(err)
	}
	result, err := tx.ExecContext(ctx, query, cursor, end)
	if err != nil {
		return 0, errors.Wrap(err, "修正 shard_no 失败")
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, errors.Wrap(err, "读取 shard_no 修正行数失败")
	}
	if rows < 0 {
		return 0, errors.Errorf("数据库返回非法修正行数 %d", rows)
	}
	return uint64(rows), nil
}

// updateBackfillProgress 在业务更新事务内推进已提交游标。
func updateBackfillProgress(ctx context.Context, tx *sql.Tx, current checkpoint, changed uint64) error {
	query, err := sqlAsset("update-backfill-progress.sql.tmpl")
	if err != nil {
		return errors.Tag(err)
	}
	result, err := tx.ExecContext(ctx, query, current.Cursor, current.UpdatedRows, current.Status, current.Job)
	if err != nil {
		return errors.Wrap(err, "推进回填 checkpoint 失败")
	}
	return requireOneRow(result, fmt.Sprintf("推进回填 checkpoint，batch_changed=%d", changed))
}

// verifyBatch 分页扫描真实 UID 与 shard_no，并使用相同 CRC32 算法全量复核。
func verifyBatch(ctx context.Context, conn *sql.Conn, opts options) (batchResult, error) {
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return batchResult{}, errors.Wrap(err, "开始校验事务失败")
	}
	defer tx.Rollback()
	current, err := loadCheckpoint(ctx, tx, opts.Job, true)
	if err != nil {
		return batchResult{}, err
	}
	if err = validateCheckpoint(current, opts); err != nil {
		return batchResult{}, err
	}
	if current.Status == statusVerified {
		return batchResult{Checkpoint: current, Done: true}, nil
	}
	if current.Status == statusMismatch {
		return batchResult{}, errors.Errorf("任务已有 %d 行不一致；修复后使用 verify -restart 全量复核", current.MismatchRows)
	}
	if current.Status != statusBackfilled && current.Status != statusVerifying {
		return batchResult{}, errors.Errorf("任务状态为 %s，必须先完成回填", current.Status)
	}
	rows, lastKey, mismatch, err := selectVerifyPage(ctx, tx, opts, current.VerifyCursor)
	if err != nil {
		return batchResult{}, err
	}
	if rows == 0 {
		if err = validateSourceGuards(ctx, tx, opts); err != nil {
			return batchResult{}, err
		}
		current.VerifyCursor = current.RangeEnd
		if current.MismatchRows == 0 {
			current.Status = statusVerified
		} else {
			current.Status = statusMismatch
		}
		if err = updateVerifyProgress(ctx, tx, current); err != nil {
			return batchResult{}, err
		}
		if err = tx.Commit(); err != nil {
			return batchResult{}, errors.Wrap(err, "提交校验完成状态失败")
		}
		return batchResult{Checkpoint: current, Done: true}, nil
	}
	current.VerifyCursor = lastKey
	current.VerifiedRows += uint64(rows)
	current.MismatchRows += mismatch
	current.Status = statusVerifying
	if err = updateVerifyProgress(ctx, tx, current); err != nil {
		return batchResult{}, err
	}
	if err = tx.Commit(); err != nil {
		return batchResult{}, errors.Wrap(err, "提交校验批次失败")
	}
	return batchResult{Checkpoint: current, Rows: rows, Changed: mismatch}, nil
}

// selectVerifyPage 读取一页业务行并返回公式不一致数量。
func selectVerifyPage(ctx context.Context, tx *sql.Tx, opts options, cursor uint64) (int, uint64, uint64, error) {
	query, err := renderSQL("select-verify-page.sql.tmpl", map[string]string{
		"__TABLE__":       opts.Table,
		"__PRIMARY_KEY__": opts.PrimaryKey,
		"__UID_COLUMN__":  opts.UIDColumn,
	})
	if err != nil {
		return 0, 0, 0, errors.Tag(err)
	}
	rows, err := tx.QueryContext(ctx, query, cursor, opts.RangeEnd, opts.BatchSize)
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "读取校验批次失败")
	}
	defer rows.Close()
	count := 0
	lastKey := cursor
	var mismatch uint64
	for rows.Next() {
		var keyText string
		var uidText string
		var shardNo sql.NullInt64
		if err = rows.Scan(&keyText, &uidText, &shardNo); err != nil {
			return 0, 0, 0, errors.Wrap(err, "读取校验行失败")
		}
		key, parseErr := strconv.ParseUint(keyText, 10, 64)
		if parseErr != nil || key <= lastKey || key > opts.RangeEnd {
			return 0, 0, 0, errors.Errorf("主键列 %s 返回非法或非递增值 %q", opts.PrimaryKey, keyText)
		}
		uid, parseErr := strconv.ParseUint(uidText, 10, 64)
		if parseErr != nil || uid == 0 {
			return 0, 0, 0, errors.Errorf("UID列 %s 返回非法正整数 %q", opts.UIDColumn, uidText)
		}
		expected := int64(crc32.ChecksumIEEE([]byte(uidText)) % 1024)
		if !shardNo.Valid || shardNo.Int64 != expected {
			mismatch++
		}
		lastKey = key
		count++
	}
	if err = rows.Err(); err != nil {
		return 0, 0, 0, errors.Wrap(err, "遍历校验行失败")
	}
	return count, lastKey, mismatch, nil
}

// updateVerifyProgress 在同一事务内推进全量校验游标和累计结果。
func updateVerifyProgress(ctx context.Context, tx *sql.Tx, current checkpoint) error {
	query, err := sqlAsset("update-verify-progress.sql.tmpl")
	if err != nil {
		return errors.Tag(err)
	}
	result, err := tx.ExecContext(ctx, query, current.VerifyCursor, current.VerifiedRows, current.MismatchRows, current.Status, current.Job)
	if err != nil {
		return errors.Wrap(err, "推进校验 checkpoint 失败")
	}
	return requireOneRow(result, "推进校验 checkpoint")
}

// loadCheckpoint 读取任务进度；执行批次时用 FOR UPDATE 保证游标单写。
func loadCheckpoint(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, job string, forUpdate bool) (checkpoint, error) {
	query, err := sqlAsset("select-checkpoint.sql.tmpl")
	if err != nil {
		return checkpoint{}, errors.Tag(err)
	}
	if forUpdate {
		query += " FOR UPDATE"
	}
	var current checkpoint
	err = queryer.QueryRowContext(ctx, query, job).Scan(
		&current.Job,
		&current.Table,
		&current.PrimaryKey,
		&current.UIDColumn,
		&current.InsertTrigger,
		&current.UpdateTrigger,
		&current.RangeStart,
		&current.RangeEnd,
		&current.Cursor,
		&current.VerifyCursor,
		&current.UpdatedRows,
		&current.VerifiedRows,
		&current.MismatchRows,
		&current.Status,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return checkpoint{}, errors.Errorf("任务 %s 不存在，请先执行 run 创建", job)
		}
		return checkpoint{}, errors.Wrap(err, "读取回填 checkpoint 失败")
	}
	return current, nil
}

// validateCheckpoint 防止同名任务在恢复时被换表、换列或换范围。
func validateCheckpoint(current checkpoint, opts options) error {
	if current.Table != opts.Table || current.PrimaryKey != opts.PrimaryKey || current.UIDColumn != opts.UIDColumn || current.InsertTrigger != opts.InsertTrigger || current.UpdateTrigger != opts.UpdateTrigger || current.RangeStart != opts.RangeStart || current.RangeEnd != opts.RangeEnd {
		return errors.Errorf("任务 %s 参数与 checkpoint 不一致，禁止复用任务名", opts.Job)
	}
	return nil
}

// printStatus 输出单行稳定字段，便于发布流水线采集。
func printStatus(ctx context.Context, db *sql.DB, job string, output io.Writer) error {
	current, err := loadCheckpoint(ctx, db, job, false)
	if err != nil {
		return err
	}
	return printCheckpoint(output, actionStatus, current, 0, 0)
}

// printBatch 输出已提交批次结果，不输出 DSN 或业务 UID。
func printBatch(output io.Writer, action string, result batchResult) error {
	return printCheckpoint(output, action, result.Checkpoint, result.Rows, result.Changed)
}

// printCheckpoint 输出回填和校验的当前持久化水位。
func printCheckpoint(output io.Writer, action string, current checkpoint, rows int, changed uint64) error {
	if _, err := fmt.Fprintf(output, "job=%s action=%s table=%s status=%s cursor=%d verify_cursor=%d range_end=%d batch_rows=%d batch_changed=%d updated_rows=%d verified_rows=%d mismatch_rows=%d\n", current.Job, action, current.Table, current.Status, current.Cursor, current.VerifyCursor, current.RangeEnd, rows, changed, current.UpdatedRows, current.VerifiedRows, current.MismatchRows); err != nil {
		return errors.Wrap(err, "输出回填任务状态失败")
	}
	return nil
}

// requireOneRow 确保 checkpoint 写入恰好影响当前任务行。
func requireOneRow(result sql.Result, action string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return errors.Wrapf(err, "%s读取影响行数失败", action)
	}
	if rows != 1 {
		return errors.Errorf("%s影响 %d 行，期望 1 行", action, rows)
	}
	return nil
}

// requireExistingRow 允许幂等重置不改变字段值，但禁止异常影响多行。
func requireExistingRow(result sql.Result, action string) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return errors.Wrapf(err, "%s读取影响行数失败", action)
	}
	if rows < 0 || rows > 1 {
		return errors.Errorf("%s影响 %d 行，期望不超过 1 行", action, rows)
	}
	return nil
}

// quoted 为已校验标识符补充反引号。
func quoted(value string) string {
	return "`" + value + "`"
}
