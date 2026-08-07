package migration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"admin/common/embedasset"
	"admin/internal/sharding"

	"github.com/Is999/go-utils/errors"
)

const (
	// migrationLockTimeoutSeconds 是拆表命令获取数据库互斥锁的等待上限。
	migrationLockTimeoutSeconds = 5
)

var (
	// sqlAssets 保存拆表命令使用的受控 SQL 模板。
	//go:embed assets/*.sql.tmpl
	sqlAssets embed.FS
	// createTablePattern 用于归一化 SHOW CREATE TABLE 中的表名。
	createTablePattern = regexp.MustCompile("(?i)^CREATE TABLE `[^`]+`")
	// autoIncrementPattern 用于忽略不影响结构一致性的自增水位。
	autoIncrementPattern = regexp.MustCompile(`(?i) AUTO_INCREMENT=\d+`)
	// checkConstraintPattern 用于忽略 CREATE TABLE LIKE 自动生成的新 CHECK 名称。
	checkConstraintPattern = regexp.MustCompile("(?i)CONSTRAINT `[^`]+` CHECK")
)

// PrepareOptions 定义创建和校验新物理表所需参数。
type PrepareOptions struct {
	FirstTable   string // 起始桶物理表
	UIDColumn    string // 业务用户 ID 字段
	ShardColumn  string // 固定桶字段
	CursorColumn string // 唯一数字游标字段
	FromCount    int    // 当前物理分片数
	ToCount      int    // 目标物理分片数
}

// CleanupOptions 定义切换观察期后的旧数据清理参数。
type CleanupOptions struct {
	PrepareOptions               // 复用目标表、字段和扩容计划参数
	BatchSize      int           // 单批删除行数
	Delay          time.Duration // 批次间隔
}

// OpenDatabase 使用应用写库 DSN 打开拆表运维连接。
func OpenDatabase(dsn string) (*sql.DB, error) {
	database, err := sql.Open("mysql", strings.TrimSpace(dsn))
	if err != nil {
		return nil, errors.Wrap(err, "打开 MySQL 连接失败")
	}
	database.SetMaxOpenConns(4)
	database.SetMaxIdleConns(2)
	database.SetConnMaxLifetime(5 * time.Minute)
	return database, nil
}

// Prepare 创建目标物理表并执行源数据、目标空表和迁移索引门禁。
func Prepare(ctx context.Context, database *sql.DB, opts PrepareOptions) ([]sharding.Move, error) {
	moves, err := validatePrepareOptions(opts)
	if err != nil {
		return nil, err
	}
	release, err := acquireLock(ctx, database, opts)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := validateSources(ctx, database, opts, moves); err != nil {
		return nil, err
	}
	for _, move := range moves {
		query, renderErr := renderSQL("create-target.sql.tmpl", map[string]string{
			"{{SOURCE}}": quoteIdentifier(move.Source),
			"{{TARGET}}": quoteIdentifier(move.Target),
		})
		if renderErr != nil {
			return nil, renderErr
		}
		if _, err := database.ExecContext(ctx, query); err != nil {
			return nil, errors.Wrapf(err, "创建目标物理表失败 target=%s", move.Target)
		}
		if err := validateTarget(ctx, database, move); err != nil {
			return nil, err
		}
	}
	return moves, nil
}

// AcquireLock 在同一条数据库连接上持有整次在线复制的互斥锁。
func AcquireLock(ctx context.Context, database *sql.DB, opts PrepareOptions) (func(), error) {
	if _, err := validatePrepareOptions(opts); err != nil {
		return nil, err
	}
	return acquireLock(ctx, database, opts)
}

// AcquireSourceReadLock 锁住本次迁移的源物理表写入，确保最终 binlog 追平后不再产生旧路由写入。
func AcquireSourceReadLock(ctx context.Context, database *sql.DB, opts PrepareOptions) (func(), error) {
	moves, err := validatePrepareOptions(opts)
	if err != nil {
		return nil, err
	}
	sources := moveSources(moves)
	connection, err := database.Conn(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "获取源表写围栏连接失败 table=%s", opts.FirstTable)
	}
	tableLocks := make([]string, 0, len(sources))
	for _, source := range sources {
		tableLocks = append(tableLocks, quoteIdentifier(source)+" READ")
	}
	query, err := renderSQL("lock-source-tables.sql.tmpl", map[string]string{
		"{{TABLE_LOCKS}}": strings.Join(tableLocks, ", "),
	})
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	unlockQuery, err := assetSQL("unlock-tables.sql.tmpl")
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	if _, err := connection.ExecContext(ctx, query); err != nil {
		_ = connection.Close()
		return nil, errors.Wrapf(err, "锁定源物理表写入失败 tables=%s", strings.Join(sources, ","))
	}
	return func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = connection.ExecContext(releaseCtx, unlockQuery)
		_ = connection.Close()
	}, nil
}

// ValidateCopy 校验在线复制启动前所有目标表仍为空且结构一致。
func ValidateCopy(ctx context.Context, database *sql.DB, opts PrepareOptions) ([]sharding.Move, error) {
	moves, err := validatePrepareOptions(opts)
	if err != nil {
		return nil, err
	}
	if err := validateSources(ctx, database, opts, moves); err != nil {
		return nil, err
	}
	for _, move := range moves {
		if err := validateTarget(ctx, database, move); err != nil {
			return nil, err
		}
	}
	return moves, nil
}

// Cleanup 分批删除扩容后已不再路由到的旧表桶区间。
func Cleanup(ctx context.Context, database *sql.DB, opts CleanupOptions) (int64, error) {
	moves, err := validatePrepareOptions(opts.PrepareOptions)
	if err != nil {
		return 0, err
	}
	if opts.BatchSize <= 0 || opts.BatchSize > 10000 {
		return 0, errors.Errorf("清理批次必须位于 1..10000 batch=%d", opts.BatchSize)
	}
	if opts.Delay < 0 || opts.Delay > time.Minute {
		return 0, errors.Errorf("清理批次间隔必须位于 0..1m delay=%s", opts.Delay)
	}
	release, err := acquireLock(ctx, database, opts.PrepareOptions)
	if err != nil {
		return 0, err
	}
	defer release()
	for _, move := range moves {
		if err := validateCleanupTarget(ctx, database, opts.PrepareOptions, move); err != nil {
			return 0, err
		}
	}
	var deleted int64
	for _, move := range moves {
		query, renderErr := renderSQL("cleanup-range.sql.tmpl", map[string]string{
			"{{SOURCE}}": quoteIdentifier(move.Source),
			"{{CURSOR}}": quoteIdentifier(opts.CursorColumn),
			"{{SHARD}}":  quoteIdentifier(opts.ShardColumn),
		})
		if renderErr != nil {
			return deleted, renderErr
		}
		for {
			result, execErr := database.ExecContext(ctx, query, move.BucketStart, move.BucketEnd, opts.BatchSize)
			if execErr != nil {
				return deleted, errors.Wrapf(execErr, "清理旧物理表失败 source=%s range=%d-%d", move.Source, move.BucketStart, move.BucketEnd)
			}
			rows, rowsErr := result.RowsAffected()
			if rowsErr != nil {
				return deleted, errors.Wrapf(rowsErr, "读取清理行数失败 source=%s", move.Source)
			}
			deleted += rows
			if rows < int64(opts.BatchSize) {
				break
			}
			timer := time.NewTimer(opts.Delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return deleted, errors.Wrap(ctx.Err(), "清理旧物理表已取消")
			case <-timer.C:
			}
		}
	}
	return deleted, nil
}

// validatePrepareOptions 校验标识符和单向扩容计划。
func validatePrepareOptions(opts PrepareOptions) ([]sharding.Move, error) {
	for name, value := range map[string]string{
		"first_table":   opts.FirstTable,
		"uid_column":    opts.UIDColumn,
		"shard_column":  opts.ShardColumn,
		"cursor_column": opts.CursorColumn,
	} {
		if err := sharding.ValidateIdentifier(value); err != nil {
			return nil, errors.Wrapf(err, "%s 无效", name)
		}
	}
	moves, err := sharding.ExpandMoves(opts.FirstTable, opts.FromCount, opts.ToCount)
	if err != nil {
		return nil, err
	}
	return moves, nil
}

// validateSources 校验源表桶边界、字段和迁移扫描索引。
func validateSources(ctx context.Context, database *sql.DB, opts PrepareOptions, moves []sharding.Move) error {
	fromPlan, err := sharding.NewPlan(opts.FirstTable, opts.FromCount)
	if err != nil {
		return err
	}
	tableEngineQuery, err := assetSQL("table-engine.sql.tmpl")
	if err != nil {
		return err
	}
	foreignKeyQuery, err := assetSQL("foreign-key-count.sql.tmpl")
	if err != nil {
		return err
	}
	triggerQuery, err := assetSQL("trigger-count.sql.tmpl")
	if err != nil {
		return err
	}
	columnExistsQuery, err := assetSQL("column-exists.sql.tmpl")
	if err != nil {
		return err
	}
	integerColumnQuery, err := assetSQL("integer-column.sql.tmpl")
	if err != nil {
		return err
	}
	rangeIndexQuery, err := assetSQL("range-index.sql.tmpl")
	if err != nil {
		return err
	}
	uniqueCursorQuery, err := assetSQL("unique-cursor.sql.tmpl")
	if err != nil {
		return err
	}
	sources := make(map[string]sharding.Table, opts.FromCount)
	for _, move := range moves {
		table, tableErr := fromPlan.TableForBucket(move.BucketStart)
		if tableErr != nil {
			return tableErr
		}
		sources[move.Source] = table
	}
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		table := sources[name]
		exists, err := tableExists(ctx, database, name)
		if err != nil {
			return err
		}
		if !exists {
			return errors.Errorf("源物理表不存在 source=%s", name)
		}
		var engine string
		if err := database.QueryRowContext(ctx, tableEngineQuery, name).Scan(&engine); err != nil {
			return errors.Wrapf(err, "校验源表存储引擎失败 source=%s", name)
		}
		if !strings.EqualFold(engine, "InnoDB") {
			return errors.Errorf("源物理表必须使用 InnoDB source=%s engine=%s", name, engine)
		}
		var foreignKeyCount int
		if err := database.QueryRowContext(ctx, foreignKeyQuery, name).Scan(&foreignKeyCount); err != nil {
			return errors.Wrapf(err, "校验源表外键失败 source=%s", name)
		}
		if foreignKeyCount != 0 {
			return errors.Errorf("源物理表存在外键，应用内在线拆表不支持 source=%s count=%d", name, foreignKeyCount)
		}
		var triggerCount int
		if err := database.QueryRowContext(ctx, triggerQuery, name).Scan(&triggerCount); err != nil {
			return errors.Wrapf(err, "校验源表触发器失败 source=%s", name)
		}
		if triggerCount != 0 {
			return errors.Errorf("源物理表存在触发器，必须单独评审并迁移，通用命令拒绝执行 source=%s count=%d", name, triggerCount)
		}
		for _, column := range []string{opts.UIDColumn, opts.ShardColumn, opts.CursorColumn} {
			var count int
			if err := database.QueryRowContext(ctx, columnExistsQuery, name, column).Scan(&count); err != nil {
				return errors.Wrapf(err, "校验源表字段失败 source=%s column=%s", name, column)
			}
			if count != 1 {
				return errors.Errorf("源表缺少字段 source=%s column=%s", name, column)
			}
		}
		var integerUIDCount int
		if err := database.QueryRowContext(ctx, integerColumnQuery, name, opts.UIDColumn).Scan(&integerUIDCount); err != nil {
			return errors.Wrapf(err, "校验非空整数 UID 字段失败 source=%s column=%s", name, opts.UIDColumn)
		}
		if integerUIDCount != 1 {
			return errors.Errorf("源表 UID 字段必须是非空整数类型 source=%s column=%s", name, opts.UIDColumn)
		}
		var integerShardCount int
		if err := database.QueryRowContext(ctx, integerColumnQuery, name, opts.ShardColumn).Scan(&integerShardCount); err != nil {
			return errors.Wrapf(err, "校验非空整数固定桶字段失败 source=%s column=%s", name, opts.ShardColumn)
		}
		if integerShardCount != 1 {
			return errors.Errorf("源表固定桶字段必须是非空整数类型 source=%s column=%s", name, opts.ShardColumn)
		}
		var integerCursorCount int
		if opts.CursorColumn == opts.UIDColumn {
			integerCursorCount = integerUIDCount
		} else if opts.CursorColumn == opts.ShardColumn {
			integerCursorCount = integerShardCount
		} else if err := database.QueryRowContext(ctx, integerColumnQuery, name, opts.CursorColumn).Scan(&integerCursorCount); err != nil {
			return errors.Wrapf(err, "校验非空整数分页游标失败 source=%s column=%s", name, opts.CursorColumn)
		}
		if integerCursorCount != 1 {
			return errors.Errorf("源表分页游标必须是非空整数类型 source=%s column=%s", name, opts.CursorColumn)
		}
		var indexCount int
		if err := database.QueryRowContext(ctx, rangeIndexQuery, name, opts.ShardColumn, opts.CursorColumn).Scan(&indexCount); err != nil {
			return errors.Wrapf(err, "校验迁移索引失败 source=%s", name)
		}
		if indexCount == 0 {
			return errors.Errorf("源表缺少以 (%s,%s) 开头的复合索引 source=%s", opts.ShardColumn, opts.CursorColumn, name)
		}
		var uniqueCursorCount int
		if err := database.QueryRowContext(ctx, uniqueCursorQuery, name, opts.CursorColumn).Scan(&uniqueCursorCount); err != nil {
			return errors.Wrapf(err, "校验唯一分页游标失败 source=%s column=%s", name, opts.CursorColumn)
		}
		if uniqueCursorCount == 0 {
			return errors.Errorf("源表分页游标必须有单列 PRIMARY KEY 或 UNIQUE 索引 source=%s column=%s", name, opts.CursorColumn)
		}
		if err := validateTableRoutes(ctx, database, opts, name, table.BucketStart, table.BucketEnd); err != nil {
			return errors.Wrapf(err, "校验源表 UID 固定桶失败 source=%s", name)
		}
	}
	return nil
}

// validateTarget 校验目标表存在、结构一致且尚未写入数据。
func validateTarget(ctx context.Context, database *sql.DB, move sharding.Move) error {
	if err := validateTargetStructure(ctx, database, move); err != nil {
		return err
	}
	query, err := renderSQL("count-table.sql.tmpl", map[string]string{"{{TABLE}}": quoteIdentifier(move.Target)})
	if err != nil {
		return err
	}
	var count int64
	if err := database.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return errors.Wrapf(err, "校验目标空表失败 target=%s", move.Target)
	}
	if count != 0 {
		return errors.Errorf("目标物理表必须为空 target=%s rows=%d；失败重跑前先由 DBA 清空未切流目标表", move.Target, count)
	}
	return nil
}

// validateTargetStructure 校验目标表存在且结构与源表一致。
func validateTargetStructure(ctx context.Context, database *sql.DB, move sharding.Move) error {
	exists, err := tableExists(ctx, database, move.Target)
	if err != nil {
		return err
	}
	if !exists {
		return errors.Errorf("目标物理表不存在 target=%s", move.Target)
	}
	sourceDDL, err := showCreateTable(ctx, database, move.Source)
	if err != nil {
		return err
	}
	targetDDL, err := showCreateTable(ctx, database, move.Target)
	if err != nil {
		return err
	}
	if normalizeCreateTable(sourceDDL) != normalizeCreateTable(targetDDL) {
		return errors.Errorf("目标物理表结构与源表不一致 source=%s target=%s", move.Source, move.Target)
	}
	return nil
}

// validateCleanupTarget 校验清理前目标结构和固定桶范围没有漂移。
func validateCleanupTarget(ctx context.Context, database *sql.DB, opts PrepareOptions, move sharding.Move) error {
	if err := validateTargetStructure(ctx, database, move); err != nil {
		return err
	}
	if err := validateTableRoutes(ctx, database, opts, move.Target, move.BucketStart, move.BucketEnd); err != nil {
		return errors.Wrapf(err, "清理前校验目标 UID 固定桶失败 target=%s", move.Target)
	}
	return nil
}

// validateTableRoutes 按固定桶索引串行校验物理表范围和 UID 路由公式。
func validateTableRoutes(
	ctx context.Context,
	database *sql.DB,
	opts PrepareOptions,
	table string,
	bucketStart int,
	bucketEnd int,
) error {
	replacements := map[string]string{
		"{{TABLE}}": quoteIdentifier(table),
		"{{UID}}":   quoteIdentifier(opts.UIDColumn),
		"{{SHARD}}": quoteIdentifier(opts.ShardColumn),
	}
	rangeQuery, err := renderSQL("route-range-mismatch.sql.tmpl", replacements)
	if err != nil {
		return err
	}
	var mismatch int
	if err := database.QueryRowContext(ctx, rangeQuery, bucketStart, bucketEnd).Scan(&mismatch); err != nil {
		return errors.Wrapf(err, "检查物理表桶范围失败 table=%s", table)
	}
	if mismatch != 0 {
		return errors.Errorf("物理表存在桶范围外数据 table=%s shard=%s expected_range=%d-%d", table, opts.ShardColumn, bucketStart, bucketEnd)
	}
	routeQuery, err := renderSQL("route-mismatch.sql.tmpl", replacements)
	if err != nil {
		return err
	}
	for bucket := bucketStart; bucket <= bucketEnd; bucket++ {
		if err := database.QueryRowContext(ctx, routeQuery, bucket).Scan(&mismatch); err != nil {
			return errors.Wrapf(err, "检查固定桶公式失败 table=%s bucket=%d", table, bucket)
		}
		if mismatch != 0 {
			return errors.Errorf(
				"物理表存在 UID 非法或固定桶公式错误 table=%s uid=%s shard=%s bucket=%d",
				table,
				opts.UIDColumn,
				opts.ShardColumn,
				bucket,
			)
		}
	}
	return nil
}

// tableExists 判断当前数据库是否存在指定物理表。
func tableExists(ctx context.Context, database *sql.DB, table string) (bool, error) {
	query, err := assetSQL("table-exists.sql.tmpl")
	if err != nil {
		return false, err
	}
	var count int
	if err := database.QueryRowContext(ctx, query, table).Scan(&count); err != nil {
		return false, errors.Wrapf(err, "检查物理表失败 table=%s", table)
	}
	return count == 1, nil
}

// moveSources 返回扩容计划中去重且稳定排序的源物理表。
func moveSources(moves []sharding.Move) []string {
	sourceSet := make(map[string]struct{}, len(moves))
	for _, move := range moves {
		sourceSet[move.Source] = struct{}{}
	}
	sources := make([]string, 0, len(sourceSet))
	for source := range sourceSet {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	return sources
}

// showCreateTable 读取物理表结构定义。
func showCreateTable(ctx context.Context, database *sql.DB, table string) (string, error) {
	query, err := renderSQL("show-create-table.sql.tmpl", map[string]string{"{{TABLE}}": quoteIdentifier(table)})
	if err != nil {
		return "", err
	}
	var tableName string
	var ddl string
	if err := database.QueryRowContext(ctx, query).Scan(&tableName, &ddl); err != nil {
		return "", errors.Wrapf(err, "读取物理表结构失败 table=%s", table)
	}
	return ddl, nil
}

// normalizeCreateTable 忽略表名和自增水位后比较真实结构。
func normalizeCreateTable(ddl string) string {
	ddl = createTablePattern.ReplaceAllString(ddl, "CREATE TABLE `__table__`")
	ddl = autoIncrementPattern.ReplaceAllString(ddl, "")
	return checkConstraintPattern.ReplaceAllString(ddl, "CONSTRAINT `__check__` CHECK")
}

// acquireLock 获取同一张逻辑表的数据库级迁移互斥锁。
func acquireLock(ctx context.Context, database *sql.DB, opts PrepareOptions) (func(), error) {
	tableDigest := sha256.Sum256([]byte(opts.FirstTable)) // 摘要避免 MySQL 命名锁超过 64 字节
	lockName := fmt.Sprintf("table_shard:%x", tableDigest[:16])
	acquireQuery, err := assetSQL("acquire-lock.sql.tmpl")
	if err != nil {
		return nil, err
	}
	releaseQuery, err := assetSQL("release-lock.sql.tmpl")
	if err != nil {
		return nil, err
	}
	connection, err := database.Conn(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "获取拆表锁连接失败 table=%s", opts.FirstTable)
	}
	var acquired sql.NullInt64
	if err := connection.QueryRowContext(ctx, acquireQuery, lockName, migrationLockTimeoutSeconds).Scan(&acquired); err != nil {
		_ = connection.Close()
		return nil, errors.Wrapf(err, "获取拆表互斥锁失败 table=%s", opts.FirstTable)
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		_ = connection.Close()
		return nil, errors.Errorf("已有拆表任务占用互斥锁 table=%s", opts.FirstTable)
	}
	return func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = connection.ExecContext(releaseCtx, releaseQuery, lockName)
		_ = connection.Close()
	}, nil
}

// assetSQL 读取固定 SQL 模板；发布物缺失内嵌资产时返回错误，由拆表命令安全终止当前操作。
func assetSQL(name string) (string, error) {
	content, err := sqlAssets.ReadFile("assets/" + name)
	if err != nil {
		return "", errors.Wrapf(err, "读取内嵌拆表 SQL 失败 asset=%s", name)
	}
	return embedasset.StripLeadingLineComments(string(content), "--"), nil
}

// renderSQL 替换已校验并引用的 SQL 标识符占位。
func renderSQL(name string, replacements map[string]string) (string, error) {
	query, err := assetSQL(name)
	if err != nil {
		return "", err
	}
	pairs := make([]string, 0, len(replacements)*2)
	for key, value := range replacements {
		pairs = append(pairs, key, value)
	}
	return strings.NewReplacer(pairs...).Replace(query), nil
}

// quoteIdentifier 引用已经通过严格校验的 MySQL 标识符。
func quoteIdentifier(identifier string) string {
	return "`" + identifier + "`"
}
