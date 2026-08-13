package database

import (
	"admin/internal/routealias"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// TestValidateDefaultMigrations 确保默认迁移清单完整、版本递增且资产存在。
func TestValidateDefaultMigrations(t *testing.T) {
	if err := ValidateDefaultMigrations(); err != nil {
		t.Fatalf("ValidateDefaultMigrations() error = %v", err)
	}
}

// TestAdminBaselinePasswordUsesDirectBcrypt 验证初始管理员密码可按当前明文入参直接校验。
func TestAdminBaselinePasswordUsesDirectBcrypt(t *testing.T) {
	sql := migrationSQLByAsset(t, "admin.sql")
	match := regexp.MustCompile(`'super999', 'super999', '([^']+)'`).FindStringSubmatch(sql)
	if len(match) != 2 {
		t.Fatal("admin.sql missing super999 password hash")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(match[1]), []byte("Temp@1234")); err != nil {
		t.Fatalf("admin.sql super999 password hash mismatch: %v", err)
	}
}

// TestAdminBaselineLastLoginIPUsesIPv6Length 确保未上线的管理员基线可完整保存 IPv6 文本。
func TestAdminBaselineLastLoginIPUsesIPv6Length(t *testing.T) {
	if !strings.Contains(migrationSQLByAsset(t, "admin.sql"), "`last_login_ip` varchar(45)") {
		t.Fatal("admin.sql 的 last_login_ip 必须为 varchar(45)")
	}
}

// TestAdminBaselineUsesNullForNeverLoggedIn 验证全新空库以 NULL 表示尚未发生的成功登录事件。
func TestAdminBaselineUsesNullForNeverLoggedIn(t *testing.T) {
	sql := migrationSQLByAsset(t, "admin.sql")
	if !strings.Contains(sql, "`last_login_time` datetime NULL DEFAULT NULL COMMENT '最后登录时间，NULL 表示从未登录'") {
		t.Fatal("admin.sql 的 last_login_time 必须允许 NULL，并以 NULL 表示从未登录")
	}
}

// TestArchiveControlBaselineUsesMicrosecondPrecision 确保归档控制表完整保留 MySQL 微秒时间。
func TestArchiveControlBaselineUsesMicrosecondPrecision(t *testing.T) {
	tests := []struct {
		asset  string   // 归档控制表基线资产
		fields []string // 必须使用 datetime(6) 的时间字段
	}{
		{asset: "archive_watermark.sql", fields: []string{"watermark_time", "updated_at"}},
		{asset: "archive_segment.sql", fields: []string{
			"range_start", "range_end", "lease_expires_at", "last_archived_time",
			"created_at", "updated_at", "completed_at",
		}},
	}
	for _, tt := range tests {
		sql := strings.ToLower(migrationSQLByAsset(t, tt.asset))
		for _, field := range tt.fields {
			want := "`" + field + "` datetime(6)"
			if !strings.Contains(sql, want) {
				t.Fatalf("%s 的 %s 必须使用 datetime(6)", tt.asset, field)
			}
		}
	}
}

// TestDefaultMigrationsCoverDatabaseSQLAssets 确保 database 下的 SQL 快照都纳入迁移治理。
func TestDefaultMigrationsCoverDatabaseSQLAssets(t *testing.T) {
	assets, err := MigrationAssetNames()
	if err != nil {
		t.Fatalf("MigrationAssetNames() error = %v", err)
	}
	covered := make(map[string]struct{}, len(DefaultMigrations()))
	for _, item := range DefaultMigrations() {
		covered[item.Asset] = struct{}{}
		if strings.HasPrefix(item.Name, "bootstrap_") && (!item.BootstrapOnly || item.Destructive) {
			t.Fatalf("admin baseline migration must be bootstrap-only and non-destructive: %+v", item)
		}
		if !strings.HasPrefix(item.Name, "bootstrap_") && item.BootstrapOnly {
			t.Fatalf("incremental migration should not be bootstrap-only: %+v", item)
		}
		if len(item.Checksum) != 64 {
			t.Fatalf("migration checksum length = %d, want 64: %+v", len(item.Checksum), item)
		}
		if strings.Contains(item.SQL, "Navicat Premium Dump SQL") || strings.Contains(item.SQL, "用户标签工作流骨架运行时表结构") {
			t.Fatalf("migration SQL header should be stripped: %s", item.Asset)
		}
		for _, forbidden := range []string{"DROP TABLE", "SET FOREIGN_KEY_CHECKS", "SET NAMES", "BEGIN;", "COMMIT;"} {
			if strings.Contains(strings.ToUpper(item.SQL), forbidden) {
				t.Fatalf("migration SQL contains dump-only statement %q: %s", forbidden, item.Asset)
			}
		}
	}
	for _, asset := range assets {
		if _, ok := covered[asset]; !ok {
			t.Fatalf("database SQL asset missing migration manifest: %s", asset)
		}
	}
}

// TestSchemaMigrationsSQL 确保迁移版本表 DDL 会剥离文件头。
func TestSchemaMigrationsSQL(t *testing.T) {
	sql := SchemaMigrationsSQL()
	if sql == "" {
		t.Fatal("SchemaMigrationsSQL() is empty")
	}
	if strings.Contains(sql, "代码资产") {
		t.Fatalf("SchemaMigrationsSQL() should strip header comments: %q", sql)
	}
	if !strings.Contains(sql, "schema_migrations") {
		t.Fatalf("SchemaMigrationsSQL() missing schema_migrations DDL: %q", sql)
	}
}

// TestDatabaseSQLAssetsCreateSingleTable 确保迁移 SQL 资产保持一张表一个文件。
func TestDatabaseSQLAssetsCreateSingleTable(t *testing.T) {
	re := regexp.MustCompile(`(?im)^\s*CREATE\s+TABLE\b`)
	for _, item := range DefaultMigrations() {
		got := len(re.FindAllString(item.SQL, -1))
		if got > 1 {
			t.Fatalf("migration asset %s CREATE TABLE count = %d, want at most 1", item.Asset, got)
		}
		if item.BootstrapOnly && got != 1 {
			t.Fatalf("bootstrap migration asset %s CREATE TABLE count = %d, want 1", item.Asset, got)
		}
	}
}

// TestUserTagRuntimeMigrationStartsSinglePhysicalShard 确保用户标签基线不会一次创建多张结果和快照表。
func TestUserTagRuntimeMigrationStartsSinglePhysicalShard(t *testing.T) {
	sql := migrationSQLByAssets(t,
		"user_tag_0.sql",
		"user_tag_0_tmp.sql",
		"user_tag_runtime_uid.sql",
		"user_tag_runtime_checkpoint.sql",
		"user_tag_event_outbox.sql",
	)
	for _, forbidden := range []string{
		"`user_tag_1`",
		"`user_tag_1_tmp`",
		"`user_tag_" + "sync_0`",
	} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("user tag migration assets should not create upfront table %s", forbidden)
		}
	}
	if !strings.Contains(sql, "`user_tag_0`") || !strings.Contains(sql, "`user_tag_0_tmp`") {
		t.Fatalf("user tag migration assets missing base result table or tmp table")
	}
	for _, want := range []string{
		"`shard_no` int NOT NULL DEFAULT 0 COMMENT 'uid取模1024分片'",
		"KEY `idx_shard_uid` (`shard_no`, `uid`)",
		"KEY `idx_tag_type_shard_uid` (`tag_type`, `shard_no`, `uid`)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("user tag migration assets missing shard_no baseline DDL %q", want)
		}
	}
}

// TestBusinessUIDMigrationAssetsCarryShardNo 确保新增业务 UID 表不会漏掉分片字段。
func TestBusinessUIDMigrationAssetsCarryShardNo(t *testing.T) {
	shardIndexRe := regexp.MustCompile("(?i)KEY\\s+`[^`]*shard[^`]*`\\s*\\([^)]*`shard_no`")
	for _, item := range DefaultMigrations() {
		if !strings.Contains(item.SQL, "`uid`") {
			continue
		}
		if !strings.Contains(item.SQL, "`shard_no`") {
			t.Fatalf("migration asset with business uid must carry shard_no: %s", item.Asset)
		}
		if !shardIndexRe.MatchString(item.SQL) {
			t.Fatalf("migration asset with business uid must keep shard_no index: %s", item.Asset)
		}
	}
}

// TestCollectorFailedEventBaselineIndexes 确保收集器概览索引内置在基线 DDL。
func TestCollectorFailedEventBaselineIndexes(t *testing.T) {
	sql := migrationSQLByAsset(t, "collector_failed_event.sql")
	for _, want := range []string{
		"UNIQUE KEY `uk_biz_event_id` (`biz_type`,`event_id`)",
		"`claim_token` varchar(64) NOT NULL DEFAULT ''",
		"`lease_until` datetime(3) NULL DEFAULT NULL",
		"KEY `idx_state_lease` (`state`,`lease_until`)",
		"KEY `idx_state_finished` (`state`,`finished_at`)",
		"KEY `idx_state_updated` (`state`,`updated_at`)",
		"KEY `idx_state_next` (`state`,`next_run_at`)",
		"KEY `idx_partition_state_next` (`partition_key`,`state`,`next_run_at`)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("collector_failed_event baseline missing overview index %q", want)
		}
	}
	if strings.Contains(sql, "idx_state_started") {
		t.Fatal("collector_failed_event baseline should use lease_until instead of started_at for lease recovery")
	}
	if strings.Contains(sql, "`transport`") {
		t.Fatal("collector_failed_event baseline should not keep removed transport column")
	}
	if strings.Contains(sql, "UNIQUE KEY `uk_event_id`") {
		t.Fatal("collector_failed_event baseline should not use global event_id unique key")
	}
}

// TestAdminLogBaselineUsesCollectorEventID 确保审计日志以 EventID 唯一索引承接 Redis/Kafka 重放。
func TestAdminLogBaselineUsesCollectorEventID(t *testing.T) {
	sql := migrationSQLByAsset(t, "admin_log.sql")
	for _, want := range []string{
		"`event_id` varchar(64) NOT NULL DEFAULT ''",
		"UNIQUE KEY `uk_event_id` (`event_id`)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("admin_log baseline missing persistent idempotency DDL %q", want)
		}
	}
}

// TestAdminRolePermissionBaselineSkipsSuperRole 确保超级管理员不依赖角色权限关系种子数据。
func TestAdminRolePermissionBaselineSkipsSuperRole(t *testing.T) {
	sql := migrationSQLByAsset(t, "admin_role_permission_rel.sql")
	if strings.Contains(sql, "(1,") || strings.Contains(sql, "VALUES\n(1,") {
		t.Fatalf("admin_role_permission_rel.sql should not seed super role permissions")
	}
}

// TestRuntimeConfigBaselineSeedsDraftRows 确保运行配置基线只逐条种草稿明细，不写默认发布快照。
func TestRuntimeConfigBaselineSeedsDraftRows(t *testing.T) {
	sql := migrationSQLByAssets(t,
		"runtime_config_release.sql",
		"runtime_config_state.sql",
		"runtime_task_periodic.sql",
		"runtime_archive_job.sql",
	)
	for _, want := range []string{
		"INSERT IGNORE INTO `runtime_config_state`",
		"INSERT IGNORE INTO `runtime_task_periodic`",
		"INSERT IGNORE INTO `runtime_archive_job`",
		"archive-admin-log-hourly",
		"task-report-daily-summary",
		"task_report.daily_summary",
		"user-tag-delta-daily",
		"active_release_id",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("runtime config migration assets missing seed fragment %q", want)
		}
	}
	releaseSQL := migrationSQLByAsset(t, "runtime_config_release.sql")
	if strings.Contains(releaseSQL, "INSERT IGNORE INTO `runtime_config_release`") || strings.Contains(releaseSQL, "snapshot_json") && strings.Contains(releaseSQL, "baseline local runtime config seed") {
		t.Fatal("runtime_config_release.sql should not seed default snapshot rows")
	}
	periodicSQL := migrationSQLByAsset(t, "runtime_task_periodic.sql")
	if got := strings.Count(periodicSQL, "INSERT IGNORE INTO `runtime_task_periodic`"); got != 5 {
		t.Fatalf("runtime_task_periodic seed count = %d, want 5", got)
	}
	if !strings.Contains(periodicSQL, "'user-tag-delta-daily'") || !strings.Contains(periodicSQL, "'[\"dry_run=1\"]'") {
		t.Fatal("user-tag-delta-daily baseline must remain disabled and dry-run only")
	}
	archiveSQL := migrationSQLByAsset(t, "runtime_archive_job.sql")
	if got := strings.Count(archiveSQL, "INSERT IGNORE INTO `runtime_archive_job`"); got != 1 {
		t.Fatalf("runtime_archive_job seed count = %d, want 1", got)
	}
	for table, seedSQL := range map[string]string{
		"runtime_task_periodic": periodicSQL,
		"runtime_archive_job":   archiveSQL,
	} {
		if strings.Contains(seedSQL, "INSERT IGNORE INTO `"+table+"` (`id`,") {
			t.Fatalf("%s seed insert should not pin auto-increment id", table)
		}
	}
	stateSQL := migrationSQLByAsset(t, "runtime_config_state.sql")
	if !strings.Contains(stateSQL, "VALUES (1, 0, 0, ''") {
		t.Fatalf("runtime_config_state should start without active release: %s", stateSQL)
	}
	sysConfigSQL := migrationSQLByAsset(t, "sys_config.sql")
	if !strings.Contains(sysConfigSQL, "INSERT IGNORE INTO `sys_config` (`id`,") {
		t.Fatal("sys_config seed insert should keep fixed ids because pid/pids reference the hierarchy")
	}
	for _, forbidden := range []string{"admin_role_permission_rel", "(1, 139", "baseline local runtime config seed"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("runtime config migration assets should not seed super role permissions: %q", forbidden)
		}
	}
	for _, forbidden := range []string{"`app_id`", "`env`", "'1', 'dev'"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("runtime config migration assets should not keep DB scope fragment %q", forbidden)
		}
	}
}

// TestDocumentPermissionBaseline 确保单篇文档权限收口在独立文档权限表基线中。
func TestDocumentPermissionBaseline(t *testing.T) {
	sql := migrationSQLByAsset(t, "admin_doc_permission.sql")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `admin_doc_permission`",
		"INSERT IGNORE INTO `admin_doc_permission`",
		"'admin', '文档首页.md'",
		"'admin', '角色文档/后端开发/AI开发提示词.md'",
		"'admin', '角色文档/后端开发/系统组件功能说明.md'",
		"'admin', '接口文档/后台系统/权限管理接口.md'",
		"'api', '接口文档/前台系统/系统接口.md'",
		"'api', '文档首页.md'",
		"'api', '角色文档/后端开发/AI开发规范.md'",
		"UNIQUE KEY `uk_site_path` (`site`,`path`)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("document permission migration missing %q", want)
		}
	}
	if got := strings.Count(sql, "INSERT IGNORE INTO `admin_doc_permission`"); got != 62 {
		t.Fatalf("document permission baseline count = %d, want 62", got)
	}
	if got := strings.Count(sql, "'admin',"); got != 48 {
		t.Fatalf("admin document permission baseline count = %d, want 48", got)
	}
	if got := strings.Count(sql, "'api',"); got != 14 {
		t.Fatalf("api document permission baseline count = %d, want 14", got)
	}
	for _, forbidden := range []string{"admin_role_doc_permission_rel", " SELECT ", " JOIN "} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("permission baseline should not seed role relations or query other tables, found %q", forbidden)
		}
	}
	idRe := regexp.MustCompile("(?m)VALUES \\((\\d+), '(?:admin|api)',")
	for index, match := range idRe.FindAllStringSubmatch(sql, -1) {
		id, err := strconv.Atoi(match[1])
		if err != nil || id != index+1 {
			t.Fatalf("document permission seed id=%q，期望从 1 连续编号", match[1])
		}
	}
}

// TestDocumentPermissionBaselineCoversDocsResources 确保每篇受保护文档都有 site + path 权限数据。
func TestDocumentPermissionBaselineCoversDocsResources(t *testing.T) {
	sql := migrationSQLByAsset(t, "admin_doc_permission.sql")
	for _, resource := range routealias.DocsResources() {
		key := "'" + resource.Site + "', '" + resource.Path + "'"
		if !strings.Contains(sql, key) {
			t.Fatalf("document permission baseline missing resource %s", key)
		}
	}
}

// TestMigrationAssetsDMLStyle 确保迁移 DML 不使用 UPDATE，且每条 DML SQL 保持单行。
func TestMigrationAssetsDMLStyle(t *testing.T) {
	dmlStartRe := regexp.MustCompile(`(?i)^(INSERT|UPDATE|DELETE|SELECT)\b`)
	for _, item := range DefaultMigrations() {
		for lineNo, line := range strings.Split(item.SQL, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "--") {
				continue
			}
			if strings.HasPrefix(strings.ToUpper(trimmed), "UPDATE ") {
				t.Fatalf("%s contains UPDATE at line %d", item.Asset, lineNo+1)
			}
			if dmlStartRe.MatchString(trimmed) && !strings.HasSuffix(trimmed, ";") {
				t.Fatalf("%s DML statement should be one line at line %d: %s", item.Asset, lineNo+1, trimmed)
			}
		}
	}
}

// TestMigrationSeedInsertIDsAscending 确保显式主键 seed 按自增 id 递增排列。
func TestMigrationSeedInsertIDsAscending(t *testing.T) {
	for _, item := range DefaultMigrations() {
		assertSeedInsertIDsAscending(t, item.Asset, item.SQL)
	}
}

// TestDocumentPermissionsStaySeparated 确保正文权限不会重新混入正常路由权限表。
func TestDocumentPermissionsStaySeparated(t *testing.T) {
	routeSQL := migrationSQLByAsset(t, "admin_permission.sql")
	for _, want := range []string{"'docs.index'", "'docs.api_service.index'"} {
		if !strings.Contains(routeSQL, want) {
			t.Fatalf("route permission baseline missing document entry %q", want)
		}
	}
	for _, forbidden := range []string{"'docs.file.", "'docs.role.", "'docs.feature.", "'docs.api.index'", "'docs.api_service.front'"} {
		if strings.Contains(routeSQL, forbidden) {
			t.Fatalf("route permission baseline contains document resource %q", forbidden)
		}
	}
	relationSQL := migrationSQLByAsset(t, "admin_role_doc_permission_rel.sql")
	if strings.Contains(relationSQL, "INSERT") || strings.Contains(relationSQL, "(1,") {
		t.Fatal("document role relation baseline should not seed super role permissions")
	}
}

// TestAdminPermissionSeedHierarchy 确保权限主键单调且不复用已下线 ID，并验证每条父级链都引用已声明节点。
func TestAdminPermissionSeedHierarchy(t *testing.T) {
	sql := migrationSQLByAsset(t, "admin_permission.sql")
	rowRe := regexp.MustCompile(`(?m)VALUES \((\d+), '([^']*)', '[^']*', '[^']*', (\d+), '([^']*)'`)
	rows := rowRe.FindAllStringSubmatch(sql, -1)
	if len(rows) == 0 {
		t.Fatal("admin_permission.sql 未找到权限种子")
	}
	// retiredIDs 是已经从初始化资产删除但存量库可能使用过的内部主键，禁止重新分配给其他权限。
	retiredIDs := map[int]string{126: "runtime.config.import"}
	seenIDs := make(map[int]struct{}, len(rows))
	seenUUIDs := make(map[string]struct{}, len(rows))
	ancestorChains := make(map[int]string, len(rows))
	previousID := 0
	for _, row := range rows {
		id, err := strconv.Atoi(row[1])
		if err != nil {
			t.Fatalf("解析权限 id 失败: %v", err)
		}
		if id <= previousID {
			t.Fatalf("权限种子必须按 id 严格递增: previous=%d current=%d", previousID, id)
		}
		if retiredModule, retired := retiredIDs[id]; retired {
			t.Fatalf("权限种子复用了已下线主键: id=%d retired_module=%s", id, retiredModule)
		}
		if _, exists := seenIDs[id]; exists {
			t.Fatalf("权限种子 id 重复: %d", id)
		}
		seenIDs[id] = struct{}{}
		previousID = id

		uuid := strings.TrimSpace(row[2])
		if uuid == "" {
			t.Fatalf("权限 uuid 不能为空: id=%d", id)
		}
		if _, exists := seenUUIDs[uuid]; exists {
			t.Fatalf("权限 uuid 重复: id=%d uuid=%s", id, uuid)
		}
		seenUUIDs[uuid] = struct{}{}

		pid, err := strconv.Atoi(row[3])
		if err != nil {
			t.Fatalf("解析权限 pid 失败 id=%d: %v", id, err)
		}
		if pid >= id {
			t.Fatalf("权限父节点必须先于子节点: id=%d pid=%d", id, pid)
		}
		pids := strings.TrimSpace(row[4])
		if pid == 0 && pids != "" {
			t.Fatalf("根权限 pids 必须为空: id=%d pids=%s", id, pids)
		}
		if pid > 0 {
			parentChain, exists := ancestorChains[pid]
			if !exists {
				t.Fatalf("权限父节点未在当前项之前声明: id=%d pid=%d", id, pid)
			}
			expectedPIDs := strconv.Itoa(pid)
			if parentChain != "" {
				expectedPIDs = parentChain + "," + expectedPIDs
			}
			if pids != expectedPIDs {
				t.Fatalf("权限祖先链不匹配: id=%d pid=%d pids=%s expected=%s", id, pid, pids, expectedPIDs)
			}
		}
		ancestorChains[id] = pids
	}
}

// TestSecurityCacheSyncBaseline 确保补偿任务按应用隔离并使用到期时间索引。
func TestSecurityCacheSyncBaseline(t *testing.T) {
	sql := migrationSQLByAsset(t, "security_cache_sync_task.sql")
	for _, want := range []string{
		"`app_id` varchar(64) NOT NULL",
		"`payload_json` json NOT NULL",
		"`revision` bigint unsigned NOT NULL DEFAULT 1",
		"UNIQUE KEY `uk_app_digest` (`app_id`,`digest`)",
		"KEY `idx_app_next_id` (`app_id`,`next_retry_at`,`id`)",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("security cache sync baseline missing %q", want)
		}
	}
}

// assertSeedInsertIDsAscending 检查同一资产同一表内带 id 的 seed 行顺序。
func assertSeedInsertIDsAscending(t *testing.T, asset string, sql string) {
	t.Helper()
	insertRe := regexp.MustCompile("(?i)^INSERT\\s+(?:IGNORE\\s+)?INTO\\s+`([^`]+)`\\s*\\(([^)]*)\\)\\s+VALUES\\s*\\((\\d+)\\s*,")
	lastIDByTable := make(map[string]int64)
	lastLineByTable := make(map[string]int)
	for lineNo, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		matches := insertRe.FindStringSubmatch(trimmed)
		if len(matches) != 4 || !insertColumnsStartWithID(matches[2]) {
			continue
		}
		id, err := strconv.ParseInt(matches[3], 10, 64)
		if err != nil {
			t.Fatalf("%s line %d seed id parse failed: %v", asset, lineNo+1, err)
		}
		table := matches[1]
		if lastID, ok := lastIDByTable[table]; ok && id <= lastID {
			t.Fatalf("%s table %s seed id order drift at line %d: id=%d after id=%d at line %d; append by auto-increment id order", asset, table, lineNo+1, id, lastID, lastLineByTable[table])
		}
		lastIDByTable[table] = id
		lastLineByTable[table] = lineNo + 1
	}
}

// insertColumnsStartWithID 判断 INSERT 列清单是否显式以主键 id 开头。
func insertColumnsStartWithID(columns string) bool {
	columns = strings.TrimSpace(columns)
	return strings.HasPrefix(columns, "`id`,") || columns == "`id`"
}

// TestPendingMigrations 确保已登记版本不会再次进入待执行列表。
func TestPendingMigrations(t *testing.T) {
	migrations := DefaultMigrations()
	if len(migrations) < 2 {
		t.Fatal("default migrations too small")
	}
	pending := PendingMigrations(map[string]struct{}{migrations[0].Version: {}})
	if len(pending) != len(migrations)-1 {
		t.Fatalf("PendingMigrations() len = %d, want %d", len(pending), len(migrations)-1)
	}
	if pending[0].Version == migrations[0].Version {
		t.Fatalf("applied migration still pending: %+v", pending[0])
	}
}

// migrationSQLByAssets 返回多个迁移资产拼接后的 SQL。
func migrationSQLByAssets(t *testing.T, assets ...string) string {
	t.Helper()
	parts := make([]string, 0, len(assets))
	for _, asset := range assets {
		parts = append(parts, migrationSQLByAsset(t, asset))
	}
	return strings.Join(parts, "\n")
}

// migrationSQLByAsset 返回指定迁移资产的 SQL。
func migrationSQLByAsset(t *testing.T, asset string) string {
	t.Helper()
	for _, item := range DefaultMigrations() {
		if item.Asset == asset {
			return item.SQL
		}
	}
	t.Fatalf("migration asset not found: %s", asset)
	return ""
}
