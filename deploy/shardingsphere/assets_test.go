package shardingsphere

import (
	"os"
	"strings"
	"testing"
)

// TestGlobalTemplateRequiresZooKeeperDigest 验证集群元数据配置包含 ZooKeeper digest ACL 凭据。
func TestGlobalTemplateRequiresZooKeeperDigest(t *testing.T) {
	template := readAsset(t, "conf/global.yaml.tmpl")
	if !strings.Contains(template, "digest: __ZOOKEEPER_DIGEST__") {
		t.Fatal("global template missing ZooKeeper digest")
	}
	renderer := readAsset(t, "render-global.sh")
	for _, want := range []string{"require_value \"$value_name\"", "validate_value ZOOKEEPER_DIGEST", "__ZOOKEEPER_DIGEST__"} {
		if !strings.Contains(renderer, want) {
			t.Fatalf("render-global.sh missing %q", want)
		}
	}
}

// TestShardNoAssetsCoverNullAndFormulaGuard 验证手工模板和最终约束都不会漏掉 NULL 错误值。
func TestShardNoAssetsCoverNullAndFormulaGuard(t *testing.T) {
	addColumn := readAsset(t, "sql/add-shard-no-column.sql.tmpl")
	for _, want := range []string{"`shard_no` int NOT NULL DEFAULT 0", "ALGORITHM=INSTANT", "LOCK=NONE"} {
		if !strings.Contains(addColumn, want) {
			t.Fatalf("add-shard-no-column.sql.tmpl missing %q", want)
		}
	}
	for _, asset := range []string{
		"sql/backfill-shard-no.sql.tmpl",
		"sql/backfill-uid-shard-no.sql.tmpl",
		"sql/verify-shard-no.sql.tmpl",
		"sql/verify-uid-shard-no.sql.tmpl",
	} {
		if text := readAsset(t, asset); !strings.Contains(text, "`shard_no` IS NULL") {
			t.Fatalf("%s missing NULL mismatch handling", asset)
		}
	}
	constraint := readAsset(t, "sql/enforce-shard-no.sql.tmpl")
	for _, want := range []string{"MOD(CRC32(CAST(`__UID_COLUMN__` AS CHAR)), 1024)", ") ENFORCED"} {
		if !strings.Contains(constraint, want) {
			t.Fatalf("enforce-shard-no.sql.tmpl missing %q", want)
		}
	}
	for _, forbidden := range []string{"LOCK=NONE", "ALGORITHM=INPLACE"} {
		if strings.Contains(constraint, forbidden) {
			t.Fatalf("enforce-shard-no.sql.tmpl must not promise unsupported %s", forbidden)
		}
	}
	guards := map[string]string{
		"sql/source-insert-shard-no-guard.sql.tmpl": "AFTER INSERT",
		"sql/source-update-shard-no-guard.sql.tmpl": "BEFORE UPDATE",
	}
	for asset, operation := range guards {
		guard := readAsset(t, asset)
		for _, want := range []string{operation, "SIGNAL SQLSTATE '45000'", "lock_wait_timeout = 5"} {
			if !strings.Contains(guard, want) {
				t.Fatalf("%s missing %q", asset, want)
			}
		}
	}
	insertGuard := readAsset(t, "sql/source-insert-shard-no-guard.sql.tmpl")
	for _, want := range []string{"NEW.`__PRIMARY_KEY__` <= 0", "NEW.`__UID_COLUMN__` <= 0"} {
		if !strings.Contains(insertGuard, want) {
			t.Fatalf("source-insert-shard-no-guard.sql.tmpl missing %q", want)
		}
	}
	updateGuard := readAsset(t, "sql/source-update-shard-no-guard.sql.tmpl")
	for _, want := range []string{"NEW.`__PRIMARY_KEY__` <=> OLD.`__PRIMARY_KEY__`", "NEW.`__UID_COLUMN__` <=> OLD.`__UID_COLUMN__`", "NEW.`__UID_COLUMN__` <= 0", "NEW.`shard_no` <=> OLD.`shard_no`"} {
		if !strings.Contains(updateGuard, want) {
			t.Fatalf("source-update-shard-no-guard.sql.tmpl missing %q", want)
		}
	}
}

// TestTargetSchemaAssetsMatchMigrationSources 验证迁移期目标表不引入源表不存在的结构且拒绝复用旧对象。
func TestTargetSchemaAssetsMatchMigrationSources(t *testing.T) {
	user := readAsset(t, "sql/create-user-target.sql.tmpl")
	for _, want := range []string{
		"CREATE TABLE `user`",
		"CHECK (`shard_no` BETWEEN 0 AND 1023)",
		"PRIMARY KEY (`id`)",
	} {
		if !strings.Contains(user, want) {
			t.Fatalf("create-user-target.sql.tmpl missing %q", want)
		}
	}
	if strings.Contains(user, "IF NOT EXISTS") || strings.Contains(user, "CHECK (`shard_no` = MOD(") {
		t.Fatal("user target schema must fail on existing objects and match the source range CHECK during migration")
	}
	userTag := readAsset(t, "sql/create-user-tag-target.sql.tmpl")
	for _, want := range []string{
		"CREATE TABLE `user_tag`",
		"`id` bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键'",
	} {
		if !strings.Contains(userTag, want) {
			t.Fatalf("create-user-tag-target.sql.tmpl missing %q", want)
		}
	}
	if strings.Contains(userTag, "IF NOT EXISTS") || strings.Contains(userTag, "CHECK (`shard_no` = MOD(") {
		t.Fatal("user_tag target schema must fail on existing objects and must not add a migration-incompatible CHECK")
	}
	sourceUserTag := readAsset(t, "../../internal/database/assets/user_tag_0.sql")
	if normalizeCreateTable(userTag) != normalizeCreateTable(sourceUserTag) {
		t.Fatal("user_tag target schema drifted from source user_tag_0 schema")
	}
}

// TestBootstrapAssetsFailOnExistingObjects 验证新迁移代际不会静默复用旧逻辑库或存储单元。
func TestBootstrapAssetsFailOnExistingObjects(t *testing.T) {
	storageUnits := readAsset(t, "sql/register-storage-units.sql.tmpl")
	for _, want := range []string{"CREATE DATABASE `__LOGICAL_DATABASE__`", "REGISTER STORAGE UNIT ds_0"} {
		if !strings.Contains(storageUnits, want) {
			t.Fatalf("register-storage-units.sql.tmpl missing %q", want)
		}
	}
	if strings.Contains(storageUnits, "IF NOT EXISTS") {
		t.Fatal("register-storage-units.sql.tmpl must fail when the target namespace already exists")
	}
}

// TestSingleTableLoadAssetUsesDefaultStorageUnit 验证单表元数据加载模板固定落到默认存储单元并提供核对命令。
func TestSingleTableLoadAssetUsesDefaultStorageUnit(t *testing.T) {
	asset := readAsset(t, "sql/load-single-tables.sql.tmpl")
	for _, want := range []string{
		"USE `__LOGICAL_DATABASE__`",
		"SET DEFAULT SINGLE TABLE STORAGE UNIT = ds_0",
		"LOAD SINGLE TABLE ds_0.*",
		"SHOW SINGLE TABLES",
	} {
		if !strings.Contains(asset, want) {
			t.Fatalf("load-single-tables.sql.tmpl missing %q", want)
		}
	}
}

// normalizeCreateTable 忽略迁移源和目标允许不同的表名与初始化幂等修饰，仅比较真实表结构。
func normalizeCreateTable(source string) string {
	start := strings.Index(source, "CREATE TABLE")
	if start < 0 {
		return ""
	}
	source = source[start:]
	source = strings.Replace(source, "CREATE TABLE IF NOT EXISTS", "CREATE TABLE", 1)
	source = strings.Replace(source, "`user_tag_0`", "`user_tag`", 1)
	return strings.Join(strings.Fields(source), " ")
}

// TestGuardTemplatesMatchBackfillValidator 验证源表和目标物理表触发器与回填器批准的完整语义不会漂移。
func TestGuardTemplatesMatchBackfillValidator(t *testing.T) {
	tests := []struct {
		name     string // 测试场景名
		guard    string // 生产触发器模板
		expected string // 回填器批准的触发器体
	}{
		{name: "source insert", guard: "sql/source-insert-shard-no-guard.sql.tmpl", expected: "../../cmd/shardbackfill/assets/expected-insert-guard.sql.tmpl"},
		{name: "source update", guard: "sql/source-update-shard-no-guard.sql.tmpl", expected: "../../cmd/shardbackfill/assets/expected-update-guard.sql.tmpl"},
		{name: "target insert", guard: "sql/target-insert-shard-no-guard.sql.tmpl", expected: "../../cmd/shardbackfill/assets/expected-insert-guard.sql.tmpl"},
		{name: "target update", guard: "sql/target-update-shard-no-guard.sql.tmpl", expected: "../../cmd/shardbackfill/assets/expected-update-guard.sql.tmpl"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			guard := triggerBody(t, readAsset(t, test.guard))
			expected := triggerBody(t, readAsset(t, test.expected))
			if normalizeTriggerAsset(guard) != normalizeTriggerAsset(expected) {
				t.Fatalf("%s guard template does not match shardbackfill validator", test.name)
			}
		})
	}
}

// triggerBody 截取 SQL 模板中的 BEGIN..END 触发器体。
func triggerBody(t *testing.T, source string) string {
	t.Helper()
	start := strings.Index(source, "BEGIN")
	end := strings.LastIndex(source, "END")
	if start < 0 || end < start {
		t.Fatal("trigger template missing BEGIN..END body")
	}
	return source[start : end+len("END")]
}

// normalizeTriggerAsset 去除模板引号和空白差异，仅比较完整语句顺序。
func normalizeTriggerAsset(source string) string {
	return strings.Map(func(value rune) rune {
		if value == '`' || value == ' ' || value == '\t' || value == '\r' || value == '\n' {
			return -1
		}
		return value
	}, source)
}

// readAsset 读取当前部署目录下的受版本控制资产。
func readAsset(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("os.ReadFile(%s) error = %v", path, err)
	}
	return string(data)
}
