package migration

import (
	"reflect"
	"strings"
	"testing"

	"admin/internal/sharding"
)

// TestNormalizeCreateTableIgnoresGeneratedNames 验证 LIKE 建表生成的新 CHECK 名称不造成结构误报。
func TestNormalizeCreateTableIgnoresGeneratedNames(t *testing.T) {
	source := "CREATE TABLE `user` (`id` bigint, CONSTRAINT `chk_user_shard_no` CHECK ((`id` > 0))) ENGINE=InnoDB AUTO_INCREMENT=99"
	target := "CREATE TABLE `user_b0512` (`id` bigint, CONSTRAINT `user_b0512_chk_1` CHECK ((`id` > 0))) ENGINE=InnoDB"
	if normalizeCreateTable(source) != normalizeCreateTable(target) {
		t.Fatalf("normalized DDL differs:\n%s\n%s", normalizeCreateTable(source), normalizeCreateTable(target))
	}
}

// TestMoveSourcesStable 验证最终写围栏只锁去重后的源物理表。
func TestMoveSourcesStable(t *testing.T) {
	moves := []sharding.Move{
		{Source: "user_b0512", Target: "user_b0768"},
		{Source: "user", Target: "user_b0256"},
		{Source: "user", Target: "user_b0512"},
	}
	want := []string{"user", "user_b0512"}
	if got := moveSources(moves); !reflect.DeepEqual(got, want) {
		t.Fatalf("moveSources() = %v, want %v", got, want)
	}
}

// TestAssetSQLStripsHeaderComment 验证嵌入 SQL 在交给驱动前移除文件头注释。
func TestAssetSQLStripsHeaderComment(t *testing.T) {
	query, err := assetSQL("table-exists.sql.tmpl")
	if err != nil {
		t.Fatalf("assetSQL() error = %v", err)
	}
	if strings.HasPrefix(query, "--") || !strings.HasPrefix(query, "SELECT") {
		t.Fatalf("assetSQL() = %q", query)
	}
}

// TestAssetSQLReturnsMissingAssetError 验证发布物资产名错误时返回受控错误，不终止拆表命令进程。
func TestAssetSQLReturnsMissingAssetError(t *testing.T) {
	if _, err := assetSQL("missing.sql.tmpl"); err == nil {
		t.Fatal("assetSQL() error = nil, want missing asset error")
	}
}

// TestCleanupSQLUsesRangeIndexOrder 验证旧数据清理按复合索引顺序取有界批次。
func TestCleanupSQLUsesRangeIndexOrder(t *testing.T) {
	query, err := renderSQL("cleanup-range.sql.tmpl", map[string]string{
		"{{SOURCE}}": "`user`",
		"{{CURSOR}}": "`id`",
		"{{SHARD}}":  "`shard_no`",
	})
	if err != nil {
		t.Fatalf("renderSQL() error = %v", err)
	}
	if !strings.Contains(query, "ORDER BY `shard_no`, `id`") {
		t.Fatalf("cleanup SQL does not use range index order: %s", query)
	}
}

// TestRouteMismatchSQLBindsUIDFormula 验证数据门禁在单桶索引范围内检查 UID 公式。
func TestRouteMismatchSQLBindsUIDFormula(t *testing.T) {
	query, err := renderSQL("route-mismatch.sql.tmpl", map[string]string{
		"{{TABLE}}": "`user_tag_0`",
		"{{UID}}":   "`uid`",
		"{{SHARD}}": "`shard_no`",
	})
	if err != nil {
		t.Fatalf("renderSQL() error = %v", err)
	}
	if !strings.Contains(query, "MOD(CRC32(CAST(`uid` AS CHAR)), 1024)") ||
		!strings.Contains(query, "`uid` > 9223372036854775807") ||
		!strings.Contains(query, "`shard_no` = ?") ||
		strings.Contains(query, "`shard_no` < ?") ||
		strings.Contains(query, "`shard_no` > ?") {
		t.Fatalf("route mismatch SQL is incomplete: %s", query)
	}
}

// TestRouteRangeMismatchSQLUsesShardBounds 验证桶范围门禁只使用固定桶索引边界。
func TestRouteRangeMismatchSQLUsesShardBounds(t *testing.T) {
	query, err := renderSQL("route-range-mismatch.sql.tmpl", map[string]string{
		"{{TABLE}}": "`user_tag_0`",
		"{{SHARD}}": "`shard_no`",
	})
	if err != nil {
		t.Fatalf("renderSQL() error = %v", err)
	}
	if !strings.Contains(query, "`shard_no` < ?") || !strings.Contains(query, "`shard_no` > ?") {
		t.Fatalf("route range mismatch SQL is incomplete: %s", query)
	}
}
