package cache

import (
	"strconv"
	"strings"
	"time"

	keys "admin/common/rediskeys"
	"admin/common/runtimecfg"
	corelogic "admin/internal/logic"
	"admin/internal/svc"
	"admin/internal/types"

	"github.com/Is999/go-utils/errors"
	tablecache "github.com/Is999/table-cache"
	"gorm.io/gorm"
)

const (
	// TableCacheMetricsSubsystem 表示表缓存 Prometheus 指标子系统。
	TableCacheMetricsSubsystem = "tcache"
	// TableCacheMetricPrefix 表示表缓存 Prometheus 指标完整前缀。
	TableCacheMetricPrefix = TableCacheMetricsSubsystem + "_"
	// tableCacheRebuildLockTTL 表示 table-cache 回源锁默认持有时长。
	tableCacheRebuildLockTTL = 10 * time.Second
	// tableCacheWaitStep 表示等待其他实例回源完成的单次等待时间。
	tableCacheWaitStep = 80 * time.Millisecond
)

// TableCacheManager 创建 admin 的表数据缓存管理器。
func TableCacheManager(base *corelogic.BaseLogic) (*tablecache.Manager, error) {
	if base == nil || base.Redis() == nil {
		return nil, WrapRedisUnavailable(nil, "表缓存管理器初始化失败")
	}
	keyPrefix := tableCacheKeyPrefix(base)
	if keyPrefix == "" {
		return nil, WrapRedisUnavailable(nil, "表缓存管理器初始化失败：app_id 缓存命名空间未初始化")
	}
	store := base.Svc.TableCacheStore
	if store == nil {
		// 兼容只手工组装 ServiceContext 的包内测试；生产入口始终由 NewServiceContext 注入进程级 Store。
		store = tablecache.NewRedisStore(base.Redis())
	}
	return tablecache.NewManager(
		store,
		tableCacheTargets(base),
		tablecache.WithKeyPrefix(keyPrefix),
		tablecache.WithEmptyMarker(keys.EmptyValueMarker, corelogic.EmptyCacheTTL()),
		tablecache.WithLockTTL(tableCacheRebuildLockTTL),
		tablecache.WithWait(tableCacheWaitStep, 3),
		tablecache.WithMetrics(base.Svc.TableCacheMetrics),
	)
}

// tableCacheKeyPrefix 返回当前站点 table-cache 托管缓存使用的 Redis Key 前缀。
// 前缀来源于运行期 app_id，确保多站点共用 Redis 时权限、配置和秘钥缓存不会相互覆盖。
func tableCacheKeyPrefix(base *corelogic.BaseLogic) string {
	if base == nil || base.AppID() == "" || base.AppID() != runtimecfg.AppID() {
		return ""
	}
	return keys.TableCachePrefix()
}

// TableCachePhysicalKey 把逻辑缓存 key 转换为 table-cache 当前要求的真实 Redis key。
// 读穿缓存、刷新、删除和直接 Redis 删除都统一使用带前缀的真实 key，确保 miss 后能回源写入当前命名空间。
func TableCachePhysicalKey(base *corelogic.BaseLogic, key string) string {
	key = strings.TrimSpace(key)
	prefix := tableCacheKeyPrefix(base)
	if key == "" || prefix == "" {
		return ""
	}
	if strings.HasPrefix(key, prefix) {
		return key
	}
	if keys.IsForeignKey(key) {
		return ""
	}
	if keys.HasPrefix(key) {
		return key
	}
	return prefix + key
}

// TableCacheLogicalKey 去掉 table-cache 项目级前缀，供分类、脱敏和模板匹配使用。
func TableCacheLogicalKey(base *corelogic.BaseLogic, key string) string {
	key = strings.TrimSpace(key)
	if base == nil || base.Svc == nil {
		return key
	}
	return keys.TrimTableCachePrefix(key)
}

// TableCachePhysicalKeys 批量转换 table-cache 托管缓存 key，并过滤空值。
func TableCachePhysicalKeys(base *corelogic.BaseLogic, cacheKeys ...string) []string {
	result := make([]string, 0, len(cacheKeys))
	for _, key := range cacheKeys {
		key = TableCachePhysicalKey(base, key)
		if key == "" {
			continue
		}
		result = append(result, key)
	}
	return result
}

// TableCacheReadDB 统一获取表缓存回源所需的读库连接，缺失时返回明确错误，避免直接触发 GORM 空指针。
func TableCacheReadDB(base *corelogic.BaseLogic, database svc.DBName, databaseLabel string) (*gorm.DB, error) {
	if base == nil || base.Svc == nil {
		return nil, errors.Errorf("服务上下文未初始化")
	}
	readDB := base.Svc.ReadDB(database)
	if readDB == nil {
		return nil, errors.Errorf("%s读库未初始化", strings.TrimSpace(databaseLabel))
	}
	return readDB, nil
}

// TableCacheWriteDB 统一获取表缓存回源所需的主库连接，缺失时返回明确错误，避免直接触发 GORM 空指针。
func TableCacheWriteDB(base *corelogic.BaseLogic, database svc.DBName, databaseLabel string) (*gorm.DB, error) {
	if base == nil || base.Svc == nil {
		return nil, errors.Errorf("服务上下文未初始化")
	}
	writeDB := base.Svc.WriteDB(database)
	if writeDB == nil {
		return nil, errors.Errorf("%s主库未初始化", strings.TrimSpace(databaseLabel))
	}
	return writeDB, nil
}

// TableCacheItems 返回缓存管理页使用的通用表缓存目标列表。
func TableCacheItems(base *corelogic.BaseLogic) []types.CacheItem {
	targets := tableCacheTargets(base)
	items := make([]types.CacheItem, 0, len(targets))
	for _, target := range targets {
		keyTitle := target.KeyTitle
		if keyTitle == "" {
			keyTitle = target.Key
		}
		keyTitle = TableCachePhysicalKey(base, keyTitle)
		logicalKeyTitle := TableCacheLogicalKey(base, keyTitle)
		items = append(items, types.CacheItem{
			Index:        target.Index,
			Key:          keyTitle,
			KeyTitle:     keyTitle,
			Type:         string(target.Type),
			Remark:       target.Remark,
			Category:     tableCacheCategory(target.Index, logicalKeyTitle),
			IsTemplate:   IsTemplateCachePattern(keyTitle),
			ExampleKey:   tableCacheExampleKey(keyTitle),
			AutoRebuild:  true,
			RefreshScope: tableCacheRefreshScope(target),
		})
	}
	return items
}

// tableCacheCategory 根据缓存目标索引和 key 模板归类缓存用途，供管理页分组展示。
func tableCacheCategory(index string, key string) string {
	switch {
	case strings.HasPrefix(key, "secret_key_"):
		return "secret"
	case strings.HasPrefix(key, "config_uuid:"), strings.HasPrefix(key, "runtime_config:"):
		return "config"
	case strings.HasPrefix(key, "admin_"), strings.HasPrefix(key, "route_permission_"), strings.Contains(index, "permission"), strings.Contains(index, "role"):
		return "auth"
	default:
		return "system"
	}
}

// tableCacheExampleKey 为模板型 key 生成管理页可直接使用的示例 key。
func tableCacheExampleKey(key string) string {
	replacer := strings.NewReplacer(
		"%d", "1",
		"%s", "demo",
		"%v", "value",
		"{adminID}", "1",
		"{roleID}", "1",
		"{uuid}", "demo",
		"{env}", "prod",
		"{releaseID}", "1",
		"{keyVersion}", "v1",
		"{routeAlias}", "admin.list",
	)
	return replacer.Replace(key)
}

// tableCacheRefreshScope 返回缓存目标刷新粒度描述。
func tableCacheRefreshScope(target tablecache.Target) string {
	switch {
	case target.RefreshAll:
		return "all"
	case IsTemplateCachePattern(target.KeyTitle):
		return "single"
	case strings.HasSuffix(target.Key, ":"):
		return "prefix"
	default:
		return "single"
	}
}

// IsTemplateCachePattern 判断缓存管理页中的 key 是否属于模板型缓存键。
// 这里同时支持 `{id}` 与 `%s/%d/%v` 两类占位写法，保证前后端识别规则一致。
func IsTemplateCachePattern(key string) bool {
	return strings.Contains(key, "{") || strings.Contains(key, "%")
}

// CacheTemplatePrefix 返回模板型缓存键的固定前缀部分。
// 管理页在匹配真实 Redis Key 与模板缓存项时，会基于该前缀做快速归类。
func CacheTemplatePrefix(key string) string {
	return keys.KeyTemplatePrefix(key)
}

// tableCacheFirstStringPart 读取前缀型缓存 key 的第一个参数。
func tableCacheFirstStringPart(params tablecache.LoadParams, title string) (string, error) {
	return tableCacheStringPart(params, 0, title)
}

// tableCacheStringPart 读取前缀型缓存 key 的指定字符串参数。
func tableCacheStringPart(params tablecache.LoadParams, index int, title string) (string, error) {
	if index < 0 || len(params.KeyParts) <= index || strings.TrimSpace(params.KeyParts[index]) == "" {
		return "", errors.Errorf("%s不能为空", title)
	}
	return strings.TrimSpace(params.KeyParts[index]), nil
}

// tableCacheFirstIntPart 读取前缀型缓存 key 的第一个整数参数。
func tableCacheFirstIntPart(params tablecache.LoadParams, title string) (int, error) {
	value, err := tableCacheFirstStringPart(params, title)
	if err != nil {
		return 0, errors.Tag(err)
	}
	id, err := strconv.Atoi(value)
	if err != nil || id <= 0 {
		return 0, errors.Errorf("%s不合法: %s", title, value)
	}
	return id, nil
}
