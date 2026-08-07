package svc

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"admin/internal/audit"
	"admin/internal/config"
	"admin/internal/infra/cdcx"
	"admin/internal/infra/collectorx"
	"admin/internal/infra/ipregion"
	"admin/internal/infra/kafkax"
	"admin/internal/infra/redislimit"
	"admin/internal/requestctx"

	utils "github.com/Is999/go-utils"
	"github.com/Is999/go-utils/errors"
	tablecache "github.com/Is999/table-cache"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// SiteDatabases 保存主库和可选命名扩展库连接。
type SiteDatabases struct {
	MainDB   *gorm.DB            // 默认主库连接
	NamedDBs map[DBName]*gorm.DB // 可选扩展库连接，按 site_mysql.<name> 注册
}

// Dependencies 表示 ServiceContext 运行所需的外部依赖集合。
// 该结构只承载已经初始化完成的资源引用，初始化顺序、失败回滚和关闭策略仍由 bootstrap 生命周期管理。
type Dependencies struct {
	SiteDBs           SiteDatabases         // 主库与可选扩展库连接集合
	Kafka             *kafkax.Producer      // Kafka 生产者，用户标签事件同步使用
	Rds               redis.UniversalClient // Redis 客户端，频控、锁和任务队列等链路复用
	RedisLimiter      *redislimit.Limiter   // 可选的进程级 Redis 分布式限流器，未传入时按 Rds 自动创建
	Audit             *audit.Recorder       // 审计日志记录器，后台敏感操作统一落审计
	IPRegion          *ipregion.Locator     // 本地 IP 归属地查询器
	SnowflakeLease    SnowflakeLease        // 雪花 node_id Redis 租约
	TableCacheMetrics tablecache.Metrics    // 表缓存运行指标记录器
}

// SnowflakeLease 约束雪花 node_id 租约的关闭能力。
type SnowflakeLease interface {
	Ready(context.Context) error
	Close(context.Context) error
}

// ServiceContext 将外部依赖集中管理：
// - SiteDBs: 主库与可选扩展库连接集合
// - Rds: Redis（频控、计数、最后发言时间等）
type ServiceContext struct {
	configValue       atomic.Value          // 当前生效的配置快照，供运行期按原子方式读取
	reloadValue       atomic.Value          // 配置热加载运行状态快照，供管理接口和日志复用
	storageValue      atomic.Value          // 文件存储运行时缓存，保存 *StorageRuntime
	uploadValue       atomic.Value          // 文件上传运行时缓存，保存 *FileTransferRuntime
	cacheSyncPending  *atomic.Bool          // 是否存在尚未完成的安全缓存失效任务
	background        *backgroundTasks      // 请求完成后的短后台任务集合，停机时先等待再关闭数据库等资源
	SiteDBs           SiteDatabases         // 主库与可选扩展库连接集合
	Kafka             *kafkax.Producer      // Kafka 生产者，未启用时为空
	Rds               redis.UniversalClient // Redis 客户端（兼容单机/集群）
	RedisLimiter      *redislimit.Limiter   // Redis 分布式限流器，同 key 跨实例共享、不同 key 相互隔离
	Audit             *audit.Recorder       // 审计日志记录器
	IPRegion          *ipregion.Locator     // 本地 IP 归属地查询器
	SnowflakeLease    SnowflakeLease        // 雪花 node_id Redis 租约
	TableCacheMetrics tablecache.Metrics    // 表缓存运行指标记录器
	TrustedProxies    *utils.TrustedProxies // 启动期解析完成的可信反向代理白名单
	Task              TaskQueue             // 任务系统接口（支持调度、DAG、队列管理）
	RuntimeAlerter    TaskRuntimeAlerter    // 独立运行告警入口，不依赖任务系统开关
	ConfigReload      ConfigReloadExecutor  // 配置热加载执行器，供管理接口手动触发重载
	Collector         *collectorx.Manager   // 通用收集器（Kafka 正常链路与失败账本重试）
	CDC               CDCConsumer           // CDC 消费器状态接口，未启用时为空
}

// CDCConsumer 约束 CDC 消费器对业务层暴露的最小能力。
type CDCConsumer interface {
	Ready(ctx context.Context, requireWorker bool) error
	Snapshot() cdcx.ConsumerStatus
	RegisteredTables() []string
}

// ConfigReloadExecutor 约束配置重载执行能力，避免 logic 层直接依赖 bootstrap 实现。
type ConfigReloadExecutor interface {
	ReloadConfig(ctx context.Context, source string) error
	ReloadRuntimeConfig(ctx context.Context, source string) (RuntimeConfigReloadResult, error)
}

// RuntimeConfigReloadResult 描述一次 DB 运行配置快照重载结果。
type RuntimeConfigReloadResult struct {
	ReleaseID       uint64 // 生效发布 ID
	VersionNo       uint64 // 生效版本号
	Checksum        string // 生效快照 SHA256
	RestartRequired bool   // 是否仍需重启进程才能完全生效
	RestartReason   string // 需要重启的原因
}

// HotReloadStatus 描述 config.yaml 热加载的当前运行状态。
// 该结构只记录监听与加载结果，不承诺基础设施连接已在线重建。
type HotReloadStatus struct {
	Enabled                bool      // 是否启用热加载
	Watching               bool      // 当前是否已启动后台监听
	ConfigFile             string    // 当前监听的配置文件路径
	CheckIntervalSeconds   int       // 当前轮询间隔，单位秒
	ConfigVersion          string    // 当前生效配置版本指纹
	ConfigSummary          string    // 当前配置摘要，便于快速确认关键开关是否已生效
	RestartRequired        bool      // 本次热加载后是否存在“需重启进程才能完全生效”的配置变更
	RestartReason          string    // 需要重启才能完全生效的原因摘要
	LastStatus             string    // 最近一次处理结果：idle/success/failed
	LastMessage            string    // 最近一次处理结果说明
	LastMessageKey         string    // 最近一次前端展示文案的多语言 key
	LastTriggerSource      string    // 最近一次触发来源：watcher/manual_api/startup 等
	LastFailureCategory    string    // 最近一次失败分类：fingerprint/load/reload/not_bound 等
	LastCheckedAt          time.Time // 最近一次检查配置文件时间
	LastReloadAt           time.Time // 最近一次触发配置重载时间
	LastSuccessAt          time.Time // 最近一次成功加载时间
	LastFailureAt          time.Time // 最近一次失败时间
	ReloadCount            int64     // 累计成功加载次数
	SuppressedFailureCount int64     // 限频压制的重复失败日志次数
}

// NewServiceContext 接收已初始化的外部依赖，并按 Redis 派生无独立生命周期的轻量限流组件。
func NewServiceContext(c config.Config, deps Dependencies) *ServiceContext {
	trustedProxies, _ := utils.NewTrustedProxies(c.TrustedProxies...)
	if len(c.TrustedProxies) == 0 {
		trustedProxies = nil
	}
	svcCtx := &ServiceContext{
		SiteDBs:           deps.SiteDBs,
		Kafka:             deps.Kafka,
		Rds:               deps.Rds,
		RedisLimiter:      deps.RedisLimiter,
		Audit:             deps.Audit,
		IPRegion:          deps.IPRegion,
		SnowflakeLease:    deps.SnowflakeLease,
		TableCacheMetrics: deps.TableCacheMetrics,
		TrustedProxies:    trustedProxies,
		cacheSyncPending:  &atomic.Bool{},
		background:        newBackgroundTasks(),
	}
	if svcCtx.RedisLimiter == nil && deps.Rds != nil {
		svcCtx.RedisLimiter = redislimit.New(deps.Rds)
	}
	svcCtx.UpdateConfig(c)
	svcCtx.UpdateHotReloadStatus(HotReloadStatus{LastStatus: "idle"})
	svcCtx.storageValue.Store(NewStorageRuntime())
	svcCtx.uploadValue.Store(NewFileTransferRuntime())
	return svcCtx
}

// ScopedWithContext 基于当前 ServiceContext 构造一份绑定请求上下文的只读作用域副本。
// 这里只复制当前快照，不发布 runtimecfg，避免请求作用域覆盖进程级运行配置。
func (s *ServiceContext) ScopedWithContext(ctx context.Context) *ServiceContext {
	if s == nil {
		return nil
	}
	scoped := &ServiceContext{
		SiteDBs:           s.SiteDBs.WithContext(ctx),
		Kafka:             s.Kafka,
		Rds:               s.Rds,
		RedisLimiter:      s.RedisLimiter,
		Audit:             s.Audit,
		IPRegion:          s.IPRegion,
		SnowflakeLease:    s.SnowflakeLease,
		TableCacheMetrics: s.TableCacheMetrics,
		TrustedProxies:    s.TrustedProxies,
		cacheSyncPending:  s.cacheSyncPending,
		background:        s.background,
	}
	scoped.configValue.Store(s.CurrentConfig())
	scoped.Task = s.Task
	scoped.RuntimeAlerter = s.RuntimeAlerter
	scoped.ConfigReload = s.ConfigReload
	scoped.Collector = s.Collector
	scoped.CDC = s.CDC
	if storageRuntime, ok := s.storageValue.Load().(*StorageRuntime); ok && storageRuntime != nil {
		scoped.storageValue.Store(storageRuntime)
	}
	if uploadRuntime, ok := s.uploadValue.Load().(*FileTransferRuntime); ok && uploadRuntime != nil {
		scoped.uploadValue.Store(uploadRuntime)
	}
	scoped.UpdateHotReloadStatus(s.CurrentHotReloadStatus())
	return scoped
}

// GoBackground 登记请求完成后继续执行的短后台任务；停机开始后返回 false。
func (s *ServiceContext) GoBackground(task func()) bool {
	return s != nil && s.background != nil && s.background.Go(task)
}

// StopBackground 停止接收短后台任务并等待已登记任务完成。
func (s *ServiceContext) StopBackground(ctx context.Context) error {
	if s == nil || s.background == nil {
		return nil
	}
	return errors.Tag(s.background.Stop(ctx))
}

// SetSecurityCacheSyncPending 更新安全缓存失效任务的进程内阻断状态。
func (s *ServiceContext) SetSecurityCacheSyncPending(pending bool) {
	if s == nil || s.cacheSyncPending == nil {
		return
	}
	s.cacheSyncPending.Store(pending)
}

// SecurityCacheSyncPending 返回当前进程是否正在等待安全缓存失效补偿。
func (s *ServiceContext) SecurityCacheSyncPending() bool {
	return s != nil && s.cacheSyncPending != nil && s.cacheSyncPending.Load()
}

// ClientIP 仅在远端命中显式可信代理时解析转发头；空配置保留只信任回环代理的安全默认值。
func (s *ServiceContext) ClientIP(r *http.Request) string {
	if s != nil && s.TrustedProxies != nil {
		return requestctx.NormalizeClientIP(utils.ClientIPWithTrustedProxies(r, s.TrustedProxies))
	}
	return requestctx.NormalizeClientIP(utils.ClientIP(r))
}

// CurrentConfig 返回当前生效的配置快照，供运行期读取最新配置。
func (s *ServiceContext) CurrentConfig() config.Config {
	if s == nil {
		return config.Config{}
	}
	if cfg, ok := s.configValue.Load().(config.Config); ok {
		return cfg
	}
	return config.Config{}
}

// UpdateConfig 原子替换运行期配置快照。
func (s *ServiceContext) UpdateConfig(c config.Config) {
	if s == nil {
		return
	}
	s.configValue.Store(c)
}

// CurrentHotReloadStatus 返回当前热加载状态快照。
func (s *ServiceContext) CurrentHotReloadStatus() HotReloadStatus {
	if s == nil {
		return HotReloadStatus{}
	}
	if status, ok := s.reloadValue.Load().(HotReloadStatus); ok {
		return status
	}
	return HotReloadStatus{}
}

// UpdateHotReloadStatus 原子替换热加载状态快照。
func (s *ServiceContext) UpdateHotReloadStatus(status HotReloadStatus) {
	if s == nil {
		return
	}
	s.reloadValue.Store(status)
}

// Lookup 根据数据库名称返回连接，空名称和 main 都指向主库。
func (s SiteDatabases) Lookup(database DBName) *gorm.DB {
	name := NormalizeDBName(database)
	if name == DatabaseMain {
		return s.MainDB
	}
	if s.NamedDBs == nil {
		return nil
	}
	return s.NamedDBs[name]
}

// WithContext 为所有站点库连接绑定请求上下文，方便日志和 trace 贯穿到 GORM。
func (s SiteDatabases) WithContext(ctx context.Context) SiteDatabases {
	s.MainDB = withDBContext(s.MainDB, ctx)
	if len(s.NamedDBs) > 0 {
		namedDBs := make(map[DBName]*gorm.DB, len(s.NamedDBs))
		for name, db := range s.NamedDBs {
			namedDBs[name] = withDBContext(db, ctx)
		}
		s.NamedDBs = namedDBs
	}
	return s
}

// withDBContext 对单个 GORM 连接绑定上下文，空连接保持为空。
func withDBContext(db *gorm.DB, ctx context.Context) *gorm.DB {
	if db == nil {
		return nil
	}
	return db.WithContext(ctx)
}
