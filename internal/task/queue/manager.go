package taskqueue

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Is999/go-utils/errors"

	keys "admin/common/rediskeys"
	"admin/common/runtimecfg"
	"admin/helper"
	"admin/internal/config"
	"admin/internal/infra/loggerx"
	"admin/internal/requestctx"
	"admin/internal/svc"
	tasklimits "admin/internal/task/limits"
	"admin/internal/task/stats"
	"admin/internal/task/taskwire"
	"admin/internal/types"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// inspectorContextRedis 把 Asynq Inspector 内部的后台 context 收口到本轮调用 context。
type inspectorContextRedis struct {
	redis.UniversalClient                 // 底层任务 Redis 客户端
	ctx                   context.Context // 本轮调用的超时与取消信号
}

// SIsMember 让 Inspector 的队列存在性检查响应调用方取消。
func (c *inspectorContextRedis) SIsMember(_ context.Context, key string, member interface{}) *redis.BoolCmd {
	return c.UniversalClient.SIsMember(c.ctx, key, member)
}

// EvalSha 让已缓存的 Asynq 详情脚本响应调用方取消。
func (c *inspectorContextRedis) EvalSha(_ context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	return c.UniversalClient.EvalSha(c.ctx, sha1, keys, args...)
}

// Eval 让 Asynq 详情脚本的冷缓存回退响应调用方取消。
func (c *inspectorContextRedis) Eval(_ context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	return c.UniversalClient.Eval(c.ctx, script, keys, args...)
}

// newContextInspector 创建响应调用方取消的 Asynq Inspector。
func (m *Manager) newContextInspector(ctx context.Context) *asynq.Inspector {
	return asynq.NewInspectorFromRedisClient(&inspectorContextRedis{UniversalClient: m.redis, ctx: ctx})
}

const (
	// QueueCritical 高优先级业务队列。
	QueueCritical = taskwire.QueueCritical
	// QueueDefault 默认业务队列。
	QueueDefault = taskwire.QueueDefault
	// QueueMaintenance 运维/缓存刷新等后台队列。
	QueueMaintenance = taskwire.QueueMaintenance

	// TypeWorkflowTrigger 是触发工作流实例的入口任务。
	TypeWorkflowTrigger = taskwire.TypeWorkflowTrigger
	// TypeWorkflowNoop 是 DAG 中用于收尾/汇聚的空任务。
	TypeWorkflowNoop = taskwire.TypeWorkflowNoop
	// TypeCacheRefreshRequest 是请求聚合缓存刷新的原始任务。
	TypeCacheRefreshRequest = "cache:refresh:request"
	// TypeCacheRefreshBatch 是聚合后的批量缓存刷新任务。
	TypeCacheRefreshBatch = "cache:refresh:batch"

	// GroupCacheRefresh 是缓存刷新任务的聚合分组。
	GroupCacheRefresh = "cache-refresh"

	headerLocale        = "x-app-locale"                  // 任务透传的语言标识
	headerUserID        = "x-app-user-id"                 // 任务透传的操作者 ID
	headerUserName      = "x-app-user-name"               // 任务透传的操作者名称
	headerTaskName      = "x-app-task-name"               // 任务展示名称
	headerTaskSource    = taskwire.HeaderTaskSource       // 任务触发来源
	HeaderTaskSource    = headerTaskSource                // 任务触发来源头，供业务报表按来源筛选任务
	HeaderPeriodicName  = taskwire.HeaderPeriodicName     // 周期任务原始名称
	HeaderScheduledAt   = taskwire.HeaderScheduledAt      // 周期任务本轮计划触发时间
	headerWorkflowID    = taskwire.HeaderWorkflowID       // 工作流实例 ID
	HeaderWorkflowID    = headerWorkflowID                // 工作流实例 ID 头，供业务报表关联工作流状态
	headerWorkflowName  = taskwire.HeaderWorkflowName     // 工作流名称
	HeaderWorkflowName  = headerWorkflowName              // 工作流名称头，供业务聚合任务反查工作流归属
	headerWorkflowNode  = taskwire.HeaderWorkflowNode     // 工作流节点名称
	HeaderWorkflowNode  = headerWorkflowNode              // 工作流节点名称头，供业务报表展示节点维度
	headerShardIndex    = "x-app-workflow-shard-index"    // 工作流分片序号
	headerShardTotal    = "x-app-workflow-shard-total"    // 工作流分片总数
	headerBusinessRetry = "x-app-workflow-business-retry" // 工作流分片允许的业务重试次数
	headerAppID         = "x-app-id"                      // 当前应用 app_id

	taskSearchFieldTaskName     = "taskName"     // 任务结果或 payload 中的展示名称字段
	taskSearchFieldTaskType     = "taskType"     // 任务结果中回写的任务类型字段
	taskSearchFieldWorkflowID   = "workflowId"   // 工作流实例 ID 字段，用于 payload/result 链路匹配
	taskSearchFieldWorkflowName = "workflowName" // 工作流名称字段，周期任务排查常用该字段定位入口任务
	taskSearchFieldWorkflowNode = "workflowNode" // 工作流节点字段，用于节点任务按脚本名检索
	taskSearchFieldTaskSource   = "taskSource"   // 任务来源字段，用于区分 api/periodic/internal 等触发来源
	taskSearchFieldMode         = "mode"         // 用户标签等任务的运行模式字段，用于保留 full/delta 等排查入口
	taskSearchFieldUniqueKey    = "uniqueKey"    // 工作流或任务幂等键字段，便于按周期 unique_key 反查任务

	taskExecutionStatusSuccess = "success" // 普通任务执行结果成功状态

	taskFinalWriteTimeout  = 5 * time.Second                                 // 任务收尾写 Redis 的短超时，避免业务 ctx 超时后失败状态无法落库
	taskListFilterPageSize = 100                                             // 任务列表二次过滤单批读取量，和接口最大页大小保持一致
	taskListFilterMaxPages = 50                                              // 任务列表二次过滤最大扫描页数，避免无索引历史查询拖垮 Redis
	taskListFilterMaxRows  = taskListFilterPageSize * taskListFilterMaxPages // 任务列表二次过滤最大扫描条数
	taskRuntimeAlertTTL    = 5 * time.Minute                                 // 任务系统同一运行异常的告警限频窗口，避免后台循环刷屏
	periodicConfigAlertTTL = taskRuntimeAlertTTL                             // 周期任务同一配置异常复用任务运行异常的告警限频窗口
	workerHealthInterval   = 5 * time.Second                                 // Worker 内部 Redis 健康检查间隔
	workerHeartbeatMaxAge  = 3 * workerHealthInterval                        // Worker 心跳最大允许间隔
)

// WorkflowTriggerPayload 是 workflow:trigger 任务的负载。
type WorkflowTriggerPayload struct {
	WorkflowID        string   `json:"workflowId"`                  // 工作流实例 ID
	WorkflowName      string   `json:"workflowName"`                // 工作流定义名称
	Queue             string   `json:"queue,omitempty"`             // 投递队列，留空时回退默认队列
	Targets           []string `json:"targets,omitempty"`           // 本次工作流要处理的目标集合
	ShardTotal        int      `json:"shardTotal,omitempty"`        // 分片总数
	GrayPercent       int      `json:"grayPercent,omitempty"`       // 灰度执行百分比
	Source            string   `json:"source,omitempty"`            // 触发来源
	TriggeredByUserID int      `json:"triggeredByUserId,omitempty"` // 触发人用户 ID
	TriggeredByUser   string   `json:"triggeredByUser,omitempty"`   // 触发人用户名
	PeriodicName      string   `json:"periodicName,omitempty"`      // 周期任务原始名称
	ScheduledAt       string   `json:"scheduledAt,omitempty"`       // 本轮计划触发时间，仅供按计划时刻隔离周期入口去重
	Retry             *int     `json:"retry,omitempty"`             // 重试次数覆盖值
	TimeoutSeconds    int      `json:"timeoutSeconds,omitempty"`    // 超时时间，单位秒
	UniqueKey         string   `json:"uniqueKey,omitempty"`         // 幂等键
	UniqueTTLSeconds  int      `json:"uniqueTTLSeconds,omitempty"`  // 幂等锁有效期，单位秒
}

// CacheRefreshPayload 是缓存刷新任务的负载。
type CacheRefreshPayload struct {
	WorkflowTaskMeta          // 关联的工作流节点元数据，非工作流触发时可为空
	Operation        string   `json:"operation,omitempty"` // 刷新动作说明，便于日志排查
	Targets          []string `json:"targets"`             // 本次需要刷新的缓存目标列表
}

// NoopPayload 是汇聚/收尾节点使用的空任务负载。
type NoopPayload struct {
	WorkflowTaskMeta        // 关联的工作流节点元数据
	Note             string `json:"note,omitempty"` // 备注信息，用于标识空节点用途
}

// TaskExecutionResult 表示任务执行成功后写回到 Asynq 的结果摘要。
// 管理后台的“任务结果”优先展示该结构，便于直接看到本次执行的关键产物。
type TaskExecutionResult struct {
	Status         string              `json:"status"`                   // 执行状态，当前统一写 success
	TraceID        string              `json:"traceId,omitempty"`        // 链路追踪 ID
	TaskID         string              `json:"taskId,omitempty"`         // 任务 ID
	TaskType       string              `json:"taskType,omitempty"`       // 任务类型
	TaskName       string              `json:"taskName,omitempty"`       // 任务名称/脚本名称
	TaskSource     string              `json:"taskSource,omitempty"`     // 任务来源（如 api/periodic/internal）
	TaskQueue      string              `json:"taskQueue,omitempty"`      // 所属队列
	Queue          string              `json:"queue,omitempty"`          // payload 中声明的业务执行队列
	LatencyMS      int64               `json:"latencyMs,omitempty"`      // 执行耗时（毫秒）
	BizCode        int                 `json:"bizCode,omitempty"`        // 统一业务状态码
	BizMessage     string              `json:"bizMessage,omitempty"`     // 统一业务结果说明
	WorkflowID     string              `json:"workflowId,omitempty"`     // 工作流实例 ID
	WorkflowName   string              `json:"workflowName,omitempty"`   // 工作流名称
	WorkflowNode   string              `json:"workflowNode,omitempty"`   // 工作流节点名称
	ShardIndex     int                 `json:"shardIndex,omitempty"`     // 分片索引
	ShardTotal     int                 `json:"shardTotal,omitempty"`     // 分片总数
	GrayPercent    int                 `json:"grayPercent,omitempty"`    // 灰度执行百分比
	Operation      string              `json:"operation,omitempty"`      // 业务动作，如缓存刷新操作名
	Mode           string              `json:"mode,omitempty"`           // 业务模式，如 full/delta/targeted
	TargetCount    int                 `json:"targetCount,omitempty"`    // 目标数量
	UIDCount       int                 `json:"uidCount,omitempty"`       // UID 数量
	TagTypeCount   int                 `json:"tagTypeCount,omitempty"`   // 标签类型数量
	Targets        []string            `json:"targets,omitempty"`        // 目标预览，避免结果过长仅保留前几项
	PayloadKeys    []string            `json:"payloadKeys,omitempty"`    // payload 顶层字段摘要，便于快速判断本次入参结构
	StartedAt      string              `json:"startedAt,omitempty"`      // 开始执行时间
	FinishedAt     string              `json:"finishedAt,omitempty"`     // 结果写回时间
	ExecutionTrace *taskstats.Snapshot `json:"executionTrace,omitempty"` // 本次任务处理量统计摘要
}

// GroupAggregator 定义单个聚合分组的批处理器。
// 同一个 group 内的多条任务会在 worker 聚合窗口结束后交给对应聚合器合并成一个批任务。
type GroupAggregator func(tasks []*asynq.Task) *asynq.Task

// TaskFinalFailureHook 定义任务进入终态失败后的业务清理钩子。
// 钩子只在任务无自动重试机会后触发，用于释放业务侧资源。
type TaskFinalFailureHook func(ctx context.Context, task *asynq.Task, meta WorkflowTaskMeta, err error) error

// PeriodicConfigInvalidHook 定义周期任务配置异常后的告警钩子。
// 钩子只用于外部通知，不改变调度器跳过无效配置的容错语义。
type PeriodicConfigInvalidHook func(ctx context.Context, report PeriodicConfigInvalidReport) error

// PeriodicConfigInvalidReport 描述一次周期任务配置校验失败。
type PeriodicConfigInvalidReport struct {
	Index        int                       // 配置在合并后周期任务列表中的位置
	Task         config.TaskPeriodicConfig // 失败的周期任务配置
	Reason       string                    // 配置失败原因
	OccurredAt   time.Time                 // 发现异常的时间
	TriggerCount int                       // 当前告警窗口累计触发次数，包含本次触发
}

// TaskRuntimeAlertHook 定义任务系统运行异常后的告警钩子。
// 钩子只用于外部通知，不改变调度器和清理任务的容错语义。
type TaskRuntimeAlertHook func(ctx context.Context, alert TaskRuntimeAlert) error

// TaskRuntimeAlert 描述一次任务系统运行期异常。
type TaskRuntimeAlert struct {
	Kind         string    // 异常类型，用于告警指纹和排障归类
	Title        string    // 告警标题
	Status       string    // 当前处理状态
	Component    string    // 发生异常的任务系统组件
	Operation    string    // 发生异常的运行操作
	TaskName     string    // 关联任务名称
	TaskType     string    // 关联任务类型
	WorkflowName string    // 关联工作流名称
	Cron         string    // 关联周期表达式
	TaskQueue    string    // 关联队列
	UniqueKey    string    // 关联幂等键
	Reason       string    // 异常原因摘要
	Advice       string    // 处理建议，留空时使用告警发送端通用建议
	OccurredAt   time.Time // 发现异常的时间
	TriggerCount int       // 当前告警窗口累计触发次数，包含本次触发
}

// periodicConfigAlertState 保存单条周期配置异常的限频状态。
type periodicConfigAlertState struct {
	LastSentAt      time.Time // 最近一次发送告警的时间
	SuppressedCount int       // 限频窗口内被合并的重复触发次数
}

// taskRuntimeAlertState 保存单条运行异常的限频状态。
type taskRuntimeAlertState struct {
	LastSentAt      time.Time // 最近一次发送告警的时间
	SuppressedCount int       // 限频窗口内被合并的重复触发次数
}

// workerHealth 描述本地 Asynq Worker 的实际运行和心跳状态。
type workerHealth struct {
	running       bool      // Worker 是否已成功启动且未进入停止流程
	lastHeartbeat time.Time // 最近一次启动确认或内部健康检查时间
	lastError     string    // 最近一次内部健康检查错误
}

// taskEnqueuer 定义任务投递能力，便于对分片部分投递故障做确定性测试。
type taskEnqueuer interface {
	EnqueueContext(context.Context, *asynq.Task, ...asynq.Option) (*asynq.TaskInfo, error)
}

// Manager 统一承接任务客户端、Worker、调度器和工作流状态管理。
// 该层只依赖 Redis 与 Asynq，不直接依赖具体业务逻辑。
type Manager struct {
	cfgValue             atomic.Value               // 任务系统配置快照，支持运行期热更新读取
	schedulerStatusValue atomic.Value               // 调度器运行状态快照，供接口与日志复用
	redis                redis.UniversalClient      // 任务系统使用的 Redis 客户端
	client               taskEnqueuer               // Asynq 任务投递客户端
	inspector            *asynq.Inspector           // Asynq 队列巡检客户端
	runInspectorTask     func(string, string) error // 立即执行巡检任务，保留故障注入边界验证重跑幂等
	mux                  *asynq.ServeMux            // Worker 处理器路由复用器
	server               *asynq.Server              // Worker 服务实例
	leader               *LeaderRunner              // Scheduler 的分布式 leader 选举器
	cancel               context.CancelFunc         // 用于停止调度器 leader 循环
	wg                   sync.WaitGroup             // 等待后台调度协程退出
	logger               asynq.Logger               // Asynq 运行日志适配器
	tracer               trace.Tracer               // 任务链路追踪器
	instance             string                     // 当前进程实例 ID，用于 leader 日志和区分多实例

	lifecycleMu              sync.Mutex                          // 保护 Worker / Scheduler 启停状态，避免并发启动或停止产生竞态
	workerHealthMu           sync.RWMutex                        // 保护 Worker 心跳状态
	workerHealth             workerHealth                        // Worker 运行和内部健康检查快照
	schedulerStatusMu        sync.Mutex                          // 保护调度器状态快照更新，避免多个后台协程并发覆盖
	archivedCleanStop        context.CancelFunc                  // 停止归档失败任务过期清理协程
	mu                       sync.RWMutex                        // 保护工作流定义和周期任务配置的并发读写
	workflows                map[string]*WorkflowDefinition      // 已注册的工作流定义集合
	periodic                 []config.TaskPeriodicConfig         // 通过配置或插件补充的周期任务列表
	handlers                 map[string]struct{}                 // 已注册的任务类型集合，用于限制通用任务投递
	aggregates               map[string]GroupAggregator          // 已注册的分组聚合器
	finalFailureHooks        []TaskFinalFailureHook              // 任务终态失败后的业务清理钩子
	periodicConfigHooks      []PeriodicConfigInvalidHook         // 周期任务配置异常后的外部告警钩子
	runtimeAlertHooks        []TaskRuntimeAlertHook              // 任务系统运行异常后的外部告警钩子
	periodicConfigAlertedMap map[string]periodicConfigAlertState // 周期任务配置异常限频记录
	runtimeAlertedMap        map[string]taskRuntimeAlertState    // 任务系统运行异常限频记录
	historySink              HistorySink                         // 终态历史 DB 存储边界
	historyStop              context.CancelFunc                  // 停止终态历史异步收集器
}

// New 创建任务管理器。
func New(cfg config.TaskQueueConfig, client redis.UniversalClient) *Manager {
	if !cfg.Enabled || client == nil {
		return nil
	}
	if strings.TrimSpace(cfg.AppID) == "" {
		return nil
	}
	m := &Manager{
		redis:                    client,
		client:                   asynq.NewClientFromRedisClient(client),
		inspector:                asynq.NewInspectorFromRedisClient(client),
		mux:                      asynq.NewServeMux(),
		logger:                   asynqLogger{},
		tracer:                   otel.Tracer("admin/task"),
		instance:                 uuid.NewString(),
		workflows:                make(map[string]*WorkflowDefinition),
		periodic:                 make([]config.TaskPeriodicConfig, 0),
		handlers:                 make(map[string]struct{}),
		aggregates:               make(map[string]GroupAggregator),
		periodicConfigAlertedMap: make(map[string]periodicConfigAlertState),
		runtimeAlertedMap:        make(map[string]taskRuntimeAlertState),
	}
	m.runInspectorTask = m.inspector.RunTask
	m.UpdateConfig(cfg)
	m.Use(m.traceAndLogMiddleware())
	return m
}

// appNamespace 返回当前任务系统实例的站点命名空间。
// 多站点共用一套 task redis 时，所有队列名、锁 key、工作流 key 都必须带站点前缀，避免互相串扰。
func (m *Manager) appNamespace() string {
	if m == nil {
		return ""
	}
	appID := strings.TrimSpace(m.CurrentConfig().AppID)
	if appID == "" {
		return ""
	}
	return appID
}

// useRedisNamespace 将当前任务管理器配置的 app_id 绑定到 Redis key helper。
func (m *Manager) useRedisNamespace() bool {
	appID := m.appNamespace()
	return appID != "" && appID == runtimecfg.AppID() && keys.Prefix() != ""
}

// namespacedQueueName 返回底层 Asynq 实际使用的队列名。
// 对外接口仍暴露逻辑队列名，内部统一追加站点前缀，避免多站点共用同一套 task redis 时互相消费任务。
func (m *Manager) namespacedQueueName(queue string) string {
	queue = strings.TrimSpace(queue)
	if queue == "" {
		return ""
	}
	if !m.useRedisNamespace() {
		return ""
	}
	return keys.TaskQueueName(queue)
}

// displayQueueName 把底层带站点前缀的队列名还原成对外展示的逻辑队列名。
func (m *Manager) displayQueueName(queue string) string {
	queue = strings.TrimSpace(queue)
	if queue == "" {
		return ""
	}
	return keys.TrimTaskQueueName(queue)
}

// namespacedGroup 返回当前站点下聚合任务使用的分组名。
func (m *Manager) namespacedGroup(group string) string {
	group = strings.TrimSpace(group)
	if group == "" {
		return ""
	}
	if !m.useRedisNamespace() {
		return ""
	}
	return keys.TaskQueueName(group)
}

// displayGroupName 把底层聚合分组名还原成对外展示名称。
func (m *Manager) displayGroupName(group string) string {
	group = strings.TrimSpace(group)
	if group == "" {
		return ""
	}
	return keys.TrimTaskQueueName(group)
}

// CurrentConfig 返回当前生效的任务系统配置快照。
func (m *Manager) CurrentConfig() config.TaskQueueConfig {
	if m == nil {
		return config.TaskQueueConfig{}
	}
	if cfg, ok := m.cfgValue.Load().(config.TaskQueueConfig); ok {
		return cfg
	}
	return config.TaskQueueConfig{}
}

// RedisClient 返回任务队列使用的 Redis 客户端。
func (m *Manager) RedisClient() redis.UniversalClient {
	if m == nil {
		return nil
	}
	return m.redis
}

// UpdateConfig 原子替换任务系统配置快照。
// 该方法不会在线重建 Worker / Redis 连接，只影响后续按快照读取的配置项。
func (m *Manager) UpdateConfig(cfg config.TaskQueueConfig) {
	if m == nil {
		return
	}
	m.cfgValue.Store(cfg)
	m.syncSchedulerConfigStatus(cfg)
}

// IsEnabled 返回任务系统是否启用。
func (m *Manager) IsEnabled() bool {
	return m != nil && m.client != nil && m.redis != nil && m.useRedisNamespace()
}

// EnqueueTask 提供轻量级基础任务投递入口。
func (m *Manager) EnqueueTask(ctx context.Context, taskType string, payload []byte, opts ...svc.TaskOption) error {
	if !m.IsEnabled() {
		return ErrTaskQueueDisabled
	}

	// options 汇总调用方传入的函数式配置，后续统一交给 enqueueTaskWithOptions 转成 Asynq 选项。
	options := &svc.TaskOptions{}
	for _, opt := range opts {
		opt(options)
	}

	_, err := m.enqueueTaskWithOptions(ctx, m.newTask(ctx, taskType, payload, nil), options)
	return errors.Tag(err)
}

// RegisterHandler 注册任务处理器。
func (m *Manager) RegisterHandler(pattern string, handler asynq.Handler) (err error) {
	if m == nil || m.mux == nil {
		return nil
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return errors.Errorf("任务处理器 pattern 不能为空")
	}
	if handler == nil {
		return errors.Errorf("任务处理器不能为空: %s", pattern)
	}

	// 先维护本地注册表，避免重复注册穿透到 Asynq ServeMux 后 panic。
	m.mu.Lock()
	if m.handlers == nil {
		m.handlers = make(map[string]struct{})
	}
	if _, exists := m.handlers[pattern]; exists {
		m.mu.Unlock()
		return errors.Errorf("任务处理器已注册: %s", pattern)
	}
	m.handlers[pattern] = struct{}{}
	m.mu.Unlock()

	// Asynq ServeMux 遇到重复注册会 panic，这里统一转成错误返回并回滚注册表。
	defer func() {
		if recovered := recover(); recovered != nil {
			m.mu.Lock()
			delete(m.handlers, pattern)
			m.mu.Unlock()
			err = errors.Errorf("注册任务处理器失败: %s: %v", pattern, recovered)
		}
	}()
	m.mux.Handle(pattern, handler)
	allowTaskMetricTaskTypeLabel(pattern)
	return nil
}

// Use 为 Worker 注册任务处理中间件，供运行时或业务插件扩展链路能力。
func (m *Manager) Use(middlewares ...asynq.MiddlewareFunc) {
	if m == nil || m.mux == nil {
		return
	}
	for _, middleware := range middlewares {
		if middleware == nil {
			continue
		}
		m.mux.Use(middleware)
	}
}

// RegisterGroupAggregator 注册聚合分组处理器，供业务插件把多条短任务合并成批处理任务。
func (m *Manager) RegisterGroupAggregator(group string, aggregator GroupAggregator) error {
	if m == nil {
		return nil
	}
	group = m.namespacedGroup(group)
	if group == "" {
		return errors.Errorf("任务分组不能为空")
	}
	if aggregator == nil {
		return errors.Errorf("任务分组聚合器为空: %s", group)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.aggregates == nil {
		m.aggregates = make(map[string]GroupAggregator)
	}
	if _, exists := m.aggregates[group]; exists {
		return errors.Errorf("任务分组聚合器已注册: %s", group)
	}
	m.aggregates[group] = aggregator
	return nil
}

// RegisterFinalFailureHook 注册任务终态失败后的清理钩子。
// 钩子由业务插件在启动期注册，Manager 只负责在 Asynq 确认任务不会继续自动重试后按顺序触发。
func (m *Manager) RegisterFinalFailureHook(hook TaskFinalFailureHook) error {
	if m == nil || hook == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finalFailureHooks = append(m.finalFailureHooks, hook)
	return nil
}

// RegisterPeriodicConfigInvalidHook 注册周期任务配置异常告警钩子。
// 钩子会按配置指纹限频触发，避免调度器周期同步时重复推送同一条异常。
func (m *Manager) RegisterPeriodicConfigInvalidHook(hook PeriodicConfigInvalidHook) error {
	if m == nil || hook == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.periodicConfigHooks = append(m.periodicConfigHooks, hook)
	return nil
}

// RegisterRuntimeAlertHook 注册任务系统运行异常告警钩子。
// 钩子会按异常指纹限频触发，用于调度、投递和清理等外层运行错误通知。
func (m *Manager) RegisterRuntimeAlertHook(hook TaskRuntimeAlertHook) error {
	if m == nil || hook == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runtimeAlertHooks = append(m.runtimeAlertHooks, hook)
	return nil
}

// RegisterWorkflow 注册 DAG 工作流定义。
func (m *Manager) RegisterWorkflow(def *WorkflowDefinition) error {
	if m == nil || def == nil {
		return nil
	}
	if err := validateWorkflowDefinition(def); err != nil {
		return errors.Tag(err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.workflows == nil {
		m.workflows = make(map[string]*WorkflowDefinition)
	}
	if _, exists := m.workflows[def.Name]; exists {
		return errors.Errorf("工作流已注册: %s", def.Name)
	}
	m.workflows[def.Name] = def
	return nil
}

// ListRegisteredTaskTypes 返回当前进程已注册的任务类型清单。
func (m *Manager) ListRegisteredTaskTypes(ctx context.Context) []types.TaskTypeRegistryItem {
	if m == nil {
		return nil
	}
	locale := displayLocale(ctx)
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := make([]types.TaskTypeRegistryItem, 0, len(m.handlers))
	for taskType := range m.handlers {
		taskType = strings.TrimSpace(taskType)
		if taskType == "" {
			continue
		}
		items = append(items, buildTaskTypeRegistryItem(taskType, locale))
	}
	sort.Slice(items, func(left int, right int) bool {
		return items[left].TaskType < items[right].TaskType
	})
	return items
}

// ListRegisteredWorkflows 返回当前进程已注册的工作流定义快照。
func (m *Manager) ListRegisteredWorkflows(ctx context.Context) []types.WorkflowRegistryItem {
	if m == nil {
		return nil
	}
	locale := displayLocale(ctx)
	m.mu.RLock()
	defer m.mu.RUnlock()
	items := make([]types.WorkflowRegistryItem, 0, len(m.workflows))
	for _, def := range m.workflows {
		if def == nil {
			continue
		}
		name := strings.TrimSpace(def.Name)
		items = append(items, types.WorkflowRegistryItem{
			Name:           name,
			Description:    workflowDescription(name, def.Description, locale),
			DefaultQueue:   strings.TrimSpace(def.DefaultQueue),
			NodeCount:      len(def.Nodes),
			UsageHint:      workflowUsageHint(name, locale),
			TargetsExample: workflowTargetsExample(name),
		})
	}
	sort.Slice(items, func(left int, right int) bool {
		return items[left].Name < items[right].Name
	})
	return items
}

// buildTaskTypeRegistryItem 构造任务类型注册清单项，补充管理后台展示需要的说明和示例。
func buildTaskTypeRegistryItem(taskType string, locale string) types.TaskTypeRegistryItem {
	item := types.TaskTypeRegistryItem{
		TaskType:          taskType,
		Description:       taskTypeDescriptionForLocale(taskType, locale),
		UsageHint:         taskTypeUsageHint(taskType, locale),
		PayloadExample:    taskTypePayloadExample(taskType),
		ManualRecommended: taskTypeManualRecommended(taskType),
	}
	return item
}

// displayLocale 从请求上下文读取管理端展示语言；后台任务缺省使用中文。
func displayLocale(ctx context.Context) string {
	if meta := requestctx.FromContext(ctx); meta != nil {
		return meta.Locale
	}
	return ""
}

// writeTaskResult 把任务执行结果写回 Asynq，便于管理后台查看成功记录的结果摘要。
func (m *Manager) writeTaskResult(ctx context.Context, task *asynq.Task, meta *requestctx.Meta, startedAt time.Time, statsSnapshot *taskstats.Snapshot) bool {
	if task == nil || task.ResultWriter() == nil {
		return false
	}
	resultBytes, err := json.Marshal(buildTaskExecutionResult(task, meta, startedAt, boundedTaskStatsSnapshot(statsSnapshot)))
	if err != nil {
		loggerx.Errorw(ctx, "任务结果 序列化失败", err, taskLogFields(task)...)
		return false
	}
	if _, err = task.ResultWriter().Write(resultBytes); err != nil {
		loggerx.Errorw(ctx, "任务结果 回写失败", err, taskLogFields(task)...)
		return false
	}
	return true
}

// RegisterPeriodicTask 注册额外的周期工作流配置，供插件在启动期补充调度任务。
func (m *Manager) RegisterPeriodicTask(cfg config.TaskPeriodicConfig) error {
	if m == nil {
		return nil
	}
	key, err := periodicTaskKey(cfg)
	if err != nil {
		return errors.Tag(err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	currentCfg := m.CurrentConfig()
	for _, existing := range append(append([]config.TaskPeriodicConfig(nil), currentCfg.Periodic...), m.periodic...) {
		existingKey, keyErr := periodicTaskKey(existing)
		if keyErr == nil && existingKey == key {
			return errors.Errorf("周期任务已注册: %s", key)
		}
	}
	m.periodic = append(m.periodic, cfg)
	return nil
}

// StartWorkflow 供 Worker 处理入口任务时直接启动 DAG 实例。
func (m *Manager) StartWorkflow(ctx context.Context, spec WorkflowStartSpec) (string, error) {
	workflowID, err := m.startWorkflow(ctx, spec)
	if err == nil || errors.Is(err, ErrWorkflowAlreadyExists) || errors.Is(err, ErrTaskQueueDisabled) {
		return workflowID, err
	}
	return workflowID, errors.Join(ErrWorkflowDispatch, err)
}

// EnqueueWorkflowTrigger 通过统一入口任务触发一个工作流实例。
func (m *Manager) EnqueueWorkflowTrigger(ctx context.Context, req *types.TriggerTaskWorkflowReq) (*types.TaskWorkflowTriggerResp, error) {
	if !m.IsEnabled() {
		return nil, ErrTaskQueueDisabled
	}
	if req == nil {
		return nil, errors.Errorf("工作流请求不能为空")
	}
	if err := req.Validate(); err != nil {
		return nil, errors.Tag(err)
	}

	// 入参校验与工作流定义预检查（避免未知 workflow 入队后反复重试）。
	workflowName := strings.TrimSpace(req.Name)
	definition, err := m.workflowDefinition(workflowName)
	if err != nil {
		return nil, errors.Tag(err)
	}
	if err = validateWorkflowShardTotal(definition, req.ShardTotal); err != nil {
		return nil, errors.Tag(err)
	}
	queue := helper.FirstNonEmptyString(req.Queue, m.defaultWorkflowQueue())
	if !m.isQueueConfigured(queue) {
		return nil, errors.Errorf("工作流触发队列未配置消费: %s", queue)
	}

	// 幂等键预占：为延迟/定时触发预留唯一键窗口，避免并发重复触发。
	workflowID := newID()
	uniqueKey := strings.TrimSpace(req.UniqueKey)
	uniqueTTL := m.uniqueTTL(req.UniqueTTLSeconds)
	now := time.Now()
	uniqueReservationTTL, err := m.workflowTriggerUniqueReservationTTL(req, uniqueTTL, now)
	if err != nil {
		return nil, errors.Tag(err)
	}
	uniqueReserved := false
	if uniqueKey != "" && uniqueTTL > 0 {
		// SETNX 在唯一键本身完成原子预占；外层互斥锁不会增强唯一性，无竞争时也会额外执行加锁和解锁，竞争时还会引入锁重试与等待。
		reserved, reserveErr := m.reserveWorkflowUnique(ctx, workflowName, uniqueKey, workflowID, uniqueReservationTTL)
		if reserveErr != nil {
			return nil, errors.Wrapf(reserveErr, "预占工作流唯一键失败 workflow=%s unique_key=%s workflow_id=%s", workflowName, uniqueKey, workflowID)
		}
		if !reserved {
			return nil, ErrWorkflowAlreadyExists
		}
		uniqueReserved = true
	}
	// releaseUniqueOnFailure 在入口任务未成功入队时按 owner 释放预占；清理保留请求值但不继承已取消信号，并由 5 秒上限防止 Redis 故障时无期阻塞返回。
	releaseUniqueOnFailure := func(cause error) error {
		if !uniqueReserved {
			return errors.Tag(cause)
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), taskFinalWriteTimeout)
		defer cancel()
		if cleanupErr := m.releaseWorkflowUniqueReservation(cleanupCtx, workflowName, uniqueKey, workflowID); cleanupErr != nil {
			return errors.Join(cause, errors.Wrapf(cleanupErr, "释放工作流触发唯一键预占失败 workflow=%s unique_key=%s workflow_id=%s", workflowName, uniqueKey, workflowID))
		}
		return errors.Tag(cause)
	}

	// 构造 workflow:trigger payload（补齐触发人、重试/超时覆盖等）。
	payload := WorkflowTriggerPayload{
		WorkflowID:        workflowID,
		WorkflowName:      workflowName,
		Queue:             strings.TrimSpace(req.Queue),
		Targets:           normalizeStrings(req.Targets),
		ShardTotal:        req.ShardTotal,
		GrayPercent:       req.GrayPercent,
		Source:            WorkflowSourceAPI,
		UniqueKey:         uniqueKey,
		TriggeredByUserID: 0,
	}
	if req.Retry != nil {
		payload.Retry = req.Retry
	}
	if req.TimeoutSeconds != nil {
		payload.TimeoutSeconds = *req.TimeoutSeconds
	}
	if req.UniqueTTLSeconds != nil {
		payload.UniqueTTLSeconds = *req.UniqueTTLSeconds
	} else if uniqueKey != "" {
		payload.UniqueTTLSeconds = int(uniqueTTL.Seconds())
	}
	if meta := requestctx.FromContext(ctx); meta != nil {
		payload.TriggeredByUserID = meta.UserID
		payload.TriggeredByUser = meta.UserName
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, releaseUniqueOnFailure(err)
	}
	if len(body) > tasklimits.MaxPayloadBytes {
		return nil, releaseUniqueOnFailure(errors.Errorf("工作流触发负载超过上限 bytes=%d max=%d", len(body), tasklimits.MaxPayloadBytes))
	}

	// 组装 Asynq 入队选项（queue/taskID/retry/timeout/processAt/deadline/unique）。
	task := m.newTask(ctx, TypeWorkflowTrigger, body, map[string]string{
		headerTaskName:     workflowTriggerTaskName(workflowName, ""),
		headerTaskSource:   WorkflowSourceAPI,
		headerWorkflowID:   workflowID,
		headerWorkflowName: workflowName,
	})
	opts := []asynq.Option{
		asynq.Queue(m.namespacedQueueName(queue)),
		asynq.TaskID(workflowID),
		asynq.MaxRetry(workflowTaskRetryBudget(m.defaultRetry(req.Retry))),
		asynq.Retention(m.CompletedRetention()),
	}
	if timeout := m.defaultTimeout(req.TimeoutSeconds); timeout > 0 {
		opts = append(opts, asynq.Timeout(timeout))
	}
	if req.ProcessInSeconds != nil && *req.ProcessInSeconds > 0 {
		opts = append(opts, asynq.ProcessIn(time.Duration(*req.ProcessInSeconds)*time.Second))
	}
	if req.ProcessAt != "" {
		t, parseErr := time.Parse(time.RFC3339, req.ProcessAt)
		if parseErr != nil {
			return nil, releaseUniqueOnFailure(errors.Wrap(parseErr, "解析 workflow processAt 失败"))
		}
		opts = append(opts, asynq.ProcessAt(t))
	}
	if req.Deadline != "" {
		t, parseErr := time.Parse(time.RFC3339, req.Deadline)
		if parseErr != nil {
			return nil, releaseUniqueOnFailure(errors.Wrap(parseErr, "解析 workflow deadline 失败"))
		}
		opts = append(opts, asynq.Deadline(t))
	}
	if req.UniqueKey != "" {
		uniqueTTL := m.uniqueTTL(req.UniqueTTLSeconds)
		if uniqueTTL > 0 {
			opts = append(opts, asynq.Unique(uniqueTTL))
		}
	}

	// 入队并生成回执；失败时释放幂等预占。
	info, err := m.client.EnqueueContext(ctx, task, opts...)
	if err != nil {
		return nil, releaseUniqueOnFailure(err)
	}
	resp := &types.TaskWorkflowTriggerResp{
		TaskID:       info.ID,
		WorkflowID:   workflowID,
		WorkflowName: payload.WorkflowName,
		Queue:        m.displayQueueName(info.Queue),
	}
	if !info.NextProcessAt.IsZero() {
		resp.ProcessAt = info.NextProcessAt.Format(time.RFC3339)
	}
	return resp, nil
}

// GetWorkflowStatus 优先读取 Redis 热状态，终态过期后回退 DB 历史快照。
// Redis 终态仍保留时只用 DB 确认落库状态，不丢失已有分片级实时明细。
func (m *Manager) GetWorkflowStatus(ctx context.Context, workflowID string) (*types.TaskWorkflowStatusResp, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	resp, err := m.getWorkflowStatusFromRedis(ctx, workflowID)
	if err == nil {
		if resp.Status != WorkflowStatusSuccess && resp.Status != WorkflowStatusFailed {
			return resp, nil
		}
		resp.HistoryStatus = "pending"
		sink := m.historySinkSnapshot()
		if sink == nil {
			resp.HistoryStatus = ""
			return resp, nil
		}
		lookupCtx, cancel := context.WithTimeout(ctx, taskHistoryObservationTimeout)
		history, historyErr := sink.GetWorkflow(lookupCtx, workflowID)
		cancel()
		switch {
		case historyErr == nil && history != nil && history.Status == resp.Status && history.FinishedAt == resp.FinishedAt:
			resp.HistoryStatus = "persisted"
			resp.PersistedAt = history.PersistedAt
		case historyErr != nil && !errors.Is(historyErr, redis.Nil):
			resp.HistoryStatus = "failed"
		}
		return resp, nil
	}
	if !errors.Is(err, redis.Nil) {
		return nil, errors.Tag(err)
	}
	sink := m.historySinkSnapshot()
	if sink == nil {
		return nil, redis.Nil
	}
	lookupCtx, cancel := context.WithTimeout(ctx, taskHistoryObservationTimeout)
	defer cancel()
	return sink.GetWorkflow(lookupCtx, workflowID)
}

// getWorkflowStatusFromRedis 只读取工作流热状态，供异步落库快照使用。
func (m *Manager) getWorkflowStatusFromRedis(ctx context.Context, workflowID string) (*types.TaskWorkflowStatusResp, error) {
	if !m.IsEnabled() {
		return nil, ErrTaskQueueDisabled
	}
	meta, nodes, err := m.readWorkflowStatus(ctx, workflowID)
	if err != nil {
		return nil, errors.Tag(err)
	}
	resp := &types.TaskWorkflowStatusResp{
		WorkflowID:   meta.WorkflowID,
		WorkflowName: meta.WorkflowName,
		PeriodicName: meta.PeriodicName,
		Status:       meta.Status,
		Source:       meta.Source,
		Queue:        m.displayQueueName(meta.Queue),
		Targets:      meta.Targets,
		ShardTotal:   meta.ShardTotal,
		GrayPercent:  meta.GrayPercent,
		ErrorMessage: meta.ErrorMessage,
		CreatedAt:    meta.CreatedAt,
		UpdatedAt:    meta.UpdatedAt,
		FinishedAt:   meta.FinishedAt,
		DurationMS:   durationBetweenRFC3339(meta.CreatedAt, meta.FinishedAt),
		Nodes:        make([]types.TaskWorkflowNodeItem, 0, len(nodes)),
		DataSource:   "redis",
		DetailLevel:  "shard",
	}
	workflowStatsSnapshots := make([]*taskstats.Snapshot, 0, len(nodes))
	workflowProgressItems := make([]*taskstats.Progress, 0, len(nodes))
	def, _ := m.workflowDefinition(meta.WorkflowName)
	for _, node := range nodes {
		nodeProgress := workflowNodeProgress(node, plannedWorkflowNodeInstances(def, meta, node))
		if nodeProgress != nil {
			workflowProgressItems = append(workflowProgressItems, nodeProgress)
		}
		shardTraces := make([]types.TaskWorkflowShardTraceItem, 0, len(node.ShardTraces))
		for _, shardTrace := range node.ShardTraces {
			shardTraces = append(shardTraces, types.TaskWorkflowShardTraceItem{
				ShardIndex:     shardTrace.ShardIndex,
				ShardTotal:     shardTrace.ShardTotal,
				Status:         shardTrace.Status,
				Progress:       shardTrace.Progress,
				ExecutionTrace: shardTrace.ExecutionTrace,
			})
		}
		if node.ExecutionTrace != nil && !node.ExecutionTrace.Empty() {
			workflowStatsSnapshots = append(workflowStatsSnapshots, node.ExecutionTrace)
		}
		resp.Nodes = append(resp.Nodes, types.TaskWorkflowNodeItem{
			Name:           node.Name,
			TaskType:       node.TaskType,
			Queue:          m.displayQueueName(node.Queue),
			Status:         node.Status,
			DependsOn:      node.DependsOn,
			Expected:       node.Expected,
			Succeeded:      node.Succeeded,
			Failed:         node.Failed,
			Skipped:        node.Skipped,
			ErrorMessage:   node.ErrorMessage,
			StartedAt:      node.StartedAt,
			FinishedAt:     node.FinishedAt,
			DurationMS:     durationBetweenRFC3339(node.StartedAt, node.FinishedAt),
			Progress:       nodeProgress,
			ExecutionTrace: node.ExecutionTrace,
			ShardTraces:    shardTraces,
		})
	}
	resp.Progress = taskstats.MergeProgress(taskstats.ProgressUnitNodeInstance, meta.Status, workflowProgressItems...)
	resp.ExecutionTrace = taskstats.MergeSnapshots("workflow."+strings.TrimSpace(meta.WorkflowName), workflowStatsSnapshots...)
	return resp, nil
}

// workflowMetaKey 返回工作流主记录在 Redis 中的 key。
func (m *Manager) workflowMetaKey(workflowID string) string {
	if !m.useRedisNamespace() {
		return ""
	}
	return keys.TaskWorkflowMetaKey(workflowID)
}

// workflowNodesKey 返回工作流节点集合在 Redis 中的 key。
func (m *Manager) workflowNodesKey(workflowID string) string {
	if !m.useRedisNamespace() {
		return ""
	}
	return keys.TaskWorkflowNodesKey(workflowID)
}

// workflowNodeKey 返回单个工作流节点状态记录的 Redis key。
func (m *Manager) workflowNodeKey(workflowID, nodeName string) string {
	if !m.useRedisNamespace() {
		return ""
	}
	return keys.TaskWorkflowNodeKey(workflowID, nodeName)
}

// workflowUniqueKey 返回工作流幂等占位记录的 Redis key。
func (m *Manager) workflowUniqueKey(name, key string) string {
	if !m.useRedisNamespace() {
		return ""
	}
	return keys.TaskWorkflowUniqueKey(name, key)
}

// enqueueTaskWithOptions 把统一任务选项转换为 Asynq 投递参数并执行入队。
func (m *Manager) enqueueTaskWithOptions(ctx context.Context, task *asynq.Task, options *svc.TaskOptions) (*asynq.TaskInfo, error) {
	if task == nil {
		return nil, errors.Errorf("任务不能为空")
	}
	if len(task.Payload()) > tasklimits.MaxPayloadBytes {
		return nil, errors.Errorf("任务负载超过上限 bytes=%d max=%d", len(task.Payload()), tasklimits.MaxPayloadBytes)
	}
	if options == nil {
		options = &svc.TaskOptions{}
	}
	if options.Retry != nil && (*options.Retry < 0 || *options.Retry > tasklimits.MaxRetry) {
		return nil, errors.Errorf("任务 retry 必须在 0-%d 之间", tasklimits.MaxRetry)
	}
	if options.Timeout < 0 || options.Timeout > time.Duration(tasklimits.MaxTimeoutSeconds)*time.Second {
		return nil, errors.Errorf("任务 timeout 不能超过 %d 秒", tasklimits.MaxTimeoutSeconds)
	}
	if options.UniqueTTL < 0 || options.UniqueTTL > time.Duration(tasklimits.MaxUniqueTTLSeconds)*time.Second {
		return nil, errors.Errorf("任务 unique TTL 不能超过 %d 秒", tasklimits.MaxUniqueTTLSeconds)
	}
	if options.Delay < 0 || options.Delay > time.Duration(tasklimits.MaxScheduleDelaySeconds)*time.Second {
		return nil, errors.Errorf("任务 delay 不能超过 %d 秒", tasklimits.MaxScheduleDelaySeconds)
	}
	if options.ProcessAt != nil && options.ProcessAt.After(time.Now().Add(time.Duration(tasklimits.MaxScheduleDelaySeconds)*time.Second)) {
		return nil, errors.Errorf("任务 processAt 不能晚于当前时间 %d 秒", tasklimits.MaxScheduleDelaySeconds)
	}
	queue := helper.FirstNonEmptyString(options.Queue, m.defaultWorkflowQueue())
	if !m.isQueueConfigured(queue) {
		return nil, errors.Errorf("任务队列未配置消费: %s", queue)
	}
	if options.Delay > 0 && options.ProcessAt != nil {
		return nil, errors.Errorf("任务 delay 和 processAt 不能同时设置")
	}
	var asynqOpts []asynq.Option
	asynqOpts = append(asynqOpts, asynq.Queue(m.namespacedQueueName(queue)))
	if options.Retry != nil {
		asynqOpts = append(asynqOpts, asynq.MaxRetry(*options.Retry))
	}
	if options.Timeout > 0 {
		asynqOpts = append(asynqOpts, asynq.Timeout(options.Timeout))
	}
	if options.Delay > 0 {
		asynqOpts = append(asynqOpts, asynq.ProcessIn(options.Delay))
	}
	if options.ProcessAt != nil && !options.ProcessAt.IsZero() {
		asynqOpts = append(asynqOpts, asynq.ProcessAt(*options.ProcessAt))
	}
	if options.Deadline != nil && !options.Deadline.IsZero() {
		asynqOpts = append(asynqOpts, asynq.Deadline(*options.Deadline))
	}
	if options.UniqueTTL > 0 {
		asynqOpts = append(asynqOpts, asynq.Unique(options.UniqueTTL))
	}
	if group := strings.TrimSpace(options.Group); group != "" {
		asynqOpts = append(asynqOpts, asynq.Group(m.namespacedGroup(group)))
	}
	asynqOpts = append(asynqOpts, asynq.Retention(m.CompletedRetention()))
	return m.client.EnqueueContext(ctx, task, asynqOpts...)
}

// taskOptionsFromRequest 把通用投递请求转换为内部任务选项结构。
func (m *Manager) taskOptionsFromRequest(req *types.EnqueueTaskReq) (*svc.TaskOptions, error) {
	options := &svc.TaskOptions{
		Queue: strings.TrimSpace(req.Queue),
		Group: strings.TrimSpace(req.Group),
	}
	if req.Retry != nil {
		options.Retry = new(*req.Retry)
	}
	if req.TimeoutSeconds != nil {
		options.Timeout = time.Duration(*req.TimeoutSeconds) * time.Second
	}
	if req.ProcessInSeconds != nil {
		options.Delay = time.Duration(*req.ProcessInSeconds) * time.Second
	}
	if req.UniqueTTLSeconds != nil {
		options.UniqueTTL = time.Duration(*req.UniqueTTLSeconds) * time.Second
	}
	if req.ProcessAt != "" {
		processAt, err := time.Parse(time.RFC3339, req.ProcessAt)
		if err != nil {
			return nil, errors.Tag(err)
		}
		options.ProcessAt = &processAt
	}
	if req.Deadline != "" {
		deadline, err := time.Parse(time.RFC3339, req.Deadline)
		if err != nil {
			return nil, errors.Tag(err)
		}
		options.Deadline = &deadline
	}
	return options, nil
}

// hasHandler 判断指定任务类型是否已经注册处理器。
func (m *Manager) hasHandler(taskType string) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.handlers[strings.TrimSpace(taskType)]
	return ok
}
