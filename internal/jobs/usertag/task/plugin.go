package task

import (
	"context"
	"strings"
	"time"

	keys "admin/common/rediskeys"
	"admin/internal/config"
	redislock "admin/internal/infra/redsync"
	"admin/internal/jobs/usertag/hook"
	usertagoptions "admin/internal/jobs/usertag/options"
	"admin/internal/jobs/usertag/repository"
	"admin/internal/jobs/usertag/route"
	usertagtypes "admin/internal/jobs/usertag/types"
	usertagworkflow "admin/internal/jobs/usertag/workflow"
	"admin/internal/svc"
	"admin/internal/task/queue"

	"github.com/Is999/go-utils/errors"
	"github.com/hibiken/asynq"
)

const (
	// PluginName 是用户标签任务插件名称，供 taskruntime 包装注册时保持唯一。
	PluginName = "user_tag"
)

const (
	// userTagRuntimeCleanupLockTTL 是运行期辅助表清理互斥锁 TTL，任务存活时由 redsync 自动续期。
	userTagRuntimeCleanupLockTTL = 10 * time.Minute
	// userTagEventOutboxRetryScanLockTTL 是异常事件 outbox 扫描互斥锁 TTL，覆盖单轮领取和派发窗口。
	userTagEventOutboxRetryScanLockTTL = 5 * time.Minute
)

// userTagStageHandlerSpec 描述一个主 DAG 节点和 Asynq 任务类型的绑定关系。
type userTagStageHandlerSpec struct {
	TaskType string // Asynq 任务类型
	Node     string // 用户标签工作流节点
}

// Runtime 描述用户标签任务注册所需的最小运行时能力。
// 该接口让 usertag/task 包承载任务细节，同时避免反向依赖 taskruntime 造成包循环。
type Runtime interface {
	// ServiceContext 返回任务处理器共享的服务上下文。
	ServiceContext() *svc.ServiceContext
	// RegisterHandler 注册 Asynq 任务处理器。
	RegisterHandler(pattern string, handler asynq.Handler) error
	// RegisterFinalFailureHook 注册任务终态失败后的业务清理钩子。
	RegisterFinalFailureHook(name string, hook taskqueue.TaskFinalFailureHook) error
	// RegisterWorkflow 注册用户标签工作流定义。
	RegisterWorkflow(def *taskqueue.WorkflowDefinition) error
}

// Setup 注册用户标签全部任务 handler 和工作流定义。
func Setup(runtime Runtime) error {
	if runtime == nil || runtime.ServiceContext() == nil || !runtime.ServiceContext().CurrentConfig().Workflows.UserTag.Enabled {
		return nil
	}
	svcCtx := runtime.ServiceContext()
	if err := registerUserTagWorkflowLeaseFinalFailureHook(runtime, svcCtx); err != nil {
		return errors.Tag(err)
	}
	if err := registerUserTagStageHandlers(runtime, svcCtx); err != nil {
		return errors.Tag(err)
	}
	if err := runtime.RegisterHandler(usertagtypes.TaskTypeUserTagEventOutboxRetry, asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		return runUserTagEventOutboxRetryTask(ctx, task, svcCtx)
	})); err != nil {
		return errors.Tag(err)
	}
	if err := runtime.RegisterHandler(usertagtypes.TaskTypeUserTagRuntimeCleanup, asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
		return runUserTagRuntimeCleanupTask(ctx, svcCtx)
	})); err != nil {
		return errors.Tag(err)
	}
	defaults := usertagoptions.NewDefaults(runtime.ServiceContext().CurrentConfig().Workflows.UserTag)
	for _, def := range WorkflowDefinitions(defaults) {
		if err := runtime.RegisterWorkflow(def); err != nil {
			return errors.Tag(err)
		}
	}
	if err := runtime.RegisterWorkflow(RuntimeCleanupWorkflowDefinition(defaults)); err != nil {
		return errors.Tag(err)
	}
	if err := runtime.RegisterWorkflow(EventOutboxRetryScanWorkflowDefinition(defaults)); err != nil {
		return errors.Tag(err)
	}
	return nil
}

// registerUserTagStageHandlers 注册用户标签主 DAG 所有节点处理器。
func registerUserTagStageHandlers(runtime Runtime, svcCtx *svc.ServiceContext) error {
	for _, spec := range userTagStageHandlerSpecs() {
		spec := spec
		if err := runtime.RegisterHandler(spec.TaskType, asynq.HandlerFunc(func(ctx context.Context, task *asynq.Task) error {
			return runUserTagTask(ctx, task, svcCtx, spec.Node)
		})); err != nil {
			return errors.Tag(err)
		}
	}
	return nil
}

// userTagStageHandlerSpecs 返回主 DAG 节点和任务类型的稳定绑定清单。
func userTagStageHandlerSpecs() []userTagStageHandlerSpec {
	return []userTagStageHandlerSpec{
		{TaskType: TaskTypeUserTagPrepare, Node: usertagtypes.NodePrepare},
		{TaskType: TaskTypeUserTagCollectScope, Node: usertagtypes.NodeCollectScope},
		{TaskType: TaskTypeUserTagEvaluateTags, Node: usertagtypes.NodeEvaluateTags},
		{TaskType: TaskTypeUserTagResolveChanges, Node: usertagtypes.NodeResolveChanges},
		{TaskType: TaskTypeUserTagPersistResults, Node: usertagtypes.NodePersistResults},
		{TaskType: TaskTypeUserTagFinalize, Node: usertagtypes.NodeFinalize},
		{TaskType: TaskTypeUserTagDispatchHooks, Node: usertagtypes.NodeDispatchHooks},
	}
}

// registerUserTagWorkflowLeaseFinalFailureHook 注册 full 工作流终态失败后的租约释放钩子。
// 该钩子只在 Asynq 确认节点任务不会继续自动重试后触发，避免重试队列中的 full 被提前释放锁。
func registerUserTagWorkflowLeaseFinalFailureHook(runtime Runtime, svcCtx *svc.ServiceContext) error {
	if runtime == nil {
		return nil
	}
	return runtime.RegisterFinalFailureHook("user_tag.workflow_lease.final_failure_release", func(ctx context.Context, task *asynq.Task, meta taskqueue.WorkflowTaskMeta, runErr error) error {
		return releaseUserTagWorkflowLeaseOnFinalFailure(ctx, svcCtx, task, meta)
	})
}

// releaseUserTagWorkflowLeaseOnFinalFailure 在 full 工作流节点终态失败后释放全局写租约。
// 非 full 工作流不持有该租约；解析失败的任务也不会释放，避免误删其他 workflow 的 owner。
func releaseUserTagWorkflowLeaseOnFinalFailure(ctx context.Context, svcCtx *svc.ServiceContext, task *asynq.Task, meta taskqueue.WorkflowTaskMeta) error {
	if task == nil || !strings.HasPrefix(task.Type(), "user_tag:") {
		return nil
	}
	payload, err := DecodePayload(task)
	if err != nil {
		return nil
	}
	workflowID := strings.TrimSpace(payload.WorkflowID)
	if workflowID == "" {
		workflowID = strings.TrimSpace(meta.WorkflowID)
	}
	if workflowID == "" {
		return nil
	}
	mode := strings.TrimSpace(payload.Mode)
	if mode == "" && strings.TrimSpace(meta.WorkflowName) == WorkflowNameUserTagFull {
		mode = usertagtypes.ModeFull
	}
	if mode != usertagtypes.ModeFull {
		return nil
	}
	if workflowName := strings.TrimSpace(meta.WorkflowName); workflowName != "" && workflowName != WorkflowNameUserTagFull {
		return nil
	}
	defaults := usertagoptions.NewDefaults(config.UserTagConfig{})
	if svcCtx != nil {
		defaults = usertagoptions.NewDefaults(svcCtx.CurrentConfig().Workflows.UserTag)
	}
	deps := repository.NewRuntimeDeps(svcCtx, route.NewShardPlanWithResult(defaults.ShardTotal, defaults.ResultShardTotal))
	return repository.NewTagRepository(deps).ReleaseWorkflowLease(ctx, usertagtypes.RuntimeOptions{
		WorkflowID: workflowID,
		Mode:       usertagtypes.ModeFull,
	})
}

// runUserTagTask 统一完成任务负载解析和 usertag 节点分发。
func runUserTagTask(ctx context.Context, task *asynq.Task, svcCtx *svc.ServiceContext, node string) error {
	payload, err := DecodePayload(task)
	if err != nil {
		return errors.Tag(err)
	}
	return runUserTagStage(ctx, svcCtx, node, payload)
}

// runUserTagStage 设置当前节点名称并执行 usertag 工作流阶段。
func runUserTagStage(ctx context.Context, svcCtx *svc.ServiceContext, node string, payload WorkflowPayload) error {
	payload.Node = node
	_, err := usertagworkflow.NewService(ctx, svcCtx).RunStage(payload)
	return errors.Tag(err)
}

// runUserTagEventOutboxRetryTask 独立重试用户标签事件 outbox。
// 该任务没有 workflow 头信息，失败只进入任务系统 retry/dead，不会回写主工作流状态。
func runUserTagEventOutboxRetryTask(ctx context.Context, task *asynq.Task, svcCtx *svc.ServiceContext) error {
	payload, err := DecodePayload(task)
	if err != nil {
		return errors.Tag(err)
	}
	if payload.Node == usertagtypes.NodeEventOutboxRetryScan {
		return runUserTagEventOutboxRetryScanTask(ctx, svcCtx)
	}
	defaults := usertagoptions.NewDefaults(config.UserTagConfig{})
	if svcCtx != nil {
		defaults = usertagoptions.NewDefaults(svcCtx.CurrentConfig().Workflows.UserTag)
	}
	opts, err := userTagEventOutboxRetryOptions(payload, defaults)
	if err != nil {
		return errors.Tag(err)
	}
	if !opts.EventHookEnabled {
		return nil
	}
	deps := repository.NewRuntimeDeps(svcCtx, route.NewShardPlanWithResult(defaults.ShardTotal, defaults.ResultShardTotal))
	_, err = repository.NewTagRepository(deps).DrainEventOutboxShard(ctx, opts, hook.DefaultRegistry().Dispatch)
	return errors.Tag(err)
}

// runUserTagEventOutboxRetryScanTask 周期扫描并重派异常用户标签事件 outbox。
func runUserTagEventOutboxRetryScanTask(ctx context.Context, svcCtx *svc.ServiceContext) error {
	defaults := usertagoptions.NewDefaults(config.UserTagConfig{})
	if svcCtx != nil {
		defaults = usertagoptions.NewDefaults(svcCtx.CurrentConfig().Workflows.UserTag)
	}
	if svcCtx == nil {
		return errors.Errorf("用户标签事件 outbox 异常扫描失败：ServiceContext 为空")
	}
	opts, err := userTagEventOutboxRetryScanOptions(defaults)
	if err != nil {
		return errors.Tag(err)
	}
	if !opts.EventHookEnabled {
		return nil
	}
	deps := repository.NewRuntimeDeps(svcCtx, route.NewShardPlanWithResult(defaults.ShardTotal, defaults.ResultShardTotal))
	lockKey := keys.UserTagEventOutboxRetryScanRedisKey()
	err = redislock.WithLockOnce(ctx, svcCtx.Rds, lockKey, userTagEventOutboxRetryScanLockTTL, func(lockCtx context.Context) error {
		_, runErr := repository.NewTagRepository(deps).RetryEventOutboxAbnormalRows(lockCtx, opts, hook.DefaultRegistry().Dispatch)
		return errors.Tag(runErr)
	})
	if redislock.IsLockTaken(err) {
		// 周期重复触发已由其它实例执行时直接跳过，不能把正常互斥竞争写入任务 retry/dead。
		return nil
	}
	return errors.Tag(err)
}

// runUserTagRuntimeCleanupTask 独立清理用户标签运行期辅助表。
// 该任务不读取 workflow 负载，失败只进入任务系统 retry/dead，不会回写或影响任何用户标签计算工作流。
func runUserTagRuntimeCleanupTask(ctx context.Context, svcCtx *svc.ServiceContext) error {
	defaults := usertagoptions.NewDefaults(config.UserTagConfig{})
	if svcCtx != nil {
		defaults = usertagoptions.NewDefaults(svcCtx.CurrentConfig().Workflows.UserTag)
	}
	if svcCtx == nil {
		return errors.Errorf("用户标签运行期清理失败：ServiceContext 为空")
	}
	deps := repository.NewRuntimeDeps(svcCtx, route.NewShardPlanWithResult(defaults.ShardTotal, defaults.ResultShardTotal))
	lockKey := keys.UserTagRuntimeCleanupRedisKey()
	err := redislock.WithLockOnce(ctx, svcCtx.Rds, lockKey, userTagRuntimeCleanupLockTTL, func(lockCtx context.Context) error {
		return repository.NewTagRepository(deps).CleanupStaleRuntimeTables(lockCtx, time.Now())
	})
	if redislock.IsLockTaken(err) {
		// 清理任务只要求同一时刻一个实例推进；已有持有者时本轮成功跳过，由下一周期继续检查。
		return nil
	}
	return errors.Tag(err)
}

// userTagEventOutboxRetryOptions 构造事件 outbox 重试任务运行参数。
func userTagEventOutboxRetryOptions(payload WorkflowPayload, defaults Defaults) (usertagtypes.RuntimeOptions, error) {
	mode := strings.TrimSpace(payload.Mode)
	switch mode {
	case usertagtypes.ModeDelta, usertagtypes.ModeTargeted, usertagtypes.ModeRecalculate, usertagtypes.ModeFull:
	default:
		return usertagtypes.RuntimeOptions{}, errors.Errorf("事件 outbox 重试 mode 非法 mode=%s", mode)
	}
	opts := usertagtypes.RuntimeOptions{
		WorkflowID:       strings.TrimSpace(payload.WorkflowID),
		Mode:             mode,
		UIDs:             payload.UIDs,
		ShardIndex:       payload.ShardIndex,
		ShardTotal:       positiveOr(payload.ShardTotal, defaults.ShardTotal),
		BatchSize:        positiveOr(payload.BatchSize, defaults.EventBatchSize),
		WorkerCount:      positiveOr(payload.WorkerCount, defaults.WorkerCount),
		DryRun:           payload.DryRun,
		EventHookEnabled: defaults.EventHookEnabled,
	}
	if err := usertagoptions.ValidateRuntimeOptions(opts); err != nil {
		return opts, errors.Tag(err)
	}
	return opts, nil
}

// userTagEventOutboxRetryScanOptions 构造周期扫描异常事件 outbox 的运行参数。
func userTagEventOutboxRetryScanOptions(defaults Defaults) (usertagtypes.RuntimeOptions, error) {
	opts := usertagtypes.RuntimeOptions{
		Mode:             usertagtypes.ModeDelta,
		ShardIndex:       0,
		ShardTotal:       1,
		BatchSize:        positiveOr(defaults.EventBatchSize, defaults.BatchSize),
		WorkerCount:      positiveOr(defaults.WorkerCount, 1),
		EventHookEnabled: defaults.EventHookEnabled,
	}
	if err := usertagoptions.ValidateRuntimeOptions(opts); err != nil {
		return opts, errors.Tag(err)
	}
	return opts, nil
}

// positiveOr 返回正数配置，非正数时使用兜底值。
func positiveOr(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}
