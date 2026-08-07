package taskqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Is999/go-utils/errors"

	"admin/common/i18n"
	keys "admin/common/rediskeys"
	"admin/common/runtimecfg"
	"admin/internal/config"
	"admin/internal/requestctx"
	"admin/internal/svc"
	tasklimits "admin/internal/task/limits"
	"admin/internal/task/stats"
	"admin/internal/types"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// enabledTaskPeriodicConfig 构造测试中显式启用的周期任务配置。
func enabledTaskPeriodicConfig(item config.TaskPeriodicConfig) config.TaskPeriodicConfig {
	enabled := true
	item.Enabled = &enabled
	return item
}

// failNthEnqueuer 在指定调用处注入一次投递失败，复现分片部分入队窗口。
type failNthEnqueuer struct {
	mu       sync.Mutex   // mu 保护调用计数和单次失败状态
	delegate taskEnqueuer // delegate 执行未注入失败的真实投递
	failAt   int          // failAt 表示第几次投递返回错误，从 1 开始
	calls    int          // calls 记录累计投递次数
	failed   bool         // failed 保证故障只注入一次
}

// failFromEnqueuer 从指定调用起持续失败，用于验证技术重试耗尽收口。
type failFromEnqueuer struct {
	mu       sync.Mutex   // mu 保护调用计数
	delegate taskEnqueuer // delegate 执行故障点之前的真实投递
	failFrom int          // failFrom 表示从第几次投递起持续失败
	calls    int          // calls 记录累计投递次数
}

// taskEnqueuerFunc 把测试闭包适配为任务投递器。
type taskEnqueuerFunc func(context.Context, *asynq.Task, ...asynq.Option) (*asynq.TaskInfo, error)

// EnqueueContext 执行测试注入的任务投递闭包。
func (f taskEnqueuerFunc) EnqueueContext(ctx context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	return f(ctx, task, opts...)
}

// EnqueueContext 实现从指定调用起持续失败的任务投递器。
func (f *failFromEnqueuer) EnqueueContext(ctx context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	f.mu.Lock()
	f.calls++
	shouldFail := f.failFrom > 0 && f.calls >= f.failFrom
	f.mu.Unlock()
	if shouldFail {
		return nil, errors.New("persistent enqueue failure")
	}
	return f.delegate.EnqueueContext(ctx, task, opts...)
}

// EnqueueContext 实现 taskEnqueuer，并在目标调用处返回确定性故障。
func (f *failNthEnqueuer) EnqueueContext(ctx context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	f.mu.Lock()
	f.calls++
	shouldFail := !f.failed && f.failAt > 0 && f.calls == f.failAt
	if shouldFail {
		f.failed = true
	}
	f.mu.Unlock()
	if shouldFail {
		return nil, errors.New("injected enqueue failure")
	}
	return f.delegate.EnqueueContext(ctx, task, opts...)
}

// TestManagerReadyChecksRedisAndWorkerState 验证任务就绪检查下探 Redis 并校验部署模式要求的 Worker。
func TestManagerReadyChecksRedisAndWorkerState(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.Ready(context.Background(), false, false); err != nil {
		t.Fatalf("仅投递模式任务就绪检查失败: %v", err)
	}
	if err := manager.Ready(context.Background(), true, false); err == nil || !strings.Contains(err.Error(), "Worker") {
		t.Fatalf("未启动 Worker 时应判定未就绪，实际=%v", err)
	}
	if err := manager.StartWorker(); err != nil {
		t.Fatalf("启动 Worker 失败: %v", err)
	}
	if err := manager.Ready(context.Background(), true, false); err != nil {
		t.Fatalf("Worker 启动后应就绪: %v", err)
	}
	manager.recordWorkerHeartbeat(errors.New("worker heartbeat failed"))
	if err := manager.Ready(context.Background(), true, false); err == nil || !strings.Contains(err.Error(), "心跳失败") {
		t.Fatalf("Worker 内部心跳失败时应判定未就绪，实际=%v", err)
	}
	manager.recordWorkerHeartbeat(nil)
	manager.workerHealthMu.Lock()
	manager.workerHealth.lastHeartbeat = time.Now().Add(-workerHeartbeatMaxAge - time.Second)
	manager.workerHealthMu.Unlock()
	if err := manager.Ready(context.Background(), true, false); err == nil || !strings.Contains(err.Error(), "心跳已超时") {
		t.Fatalf("Worker 心跳停滞时应判定未就绪，实际=%v", err)
	}
	manager.recordWorkerHeartbeat(nil)
	if err := manager.Stop(context.Background()); err != nil {
		t.Fatalf("停止 Worker 失败: %v", err)
	}
	if err := manager.redis.Close(); err != nil {
		t.Fatalf("关闭任务 Redis 失败: %v", err)
	}
	if err := manager.Ready(context.Background(), false, false); err == nil || !strings.Contains(err.Error(), "Redis") {
		t.Fatalf("任务 Redis 关闭后应判定未就绪，实际=%v", err)
	}
}

// TestManagerReadyChecksSchedulerState 验证 Scheduler 模式不会把未启动的选举循环误报为就绪。
func TestManagerReadyChecksSchedulerState(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterWorkflow(testWorkflowDefinition("ready.workflow")); err != nil {
		t.Fatalf("注册就绪检查工作流失败: %v", err)
	}
	cfg := manager.CurrentConfig()
	cfg.Scheduler.Enabled = true
	cfg.Periodic = []config.TaskPeriodicConfig{enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
		Name: "ready-periodic", Cron: "*/5 * * * *", Workflow: "ready.workflow",
	})}
	manager.UpdateConfig(cfg)
	if err := manager.Ready(context.Background(), false, true); err == nil || !strings.Contains(err.Error(), "Scheduler") {
		t.Fatalf("未启动 Scheduler 时应判定未就绪，实际=%v", err)
	}
	if err := manager.StartScheduler(); err != nil {
		t.Fatalf("启动 Scheduler 失败: %v", err)
	}
	if err := manager.Ready(context.Background(), false, true); err != nil {
		t.Fatalf("Scheduler 启动后应就绪: %v", err)
	}
	manager.leader.markHeartbeat(errors.New("scheduler heartbeat failed"))
	if err := manager.Ready(context.Background(), false, true); err == nil || !strings.Contains(err.Error(), "心跳失败") {
		t.Fatalf("Scheduler 选举心跳失败时应判定未就绪，实际=%v", err)
	}
	manager.leader.markHeartbeat(nil)
	manager.leader.healthMu.Lock()
	manager.leader.health.lastHeartbeat = time.Now().Add(-4 * manager.leader.renewInterval)
	manager.leader.healthMu.Unlock()
	if err := manager.Ready(context.Background(), false, true); err == nil || !strings.Contains(err.Error(), "心跳已超时") {
		t.Fatalf("Scheduler 选举心跳停滞时应判定未就绪，实际=%v", err)
	}
	manager.leader.markHeartbeat(nil)
}

// TestManagerStartSchedulerRejectsInvalidLeaseKey 验证非法站点租约键会立即失败，不会阻塞启动线程。
func TestManagerStartSchedulerRejectsInvalidLeaseKey(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	cfg := manager.CurrentConfig()
	cfg.Scheduler.Enabled = true
	cfg.Scheduler.LeaseKey = "app:other:task:scheduler:leader"
	cfg.Periodic = []config.TaskPeriodicConfig{enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
		Name: "invalid-lease-key", Cron: "*/5 * * * *", Workflow: "cache.refresh",
	})}
	manager.UpdateConfig(cfg)

	done := make(chan error, 1)
	go func() {
		done <- manager.StartScheduler()
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "租约键无效") {
			t.Fatalf("非法租约键应直接返回启动错误，实际=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("非法租约键导致调度器启动阻塞")
	}
}

// TestManagerStopHonorsContextDeadline 确保后台协程未退出时不会突破应用统一停止期限。
func TestManagerStopHonorsContextDeadline(t *testing.T) {
	manager := &Manager{}
	manager.wg.Add(1)
	t.Cleanup(manager.wg.Done)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	err := manager.Stop(ctx)
	if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("Stop() error = %v, want deadline exceeded", err)
	}
	if time.Since(startedAt) > time.Second {
		t.Fatalf("Stop() ignored deadline, elapsed=%s", time.Since(startedAt))
	}
}

// TestTaskLogMessageIncludesTaskIdentity 验证任务生命周期日志正文带上任务身份字段。
func TestTaskLogMessageIncludesTaskIdentity(t *testing.T) {
	msg := taskLogMessage("任务 开始执行", &requestctx.Meta{
		TaskName:     "工作流触发:user_tag.delta.refresh",
		TaskType:     TypeWorkflowTrigger,
		TaskID:       "task-001",
		TaskQueue:    QueueMaintenance,
		WorkflowName: "user_tag.delta.refresh",
		WorkflowNode: "snapshot",
		WorkflowID:   "wf-001",
		Mode:         "delta",
		ShardIndex:   1,
		ShardTotal:   4,
		LatencyMS:    36,
	})
	for _, want := range []string{
		"任务 开始执行",
		"task_name=工作流触发:user_tag.delta.refresh",
		"task_type=workflow:trigger",
		"task_id=task-001",
		"queue=maintenance",
		"workflow=user_tag.delta.refresh",
		"node=snapshot",
		"workflow_id=wf-001",
		"mode=delta",
		"shard=1/4",
		"latency_ms=36",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("taskLogMessage() = %q, want contains %q", msg, want)
		}
	}
}

// TestNewReturnsNilWhenAppIDMissing 确保任务系统缺失 app_id 时失败闭合，不在运行时 panic。
func TestNewReturnsNilWhenAppIDMissing(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
		server.Close()
	})

	manager := New(config.TaskQueueConfig{Enabled: true}, client)
	if manager != nil {
		t.Fatal("期望 app_id 缺失时任务管理器不启动")
	}
}

// TestTaskListTimeRangeUsesActivityTime 验证时间范围按任务活动时间过滤。
func TestTaskListTimeRangeUsesActivityTime(t *testing.T) {
	startedAt := time.Date(2026, 5, 23, 3, 15, 0, 0, time.UTC)
	completedAt := time.Date(2026, 5, 23, 4, 30, 0, 0, time.UTC)
	nextProcessAt := time.Date(2026, 5, 23, 5, 30, 0, 0, time.UTC)
	rangeStart := time.Date(2026, 5, 23, 4, 0, 0, 0, time.UTC)
	rangeEnd := time.Date(2026, 5, 23, 5, 0, 0, 0, time.UTC)
	timeRange, err := parseTaskListTimeRange(rangeStart.Format(time.RFC3339), rangeEnd.Format(time.RFC3339))
	if err != nil {
		t.Fatalf("parseTaskListTimeRange() error = %v", err)
	}

	completedTask := types.TaskItem{
		State:       "completed",
		StartedAt:   startedAt.Format(time.RFC3339),
		CompletedAt: completedAt.Format(time.RFC3339),
	}
	if !timeRange.Contains(completedTask) {
		t.Fatalf("completed task should match by completedAt, item=%+v", completedTask)
	}

	scheduledTask := types.TaskItem{
		State:         "scheduled",
		StartedAt:     startedAt.Format(time.RFC3339),
		NextProcessAt: nextProcessAt.Format(time.RFC3339),
	}
	if timeRange.Contains(scheduledTask) {
		t.Fatalf("scheduled task should still match by nextProcessAt, item=%+v", scheduledTask)
	}
}

// TestRegisterWorkflowRejectsInvalidDAG 验证非法 DAG 定义会在注册阶段被拦截。
func TestRegisterWorkflowRejectsInvalidDAG(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	tests := []struct {
		name string              // name 表示测试场景名称。
		def  *WorkflowDefinition // def 表示测试字段。
	}{
		{
			name: "missing dependency",
			def: &WorkflowDefinition{
				Name: "missing-dep",
				Nodes: map[string]*WorkflowNodeDefinition{
					"root": {
						TaskType:     TypeWorkflowNoop,
						DependsOn:    []string{"missing"},
						BuildPayload: testNoopPayload,
					},
				},
			},
		},
		{
			name: "cycle",
			def: &WorkflowDefinition{
				Name: "cycle",
				Nodes: map[string]*WorkflowNodeDefinition{
					"a": {
						TaskType:     TypeWorkflowNoop,
						DependsOn:    []string{"b"},
						BuildPayload: testNoopPayload,
					},
					"b": {
						TaskType:     TypeWorkflowNoop,
						DependsOn:    []string{"a"},
						BuildPayload: testNoopPayload,
					},
				},
			},
		},
		{
			name: "no root",
			def: &WorkflowDefinition{
				Name: "no-root",
				Nodes: map[string]*WorkflowNodeDefinition{
					"a": {
						TaskType:     TypeWorkflowNoop,
						DependsOn:    []string{"b"},
						BuildPayload: testNoopPayload,
					},
					"b": {
						TaskType:     TypeWorkflowNoop,
						DependsOn:    []string{"a"},
						BuildPayload: testNoopPayload,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := manager.RegisterWorkflow(tt.def); err == nil {
				t.Fatalf("期望工作流 %s 返回定义校验错误", tt.name)
			}
		})
	}
}

// TestRegisterWorkflowRejectsShardLimitAboveSystemMaximum 校验工作流定义不能绕过系统分片硬上限。
func TestRegisterWorkflowRejectsShardLimitAboveSystemMaximum(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	definition := testWorkflowDefinition("oversized-shard-limit")
	definition.MaxShardTotal = tasklimits.MaxShardTotal + 1
	if err := manager.RegisterWorkflow(definition); err == nil {
		t.Fatal("期望超过系统上限的工作流定义被拒绝")
	}
}

// TestStartWorkflowRejectsShardTotalAboveSystemLimit 校验未自定义上限的工作流仍受系统分片硬上限约束。
func TestStartWorkflowRejectsShardTotalAboveSystemLimit(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	definition := testWorkflowDefinition("system-shard-limit")
	if err := manager.RegisterWorkflow(definition); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	if _, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		Name:       definition.Name,
		ShardTotal: tasklimits.MaxShardTotal + 1,
	}); err == nil || !strings.Contains(err.Error(), "分片数超过上限") {
		t.Fatalf("StartWorkflow() error = %v, want shard limit", err)
	}
}

// TestStartWorkflowRejectsOversizedTargets 校验内部工作流入口也不能绕过目标体积硬上限。
func TestStartWorkflowRejectsOversizedTargets(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	definition := testWorkflowDefinition("oversized-targets")
	if err := manager.RegisterWorkflow(definition); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	if _, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		Name:    definition.Name,
		Targets: []string{strings.Repeat("x", tasklimits.MaxWorkflowTargetsBytes+1)},
	}); err == nil || !strings.Contains(err.Error(), "工作流目标") {
		t.Fatalf("StartWorkflow() error = %v, want target limit", err)
	}
}

// TestStartWorkflowRejectsOversizedNodePayload 校验工作流定义生成的异常大负载不会进入 Redis。
func TestStartWorkflowRejectsOversizedNodePayload(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	definition := testWorkflowDefinition("oversized-node-payload")
	definition.Nodes["root"].BuildPayload = func(WorkflowStartSpec, *WorkflowNodeDefinition, int, int) ([]byte, error) {
		return make([]byte, tasklimits.MaxPayloadBytes+1), nil
	}
	if err := manager.RegisterWorkflow(definition); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	if _, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: definition.Name}); err == nil || !strings.Contains(err.Error(), "节点负载超过上限") {
		t.Fatalf("StartWorkflow() error = %v, want payload limit", err)
	}
}

// TestEnqueueTaskRejectsUnsafeInternalOptions 校验内部投递入口同样遵守延迟、重试和负载硬上限。
func TestEnqueueTaskRejectsUnsafeInternalOptions(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.EnqueueTask(context.Background(), "demo:task", []byte(`{}`), svc.WithTaskDelay(time.Duration(tasklimits.MaxScheduleDelaySeconds+1)*time.Second)); err == nil {
		t.Fatal("期望超长延迟任务被拒绝")
	}
	if err := manager.EnqueueTask(context.Background(), "demo:task", []byte(`{}`), svc.WithTaskRetry(tasklimits.MaxRetry+1)); err == nil {
		t.Fatal("期望超限重试任务被拒绝")
	}
	if err := manager.EnqueueTask(context.Background(), "demo:task", make([]byte, tasklimits.MaxPayloadBytes+1)); err == nil {
		t.Fatal("期望超限内部任务负载被拒绝")
	}
	if err := manager.EnqueueTask(context.Background(), "demo:task", []byte(`{}`), svc.WithTaskQueue("orphan")); err == nil {
		t.Fatal("期望无人消费的任务队列被拒绝")
	}
	if err := manager.EnqueueCacheRefresh(context.Background(), "refresh", []string{strings.Repeat("x", tasklimits.MaxPayloadBytes)}); err == nil {
		t.Fatal("期望超限缓存刷新负载被拒绝")
	}
}

// TestEnrichTaskContextSetsModeField 验证任务 payload 中的 mode 会进入统一日志链路字段。
func TestEnrichTaskContextSetsModeField(t *testing.T) {
	manager := &Manager{}
	payload := mustJSONBytes(map[string]any{
		"workflowId":   "wf-mode-1",
		"workflowName": "user_tag_delta",
		"workflowNode": "collect_scope",
		"mode":         "delta",
		"shardIndex":   2,
		"shardTotal":   8,
	})
	task := asynq.NewTask(TypeWorkflowNoop, payload)

	ctx := manager.enrichTaskContext(context.Background(), task)
	meta := requestctx.FromContext(ctx)
	if meta == nil {
		t.Fatal("期望任务上下文存在统一元数据")
	}
	if meta.Mode != "delta" {
		t.Fatalf("期望 mode=delta，实际为 %q", meta.Mode)
	}
	if meta.WorkflowNode != "collect_scope" || meta.ShardIndex != 2 || meta.ShardTotal != 8 {
		t.Fatalf("工作流链路字段不符合预期: %+v", meta)
	}
}

// TestTaskItemWorkflowIDUsesCompletedResult 验证周期触发任务可从成功结果补齐工作流实例 ID。
func TestTaskItemWorkflowIDUsesCompletedResult(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	item := manager.toTaskItem(context.Background(), &asynq.TaskInfo{
		ID:     "trigger-result-workflow",
		Type:   TypeWorkflowTrigger,
		Queue:  manager.namespacedQueueName(QueueMaintenance),
		State:  asynq.TaskStateCompleted,
		Result: []byte(`{"workflowId":"wf-result"}`),
	})
	if item.WorkflowID != "wf-result" {
		t.Fatalf("任务工作流实例 ID=%q，期望=wf-result", item.WorkflowID)
	}
}

// TestTaskItemWorkflowIDUsesPayloadOrTaskID 验证失败触发任务仍可恢复稳定工作流实例 ID。
func TestTaskItemWorkflowIDUsesPayloadOrTaskID(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	fromPayload := manager.toTaskItem(context.Background(), &asynq.TaskInfo{
		ID:      "trigger-payload-task",
		Type:    TypeWorkflowTrigger,
		Queue:   manager.namespacedQueueName(QueueMaintenance),
		State:   asynq.TaskStateArchived,
		Payload: []byte(`{"workflowId":"wf-payload"}`),
	})
	if fromPayload.WorkflowID != "wf-payload" {
		t.Fatalf("载荷工作流实例 ID=%q，期望=wf-payload", fromPayload.WorkflowID)
	}
	fromTaskID := manager.toTaskItem(context.Background(), &asynq.TaskInfo{
		ID:      "wf-task-id",
		Type:    TypeWorkflowTrigger,
		Queue:   manager.namespacedQueueName(QueueMaintenance),
		State:   asynq.TaskStateArchived,
		Payload: []byte(`{"workflowName":"archive.run"}`),
	})
	if fromTaskID.WorkflowID != "wf-task-id" {
		t.Fatalf("任务 ID 恢复的工作流实例 ID=%q，期望=wf-task-id", fromTaskID.WorkflowID)
	}
}

// TestGetWorkflowStatusMissingPreservesRedisNil 验证历史兜底可区分快照过期与 Redis 读取故障。
func TestGetWorkflowStatusMissingPreservesRedisNil(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	if _, err := manager.GetWorkflowStatus(context.Background(), "missing-workflow"); !errors.Is(err, redis.Nil) {
		t.Fatalf("缺失工作流错误=%v，期望保留 redis.Nil", err)
	}
}

// TestRegisterWorkflowRejectsDuplicateName 验证 Manager 层不会静默覆盖同名工作流定义。
func TestRegisterWorkflowRejectsDuplicateName(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := testWorkflowDefinition("demo.duplicate.workflow")
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("首次注册工作流失败: %v", err)
	}
	if err := manager.RegisterWorkflow(def); err == nil {
		t.Fatal("期望重复注册工作流返回错误，实际为 nil")
	}
}

// TestRegisterHandlerRejectsDuplicatePattern 验证 Manager 层提前拦截重复处理器，避免 Asynq ServeMux panic。
func TestRegisterHandlerRejectsDuplicatePattern(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	handler := asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return nil
	})
	if err := manager.RegisterHandler("demo:duplicate", handler); err != nil {
		t.Fatalf("首次注册处理器失败: %v", err)
	}
	if err := manager.RegisterHandler("demo:duplicate", handler); err == nil {
		t.Fatal("期望重复注册处理器返回错误，实际为 nil")
	}
}

// TestRegisterHandlerRecoversServeMuxPanic 验证底层 ServeMux 异常会被转成错误并回滚本地注册表。
func TestRegisterHandlerRecoversServeMuxPanic(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	handler := asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return nil
	})
	manager.mux.Handle("demo:mux-only", handler)
	if err := manager.RegisterHandler("demo:mux-only", handler); err == nil {
		t.Fatal("期望 ServeMux 重复注册被转成错误，实际为 nil")
	}
	for _, item := range manager.ListRegisteredTaskTypes(context.Background()) {
		if item.TaskType == "demo:mux-only" {
			t.Fatalf("期望本地注册表已回滚，实际仍存在: %+v", item)
		}
	}
}

// TestRegistryDisplayUsesRequestLocale 验证前端注册表展示文案通过统一语言包按请求语言返回。
func TestRegistryDisplayUsesRequestLocale(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterHandler(TypeCacheRefreshRequest, asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return nil
	})); err != nil {
		t.Fatalf("注册测试任务处理器失败: %v", err)
	}
	if err := manager.RegisterWorkflow(&WorkflowDefinition{
		Name:         WorkflowNameCacheRefresh,
		Description:  "中文兜底说明",
		DefaultQueue: QueueMaintenance,
		Nodes: map[string]*WorkflowNodeDefinition{
			"refresh": {
				Name:     "refresh",
				TaskType: TypeCacheRefreshBatch,
				BuildPayload: func(WorkflowStartSpec, *WorkflowNodeDefinition, int, int) ([]byte, error) {
					return []byte(`{}`), nil
				},
			},
		},
	}); err != nil {
		t.Fatalf("注册测试工作流失败: %v", err)
	}

	ctx, _ := requestctx.New(context.Background())
	requestctx.SetLocale(ctx, i18n.LocaleENUS)

	var taskItem types.TaskTypeRegistryItem
	for _, item := range manager.ListRegisteredTaskTypes(ctx) {
		if item.TaskType == TypeCacheRefreshRequest {
			taskItem = item
			break
		}
	}
	if taskItem.Description != i18n.MessageByKey(i18n.MsgKeyTaskRegistryTypeCacheRefreshRequestDesc, i18n.LocaleENUS) {
		t.Fatalf("任务类型说明未按语言包返回: %+v", taskItem)
	}
	if strings.Contains(taskItem.Description, "缓存") || strings.Contains(taskItem.UsageHint, "适合") {
		t.Fatalf("任务类型注册表残留中文展示文案: %+v", taskItem)
	}

	var workflowItem types.WorkflowRegistryItem
	for _, item := range manager.ListRegisteredWorkflows(ctx) {
		if item.Name == WorkflowNameCacheRefresh {
			workflowItem = item
			break
		}
	}
	if workflowItem.Description != i18n.MessageByKey(i18n.MsgKeyTaskRegistryWorkflowCacheRefreshDesc, i18n.LocaleENUS) {
		t.Fatalf("工作流说明未按语言包返回: %+v", workflowItem)
	}
	if strings.Contains(workflowItem.Description, "中文") || strings.Contains(workflowItem.UsageHint, "适合") {
		t.Fatalf("工作流注册表残留中文展示文案: %+v", workflowItem)
	}
}

// TestPeriodicNextProcessAtForTaskItem 验证周期任务历史记录能按周期名称或工作流名称补齐下一次执行时间。
func TestPeriodicNextProcessAtForTaskItem(t *testing.T) {
	nextRuns := []periodicNextRun{
		{
			PeriodicName:  "daily-user-tag",
			WorkflowName:  "user_tag.full.rebuild",
			NextProcessAt: "2026-06-08T12:00:00Z",
		},
		{
			PeriodicName:  "archive-admin-log",
			WorkflowName:  "archive.run",
			NextProcessAt: "2026-06-08T13:00:00Z",
		},
	}

	item := types.TaskItem{
		Headers: map[string]string{
			HeaderPeriodicName: "daily-user-tag",
			headerTaskSource:   WorkflowSourcePeriodic,
		},
	}
	if got := periodicNextProcessAtForTaskItem(item, nextRuns); got != "2026-06-08T12:00:00Z" {
		t.Fatalf("期望按周期任务名称补齐下一次执行时间，实际=%q", got)
	}

	item = types.TaskItem{
		Headers: map[string]string{
			headerTaskSource:   WorkflowSourcePeriodic,
			headerWorkflowName: "archive.run",
		},
	}
	if got := periodicNextProcessAtForTaskItem(item, nextRuns); got != "2026-06-08T13:00:00Z" {
		t.Fatalf("期望按工作流名称兜底补齐下一次执行时间，实际=%q", got)
	}

	item = types.TaskItem{
		Payload: json.RawMessage(`{"source":"manual","workflowName":"archive.run"}`),
	}
	if got := periodicNextProcessAtForTaskItem(item, nextRuns); got != "" {
		t.Fatalf("非周期任务不应补齐下一次执行时间，实际=%q", got)
	}
}

// TestEnqueueWorkflowTriggerReservesUniqueBeforeEnqueue 验证触发前会先预占工作流唯一键。
func TestEnqueueWorkflowTriggerReservesUniqueBeforeEnqueue(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterWorkflow(testWorkflowDefinition("demo.workflow")); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	req := &types.TriggerTaskWorkflowReq{
		Name:      "demo.workflow",
		UniqueKey: "same",
	}
	resp, err := manager.EnqueueWorkflowTrigger(context.Background(), req)
	if err != nil {
		t.Fatalf("首次投递工作流触发任务失败: %v", err)
	}
	if resp.WorkflowID == "" {
		t.Fatal("期望回执里包含工作流 ID")
	}
	info, err := manager.inspector.GetTaskInfo(manager.namespacedQueueName(QueueDefault), resp.TaskID)
	if err != nil {
		t.Fatalf("读取工作流触发任务失败: %v", err)
	}
	if info.Retention != taskCompletedRetention {
		t.Fatalf("工作流触发任务 retention=%s，期望=%s", info.Retention, taskCompletedRetention)
	}
	if _, err := manager.EnqueueWorkflowTrigger(context.Background(), req); !errors.Is(err, ErrWorkflowAlreadyExists) {
		t.Fatalf("期望返回 ErrWorkflowAlreadyExists，实际为 %v", err)
	}
	storedID, err := manager.redis.Get(context.Background(), manager.workflowUniqueKey(req.Name, req.UniqueKey)).Result()
	if err != nil {
		t.Fatalf("读取唯一键预占结果失败: %v", err)
	}
	if storedID != resp.WorkflowID {
		t.Fatalf("预占工作流 ID 不符合预期，期望=%s 实际=%s", resp.WorkflowID, storedID)
	}
}

// TestEnqueueWorkflowTriggerReleasesUniqueReservationAfterCanceledEnqueue 验证请求取消导致入队失败后仍会释放当前实例的唯一键预占。
func TestEnqueueWorkflowTriggerReleasesUniqueReservationAfterCanceledEnqueue(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	const (
		workflowName = "demo.workflow.canceled-enqueue"
		uniqueKey    = "retry"
	)
	if err := manager.RegisterWorkflow(testWorkflowDefinition(workflowName)); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}

	realClient := manager.client
	ctx, cancel := context.WithCancel(context.Background())
	manager.client = taskEnqueuerFunc(func(context.Context, *asynq.Task, ...asynq.Option) (*asynq.TaskInfo, error) {
		cancel()
		return nil, context.Canceled
	})
	req := &types.TriggerTaskWorkflowReq{Name: workflowName, UniqueKey: uniqueKey}
	if _, err := manager.EnqueueWorkflowTrigger(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("期望返回 context.Canceled，实际为 %v", err)
	}

	reservationKey := manager.workflowUniqueKey(workflowName, uniqueKey)
	exists, err := manager.redis.Exists(context.Background(), reservationKey).Result()
	if err != nil {
		t.Fatalf("检查唯一键预占失败: %v", err)
	}
	if exists != 0 {
		t.Fatalf("入队失败后唯一键预占仍存在 key=%s", reservationKey)
	}

	manager.client = realClient
	if _, err := manager.EnqueueWorkflowTrigger(context.Background(), req); err != nil {
		t.Fatalf("清理预占后相同业务请求应可立即重试: %v", err)
	}
}

// TestEnqueueWorkflowTriggerUniqueReservationIsAtomic 校验并发触发只由唯一键 SETNX 选出一个工作流实例。
func TestEnqueueWorkflowTriggerUniqueReservationIsAtomic(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	const (
		workflowName = "demo.workflow.concurrent"
		uniqueKey    = "same"
		concurrency  = 16
	)
	if err := manager.RegisterWorkflow(testWorkflowDefinition(workflowName)); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	type enqueueResult struct {
		resp *types.TaskWorkflowTriggerResp // 成功预占者的工作流回执
		err  error                          // 其余竞争者应返回 ErrWorkflowAlreadyExists
	}
	start := make(chan struct{})
	results := make(chan enqueueResult, concurrency)
	var waitGroup sync.WaitGroup
	waitGroup.Add(concurrency)
	for range concurrency {
		go func() {
			defer waitGroup.Done()
			<-start
			// 每个 HTTP/任务调用都有独立 DTO；这里只共享同一业务唯一键来验证 Redis 原子竞争。
			resp, err := manager.EnqueueWorkflowTrigger(context.Background(), &types.TriggerTaskWorkflowReq{
				Name:      workflowName,
				UniqueKey: uniqueKey,
			})
			results <- enqueueResult{resp: resp, err: err}
		}()
	}
	close(start)
	waitGroup.Wait()
	close(results)

	successCount := 0
	conflictCount := 0
	winnerID := ""
	for result := range results {
		switch {
		case result.err == nil:
			successCount++
			if result.resp == nil || result.resp.WorkflowID == "" {
				t.Fatalf("成功预占缺少工作流回执: %+v", result.resp)
			}
			winnerID = result.resp.WorkflowID
		case errors.Is(result.err, ErrWorkflowAlreadyExists):
			conflictCount++
		default:
			t.Fatalf("并发预占返回非预期错误: %v", result.err)
		}
	}
	if successCount != 1 || conflictCount != concurrency-1 {
		t.Fatalf("并发预占结果 success=%d conflict=%d，期望 1/%d", successCount, conflictCount, concurrency-1)
	}
	storedID, err := manager.redis.Get(context.Background(), manager.workflowUniqueKey(workflowName, uniqueKey)).Result()
	if err != nil {
		t.Fatalf("读取并发唯一键预占结果失败: %v", err)
	}
	if storedID != winnerID {
		t.Fatalf("唯一键 owner=%s，期望成功工作流 %s", storedID, winnerID)
	}
}

// TestEnqueueWorkflowTriggerExtendsUniqueReservationForDelayedTrigger 验证延迟触发会延长唯一键保留时间。
func TestEnqueueWorkflowTriggerExtendsUniqueReservationForDelayedTrigger(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterWorkflow(testWorkflowDefinition("demo.workflow")); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	processIn := 3600
	req := &types.TriggerTaskWorkflowReq{
		Name:             "demo.workflow",
		UniqueKey:        "delayed",
		ProcessInSeconds: &processIn,
	}
	resp, err := manager.EnqueueWorkflowTrigger(context.Background(), req)
	if err != nil {
		t.Fatalf("投递延迟工作流触发任务失败: %v", err)
	}
	if resp.WorkflowID == "" {
		t.Fatal("期望回执中包含工作流 ID")
	}

	ttl, err := manager.redis.TTL(context.Background(), manager.workflowUniqueKey(req.Name, req.UniqueKey)).Result()
	if err != nil {
		t.Fatalf("读取唯一键预占 TTL 失败: %v", err)
	}
	if ttl < time.Duration(processIn)*time.Second {
		t.Fatalf("期望唯一键预占 TTL 覆盖延迟执行时间，实际为 %v", ttl)
	}
}

// TestManagerUpdateConfigRefreshesSnapshot 验证更新配置后会同步刷新运行期快照。
func TestManagerUpdateConfigRefreshesSnapshot(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled:      true,
		AppID:        "1",
		DefaultQueue: "maintenance",
		Concurrency:  23,
		Queues: map[string]int{
			"critical": 9,
			"default":  4,
		},
		Scheduler: config.TaskQueueSchedulerConfig{
			LeaseKey: "task:scheduler:new",
		},
		Periodic: []config.TaskPeriodicConfig{
			enabledTaskPeriodicConfig(config.TaskPeriodicConfig{Name: "demo", Cron: "*/5 * * * *", Workflow: "cache.refresh"}),
		},
	})

	if got := manager.CurrentConfig().DefaultQueue; got != "maintenance" {
		t.Fatalf("期望默认队列已更新，实际为 %q", got)
	}
	if got := manager.concurrency(); got != 23 {
		t.Fatalf("期望并发数更新为 23，实际为 %d", got)
	}
	if got := manager.schedulerLeaseKey(); got != "app:1:task:scheduler:new" {
		t.Fatalf("期望调度租约键已更新，实际为 %q", got)
	}
	if got := len(manager.allPeriodicTasks()); got != 1 {
		t.Fatalf("期望更新后只有 1 个周期任务，实际为 %d", got)
	}
}

// TestManagerNamespacesTaskArtifactsByAppID 验证任务队列、工作流键和锁会按 app_id 做命名空间隔离。
func TestManagerNamespacesTaskArtifactsByAppID(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	useRuntimeAppID(t, "215")
	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled:      true,
		AppID:        "215",
		DefaultQueue: QueueDefault,
		Queues: map[string]int{
			QueueDefault:     3,
			QueueMaintenance: 1,
		},
		Scheduler: config.TaskQueueSchedulerConfig{
			LeaseKey: "task:scheduler:leader",
		},
	})

	if got := manager.namespacedQueueName(QueueMaintenance); got != "app:215:maintenance" {
		t.Fatalf("期望 maintenance 队列带站点命名空间，实际为 %q", got)
	}
	if got := manager.namespacedQueueName("app:102:maintenance"); got != "" {
		t.Fatalf("期望其它站点完整队列名失败闭合，实际为 %q", got)
	}
	if got := manager.displayQueueName("app:215:maintenance"); got != QueueMaintenance {
		t.Fatalf("期望展示逻辑队列名 maintenance，实际为 %q", got)
	}
	if got := manager.schedulerLeaseKey(); got != "app:215:task:scheduler:leader" {
		t.Fatalf("期望调度租约键带站点命名空间，实际为 %q", got)
	}
	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled:      true,
		AppID:        "215",
		DefaultQueue: QueueDefault,
		Queues: map[string]int{
			QueueDefault:     3,
			QueueMaintenance: 1,
		},
		Scheduler: config.TaskQueueSchedulerConfig{
			LeaseKey: "app:102:task:scheduler:leader",
		},
	})
	if got := manager.schedulerLeaseKey(); got != "" {
		t.Fatalf("期望其它站点调度租约键失败闭合，实际为 %q", got)
	}
	if got := manager.workflowMetaKey("wf-1"); got != "app:215:task:workflow:wf-1:meta" {
		t.Fatalf("期望工作流元数据键带站点命名空间，实际为 %q", got)
	}
	if got := manager.workflowUniqueKey("user_tag.delta.refresh", "daily"); got != "app:215:task:workflow:unique:user_tag.delta.refresh:daily" {
		t.Fatalf("期望工作流唯一键带站点命名空间，实际为 %q", got)
	}
	weights := manager.queueWeights()
	if weights["app:215:default"] != 3 || weights["app:215:maintenance"] != 1 {
		t.Fatalf("期望队列权重按站点命名空间隔离，实际为 %+v", weights)
	}
}

// TestManagerFailsClosedOnRuntimeAppIDMismatch 确保任务实例不会在错误运行态下写入其它站点命名空间。
func TestManagerFailsClosedOnRuntimeAppIDMismatch(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	useRuntimeAppID(t, "2")
	if manager.IsEnabled() {
		t.Fatal("运行态 app_id 与任务配置不一致时任务系统应失败关闭")
	}
	if got := manager.namespacedQueueName(QueueDefault); got != "" {
		t.Fatalf("mismatched namespacedQueueName() = %q, want empty", got)
	}
	if got := manager.workflowMetaKey("wf-1"); got != "" {
		t.Fatalf("mismatched workflowMetaKey() = %q, want empty", got)
	}
	if err := manager.EnqueueTask(context.Background(), TypeWorkflowNoop, []byte(`{}`)); !errors.Is(err, ErrTaskQueueDisabled) {
		t.Fatalf("mismatched EnqueueTask() error = %v, want ErrTaskQueueDisabled", err)
	}
}

// TestVisibleServerQueuesFiltersByAppID 验证共享 Redis 下 worker 快照只展示当前站点队列。
func TestVisibleServerQueuesFiltersByAppID(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	useRuntimeAppID(t, "2")
	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled: true,
		AppID:   "2",
	})

	queues, visible := manager.visibleServerQueues(map[string]int{
		"app:1:critical":    6,
		"app:1:default":     3,
		"app:2:default":     3,
		"app:2:maintenance": 1,
	})
	if !visible {
		t.Fatal("期望包含当前站点队列的 worker 可见，实际为不可见")
	}
	if len(queues) != 2 || queues[QueueDefault] != 3 || queues[QueueMaintenance] != 1 {
		t.Fatalf("期望只返回当前站点逻辑队列，实际为 %+v", queues)
	}

	_, visible = manager.visibleServerQueues(map[string]int{
		"app:1:critical": 6,
		"app:1:default":  3,
	})
	if visible {
		t.Fatal("仅监听其它站点队列的 worker 不应在当前站点可见")
	}
}

// TestEnqueueWorkflowTriggerRejectsUnknownWorkflowBeforeEnqueue 验证未知工作流会在入队前直接被拒绝。
func TestEnqueueWorkflowTriggerRejectsUnknownWorkflowBeforeEnqueue(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	_, err := manager.EnqueueWorkflowTrigger(context.Background(), &types.TriggerTaskWorkflowReq{Name: "missing.workflow"})
	if !errors.Is(err, ErrWorkflowNotFound) {
		t.Fatalf("期望返回 ErrWorkflowNotFound，实际为 %v", err)
	}
}

// TestEnqueueWorkflowTriggerRejectsWorkflowShardLimitBeforeEnqueue 验证手动触发不会把超过工作流上限的任务写入 Redis。
func TestEnqueueWorkflowTriggerRejectsWorkflowShardLimitBeforeEnqueue(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	definition := testWorkflowDefinition("single-shard.workflow")
	definition.MaxShardTotal = 1
	if err := manager.RegisterWorkflow(definition); err != nil {
		t.Fatalf("注册单分片工作流失败: %v", err)
	}
	_, err := manager.EnqueueWorkflowTrigger(context.Background(), &types.TriggerTaskWorkflowReq{
		Name:       definition.Name,
		ShardTotal: 2,
	})
	if err == nil || !strings.Contains(err.Error(), "max=1") {
		t.Fatalf("EnqueueWorkflowTrigger() error = %v, want workflow shard limit", err)
	}
}

// TestEnqueueWorkflowTriggerRejectsConflictingScheduleOptions 验证 processAt 与 processInSeconds 不能同时设置。
func TestEnqueueWorkflowTriggerRejectsConflictingScheduleOptions(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterWorkflow(testWorkflowDefinition("demo.workflow")); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	_, err := manager.EnqueueWorkflowTrigger(context.Background(), &types.TriggerTaskWorkflowReq{
		Name:             "demo.workflow",
		ProcessAt:        time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
		ProcessInSeconds: new(30),
	})
	if err == nil {
		t.Fatal("期望返回调度参数冲突错误，实际为 nil")
	}
}

// TestStartWorkflowAllowsReservedUniqueReservation 验证已由当前实例预占的唯一键允许继续启动工作流。
func TestStartWorkflowAllowsReservedUniqueReservation(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := testWorkflowDefinition("reserved-unique")
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}

	workflowID := "wf-reserved"
	if locked, err := manager.reserveWorkflowUnique(context.Background(), def.Name, "uniq", workflowID, time.Minute); err != nil || !locked {
		t.Fatalf("预占工作流唯一键失败: locked=%v err=%v", locked, err)
	}

	gotID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		WorkflowID: workflowID,
		Name:       def.Name,
		UniqueKey:  "uniq",
		UniqueTTL:  time.Minute,
	})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	if gotID != workflowID {
		t.Fatalf("期望工作流 ID 为 %s，实际为 %s", workflowID, gotID)
	}
}

// TestEnsureWorkflowUniqueIsOwnerSafe 验证唯一键确认、续期和空 key 抢占由单个原子脚本完成。
func TestEnsureWorkflowUniqueIsOwnerSafe(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	ctx := context.Background()
	const (
		workflowName = "owner-safe-workflow"
		uniqueKey    = "daily"
		firstOwner   = "workflow-a"
		secondOwner  = "workflow-b"
	)
	key := manager.workflowUniqueKey(workflowName, uniqueKey)

	if locked, err := manager.ensureWorkflowUnique(ctx, workflowName, uniqueKey, firstOwner, time.Minute); err != nil || !locked {
		t.Fatalf("空唯一键抢占失败: locked=%v err=%v", locked, err)
	}
	manager.redis.Set(ctx, key, secondOwner, 30*time.Second)
	beforeTTL := manager.redis.PTTL(ctx, key).Val()
	if locked, err := manager.ensureWorkflowUnique(ctx, workflowName, uniqueKey, firstOwner, time.Hour); err != nil || locked {
		t.Fatalf("旧 owner 不应确认新 owner 的唯一键: locked=%v err=%v", locked, err)
	}
	if owner := manager.redis.Get(ctx, key).Val(); owner != secondOwner {
		t.Fatalf("唯一键 owner 被旧实例覆盖: %q", owner)
	}
	if afterTTL := manager.redis.PTTL(ctx, key).Val(); afterTTL > beforeTTL {
		t.Fatalf("旧 owner 不应给新 owner 续期: before=%s after=%s", beforeTTL, afterTTL)
	}
	if locked, err := manager.ensureWorkflowUnique(ctx, workflowName, uniqueKey, secondOwner, time.Hour); err != nil || !locked {
		t.Fatalf("当前 owner 续期失败: locked=%v err=%v", locked, err)
	}
	if ttl := manager.redis.PTTL(ctx, key).Val(); ttl < 59*time.Minute {
		t.Fatalf("当前 owner 续期未生效: ttl=%s", ttl)
	}
}

// TestStartWorkflowRebuildsIncompleteMetadata 验证工作流半初始化元数据会被清理并重建。
func TestStartWorkflowRebuildsIncompleteMetadata(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := testWorkflowDefinition("demo.incomplete.workflow")
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID := "wf-incomplete"
	ctx := context.Background()
	if err := manager.redis.HSet(ctx, manager.workflowMetaKey(workflowID),
		"workflowId", workflowID,
		"workflowName", def.Name,
		"status", WorkflowStatusPending,
	).Err(); err != nil {
		t.Fatalf("准备半初始化工作流 meta 失败: %v", err)
	}

	gotID, err := manager.StartWorkflow(ctx, WorkflowStartSpec{
		WorkflowID: workflowID,
		Name:       def.Name,
	})
	if err != nil {
		t.Fatalf("重建半初始化工作流失败: %v", err)
	}
	if gotID != workflowID {
		t.Fatalf("期望工作流 ID 为 %s，实际为 %s", workflowID, gotID)
	}
	complete, err := manager.workflowMetadataComplete(ctx, workflowID, def)
	if err != nil {
		t.Fatalf("校验重建后的工作流元数据失败: %v", err)
	}
	if !complete {
		t.Fatal("期望工作流元数据已完整重建")
	}
}

// TestStartWorkflowResumesCompletePendingMetadata 验证入口重试会恢复已完整但尚未转为运行态的工作流。
func TestStartWorkflowResumesCompletePendingMetadata(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := testWorkflowDefinition("demo.pending.workflow")
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: def.Name})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	if err = manager.redis.HSet(context.Background(), manager.workflowMetaKey(workflowID), "status", WorkflowStatusPending).Err(); err != nil {
		t.Fatalf("准备 pending 工作流状态失败: %v", err)
	}
	if _, err = manager.StartWorkflow(context.Background(), WorkflowStartSpec{WorkflowID: workflowID, Name: def.Name}); err != nil {
		t.Fatalf("恢复 pending 工作流失败: %v", err)
	}
	if status := manager.redis.HGet(context.Background(), manager.workflowMetaKey(workflowID), "status").Val(); status != WorkflowStatusRunning {
		t.Fatalf("期望工作流恢复为 running，实际为 %s", status)
	}
}

// TestWorkflowProgressionCompletesDAG 验证工作流节点按依赖推进后会正确进入成功终态。
func TestWorkflowProgressionCompletesDAG(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.progress",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				TaskType:     TypeWorkflowNoop,
				BuildPayload: testNoopPayload,
			},
			"final": {
				TaskType:     TypeWorkflowNoop,
				DependsOn:    []string{"root"},
				BuildPayload: testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}

	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: def.Name})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	if err := manager.markTaskSuccess(context.Background(), WorkflowTaskMeta{
		WorkflowID:   workflowID,
		WorkflowName: def.Name,
		WorkflowNode: "root",
	}); err != nil {
		t.Fatalf("标记根节点成功失败: %v", err)
	}
	if err := manager.markTaskSuccess(context.Background(), WorkflowTaskMeta{
		WorkflowID:   workflowID,
		WorkflowName: def.Name,
		WorkflowNode: "final",
	}); err != nil {
		t.Fatalf("标记终节点成功失败: %v", err)
	}

	meta, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取工作流状态失败: %v", err)
	}
	if meta.Status != WorkflowStatusSuccess {
		t.Fatalf("期望工作流状态为 success，实际为 %s", meta.Status)
	}
	if len(nodes) != 2 {
		t.Fatalf("期望存在 2 个工作流节点，实际为 %d", len(nodes))
	}
}

// TestRecordTaskRuntimeFinishUsesDetachedContext 验证任务超时后仍能写入完成耗时快照。
func TestRecordTaskRuntimeFinishUsesDetachedContext(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	ctx, _ = requestctx.New(ctx)
	requestctx.SetTask(ctx, "task-timeout", "demo:timeout", QueueMaintenance)
	cancel()

	attemptToken := manager.recordTaskRuntimeStart(context.Background(), QueueMaintenance, "task-timeout", time.Now().Add(-time.Second))
	manager.recordTaskRuntimeFinish(ctx, QueueMaintenance, "task-timeout", attemptToken, time.Now().Add(-time.Second), errors.New("boom"), nil)

	values, err := manager.redis.HGetAll(context.Background(), manager.taskRuntimeKey(QueueMaintenance, "task-timeout")).Result()
	if err != nil {
		t.Fatalf("读取任务耗时快照失败: %v", err)
	}
	if values["status"] != "failed" {
		t.Fatalf("期望超时后完成快照仍写入 failed，实际为 %q values=%+v", values["status"], values)
	}
	if values["startedAt"] == "" || toInt64(values["startedAtMs"]) <= 0 {
		t.Fatalf("期望完成快照兜底写入开始时间，实际 values=%+v", values)
	}
	if values["finishedAt"] == "" || toInt64(values["durationMs"]) <= 0 {
		t.Fatalf("期望完成时间和耗时已写入，实际 values=%+v", values)
	}
	if !strings.Contains(values["lastErr"], "boom") {
		t.Fatalf("期望失败原因写入 lastErr，实际 values=%+v", values)
	}
	if ttl := manager.redis.TTL(context.Background(), manager.taskRuntimeKey(QueueMaintenance, "task-timeout")).Val(); ttl <= 0 {
		t.Fatalf("任务运行快照 TTL=%s，期望大于 0", ttl)
	}
}

// TestRecordTaskRuntimeFinishBoundsError 验证失败运行快照不会保留无界或敏感错误文本。
func TestRecordTaskRuntimeFinishBoundsError(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	const taskID = "task-bounded-error"
	attemptToken := manager.recordTaskRuntimeStart(context.Background(), QueueMaintenance, taskID, time.Now().Add(-time.Second))
	runErr := errors.New(`{"password":"secret","message":"` + strings.Repeat("错", maxTaskRuntimeErrorRunes+100) + `"}`)
	manager.recordTaskRuntimeFinish(context.Background(), QueueMaintenance, taskID, attemptToken, time.Now().Add(-time.Second), runErr, nil)

	lastErr := manager.redis.HGet(context.Background(), manager.taskRuntimeKey(QueueMaintenance, taskID), "lastErr").Val()
	if strings.Contains(lastErr, "secret") {
		t.Fatalf("失败运行快照泄露敏感字段: %s", lastErr)
	}
	if count := len([]rune(lastErr)); count > maxTaskRuntimeErrorRunes {
		t.Fatalf("失败运行快照错误长度=%d，期望不超过 %d", count, maxTaskRuntimeErrorRunes)
	}
}

// TestTaskRuntimeActiveRetentionDoesNotUseArchiveWindow 验证孤儿 active 快照不会按失败归档窗口长期残留。
func TestTaskRuntimeActiveRetentionDoesNotUseArchiveWindow(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	cfg := manager.CurrentConfig()
	cfg.CompletedRetentionSeconds = 10 * 60
	cfg.ArchivedRetentionSeconds = 7 * 24 * 60 * 60
	manager.UpdateConfig(cfg)
	if got, want := manager.taskRuntimeActiveRetention(), manager.CompletedRetention()+time.Hour; got != want {
		t.Fatalf("active 运行快照保留时间=%s，期望=%s", got, want)
	}
}

// TestRecordTaskRuntimeClearsPreviousFinalFields 验证同一任务 ID 再次运行不会继承旧错误和处理量。
func TestRecordTaskRuntimeClearsPreviousFinalFields(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	ctx := context.Background()
	key := manager.taskRuntimeKey(QueueDefault, "task-reused")
	if err := manager.redis.HSet(ctx, key,
		"status", "failed",
		"finishedAt", "2026-07-18T09:00:00+08:00",
		"durationMs", 99,
		"executionTrace", `{"totalCount":1}`,
		"lastErr", "old failure",
	).Err(); err != nil {
		t.Fatalf("写入旧任务运行快照失败: %v", err)
	}
	manager.recordTaskRuntimeStart(ctx, QueueDefault, "task-reused", time.Now())
	values, err := manager.redis.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatalf("读取重新开始后的任务运行快照失败: %v", err)
	}
	for _, field := range taskRuntimeFinalFields {
		if _, exists := values[field]; exists {
			t.Fatalf("重新开始后仍残留终态字段 %s: %+v", field, values)
		}
	}
	if values["status"] != "running" {
		t.Fatalf("重新开始后状态=%q，期望 running", values["status"])
	}
	if ttl := manager.redis.TTL(ctx, key).Val(); ttl <= 0 {
		t.Fatalf("重新开始后的任务运行快照 TTL=%s，期望大于 0", ttl)
	}
}

// TestTaskRuntimeIsolatesSameIDAcrossQueues 验证不同队列复用 TaskID 时运行快照互不覆盖。
func TestTaskRuntimeIsolatesSameIDAcrossQueues(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	ctx := context.Background()
	const taskID = "shared-task-id"
	defaultStarted := time.Now().Add(-2 * time.Second)
	maintenanceStarted := time.Now().Add(-time.Second)

	defaultToken := manager.recordTaskRuntimeStart(ctx, QueueDefault, taskID, defaultStarted)
	maintenanceToken := manager.recordTaskRuntimeStart(ctx, QueueMaintenance, taskID, maintenanceStarted)
	manager.recordTaskRuntimeFinish(ctx, QueueDefault, taskID, defaultToken, defaultStarted, errors.New("default failed"), nil)
	manager.recordTaskRuntimeFinish(ctx, QueueMaintenance, taskID, maintenanceToken, maintenanceStarted, nil, nil)
	defaultRecord := manager.readTaskRuntime(ctx, QueueDefault, taskID)
	maintenanceRecord := manager.readTaskRuntime(ctx, QueueMaintenance, taskID)
	if defaultRecord.DurationMS <= maintenanceRecord.DurationMS {
		t.Fatalf("跨队列运行快照未独立保存: default=%+v maintenance=%+v", defaultRecord, maintenanceRecord)
	}
	defaultValues := manager.redis.HGetAll(ctx, manager.taskRuntimeKey(QueueDefault, taskID)).Val()
	maintenanceValues := manager.redis.HGetAll(ctx, manager.taskRuntimeKey(QueueMaintenance, taskID)).Val()
	if !strings.Contains(defaultValues["lastErr"], "default failed") || maintenanceValues["lastErr"] != "" {
		t.Fatalf("跨队列终态字段发生污染: default=%+v maintenance=%+v", defaultValues, maintenanceValues)
	}
}

// TestTaskRuntimeRejectsStaleAttemptFinish 验证租约丢失后的迟到实例不会覆盖新 attempt 快照。
func TestTaskRuntimeRejectsStaleAttemptFinish(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	ctx := context.Background()
	const taskID = "leased-task"
	oldStarted := time.Now().Add(-2 * time.Second)
	newStarted := time.Now().Add(-time.Second)
	oldToken := manager.recordTaskRuntimeStart(ctx, QueueMaintenance, taskID, oldStarted)
	newToken := manager.recordTaskRuntimeStart(ctx, QueueMaintenance, taskID, newStarted)

	manager.recordTaskRuntimeFinish(ctx, QueueMaintenance, taskID, oldToken, oldStarted, errors.New("stale failure"), nil)
	values := manager.redis.HGetAll(ctx, manager.taskRuntimeKey(QueueMaintenance, taskID)).Val()
	if values["attemptToken"] != newToken || values["status"] != "running" || values["lastErr"] != "" {
		t.Fatalf("迟到 attempt 覆盖了新快照: %+v", values)
	}

	manager.recordTaskRuntimeFinish(ctx, QueueMaintenance, taskID, newToken, newStarted, nil, nil)
	values = manager.redis.HGetAll(ctx, manager.taskRuntimeKey(QueueMaintenance, taskID)).Val()
	if values["attemptToken"] != newToken || values["status"] != "success" || values["finishedAt"] == "" {
		t.Fatalf("当前 attempt 未正确写入终态: %+v", values)
	}
}

// TestDeleteSuccessfulTaskRuntimeRejectsStaleAttempt 验证迟到实例不能删除新 attempt 的运行快照。
func TestDeleteSuccessfulTaskRuntimeRejectsStaleAttempt(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	ctx := context.Background()
	taskID := "task-stale-success-cleanup"
	oldToken := manager.recordTaskRuntimeStart(ctx, QueueMaintenance, taskID, time.Now().Add(-time.Second))
	newToken := manager.recordTaskRuntimeStart(ctx, QueueMaintenance, taskID, time.Now())

	if err := manager.deleteSuccessfulTaskRuntime(ctx, QueueMaintenance, taskID, oldToken); err != nil {
		t.Fatalf("迟到实例删除运行快照失败: %v", err)
	}
	if token := manager.redis.HGet(ctx, manager.taskRuntimeKey(QueueMaintenance, taskID), "attemptToken").Val(); token != newToken {
		t.Fatalf("迟到实例误删或覆盖新 attempt token=%q want=%q", token, newToken)
	}
	if err := manager.deleteSuccessfulTaskRuntime(ctx, QueueMaintenance, taskID, newToken); err != nil {
		t.Fatalf("当前实例删除运行快照失败: %v", err)
	}
	if exists := manager.redis.Exists(ctx, manager.taskRuntimeKey(QueueMaintenance, taskID)).Val(); exists != 0 {
		t.Fatalf("当前成功 attempt 的重复运行快照未删除 exists=%d", exists)
	}
}

// TestRecordTaskRuntimeHeartbeatWritesLiveStats 验证执行中任务会更新耗时和处理量，且不会提前写入终态字段。
func TestRecordTaskRuntimeHeartbeatWritesLiveStats(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	ctx := context.Background()
	const taskID = "runtime-heartbeat"
	begin := time.Now().Add(-6 * time.Second)
	attemptToken := manager.recordTaskRuntimeStart(ctx, QueueMaintenance, taskID, begin)
	snapshot := &taskstats.Snapshot{
		Name:        "archive.orders",
		StartedAt:   begin.Format(time.RFC3339),
		FinishedAt:  time.Now().Format(time.RFC3339),
		DurationMS:  6_000,
		TotalCount:  12,
		UpdateCount: 12,
		Details: []taskstats.Detail{
			{Action: taskstats.ActionUpdate, Name: "orders.rows", Count: 12, Times: 2},
		},
	}

	written, err := manager.recordTaskRuntimeHeartbeat(
		ctx,
		QueueMaintenance,
		taskID,
		attemptToken,
		begin,
		time.Now(),
		snapshot,
	)
	if err != nil || !written {
		t.Fatalf("写入任务运行指标心跳失败: written=%t err=%v", written, err)
	}
	values := manager.redis.HGetAll(ctx, manager.taskRuntimeKey(QueueMaintenance, taskID)).Val()
	record := manager.readTaskRuntime(ctx, QueueMaintenance, taskID)
	if values["status"] != "running" || values["finishedAt"] != "" || values["lastErr"] != "" {
		t.Fatalf("任务运行指标心跳污染终态字段: %+v", values)
	}
	if record.DurationMS < 5_000 || record.ExecutionTrace == nil || record.ExecutionTrace.UpdateCount != 12 {
		t.Fatalf("任务运行指标心跳内容异常: %+v", record)
	}
	if ttl := manager.redis.TTL(ctx, manager.taskRuntimeKey(QueueMaintenance, taskID)).Val(); ttl <= 0 {
		t.Fatalf("任务运行指标心跳 TTL=%s，期望大于 0", ttl)
	}
	if taskRuntimeHeartbeatInterval != 5*time.Second {
		t.Fatalf("任务运行指标心跳间隔=%s，期望 5s", taskRuntimeHeartbeatInterval)
	}
}

// TestRecordTaskRuntimeHeartbeatRejectsStaleAttempt 验证旧执行实例的心跳不会覆盖新实例。
func TestRecordTaskRuntimeHeartbeatRejectsStaleAttempt(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	ctx := context.Background()
	const taskID = "runtime-heartbeat-stale"
	oldBegin := time.Now().Add(-10 * time.Second)
	oldToken := manager.recordTaskRuntimeStart(ctx, QueueDefault, taskID, oldBegin)
	newBegin := time.Now().Add(-time.Second)
	newToken := manager.recordTaskRuntimeStart(ctx, QueueDefault, taskID, newBegin)

	written, err := manager.recordTaskRuntimeHeartbeat(
		ctx,
		QueueDefault,
		taskID,
		oldToken,
		oldBegin,
		time.Now(),
		&taskstats.Snapshot{TotalCount: 99, ReadCount: 99},
	)
	if err != nil || written {
		t.Fatalf("旧任务实例心跳未被拒绝: written=%t err=%v", written, err)
	}
	values := manager.redis.HGetAll(ctx, manager.taskRuntimeKey(QueueDefault, taskID)).Val()
	if values["attemptToken"] != newToken || values["status"] != "running" || values["executionTrace"] != "" {
		t.Fatalf("旧任务实例心跳覆盖新实例: %+v", values)
	}
}

// TestReadTaskRuntimeBatchPreservesQueueIdentity 验证列表批量读取按队列隔离同名任务并解析处理量。
func TestReadTaskRuntimeBatchPreservesQueueIdentity(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	ctx := context.Background()
	const taskID = "runtime-batch"
	defaultKey := manager.taskRuntimeKey(QueueDefault, taskID)
	maintenanceKey := manager.taskRuntimeKey(QueueMaintenance, taskID)
	if err := manager.redis.HSet(ctx, defaultKey, map[string]any{
		"durationMs":     "1000",
		"executionTrace": `{"totalCount":2,"readCount":2}`,
	}).Err(); err != nil {
		t.Fatalf("写入 default 运行快照失败: %v", err)
	}
	if err := manager.redis.HSet(ctx, maintenanceKey, map[string]any{
		"durationMs":     "2000",
		"executionTrace": `{"totalCount":3,"updateCount":3}`,
	}).Err(); err != nil {
		t.Fatalf("写入 maintenance 运行快照失败: %v", err)
	}
	infos := []*asynq.TaskInfo{
		{ID: taskID, Queue: manager.namespacedQueueName(QueueDefault)},
		{ID: taskID, Queue: manager.namespacedQueueName(QueueMaintenance)},
	}
	records := manager.readTaskRuntimeBatch(ctx, infos)
	defaultRecord := records[taskRuntimeIdentity(infos[0].Queue, taskID)]
	maintenanceRecord := records[taskRuntimeIdentity(infos[1].Queue, taskID)]
	if defaultRecord.DurationMS != 1000 || defaultRecord.ExecutionTrace == nil || defaultRecord.ExecutionTrace.ReadCount != 2 {
		t.Fatalf("default 批量运行快照异常: %+v", defaultRecord)
	}
	if maintenanceRecord.DurationMS != 2000 || maintenanceRecord.ExecutionTrace == nil || maintenanceRecord.ExecutionTrace.UpdateCount != 3 {
		t.Fatalf("maintenance 批量运行快照异常: %+v", maintenanceRecord)
	}
}

// TestBoundedTaskStatsSnapshotPreservesTotals 验证处理量快照裁剪明细时保留聚合计数并限制序列化大小。
func TestBoundedTaskStatsSnapshotPreservesTotals(t *testing.T) {
	details := make([]taskstats.Detail, 0, 1000)
	for index := 0; index < cap(details); index++ {
		details = append(details, taskstats.Detail{Action: "read", Name: strings.Repeat("明细", 300), Count: int64(index + 1)})
	}
	original := &taskstats.Snapshot{
		Name:       strings.Repeat("任务", 300),
		StartedAt:  strings.Repeat("s", 100_000),
		FinishedAt: strings.Repeat("f", 100_000),
		TotalCount: 9_999,
		ReadCount:  9_999,
		Details:    details,
	}
	bounded := boundedTaskStatsSnapshot(original)
	raw, err := json.Marshal(bounded)
	if err != nil {
		t.Fatalf("序列化受控处理量快照失败: %v", err)
	}
	if len(raw) > maxTaskExecutionTraceBytes || bounded.TotalCount != original.TotalCount || bounded.ReadCount != original.ReadCount || len(bounded.Details) > maxTaskExecutionTraceDetails {
		t.Fatalf("处理量快照限制异常: bytes=%d bounded=%+v", len(raw), bounded)
	}
	if len(original.Details) != 1000 || original.Name == bounded.Name {
		t.Fatalf("处理量快照限制不应修改原始快照")
	}
}

// TestMarkTaskFailureUsesDetachedContext 验证业务 ctx 已取消时仍能把工作流节点标记为失败。
func TestMarkTaskFailureUsesDetachedContext(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name:         "dag.detached.failure",
		DefaultQueue: QueueMaintenance,
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:         "root",
				TaskType:     TypeWorkflowNoop,
				BuildPayload: testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		Name:       def.Name,
		Queue:      QueueMaintenance,
		ShardTotal: 1,
	})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx, _ = requestctx.New(ctx)
	requestctx.SetWorkflow(ctx, workflowID, def.Name, "root", 0, 1)
	cancel()

	markErr := manager.markTaskFailureWithFinalContext(ctx, WorkflowTaskMeta{
		WorkflowID:   workflowID,
		WorkflowName: def.Name,
		WorkflowNode: "root",
		ShardIndex:   0,
		ShardTotal:   1,
	}, errors.New("boom"))
	if markErr != nil {
		t.Fatalf("超时 ctx 下标记工作流失败失败: %v", markErr)
	}

	meta, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取工作流状态失败: %v", err)
	}
	if meta.Status != WorkflowStatusFailed || !strings.Contains(meta.ErrorMessage, "boom") {
		t.Fatalf("期望工作流失败状态已写入，实际 meta=%+v", meta)
	}
	if len(nodes) != 1 || nodes[0].Status != NodeStatusFailed || nodes[0].Failed != 1 || !strings.Contains(nodes[0].ErrorMessage, "boom") {
		t.Fatalf("期望节点失败状态已写入，实际 nodes=%+v", nodes)
	}
}

// TestEffectiveRetryNodeCanBlockWorkflowRetryOverride 验证非幂等节点可以禁止全局重试覆盖。
func TestEffectiveRetryNodeCanBlockWorkflowRetryOverride(t *testing.T) {
	got := effectiveRetry(&WorkflowNodeDefinition{MaxRetry: -1}, WorkflowStartSpec{RetryOverride: new(3)})
	if got != 0 {
		t.Fatalf("期望非幂等节点强制关闭重试，实际为 %d", got)
	}
}

// TestNonIdempotentWorkflowNodeDisablesAllRetries 验证非幂等节点不会被编排重试预算重新开启重试。
func TestNonIdempotentWorkflowNodeDisablesAllRetries(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.non-idempotent",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:         "root",
				TaskType:     TypeWorkflowNoop,
				MaxRetry:     -1,
				BuildPayload: testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	retry := 3
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: def.Name, RetryOverride: &retry})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	info, err := manager.inspector.GetTaskInfo(manager.namespacedQueueName(QueueDefault), workflowNodeTaskID(workflowID, "root", 0))
	if err != nil {
		t.Fatalf("读取非幂等节点任务失败: %v", err)
	}
	if info.MaxRetry != 0 {
		t.Fatalf("非幂等节点不应保留任何重试预算，实际为 %d", info.MaxRetry)
	}
}

// TestWorkflowSpecByIDRestoresSchedulingOverrides 验证工作流推进后仍能继承触发时的重试和超时覆盖。
func TestWorkflowSpecByIDRestoresSchedulingOverrides(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := testWorkflowDefinition("demo.restore.overrides")
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	retry := 0
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		Name:            def.Name,
		PeriodicName:    "daily-demo",
		RetryOverride:   &retry,
		TimeoutOverride: 37 * time.Second,
	})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}

	got, err := manager.workflowSpecByID(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("还原工作流启动参数失败: %v", err)
	}
	if got.RetryOverride == nil || *got.RetryOverride != retry {
		t.Fatalf("期望保留 retryOverride=%d，实际为 %#v", retry, got.RetryOverride)
	}
	if got.TimeoutOverride != 37*time.Second {
		t.Fatalf("期望保留 timeoutOverride=37s，实际为 %v", got.TimeoutOverride)
	}
	if got.PeriodicName != "daily-demo" {
		t.Fatalf("期望保留周期任务名称，实际为 %q", got.PeriodicName)
	}
}

// TestWorkflowShardedSuccessDedupesDuplicateShardAck 验证同一分片重复回写成功时不会把节点成功数重复累加。
func TestWorkflowShardedSuccessDedupesDuplicateShardAck(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.sharded.dedupe",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:             "root",
				TaskType:         TypeWorkflowNoop,
				SupportsSharding: true,
				BuildPayload:     testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}

	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		Name:       def.Name,
		ShardTotal: 2,
	})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}

	successMeta := WorkflowTaskMeta{
		WorkflowID:   workflowID,
		WorkflowName: def.Name,
		WorkflowNode: "root",
		ShardTotal:   2,
	}
	if err := manager.markTaskSuccess(context.Background(), successMeta); err != nil {
		t.Fatalf("首次标记分片 0 成功失败: %v", err)
	}
	nodeDone, err := manager.redis.HGet(context.Background(), manager.workflowNodeKey(workflowID, "root"), workflowNodeInstanceField(0)).Result()
	if err != nil {
		t.Fatalf("读取节点 hash 内分片去重字段失败: %v", err)
	}
	if nodeDone != "succeeded" {
		t.Fatalf("期望分片终态标记记录成功结果，实际为 %s", nodeDone)
	}
	if err := manager.markTaskSuccess(context.Background(), successMeta); err != nil {
		t.Fatalf("重复标记分片 0 成功失败: %v", err)
	}

	meta, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取工作流状态失败: %v", err)
	}
	if meta.Status != WorkflowStatusRunning {
		t.Fatalf("期望重复回写后工作流仍处于 running，实际为 %s", meta.Status)
	}
	if len(nodes) != 1 || nodes[0].Succeeded != 1 {
		t.Fatalf("期望重复回写不重复累加成功数，实际节点状态为 %+v", nodes)
	}

	successMeta.ShardIndex = 1
	if err := manager.markTaskSuccess(context.Background(), successMeta); err != nil {
		t.Fatalf("标记分片 1 成功失败: %v", err)
	}

	meta, nodes, err = manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取工作流最终状态失败: %v", err)
	}
	if meta.Status != WorkflowStatusSuccess {
		t.Fatalf("期望工作流最终成功，实际为 %s", meta.Status)
	}
	if len(nodes) != 1 || nodes[0].Succeeded != 2 || nodes[0].Status != NodeStatusSuccess {
		t.Fatalf("期望节点按 2 个分片成功完成，实际为 %+v", nodes)
	}
}

// TestWorkflowRetryReplenishesMissingShard 验证工作流入口重试会按稳定任务 ID 补齐缺失分片。
func TestWorkflowRetryReplenishesMissingShard(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.sharded.replenish",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:             "root",
				TaskType:         TypeWorkflowNoop,
				SupportsSharding: true,
				BuildPayload:     testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: def.Name, ShardTotal: 3})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	queue := manager.namespacedQueueName(QueueDefault)
	completedTaskID := workflowNodeTaskID(workflowID, "root", 0)
	missingTaskID := workflowNodeTaskID(workflowID, "root", 1)
	if err = manager.markTaskSuccess(context.Background(), WorkflowTaskMeta{
		WorkflowID: workflowID, WorkflowName: def.Name, WorkflowNode: "root", ShardIndex: 0, ShardTotal: 3,
	}); err != nil {
		t.Fatalf("记录已完成分片失败: %v", err)
	}
	if err = manager.inspector.DeleteTask(queue, completedTaskID); err != nil {
		t.Fatalf("删除已完成分片的任务留存失败: %v", err)
	}
	if err = manager.inspector.DeleteTask(queue, missingTaskID); err != nil {
		t.Fatalf("删除待补齐分片失败: %v", err)
	}
	if _, err = manager.StartWorkflow(context.Background(), WorkflowStartSpec{WorkflowID: workflowID, Name: def.Name, ShardTotal: 3}); err != nil {
		t.Fatalf("重试工作流补齐分片失败: %v", err)
	}
	if _, err = manager.inspector.GetTaskInfo(queue, completedTaskID); err == nil {
		t.Fatalf("已完成分片不应因任务留存清理而重复投递 task_id=%s", completedTaskID)
	}
	for shardIndex := 1; shardIndex < 3; shardIndex++ {
		taskID := workflowNodeTaskID(workflowID, "root", shardIndex)
		if _, err = manager.inspector.GetTaskInfo(queue, taskID); err != nil {
			t.Fatalf("分片任务未补齐 task_id=%s: %v", taskID, err)
		}
	}
}

// TestWorkflowRootPartialEnqueueRetryReplenishesMissingShards 验证根节点部分投递后按稳定 ID 自动补齐。
func TestWorkflowRootPartialEnqueueRetryReplenishesMissingShards(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.root.partial.enqueue",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:             "root",
				TaskType:         TypeWorkflowNoop,
				SupportsSharding: true,
				BuildPayload:     testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	base := manager.client
	manager.client = &failNthEnqueuer{delegate: base, failAt: 2}
	retry := 0
	spec := WorkflowStartSpec{WorkflowID: "wf-root-partial", Name: def.Name, ShardTotal: 3, RetryOverride: &retry}
	if _, err := manager.StartWorkflow(context.Background(), spec); err == nil || !strings.Contains(err.Error(), "injected enqueue failure") {
		t.Fatalf("期望第二个根分片投递失败，实际=%v", err)
	}

	queue := manager.namespacedQueueName(QueueDefault)
	first, err := manager.inspector.GetTaskInfo(queue, workflowNodeTaskID(spec.WorkflowID, "root", 0))
	if err != nil {
		t.Fatalf("首个根分片应已投递: %v", err)
	}
	if first.MaxRetry != workflowTaskRetryBudget(0) {
		t.Fatalf("retry=0 的分片仍应预留编排重试，实际=%d 期望=%d", first.MaxRetry, workflowTaskRetryBudget(0))
	}
	for shardIndex := 1; shardIndex < 3; shardIndex++ {
		if _, err = manager.inspector.GetTaskInfo(queue, workflowNodeTaskID(spec.WorkflowID, "root", shardIndex)); !errors.Is(err, asynq.ErrTaskNotFound) {
			t.Fatalf("故障后的分片不应已入队 shard=%d err=%v", shardIndex, err)
		}
	}

	manager.client = base
	if _, err = manager.StartWorkflow(context.Background(), spec); err != nil {
		t.Fatalf("触发重试补齐根分片失败: %v", err)
	}
	for shardIndex := 0; shardIndex < 3; shardIndex++ {
		if _, err = manager.inspector.GetTaskInfo(queue, workflowNodeTaskID(spec.WorkflowID, "root", shardIndex)); err != nil {
			t.Fatalf("根分片未补齐 shard=%d err=%v", shardIndex, err)
		}
	}
}

// TestWorkflowDispatchRetryExhaustionFailsAndFencesLateShard 验证编排重试耗尽会失败收口并归档晚到分片。
func TestWorkflowDispatchRetryExhaustionFailsAndFencesLateShard(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.dispatch.exhausted",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:             "root",
				TaskType:         TypeWorkflowNoop,
				SupportsSharding: true,
				BuildPayload:     testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	base := manager.client
	manager.client = &failFromEnqueuer{delegate: base, failFrom: 2}
	spec := WorkflowStartSpec{
		WorkflowID: "wf-dispatch-exhausted",
		Name:       def.Name,
		ShardTotal: 3,
		UniqueKey:  "periodic:dispatch-exhausted",
		UniqueTTL:  time.Hour,
	}
	for attempt := 0; attempt <= workflowRepairRetryCount; attempt++ {
		if _, err := manager.StartWorkflow(context.Background(), spec); !errors.Is(err, ErrWorkflowDispatch) {
			t.Fatalf("第 %d 次持续投递失败应返回编排错误，实际=%v", attempt+1, err)
		}
	}
	if owner := manager.redis.Get(context.Background(), manager.workflowUniqueKey(spec.Name, spec.UniqueKey)).Val(); owner != spec.WorkflowID {
		t.Fatalf("终态收口前唯一键 owner 异常: %s", owner)
	}

	trigger := asynq.NewTaskWithHeaders(TypeWorkflowTrigger, mustJSONBytes(WorkflowTriggerPayload{
		WorkflowID: spec.WorkflowID, WorkflowName: spec.Name, UniqueKey: spec.UniqueKey, UniqueTTLSeconds: int(spec.UniqueTTL / time.Second),
	}), map[string]string{headerWorkflowID: spec.WorkflowID, headerWorkflowName: spec.Name})
	terminalErr := errors.Join(ErrWorkflowDispatch, asynq.SkipRetry, errors.New("persistent enqueue failure"))
	manager.handleTaskError(context.Background(), trigger, terminalErr)

	meta, _, err := manager.readWorkflowStatus(context.Background(), spec.WorkflowID)
	if err != nil {
		t.Fatalf("读取终态工作流失败: %v", err)
	}
	if meta.Status != WorkflowStatusFailed {
		t.Fatalf("编排重试耗尽后工作流未失败收口: %+v", meta)
	}
	if exists := manager.redis.Exists(context.Background(), manager.workflowUniqueKey(spec.Name, spec.UniqueKey)).Val(); exists != 0 {
		t.Fatal("编排失败终态仍占用周期唯一键")
	}

	manager.client = base
	queue := manager.namespacedQueueName(QueueDefault)
	lateInfo, err := manager.inspector.GetTaskInfo(queue, workflowNodeTaskID(spec.WorkflowID, "root", 0))
	if err != nil {
		t.Fatalf("读取故障前已投递分片失败: %v", err)
	}
	lateTask := asynq.NewTaskWithHeaders(lateInfo.Type, lateInfo.Payload, lateInfo.Headers)
	var businessCalls atomic.Int32
	runErr := manager.traceAndLogMiddleware()(asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		businessCalls.Add(1)
		return nil
	})).ProcessTask(context.Background(), lateTask)
	if !errors.Is(runErr, asynq.SkipRetry) {
		t.Fatalf("晚到未执行分片应进入归档，实际=%v", runErr)
	}
	if businessCalls.Load() != 0 {
		t.Fatalf("失败终态后的晚到分片仍执行业务: %d", businessCalls.Load())
	}
	manager.handleTaskError(context.Background(), lateTask, runErr)
	outcome := manager.redis.HGet(context.Background(), manager.workflowNodeKey(spec.WorkflowID, "root"), workflowNodeInstanceField(0)).Val()
	if outcome != "failed" {
		t.Fatalf("晚到未执行分片未记录可重跑失败终态 outcome=%q", outcome)
	}
	meta, _, err = manager.readWorkflowStatus(context.Background(), spec.WorkflowID)
	if err != nil || meta.Status != WorkflowStatusFailed {
		t.Fatalf("晚到成功分片反转工作流终态 meta=%+v err=%v", meta, err)
	}
}

// TestWorkflowArchivedRerunBackfillsMissingShardsAfterEarlyFailure 验证早期失败不会让部分投递缺失分片永久丢失。
func TestWorkflowArchivedRerunBackfillsMissingShardsAfterEarlyFailure(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.child.partial.early.failure",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:         "root",
				TaskType:     TypeWorkflowNoop,
				BuildPayload: testNoopPayload,
			},
			"child": {
				Name:             "child",
				TaskType:         TypeWorkflowNoop,
				DependsOn:        []string{"root"},
				SupportsSharding: true,
				BuildPayload:     testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		WorkflowID: "wf-child-partial-early-failure",
		Name:       def.Name,
		ShardTotal: 3,
	})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	base := manager.client
	enqueueCalls := 0
	manager.client = taskEnqueuerFunc(func(ctx context.Context, task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
		enqueueCalls++
		if enqueueCalls == 2 {
			return nil, errors.New("injected child shard enqueue failure")
		}
		info, enqueueErr := base.EnqueueContext(ctx, task, opts...)
		if enqueueErr != nil {
			return nil, enqueueErr
		}
		if enqueueCalls == 1 {
			if _, markErr := manager.markTaskFailure(ctx, WorkflowTaskMeta{
				WorkflowID:   workflowID,
				WorkflowName: def.Name,
				WorkflowNode: "child",
				ShardIndex:   0,
				ShardTotal:   3,
			}, errors.New("child shard 0 failed before remaining dispatch")); markErr != nil {
				return nil, markErr
			}
		}
		return info, nil
	})
	err = manager.markTaskSuccess(context.Background(), WorkflowTaskMeta{
		WorkflowID:   workflowID,
		WorkflowName: def.Name,
		WorkflowNode: "root",
		ShardTotal:   1,
	})
	if !errors.Is(err, ErrWorkflowDispatch) {
		t.Fatalf("下游部分投递故障应返回编排错误，实际=%v", err)
	}
	manager.client = base
	queue := manager.namespacedQueueName(QueueDefault)
	if _, err = manager.inspector.GetTaskInfo(queue, workflowNodeTaskID(workflowID, "child", 1)); !errors.Is(err, asynq.ErrTaskNotFound) {
		t.Fatalf("故障窗口内 child shard 1 应尚未投递，实际=%v", err)
	}
	failedInfo, err := manager.inspector.GetTaskInfo(queue, workflowNodeTaskID(workflowID, "child", 0))
	if err != nil {
		t.Fatalf("读取已投递失败分片失败: %v", err)
	}
	manager.client = &failNthEnqueuer{delegate: base, failAt: 3}
	if err = manager.prepareWorkflowArchivedTaskRerun(context.Background(), failedInfo); !errors.Is(err, ErrWorkflowDispatch) {
		t.Fatalf("首次手工重跑补投部分失败应保留编排错误，实际=%v", err)
	}
	if _, err = manager.inspector.GetTaskInfo(queue, workflowNodeTaskID(workflowID, "child", 1)); err != nil {
		t.Fatalf("首次手工重跑应已补入 child shard 1: %v", err)
	}
	if _, err = manager.inspector.GetTaskInfo(queue, workflowNodeTaskID(workflowID, "child", 2)); !errors.Is(err, asynq.ErrTaskNotFound) {
		t.Fatalf("注入故障后 child shard 2 应仍缺失，实际=%v", err)
	}
	manager.client = base
	if err = manager.prepareWorkflowArchivedTaskRerun(context.Background(), failedInfo); err != nil {
		t.Fatalf("重试准备失败分片后补齐剩余分片失败: %v", err)
	}
	for shard := 0; shard < 3; shard++ {
		if _, err = manager.inspector.GetTaskInfo(queue, workflowNodeTaskID(workflowID, "child", shard)); err != nil {
			t.Fatalf("手工重跑未补齐 child shard %d: %v", shard, err)
		}
	}
	for shard := 0; shard < 3; shard++ {
		if err = manager.markTaskSuccess(context.Background(), WorkflowTaskMeta{
			WorkflowID:   workflowID,
			WorkflowName: def.Name,
			WorkflowNode: "child",
			ShardIndex:   shard,
			ShardTotal:   3,
		}); err != nil {
			t.Fatalf("回写 child shard %d 成功结果失败: %v", shard, err)
		}
	}
	meta, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取恢复后工作流状态失败: %v", err)
	}
	if meta.Status != WorkflowStatusSuccess || len(nodes) != 2 {
		t.Fatalf("补齐并完成所有分片后工作流未成功 meta=%+v nodes=%+v", meta, nodes)
	}
}

// TestWorkflowDownstreamPartialEnqueueRetrySkipsSucceededBusiness 验证业务成功后的技术重试只补齐下游分片。
func TestWorkflowDownstreamPartialEnqueueRetrySkipsSucceededBusiness(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.child.partial.enqueue",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {Name: "root", TaskType: TypeWorkflowNoop, BuildPayload: testNoopPayload},
			"child": {
				Name:             "child",
				TaskType:         TypeWorkflowNoop,
				DependsOn:        []string{"root"},
				SupportsSharding: true,
				BuildPayload:     testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	retry := 0
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		WorkflowID: "wf-child-partial", Name: def.Name, ShardTotal: 3, RetryOverride: &retry,
	})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	queue := manager.namespacedQueueName(QueueDefault)
	rootInfo, err := manager.inspector.GetTaskInfo(queue, workflowNodeTaskID(workflowID, "root", 0))
	if err != nil {
		t.Fatalf("读取根节点任务失败: %v", err)
	}
	rootTask := asynq.NewTaskWithHeaders(rootInfo.Type, rootInfo.Payload, rootInfo.Headers)
	var businessCalls atomic.Int32
	wrapped := manager.traceAndLogMiddleware()(asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		businessCalls.Add(1)
		return nil
	}))

	base := manager.client
	manager.client = &failNthEnqueuer{delegate: base, failAt: 2}
	if err = wrapped.ProcessTask(context.Background(), rootTask); err == nil || !strings.Contains(err.Error(), "injected enqueue failure") {
		t.Fatalf("期望下游第二个分片投递失败，实际=%v", err)
	}
	if errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("编排投递错误不应占用 retry=0 的业务终态: %v", err)
	}

	manager.client = base
	if err = wrapped.ProcessTask(context.Background(), rootTask); err != nil {
		t.Fatalf("技术重试补齐下游分片失败: %v", err)
	}
	if businessCalls.Load() != 1 {
		t.Fatalf("已成功根分片不应重复执行业务，实际执行=%d", businessCalls.Load())
	}
	for shardIndex := 0; shardIndex < 3; shardIndex++ {
		if _, err = manager.inspector.GetTaskInfo(queue, workflowNodeTaskID(workflowID, "child", shardIndex)); err != nil {
			t.Fatalf("下游分片未补齐 shard=%d err=%v", shardIndex, err)
		}
	}
	_, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取工作流状态失败: %v", err)
	}
	for _, node := range nodes {
		if node.Name == "root" && (node.Succeeded != 1 || node.Status != NodeStatusSuccess) {
			t.Fatalf("根节点重复 ack 后计数漂移: %+v", node)
		}
	}
}

// TestWorkflowBusinessRetryZeroMarksTerminalFailure 验证业务 retry=0 仍首次失败即进入工作流终态。
func TestWorkflowBusinessRetryZeroMarksTerminalFailure(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := testWorkflowDefinition("dag.business.retry.zero")
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	retry := 0
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		WorkflowID: "wf-business-retry-zero", Name: def.Name, RetryOverride: &retry,
	})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	queue := manager.namespacedQueueName(QueueDefault)
	info, err := manager.inspector.GetTaskInfo(queue, workflowNodeTaskID(workflowID, "root", 0))
	if err != nil {
		t.Fatalf("读取根节点任务失败: %v", err)
	}
	if info.MaxRetry != workflowTaskRetryBudget(0) {
		t.Fatalf("技术重试预算不符合预期: %d", info.MaxRetry)
	}
	task := asynq.NewTaskWithHeaders(info.Type, info.Payload, info.Headers)
	runErr := manager.traceAndLogMiddleware()(asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return errors.New("business failed")
	})).ProcessTask(context.Background(), task)
	if !errors.Is(runErr, asynq.SkipRetry) {
		t.Fatalf("retry=0 的首次业务失败应立即终止重试，实际=%v", runErr)
	}
	manager.handleTaskError(context.Background(), task, runErr)

	meta, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取失败工作流状态失败: %v", err)
	}
	if meta.Status != WorkflowStatusFailed || len(nodes) != 1 || nodes[0].Status != NodeStatusFailed || nodes[0].Failed != 1 {
		t.Fatalf("业务终态未正确回写 workflow=%+v nodes=%+v", meta, nodes)
	}
}

// TestWorkflowBusinessFailureCounterErrorIsDispatchFailure 验证业务失败计数写入异常走编排故障收口。
func TestWorkflowBusinessFailureCounterErrorIsDispatchFailure(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	task := asynq.NewTaskWithHeaders(TypeWorkflowNoop, nil, map[string]string{
		headerWorkflowID:    "wf-counter-error",
		headerWorkflowName:  "dag.counter.error",
		headerWorkflowNode:  "root",
		headerShardIndex:    "0",
		headerBusinessRetry: "1",
	})
	if err := manager.redis.Close(); err != nil {
		t.Fatalf("关闭测试 Redis 失败: %v", err)
	}
	runErr := manager.applyWorkflowBusinessRetryLimit(context.Background(), task, errors.New("business failed"))
	if !errors.Is(runErr, ErrWorkflowDispatch) {
		t.Fatalf("业务失败计数写入异常应标记为编排故障，实际=%v", runErr)
	}
}

// TestWorkflowFinalizedParentCanReplenishChildShard 验证父节点重复完成回写仍会补齐下游缺失分片。
func TestWorkflowFinalizedParentCanReplenishChildShard(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.child.replenish",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {Name: "root", TaskType: TypeWorkflowNoop, BuildPayload: testNoopPayload},
			"child": {
				Name:             "child",
				TaskType:         TypeWorkflowNoop,
				DependsOn:        []string{"root"},
				SupportsSharding: true,
				BuildPayload:     testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: def.Name, ShardTotal: 2})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	rootMeta := WorkflowTaskMeta{WorkflowID: workflowID, WorkflowName: def.Name, WorkflowNode: "root", ShardTotal: 1}
	if err = manager.markTaskSuccess(context.Background(), rootMeta); err != nil {
		t.Fatalf("完成根节点失败: %v", err)
	}
	queue := manager.namespacedQueueName(QueueDefault)
	missingTaskID := workflowNodeTaskID(workflowID, "child", 1)
	if err = manager.inspector.DeleteTask(queue, missingTaskID); err != nil {
		t.Fatalf("删除下游分片失败: %v", err)
	}
	if err = manager.markTaskSuccess(context.Background(), rootMeta); err != nil {
		t.Fatalf("重复完成根节点补齐下游失败: %v", err)
	}
	if _, err = manager.inspector.GetTaskInfo(queue, missingTaskID); err != nil {
		t.Fatalf("下游分片未补齐 task_id=%s: %v", missingTaskID, err)
	}
}

// TestWorkflowShardedStatsVisibleInStatus 验证分片处理量会写入工作流状态并按节点聚合。
func TestWorkflowShardedStatsVisibleInStatus(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.sharded.stats.status",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:             "root",
				TaskType:         TypeWorkflowNoop,
				SupportsSharding: true,
				BuildPayload:     testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}

	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		Name:       def.Name,
		ShardTotal: 2,
	})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}

	shardSnapshots := []*taskstats.Snapshot{
		{
			Name:       "root.0",
			TotalCount: 3,
			ReadCount:  3,
			Details: []taskstats.Detail{
				{Action: taskstats.ActionRead, Name: "source.rows", Count: 3, Times: 1},
			},
		},
		{
			Name:        "root.1",
			TotalCount:  5,
			UpdateCount: 5,
			Details: []taskstats.Detail{
				{Action: taskstats.ActionUpdate, Name: "target.rows", Count: 5, Times: 1},
			},
		},
	}
	for shard, snapshot := range shardSnapshots {
		meta := WorkflowTaskMeta{
			WorkflowID:   workflowID,
			WorkflowName: def.Name,
			WorkflowNode: "root",
			ShardIndex:   shard,
			ShardTotal:   2,
		}
		attemptToken := fmt.Sprintf("workflow-stats-%d", shard)
		if err := manager.startWorkflowTaskStatsAttempt(context.Background(), meta, attemptToken); err != nil {
			t.Fatalf("初始化分片 %d 处理量 attempt 失败: %v", shard, err)
		}
		if err := manager.recordWorkflowTaskStats(context.Background(), meta, attemptToken, snapshot); err != nil {
			t.Fatalf("记录分片 %d 处理量失败: %v", shard, err)
		}
		if err := manager.markTaskSuccess(context.Background(), meta); err != nil {
			t.Fatalf("标记分片 %d 成功失败: %v", shard, err)
		}
	}

	meta, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取工作流状态失败: %v", err)
	}
	if meta.Status != WorkflowStatusSuccess {
		t.Fatalf("期望工作流最终成功，实际为 %s", meta.Status)
	}
	if len(nodes) != 1 {
		t.Fatalf("期望只有 root 节点，实际为 %+v", nodes)
	}
	node := nodes[0]
	if node.Progress == nil || node.Progress.Total != 2 || node.Progress.Succeeded != 2 || node.Progress.Percent != 100 {
		t.Fatalf("期望节点进度按 2 个成功分片聚合，实际为 %+v", node.Progress)
	}
	if node.ExecutionTrace == nil || node.ExecutionTrace.TotalCount != 8 || node.ExecutionTrace.ReadCount != 3 || node.ExecutionTrace.UpdateCount != 5 {
		t.Fatalf("期望节点处理量按分片聚合，实际为 %+v", node.ExecutionTrace)
	}
	if len(node.ShardTraces) != 2 {
		t.Fatalf("期望返回 2 个分片处理量明细，实际为 %+v", node.ShardTraces)
	}
	for shard, trace := range node.ShardTraces {
		if trace.ShardIndex != shard || trace.ShardTotal != 2 || trace.Status != NodeStatusSuccess {
			t.Fatalf("期望分片 %d 终态和索引正确，实际为 %+v", shard, trace)
		}
		if trace.Progress == nil || trace.Progress.Succeeded != 1 || trace.Progress.Percent != 100 {
			t.Fatalf("期望分片 %d 进度成功完成，实际为 %+v", shard, trace.Progress)
		}
		if trace.ExecutionTrace == nil || trace.ExecutionTrace.TotalCount != shardSnapshots[shard].TotalCount {
			t.Fatalf("期望分片 %d 保留原始处理量，实际为 %+v", shard, trace.ExecutionTrace)
		}
	}
}

// TestWorkflowTaskStatsRejectsStaleAttempt 验证旧重试不能覆盖新实例的工作流分片处理量。
func TestWorkflowTaskStatsRejectsStaleAttempt(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	ctx := context.Background()
	meta := WorkflowTaskMeta{
		WorkflowID:   "workflow-stats-stale",
		WorkflowNode: "root",
		ShardIndex:   1,
		ShardTotal:   2,
	}
	const (
		oldAttempt = "attempt-old"
		newAttempt = "attempt-new"
	)
	if err := manager.startWorkflowTaskStatsAttempt(ctx, meta, oldAttempt); err != nil {
		t.Fatalf("初始化旧处理量 attempt 失败: %v", err)
	}
	if err := manager.recordWorkflowTaskStats(ctx, meta, oldAttempt, &taskstats.Snapshot{TotalCount: 3, ReadCount: 3}); err != nil {
		t.Fatalf("写入旧处理量失败: %v", err)
	}
	if err := manager.startWorkflowTaskStatsAttempt(ctx, meta, newAttempt); err != nil {
		t.Fatalf("初始化新处理量 attempt 失败: %v", err)
	}
	traceField := workflowNodeTraceField(meta.ShardIndex)
	nodeKey := manager.workflowNodeKey(meta.WorkflowID, meta.WorkflowNode)
	if manager.redis.HExists(ctx, nodeKey, traceField).Val() {
		t.Fatal("新处理量 attempt 未清理上一轮快照")
	}
	if err := manager.recordWorkflowTaskStats(ctx, meta, oldAttempt, &taskstats.Snapshot{TotalCount: 99, ReadCount: 99}); err != nil {
		t.Fatalf("迟到旧处理量写入返回错误: %v", err)
	}
	if manager.redis.HExists(ctx, nodeKey, traceField).Val() {
		t.Fatal("迟到旧处理量覆盖了新 attempt")
	}
	if err := manager.recordWorkflowTaskStats(ctx, meta, newAttempt, &taskstats.Snapshot{TotalCount: 5, UpdateCount: 5}); err != nil {
		t.Fatalf("写入新处理量失败: %v", err)
	}
	var trace WorkflowNodeShardTraceItem
	if err := json.Unmarshal([]byte(manager.redis.HGet(ctx, nodeKey, traceField).Val()), &trace); err != nil {
		t.Fatalf("解析新处理量失败: %v", err)
	}
	if trace.ExecutionTrace == nil || trace.ExecutionTrace.TotalCount != 5 || trace.ExecutionTrace.UpdateCount != 5 {
		t.Fatalf("新处理量内容异常: %+v", trace.ExecutionTrace)
	}
}

// TestWorkflowShardFailureIgnoresLateSuccessAck 验证失败终态不会被迟到成功回执反转。
func TestWorkflowShardFailureIgnoresLateSuccessAck(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.sharded.failure.success.repair",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:             "root",
				TaskType:         TypeWorkflowNoop,
				SupportsSharding: true,
				BuildPayload:     testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		Name:       def.Name,
		ShardTotal: 1,
	})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	meta := WorkflowTaskMeta{
		WorkflowID:   workflowID,
		WorkflowName: def.Name,
		WorkflowNode: "root",
		ShardIndex:   0,
		ShardTotal:   1,
	}
	failureRecorded, err := manager.markTaskFailure(context.Background(), meta, errors.New("first failed"))
	if err != nil {
		t.Fatalf("标记分片失败失败: %v", err)
	}
	if !failureRecorded {
		t.Fatal("首次分片失败应记录终态")
	}
	if err := manager.markTaskSuccess(context.Background(), meta); err != nil {
		t.Fatalf("迟到成功回执应被幂等忽略: %v", err)
	}
	wf, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取工作流状态失败: %v", err)
	}
	if wf.Status != WorkflowStatusFailed || len(nodes) != 1 || nodes[0].Status != NodeStatusFailed || nodes[0].Succeeded != 0 || nodes[0].Failed != 1 {
		t.Fatalf("迟到成功回执反转了失败终态 wf=%+v nodes=%+v", wf, nodes)
	}
}

// TestWorkflowNodeOutcomeFailureWinsLateSuccessRace 验证成功回执预读 running 后，节点失败终态仍不可被反转。
func TestWorkflowNodeOutcomeFailureWinsLateSuccessRace(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := testWorkflowDefinition("dag.sharded.failure.success.race")
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: def.Name})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	meta := WorkflowTaskMeta{
		WorkflowID:   workflowID,
		WorkflowName: def.Name,
		WorkflowNode: "root",
		ShardTotal:   1,
	}
	failed, err := manager.workflowFailed(context.Background(), workflowID)
	if err != nil || failed {
		t.Fatalf("成功回执预读时工作流应为 running failed=%v err=%v", failed, err)
	}
	nodeKey := manager.workflowNodeKey(workflowID, "root")
	result, err := recordNodeOutcomeScript.Run(context.Background(), manager.redis, []string{nodeKey}, workflowNodeInstanceField(0), "failed").Int()
	if err != nil || result != 1 {
		t.Fatalf("并发失败回执写入节点终态失败 result=%d err=%v", result, err)
	}
	if err = manager.markTaskSuccess(context.Background(), meta); err != nil {
		t.Fatalf("迟到成功回执应被节点 CAS 忽略: %v", err)
	}
	marker := manager.redis.HGet(context.Background(), nodeKey, workflowNodeInstanceField(0)).Val()
	succeeded := manager.redis.HGet(context.Background(), nodeKey, "succeeded").Val()
	failedCount := manager.redis.HGet(context.Background(), nodeKey, "failed").Val()
	if marker != "failed" || succeeded != "0" || failedCount != "1" {
		t.Fatalf("迟到成功回执反转了节点失败终态 marker=%q succeeded=%q failed=%q", marker, succeeded, failedCount)
	}
	failureRecorded, err := manager.markTaskFailure(context.Background(), meta, errors.New("failed before success ack"))
	if err != nil {
		t.Fatalf("重放失败回执收口工作流失败: %v", err)
	}
	if failureRecorded {
		t.Fatal("同一失败终态重放不应重复增加失败计数")
	}
	wf, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取竞态后工作流状态失败: %v", err)
	}
	if wf.Status != WorkflowStatusFailed || len(nodes) != 1 || nodes[0].Status != NodeStatusFailed || nodes[0].Succeeded != 0 || nodes[0].Failed != 1 {
		t.Fatalf("失败回执重放未收口工作流 wf=%+v nodes=%+v", wf, nodes)
	}
}

// TestWorkflowShardSuccessIgnoresLateFailureAck 验证成功分片遇到迟到失败回执时不会把节点和工作流回滚为失败。
func TestWorkflowShardSuccessIgnoresLateFailureAck(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.sharded.success.late.failure",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:             "root",
				TaskType:         TypeWorkflowNoop,
				SupportsSharding: true,
				BuildPayload:     testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		Name:       def.Name,
		ShardTotal: 1,
	})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	meta := WorkflowTaskMeta{
		WorkflowID:   workflowID,
		WorkflowName: def.Name,
		WorkflowNode: "root",
		ShardIndex:   0,
		ShardTotal:   1,
	}
	if err := manager.markTaskSuccess(context.Background(), meta); err != nil {
		t.Fatalf("标记分片成功失败: %v", err)
	}
	failureRecorded, err := manager.markTaskFailure(context.Background(), meta, errors.New("late failed"))
	if err != nil {
		t.Fatalf("迟到失败回写不应返回错误: %v", err)
	}
	if failureRecorded {
		t.Fatal("成功后的迟到失败不应重复记录终态")
	}
	wf, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取工作流状态失败: %v", err)
	}
	if wf.Status != WorkflowStatusSuccess || len(nodes) != 1 || nodes[0].Status != NodeStatusSuccess || nodes[0].Succeeded != 1 || nodes[0].Failed != 0 {
		t.Fatalf("期望迟到失败不回滚成功终态，实际 wf=%+v nodes=%+v", wf, nodes)
	}
}

// TestWorkflowArchivedRerunRepairsMultipleFailedShards 验证多个归档失败分片可逐个修复。
func TestWorkflowArchivedRerunRepairsMultipleFailedShards(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.sharded.rerun.repair",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:             "root",
				TaskType:         TypeWorkflowNoop,
				SupportsSharding: true,
				BuildPayload:     testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		Name:       def.Name,
		ShardTotal: 2,
	})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	for shard := 0; shard < 2; shard++ {
		failureRecorded, err := manager.markTaskFailure(context.Background(), WorkflowTaskMeta{
			WorkflowID:   workflowID,
			WorkflowName: def.Name,
			WorkflowNode: "root",
			ShardIndex:   shard,
			ShardTotal:   2,
		}, errors.Errorf("boom shard=%d", shard))
		if err != nil {
			t.Fatalf("标记分片 %d 失败失败: %v", shard, err)
		}
		if !failureRecorded {
			t.Fatalf("首次标记分片 %d 失败应记录终态", shard)
		}
		if err = manager.redis.HSet(context.Background(), manager.workflowNodeKey(workflowID, "root"), workflowNodeBusinessFailureField(shard), 2).Err(); err != nil {
			t.Fatalf("写入分片 %d 业务失败次数失败: %v", shard, err)
		}
	}
	for shard := 0; shard < 2; shard++ {
		payload, err := testNoopPayload(WorkflowStartSpec{WorkflowID: workflowID, Name: def.Name}, def.Nodes["root"], shard, 2)
		if err != nil {
			t.Fatalf("构造分片 %d 负载失败: %v", shard, err)
		}
		info := &asynq.TaskInfo{
			Payload: payload,
			Headers: map[string]string{
				headerWorkflowID:   workflowID,
				headerWorkflowName: def.Name,
				headerWorkflowNode: "root",
				headerShardIndex:   strconv.Itoa(shard),
				headerShardTotal:   "2",
			},
		}
		if err := manager.prepareWorkflowArchivedTaskRerun(context.Background(), info); err != nil {
			t.Fatalf("修复分片 %d 归档重跑状态失败: %v", shard, err)
		}
	}
	meta, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取修复后工作流状态失败: %v", err)
	}
	if meta.Status != WorkflowStatusRunning {
		t.Fatalf("期望工作流恢复 running，实际 meta=%+v", meta)
	}
	if len(nodes) != 1 || nodes[0].Failed != 0 || nodes[0].Status != NodeStatusRunning {
		t.Fatalf("期望所有失败分片计数被撤销，实际 nodes=%+v", nodes)
	}
	if manualRerun := manager.redis.HGet(context.Background(), manager.workflowMetaKey(workflowID), "manualRerun").Val(); manualRerun != "1" {
		t.Fatalf("期望多分片修复共用手工重跑窗口，实际 manualRerun=%q", manualRerun)
	}
	stateKeys, err := manager.workflowStateKeys(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取手工重跑状态 key 失败: %v", err)
	}
	for _, key := range stateKeys {
		if ttl := manager.redis.TTL(context.Background(), key).Val(); ttl <= 0 || ttl > manager.workflowManualRerunRetention() {
			t.Fatalf("手工重跑运行态 key 应保留安全 TTL key=%s ttl=%s", key, ttl)
		}
	}
	for shard := 0; shard < 2; shard++ {
		if marker := manager.redis.HGet(context.Background(), manager.workflowNodeKey(workflowID, "root"), workflowNodeInstanceField(shard)).Val(); marker != workflowNodeOutcomeRerunPrepared {
			t.Fatalf("分片 %d 未进入可重入的重跑准备态 marker=%q", shard, marker)
		}
		if exists := manager.redis.HExists(context.Background(), manager.workflowNodeKey(workflowID, "root"), workflowNodeBusinessFailureField(shard)).Val(); exists {
			t.Fatalf("分片 %d 业务失败次数未被重置", shard)
		}
	}
	for shard := 0; shard < 2; shard++ {
		if err := manager.markTaskSuccess(context.Background(), WorkflowTaskMeta{
			WorkflowID:   workflowID,
			WorkflowName: def.Name,
			WorkflowNode: "root",
			ShardIndex:   shard,
			ShardTotal:   2,
		}); err != nil {
			t.Fatalf("重跑成功回写分片 %d 失败: %v", shard, err)
		}
	}
	meta, nodes, err = manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取重跑完成工作流状态失败: %v", err)
	}
	if meta.Status != WorkflowStatusSuccess || len(nodes) != 1 || nodes[0].Status != NodeStatusSuccess || nodes[0].Succeeded != 2 || nodes[0].Failed != 0 {
		t.Fatalf("期望多失败分片重跑后推进成功，实际 meta=%+v nodes=%+v", meta, nodes)
	}
	if exists := manager.redis.HExists(context.Background(), manager.workflowMetaKey(workflowID), "manualRerun").Val(); exists {
		t.Fatal("工作流再次进入终态后应清除手工重跑标记")
	}
	for _, key := range stateKeys {
		if ttl := manager.redis.TTL(context.Background(), key).Val(); ttl <= 0 {
			t.Fatalf("手工重跑完成后应恢复终态 TTL key=%s ttl=%s", key, ttl)
		}
	}
}

// TestWorkflowArchivedRerunRejectsSuccessWorkflow 验证成功终态无法被 archived 分片修复入口复活。
func TestWorkflowArchivedRerunRejectsSuccessWorkflow(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := testWorkflowDefinition("dag.archived.rerun.reject.success")
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: def.Name})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	nodeKey := manager.workflowNodeKey(workflowID, "root")
	if _, err = recordNodeOutcomeScript.Run(context.Background(), manager.redis, []string{nodeKey}, workflowNodeInstanceField(0), "failed").Result(); err != nil {
		t.Fatalf("构造失败分片标记失败: %v", err)
	}
	if err = manager.updateWorkflowNode(context.Background(), workflowID, "root", map[string]any{"status": NodeStatusFailed}); err != nil {
		t.Fatalf("构造失败节点状态失败: %v", err)
	}
	if err = manager.completeWorkflow(context.Background(), workflowID); err != nil {
		t.Fatalf("构造成功工作流终态失败: %v", err)
	}
	payload, err := testNoopPayload(WorkflowStartSpec{WorkflowID: workflowID, Name: def.Name}, def.Nodes["root"], 0, 1)
	if err != nil {
		t.Fatalf("构造归档任务负载失败: %v", err)
	}
	err = manager.prepareWorkflowArchivedTaskRerun(context.Background(), &asynq.TaskInfo{
		Payload: payload,
		Headers: map[string]string{
			headerWorkflowID:   workflowID,
			headerWorkflowName: def.Name,
			headerWorkflowNode: "root",
			headerShardIndex:   "0",
			headerShardTotal:   "1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "成功工作流不允许手工重跑") {
		t.Fatalf("成功工作流应拒绝 archived 重跑，实际 err=%v", err)
	}
	status := manager.redis.HGet(context.Background(), manager.workflowMetaKey(workflowID), "status").Val()
	marker := manager.redis.HGet(context.Background(), nodeKey, workflowNodeInstanceField(0)).Val()
	if status != WorkflowStatusSuccess || marker != "failed" {
		t.Fatalf("拒绝重跑后状态被修改 status=%q marker=%q", status, marker)
	}
}

// TestWorkflowRunningStateOnlyExpiresAfterTerminal 验证运行态长期保留，进入终态后才统一设置保留时间。
func TestWorkflowRunningStateOnlyExpiresAfterTerminal(t *testing.T) {
	manager, server, cleanup := newTestManagerWithServer(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.running.state.retention",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:         "root",
				TaskType:     TypeWorkflowNoop,
				BuildPayload: testNoopPayload,
			},
			"child": {
				Name:         "child",
				TaskType:     TypeWorkflowNoop,
				DependsOn:    []string{"root"},
				BuildPayload: testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: def.Name})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	stateKeys, err := manager.workflowStateKeys(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取工作流状态 key 失败: %v", err)
	}
	for _, key := range stateKeys {
		if ttl := manager.redis.TTL(context.Background(), key).Val(); ttl >= 0 {
			t.Fatalf("运行态 key 不应设置 TTL key=%s ttl=%s", key, ttl)
		}
	}
	server.FastForward(taskCompletedRetention + time.Hour)
	for _, key := range stateKeys {
		if exists := manager.redis.Exists(context.Background(), key).Val(); exists != 1 {
			t.Fatalf("运行态 key 被保留时间误删 key=%s", key)
		}
	}
	if err = manager.failWorkflow(context.Background(), workflowID, "stop for retention test"); err != nil {
		t.Fatalf("结束工作流失败: %v", err)
	}
	for _, key := range stateKeys {
		if ttl := manager.redis.TTL(context.Background(), key).Val(); ttl <= 0 {
			t.Fatalf("终态 key 应设置 TTL key=%s ttl=%s", key, ttl)
		}
	}
	server.FastForward(manager.workflowTerminalRetention(WorkflowStatusFailed) + time.Hour)
	for _, key := range stateKeys {
		if exists := manager.redis.Exists(context.Background(), key).Val(); exists != 0 {
			t.Fatalf("终态保留时间后 key 仍存在 key=%s", key)
		}
	}
}

// TestRefreshWorkflowManualRerunRetentionRestoresConcurrentTerminalTTL 验证并发终态覆盖后不会被迟到刷新延长。
func TestRefreshWorkflowManualRerunRetentionRestoresConcurrentTerminalTTL(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := testWorkflowDefinition("dag.manual.rerun.concurrent.terminal.ttl")
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: def.Name})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	if _, err = manager.markTaskFailure(context.Background(), WorkflowTaskMeta{
		WorkflowID:   workflowID,
		WorkflowName: def.Name,
		WorkflowNode: "root",
		ShardTotal:   1,
	}, errors.New("prepare concurrent terminal")); err != nil {
		t.Fatalf("构造失败工作流失败: %v", err)
	}
	stateKeys, err := manager.workflowStateKeys(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取工作流状态 key 失败: %v", err)
	}
	if _, err = reopenWorkflowForManualRerunScript.Run(
		context.Background(),
		manager.redis,
		[]string{manager.workflowMetaKey(workflowID)},
		time.Now().Format(time.RFC3339),
		manager.workflowManualRerunRetention().Milliseconds(),
	).Result(); err != nil {
		t.Fatalf("构造并发手工重跑窗口失败: %v", err)
	}
	if err = manager.completeWorkflow(context.Background(), workflowID); err != nil {
		t.Fatalf("构造并发成功终态失败: %v", err)
	}
	err = manager.refreshWorkflowManualRerunRetention(context.Background(), workflowID)
	if err == nil || !strings.Contains(err.Error(), "已进入终态") {
		t.Fatalf("迟到刷新应识别并发终态，实际 err=%v", err)
	}
	for _, key := range stateKeys {
		if ttl := manager.redis.TTL(context.Background(), key).Val(); ttl <= 0 {
			t.Fatalf("并发终态 key 的 TTL 未恢复 key=%s ttl=%s", key, ttl)
		}
	}
}

// TestWorkflowPreparedRerunArchivesAgainAfterConcurrentFailure 验证准备态任务遇到并发失败会重新归档而非假成功。
func TestWorkflowPreparedRerunArchivesAgainAfterConcurrentFailure(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := testWorkflowDefinition("dag.manual.rerun.concurrent.failure")
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: def.Name})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	meta := WorkflowTaskMeta{
		WorkflowID:   workflowID,
		WorkflowName: def.Name,
		WorkflowNode: "root",
		ShardTotal:   1,
	}
	if _, err = manager.markTaskFailure(context.Background(), meta, errors.New("first failure")); err != nil {
		t.Fatalf("构造失败工作流失败: %v", err)
	}
	payload, err := testNoopPayload(WorkflowStartSpec{WorkflowID: workflowID, Name: def.Name}, def.Nodes["root"], 0, 1)
	if err != nil {
		t.Fatalf("构造任务负载失败: %v", err)
	}
	info := &asynq.TaskInfo{Payload: payload, Headers: map[string]string{
		headerWorkflowID:   workflowID,
		headerWorkflowName: def.Name,
		headerWorkflowNode: "root",
		headerShardIndex:   "0",
		headerShardTotal:   "1",
	}}
	if err = manager.prepareWorkflowArchivedTaskRerun(context.Background(), info); err != nil {
		t.Fatalf("准备归档重跑失败: %v", err)
	}
	if err = manager.failWorkflow(context.Background(), workflowID, "concurrent failure"); err != nil {
		t.Fatalf("构造并发失败终态失败: %v", err)
	}
	task := manager.newTask(context.Background(), TypeWorkflowNoop, payload, info.Headers)
	settled, err := manager.workflowTaskSettled(context.Background(), task)
	if settled || !errors.Is(err, asynq.SkipRetry) {
		t.Fatalf("准备态任务遇到并发失败应重新归档 settled=%v err=%v", settled, err)
	}
}

// TestWorkflowFailedShardArchivesUnsettledSiblingsAndRerunsToSuccess 验证并发失败后每个分片都有可恢复终态。
func TestWorkflowFailedShardArchivesUnsettledSiblingsAndRerunsToSuccess(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.sharded.concurrent.failure.recovery",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:             "root",
				TaskType:         TypeWorkflowNoop,
				SupportsSharding: true,
				BuildPayload:     testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: def.Name, ShardTotal: 3})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	meta := func(shard int) WorkflowTaskMeta {
		return WorkflowTaskMeta{
			WorkflowID:   workflowID,
			WorkflowName: def.Name,
			WorkflowNode: "root",
			ShardIndex:   shard,
			ShardTotal:   3,
		}
	}
	if _, err = manager.markTaskFailure(context.Background(), meta(0), errors.New("shard 0 failed")); err != nil {
		t.Fatalf("标记首个分片失败失败: %v", err)
	}
	if err = manager.markTaskSuccess(context.Background(), meta(2)); err != nil {
		t.Fatalf("工作流并发失败后记录已完成业务分片成功结果失败: %v", err)
	}
	pendingPayload, err := testNoopPayload(WorkflowStartSpec{WorkflowID: workflowID, Name: def.Name}, def.Nodes["root"], 1, 3)
	if err != nil {
		t.Fatalf("构造未执行分片负载失败: %v", err)
	}
	pendingTask := manager.newTask(context.Background(), TypeWorkflowNoop, pendingPayload, map[string]string{
		headerWorkflowID:   workflowID,
		headerWorkflowName: def.Name,
		headerWorkflowNode: "root",
		headerShardIndex:   "1",
		headerShardTotal:   "3",
	})
	settled, settledErr := manager.workflowTaskSettled(context.Background(), pendingTask)
	if settled || !errors.Is(settledErr, asynq.SkipRetry) {
		t.Fatalf("失败工作流中的未执行分片应转归档 settled=%v err=%v", settled, settledErr)
	}
	if recorded, markErr := manager.markTaskFailure(context.Background(), meta(1), settledErr); markErr != nil || !recorded {
		t.Fatalf("未执行分片归档终态写入失败 recorded=%v err=%v", recorded, markErr)
	}
	_, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取并发失败后的工作流状态失败: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Succeeded != 1 || nodes[0].Failed != 2 {
		t.Fatalf("所有分片终态未完整记录 nodes=%+v", nodes)
	}
	for _, shard := range []int{0, 1} {
		payload, payloadErr := testNoopPayload(WorkflowStartSpec{WorkflowID: workflowID, Name: def.Name}, def.Nodes["root"], shard, 3)
		if payloadErr != nil {
			t.Fatalf("构造失败分片 %d 重跑负载失败: %v", shard, payloadErr)
		}
		info := &asynq.TaskInfo{Payload: payload, Headers: map[string]string{
			headerWorkflowID:   workflowID,
			headerWorkflowName: def.Name,
			headerWorkflowNode: "root",
			headerShardIndex:   strconv.Itoa(shard),
			headerShardTotal:   "3",
		}}
		if err = manager.prepareWorkflowArchivedTaskRerun(context.Background(), info); err != nil {
			t.Fatalf("准备失败分片 %d 重跑失败: %v", shard, err)
		}
	}
	for _, shard := range []int{0, 1} {
		if err = manager.markTaskSuccess(context.Background(), meta(shard)); err != nil {
			t.Fatalf("回写重跑分片 %d 成功结果失败: %v", shard, err)
		}
	}
	wf, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取重跑完成工作流状态失败: %v", err)
	}
	if wf.Status != WorkflowStatusSuccess || len(nodes) != 1 || nodes[0].Status != NodeStatusSuccess || nodes[0].Succeeded != 3 || nodes[0].Failed != 0 {
		t.Fatalf("全部失败分片重跑后工作流未完成 wf=%+v nodes=%+v", wf, nodes)
	}
}

// TestWorkflowGraySkipRootStillSchedulesChild 验证根节点因灰度全跳过时，DAG 仍会继续推进下游节点。
func TestWorkflowGraySkipRootStillSchedulesChild(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	def := &WorkflowDefinition{
		Name: "dag.gray.skip",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:             "root",
				TaskType:         TypeWorkflowNoop,
				SupportsSharding: true,
				SupportsGray:     true,
				BuildPayload:     testNoopPayload,
			},
			"child": {
				Name:         "child",
				TaskType:     TypeWorkflowNoop,
				DependsOn:    []string{"root"},
				BuildPayload: testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}

	workflowID := findWorkflowIDWithoutGrayShard(t, "root", 4, 1)
	gotID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		WorkflowID:  workflowID,
		Name:        def.Name,
		ShardTotal:  4,
		GrayPercent: 1,
	})
	if err != nil {
		t.Fatalf("启动灰度工作流失败: %v", err)
	}
	if gotID != workflowID {
		t.Fatalf("期望工作流 ID 保持不变，实际为 %s", gotID)
	}

	meta, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取工作流状态失败: %v", err)
	}
	if meta.Status != WorkflowStatusRunning {
		t.Fatalf("期望工作流仍在等待子节点执行，实际为 %s", meta.Status)
	}
	if len(nodes) != 2 {
		t.Fatalf("期望存在 2 个节点状态，实际为 %d", len(nodes))
	}
	if nodes[0].Name != "child" || nodes[0].Status != NodeStatusRunning || nodes[0].Expected != 1 {
		t.Fatalf("期望 child 节点已被推进执行，实际为 %+v", nodes[0])
	}
	if nodes[1].Name != "root" || nodes[1].Status != NodeStatusSkipped || nodes[1].Skipped != 4 {
		t.Fatalf("期望 root 节点按灰度整体跳过，实际为 %+v", nodes[1])
	}
}

// TestTaskRetryMovesTaskToRetryQueue 验证任务失败重试后会进入重试队列。
func TestTaskRetryMovesTaskToRetryQueue(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	var called atomic.Int32
	if err := manager.RegisterHandler("demo:retry", asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		called.Add(1)
		return errors.New("boom")
	})); err != nil {
		t.Fatalf("注册重试测试处理器失败: %v", err)
	}
	if err := manager.StartWorker(); err != nil {
		t.Fatalf("启动 worker 失败: %v", err)
	}
	defer func() { _ = manager.Stop(context.Background()) }()

	if err := manager.EnqueueTask(context.Background(), "demo:retry", []byte(`{}`), svc.WithTaskRetry(1)); err != nil {
		t.Fatalf("投递任务失败: %v", err)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		return called.Load() >= 1
	})
	waitForCondition(t, 5*time.Second, func() bool {
		info, err := manager.inspector.GetQueueInfo(manager.namespacedQueueName(QueueDefault))
		return err == nil && info.Retry >= 1
	})
}

// TestTaskRetryDoesNotSetTaskHashTTL 验证仍会重试的失败任务不会被项目层设置 task hash TTL。
func TestTaskRetryDoesNotSetTaskHashTTL(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterHandler("demo:retry-ttl", asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return errors.New("retry later")
	})); err != nil {
		t.Fatalf("注册重试 task hash 测试处理器失败: %v", err)
	}
	if err := manager.StartWorker(); err != nil {
		t.Fatalf("启动 worker 失败: %v", err)
	}
	defer func() { _ = manager.Stop(context.Background()) }()

	resp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType: "demo:retry-ttl",
		Payload:  json.RawMessage(`{"demo":true}`),
	})
	if err != nil {
		t.Fatalf("投递重试 task hash 测试任务失败: %v", err)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		info, infoErr := manager.inspector.GetTaskInfo(manager.namespacedQueueName(QueueDefault), resp.TaskID)
		return infoErr == nil && info.State == asynq.TaskStateRetry
	})

	ttl := manager.redis.TTL(context.Background(), keys.TaskAsynqTaskHashKey(manager.namespacedQueueName(QueueDefault), resp.TaskID)).Val()
	if ttl != -1 {
		t.Fatalf("重试任务不应设置项目 task hash TTL，实际 TTL=%s", ttl)
	}
}

// TestFinalFailureHookOnlyRunsForArchivedTask 验证中间重试失败不触发通知，进入 archived 终态后才触发通知钩子。
func TestFinalFailureHookOnlyRunsForArchivedTask(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	var retryCalled atomic.Int32
	var hookCalls atomic.Int32
	type hookSnapshot struct {
		taskType   string // taskType 表示测试字段。
		workflowID string // workflowID 表示测试字段。
		mode       string // mode 表示测试字段。
		errText    string // errText 表示测试字段。
	}
	hookCh := make(chan hookSnapshot, 2)
	if err := manager.RegisterFinalFailureHook(func(ctx context.Context, task *asynq.Task, meta WorkflowTaskMeta, runErr error) error {
		hookCalls.Add(1)
		snapshot := hookSnapshot{
			workflowID: strings.TrimSpace(meta.WorkflowID),
		}
		if task != nil {
			snapshot.taskType = task.Type()
		}
		if ctxMeta := requestctx.FromContext(ctx); ctxMeta != nil {
			snapshot.mode = strings.TrimSpace(ctxMeta.Mode)
		}
		if runErr != nil {
			snapshot.errText = runErr.Error()
		}
		hookCh <- snapshot
		return nil
	}); err != nil {
		t.Fatalf("注册终态失败钩子失败: %v", err)
	}
	if err := manager.RegisterHandler("demo:retry-hook", asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		retryCalled.Add(1)
		return errors.New("retry later")
	})); err != nil {
		t.Fatalf("注册重试钩子测试处理器失败: %v", err)
	}
	if err := manager.RegisterHandler("demo:archive-hook", asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return errors.Wrap(asynq.SkipRetry, "archive now")
	})); err != nil {
		t.Fatalf("注册归档钩子测试处理器失败: %v", err)
	}
	if err := manager.StartWorker(); err != nil {
		t.Fatalf("启动 worker 失败: %v", err)
	}
	defer func() { _ = manager.Stop(context.Background()) }()

	retry := 1
	if _, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType: "demo:retry-hook",
		Retry:    &retry,
		Payload:  json.RawMessage(`{"demo":true}`),
	}); err != nil {
		t.Fatalf("投递重试钩子测试任务失败: %v", err)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		info, infoErr := manager.inspector.GetQueueInfo(manager.namespacedQueueName(QueueDefault))
		return retryCalled.Load() >= 1 && infoErr == nil && info.Retry >= 1
	})
	if got := hookCalls.Load(); got != 0 {
		t.Fatalf("重试中的失败不应触发终态失败钩子，实际触发 %d 次", got)
	}

	if _, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType: "demo:archive-hook",
		Payload: json.RawMessage(`{
			"workflowId":"wf-hook",
			"workflowName":"workflow.demo",
			"workflowNode":"root",
			"mode":"full",
			"shardIndex":1,
			"shardTotal":2
		}`),
	}); err != nil {
		t.Fatalf("投递归档钩子测试任务失败: %v", err)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		return hookCalls.Load() == 1
	})

	got := <-hookCh
	if got.taskType != "demo:archive-hook" || got.workflowID != "wf-hook" || got.mode != "full" || !strings.Contains(got.errText, asynq.SkipRetry.Error()) {
		t.Fatalf("终态失败钩子收到的任务上下文不符合预期: %+v", got)
	}
}

// TestWorkflowMarkSuccessFailureDoesNotSetTaskHashTTL 验证工作流成功回写失败时不会设置项目 task hash TTL。
func TestWorkflowMarkSuccessFailureDoesNotSetTaskHashTTL(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterHandler("demo:workflow-mark-success-fail", asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return nil
	})); err != nil {
		t.Fatalf("注册工作流成功回写失败测试处理器失败: %v", err)
	}
	if err := manager.StartWorker(); err != nil {
		t.Fatalf("启动 worker 失败: %v", err)
	}
	defer func() { _ = manager.Stop(context.Background()) }()

	resp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType: "demo:workflow-mark-success-fail",
		Payload: json.RawMessage(`{
			"workflowId":"wf-mark-success-fail",
			"workflowName":"missing.workflow",
			"workflowNode":"root",
			"shardTotal":1
		}`),
	})
	if err != nil {
		t.Fatalf("投递工作流成功回写失败测试任务失败: %v", err)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		info, infoErr := manager.inspector.GetTaskInfo(manager.namespacedQueueName(QueueDefault), resp.TaskID)
		return infoErr == nil && info.State == asynq.TaskStateRetry
	})

	ttl := manager.redis.TTL(context.Background(), keys.TaskAsynqTaskHashKey(manager.namespacedQueueName(QueueDefault), resp.TaskID)).Val()
	if ttl != -1 {
		t.Fatalf("成功回写失败的重试任务不应设置项目 task hash TTL，实际 TTL=%s", ttl)
	}
}

// TestArchivedTaskKeepsRuntimeSnapshotTTL 验证 archived 任务只保留业务运行快照 TTL，不干预 Asynq task hash。
func TestArchivedTaskKeepsRuntimeSnapshotTTL(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	cfg := manager.CurrentConfig()
	cfg.ArchivedRetentionSeconds = 120
	manager.UpdateConfig(cfg)

	if err := manager.RegisterHandler("demo:archive-ttl", asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return asynq.SkipRetry
	})); err != nil {
		t.Fatalf("注册归档运行快照测试处理器失败: %v", err)
	}
	if err := manager.StartWorker(); err != nil {
		t.Fatalf("启动 worker 失败: %v", err)
	}
	defer func() { _ = manager.Stop(context.Background()) }()

	resp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType: "demo:archive-ttl",
		Payload:  json.RawMessage(`{"demo":true}`),
	})
	if err != nil {
		t.Fatalf("投递归档运行快照测试任务失败: %v", err)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		info, infoErr := manager.inspector.GetTaskInfo(manager.namespacedQueueName(QueueDefault), resp.TaskID)
		return infoErr == nil && info.State == asynq.TaskStateArchived
	})
	item, err := manager.GetTaskInfo(context.Background(), &types.GetTaskInfoReq{
		Queue:  QueueDefault,
		TaskID: resp.TaskID,
	})
	if err != nil {
		t.Fatalf("查询归档任务详情失败: %v", err)
	}
	if item.StartedAt == "" || item.DurationMS <= 0 {
		t.Fatalf("归档任务应保留开始时间和耗时，实际 item=%+v", item)
	}

	ttl := manager.redis.TTL(context.Background(), keys.TaskAsynqTaskHashKey(manager.namespacedQueueName(QueueDefault), resp.TaskID)).Val()
	if ttl != -1 {
		t.Fatalf("归档终态任务不应设置项目 task hash TTL，实际 TTL=%s", ttl)
	}
	runtimeTTL := manager.redis.TTL(context.Background(), manager.taskRuntimeKey(QueueDefault, resp.TaskID)).Val()
	expectedRuntimeTTL := manager.archivedRetention() + time.Hour
	if runtimeTTL <= manager.archivedRetention() || runtimeTTL > expectedRuntimeTTL+time.Second {
		t.Fatalf("归档终态任务 runtime TTL = %s，期望约为 %s", runtimeTTL, expectedRuntimeTTL)
	}
	if err = manager.Stop(context.Background()); err != nil {
		t.Fatalf("停止 worker 失败: %v", err)
	}
	if err = manager.RunTask(context.Background(), &types.OperateTaskReq{Queue: QueueDefault, TaskID: resp.TaskID}); err != nil {
		t.Fatalf("立即执行归档任务失败: %v", err)
	}
	ttl = manager.redis.TTL(context.Background(), keys.TaskAsynqTaskHashKey(manager.namespacedQueueName(QueueDefault), resp.TaskID)).Val()
	if ttl != -1 {
		t.Fatalf("归档任务重跑后仍不应设置项目 task hash TTL，实际 TTL=%s", ttl)
	}
}

// TestRunTaskRepairsArchivedWorkflowFailureState 验证归档失败节点重跑后 DAG 可继续。
func TestRunTaskRepairsArchivedWorkflowFailureState(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	var rootCalls atomic.Int32
	var childCalls atomic.Int32
	if err := manager.RegisterHandler("demo:rerun-root", asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		if rootCalls.Add(1) == 1 {
			return errors.New("root boom")
		}
		return nil
	})); err != nil {
		t.Fatalf("注册根节点处理器失败: %v", err)
	}
	if err := manager.RegisterHandler("demo:rerun-child", asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		childCalls.Add(1)
		return nil
	})); err != nil {
		t.Fatalf("注册下游节点处理器失败: %v", err)
	}
	def := &WorkflowDefinition{
		Name: "demo.archived.rerun",
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				Name:         "root",
				TaskType:     "demo:rerun-root",
				BuildPayload: testNoopPayload,
			},
			"child": {
				Name:         "child",
				TaskType:     "demo:rerun-child",
				DependsOn:    []string{"root"},
				BuildPayload: testNoopPayload,
			},
		},
	}
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	if err := manager.StartWorker(); err != nil {
		t.Fatalf("启动 worker 失败: %v", err)
	}
	defer func() { _ = manager.Stop(context.Background()) }()

	workflowID, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{Name: def.Name})
	if err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	var archivedTaskID string
	waitForCondition(t, 5*time.Second, func() bool {
		resp, listErr := manager.ListTasks(context.Background(), &types.ListTaskItemsReq{
			Queue:    QueueDefault,
			State:    "archived",
			Page:     1,
			PageSize: 10,
		})
		if listErr != nil || resp == nil || len(resp.Tasks) == 0 {
			return false
		}
		for _, item := range resp.Tasks {
			if item.WorkflowID == workflowID && item.TaskType == "demo:rerun-root" {
				archivedTaskID = item.ID
				return true
			}
		}
		return false
	})
	realRunTask := manager.runInspectorTask
	runTaskCalls := 0
	manager.runInspectorTask = func(queue, taskID string) error {
		runTaskCalls++
		if runTaskCalls == 1 {
			return errors.New("injected run task failure")
		}
		return realRunTask(queue, taskID)
	}
	if err = manager.RunTask(context.Background(), &types.OperateTaskReq{Queue: QueueDefault, TaskID: archivedTaskID}); err == nil {
		t.Fatal("首次立即执行应返回注入故障")
	}
	metaAfterFailure, _, readErr := manager.readWorkflowStatus(context.Background(), workflowID)
	if readErr != nil {
		t.Fatalf("读取首次立即执行失败后的工作流状态失败: %v", readErr)
	}
	marker := manager.redis.HGet(context.Background(), manager.workflowNodeKey(workflowID, "root"), workflowNodeInstanceField(0)).Val()
	if metaAfterFailure.Status != WorkflowStatusRunning || marker != workflowNodeOutcomeRerunPrepared {
		t.Fatalf("首次立即执行失败后应保留可重入准备态 meta=%+v marker=%q", metaAfterFailure, marker)
	}
	infoAfterFailure, infoErr := manager.inspector.GetTaskInfo(manager.namespacedQueueName(QueueDefault), archivedTaskID)
	if infoErr != nil || infoAfterFailure.State != asynq.TaskStateArchived {
		t.Fatalf("首次立即执行失败后任务应保持 archived info=%+v err=%v", infoAfterFailure, infoErr)
	}
	if err = manager.RunTask(context.Background(), &types.OperateTaskReq{Queue: QueueDefault, TaskID: archivedTaskID}); err != nil {
		t.Fatalf("立即执行归档任务失败: %v", err)
	}
	waitForCondition(t, 5*time.Second, func() bool {
		return rootCalls.Load() >= 2 && childCalls.Load() >= 1
	})
	waitForCondition(t, 5*time.Second, func() bool {
		meta, _, readErr := manager.readWorkflowStatus(context.Background(), workflowID)
		return readErr == nil && meta.Status == WorkflowStatusSuccess
	})
	meta, nodes, err := manager.readWorkflowStatus(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("读取工作流状态失败: %v", err)
	}
	if meta.Status != WorkflowStatusSuccess {
		t.Fatalf("期望工作流重跑后成功，实际 meta=%+v nodes=%+v", meta, nodes)
	}
}

// TestCompletedTaskWritesResult 验证成功任务会把统一结果摘要写回 completed 记录。
func TestCompletedTaskWritesResult(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterHandler("demo:result", asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return nil
	})); err != nil {
		t.Fatalf("注册结果测试处理器失败: %v", err)
	}
	if err := manager.StartWorker(); err != nil {
		t.Fatalf("启动 worker 失败: %v", err)
	}
	defer func() { _ = manager.Stop(context.Background()) }()

	resp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType: "demo:result",
		Payload:  json.RawMessage(`{"demo":true}`),
	})
	if err != nil {
		t.Fatalf("投递结果测试任务失败: %v", err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		info, infoErr := manager.inspector.GetQueueInfo(manager.namespacedQueueName(QueueDefault))
		return infoErr == nil && info.Completed >= 1
	})

	item, err := manager.GetTaskInfo(context.Background(), &types.GetTaskInfoReq{
		Queue:  QueueDefault,
		TaskID: resp.TaskID,
	})
	if err != nil {
		t.Fatalf("查询任务详情失败: %v", err)
	}
	if item.State != "completed" {
		t.Fatalf("期望任务已完成，实际状态为 %s", item.State)
	}
	info, err := manager.inspector.GetTaskInfo(manager.namespacedQueueName(QueueDefault), resp.TaskID)
	if err != nil {
		t.Fatalf("读取已完成任务失败: %v", err)
	}
	if info.Retention != taskCompletedRetention {
		t.Fatalf("普通任务 retention=%s，期望=%s", info.Retention, taskCompletedRetention)
	}
	if len(item.Result) == 0 {
		t.Fatal("期望已完成任务写回结果摘要，实际为空")
	}

	var result TaskExecutionResult
	if err = json.Unmarshal(item.Result, &result); err != nil {
		t.Fatalf("解析任务结果摘要失败: %v", err)
	}
	if result.Status != "success" {
		t.Fatalf("期望结果状态为 success，实际为 %+v", result)
	}
	if result.TaskID != resp.TaskID {
		t.Fatalf("期望结果中的 task_id 与回执一致，期望=%s 实际=%s", resp.TaskID, result.TaskID)
	}
	if result.TaskName == "" {
		t.Fatalf("期望结果中包含任务名称，实际为 %+v", result)
	}
	if result.StartedAt == "" {
		t.Fatalf("期望结果中包含开始时间，实际为 %+v", result)
	}
	if _, parseErr := time.Parse(time.RFC3339, result.StartedAt); parseErr != nil {
		t.Fatalf("结果开始时间格式非法: %v", parseErr)
	}
	if item.StartedAt == "" || item.DurationMS <= 0 {
		t.Fatalf("成功任务应保留开始时间和耗时，实际 item=%+v", item)
	}
	taskTTL := manager.redis.TTL(context.Background(), keys.TaskAsynqTaskHashKey(manager.namespacedQueueName(QueueDefault), resp.TaskID)).Val()
	if taskTTL != -1 {
		t.Fatalf("成功终态任务不应设置项目 task hash TTL，实际 TTL=%s", taskTTL)
	}
	runtimeTTL := manager.redis.TTL(context.Background(), manager.taskRuntimeKey(QueueDefault, resp.TaskID)).Val()
	if runtimeTTL != -2 {
		t.Fatalf("成功终态任务应删除重复 runtime hash，实际 TTL=%s", runtimeTTL)
	}
	fallbackItem, err := manager.GetTaskInfo(context.Background(), &types.GetTaskInfoReq{
		Queue:  QueueDefault,
		TaskID: resp.TaskID,
	})
	if err != nil {
		t.Fatalf("查询 runtime 缺失后的任务详情失败: %v", err)
	}
	if fallbackItem.StartedAt != result.StartedAt {
		t.Fatalf("runtime 缺失时应从结果兜底开始时间，got=%q want=%q", fallbackItem.StartedAt, result.StartedAt)
	}
}

// TestCompletedTaskHashUsesNamespacedQueue 验证多站点命名空间下只检查真实 Asynq 队列 task hash。
func TestCompletedTaskHashUsesNamespacedQueue(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	cfg := manager.CurrentConfig()
	cfg.AppID = "17"
	useRuntimeAppID(t, cfg.AppID)
	manager.UpdateConfig(cfg)

	if err := manager.RegisterHandler("demo:namespaced-complete-ttl", asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return nil
	})); err != nil {
		t.Fatalf("注册命名空间 task hash 测试处理器失败: %v", err)
	}
	if err := manager.StartWorker(); err != nil {
		t.Fatalf("启动 worker 失败: %v", err)
	}
	defer func() { _ = manager.Stop(context.Background()) }()

	resp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType: "demo:namespaced-complete-ttl",
		Payload:  json.RawMessage(`{"demo":true}`),
	})
	if err != nil {
		t.Fatalf("投递命名空间 task hash 测试任务失败: %v", err)
	}
	internalQueue := manager.namespacedQueueName(QueueDefault)
	waitForCondition(t, 5*time.Second, func() bool {
		info, infoErr := manager.inspector.GetTaskInfo(internalQueue, resp.TaskID)
		return infoErr == nil && info.State == asynq.TaskStateCompleted
	})

	internalKey := keys.TaskAsynqTaskHashKey(internalQueue, resp.TaskID)
	if exists := manager.redis.Exists(context.Background(), internalKey).Val(); exists != 1 {
		t.Fatalf("期望真实队列任务 hash 存在，exists=%d", exists)
	}
	ttl := manager.redis.TTL(context.Background(), internalKey).Val()
	if ttl != -1 {
		t.Fatalf("命名空间成功任务不应设置项目 task hash TTL，实际 TTL=%s", ttl)
	}
	if exists := manager.redis.Exists(context.Background(), keys.TaskAsynqTaskHashKey(QueueDefault, resp.TaskID)).Val(); exists != 0 {
		t.Fatalf("不应写入逻辑队列任务 hash，exists=%d", exists)
	}
}

// TestListCompletedTasksReturnsErrorForOrphanedZSetMember 验证历史孤儿 completed 成员会显式暴露。
func TestListCompletedTasksReturnsErrorForOrphanedZSetMember(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	ctx := context.Background()
	internalQueue := manager.namespacedQueueName(QueueDefault)
	if err := manager.redis.SAdd(ctx, "asynq:queues", internalQueue).Err(); err != nil {
		t.Fatalf("写入 Asynq 队列索引失败: %v", err)
	}
	completedKey, err := keys.TaskAsynqStateZSetKey(internalQueue, "completed")
	if err != nil {
		t.Fatalf("生成 completed key 失败: %v", err)
	}
	if err = manager.redis.ZAdd(ctx, completedKey, redis.Z{
		Score:  float64(time.Now().Add(time.Hour).Unix()),
		Member: "orphan-completed-task",
	}).Err(); err != nil {
		t.Fatalf("写入孤儿 completed 成员失败: %v", err)
	}

	resp, err := manager.ListTasks(ctx, &types.ListTaskItemsReq{
		Queue:    QueueDefault,
		State:    "completed",
		Page:     1,
		PageSize: 20,
	})
	if err == nil {
		t.Fatalf("查询包含孤儿成员的 completed 列表应失败，实际 resp=%+v", resp)
	}
	if !errors.Is(err, asynq.ErrTaskNotFound) {
		t.Fatalf("期望暴露任务详情缺失错误，实际: %v", err)
	}
}

// TestListReportQueuesUsesFixedMetadataLimit 验证日报队列入口只读取固定数量的队列元数据。
func TestListReportQueuesUsesFixedMetadataLimit(t *testing.T) {
	manager, server, cleanup := newTestManagerWithServer(t)
	defer cleanup()
	ctx := context.Background()
	queueNames := []string{QueueCritical, QueueDefault, QueueMaintenance}
	for index := 0; index < 35; index++ {
		queueNames = append(queueNames, fmt.Sprintf("report-extra-%02d", index))
	}
	cfg := manager.CurrentConfig()
	cfg.Queues = make(map[string]int, len(queueNames))
	for _, queueName := range queueNames {
		cfg.Queues[queueName] = 1
	}
	manager.UpdateConfig(cfg)
	if err := manager.redis.LPush(ctx, keys.TaskAsynqPendingKey(manager.namespacedQueueName(QueueCritical)), "critical-task").Err(); err != nil {
		t.Fatalf("写入日报 pending 计数失败: %v", err)
	}
	if err := manager.redis.LPush(ctx, keys.TaskAsynqActiveKey(manager.namespacedQueueName(QueueDefault)), "default-task").Err(); err != nil {
		t.Fatalf("写入日报 active 计数失败: %v", err)
	}
	commandsBefore := server.CommandCount()
	resp, limited, err := manager.ListReportQueues(ctx, 2)
	if err != nil {
		t.Fatalf("读取有界日报队列概览失败: %v", err)
	}
	if !limited || resp == nil || len(resp.Queues) != 2 {
		t.Fatalf("日报队列概览未按固定上限截断: limited=%t resp=%+v", limited, resp)
	}
	if resp.Queues[0].Name != QueueCritical || resp.Queues[1].Name != QueueDefault {
		t.Fatalf("日报队列优先顺序异常: %+v", resp.Queues)
	}
	if resp.Queues[0].Pending != 1 || resp.Queues[1].Active != 1 {
		t.Fatalf("日报队列基础计数异常: %+v", resp.Queues)
	}
	if commands := server.CommandCount() - commandsBefore; commands != 25 {
		t.Fatalf("日报两个队列应执行 20 条状态计数、4 条有界聚合组命令和 1 条延迟读取，实际=%d", commands)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err = manager.ListReportQueues(canceledCtx, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("日报队列读取应响应 context 取消，实际 error=%v", err)
	}
}

// TestListReportQueuesReadsDefaultQueues 验证未配置队列权重时日报仍读取 Worker 默认队列的真实计数。
func TestListReportQueuesReadsDefaultQueues(t *testing.T) {
	manager, _, cleanup := newTestManagerWithServer(t)
	defer cleanup()
	ctx := context.Background()
	cfg := manager.CurrentConfig()
	cfg.Queues = nil
	manager.UpdateConfig(cfg)

	if err := manager.redis.LPush(ctx, keys.TaskAsynqPendingKey(manager.namespacedQueueName(QueueCritical)), "critical-task").Err(); err != nil {
		t.Fatalf("写入默认 critical 队列 pending 计数失败: %v", err)
	}
	if err := manager.redis.ZAdd(ctx, keys.TaskAsynqScheduledKey(manager.namespacedQueueName(QueueMaintenance)), redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: "maintenance-task",
	}).Err(); err != nil {
		t.Fatalf("写入默认 maintenance 队列 scheduled 计数失败: %v", err)
	}
	internalDefault := manager.namespacedQueueName(QueueDefault)
	if err := manager.redis.SAdd(ctx, keys.TaskAsynqGroupsKey(internalDefault), "refresh").Err(); err != nil {
		t.Fatalf("写入默认 default 队列聚合组失败: %v", err)
	}
	if err := manager.redis.ZAdd(ctx, keys.TaskAsynqGroupKey(internalDefault, "refresh"),
		redis.Z{Score: 1, Member: "aggregate-task-1"},
		redis.Z{Score: 2, Member: "aggregate-task-2"},
	).Err(); err != nil {
		t.Fatalf("写入默认 default 队列聚合任务失败: %v", err)
	}

	resp, limited, err := manager.ListReportQueues(ctx, 3)
	if err != nil {
		t.Fatalf("读取默认日报队列概览失败: %v", err)
	}
	if limited || resp == nil || len(resp.Queues) != 3 {
		t.Fatalf("默认日报队列概览异常: limited=%t resp=%+v", limited, resp)
	}
	if resp.Queues[0].Name != QueueCritical || resp.Queues[0].Pending != 1 {
		t.Fatalf("默认 critical 队列计数异常: %+v", resp.Queues[0])
	}
	if resp.Queues[1].Name != QueueDefault || resp.Queues[1].Aggregating != 2 {
		t.Fatalf("默认 default 队列顺序异常: %+v", resp.Queues)
	}
	if resp.Queues[2].Name != QueueMaintenance || resp.Queues[2].Scheduled != 1 {
		t.Fatalf("默认 maintenance 队列计数异常: %+v", resp.Queues[2])
	}
}

// TestRegisterGroupAggregatorDispatchesByGroup 验证分组聚合器会按 group 分发任务集合。
func TestRegisterGroupAggregatorDispatchesByGroup(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	expected := asynq.NewTask("demo:batch", []byte(`{"ok":true}`))
	if err := manager.RegisterGroupAggregator("demo.group", func(tasks []*asynq.Task) *asynq.Task {
		if len(tasks) != 1 || tasks[0].Type() != "demo:item" {
			t.Fatalf("分组任务集合不符合预期: %+v", tasks)
		}
		return expected
	}); err != nil {
		t.Fatalf("注册分组聚合器失败: %v", err)
	}
	got := manager.aggregateTasks(manager.namespacedGroup("demo.group"), []*asynq.Task{
		asynq.NewTask("demo:item", []byte(`{}`)),
	})
	if got == nil || got.Type() != expected.Type() {
		t.Fatalf("期望聚合后任务类型为 %s，实际为 %#v", expected.Type(), got)
	}
}

// TestRegisterPeriodicTaskRejectsConfigDuplicate 验证对应场景。
func TestRegisterPeriodicTaskRejectsConfigDuplicate(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	cfg := enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
		Name:     "dup-periodic",
		Cron:     "*/5 * * * *",
		Workflow: WorkflowNameCacheRefresh,
		Queue:    QueueMaintenance,
	})
	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled:  true,
		AppID:    "1",
		Periodic: []config.TaskPeriodicConfig{cfg},
	})

	if err := manager.RegisterPeriodicTask(cfg); err == nil {
		t.Fatal("期望返回重复周期任务错误，实际为 nil")
	}
}

// TestAllPeriodicTasksDedupesConfigAndPluginTasks 验证对应场景。
func TestAllPeriodicTasksDedupesConfigAndPluginTasks(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	cfg := enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
		Name:     "same-periodic",
		Cron:     "*/5 * * * *",
		Workflow: WorkflowNameCacheRefresh,
		Queue:    QueueMaintenance,
	})
	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled:  true,
		AppID:    "1",
		Periodic: []config.TaskPeriodicConfig{cfg},
	})
	manager.periodic = append(manager.periodic, cfg)

	items := manager.allPeriodicTasks()
	if len(items) != 1 {
		t.Fatalf("期望去重后仅保留 1 个周期任务，实际为 %d", len(items))
	}
	if got := manager.periodicTaskCount(); got != 1 {
		t.Fatalf("期望周期任务计数去重后为 1，实际为 %d", got)
	}
}

// TestAllPeriodicTasksDefaultsMissingEnabledToEnabled 验证周期任务缺省 enabled 时默认参与调度。
func TestAllPeriodicTasksDefaultsMissingEnabledToEnabled(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	disabled := false
	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled: true,
		AppID:   "1",
		Periodic: []config.TaskPeriodicConfig{
			{
				Enabled:  &disabled,
				Name:     "disabled-periodic",
				Cron:     "*/5 * * * *",
				Workflow: WorkflowNameCacheRefresh,
				Queue:    QueueMaintenance,
			},
			{
				Name:     "missing-enabled-periodic",
				Cron:     "*/10 * * * *",
				Workflow: WorkflowNameCacheRefresh,
				Queue:    QueueMaintenance,
			},
			enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
				Name:     "enabled-periodic",
				Cron:     "*/15 * * * *",
				Workflow: WorkflowNameCacheRefresh,
				Queue:    QueueMaintenance,
			}),
		},
	})

	items := manager.allPeriodicTasks()
	if len(items) != 2 {
		t.Fatalf("期望保留缺省启用和显式启用的周期任务，实际为 %d", len(items))
	}
	if items[0].Name != "missing-enabled-periodic" || items[1].Name != "enabled-periodic" {
		t.Fatalf("保留的周期任务 = %+v", items)
	}
}

// TestPeriodicConfigsSkipsInvalidAndKeepsValid 验证周期任务配置异常只跳过当前任务，不影响其它合法任务。
func TestPeriodicConfigsSkipsInvalidAndKeepsValid(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterWorkflow(testWorkflowDefinition("valid.workflow")); err != nil {
		t.Fatalf("注册合法工作流失败: %v", err)
	}
	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled: true,
		AppID:   "1",
		Periodic: []config.TaskPeriodicConfig{
			enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
				Name:     "missing-workflow",
				Cron:     "*/5 * * * *",
				Workflow: "missing.workflow",
			}),
			enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
				Name:     "invalid-cron",
				Cron:     "bad cron",
				Workflow: "valid.workflow",
			}),
			enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
				Name:             "valid-periodic",
				Cron:             "*/5 * * * *",
				Workflow:         "valid.workflow",
				Queue:            QueueMaintenance,
				UniqueKey:        "valid-periodic",
				UniqueTTLSeconds: 60,
			}),
		},
	})

	configs, err := manager.periodicConfigs()
	if err != nil {
		t.Fatalf("生成周期任务配置失败: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("期望只保留 1 个合法周期任务，实际为 %d", len(configs))
	}
	if got := configs[0].Cronspec; got != "*/5 * * * *" {
		t.Fatalf("期望保留合法 cron，实际为 %s", got)
	}
	var payload WorkflowTriggerPayload
	if err = json.Unmarshal(configs[0].Task.Payload(), &payload); err != nil {
		t.Fatalf("解析周期工作流入口 payload 失败: %v", err)
	}
	if payload.PeriodicName != "valid-periodic" {
		t.Fatalf("周期任务原始名称写入 payload 异常: %q", payload.PeriodicName)
	}
	headers := configs[0].Task.Headers()
	if headers[HeaderPeriodicName] != "valid-periodic" {
		t.Fatalf("周期任务原始名称写入 header 异常: %q", headers[HeaderPeriodicName])
	}
	if headers[headerTaskName] != "周期任务触发:valid-periodic" {
		t.Fatalf("周期入口展示名称异常: %q", headers[headerTaskName])
	}
}

// TestPeriodicNextRunsKeepsOriginalName 验证下一次执行时间使用周期任务原始名称关联历史任务。
func TestPeriodicNextRunsKeepsOriginalName(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterWorkflow(testWorkflowDefinition("periodic.next.run")); err != nil {
		t.Fatalf("注册周期工作流失败: %v", err)
	}
	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled: true,
		AppID:   "1",
		Periodic: []config.TaskPeriodicConfig{enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
			Name:     "periodic-next-run",
			Cron:     "*/5 * * * *",
			Workflow: "periodic.next.run",
		})},
	})

	runs := manager.periodicNextRuns(time.Date(2026, 8, 6, 10, 0, 0, 0, time.UTC))
	if len(runs) != 1 {
		t.Fatalf("期望生成 1 条下一次执行时间，实际=%d", len(runs))
	}
	if runs[0].PeriodicName != "periodic-next-run" {
		t.Fatalf("下一次执行时间的周期任务名称异常: %q", runs[0].PeriodicName)
	}
}

// TestValidatePeriodicTaskConfigsRejectsWorkflowShardLimit 验证发布预检只接受当前运行时可执行的启用周期任务。
func TestValidatePeriodicTaskConfigsRejectsWorkflowShardLimit(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	definition := testWorkflowDefinition("single-shard.periodic.workflow")
	definition.MaxShardTotal = 1
	if err := manager.RegisterWorkflow(definition); err != nil {
		t.Fatalf("注册单分片周期工作流失败: %v", err)
	}
	item := config.TaskPeriodicConfig{
		Name:       "single-shard-periodic",
		Cron:       "*/5 * * * *",
		Workflow:   definition.Name,
		Queue:      QueueMaintenance,
		ShardTotal: 2,
	}
	if err := manager.ValidatePeriodicTaskConfigs([]config.TaskPeriodicConfig{item}); err == nil || !strings.Contains(err.Error(), "max=1") {
		t.Fatalf("ValidatePeriodicTaskConfigs() error = %v, want workflow shard limit", err)
	}

	disabled := false
	item.Enabled = &disabled
	if err := manager.ValidatePeriodicTaskConfigs([]config.TaskPeriodicConfig{item}); err != nil {
		t.Fatalf("停用的预置周期任务不应参与运行时校验: %v", err)
	}

	item.Enabled = nil
	item.ShardTotal = 1
	if err := manager.ValidatePeriodicTaskConfigs([]config.TaskPeriodicConfig{item}); err != nil {
		t.Fatalf("合法单分片周期任务校验失败: %v", err)
	}
}

// TestValidatePeriodicTaskConfigRequiresScheduledUnique 验证按计划时刻防重的工作流必须显式配置完整唯一键。
func TestValidatePeriodicTaskConfigRequiresScheduledUnique(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	definition := testWorkflowDefinition("scheduled-unique-config")
	definition.PeriodicUniqueBySchedule = true
	if err := manager.RegisterWorkflow(definition); err != nil {
		t.Fatalf("注册按计划时刻防重工作流失败: %v", err)
	}
	base := config.TaskPeriodicConfig{
		Name:     "scheduled-unique-config",
		Cron:     "0 10 * * *",
		Workflow: definition.Name,
		Queue:    QueueMaintenance,
	}
	tests := []struct {
		name      string // 测试场景名称
		uniqueKey string // 周期任务唯一键
		uniqueTTL int    // 周期任务唯一键有效期秒数
		wantErr   string // 期望错误片段，为空表示校验成功
	}{
		{name: "missing_unique_key", uniqueTTL: 60, wantErr: "unique_key"},
		{name: "missing_unique_ttl", uniqueKey: "scheduled-unique-config", wantErr: "unique_ttl_seconds"},
		{name: "valid", uniqueKey: "scheduled-unique-config", uniqueTTL: 60},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := base
			item.UniqueKey = tt.uniqueKey
			item.UniqueTTLSeconds = tt.uniqueTTL
			normalized, err := manager.validatePeriodicTaskConfig(item)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("周期唯一配置校验 error=%v, want contains %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("合法周期唯一配置校验失败: %v", err)
			}
			if normalized.UniqueKey != tt.uniqueKey || normalized.UniqueTTLSeconds != tt.uniqueTTL {
				t.Fatalf("周期唯一配置归一化异常: %+v", normalized)
			}
		})
	}
}

// TestPeriodicConfigsSupportsSecondLevelSchedule 验证周期任务支持秒级 cron 和 every_seconds 固定间隔。
func TestPeriodicConfigsSupportsSecondLevelSchedule(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterWorkflow(testWorkflowDefinition("second.workflow")); err != nil {
		t.Fatalf("注册秒级工作流失败: %v", err)
	}
	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled: true,
		AppID:   "1",
		Periodic: []config.TaskPeriodicConfig{
			enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
				Name:     "six-field-cron",
				Cron:     "*/10 * * * * *",
				Workflow: "second.workflow",
			}),
			enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
				Name:         "every-seconds",
				EverySeconds: 5,
				Workflow:     "second.workflow",
			}),
		},
	})

	configs, err := manager.periodicConfigs()
	if err != nil {
		t.Fatalf("生成秒级周期任务配置失败: %v", err)
	}
	if len(configs) != 2 {
		t.Fatalf("期望生成 2 个秒级周期任务，实际为 %d", len(configs))
	}
	got := map[string]struct{}{}
	for _, cfg := range configs {
		got[cfg.Cronspec] = struct{}{}
	}
	if _, ok := got["*/10 * * * * *"]; !ok {
		t.Fatalf("期望支持 6 段秒级 cron，实际为 %+v", got)
	}
	if _, ok := got["@every 5s"]; !ok {
		t.Fatalf("期望支持 every_seconds 转换为 @every 5s，实际为 %+v", got)
	}
}

// TestPeriodicWorkflowTriggerRetryZeroKeepsDispatchBudget 验证周期业务 retry=0 时入口仍保留独立编排重试。
func TestPeriodicWorkflowTriggerRetryZeroKeepsDispatchBudget(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterWorkflow(testWorkflowDefinition("periodic.retry.zero")); err != nil {
		t.Fatalf("注册周期工作流失败: %v", err)
	}
	cfg := manager.CurrentConfig()
	cfg.Periodic = []config.TaskPeriodicConfig{enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
		Name: "periodic-retry-zero", Cron: "*/5 * * * *", Workflow: "periodic.retry.zero", Retry: 0,
	})}
	manager.UpdateConfig(cfg)
	configs, err := manager.periodicConfigs()
	if err != nil || len(configs) != 1 {
		t.Fatalf("生成周期任务配置失败 configs=%d err=%v", len(configs), err)
	}
	opts := append([]asynq.Option{}, configs[0].Opts...)
	opts = append(opts, asynq.ProcessAt(time.Now().Add(time.Minute)))
	info, err := manager.client.EnqueueContext(context.Background(), configs[0].Task, opts...)
	if err != nil {
		t.Fatalf("投递周期入口任务失败: %v", err)
	}
	if info.MaxRetry != workflowTaskRetryBudget(manager.defaultRetry(nil)) || info.MaxRetry <= manager.defaultRetry(nil) {
		t.Fatalf("周期入口未预留独立编排重试 actual=%d base=%d", info.MaxRetry, manager.defaultRetry(nil))
	}
	if info.Retention != taskCompletedRetention {
		t.Fatalf("周期入口任务 retention=%s，期望=%s", info.Retention, taskCompletedRetention)
	}
}

// TestPeriodicTaskCronspecRejectsTooSmallEverySeconds 验证过小固定间隔会被拒绝，避免误配置造成队列压力。
func TestPeriodicTaskCronspecRejectsTooSmallEverySeconds(t *testing.T) {
	if _, err := periodicTaskCronspec(config.TaskPeriodicConfig{EverySeconds: tasklimits.MinPeriodicEverySeconds - 1}); err == nil {
		t.Fatal("期望 every_seconds 小于最小值时返回错误")
	}
	got, err := periodicTaskCronspec(config.TaskPeriodicConfig{EverySeconds: tasklimits.MinPeriodicEverySeconds})
	if err != nil {
		t.Fatalf("期望最小 every_seconds 合法，实际错误: %v", err)
	}
	if got != "@every 5s" {
		t.Fatalf("Cronspec = %s, want @every 5s", got)
	}
}

// TestValidatePeriodicCronspecRejectsTooFrequentSecondField 验证六段 cron 不能绕过固定间隔的五秒下限。
func TestValidatePeriodicCronspecRejectsTooFrequentSecondField(t *testing.T) {
	for _, cronspec := range []string{"* * * * * *", "0,1 * * * * *", "1,58 0 * * * *"} {
		if err := validatePeriodicCronspec(cronspec); err == nil {
			t.Fatalf("秒级 cron=%q 存在五秒内触发可能时应被拒绝", cronspec)
		}
	}
	for _, cronspec := range []string{"*/5 * * * * *", "0 * * * * *", "* * * * *", "@every 5s"} {
		if err := validatePeriodicCronspec(cronspec); err != nil {
			t.Fatalf("cron=%q 应通过最小间隔校验: %v", cronspec, err)
		}
	}
}

// TestPeriodicCronParserNextTimes 验证 5 段分钟 cron、6 段秒级 cron 和 @every 的实际下一次触发时间。
func TestPeriodicCronParserNextTimes(t *testing.T) {
	base := time.Date(2026, 5, 9, 10, 0, 3, 0, time.Local)
	tests := []struct {
		name string    // name 表示测试场景名称。
		spec string    // spec 表示测试字段。
		want time.Time // want 表示期望结果。
	}{
		{
			name: "five-field-minute-cron",
			spec: "*/5 * * * *",
			want: time.Date(2026, 5, 9, 10, 5, 0, 0, time.Local),
		},
		{
			name: "six-field-second-cron",
			spec: "*/10 * * * * *",
			want: time.Date(2026, 5, 9, 10, 0, 10, 0, time.Local),
		},
		{
			name: "every-seconds",
			spec: "@every 5s",
			want: time.Date(2026, 5, 9, 10, 0, 8, 0, time.Local),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule, err := periodicCronParser.Parse(tt.spec)
			if err != nil {
				t.Fatalf("解析周期表达式失败: %v", err)
			}
			if got := schedule.Next(base); !got.Equal(tt.want) {
				t.Fatalf("下一次触发时间不符合预期: got=%s want=%s", got.Format(time.RFC3339), tt.want.Format(time.RFC3339))
			}
		})
	}
}

// TestSchedulerEnabledRespectsConfigSwitch 确保 scheduler.enabled=false 时不会因为存在周期任务配置而仍被判定为开启。
func TestSchedulerEnabledRespectsConfigSwitch(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled: true,
		AppID:   "1",
		Scheduler: config.TaskQueueSchedulerConfig{
			Enabled: false,
		},
		Periodic: []config.TaskPeriodicConfig{
			enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
				Name:     "disabled-scheduler-periodic",
				Cron:     "*/5 * * * *",
				Workflow: "first.workflow",
			}),
		},
	})

	if manager.schedulerEnabled() {
		t.Fatal("期望 scheduler.enabled=false 时不启动调度器，实际返回 true")
	}
	status := manager.schedulerStatusSnapshot(context.Background())
	if status == nil {
		t.Fatal("期望调度器状态快照存在，实际为 nil")
	}
	if status.Enabled {
		t.Fatal("期望调度器状态中的 enabled=false，实际为 true")
	}
	if status.PeriodicTaskCount != 1 {
		t.Fatalf("期望周期任务数量为 1，实际为 %d", status.PeriodicTaskCount)
	}
}

// TestSchedulerEnabledRequiresConfigSwitch 确保未配置 scheduler.enabled 时不会隐式启动调度器。
func TestSchedulerEnabledRequiresConfigSwitch(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled: true,
		AppID:   "1",
		Periodic: []config.TaskPeriodicConfig{
			enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
				Name:     "default-enabled-periodic",
				Cron:     "*/5 * * * *",
				Workflow: "first.workflow",
			}),
		},
	})

	if manager.schedulerEnabled() {
		t.Fatal("期望未配置 scheduler.enabled 时不启动调度器，实际返回 true")
	}
	status := manager.schedulerStatusSnapshot(context.Background())
	if status == nil {
		t.Fatal("期望调度器状态快照存在，实际为 nil")
	}
	if status.Enabled {
		t.Fatal("期望调度器状态中的 enabled=false，实际为 true")
	}
	if status.PeriodicTaskCount != 1 {
		t.Fatalf("期望周期任务数量为 1，实际为 %d", status.PeriodicTaskCount)
	}
}

// TestListQueuesReturnsSchedulerStatus 确保任务队列概览接口会附带调度器运行状态。
func TestListQueuesReturnsSchedulerStatus(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	manager.UpdateConfig(config.TaskQueueConfig{
		Enabled: true,
		AppID:   "1",
		Queues: map[string]int{
			QueueDefault: 1,
		},
		Scheduler: config.TaskQueueSchedulerConfig{
			Enabled:                  true,
			SyncIntervalSeconds:      30,
			HeartbeatIntervalSeconds: 12,
			MaxQueueBacklog:          500,
		},
	})

	resp, err := manager.ListQueues(context.Background())
	if err != nil {
		t.Fatalf("查询任务队列概览失败: %v", err)
	}
	if resp.Scheduler == nil {
		t.Fatal("期望返回调度器状态，实际为 nil")
	}
	if !resp.Scheduler.Enabled {
		t.Fatal("期望调度器状态 enabled=true，实际为 false")
	}
	if resp.Scheduler.SyncIntervalSeconds != 30 {
		t.Fatalf("期望同步间隔为 30 秒，实际为 %d", resp.Scheduler.SyncIntervalSeconds)
	}
	if resp.Scheduler.HeartbeatIntervalSeconds != 12 {
		t.Fatalf("期望心跳间隔为 12 秒，实际为 %d", resp.Scheduler.HeartbeatIntervalSeconds)
	}
	if resp.Scheduler.MaxQueueBacklog != 500 {
		t.Fatalf("期望队列积压上限为 500，实际为 %d", resp.Scheduler.MaxQueueBacklog)
	}
}

// TestListQueuesBoundsHighCardinalityGroups 验证聚合组异常增多时跳过 Asynq 无界巡检并明确降级。
func TestListQueuesBoundsHighCardinalityGroups(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	ctx := context.Background()
	internalQueue := manager.namespacedQueueName(QueueDefault)
	if err := manager.redis.SAdd(ctx, "asynq:queues", internalQueue).Err(); err != nil {
		t.Fatalf("写入 Asynq 队列索引失败: %v", err)
	}
	groups := make([]any, 0, taskQueueGroupReadLimit+1)
	for index := int64(0); index <= taskQueueGroupReadLimit; index++ {
		groups = append(groups, fmt.Sprintf("group-%03d", index))
	}
	if err := manager.redis.SAdd(ctx, keys.TaskAsynqGroupsKey(internalQueue), groups...).Err(); err != nil {
		t.Fatalf("写入高基数聚合组失败: %v", err)
	}
	if err := manager.redis.LPush(ctx, keys.TaskAsynqPendingKey(internalQueue), "pending-task").Err(); err != nil {
		t.Fatalf("写入待执行任务失败: %v", err)
	}

	resp, err := manager.ListQueues(ctx)
	if err != nil {
		t.Fatalf("高基数聚合组应安全降级: %v", err)
	}
	if !resp.MetricsLimited {
		t.Fatalf("高基数聚合组降级响应异常: %+v", resp)
	}
	var defaultQueue *types.TaskQueueItem
	for index := range resp.Queues {
		if resp.Queues[index].Name == QueueDefault {
			defaultQueue = &resp.Queues[index]
			break
		}
	}
	if defaultQueue == nil || defaultQueue.Pending != 1 || defaultQueue.Aggregating != 0 {
		t.Fatalf("高基数聚合组核心计数异常: %+v", resp.Queues)
	}
}

// TestListQueuesKeepsCoreCountsForOrphanedCompletedMember 验证任务详情缺失只降级内存估算，不拖垮队列概览。
func TestListQueuesKeepsCoreCountsForOrphanedCompletedMember(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	ctx := context.Background()
	internalQueue := manager.namespacedQueueName(QueueDefault)
	if err := manager.redis.SAdd(ctx, "asynq:queues", internalQueue).Err(); err != nil {
		t.Fatalf("写入 Asynq 队列索引失败: %v", err)
	}
	completedKey, err := keys.TaskAsynqStateZSetKey(internalQueue, "completed")
	if err != nil {
		t.Fatalf("生成 completed key 失败: %v", err)
	}
	if err = manager.redis.ZAdd(ctx, completedKey, redis.Z{
		Score:  float64(time.Now().Add(time.Hour).Unix()),
		Member: "orphan-completed-task",
	}).Err(); err != nil {
		t.Fatalf("写入孤儿 completed 成员失败: %v", err)
	}

	resp, err := manager.ListQueues(ctx)
	if err != nil {
		t.Fatalf("孤儿 completed 成员不应拖垮队列概览: %v", err)
	}
	if !resp.MetricsLimited {
		t.Fatalf("孤儿 completed 成员应标记内存估算降级: %+v", resp)
	}
	var completed int
	for _, queue := range resp.Queues {
		if queue.Name == QueueDefault {
			completed = queue.Completed
		}
	}
	if completed != 1 {
		t.Fatalf("孤儿 completed 核心计数应继续可见: %+v", resp.Queues)
	}
}

// TestEnqueueRegisteredTaskRejectsUnknownType 验证对应场景。
func TestEnqueueRegisteredTaskRejectsUnknownType(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	_, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType: "unknown:type",
		Payload:  json.RawMessage(`{"demo":true}`),
	})
	if !errors.Is(err, ErrTaskTypeNotFound) {
		t.Fatalf("期望返回 ErrTaskTypeNotFound，实际为 %v", err)
	}
}

// TestEnqueueRegisteredTaskSupportsScheduling 验证对应场景。
func TestEnqueueRegisteredTaskSupportsScheduling(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterHandler("demo:scheduled", asynq.HandlerFunc(func(context.Context, *asynq.Task) error { return nil })); err != nil {
		t.Fatalf("注册调度测试处理器失败: %v", err)
	}
	processAt := time.Now().Add(time.Minute).UTC().Format(time.RFC3339)
	resp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType:  "demo:scheduled",
		Payload:   json.RawMessage(`{"demo":true}`),
		ProcessAt: processAt,
	})
	if err != nil {
		t.Fatalf("投递已注册任务失败: %v", err)
	}
	if resp.TaskID == "" || resp.TaskType != "demo:scheduled" {
		t.Fatalf("入队回执不符合预期: %+v", resp)
	}
	if resp.ProcessAt == "" {
		t.Fatalf("期望回执中包含调度执行时间: %+v", resp)
	}
}

// TestEnqueueRegisteredTaskKeepsExplicitZeroRetry 校验人工投递可显式关闭重试而不是回落到 Asynq 默认值。
func TestEnqueueRegisteredTaskKeepsExplicitZeroRetry(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterHandler("demo:no-retry", asynq.HandlerFunc(func(context.Context, *asynq.Task) error { return nil })); err != nil {
		t.Fatalf("注册零重试测试处理器失败: %v", err)
	}
	retry := 0
	resp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType:  "demo:no-retry",
		Payload:   json.RawMessage(`{"demo":true}`),
		Retry:     &retry,
		ProcessAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("投递零重试任务失败: %v", err)
	}
	info, err := manager.inspector.GetTaskInfo(manager.namespacedQueueName(QueueDefault), resp.TaskID)
	if err != nil {
		t.Fatalf("读取零重试任务失败: %v", err)
	}
	if info.MaxRetry != 0 {
		t.Fatalf("零重试任务 max_retry=%d，期望 0", info.MaxRetry)
	}
}

// TestListTasksAndGetTaskInfo 验证对应场景。
func TestListTasksAndGetTaskInfo(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterHandler("demo:list", asynq.HandlerFunc(func(context.Context, *asynq.Task) error { return nil })); err != nil {
		t.Fatalf("注册列表测试处理器失败: %v", err)
	}
	resp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType:  "demo:list",
		Payload:   json.RawMessage(`{"demo":true}`),
		ProcessAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("投递任务失败: %v", err)
	}

	listResp, err := manager.ListTasks(context.Background(), &types.ListTaskItemsReq{
		Queue:    QueueDefault,
		State:    "scheduled",
		Page:     1,
		PageSize: 10,
	})
	if err != nil {
		t.Fatalf("查询任务列表失败: %v", err)
	}
	if len(listResp.Tasks) == 0 {
		t.Fatal("期望任务列表中存在已调度任务")
	}

	item, err := manager.GetTaskInfo(context.Background(), &types.GetTaskInfoReq{
		Queue:  QueueDefault,
		TaskID: resp.TaskID,
	})
	if err != nil {
		t.Fatalf("获取任务详情失败: %v", err)
	}
	if item.ID != resp.TaskID || item.TaskType != "demo:list" || item.State == "" {
		t.Fatalf("任务详情不符合预期: %+v", item)
	}
}

// TestListTasksScheduledTimeDescAndRange 验证任务列表按计划执行时间倒序展示，并支持时间段筛选定时任务。
func TestListTasksScheduledTimeDescAndRange(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterHandler("demo:scheduled-sort", asynq.HandlerFunc(func(context.Context, *asynq.Task) error { return nil })); err != nil {
		t.Fatalf("注册定时排序测试处理器失败: %v", err)
	}
	now := time.Now().UTC()
	earlyAt := now.Add(10 * time.Minute).Truncate(time.Second)
	middleAt := now.Add(20 * time.Minute).Truncate(time.Second)
	lateAt := now.Add(30 * time.Minute).Truncate(time.Second)
	earlyResp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType:  "demo:scheduled-sort",
		Payload:   json.RawMessage(`{"name":"early"}`),
		ProcessAt: earlyAt.Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("投递较早定时任务失败: %v", err)
	}
	middleResp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType:  "demo:scheduled-sort",
		Payload:   json.RawMessage(`{"name":"middle"}`),
		ProcessAt: middleAt.Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("投递中间定时任务失败: %v", err)
	}
	lateResp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType:  "demo:scheduled-sort",
		Payload:   json.RawMessage(`{"name":"late"}`),
		ProcessAt: lateAt.Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("投递较晚定时任务失败: %v", err)
	}

	listResp, err := manager.ListTasks(context.Background(), &types.ListTaskItemsReq{
		Queue:    QueueDefault,
		State:    "scheduled",
		Page:     1,
		PageSize: 10,
	})
	if err != nil {
		t.Fatalf("查询定时任务列表失败: %v", err)
	}
	gotIDs := []string{listResp.Tasks[0].ID, listResp.Tasks[1].ID, listResp.Tasks[2].ID}
	wantIDs := []string{lateResp.TaskID, middleResp.TaskID, earlyResp.TaskID}
	if !slices.Equal(gotIDs, wantIDs) {
		t.Fatalf("定时任务排序不符合预期: got=%v want=%v", gotIDs, wantIDs)
	}

	rangeResp, err := manager.ListTasks(context.Background(), &types.ListTaskItemsReq{
		Queue:     QueueDefault,
		State:     "scheduled",
		StartTime: middleAt.Add(-time.Second).Format(time.RFC3339),
		EndTime:   middleAt.Add(time.Second).Format(time.RFC3339),
		Page:      1,
		PageSize:  10,
	})
	if err != nil {
		t.Fatalf("按时间段查询定时任务失败: %v", err)
	}
	if rangeResp.Total != 1 || len(rangeResp.Tasks) != 1 || rangeResp.Tasks[0].ID != middleResp.TaskID {
		t.Fatalf("时间段过滤不符合预期: total=%d tasks=%+v", rangeResp.Total, rangeResp.Tasks)
	}
}

// TestListTasksFiltersByTaskName 验证任务列表支持按任务名称关键字筛选周期/工作流任务。
func TestListTasksFiltersByTaskName(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterWorkflow(testWorkflowDefinition("demo.periodic.workflow")); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	resp, err := manager.EnqueueWorkflowTrigger(context.Background(), &types.TriggerTaskWorkflowReq{
		Name:      "demo.periodic.workflow",
		Queue:     QueueMaintenance,
		ProcessAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("投递工作流触发任务失败: %v", err)
	}

	listResp, err := manager.ListTasks(context.Background(), &types.ListTaskItemsReq{
		Queue:    QueueMaintenance,
		State:    "scheduled",
		TaskName: "periodic.workflow",
		Page:     1,
		PageSize: 10,
	})
	if err != nil {
		t.Fatalf("按任务名称查询任务列表失败: %v", err)
	}
	if listResp.Total != 1 || len(listResp.Tasks) != 1 {
		t.Fatalf("期望按任务名称命中 1 条任务，实际 total=%d tasks=%d", listResp.Total, len(listResp.Tasks))
	}
	if listResp.Tasks[0].ID != resp.TaskID {
		t.Fatalf("按任务名称命中的任务不符合预期: got=%s want=%s", listResp.Tasks[0].ID, resp.TaskID)
	}

	missResp, err := manager.ListTasks(context.Background(), &types.ListTaskItemsReq{
		Queue:    QueueMaintenance,
		State:    "scheduled",
		TaskName: "not-exists",
		Page:     1,
		PageSize: 10,
	})
	if err != nil {
		t.Fatalf("按不存在任务名称查询失败: %v", err)
	}
	if missResp.Total != 0 || len(missResp.Tasks) != 0 {
		t.Fatalf("期望不存在任务名称无结果，实际 total=%d tasks=%d", missResp.Total, len(missResp.Tasks))
	}
}

// TestListTasksFilterUsesBatchPage 验证任务名称筛选不会逐条读取 Asynq 任务详情。
func TestListTasksFilterUsesBatchPage(t *testing.T) {
	manager, server, cleanup := newTestManagerWithServer(t)
	defer cleanup()
	const taskCount = 24
	if err := manager.RegisterHandler("demo:batch-filter", asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return nil
	})); err != nil {
		t.Fatalf("注册批量筛选测试处理器失败: %v", err)
	}
	// processAtBase 让测试任务稳定停留在 scheduled 状态。
	processAtBase := time.Now().Add(time.Hour).UTC()
	for index := 0; index < taskCount; index++ {
		if _, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
			TaskType:  "demo:batch-filter",
			Payload:   json.RawMessage(fmt.Sprintf(`{"index":%d}`, index)),
			ProcessAt: processAtBase.Add(time.Duration(index) * time.Second).Format(time.RFC3339),
		}); err != nil {
			t.Fatalf("投递批量筛选测试任务失败 index=%d err=%v", index, err)
		}
	}

	// commandsBefore 记录查询前的 Redis 命令数，避免把投递命令计入筛选开销。
	commandsBefore := server.CommandCount()
	listResp, err := manager.ListTasks(context.Background(), &types.ListTaskItemsReq{
		Queue:    QueueDefault,
		State:    "scheduled",
		TaskName: "batch-filter",
		Page:     1,
		PageSize: 20,
	})
	if err != nil {
		t.Fatalf("批量筛选定时任务失败: %v", err)
	}
	if listResp.Total != taskCount || len(listResp.Tasks) != 20 {
		t.Fatalf("批量筛选结果不符合预期: total=%d tasks=%d", listResp.Total, len(listResp.Tasks))
	}
	// batchCommandCount 记录批量筛选路径的 Redis 命令量。
	batchCommandCount := server.CommandCount() - commandsBefore
	// legacyCommandsBefore 用现有逐条详情读取方法建立同一批任务的命令量基准。
	legacyCommandsBefore := server.CommandCount()
	legacyItems, _, err := manager.listTaskInfoPage(context.Background(), manager.namespacedQueueName(QueueDefault), "", "scheduled", 1, taskListFilterPageSize, taskListTimeRange{})
	if err != nil {
		t.Fatalf("读取逐条详情基准失败: %v", err)
	}
	manager.readTaskRuntimeBatch(context.Background(), legacyItems)
	// legacyCommandCount 包含旧筛选路径读取一页详情和运行快照的命令量。
	legacyCommandCount := server.CommandCount() - legacyCommandsBefore
	if batchCommandCount >= legacyCommandCount {
		t.Fatalf("批量筛选 Redis 命令数=%d，不应达到逐条读取基准=%d", batchCommandCount, legacyCommandCount)
	}
}

// TestListTasksFilterDistinguishesScanLimit 验证恰好命中扫描上限仍返回完整结果，存在下一条时返回受控窗口标记。
func TestListTasksFilterDistinguishesScanLimit(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	ctx := context.Background()
	queue := manager.namespacedQueueName(QueueDefault)
	processAt := time.Now().Add(time.Hour)
	enqueue := func(index int) {
		t.Helper()
		taskID := fmt.Sprintf("scan-limit-%05d", index)
		_, err := manager.client.EnqueueContext(
			ctx,
			asynq.NewTask("demo:scan-limit", nil),
			asynq.Queue(queue),
			asynq.TaskID(taskID),
			asynq.ProcessAt(processAt.Add(time.Duration(index)*time.Millisecond)),
		)
		if err != nil {
			t.Fatalf("投递扫描边界测试任务失败 index=%d err=%v", index, err)
		}
	}
	for index := 0; index < taskListFilterMaxRows; index++ {
		enqueue(index)
	}
	req := &types.ListTaskItemsReq{
		Queue:    QueueDefault,
		State:    "scheduled",
		TaskID:   "scan-limit-",
		Page:     1,
		PageSize: 1,
	}
	resp, err := manager.ListTasks(ctx, req)
	if err != nil {
		t.Fatalf("恰好命中扫描上限查询失败: %v", err)
	}
	if resp == nil || resp.Total != taskListFilterMaxRows || len(resp.Tasks) != 1 {
		t.Fatalf("恰好命中扫描上限结果异常: %+v", resp)
	}

	enqueue(taskListFilterMaxRows)
	resp, err = manager.ListTasks(ctx, req)
	if err != nil {
		t.Fatalf("存在第 %d 条任务时应有界返回，实际 err=%v", taskListFilterMaxRows+1, err)
	}
	if resp == nil || !resp.ScanLimited || resp.Total != taskListFilterMaxRows || len(resp.Tasks) != 1 {
		t.Fatalf("扫描超限应返回受控窗口和显式标记，实际 resp=%+v", resp)
	}
}

// TestListTasksFiltersByTaskID 验证任务列表支持按任务 ID 关键字筛选。
func TestListTasksFiltersByTaskID(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterHandler("demo:task-id-filter", asynq.HandlerFunc(func(context.Context, *asynq.Task) error { return nil })); err != nil {
		t.Fatalf("注册任务 ID 筛选测试处理器失败: %v", err)
	}
	firstResp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType:  "demo:task-id-filter",
		Payload:   json.RawMessage(`{"name":"first"}`),
		ProcessAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("投递第一个任务失败: %v", err)
	}
	if _, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType:  "demo:task-id-filter",
		Payload:   json.RawMessage(`{"name":"second"}`),
		ProcessAt: time.Now().Add(2 * time.Minute).UTC().Format(time.RFC3339),
	}); err != nil {
		t.Fatalf("投递第二个任务失败: %v", err)
	}

	listResp, err := manager.ListTasks(context.Background(), &types.ListTaskItemsReq{
		Queue:    QueueDefault,
		State:    "scheduled",
		TaskID:   firstResp.TaskID[:8],
		Page:     1,
		PageSize: 10,
	})
	if err != nil {
		t.Fatalf("按任务 ID 查询任务列表失败: %v", err)
	}
	if listResp.Total != 1 || len(listResp.Tasks) != 1 {
		t.Fatalf("期望按任务 ID 命中 1 条任务，实际 total=%d tasks=%d", listResp.Total, len(listResp.Tasks))
	}
	if listResp.Tasks[0].ID != firstResp.TaskID {
		t.Fatalf("按任务 ID 命中的任务不符合预期: got=%s want=%s", listResp.Tasks[0].ID, firstResp.TaskID)
	}
}

// TestListTasksFiltersPeriodicTaskByWorkflowName 验证周期入口任务可以用 workflow 名称反查。
func TestListTasksFiltersPeriodicTaskByWorkflowName(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterWorkflow(testWorkflowDefinition("user_tag.delta.refresh")); err != nil {
		t.Fatalf("注册周期工作流失败: %v", err)
	}
	manager.mu.Lock()
	manager.periodic = []config.TaskPeriodicConfig{
		enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
			Name:       "user-tag-delta-daily",
			Cron:       "15 3 * * *",
			Workflow:   "user_tag.delta.refresh",
			Queue:      QueueMaintenance,
			ShardTotal: 10,
		}),
	}
	manager.mu.Unlock()
	configs, err := manager.periodicConfigs()
	if err != nil {
		t.Fatalf("生成周期任务配置失败: %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("期望生成 1 个周期任务配置，实际为 %d", len(configs))
	}
	opts := append([]asynq.Option{}, configs[0].Opts...)
	opts = append(opts, asynq.ProcessAt(time.Now().Add(time.Minute)))
	info, err := manager.client.EnqueueContext(context.Background(), configs[0].Task, opts...)
	if err != nil {
		t.Fatalf("投递周期入口任务失败: %v", err)
	}

	listResp, err := manager.ListTasks(context.Background(), &types.ListTaskItemsReq{
		Queue:    QueueMaintenance,
		State:    "scheduled",
		TaskName: "user_tag.delta.refresh",
		Page:     1,
		PageSize: 10,
	})
	if err != nil {
		t.Fatalf("按 workflow 名称查询周期任务失败: %v", err)
	}
	if listResp.Total != 1 || len(listResp.Tasks) != 1 {
		t.Fatalf("期望按 workflow 名称命中 1 条周期任务，实际 total=%d tasks=%d", listResp.Total, len(listResp.Tasks))
	}
	if listResp.Tasks[0].ID != info.ID {
		t.Fatalf("按 workflow 名称命中的周期任务不符合预期: got=%s want=%s", listResp.Tasks[0].ID, info.ID)
	}
}

// TestValidatePeriodicTaskDefinitionsToleratesMissingWorkflow 验证启动期不会因当前版本不认识的周期任务阻断进程。
func TestValidatePeriodicTaskDefinitionsToleratesMissingWorkflow(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	cfg := manager.CurrentConfig()
	cfg.Periodic = []config.TaskPeriodicConfig{
		enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
			Name:     "missing-periodic-workflow",
			Cron:     "*/10 * * * *",
			Workflow: "missing.workflow",
		}),
	}
	manager.UpdateConfig(cfg)

	err := manager.ValidatePeriodicTaskDefinitions()
	if err != nil {
		t.Fatalf("当前版本不认识的周期任务不应阻断启动: %v", err)
	}

	if registerErr := manager.RegisterWorkflow(testWorkflowDefinition("missing.workflow")); registerErr != nil {
		t.Fatalf("注册补齐工作流失败: %v", registerErr)
	}
	if err = manager.ValidatePeriodicTaskDefinitions(); err != nil {
		t.Fatalf("补齐工作流后不应再失败: %v", err)
	}
}

// TestPeriodicConfigInvalidHookThrottlesRepeatedAlert 验证同一条周期配置异常不会在同步循环中重复告警。
func TestPeriodicConfigInvalidHookThrottlesRepeatedAlert(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	if periodicConfigAlertTTL != 5*time.Minute {
		t.Fatalf("周期配置异常告警限频窗口 = %s, want 5m", periodicConfigAlertTTL)
	}

	cfg := manager.CurrentConfig()
	cfg.Periodic = []config.TaskPeriodicConfig{
		enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
			Name:      "missing-periodic-workflow",
			Cron:      "*/10 * * * *",
			Workflow:  "missing.workflow",
			Queue:     QueueDefault,
			UniqueKey: "periodic:missing.workflow",
		}),
	}
	manager.UpdateConfig(cfg)

	var calls atomic.Int32
	var got PeriodicConfigInvalidReport
	if err := manager.RegisterPeriodicConfigInvalidHook(func(ctx context.Context, report PeriodicConfigInvalidReport) error {
		_ = ctx
		calls.Add(1)
		got = report
		return nil
	}); err != nil {
		t.Fatalf("注册周期配置异常钩子失败: %v", err)
	}

	if err := manager.ValidatePeriodicTaskDefinitions(); err != nil {
		t.Fatalf("校验周期任务定义失败: %v", err)
	}
	if err := manager.ValidatePeriodicTaskDefinitions(); err != nil {
		t.Fatalf("重复校验周期任务定义失败: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("同一异常应只触发 1 次告警钩子，实际=%d", calls.Load())
	}
	if got.Task.Name != "missing-periodic-workflow" || got.Reason == "" {
		t.Fatalf("告警报告不符合预期: %+v", got)
	}
	if got.TriggerCount != 1 {
		t.Fatalf("首次告警触发次数 = %d, want 1", got.TriggerCount)
	}

	key := periodicConfigInvalidAlertKey(0, cfg.Periodic[0], got.Reason)
	manager.mu.Lock()
	state := manager.periodicConfigAlertedMap[key]
	state.LastSentAt = time.Now().Add(-periodicConfigAlertTTL - time.Second)
	manager.periodicConfigAlertedMap[key] = state
	manager.mu.Unlock()

	if err := manager.ValidatePeriodicTaskDefinitions(); err != nil {
		t.Fatalf("限频窗口后重复校验周期任务定义失败: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("限频窗口后应再次触发告警钩子，实际=%d", calls.Load())
	}
	if got.TriggerCount != 2 {
		t.Fatalf("窗口后告警触发次数 = %d, want 2", got.TriggerCount)
	}
}

// TestPeriodicConfigInvalidHookFailureRetriesImmediately 验证告警发送失败不会占用限频窗口。
func TestPeriodicConfigInvalidHookFailureRetriesImmediately(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	item := enabledTaskPeriodicConfig(config.TaskPeriodicConfig{
		Name:     "invalid-periodic-hook-retry",
		Cron:     "bad cron",
		Workflow: "missing.workflow",
	})
	reason := "invalid periodic config"
	var calls atomic.Int32
	var triggerCount atomic.Int32
	if err := manager.RegisterPeriodicConfigInvalidHook(func(_ context.Context, report PeriodicConfigInvalidReport) error {
		call := calls.Add(1)
		if call == 1 {
			manager.runPeriodicConfigInvalidHooks(0, item, reason, nil)
			return errors.New("injected periodic alert failure")
		}
		triggerCount.Store(int32(report.TriggerCount))
		return nil
	}); err != nil {
		t.Fatalf("注册周期配置异常钩子失败: %v", err)
	}
	manager.runPeriodicConfigInvalidHooks(0, item, reason, nil)
	key := periodicConfigInvalidAlertKey(0, item, reason)
	manager.mu.Lock()
	state := manager.periodicConfigAlertedMap[key]
	manager.mu.Unlock()
	if !state.LastSentAt.IsZero() || state.SuppressedCount != 1 {
		t.Fatalf("失败发送应回滚限频并保留期间压制次数 state=%+v", state)
	}
	manager.runPeriodicConfigInvalidHooks(0, item, reason, nil)
	if calls.Load() != 2 || triggerCount.Load() != 2 {
		t.Fatalf("失败后应立即重试 calls=%d trigger_count=%d", calls.Load(), triggerCount.Load())
	}
}

// TestTaskRuntimeAlertHookThrottlesRepeatedAlert 验证任务系统运行异常按同一指纹限频告警。
func TestTaskRuntimeAlertHookThrottlesRepeatedAlert(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	if taskRuntimeAlertTTL != 5*time.Minute {
		t.Fatalf("任务运行异常告警限频窗口 = %s, want 5m", taskRuntimeAlertTTL)
	}

	alert := TaskRuntimeAlert{
		Kind:      taskRuntimeAlertKindPeriodicEnqueueFailed,
		Title:     "【P1 周期任务入队失败】",
		Component: "scheduler",
		Operation: "enqueue_periodic_task",
		TaskName:  "周期任务触发:demo",
		TaskType:  TypeWorkflowTrigger,
		Cron:      "*/2 * * * *",
		TaskQueue: QueueDefault,
		UniqueKey: "periodic:demo",
		Reason:    "redis timeout",
	}

	var calls atomic.Int32
	var got TaskRuntimeAlert
	if err := manager.RegisterRuntimeAlertHook(func(ctx context.Context, report TaskRuntimeAlert) error {
		_ = ctx
		calls.Add(1)
		got = report
		return nil
	}); err != nil {
		t.Fatalf("注册任务运行异常钩子失败: %v", err)
	}

	manager.notifyTaskRuntimeAlert(context.Background(), alert)
	alert.Reason = "redis timeout again"
	manager.notifyTaskRuntimeAlert(context.Background(), alert)
	if calls.Load() != 1 {
		t.Fatalf("同一运行异常应只触发 1 次告警钩子，实际=%d", calls.Load())
	}
	if got.TriggerCount != 1 || got.Reason != "redis timeout" {
		t.Fatalf("首次运行异常告警不符合预期: %+v", got)
	}

	key := taskRuntimeAlertKey(alert)
	manager.mu.Lock()
	state := manager.runtimeAlertedMap[key]
	state.LastSentAt = time.Now().Add(-taskRuntimeAlertTTL - time.Second)
	manager.runtimeAlertedMap[key] = state
	manager.mu.Unlock()

	manager.notifyTaskRuntimeAlert(context.Background(), alert)
	if calls.Load() != 2 {
		t.Fatalf("限频窗口后应再次触发运行异常钩子，实际=%d", calls.Load())
	}
	if got.TriggerCount != 2 || got.Reason != "redis timeout again" {
		t.Fatalf("窗口后运行异常告警不符合预期: %+v", got)
	}
}

// TestTaskRuntimeAlertHookFailureRetriesImmediately 验证运行告警发送失败不会占用限频窗口。
func TestTaskRuntimeAlertHookFailureRetriesImmediately(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	alert := TaskRuntimeAlert{
		Kind:       taskRuntimeAlertKindPeriodicEnqueueFailed,
		Component:  "scheduler",
		Operation:  "enqueue_periodic_task",
		TaskName:   "周期任务触发:retry-alert",
		TaskType:   TypeWorkflowTrigger,
		TaskQueue:  QueueDefault,
		UniqueKey:  "periodic:retry-alert",
		Reason:     "redis timeout",
		OccurredAt: time.Now(),
	}
	var calls atomic.Int32
	var triggerCount atomic.Int32
	if err := manager.RegisterRuntimeAlertHook(func(ctx context.Context, report TaskRuntimeAlert) error {
		call := calls.Add(1)
		if call == 1 {
			manager.notifyTaskRuntimeAlert(ctx, alert)
			return errors.New("injected runtime alert failure")
		}
		triggerCount.Store(int32(report.TriggerCount))
		return nil
	}); err != nil {
		t.Fatalf("注册任务运行异常钩子失败: %v", err)
	}
	manager.notifyTaskRuntimeAlert(context.Background(), alert)
	key := taskRuntimeAlertKey(alert)
	manager.mu.Lock()
	state := manager.runtimeAlertedMap[key]
	manager.mu.Unlock()
	if !state.LastSentAt.IsZero() || state.SuppressedCount != 1 {
		t.Fatalf("失败发送应回滚限频并保留期间压制次数 state=%+v", state)
	}
	manager.notifyTaskRuntimeAlert(context.Background(), alert)
	if calls.Load() != 2 || triggerCount.Load() != 2 {
		t.Fatalf("失败后应立即重试 calls=%d trigger_count=%d", calls.Load(), triggerCount.Load())
	}
}

// TestTaskRuntimeAlertThrottleUsesSendWindow 验证业务上报历史发生时间不会绕过当前发送窗口限频。
func TestTaskRuntimeAlertThrottleUsesSendWindow(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	var calls atomic.Int32
	if err := manager.RegisterRuntimeAlertHook(func(ctx context.Context, report TaskRuntimeAlert) error {
		_ = ctx
		_ = report
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("注册任务运行异常钩子失败: %v", err)
	}

	alert := TaskRuntimeAlert{
		Kind:       taskRuntimeAlertKindPeriodicEnqueueFailed,
		Component:  "scheduler",
		Operation:  "enqueue_periodic_task",
		TaskName:   "周期任务触发:demo",
		TaskType:   TypeWorkflowTrigger,
		TaskQueue:  QueueDefault,
		UniqueKey:  "periodic:demo",
		Reason:     "redis timeout",
		OccurredAt: time.Now().Add(-time.Hour),
	}
	manager.notifyTaskRuntimeAlert(context.Background(), alert)
	alert.OccurredAt = time.Time{}
	alert.Reason = "redis timeout again"
	manager.notifyTaskRuntimeAlert(context.Background(), alert)
	if calls.Load() != 1 {
		t.Fatalf("历史发生时间不应绕过当前发送窗口限频，实际告警次数=%d", calls.Load())
	}
}

// TestRunTaskMovesScheduledTaskToPending 验证对应场景。
func TestRunTaskMovesScheduledTaskToPending(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterHandler("demo:run", asynq.HandlerFunc(func(context.Context, *asynq.Task) error { return nil })); err != nil {
		t.Fatalf("注册立即执行测试处理器失败: %v", err)
	}
	resp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType:  "demo:run",
		Payload:   json.RawMessage(`{"demo":true}`),
		ProcessAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("投递任务失败: %v", err)
	}
	if err := manager.RunTask(context.Background(), &types.OperateTaskReq{
		Queue:  QueueDefault,
		TaskID: resp.TaskID,
	}); err != nil {
		t.Fatalf("立即执行任务失败: %v", err)
	}
	waitForCondition(t, 3*time.Second, func() bool {
		info, err := manager.inspector.GetTaskInfo(manager.namespacedQueueName(QueueDefault), resp.TaskID)
		return err == nil && info.State == asynq.TaskStatePending
	})
}

// TestDeleteTaskRemovesScheduledTask 验证对应场景。
func TestDeleteTaskRemovesScheduledTask(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	if err := manager.RegisterHandler("demo:delete", asynq.HandlerFunc(func(context.Context, *asynq.Task) error { return nil })); err != nil {
		t.Fatalf("注册删除测试处理器失败: %v", err)
	}
	resp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType:  "demo:delete",
		Payload:   json.RawMessage(`{"demo":true}`),
		ProcessAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("投递任务失败: %v", err)
	}
	if err := manager.DeleteTask(context.Background(), &types.OperateTaskReq{
		Queue:  QueueDefault,
		TaskID: resp.TaskID,
	}); err != nil {
		t.Fatalf("删除任务失败: %v", err)
	}
	if _, err := manager.inspector.GetTaskInfo(manager.namespacedQueueName(QueueDefault), resp.TaskID); !errors.Is(err, asynq.ErrTaskNotFound) {
		t.Fatalf("期望删除后任务不存在，实际为 %v", err)
	}
}

// TestTaskCompletedRetentionUsesBoundedConfig 验证成功任务默认短留并允许在安全范围内调整。
func TestTaskCompletedRetentionUsesBoundedConfig(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	cfg := manager.CurrentConfig()
	if cfg.ArchivedRetentionSeconds != 0 {
		t.Fatalf("测试配置应模拟未配置归档保留时间，实际=%d", cfg.ArchivedRetentionSeconds)
	}
	if got := manager.CompletedRetention(); got != taskCompletedRetention {
		t.Fatalf("成功任务默认保留时间 = %s，期望 %s", got, taskCompletedRetention)
	}
	cfg.CompletedRetentionSeconds = 600
	manager.UpdateConfig(cfg)
	if got := manager.CompletedRetention(); got != 10*time.Minute {
		t.Fatalf("成功任务配置保留时间 = %s，期望 10m", got)
	}
	if got, want := manager.ArchivedRetention(), time.Duration(taskArchivedRetentionDefaultSeconds)*time.Second; got != want {
		t.Fatalf("未配置 archived_retention_seconds 时默认值 = %s，期望 %s", got, want)
	}
}

// TestCompletedScoreRangeUsesRetention 验证 completed 索引查询使用实际保留期反推完成时间。
func TestCompletedScoreRangeUsesRetention(t *testing.T) {
	start := time.Date(2026, 7, 17, 10, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	minScore, maxScore, ok := descZSetScoreRange("completed", taskListTimeRange{
		start:    start,
		end:      end,
		hasRange: true,
	}, taskCompletedRetention)
	if !ok {
		t.Fatal("completed 时间窗口应支持 zset score 查询")
	}
	wantMin := strconv.FormatInt(start.Add(taskCompletedRetention).Unix(), 10)
	wantMax := strconv.FormatInt(end.Add(taskCompletedRetention).Unix(), 10)
	if minScore != wantMin || maxScore != wantMax {
		t.Fatalf("completed score 窗口=(%s,%s)，期望=(%s,%s)", minScore, maxScore, wantMin, wantMax)
	}
}

// TestPeriodicTaskWithScheduledAt 验证计划时间传播及按计划时刻隔离入口去重。
func TestPeriodicTaskWithScheduledAt(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	original := asynq.NewTaskWithHeaders(TypeWorkflowTrigger, mustJSONBytes(WorkflowTriggerPayload{
		WorkflowName: "task_report.daily_summary",
		Source:       WorkflowSourcePeriodic,
		UniqueKey:    "periodic:task_report.daily_summary",
	}), map[string]string{headerTaskName: "demo"})
	scheduledAt := time.Date(2026, 7, 18, 10, 0, 0, 0, time.FixedZone("CST", 8*3600))
	plain, err := periodicTaskWithScheduledAt(original, scheduledAt, false)
	if err != nil {
		t.Fatalf("复制普通周期任务失败: %v", err)
	}
	if plain.Headers()[HeaderScheduledAt] != scheduledAt.Format(time.RFC3339) {
		t.Fatalf("周期任务计划触发时间=%q，期望=%q", plain.Headers()[HeaderScheduledAt], scheduledAt.Format(time.RFC3339))
	}
	if original.Headers()[HeaderScheduledAt] != "" {
		t.Fatalf("原周期任务不应被修改: %+v", original.Headers())
	}
	if plain.Type() != original.Type() || string(plain.Payload()) != string(original.Payload()) {
		t.Fatalf("周期任务副本未保留类型或载荷")
	}
	options := []asynq.Option{
		asynq.Queue(manager.namespacedQueueName(QueueMaintenance)),
		asynq.Unique(48 * time.Hour),
	}
	if _, err := manager.client.EnqueueContext(context.Background(), original, options...); err != nil {
		t.Fatalf("投递原周期任务失败: %v", err)
	}
	if _, err := manager.client.EnqueueContext(context.Background(), plain, options...); !errors.Is(err, asynq.ErrDuplicateTask) {
		t.Fatalf("动态 header 不应改变 Asynq Unique 结果，实际=%v", err)
	}

	first, err := periodicTaskWithScheduledAt(original, scheduledAt, true)
	if err != nil {
		t.Fatalf("构造首日按计划去重任务失败: %v", err)
	}
	var firstPayload WorkflowTriggerPayload
	if err = json.Unmarshal(first.Payload(), &firstPayload); err != nil || firstPayload.ScheduledAt != scheduledAt.Format(time.RFC3339) {
		t.Fatalf("首日计划时间未写入 payload: payload=%+v error=%v", firstPayload, err)
	}
	if _, err = manager.client.EnqueueContext(context.Background(), first, options...); err != nil {
		t.Fatalf("投递首日按计划去重任务失败: %v", err)
	}
	firstDuplicate, err := periodicTaskWithScheduledAt(original, scheduledAt, true)
	if err != nil {
		t.Fatalf("构造首日重复任务失败: %v", err)
	}
	if _, err = manager.client.EnqueueContext(context.Background(), firstDuplicate, options...); !errors.Is(err, asynq.ErrDuplicateTask) {
		t.Fatalf("同一计划时刻必须保持去重，实际=%v", err)
	}
	second, err := periodicTaskWithScheduledAt(original, scheduledAt.Add(24*time.Hour), true)
	if err != nil {
		t.Fatalf("构造次日按计划去重任务失败: %v", err)
	}
	if _, err = manager.client.EnqueueContext(context.Background(), second, options...); err != nil {
		t.Fatalf("不同计划时刻不应被旧入口去重键拦截: %v", err)
	}
}

// TestPeriodicTaskUsesScheduledUnique 验证只有显式声明的工作流会改变周期入口去重摘要。
func TestPeriodicTaskUsesScheduledUnique(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	definition := testWorkflowDefinition("scheduled-unique-workflow")
	definition.PeriodicUniqueBySchedule = true
	if err := manager.RegisterWorkflow(definition); err != nil {
		t.Fatalf("注册按计划时刻去重工作流失败: %v", err)
	}
	scheduler := newPeriodicTaskScheduler(manager)
	task := asynq.NewTask(TypeWorkflowTrigger, mustJSONBytes(WorkflowTriggerPayload{WorkflowName: definition.Name}))
	if !periodicTaskUsesScheduledUnique(scheduler, &asynq.PeriodicTaskConfig{Task: task}) {
		t.Fatal("显式声明的工作流应按计划时刻隔离周期入口去重")
	}
	plainDefinition := testWorkflowDefinition("plain-periodic-workflow")
	if err := manager.RegisterWorkflow(plainDefinition); err != nil {
		t.Fatalf("注册普通周期工作流失败: %v", err)
	}
	plainTask := asynq.NewTask(TypeWorkflowTrigger, mustJSONBytes(WorkflowTriggerPayload{WorkflowName: plainDefinition.Name}))
	if periodicTaskUsesScheduledUnique(scheduler, &asynq.PeriodicTaskConfig{Task: plainTask}) {
		t.Fatal("未声明的工作流不应改变现有周期入口去重语义")
	}
}

// TestPeriodicScheduledAtUsesCronEntry 验证回调延迟时仍使用 cron 登记的本轮计划时刻。
func TestPeriodicScheduledAtUsesCronEntry(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)
	delayedAt := time.Date(2026, 7, 18, 10, 1, 20, 0, loc)
	want := time.Date(2026, 7, 18, 10, 0, 0, 0, loc)
	if got := periodicScheduledAt(want, delayedAt); !got.Equal(want) {
		t.Fatalf("迟到回调计划时间=%s，期望=%s", got, want)
	}
	if got, fallback := periodicScheduledAt(time.Time{}, delayedAt), delayedAt.Truncate(time.Second); !got.Equal(fallback) {
		t.Fatalf("缺失 cron 记录时计划时间=%s，期望=%s", got, fallback)
	}
}

// TestWorkflowScheduledAtPersistsAndPropagates 验证周期触发时间可恢复并传递到节点任务。
func TestWorkflowScheduledAtPersistsAndPropagates(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	def := testWorkflowDefinition("scheduled-at-propagation")
	if err := manager.RegisterWorkflow(def); err != nil {
		t.Fatalf("注册工作流失败: %v", err)
	}
	workflowID := "wf-scheduled-at"
	scheduledAt := "2026-07-18T10:00:00+08:00"
	if _, err := manager.StartWorkflow(context.Background(), WorkflowStartSpec{
		WorkflowID:   workflowID,
		Name:         def.Name,
		Queue:        QueueMaintenance,
		Source:       WorkflowSourcePeriodic,
		PeriodicName: "scheduled-at-periodic",
		ScheduledAt:  scheduledAt,
	}); err != nil {
		t.Fatalf("启动工作流失败: %v", err)
	}
	stored, err := manager.workflowSpecByID(context.Background(), workflowID)
	if err != nil {
		t.Fatalf("恢复工作流启动参数失败: %v", err)
	}
	if stored.ScheduledAt != scheduledAt {
		t.Fatalf("恢复的计划触发时间=%q，期望=%q", stored.ScheduledAt, scheduledAt)
	}
	info, err := manager.inspector.GetTaskInfo(manager.namespacedQueueName(QueueMaintenance), workflowNodeTaskID(workflowID, "root", 0))
	if err != nil {
		t.Fatalf("读取节点任务失败: %v", err)
	}
	if info.Headers[HeaderScheduledAt] != scheduledAt {
		t.Fatalf("节点任务计划触发时间=%q，期望=%q", info.Headers[HeaderScheduledAt], scheduledAt)
	}
	if info.Retention != taskCompletedRetention {
		t.Fatalf("工作流节点 retention=%s，期望=%s", info.Retention, taskCompletedRetention)
	}
}

// TestWorkflowUniqueKeyForSchedule 验证同一计划时刻稳定防重且相邻周期使用不同幂等键。
func TestWorkflowUniqueKeyForSchedule(t *testing.T) {
	first, err := workflowUniqueKeyForSchedule("periodic:report", "2026-07-18T10:00:00+08:00")
	if err != nil {
		t.Fatalf("生成首个计划幂等键失败: %v", err)
	}
	second, err := workflowUniqueKeyForSchedule("periodic:report", "2026-07-19T10:00:00+08:00")
	if err != nil {
		t.Fatalf("生成下一周期幂等键失败: %v", err)
	}
	if first == second {
		t.Fatalf("相邻周期幂等键不应相同: %s", first)
	}
	if equivalent, err := workflowUniqueKeyForSchedule("periodic:report", "2026-07-18T02:00:00Z"); err != nil || equivalent != first {
		t.Fatalf("同一时刻幂等键不稳定: got=%q want=%q error=%v", equivalent, first, err)
	}
	if _, err := workflowUniqueKeyForSchedule("periodic:report", "invalid"); err == nil {
		t.Fatal("非法计划时刻应返回错误")
	}
}

// TestDeleteExpiredArchivedTasksRemovesTaskCache 验证归档失败任务过期后会同步清理 zset、hash 和唯一键。
func TestDeleteExpiredArchivedTasksRemovesTaskCache(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()

	ctx := context.Background()
	internalQueue := manager.namespacedQueueName(QueueMaintenance)
	archivedKey, err := keys.TaskAsynqStateZSetKey(internalQueue, "archived")
	if err != nil {
		t.Fatalf("生成 archived key 失败: %v", err)
	}
	oldTaskIDs := []string{"old-archived-task-1", "old-archived-task-2", "old-archived-task-3"}
	newTaskID := "new-archived-task"
	uniqueKey := keys.TaskAsynqUniqueKey(internalQueue, "demo:archived", []byte("test"))
	newTaskKey := keys.TaskAsynqTaskHashKey(internalQueue, newTaskID)
	if err = manager.redis.ZAdd(ctx, archivedKey,
		redis.Z{Score: 100, Member: oldTaskIDs[0]},
		redis.Z{Score: 110, Member: oldTaskIDs[1]},
		redis.Z{Score: 120, Member: oldTaskIDs[2]},
		redis.Z{Score: 300, Member: newTaskID},
	).Err(); err != nil {
		t.Fatalf("写入 archived zset 失败: %v", err)
	}
	for index, oldTaskID := range oldTaskIDs {
		fields := []any{"state", "archived"}
		if index == 0 {
			fields = append(fields, "unique_key", uniqueKey)
		}
		if err = manager.redis.HSet(ctx, keys.TaskAsynqTaskHashKey(internalQueue, oldTaskID), fields...).Err(); err != nil {
			t.Fatalf("写入过期任务 hash 失败: %v", err)
		}
	}
	if err = manager.redis.HSet(ctx, newTaskKey, "state", "archived").Err(); err != nil {
		t.Fatalf("写入新任务 hash 失败: %v", err)
	}
	if err = manager.redis.Set(ctx, uniqueKey, oldTaskIDs[0], time.Hour).Err(); err != nil {
		t.Fatalf("写入唯一键失败: %v", err)
	}

	deleted, err := manager.cleanupExpiredArchivedQueue(ctx, internalQueue, 200, 2, 2)
	if err != nil {
		t.Fatalf("清理过期归档任务失败: %v", err)
	}
	if deleted != int64(len(oldTaskIDs)) {
		t.Fatalf("清理数量 = %d，期望 %d", deleted, len(oldTaskIDs))
	}
	oldTaskKeys := []string{uniqueKey}
	for _, oldTaskID := range oldTaskIDs {
		oldTaskKeys = append(oldTaskKeys, keys.TaskAsynqTaskHashKey(internalQueue, oldTaskID))
	}
	if exists := manager.redis.Exists(ctx, oldTaskKeys...).Val(); exists != 0 {
		t.Fatalf("过期任务 hash 或唯一键仍存在，exists=%d", exists)
	}
	for _, oldTaskID := range oldTaskIDs {
		if _, err = manager.redis.ZScore(ctx, archivedKey, oldTaskID).Result(); !errors.Is(err, redis.Nil) {
			t.Fatalf("过期任务 zset 成员仍存在，task_id=%s err=%v", oldTaskID, err)
		}
	}
	if exists := manager.redis.Exists(ctx, newTaskKey).Val(); exists != 1 {
		t.Fatalf("新任务 hash 应保留，exists=%d", exists)
	}
	if _, err = manager.redis.ZScore(ctx, archivedKey, newTaskID).Result(); err != nil {
		t.Fatalf("新任务 zset 成员应保留，err=%v", err)
	}
}

// TestLeaderRunnerAvoidsDuplicateSchedulerAndFailsOver 验证对应场景。
func TestLeaderRunnerAvoidsDuplicateSchedulerAndFailsOver(t *testing.T) {
	server, client := newTestRedis(t)
	defer server.Close()
	defer client.Close()

	var acquired1 atomic.Int32
	var acquired2 atomic.Int32
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	runner1 := NewLeaderRunner(client, "task:test:leader", 300*time.Millisecond, 100*time.Millisecond, func(context.Context) (func(), error) {
		acquired1.Add(1)
		return func() {}, nil
	})
	go runner1.Start(ctx1)
	waitForCondition(t, 2*time.Second, func() bool { return acquired1.Load() == 1 })

	runner2 := NewLeaderRunner(client, "task:test:leader", 300*time.Millisecond, 100*time.Millisecond, func(context.Context) (func(), error) {
		acquired2.Add(1)
		return func() {}, nil
	})
	go runner2.Start(ctx2)

	time.Sleep(250 * time.Millisecond)
	if acquired2.Load() != 0 {
		t.Fatalf("期望首个 leader 存活期间第二个 leader 无法抢锁，实际为 %d", acquired2.Load())
	}

	cancel1()
	waitForCondition(t, 2*time.Second, func() bool { return acquired2.Load() == 1 })
}

// newTestManager 构造测试依赖。
func newTestManager(t *testing.T) (*Manager, func()) {
	t.Helper()
	manager, _, cleanup := newTestManagerWithServer(t)
	return manager, cleanup
}

// newTestManagerWithServer 构造可推进 Redis 时钟的测试依赖。
func newTestManagerWithServer(t *testing.T) (*Manager, *miniredis.Miniredis, func()) {
	t.Helper()
	server, client := newTestRedis(t)
	cfg := config.TaskQueueConfig{
		Enabled:                 true,
		AppID:                   "1",
		DefaultQueue:            QueueDefault,
		DefaultRetry:            1,
		DefaultTimeoutSeconds:   1,
		DefaultUniqueTTLSeconds: 60,
		DelayedTaskCheckSeconds: 1,
		TaskCheckSeconds:        1,
		Queues: map[string]int{
			QueueDefault:     1,
			QueueMaintenance: 1,
		},
	}
	useRuntimeAppID(t, cfg.AppID)
	manager := New(cfg, client)
	if manager == nil {
		t.Fatal("期望成功创建测试任务管理器")
	}
	cleanup := func() {
		_ = manager.Stop(context.Background())
		_ = client.Close()
		server.Close()
	}
	return manager, server, cleanup
}

// useRuntimeAppID 表示测试辅助逻辑。
func useRuntimeAppID(t *testing.T, appID string) {
	t.Helper()
	prev := runtimecfg.Get()
	runtimecfg.Set(config.Config{AppID: appID})
	t.Cleanup(func() {
		runtimecfg.Restore(prev)
	})
}

// newTestRedis 构造测试依赖。
func newTestRedis(t *testing.T) (*miniredis.Miniredis, redis.UniversalClient) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	return server, client
}

// testNoopPayload 表示测试辅助逻辑。
func testNoopPayload(spec WorkflowStartSpec, node *WorkflowNodeDefinition, shardIndex, shardTotal int) ([]byte, error) {
	return mustJSONBytes(NoopPayload{
		WorkflowTaskMeta: WorkflowTaskMeta{
			WorkflowID:   spec.WorkflowID,
			WorkflowName: spec.Name,
			WorkflowNode: node.Name,
			ShardIndex:   shardIndex,
			ShardTotal:   shardTotal,
		},
		Note: "test",
	}), nil
}

// testWorkflowDefinition 表示测试辅助逻辑。
func testWorkflowDefinition(name string) *WorkflowDefinition {
	return &WorkflowDefinition{
		Name: name,
		Nodes: map[string]*WorkflowNodeDefinition{
			"root": {
				TaskType:     TypeWorkflowNoop,
				BuildPayload: testNoopPayload,
			},
		},
	}
}

// findWorkflowIDWithoutGrayShard 表示测试辅助逻辑。
func findWorkflowIDWithoutGrayShard(t *testing.T, nodeName string, shardTotal int, grayPercent int) string {
	t.Helper()
	for i := 0; i < 1000; i++ {
		workflowID := "wf-gray-skip-" + time.Now().Add(time.Duration(i)*time.Nanosecond).Format("150405.000000000")
		if len(selectedShardIndexes(workflowID, nodeName, shardTotal, grayPercent, true)) == 0 {
			return workflowID
		}
	}
	t.Fatalf("未找到满足灰度全跳过条件的工作流 ID: node=%s shardTotal=%d grayPercent=%d", nodeName, shardTotal, grayPercent)
	return ""
}

// waitForCondition 表示测试辅助逻辑。
func waitForCondition(t *testing.T, timeout time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("超时前条件仍未满足")
}
