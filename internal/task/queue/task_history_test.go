package taskqueue

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	tasklimits "admin/internal/task/limits"
	taskstats "admin/internal/task/stats"
	"admin/internal/types"

	"github.com/Is999/go-utils/errors"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// TestBoundWorkflowHistorySnapshotKeepsSmallShardDetails 验证小型工作流历史保留分片处理明细。
func TestBoundWorkflowHistorySnapshotKeepsSmallShardDetails(t *testing.T) {
	status := &types.TaskWorkflowStatusResp{
		WorkflowID: "workflow-small",
		Targets:    []string{"user:1"},
		ExecutionTrace: &taskstats.Snapshot{
			TotalCount: 10,
			Details:    []taskstats.Detail{{Action: "read", Name: "all", Count: 10}},
		},
		Nodes: []types.TaskWorkflowNodeItem{{
			Name: "refresh",
			ExecutionTrace: &taskstats.Snapshot{
				TotalCount: 10,
				Details:    []taskstats.Detail{{Action: "read", Name: "users", Count: 10}},
			},
			ShardTraces: []types.TaskWorkflowShardTraceItem{{
				ShardIndex: 0,
				ShardTotal: 1,
				ExecutionTrace: &taskstats.Snapshot{
					TotalCount: 10,
					Details:    []taskstats.Detail{{Action: "read", Name: "user:1", Count: 10}},
				},
			}},
		}},
	}

	boundWorkflowHistorySnapshot(status)

	if len(status.Targets) != 0 || len(status.ExecutionTrace.Details) != 0 {
		t.Fatalf("历史快照应移除目标和重复的全局明细: %+v", status)
	}
	if status.DetailLevel != "shard" || status.DetailTruncated {
		t.Fatalf("小型快照应完整保留分片明细: %+v", status)
	}
	if len(status.Nodes[0].ExecutionTrace.Details) != 1 || len(status.Nodes[0].ShardTraces[0].ExecutionTrace.Details) != 1 {
		t.Fatalf("小型快照的节点和分片明细不应被裁剪: %+v", status.Nodes[0])
	}
	assertWorkflowHistorySnapshotBounded(t, status)
}

// TestBoundWorkflowHistorySnapshotSanitizesErrors 验证延长保留的工作流错误不会携带敏感字段原值。
func TestBoundWorkflowHistorySnapshotSanitizesErrors(t *testing.T) {
	status := &types.TaskWorkflowStatusResp{
		WorkflowID:   "workflow-error",
		ErrorMessage: `{"token":"workflow-secret"}`,
		Nodes: []types.TaskWorkflowNodeItem{{
			Name:         "refresh",
			ErrorMessage: `{"password":"node-secret"}`,
		}},
	}

	boundWorkflowHistorySnapshot(status)

	if strings.Contains(status.ErrorMessage, "workflow-secret") || strings.Contains(status.Nodes[0].ErrorMessage, "node-secret") {
		t.Fatalf("历史错误信息未脱敏: %+v", status)
	}
}

// TestBoundWorkflowHistorySnapshotDropsShardDetailsBeforeShardSummary 验证超限时优先保留分片指标。
func TestBoundWorkflowHistorySnapshotDropsShardDetailsBeforeShardSummary(t *testing.T) {
	shards := make([]types.TaskWorkflowShardTraceItem, tasklimits.MaxShardTotal)
	for index := range shards {
		shards[index] = types.TaskWorkflowShardTraceItem{
			ShardIndex: index,
			ShardTotal: len(shards),
			Status:     "success",
			ExecutionTrace: &taskstats.Snapshot{
				TotalCount: 1,
				Details: []taskstats.Detail{{
					Action: "read",
					Name:   strings.Repeat("detail-", 160),
					Count:  1,
				}},
			},
		}
	}
	status := &types.TaskWorkflowStatusResp{
		WorkflowID: "workflow-shard-summary",
		Nodes: []types.TaskWorkflowNodeItem{{
			Name:           "refresh",
			ExecutionTrace: &taskstats.Snapshot{TotalCount: 64},
			ShardTraces:    shards,
		}},
	}

	boundWorkflowHistorySnapshot(status)

	if status.DetailLevel != "shard" || !status.DetailTruncated || len(status.Nodes[0].ShardTraces) != len(shards) {
		t.Fatalf("超限快照应先保留分片指标并标记裁剪: level=%s truncated=%v shards=%d", status.DetailLevel, status.DetailTruncated, len(status.Nodes[0].ShardTraces))
	}
	for _, shard := range status.Nodes[0].ShardTraces {
		if shard.ExecutionTrace != nil && len(shard.ExecutionTrace.Details) != 0 {
			t.Fatal("分片高基数明细应先于分片指标被裁剪")
		}
	}
	assertWorkflowHistorySnapshotBounded(t, status)
}

// TestBoundWorkflowHistorySnapshotCompactsLargeShardSummary 验证大量分片优先压缩重复字段并保留核心指标。
func TestBoundWorkflowHistorySnapshotCompactsLargeShardSummary(t *testing.T) {
	nodes := make([]types.TaskWorkflowNodeItem, 6)
	for nodeIndex := range nodes {
		shards := make([]types.TaskWorkflowShardTraceItem, tasklimits.MaxShardTotal)
		for index := range shards {
			shards[index] = types.TaskWorkflowShardTraceItem{
				ShardIndex: index,
				ShardTotal: len(shards),
				Status:     "success",
				Progress:   &taskstats.Progress{Status: taskstats.ProgressStatusSuccess, Total: 1, Finished: 1, Succeeded: 1, Percent: 100},
				ExecutionTrace: &taskstats.Snapshot{
					Name:       strings.Repeat("shard-trace-", 100),
					StartedAt:  "2026-08-05T18:07:17+08:00",
					FinishedAt: "2026-08-05T18:07:30+08:00",
					DurationMS: 13_000,
					TotalCount: 1,
				},
			}
		}
		nodes[nodeIndex] = types.TaskWorkflowNodeItem{
			Name:           "refresh-" + strconv.Itoa(nodeIndex),
			ExecutionTrace: &taskstats.Snapshot{TotalCount: tasklimits.MaxShardTotal, Details: []taskstats.Detail{{Action: "read", Name: "users", Count: tasklimits.MaxShardTotal}}},
			ShardTraces:    shards,
		}
	}
	status := &types.TaskWorkflowStatusResp{
		WorkflowID: "workflow-node-summary",
		Nodes:      nodes,
	}

	boundWorkflowHistorySnapshot(status)

	if status.DetailLevel != "shard" || !status.DetailTruncated || len(status.Nodes[0].ShardTraces) != tasklimits.MaxShardTotal {
		t.Fatalf("大量分片应压缩后保留分片指标: level=%s truncated=%v shards=%d", status.DetailLevel, status.DetailTruncated, len(status.Nodes[0].ShardTraces))
	}
	first := status.Nodes[0].ShardTraces[0]
	if first.Progress != nil || first.ExecutionTrace == nil || first.ExecutionTrace.Name != "" || first.ExecutionTrace.StartedAt != "" || first.ExecutionTrace.TotalCount != 1 || first.ExecutionTrace.DurationMS != 13_000 {
		t.Fatalf("分片压缩结果异常: %+v", first)
	}
	assertWorkflowHistorySnapshotBounded(t, status)
}

// TestBoundWorkflowHistorySnapshotTrimsNodeDetailsBeforeDroppingShards 验证超限时先保留代表性节点明细和分片指标。
func TestBoundWorkflowHistorySnapshotTrimsNodeDetailsBeforeDroppingShards(t *testing.T) {
	details := make([]taskstats.Detail, 256)
	for index := range details {
		details[index] = taskstats.Detail{
			Action: "read",
			Name:   strings.Repeat("node-detail-", 40) + strconv.Itoa(index),
			Count:  1,
		}
	}
	shards := make([]types.TaskWorkflowShardTraceItem, tasklimits.MaxShardTotal)
	for index := range shards {
		shards[index] = types.TaskWorkflowShardTraceItem{
			ShardIndex: index,
			ShardTotal: len(shards),
			Status:     "success",
			ExecutionTrace: &taskstats.Snapshot{
				TotalCount: 1,
				DurationMS: 10,
			},
		}
	}
	status := &types.TaskWorkflowStatusResp{
		WorkflowID: "workflow-node-detail-with-shards",
		Nodes: []types.TaskWorkflowNodeItem{{
			Name:           "refresh",
			ExecutionTrace: &taskstats.Snapshot{TotalCount: int64(len(shards)), Details: details},
			ShardTraces:    shards,
		}},
	}

	boundWorkflowHistorySnapshot(status)

	kept := len(status.Nodes[0].ExecutionTrace.Details)
	if status.DetailLevel != "shard" || !status.DetailTruncated || len(status.Nodes[0].ShardTraces) != len(shards) {
		t.Fatalf("节点明细可裁剪时应继续保留分片指标: level=%s truncated=%v shards=%d", status.DetailLevel, status.DetailTruncated, len(status.Nodes[0].ShardTraces))
	}
	if kept < taskHistoryNodeDetailFloor || kept >= len(details) {
		t.Fatalf("节点代表性明细保留数量异常: kept=%d floor=%d", kept, taskHistoryNodeDetailFloor)
	}
	assertWorkflowHistorySnapshotBounded(t, status)
}

// TestBoundWorkflowHistorySnapshotFallsBackToNodeDetails 验证极端分片量最终降级为节点聚合明细。
func TestBoundWorkflowHistorySnapshotFallsBackToNodeDetails(t *testing.T) {
	shards := make([]types.TaskWorkflowShardTraceItem, 4096)
	for index := range shards {
		shards[index] = types.TaskWorkflowShardTraceItem{
			ShardIndex: index,
			ShardTotal: len(shards),
			Status:     "success",
			ExecutionTrace: &taskstats.Snapshot{
				Name:       strings.Repeat("shard-trace-", 10),
				TotalCount: 1,
			},
		}
	}
	status := &types.TaskWorkflowStatusResp{
		WorkflowID: "workflow-node-summary",
		Nodes: []types.TaskWorkflowNodeItem{{
			Name:           "refresh",
			ExecutionTrace: &taskstats.Snapshot{TotalCount: int64(len(shards)), Details: []taskstats.Detail{{Action: "read", Name: "users", Count: int64(len(shards))}}},
			ShardTraces:    shards,
		}},
	}

	boundWorkflowHistorySnapshot(status)

	if status.DetailLevel != "node" || !status.DetailTruncated || len(status.Nodes[0].ShardTraces) != 0 {
		t.Fatalf("极端分片量应降级为节点级快照: level=%s truncated=%v shards=%d", status.DetailLevel, status.DetailTruncated, len(status.Nodes[0].ShardTraces))
	}
	if len(status.Nodes[0].ExecutionTrace.Details) != 1 {
		t.Fatalf("节点聚合明细应在容量允许时保留: %+v", status.Nodes[0].ExecutionTrace)
	}
	assertWorkflowHistorySnapshotBounded(t, status)
}

// TestBoundWorkflowHistorySnapshotTrimsOversizedNodeDetails 验证节点明细自身超限时仍能有界保留一部分信息。
func TestBoundWorkflowHistorySnapshotTrimsOversizedNodeDetails(t *testing.T) {
	details := make([]taskstats.Detail, 1024)
	for index := range details {
		details[index] = taskstats.Detail{
			Action: "read",
			Name:   strings.Repeat("node-detail-", 20) + strconv.Itoa(index),
			Count:  1,
		}
	}
	status := &types.TaskWorkflowStatusResp{
		WorkflowID: "workflow-node-details",
		Nodes: []types.TaskWorkflowNodeItem{{
			Name:           "refresh",
			ExecutionTrace: &taskstats.Snapshot{TotalCount: 128, Details: details},
		}},
	}

	boundWorkflowHistorySnapshot(status)

	kept := len(status.Nodes[0].ExecutionTrace.Details)
	if status.DetailLevel != "node" || !status.DetailTruncated || kept == 0 || kept >= len(details) {
		t.Fatalf("超限节点明细应有界裁剪并保留部分信息: level=%s truncated=%v kept=%d", status.DetailLevel, status.DetailTruncated, kept)
	}
	assertWorkflowHistorySnapshotBounded(t, status)
}

// assertWorkflowHistorySnapshotBounded 校验快照和完整事件都未突破硬上限。
func assertWorkflowHistorySnapshotBounded(t *testing.T, status *types.TaskWorkflowStatusResp) {
	t.Helper()
	raw, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("序列化工作流快照失败: %v", err)
	}
	if len(raw) > taskHistoryWorkflowSnapshotMaxBytes {
		t.Fatalf("工作流快照超过预算: bytes=%d max=%d", len(raw), taskHistoryWorkflowSnapshotMaxBytes)
	}
	raw, err = json.Marshal(HistoryEvent{EventID: "workflow-event", Kind: "workflow", Workflow: status})
	if err != nil {
		t.Fatalf("序列化工作流事件失败: %v", err)
	}
	if len(raw) > taskHistoryEventMaxBytes {
		t.Fatalf("工作流事件超过硬上限: bytes=%d max=%d", len(raw), taskHistoryEventMaxBytes)
	}
}

// historySinkStub 记录测试中的持久化事件，并可注入数据库故障或租约竞争。
type historySinkStub struct {
	mu               sync.Mutex                    // mu 保护异步收集器写入的测试事件
	persistErr       error                         // persistErr 是持久化故障
	invalidEventID   string                        // invalidEventID 是需要模拟隔离的坏事件
	onPersist        func(context.Context)         // onPersist 在事务期间模拟并发动作
	events           []HistoryEvent                // events 保存已接收事件
	workflow         *types.TaskWorkflowStatusResp // workflow 是 DB 已落库快照
	workflowErr      error                         // workflowErr 是 DB 查询故障
	getWorkflowCalls int                           // getWorkflowCalls 记录终态落库确认次数
	failures         *types.TaskFailureListResp    // failures 是 DB 失败历史查询结果
	cleanupDeleted   int64                         // cleanupDeleted 模拟单批清理数量
	cleanupCalls     int                           // cleanupCalls 记录单轮清理追赶次数
}

// Persist 实现终态历史批量落库测试桩。
func (s *historySinkStub) Persist(ctx context.Context, events []HistoryEvent) error {
	for _, event := range events {
		if event.EventID == s.invalidEventID {
			return NewHistoryEventValidationError(event.EventID, errors.New("invalid history event"))
		}
	}
	s.mu.Lock()
	s.events = append(s.events, events...)
	s.mu.Unlock()
	if s.onPersist != nil {
		s.onPersist(ctx)
	}
	return s.persistErr
}

// eventSnapshot 返回异步持久化事件的并发安全副本。
func (s *historySinkStub) eventSnapshot() []HistoryEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]HistoryEvent(nil), s.events...)
}

// GetTaskRun 实现任务终态历史详情测试桩。
func (*historySinkStub) GetTaskRun(context.Context, uint64) (*types.TaskRunHistoryItem, error) {
	return nil, redis.Nil
}

// ListTaskRuns 实现全部任务终态历史列表测试桩。
func (*historySinkStub) ListTaskRuns(context.Context, *types.ListTaskRunsReq) (*types.TaskRunHistoryListResp, error) {
	return &types.TaskRunHistoryListResp{}, nil
}

// GetWorkflow 实现测试桩查询接口。
func (s *historySinkStub) GetWorkflow(context.Context, string) (*types.TaskWorkflowStatusResp, error) {
	s.getWorkflowCalls++
	if s.workflowErr != nil {
		return nil, s.workflowErr
	}
	if s.workflow == nil {
		return nil, redis.Nil
	}
	return s.workflow, nil
}

// ListWorkflows 实现测试桩查询接口。
func (*historySinkStub) ListWorkflows(context.Context, *types.ListTaskWorkflowsReq) (*types.TaskWorkflowHistoryListResp, error) {
	return &types.TaskWorkflowHistoryListResp{}, nil
}

// ListFailures 实现测试桩查询接口。
func (s *historySinkStub) ListFailures(context.Context, *types.ListTaskFailuresReq) (*types.TaskFailureListResp, error) {
	if s.failures != nil {
		return s.failures, nil
	}
	return &types.TaskFailureListResp{}, nil
}

// WindowSummary 实现测试桩聚合接口。
func (*historySinkStub) WindowSummary(context.Context, time.Time, time.Time) (types.TaskHistoryWindowSummary, error) {
	return types.TaskHistoryWindowSummary{}, nil
}

// Cleanup 实现测试桩清理接口。
func (s *historySinkStub) Cleanup(context.Context, time.Time, time.Time, time.Time, int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanupCalls++
	return s.cleanupDeleted, nil
}

// TestStandaloneTaskHistoryPersistsTerminalSummary 验证普通任务成功后只异步保存有界终态摘要。
func TestStandaloneTaskHistoryPersistsTerminalSummary(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	sink := &historySinkStub{}
	manager.AttachHistorySink(sink)
	if err := manager.RegisterHandler("history:standalone", asynq.HandlerFunc(func(ctx context.Context, _ *asynq.Task) error {
		taskstats.RecordRead(ctx, "rows", 3)
		return nil
	})); err != nil {
		t.Fatalf("注册普通任务历史测试处理器失败: %v", err)
	}
	if err := manager.StartWorker(); err != nil {
		t.Fatalf("启动普通任务历史测试 Worker 失败: %v", err)
	}
	resp, err := manager.EnqueueRegisteredTask(context.Background(), &types.EnqueueTaskReq{
		TaskType: "history:standalone",
		Payload:  json.RawMessage(`{"ignored":"payload"}`),
	})
	if err != nil {
		t.Fatalf("投递普通任务历史测试任务失败: %v", err)
	}
	waitForCondition(t, 8*time.Second, func() bool {
		manager.flushHistory(context.Background(), sink)
		for _, event := range sink.eventSnapshot() {
			if event.Kind == "task" && event.Task != nil && event.Task.TaskID == resp.TaskID {
				return true
			}
		}
		return false
	})
	for _, event := range sink.eventSnapshot() {
		if event.Kind != "task" || event.Task == nil || event.Task.TaskID != resp.TaskID {
			continue
		}
		if event.Task.Status != taskExecutionStatusSuccess || event.Task.ExecutionTrace == nil || event.Task.ExecutionTrace.ReadCount != 3 {
			t.Fatalf("普通任务终态摘要异常: %+v", event.Task)
		}
		raw, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			t.Fatalf("序列化普通任务终态事件失败: %v", marshalErr)
		}
		if strings.Contains(strings.ToLower(string(raw)), "ignored") {
			t.Fatalf("普通任务终态历史不得复制原始 payload: %s", raw)
		}
		return
	}
	t.Fatal("未找到普通任务终态历史事件")
}

// TestWorkflowTaskHistoryPersistsCompactTerminalSummary 验证工作流实际任务会落紧凑摘要且不复制大明细。
func TestWorkflowTaskHistoryPersistsCompactTerminalSummary(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	sink := &historySinkStub{}
	manager.AttachHistorySink(sink)
	const workflowName = "history.workflow"
	if err := manager.RegisterHandler(TypeWorkflowNoop, asynq.HandlerFunc(func(context.Context, *asynq.Task) error {
		return nil
	})); err != nil {
		t.Fatalf("注册工作流任务历史测试处理器失败: %v", err)
	}
	definition := testWorkflowDefinition(workflowName)
	definition.Nodes["root"].SupportsSharding = true
	if err := manager.RegisterWorkflow(definition); err != nil {
		t.Fatalf("注册工作流历史测试定义失败: %v", err)
	}
	workflowID, err := manager.startWorkflow(context.Background(), WorkflowStartSpec{
		WorkflowID: "workflow-1",
		Name:       workflowName,
		ShardTotal: 16,
		Source:     WorkflowSourceAPI,
	})
	if err != nil {
		t.Fatalf("启动工作流历史测试实例失败: %v", err)
	}
	if err = manager.StartWorker(); err != nil {
		t.Fatalf("启动工作流历史测试 Worker 失败: %v", err)
	}
	waitForCondition(t, 8*time.Second, func() bool {
		manager.flushHistory(context.Background(), sink)
		for _, event := range sink.eventSnapshot() {
			if event.Kind == "task" && event.Task != nil && event.Task.WorkflowID == workflowID && event.Task.WorkflowNode == "root" {
				return true
			}
		}
		return false
	})
	for _, event := range sink.eventSnapshot() {
		if event.Kind != "task" || event.Task == nil || event.Task.WorkflowID != workflowID || event.Task.WorkflowNode != "root" {
			continue
		}
		if event.Task.WorkflowName != workflowName || event.Task.ShardTotal != 16 || event.Task.Status != taskExecutionStatusSuccess || event.Task.ExecutionTrace != nil {
			t.Fatalf("工作流任务紧凑终态摘要异常: %+v", event.Task)
		}
		return
	}
	t.Fatal("未找到工作流实际任务终态历史事件")
}

// TestTaskHistoryRetentionDefaultsToOneDay 验证高频任务终态表默认只保留一天。
func TestTaskHistoryRetentionDefaultsToOneDay(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	if got := manager.historyTaskRetention(); got != 24*time.Hour {
		t.Fatalf("全部任务终态默认保留期=%s，期望=%s", got, 24*time.Hour)
	}
	cfg := manager.CurrentConfig()
	cfg.History.TaskRetentionDays = 3
	manager.UpdateConfig(cfg)
	if got := manager.historyTaskRetention(); got != 72*time.Hour {
		t.Fatalf("全部任务终态配置保留期=%s，期望=%s", got, 72*time.Hour)
	}
}

// TestTaskHistoryCleanupHasBoundedCatchUp 验证过期数据积压时按固定批数追赶而不是无界删除。
func TestTaskHistoryCleanupHasBoundedCatchUp(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	sink := &historySinkStub{cleanupDeleted: taskHistoryCleanupBatchSize}
	manager.cleanupHistory(context.Background(), sink)
	sink.mu.Lock()
	calls := sink.cleanupCalls
	sink.mu.Unlock()
	if calls != taskHistoryCleanupMaxBatches {
		t.Fatalf("任务历史清理批次数=%d，期望=%d", calls, taskHistoryCleanupMaxBatches)
	}
}

// TestHistoryPendingBufferDropsOldestAtHardLimit 验证待落库缓冲始终受硬上限约束。
func TestHistoryPendingBufferDropsOldestAtHardLimit(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	cfg := manager.CurrentConfig()
	cfg.History.PendingLimit = 2
	manager.UpdateConfig(cfg)
	manager.AttachHistorySink(&historySinkStub{})

	for _, eventID := range []string{"event-1", "event-2", "event-3"} {
		if err := manager.enqueueHistoryEvent(context.Background(), HistoryEvent{
			EventID: eventID,
			Kind:    "failure",
			Failure: &types.TaskFailureItem{TaskID: eventID},
		}); err != nil {
			t.Fatalf("写入历史缓冲失败: %v", err)
		}
		time.Sleep(time.Millisecond)
	}

	if total := manager.redis.ZCard(context.Background(), manager.historyOrderKey()).Val(); total != 2 {
		t.Fatalf("期望待落库事件硬限制为 2，实际=%d", total)
	}
	if manager.redis.HExists(context.Background(), manager.historyEventsKey(), "event-1").Val() {
		t.Fatal("超过硬上限时应淘汰最老事件")
	}
	if dropped := manager.redis.HGet(context.Background(), manager.historyStatusKey(), "dropped").Val(); dropped != "1" {
		t.Fatalf("期望记录 1 条丢弃事件，实际=%s", dropped)
	}
	pendingBytes, _ := manager.redis.HGet(context.Background(), manager.historyStatusKey(), "pendingBytes").Int64()
	if pendingBytes <= 0 {
		t.Fatalf("待落库载荷字节数应保持可观测，实际=%d", pendingBytes)
	}
	health, err := manager.historyPendingHealth(context.Background())
	if err != nil || health.PendingBytes != pendingBytes || health.PendingMaxBytes != taskHistoryPendingMaxBytes {
		t.Fatalf("历史缓冲容量观测异常: health=%+v err=%v", health, err)
	}
}

// TestHistoryPendingBufferDropsOldestAtByteLimit 验证 Lua 同时执行载荷总量硬限制。
func TestHistoryPendingBufferDropsOldestAtByteLimit(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	ctx := context.Background()
	keys := []string{manager.historyEventsKey(), manager.historyOrderKey(), manager.historyStatusKey()}
	for index, payload := range []string{strings.Repeat("a", 80), strings.Repeat("b", 80)} {
		if _, err := taskHistoryEnqueueScript.Run(ctx, manager.redis, keys,
			"event-"+strconv.Itoa(index), payload, time.Now().UnixMilli()+int64(index), 10, 100, 10,
		).Result(); err != nil {
			t.Fatalf("按字节限制写入历史缓冲失败: %v", err)
		}
	}
	if total := manager.redis.ZCard(ctx, manager.historyOrderKey()).Val(); total != 1 {
		t.Fatalf("字节超限后应只保留最新事件，实际=%d", total)
	}
	if pendingBytes := manager.redis.HGet(ctx, manager.historyStatusKey(), "pendingBytes").Val(); pendingBytes != "80" {
		t.Fatalf("字节超限裁剪后的载荷统计错误，实际=%s", pendingBytes)
	}
}

// TestHistoryFlushKeepsPendingEventsOnDatabaseFailure 验证数据库故障不会确认或丢失待落库事件。
func TestHistoryFlushKeepsPendingEventsOnDatabaseFailure(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	sink := &historySinkStub{persistErr: errors.New("database unavailable")}
	manager.AttachHistorySink(sink)
	if err := manager.enqueueHistoryEvent(context.Background(), HistoryEvent{
		EventID: "event-failed",
		Kind:    "failure",
		Failure: &types.TaskFailureItem{TaskID: "task-failed"},
	}); err != nil {
		t.Fatalf("写入历史缓冲失败: %v", err)
	}

	manager.flushHistory(context.Background(), sink)

	if total := manager.redis.ZCard(context.Background(), manager.historyOrderKey()).Val(); total != 1 {
		t.Fatalf("数据库故障后待落库事件应保留，实际=%d", total)
	}
	if lastError := manager.redis.HGet(context.Background(), manager.historyStatusKey(), "lastError").Val(); lastError == "" {
		t.Fatal("数据库故障应写入收集器健康状态")
	}
}

// TestHistoryFlushIsolatesInvalidEventAndContinues 验证单条确定性坏事件不会阻塞后续历史落库。
func TestHistoryFlushIsolatesInvalidEventAndContinues(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	sink := &historySinkStub{invalidEventID: "event-invalid"}
	manager.AttachHistorySink(sink)
	for _, eventID := range []string{"event-invalid", "event-valid"} {
		if err := manager.enqueueHistoryEvent(context.Background(), HistoryEvent{
			EventID: eventID,
			Kind:    "failure",
			Failure: &types.TaskFailureItem{TaskID: eventID},
		}); err != nil {
			t.Fatalf("写入历史缓冲失败: %v", err)
		}
		time.Sleep(time.Millisecond)
	}

	manager.flushHistory(context.Background(), sink)

	events := sink.eventSnapshot()
	if len(events) != 1 || events[0].EventID != "event-valid" {
		t.Fatalf("坏事件隔离后应继续持久化后续事件: %+v", events)
	}
	if pending := manager.redis.ZCard(context.Background(), manager.historyOrderKey()).Val(); pending != 0 {
		t.Fatalf("坏事件隔离后不应残留待落库事件: %d", pending)
	}
	if dropped := manager.redis.HGet(context.Background(), manager.historyStatusKey(), "dropped").Val(); dropped != "1" {
		t.Fatalf("坏事件隔离数量应可观测，实际=%s", dropped)
	}
	if lastError := manager.redis.HGet(context.Background(), manager.historyStatusKey(), "lastError").Val(); lastError != "" {
		t.Fatalf("已隔离坏事件不应把健康状态永久标记为异常，实际=%q", lastError)
	}
}

// TestHistoryFlushAcknowledgesBatchAndDoesNotDeleteNewOwnerLock 验证成功确认和锁所有者保护同时成立。
func TestHistoryFlushAcknowledgesBatchAndDoesNotDeleteNewOwnerLock(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	sink := &historySinkStub{}
	sink.onPersist = func(ctx context.Context) {
		if err := manager.redis.Set(ctx, manager.historyLockKey(), "new-owner", time.Minute).Err(); err != nil {
			t.Fatalf("模拟新实例获得租约失败: %v", err)
		}
	}
	manager.AttachHistorySink(sink)
	if err := manager.enqueueHistoryEvent(context.Background(), HistoryEvent{
		EventID: "event-success",
		Kind:    "failure",
		Failure: &types.TaskFailureItem{TaskID: "task-success"},
	}); err != nil {
		t.Fatalf("写入历史缓冲失败: %v", err)
	}

	manager.flushHistory(context.Background(), sink)

	if len(sink.events) != 1 {
		t.Fatalf("期望持久化 1 条事件，实际=%d", len(sink.events))
	}
	if total := manager.redis.ZCard(context.Background(), manager.historyOrderKey()).Val(); total != 0 {
		t.Fatalf("成功落库后应确认 Redis 事件，实际=%d", total)
	}
	if pendingBytes := manager.redis.HGet(context.Background(), manager.historyStatusKey(), "pendingBytes").Val(); pendingBytes != "0" {
		t.Fatalf("成功落库后载荷字节数应归零，实际=%s", pendingBytes)
	}
	if owner := manager.redis.Get(context.Background(), manager.historyLockKey()).Val(); owner != "new-owner" {
		t.Fatalf("旧实例不得删除新实例租约，实际=%q", owner)
	}
}

// TestHistoryFlushDrainsSeveralBoundedBatches 验证高频终态能在一次租约内有界追平多个批次。
func TestHistoryFlushDrainsSeveralBoundedBatches(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	sink := &historySinkStub{}
	manager.AttachHistorySink(sink)
	eventTotal := taskHistoryFlushBatchSize*2 + 5
	for index := range eventTotal {
		eventID := "event-batch-" + strconv.Itoa(index)
		if err := manager.enqueueHistoryEvent(context.Background(), HistoryEvent{
			EventID: eventID,
			Kind:    "failure",
			Failure: &types.TaskFailureItem{TaskID: eventID},
		}); err != nil {
			t.Fatalf("写入历史缓冲失败: %v", err)
		}
	}

	manager.flushHistory(context.Background(), sink)

	if len(sink.events) != eventTotal {
		t.Fatalf("高频终态未在单次租约内追平: persisted=%d want=%d", len(sink.events), eventTotal)
	}
	if pending := manager.redis.ZCard(context.Background(), manager.historyOrderKey()).Val(); pending != 0 {
		t.Fatalf("追平后仍有待落库事件: %d", pending)
	}
}

// TestGetWorkflowStatusPreservesRedisDetailAndReportsHistoryState 验证终态落库确认不会覆盖 Redis 分片详情。
func TestGetWorkflowStatusPreservesRedisDetailAndReportsHistoryState(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	ctx := context.Background()
	workflowID := "workflow-history-state"
	now := time.Now().Format(time.RFC3339Nano)
	if err := manager.redis.HSet(ctx, manager.workflowMetaKey(workflowID),
		"workflowId", workflowID,
		"workflowName", "history.state",
		"status", WorkflowStatusSuccess,
		"source", WorkflowSourcePeriodic,
		"queue", manager.namespacedQueueName(QueueMaintenance),
		"createdAt", now,
		"updatedAt", now,
		"finishedAt", now,
	).Err(); err != nil {
		t.Fatalf("写入工作流终态失败: %v", err)
	}

	sink := &historySinkStub{workflow: &types.TaskWorkflowStatusResp{
		Status: WorkflowStatusSuccess, FinishedAt: now, PersistedAt: "2026-08-05T12:00:00Z",
	}}
	manager.AttachHistorySink(sink)
	resp, err := manager.GetWorkflowStatus(ctx, workflowID)
	if err != nil {
		t.Fatalf("查询 Redis 终态失败: %v", err)
	}
	if resp.DataSource != "redis" || resp.DetailLevel != "shard" || resp.HistoryStatus != "persisted" || resp.PersistedAt != sink.workflow.PersistedAt {
		t.Fatalf("终态数据源或落库状态异常: %+v", resp)
	}

	sink.workflow = &types.TaskWorkflowStatusResp{
		Status: WorkflowStatusFailed, FinishedAt: now, PersistedAt: "2026-08-05T11:00:00Z",
	}
	resp, err = manager.GetWorkflowStatus(ctx, workflowID)
	if err != nil || resp.HistoryStatus != "pending" || resp.PersistedAt != "" {
		t.Fatalf("旧终态快照不应冒充当前终态已落库: resp=%+v err=%v", resp, err)
	}

	sink.workflow = nil
	sink.workflowErr = errors.New("database unavailable")
	resp, err = manager.GetWorkflowStatus(ctx, workflowID)
	if err != nil || resp.HistoryStatus != "failed" || resp.DataSource != "redis" {
		t.Fatalf("DB 故障时应保留 Redis 终态并标记落库异常: resp=%+v err=%v", resp, err)
	}

	if err = manager.redis.HSet(ctx, manager.workflowMetaKey(workflowID), "status", WorkflowStatusRunning).Err(); err != nil {
		t.Fatalf("更新工作流运行态失败: %v", err)
	}
	queryCount := sink.getWorkflowCalls
	resp, err = manager.GetWorkflowStatus(ctx, workflowID)
	if err != nil || resp.HistoryStatus != "" || sink.getWorkflowCalls != queryCount {
		t.Fatalf("运行态不应查询 DB 终态历史: resp=%+v calls=%d err=%v", resp, sink.getWorkflowCalls, err)
	}
}

// TestEnqueueWorkflowHistoryIgnoresCanceledBusinessContext 验证业务超时后仍能独立写入历史缓冲。
func TestEnqueueWorkflowHistoryIgnoresCanceledBusinessContext(t *testing.T) {
	manager, cleanup := newTestManager(t)
	defer cleanup()
	manager.AttachHistorySink(&historySinkStub{})
	workflowID := "workflow-canceled-context"
	now := time.Now().Format(time.RFC3339Nano)
	if err := manager.redis.HSet(context.Background(), manager.workflowMetaKey(workflowID),
		"workflowId", workflowID,
		"workflowName", "history.canceled",
		"status", WorkflowStatusSuccess,
		"source", WorkflowSourcePeriodic,
		"queue", manager.namespacedQueueName(QueueMaintenance),
		"createdAt", now,
		"updatedAt", now,
		"finishedAt", now,
	).Err(); err != nil {
		t.Fatalf("写入工作流终态失败: %v", err)
	}
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	manager.enqueueWorkflowHistory(canceledCtx, workflowID)
	if total := manager.redis.ZCard(context.Background(), manager.historyOrderKey()).Val(); total != 1 {
		t.Fatalf("取消业务上下文后历史缓冲事件数=%d，期望=1", total)
	}
}

// TestTerminalWorkflowReplayRepairsMissingHistoryEvent 验证同终态重放可补齐首次收尾中断后缺失的历史事件。
func TestTerminalWorkflowReplayRepairsMissingHistoryEvent(t *testing.T) {
	for _, testCase := range []struct {
		name   string                                        // name 标识工作流终态重放场景。
		status string                                        // status 是首次写入 Redis 的工作流终态。
		finish func(*Manager, context.Context, string) error // finish 再次执行相同终态收尾以补齐历史事件。
	}{
		{name: "success", status: WorkflowStatusSuccess, finish: func(manager *Manager, ctx context.Context, workflowID string) error {
			return manager.completeWorkflow(ctx, workflowID)
		}},
		{name: "failed", status: WorkflowStatusFailed, finish: func(manager *Manager, ctx context.Context, workflowID string) error {
			return manager.failWorkflow(ctx, workflowID, "same terminal replay")
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager, cleanup := newTestManager(t)
			defer cleanup()
			manager.AttachHistorySink(&historySinkStub{})
			ctx := context.Background()
			workflowID := "workflow-terminal-replay-" + testCase.name
			now := time.Now().Format(time.RFC3339Nano)
			if err := manager.redis.HSet(ctx, manager.workflowMetaKey(workflowID),
				"workflowId", workflowID,
				"workflowName", "history.terminal.replay",
				"status", testCase.status,
				"source", WorkflowSourcePeriodic,
				"queue", manager.namespacedQueueName(QueueMaintenance),
				"createdAt", now,
				"updatedAt", now,
				"finishedAt", now,
			).Err(); err != nil {
				t.Fatalf("写入已有工作流终态失败: %v", err)
			}

			if err := testCase.finish(manager, ctx, workflowID); err != nil {
				t.Fatalf("同终态重放失败: %v", err)
			}
			if total := manager.redis.ZCard(ctx, manager.historyOrderKey()).Val(); total != 1 {
				t.Fatalf("同终态重放应补齐 1 条历史事件，实际=%d", total)
			}
			if err := testCase.finish(manager, ctx, workflowID); err != nil {
				t.Fatalf("重复终态重放失败: %v", err)
			}
			if total := manager.redis.ZCard(ctx, manager.historyOrderKey()).Val(); total != 1 {
				t.Fatalf("历史事件应幂等去重，实际=%d", total)
			}
		})
	}
}

// TestListTaskFailuresKeepsDatabaseRowsWhenRedisCheckFails 验证 Redis 故障只禁用重跑，不吞掉 DB 失败历史。
func TestListTaskFailuresKeepsDatabaseRowsWhenRedisCheckFails(t *testing.T) {
	manager, server, cleanup := newTestManagerWithServer(t)
	defer cleanup()
	sink := &historySinkStub{failures: &types.TaskFailureListResp{
		Items: []types.TaskFailureItem{{TaskID: "archived-task", Queue: QueueMaintenance}},
	}}
	manager.AttachHistorySink(sink)
	server.Close()
	resp, err := manager.ListTaskFailures(context.Background(), &types.ListTaskFailuresReq{})
	if err != nil {
		t.Fatalf("Redis 校验故障不应阻断 DB 失败历史: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].Rerunnable || resp.RerunCheckError == "" {
		t.Fatalf("Redis 校验降级响应异常: %+v", resp)
	}
}
