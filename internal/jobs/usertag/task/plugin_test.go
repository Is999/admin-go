package task

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	keys "admin/common/rediskeys"
	"admin/internal/config"
	redislock "admin/internal/infra/redsync"
	"admin/internal/jobs/usertag/types"
	"admin/internal/svc"
	"admin/internal/task/queue"

	"github.com/alicebob/miniredis/v2"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// TestReleaseUserTagWorkflowLeaseOnFinalFailureReleasesFullOwner 验证 full 节点终态失败后会精确释放当前 workflow owner。
func TestReleaseUserTagWorkflowLeaseOnFinalFailureReleasesFullOwner(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	svcCtx := svc.NewServiceContext(config.Config{AppID: "215"}, svc.Dependencies{Rds: client})
	ctx := context.Background()
	leaseKey := keys.UserTagWorkflowLeaseRedisKey()
	if err := client.Set(ctx, leaseKey, "wf-full|full", time.Hour).Err(); err != nil {
		t.Fatalf("seed full lease failed: %v", err)
	}
	payload, err := json.Marshal(types.WorkflowPayload{WorkflowID: "wf-full", Mode: types.ModeFull})
	if err != nil {
		t.Fatalf("marshal payload failed: %v", err)
	}
	task := asynq.NewTask(TaskTypeUserTagEvaluateTags, payload)

	if err = releaseUserTagWorkflowLeaseOnFinalFailure(ctx, svcCtx, task, taskqueue.WorkflowTaskMeta{WorkflowName: WorkflowNameUserTagFull}); err != nil {
		t.Fatalf("release full lease on final failure failed: %v", err)
	}
	exists, err := client.Exists(ctx, leaseKey).Result()
	if err != nil {
		t.Fatalf("check lease exists failed: %v", err)
	}
	if exists != 0 {
		t.Fatalf("full lease should be released, exists=%d", exists)
	}
}

// TestReleaseUserTagWorkflowLeaseOnFinalFailureSkipsNonFull 验证 delta 终态失败不会释放正在运行的 full owner。
func TestReleaseUserTagWorkflowLeaseOnFinalFailureSkipsNonFull(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	svcCtx := svc.NewServiceContext(config.Config{AppID: "215"}, svc.Dependencies{Rds: client})
	ctx := context.Background()
	leaseKey := keys.UserTagWorkflowLeaseRedisKey()
	if err := client.Set(ctx, leaseKey, "wf-full|full", time.Hour).Err(); err != nil {
		t.Fatalf("seed full lease failed: %v", err)
	}
	payload, err := json.Marshal(types.WorkflowPayload{WorkflowID: "wf-delta", Mode: types.ModeDelta})
	if err != nil {
		t.Fatalf("marshal payload failed: %v", err)
	}
	task := asynq.NewTask(TaskTypeUserTagCollectScope, payload)

	if err = releaseUserTagWorkflowLeaseOnFinalFailure(ctx, svcCtx, task, taskqueue.WorkflowTaskMeta{WorkflowName: WorkflowNameUserTagDelta}); err != nil {
		t.Fatalf("non-full final failure cleanup should be no-op: %v", err)
	}
	current, err := client.Get(ctx, leaseKey).Result()
	if err != nil {
		t.Fatalf("read lease owner failed: %v", err)
	}
	if current != "wf-full|full" {
		t.Fatalf("delta cleanup should keep full lease, got=%s", current)
	}
}

// TestUserTagStageHandlerSpecsCoverMainSkeletonNodes 验证插件注册清单覆盖主 DAG 骨架节点。
func TestUserTagStageHandlerSpecsCoverMainSkeletonNodes(t *testing.T) {
	specs := userTagStageHandlerSpecs()
	got := make(map[string]string, len(specs))
	for _, spec := range specs {
		got[spec.Node] = spec.TaskType
	}
	want := map[string]string{
		types.NodePrepare:        TaskTypeUserTagPrepare,
		types.NodeCollectScope:   TaskTypeUserTagCollectScope,
		types.NodeEvaluateTags:   TaskTypeUserTagEvaluateTags,
		types.NodeResolveChanges: TaskTypeUserTagResolveChanges,
		types.NodePersistResults: TaskTypeUserTagPersistResults,
		types.NodeFinalize:       TaskTypeUserTagFinalize,
		types.NodeDispatchHooks:  TaskTypeUserTagDispatchHooks,
	}
	for node, taskType := range want {
		if got[node] != taskType {
			t.Fatalf("node=%s taskType=%s want=%s specs=%+v", node, got[node], taskType, specs)
		}
	}
}

// TestUserTagEventOutboxRetryScanOptionsScansAllShards 验证异常扫描任务不会只锁定 0 号 outbox 分片。
func TestUserTagEventOutboxRetryScanOptionsScansAllShards(t *testing.T) {
	opts, err := userTagEventOutboxRetryScanOptions(Defaults{ShardTotal: 10, BatchSize: 100, EventBatchSize: 50, WorkerCount: 2, EventHookEnabled: true})
	if err != nil {
		t.Fatalf("userTagEventOutboxRetryScanOptions() error = %v", err)
	}
	if opts.ShardTotal != 1 {
		t.Fatalf("retry scan should run as single unsharded worker, ShardTotal=%d", opts.ShardTotal)
	}
	if !opts.EventHookEnabled || opts.BatchSize != 50 {
		t.Fatalf("unexpected retry scan options: %+v", opts)
	}
}

// TestPeriodicMaintenanceTasksSkipHeldLock 校验重复周期触发只抢锁一次并成功跳过，不进入任务 retry/dead。
func TestPeriodicMaintenanceTasksSkipHeldLock(t *testing.T) {
	// cases 覆盖运行期清理和异常 outbox 扫描两个独立入口，锁竞争时都不应访问数据库。
	cases := []struct {
		name   string                                           // 周期任务场景
		key    string                                           // 当前任务的低基数互斥 Key
		config config.Config                                    // 决定任务是否进入锁竞争的运行配置
		run    func(context.Context, *svc.ServiceContext) error // 被测周期任务入口
	}{
		{
			name:   "runtime cleanup",
			key:    keys.UserTagRuntimeCleanupRedisKey(),
			config: config.Config{AppID: "215"},
			run:    runUserTagRuntimeCleanupTask,
		},
		{
			name: "event outbox retry scan",
			key:  keys.UserTagEventOutboxRetryScanRedisKey(),
			config: config.Config{
				AppID: "215",
				Workflows: config.WorkflowsConfig{UserTag: config.UserTagConfig{
					EventHookEnabled: true,
				}},
			},
			run: runUserTagEventOutboxRetryScanTask,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { _ = client.Close() })
			service := svc.NewServiceContext(tt.config, svc.Dependencies{Rds: client})

			holder := redislock.NewLock(client, tt.key)
			if err := holder.TryLock(context.Background(), time.Minute); err != nil {
				t.Fatalf("holder.TryLock() error = %v", err)
			}
			t.Cleanup(func() { _ = holder.Unlock() })

			startedAt := time.Now()
			if err := tt.run(context.Background(), service); err != nil {
				t.Fatalf("periodic task lock contention error = %v, want successful skip", err)
			}
			if elapsed := time.Since(startedAt); elapsed >= 500*time.Millisecond {
				t.Fatalf("periodic task lock contention elapsed = %v, want no retry backoff", elapsed)
			}
		})
	}
}
