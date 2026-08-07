package keys_test

import (
	"testing"
	"time"

	keys "admin/common/rediskeys"
)

// TestTaskQueueName 验证队列名统一追加 app_id，完整跨站点队列名会失败闭合。
func TestTaskQueueName(t *testing.T) {
	tests := []struct {
		name  string // name 表示测试场景名称。
		appID string // appID 表示测试应用 ID。
		queue string // queue 表示任务队列名。
		want  string // want 表示期望结果。
	}{
		{
			name:  "逻辑队列追加当前app前缀",
			appID: "215",
			queue: "maintenance",
			want:  "app:215:maintenance",
		},
		{
			name:  "当前app队列保持不变",
			appID: "215",
			queue: "app:215:maintenance",
			want:  "app:215:maintenance",
		},
		{
			name:  "其它app完整队列失败闭合",
			appID: "102",
			queue: "app:215:maintenance",
			want:  "",
		},
		{
			name:  "不完整app前缀按逻辑队列处理",
			appID: "102",
			queue: "app:215",
			want:  "app:102:app:215",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useAppID(t, tt.appID)
			if got := keys.TaskQueueName(tt.queue); got != tt.want {
				t.Fatalf("keys.TaskQueueName() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestTaskQueueRedisKey 验证任务系统自管 key 统一收敛到 task 二级前缀。
func TestTaskQueueRedisKey(t *testing.T) {
	tests := []struct {
		name  string // name 表示测试场景名称。
		appID string // appID 表示测试应用 ID。
		key   string // key 表示待验证 key。
		want  string // want 表示期望结果。
	}{
		{
			name:  "逻辑key追加task前缀",
			appID: "215",
			key:   "workflow:wf-1:meta",
			want:  "app:215:task:workflow:wf-1:meta",
		},
		{
			name:  "已有task前缀不重复追加",
			appID: "215",
			key:   "task:scheduler:leader",
			want:  "app:215:task:scheduler:leader",
		},
		{
			name:  "task根段只保留一次",
			appID: "215",
			key:   "task",
			want:  "app:215:task",
		},
		{
			name:  "当前app完整key保持不变",
			appID: "215",
			key:   "app:215:task:scheduler:leader",
			want:  "app:215:task:scheduler:leader",
		},
		{
			name:  "其它app完整key失败闭合",
			appID: "102",
			key:   "app:215:task:scheduler:leader",
			want:  "",
		},
		{
			name:  "不完整app前缀按逻辑key处理",
			appID: "102",
			key:   "app:215",
			want:  "app:102:task:app:215",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useAppID(t, tt.appID)
			if got := keys.TaskQueueRedisKey(tt.key); got != tt.want {
				t.Fatalf("keys.TaskQueueRedisKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestTaskRuntimeAndWorkflowKeys 验证任务运行快照和工作流 key 的完整规则。
func TestTaskRuntimeAndWorkflowKeys(t *testing.T) {
	useAppID(t, "215")
	if got := keys.TaskRuntimeKey("maintenance", "task-1"); got != "app:215:task:runtime:11:maintenance:6:task-1" {
		t.Fatalf("keys.TaskRuntimeKey() = %q", got)
	}
	if got := keys.TaskRuntimeKey(" maintenance ", " task-1 "); got != "app:215:task:runtime:11:maintenance:6:task-1" {
		t.Fatalf("keys.TaskRuntimeKey(trim) = %q", got)
	}
	if got := keys.TaskRuntimeKey("", "task-1"); got != "" {
		t.Fatalf("空队列不应生成运行快照 key: %q", got)
	}
	if got := keys.TaskRuntimeKey("maintenance", ""); got != "" {
		t.Fatalf("空任务 ID 不应生成运行快照 key: %q", got)
	}
	if keys.TaskRuntimeKey("a:b", "c") == keys.TaskRuntimeKey("a", "b:c") {
		t.Fatal("含冒号的队列名和任务 ID 不应生成相同运行快照 key")
	}
	if got := keys.TaskHistoryPendingEventsKey(); got != "app:215:task:history:{app:215}:events" {
		t.Fatalf("keys.TaskHistoryPendingEventsKey() = %q", got)
	}
	if got := keys.TaskHistoryPendingOrderKey(); got != "app:215:task:history:{app:215}:order" {
		t.Fatalf("keys.TaskHistoryPendingOrderKey() = %q", got)
	}
	if got := keys.TaskHistoryStatusKey(); got != "app:215:task:history:{app:215}:status" {
		t.Fatalf("keys.TaskHistoryStatusKey() = %q", got)
	}
	if got := keys.TaskHistoryCollectorLockKey(); got != "app:215:task:history:{app:215}:lock" {
		t.Fatalf("keys.TaskHistoryCollectorLockKey() = %q", got)
	}
	if got := keys.TaskWorkflowPrefix(); got != "app:215:task:workflow" {
		t.Fatalf("keys.TaskWorkflowPrefix() = %q", got)
	}
	if got := keys.TaskWorkflowMetaKey("wf-1"); got != "app:215:task:workflow:wf-1:meta" {
		t.Fatalf("keys.TaskWorkflowMetaKey() = %q", got)
	}
	if got := keys.TaskWorkflowNodesKey("wf-1"); got != "app:215:task:workflow:wf-1:nodes" {
		t.Fatalf("keys.TaskWorkflowNodesKey() = %q", got)
	}
	if got := keys.TaskWorkflowNodeKey("wf-1", "root"); got != "app:215:task:workflow:wf-1:node:root" {
		t.Fatalf("keys.TaskWorkflowNodeKey() = %q", got)
	}
	if got := keys.TaskWorkflowUniqueKey("user_tag.delta.refresh", "daily"); got != "app:215:task:workflow:unique:user_tag.delta.refresh:daily" {
		t.Fatalf("keys.TaskWorkflowUniqueKey() = %q", got)
	}
}

// TestTaskSchedulerLeaderRedisKey 验证调度 leader 锁 key 的默认值和归一规则。
func TestTaskSchedulerLeaderRedisKey(t *testing.T) {
	tests := []struct {
		name     string // name 表示测试场景名称。
		appID    string // appID 表示测试应用 ID。
		leaseKey string // leaseKey 表示租约 key。
		want     string // want 表示期望结果。
	}{
		{name: "空配置使用默认key", appID: "215", want: "app:215:task:scheduler:leader"},
		{name: "自定义逻辑key归入task前缀", appID: "215", leaseKey: "scheduler:new", want: "app:215:task:scheduler:new"},
		{name: "已有task根段不重复追加", appID: "215", leaseKey: "task:scheduler:leader", want: "app:215:task:scheduler:leader"},
		{name: "其它app完整key失败闭合", appID: "215", leaseKey: "app:102:task:scheduler:leader", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			useAppID(t, tt.appID)
			if got := keys.TaskSchedulerLeaderRedisKey(tt.leaseKey); got != tt.want {
				t.Fatalf("keys.TaskSchedulerLeaderRedisKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestTaskAsynqKeys 验证 Asynq 框架 key 保持官方存储形态。
func TestTaskAsynqKeys(t *testing.T) {
	useAppID(t, "215")
	queue := keys.TaskQueueName("maintenance")
	if got := keys.TaskAsynqPendingKey(queue); got != "asynq:{app:215:maintenance}:pending" {
		t.Fatalf("keys.TaskAsynqPendingKey() = %q", got)
	}
	if got := keys.TaskAsynqActiveKey(queue); got != "asynq:{app:215:maintenance}:active" {
		t.Fatalf("keys.TaskAsynqActiveKey() = %q", got)
	}
	if got := keys.TaskAsynqPendingKey(""); got != "asynq:{}:pending" {
		t.Fatalf("keys.TaskAsynqPendingKey(empty) = %q", got)
	}
	if got := keys.TaskAsynqActiveKey(""); got != "asynq:{}:active" {
		t.Fatalf("keys.TaskAsynqActiveKey(empty) = %q", got)
	}
	if got := keys.TaskAsynqScheduledKey(queue); got != "asynq:{app:215:maintenance}:scheduled" {
		t.Fatalf("keys.TaskAsynqScheduledKey() = %q", got)
	}
	if got := keys.TaskAsynqPausedKey(queue); got != "asynq:{app:215:maintenance}:paused" {
		t.Fatalf("keys.TaskAsynqPausedKey() = %q", got)
	}
	at := time.Date(2026, time.August, 5, 8, 0, 0, 0, time.FixedZone("UTC+8", 8*60*60))
	if got := keys.TaskAsynqDailyProcessedKey(queue, at); got != "asynq:{app:215:maintenance}:processed:2026-08-05" {
		t.Fatalf("keys.TaskAsynqDailyProcessedKey() = %q", got)
	}
	if got := keys.TaskAsynqDailyFailedKey(queue, at); got != "asynq:{app:215:maintenance}:failed:2026-08-05" {
		t.Fatalf("keys.TaskAsynqDailyFailedKey() = %q", got)
	}
	if got := keys.TaskAsynqTaskHashKeyPrefix(queue); got != "asynq:{app:215:maintenance}:t:" {
		t.Fatalf("keys.TaskAsynqTaskHashKeyPrefix() = %q", got)
	}
	if got := keys.TaskAsynqTaskHashKey(queue, "task-1"); got != "asynq:{app:215:maintenance}:t:task-1" {
		t.Fatalf("keys.TaskAsynqTaskHashKey() = %q", got)
	}
	if got := keys.TaskAsynqUniqueKey(queue, "email:send", []byte(`{"uid":1}`)); got != "asynq:{app:215:maintenance}:unique:email:send:0b43657867acc68460ccc015a031a4a8" {
		t.Fatalf("keys.TaskAsynqUniqueKey() = %q", got)
	}
	if got := keys.TaskAsynqUniqueKey(queue, "reindex", nil); got != "asynq:{app:215:maintenance}:unique:reindex:" {
		t.Fatalf("keys.TaskAsynqUniqueKey(nil) = %q", got)
	}
	if got, err := keys.TaskAsynqStateZSetKey(queue, "archived"); err != nil || got != "asynq:{app:215:maintenance}:archived" {
		t.Fatalf("keys.TaskAsynqStateZSetKey() = %q err=%v", got, err)
	}
	if got, err := keys.TaskAsynqStateZSetKey(queue, " retry "); err != nil || got != "asynq:{app:215:maintenance}:retry" {
		t.Fatalf("keys.TaskAsynqStateZSetKey(trim) = %q err=%v", got, err)
	}
	if _, err := keys.TaskAsynqStateZSetKey(queue, "pending"); err == nil {
		t.Fatal("期望不支持的 Asynq zset 状态返回错误")
	}
}
