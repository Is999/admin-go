package configload

import (
	"strings"
	"testing"

	"admin/internal/config"
	tasklimits "admin/internal/task/limits"
)

// TestValidateTaskLimitsRejectsUnsafeHistoryAndRetentionBounds 验证缓存、数据库和调度背压边界不能被配置放大。
func TestValidateTaskLimitsRejectsUnsafeHistoryAndRetentionBounds(t *testing.T) {
	tests := []config.TaskQueueConfig{
		{CompletedRetentionSeconds: 599},
		{CompletedRetentionSeconds: 7201},
		{ArchivedRetentionSeconds: 86399},
		{ArchivedRetentionSeconds: 30*86400 + 1},
		{History: config.TaskQueueHistoryConfig{WorkflowRetentionDays: 31}},
		{History: config.TaskQueueHistoryConfig{TaskRetentionDays: maxTaskHistoryRetentionDays + 1}},
		{History: config.TaskQueueHistoryConfig{FailureRetentionDays: maxTaskHistoryRetentionDays + 1}},
		{History: config.TaskQueueHistoryConfig{PendingLimit: 10001}},
		{History: config.TaskQueueHistoryConfig{FlushIntervalSeconds: maxTaskHistoryFlushSeconds + 1}},
		{History: config.TaskQueueHistoryConfig{CleanupIntervalSeconds: 29}},
		{History: config.TaskQueueHistoryConfig{CleanupIntervalSeconds: maxTaskHistoryCleanupSeconds + 1}},
		{Scheduler: config.TaskQueueSchedulerConfig{MaxQueueBacklog: maxTaskQueueBacklog + 1}},
		{Periodic: []config.TaskPeriodicConfig{{Targets: []string{strings.Repeat("x", tasklimits.MaxWorkflowTargetBytes+1)}}}},
		{Periodic: []config.TaskPeriodicConfig{{UniqueKey: strings.Repeat("x", tasklimits.MaxUniqueKeyBytes+1)}}},
	}
	for index, taskCfg := range tests {
		if err := validateTaskLimits(taskCfg); err == nil {
			t.Fatalf("场景 %d 期望不安全任务历史配置返回错误", index)
		}
	}
}

// TestValidateTaskLimitsAcceptsSafeHistoryDefaultsAndEdges 验证缺省值和允许的生产边界可以启动。
func TestValidateTaskLimitsAcceptsSafeHistoryDefaultsAndEdges(t *testing.T) {
	tests := []config.TaskQueueConfig{
		{},
		{
			CompletedRetentionSeconds: 600,
			ArchivedRetentionSeconds:  86400,
			History: config.TaskQueueHistoryConfig{
				TaskRetentionDays: maxTaskHistoryRetentionDays, WorkflowRetentionDays: maxTaskHistoryRetentionDays, FailureRetentionDays: maxTaskHistoryRetentionDays, PendingLimit: 10000,
				FlushIntervalSeconds: maxTaskHistoryFlushSeconds, CleanupIntervalSeconds: maxTaskHistoryCleanupSeconds,
			},
			Scheduler: config.TaskQueueSchedulerConfig{MaxQueueBacklog: maxTaskQueueBacklog},
		},
	}
	for index, taskCfg := range tests {
		if err := validateTaskLimits(taskCfg); err != nil {
			t.Fatalf("场景 %d 期望安全任务历史配置通过，实际错误=%v", index, err)
		}
	}
}

// TestValidateRuntimeListCounts 校验文件入口与数据库发布入口使用相同的一万条任务总量边界。
func TestValidateRuntimeListCounts(t *testing.T) {
	if err := validateTaskLimits(config.TaskQueueConfig{Periodic: make([]config.TaskPeriodicConfig, tasklimits.MaxPeriodicCount+1)}); err == nil {
		t.Fatal("超过一万条的周期任务文件配置应被拒绝")
	}
	if err := validateArchiveLimits(config.ArchiveConfig{Jobs: make([]config.ArchiveJobConfig, tasklimits.MaxArchiveJobCount+1)}); err == nil {
		t.Fatal("超过一万条的归档任务文件配置应被拒绝")
	}
	if err := validateArchiveLimits(config.ArchiveConfig{Jobs: make([]config.ArchiveJobConfig, tasklimits.MaxArchiveJobCount)}); err != nil {
		t.Fatalf("一万条归档任务文件配置应处于允许边界: %v", err)
	}
}

// TestTaskSamplesRespectRuntimeResourceLimits 确保两份交付样例不会被任务生产边界拒绝。
func TestTaskSamplesRespectRuntimeResourceLimits(t *testing.T) {
	for _, path := range []string{"../../../etc/config.sample.yaml", "../../../etc/config.dnmp.sample.yaml"} {
		cfg, err := loadBaseConfig(path)
		if err != nil {
			t.Fatalf("读取样例配置 %s 失败: %v", path, err)
		}
		if err = validateTaskLimits(cfg.Task); err != nil {
			t.Fatalf("样例配置 %s 超出任务生产边界: %v", path, err)
		}
	}
}

// TestValidateTaskLimitsRejectsUnconsumedConfiguredQueues 确保默认队列和周期队列不会进入无人消费状态。
func TestValidateTaskLimitsRejectsUnconsumedConfiguredQueues(t *testing.T) {
	tests := []config.TaskQueueConfig{
		{DefaultQueue: "default", Queues: map[string]int{"critical": 1}},
		{
			DefaultQueue: "default",
			Queues:       map[string]int{"default": 1},
			Periodic:     []config.TaskPeriodicConfig{{Queue: "maintenance"}},
		},
	}
	for index, taskCfg := range tests {
		if err := validateTaskLimits(taskCfg); err == nil {
			t.Fatalf("场景 %d 期望未消费队列配置返回错误", index)
		}
	}
}
