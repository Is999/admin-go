package taskqueue

import (
	"fmt"
	"strings"
	"time"

	"github.com/Is999/go-utils/errors"

	keys "admin/common/rediskeys"
	"admin/helper"
	"admin/internal/config"
	"admin/internal/types"
)

const (
	// taskCompletedRetentionDefaultSeconds 是成功任务近期明细的默认保留时间。
	taskCompletedRetentionDefaultSeconds = 30 * 60
	// taskCompletedRetention 是默认保留时间，保留该常量供边界测试复用。
	taskCompletedRetention = time.Duration(taskCompletedRetentionDefaultSeconds) * time.Second
	// taskArchivedRetentionDefaultSeconds 是失败归档任务未配置时的七天保留期。
	taskArchivedRetentionDefaultSeconds = 7 * 24 * 60 * 60
)

// workflowManualRerunRetention 返回手工重跑运行态的安全保留时长，保证中断后状态最终可回收。
func (m *Manager) workflowManualRerunRetention() time.Duration {
	return max(m.CompletedRetention(), m.ArchivedRetention())
}

// workflowTerminalRetention 返回工作流终态热状态保留时间。
// 失败实例需要和 archived 对齐以支持人工重跑，成功实例只保留近期观测窗口。
func (m *Manager) workflowTerminalRetention(status string) time.Duration {
	if strings.TrimSpace(status) == WorkflowStatusFailed {
		return m.ArchivedRetention()
	}
	return m.CompletedRetention()
}

// CompletedRetention 返回成功任务交给 Asynq completed 队列保留的时长。
func (m *Manager) CompletedRetention() time.Duration {
	seconds := m.CurrentConfig().CompletedRetentionSeconds
	if seconds <= 0 {
		seconds = taskCompletedRetentionDefaultSeconds
	}
	return time.Duration(seconds) * time.Second
}

// archivedRetention 返回归档失败任务在 Asynq 中的保留时长。
func (m *Manager) archivedRetention() time.Duration {
	seconds := m.CurrentConfig().ArchivedRetentionSeconds
	if seconds <= 0 {
		seconds = taskArchivedRetentionDefaultSeconds
	}
	return time.Duration(seconds) * time.Second
}

// ArchivedRetention 返回归档失败任务在 Asynq 中的保留时长。
func (m *Manager) ArchivedRetention() time.Duration {
	return m.archivedRetention()
}

// defaultWorkflowQueue 返回工作流默认执行队列。
func (m *Manager) defaultWorkflowQueue() string {
	return helper.FirstNonEmptyString(m.CurrentConfig().DefaultQueue, QueueDefault)
}

// queueWeights 返回 Worker 队列权重配置；未配置时使用默认权重。
func (m *Manager) queueWeights() map[string]int {
	cfg := m.CurrentConfig()
	result := make(map[string]int)
	if len(cfg.Queues) > 0 {
		for queueName, weight := range cfg.Queues {
			result[m.namespacedQueueName(queueName)] = weight
		}
		return result
	}
	result[m.namespacedQueueName(QueueCritical)] = 6
	result[m.namespacedQueueName(QueueDefault)] = 3
	result[m.namespacedQueueName(QueueMaintenance)] = 1
	return result
}

// isQueueConfigured 判断逻辑队列是否由当前 Worker 配置消费，避免任务进入无人监听的 Redis 队列。
func (m *Manager) isQueueConfigured(queue string) bool {
	internalQueue := m.namespacedQueueName(helper.FirstNonEmptyString(queue, m.defaultWorkflowQueue()))
	if internalQueue == "" {
		return false
	}
	_, ok := m.queueWeights()[internalQueue]
	return ok
}

// concurrency 返回 Worker 并发度。
func (m *Manager) concurrency() int {
	if m.CurrentConfig().Concurrency > 0 {
		return m.CurrentConfig().Concurrency
	}
	return 10
}

// defaultRetry 返回任务最终使用的重试次数。
func (m *Manager) defaultRetry(override *int) int {
	if override != nil {
		return *override
	}
	if m.CurrentConfig().DefaultRetry > 0 {
		return m.CurrentConfig().DefaultRetry
	}
	return 3
}

// defaultTimeout 返回任务最终使用的超时时间。
func (m *Manager) defaultTimeout(override *int) time.Duration {
	if override != nil && *override > 0 {
		return time.Duration(*override) * time.Second
	}
	if m.CurrentConfig().DefaultTimeoutSeconds > 0 {
		return time.Duration(m.CurrentConfig().DefaultTimeoutSeconds) * time.Second
	}
	return 2 * time.Minute
}

// uniqueTTL 返回任务唯一键的去重窗口时长。
func (m *Manager) uniqueTTL(override *int) time.Duration {
	if override != nil && *override > 0 {
		return time.Duration(*override) * time.Second
	}
	if m.CurrentConfig().DefaultUniqueTTLSeconds > 0 {
		return time.Duration(m.CurrentConfig().DefaultUniqueTTLSeconds) * time.Second
	}
	return 0
}

// shutdownTimeout 返回 Worker 关闭时的优雅退出等待时长。
func (m *Manager) shutdownTimeout() time.Duration {
	if m.CurrentConfig().ShutdownTimeoutSeconds > 0 {
		return time.Duration(m.CurrentConfig().ShutdownTimeoutSeconds) * time.Second
	}
	return 15 * time.Second
}

// groupGracePeriod 返回聚合分组收尾等待时间。
func (m *Manager) groupGracePeriod() time.Duration {
	if m.CurrentConfig().GroupGracePeriodSeconds > 0 {
		return time.Duration(m.CurrentConfig().GroupGracePeriodSeconds) * time.Second
	}
	return 5 * time.Second
}

// groupMaxDelay 返回聚合任务允许等待的最长窗口。
func (m *Manager) groupMaxDelay() time.Duration {
	if m.CurrentConfig().GroupMaxDelaySeconds > 0 {
		return time.Duration(m.CurrentConfig().GroupMaxDelaySeconds) * time.Second
	}
	return 20 * time.Second
}

// groupMaxSize 返回单个聚合任务可容纳的最大条数。
func (m *Manager) groupMaxSize() int {
	if m.CurrentConfig().GroupMaxSize > 0 {
		return m.CurrentConfig().GroupMaxSize
	}
	return 100
}

// delayedTaskCheckInterval 返回 Asynq 检查延迟任务的轮询间隔。
func (m *Manager) delayedTaskCheckInterval() time.Duration {
	if m.CurrentConfig().DelayedTaskCheckSeconds > 0 {
		return time.Duration(m.CurrentConfig().DelayedTaskCheckSeconds) * time.Second
	}
	return 5 * time.Second
}

// taskCheckInterval 返回 Worker 普通任务轮询间隔。
func (m *Manager) taskCheckInterval() time.Duration {
	if m.CurrentConfig().TaskCheckSeconds > 0 {
		return time.Duration(m.CurrentConfig().TaskCheckSeconds) * time.Second
	}
	return time.Second
}

// schedulerEnabled 判断是否需要启动周期调度模块。
func (m *Manager) schedulerEnabled() bool {
	return m.CurrentConfig().Scheduler.Enabled
}

// allPeriodicTasks 合并配置文件和插件注册的周期任务，并去重输出。
func (m *Manager) allPeriodicTasks() []config.TaskPeriodicConfig {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	cfg := m.CurrentConfig()
	items := make([]config.TaskPeriodicConfig, 0, len(cfg.Periodic)+len(m.periodic))
	items = append(items, cfg.Periodic...)
	items = append(items, m.periodic...)
	return dedupePeriodicTasks(enabledPeriodicTasks(items))
}

// periodicTaskCount 返回当前有效周期任务数量。
func (m *Manager) periodicTaskCount() int {
	if m == nil {
		return 0
	}
	return len(m.allPeriodicTasks())
}

// schedulerLeaseKey 返回调度 leader 竞争锁的 Redis key。
func (m *Manager) schedulerLeaseKey() string {
	return m.schedulerLeaseKeyByConfig(m.CurrentConfig())
}

// schedulerLeaseKeyByConfig 根据给定配置计算调度 leader 锁的 Redis key。
func (m *Manager) schedulerLeaseKeyByConfig(cfg config.TaskQueueConfig) string {
	return keys.TaskSchedulerLeaderRedisKey(cfg.Scheduler.LeaseKey)
}

// schedulerLeaseTTL 返回调度 leader 锁租约时长。
func (m *Manager) schedulerLeaseTTL() time.Duration {
	return m.schedulerLeaseTTLByConfig(m.CurrentConfig())
}

// schedulerLeaseTTLByConfig 根据给定配置返回调度 leader 锁租约时长。
func (m *Manager) schedulerLeaseTTLByConfig(cfg config.TaskQueueConfig) time.Duration {
	if cfg.Scheduler.LeaseTTLSeconds > 0 {
		return time.Duration(cfg.Scheduler.LeaseTTLSeconds) * time.Second
	}
	return 30 * time.Second
}

// schedulerRenewInterval 返回调度 leader 锁续租间隔。
func (m *Manager) schedulerRenewInterval() time.Duration {
	return m.schedulerRenewIntervalByConfig(m.CurrentConfig())
}

// schedulerRenewIntervalByConfig 根据给定配置返回调度 leader 锁续租间隔。
func (m *Manager) schedulerRenewIntervalByConfig(cfg config.TaskQueueConfig) time.Duration {
	if cfg.Scheduler.RenewIntervalSeconds > 0 {
		return time.Duration(cfg.Scheduler.RenewIntervalSeconds) * time.Second
	}
	return 10 * time.Second
}

// schedulerSyncInterval 返回周期任务配置同步间隔。
func (m *Manager) schedulerSyncInterval() time.Duration {
	return m.schedulerSyncIntervalByConfig(m.CurrentConfig())
}

// schedulerSyncIntervalByConfig 根据给定配置返回周期任务配置同步间隔。
func (m *Manager) schedulerSyncIntervalByConfig(cfg config.TaskQueueConfig) time.Duration {
	if cfg.Scheduler.SyncIntervalSeconds > 0 {
		return time.Duration(cfg.Scheduler.SyncIntervalSeconds) * time.Second
	}
	return time.Minute
}

// schedulerHeartbeatInterval 返回调度器心跳上报间隔。
func (m *Manager) schedulerHeartbeatInterval() time.Duration {
	return m.schedulerHeartbeatIntervalByConfig(m.CurrentConfig())
}

// schedulerHeartbeatIntervalByConfig 根据给定配置返回调度器心跳上报间隔。
func (m *Manager) schedulerHeartbeatIntervalByConfig(cfg config.TaskQueueConfig) time.Duration {
	if cfg.Scheduler.HeartbeatIntervalSeconds > 0 {
		return time.Duration(cfg.Scheduler.HeartbeatIntervalSeconds) * time.Second
	}
	return 10 * time.Second
}

// schedulerMaxQueueBacklog 返回周期任务投递前允许的队列积压上限。
func (m *Manager) schedulerMaxQueueBacklog() int64 {
	if m == nil {
		return 0
	}
	limit := m.CurrentConfig().Scheduler.MaxQueueBacklog
	if limit <= 0 {
		return 10_000
	}
	return int64(limit)
}

// workflowTriggerUniqueReservationTTL 计算触发请求在排队延迟场景下需要额外预留的幂等窗口。
func (m *Manager) workflowTriggerUniqueReservationTTL(req *types.TriggerTaskWorkflowReq, uniqueTTL time.Duration, now time.Time) (time.Duration, error) {
	if req == nil || uniqueTTL <= 0 {
		return uniqueTTL, nil
	}
	delay := time.Duration(0)
	if req.ProcessAt != "" {
		// processAt 使用 RFC3339 绝对时间，解析失败要尽早返回给 API 调用方，而不是默默降级成即时任务。
		processAt, err := time.Parse(time.RFC3339, req.ProcessAt)
		if err != nil {
			return 0, errors.Wrap(err, "解析 workflow processAt 失败")
		}
		if processAt.After(now) {
			delay = processAt.Sub(now)
		}
	}
	if delay <= 0 && req.ProcessInSeconds != nil && *req.ProcessInSeconds > 0 {
		delay = time.Duration(*req.ProcessInSeconds) * time.Second
	}
	return uniqueTTL + delay, nil
}

// periodicTaskKey 生成周期任务的稳定去重键。
func periodicTaskKey(cfg config.TaskPeriodicConfig) (string, error) {
	cron, err := periodicTaskCronspec(cfg)
	if err != nil {
		return "", errors.Tag(err)
	}
	if err = validatePeriodicCronspec(cron); err != nil {
		return "", errors.Tag(err)
	}
	workflow := strings.TrimSpace(cfg.Workflow)
	if workflow == "" {
		return "", errors.Errorf("周期任务 workflow 不能为空")
	}
	if name := strings.TrimSpace(cfg.Name); name != "" {
		return "name:" + name, nil
	}
	return fmt.Sprintf("cron:%s|workflow:%s|queue:%s", cron, workflow, strings.TrimSpace(cfg.Queue)), nil
}

// PeriodicTaskConfigKey 返回周期任务的稳定去重键，供 bootstrap 配置合并阶段复用。
// 该键与调度器注册阶段保持一致，避免外部配置文件加载成功但运行期才暴露重复任务。
func PeriodicTaskConfigKey(cfg config.TaskPeriodicConfig) (string, error) {
	return periodicTaskKey(cfg)
}

// enabledPeriodicTasks 保留默认启用或显式启用的周期任务，显式 enabled=false 才关闭。
func enabledPeriodicTasks(items []config.TaskPeriodicConfig) []config.TaskPeriodicConfig {
	result := make([]config.TaskPeriodicConfig, 0, len(items))
	for _, item := range items {
		if !item.EnabledOrDefault() {
			continue
		}
		result = append(result, item)
	}
	return result
}

// dedupePeriodicTasks 按稳定键去重周期任务配置。
func dedupePeriodicTasks(items []config.TaskPeriodicConfig) []config.TaskPeriodicConfig {
	result := make([]config.TaskPeriodicConfig, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		key, err := periodicTaskKey(item)
		if err != nil {
			result = append(result, item)
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, item)
	}
	return result
}
