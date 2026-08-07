package taskqueue

import (
	"strings"
	"sync"
	"time"

	"admin/common/prometheusx"
	"admin/internal/task/stats"

	"github.com/Is999/go-utils/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	maxTaskMetricQueueLabels    = 32        // 限制 queue label 数量，避免多站点或异常队列污染指标维度
	maxTaskMetricTaskTypeLabels = 128       // 限制 task_type label 数量，任务类型超限后统一归并
	taskMetricLabelOther        = "other"   // 超出上限的动态 label 统一归并值
	taskMetricLabelUnknown      = "unknown" // 空 label 的统一归并值
	taskMetricResultSuccess     = "success" // 任务成功结果 label
	taskMetricResultFailed      = "failed"  // 任务失败结果 label
)

// 任务队列指标变量集中定义执行次数、耗时和业务处理量统计。
var (
	taskMetricsOnce sync.Once // 保证任务指标只注册一次
	taskMetricsErr  error     // 保存启动期指标注册冲突，运行期记录遇到错误时直接跳过
	// taskMetricLabelGuard 保护任务队列动态指标标签白名单。
	taskMetricLabelGuard = struct {
		mu        sync.RWMutex        // 保护动态 label 白名单
		queues    map[string]struct{} // 已允许的 queue label 集合
		taskTypes map[string]struct{} // 已允许的 task_type label 集合
	}{
		queues:    make(map[string]struct{}),
		taskTypes: make(map[string]struct{}),
	}

	// taskExecutionsTotal 统计任务执行结果次数。
	taskExecutionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "admin",
			Subsystem: "taskqueue",
			Name:      "executions_total",
			Help:      "任务执行累计数量。",
		},
		[]string{"queue", "task_type", "result"},
	)
	// taskExecutionDuration 统计任务执行耗时分布。
	taskExecutionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "admin",
			Subsystem: "taskqueue",
			Name:      "execution_duration_seconds",
			Help:      "任务执行耗时分布。",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600, 1800},
		},
		[]string{"queue", "task_type", "result"},
	)
	// taskProcessedItemsTotal 统计任务执行快照中的业务处理数量。
	taskProcessedItemsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "admin",
			Subsystem: "taskqueue",
			Name:      "processed_items_total",
			Help:      "任务执行统计快照中的业务处理量累计数量。",
		},
		[]string{"queue", "task_type", "action"},
	)
)

// RegisterMetrics 注册任务系统指标；同类型重复指标复用既有实例，冲突通过启动错误返回。
func RegisterMetrics() error {
	taskMetricsOnce.Do(func() {
		if taskExecutionsTotal, taskMetricsErr = prometheusx.Register(taskExecutionsTotal); taskMetricsErr != nil {
			return
		}
		if taskExecutionDuration, taskMetricsErr = prometheusx.Register(taskExecutionDuration); taskMetricsErr != nil {
			return
		}
		taskProcessedItemsTotal, taskMetricsErr = prometheusx.Register(taskProcessedItemsTotal)
	})
	return errors.Tag(taskMetricsErr)
}

// allowTaskMetricTaskTypeLabel 登记已注册任务类型，优先保留正式处理器的 task_type 维度。
func allowTaskMetricTaskTypeLabel(taskType string) {
	value := strings.TrimSpace(taskType)
	if value == "" {
		return
	}
	taskMetricLabelGuard.mu.Lock()
	defer taskMetricLabelGuard.mu.Unlock()
	taskMetricLabelGuard.taskTypes[value] = struct{}{}
}

// recordTaskExecutionMetrics 记录任务执行次数、耗时和低基数处理量指标。
func recordTaskExecutionMetrics(queue string, taskType string, runErr error, duration time.Duration, snapshot *taskstats.Snapshot) {
	if RegisterMetrics() != nil {
		return
	}
	queueLabel := normalizeTaskMetricQueueLabel(queue)
	taskTypeLabel := normalizeTaskMetricTaskTypeLabel(taskType)
	result := taskMetricResultSuccess
	if runErr != nil {
		result = taskMetricResultFailed
	}
	if duration <= 0 {
		duration = time.Millisecond
	}
	taskExecutionsTotal.WithLabelValues(queueLabel, taskTypeLabel, result).Inc()
	taskExecutionDuration.WithLabelValues(queueLabel, taskTypeLabel, result).Observe(duration.Seconds())
	recordTaskStatsMetricCounts(queueLabel, taskTypeLabel, snapshot)
}

// recordTaskStatsMetricCounts 只按固定动作聚合处理量，不把明细名称作为指标 label。
func recordTaskStatsMetricCounts(queueLabel string, taskTypeLabel string, snapshot *taskstats.Snapshot) {
	if snapshot == nil || snapshot.Empty() {
		return
	}
	recordTaskProcessedItems(queueLabel, taskTypeLabel, taskstats.ActionRead, snapshot.ReadCount)
	recordTaskProcessedItems(queueLabel, taskTypeLabel, taskstats.ActionInsert, snapshot.InsertCount)
	recordTaskProcessedItems(queueLabel, taskTypeLabel, taskstats.ActionUpdate, snapshot.UpdateCount)
	recordTaskProcessedItems(queueLabel, taskTypeLabel, taskstats.ActionDelete, snapshot.DeleteCount)
	recordTaskProcessedItems(queueLabel, taskTypeLabel, taskstats.ActionUpsert, snapshot.UpsertCount)
	recordTaskProcessedItems(queueLabel, taskTypeLabel, taskstats.ActionSkip, snapshot.SkipCount)
	recordTaskProcessedItems(queueLabel, taskTypeLabel, taskstats.ActionError, snapshot.ErrorCount)
}

// recordTaskProcessedItems 记录单个动作的处理量，非正数不写入指标。
func recordTaskProcessedItems(queueLabel string, taskTypeLabel string, action string, count int64) {
	if count <= 0 {
		return
	}
	taskProcessedItemsTotal.WithLabelValues(queueLabel, taskTypeLabel, action).Add(float64(count))
}

// normalizeTaskMetricQueueLabel 规整 queue label，并限制动态队列数量。
func normalizeTaskMetricQueueLabel(queue string) string {
	return normalizeTaskMetricGuardedLabel(queue, maxTaskMetricQueueLabels, taskMetricLabelGuard.queues)
}

// normalizeTaskMetricTaskTypeLabel 规整 task_type label，并限制动态任务类型数量。
func normalizeTaskMetricTaskTypeLabel(taskType string) string {
	return normalizeTaskMetricGuardedLabel(taskType, maxTaskMetricTaskTypeLabels, taskMetricLabelGuard.taskTypes)
}

// normalizeTaskMetricGuardedLabel 归一化动态指标 label，超出上限时归并到 other。
func normalizeTaskMetricGuardedLabel(value string, limit int, labels map[string]struct{}) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return taskMetricLabelUnknown
	}
	taskMetricLabelGuard.mu.RLock()
	_, ok := labels[value]
	taskMetricLabelGuard.mu.RUnlock()
	if ok {
		return value
	}
	taskMetricLabelGuard.mu.Lock()
	defer taskMetricLabelGuard.mu.Unlock()
	if _, ok = labels[value]; ok {
		return value
	}
	if len(labels) >= limit {
		return taskMetricLabelOther
	}
	labels[value] = struct{}{}
	return value
}
