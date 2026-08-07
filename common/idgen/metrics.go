package idgen

import (
	"strings"
	"sync"
	"time"

	"admin/common/prometheusx"

	"github.com/Is999/go-utils/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// 指标命名空间固定为当前服务名，避免 admin/api 指标混淆。
const idgenMetricNamespace = "admin"

var (
	idgenMetricsOnce sync.Once // idgenMetricsOnce 保证 ID 指标只注册一次
	idgenMetricsErr  error     // idgenMetricsErr 保存启动期指标注册冲突，运行期记录遇到错误时直接跳过

	// 记录 ID 生成结果总量，标签为业务 namespace、策略和结果。
	idgenGeneratedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: idgenMetricNamespace,
			Subsystem: "idgen",
			Name:      "generated_total",
			Help:      "ID 生成结果累计数量。",
		},
		[]string{"biz_type", "strategy", "result"},
	)
	// 记录 ID 生成耗时，用于观察不同取号策略的延迟抖动。
	idgenGenerateDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: idgenMetricNamespace,
			Subsystem: "idgen",
			Name:      "generate_duration_seconds",
			Help:      "ID 生成耗时。",
			Buckets:   []float64{0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1},
		},
		[]string{"biz_type", "strategy", "result"},
	)
	// 记录 Redis Segment 号段申请结果总量，标签为业务 namespace 和结果。
	idgenSegmentAllocationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: idgenMetricNamespace,
			Subsystem: "idgen_segment",
			Name:      "allocations_total",
			Help:      "Redis Segment 号段分配结果累计数量。",
		},
		[]string{"biz_type", "result"},
	)
	// 记录 Redis Segment 号段申请耗时，用于排查 Redis 或锁竞争问题。
	idgenSegmentAllocationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: idgenMetricNamespace,
			Subsystem: "idgen_segment",
			Name:      "allocation_duration_seconds",
			Help:      "Redis Segment 号段分配耗时。",
			Buckets:   []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2},
		},
		[]string{"biz_type", "result"},
	)
	// 记录当前进程本地缓存的 Segment 剩余容量，标签为业务 namespace。
	idgenSegmentRemaining = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: idgenMetricNamespace,
			Subsystem: "idgen_segment",
			Name:      "remaining",
			Help:      "当前进程本地 Segment 可用 ID 数量。",
		},
		[]string{"biz_type"},
	)
	// 记录 Snowflake Redis node_id 租约事件总量，用于观察抢占、续约和丢失。
	idgenSnowflakeLeaseEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: idgenMetricNamespace,
			Subsystem: "idgen_snowflake",
			Name:      "lease_events_total",
			Help:      "Snowflake Redis node_id 租约事件累计数量。",
		},
		[]string{"biz_type", "event"},
	)
)

// RegisterMetrics 注册 ID 指标；同类型重复指标复用既有实例，冲突通过启动错误返回。
func RegisterMetrics() error {
	idgenMetricsOnce.Do(func() {
		if idgenGeneratedTotal, idgenMetricsErr = prometheusx.Register(idgenGeneratedTotal); idgenMetricsErr != nil {
			return
		}
		if idgenGenerateDuration, idgenMetricsErr = prometheusx.Register(idgenGenerateDuration); idgenMetricsErr != nil {
			return
		}
		if idgenSegmentAllocationsTotal, idgenMetricsErr = prometheusx.Register(idgenSegmentAllocationsTotal); idgenMetricsErr != nil {
			return
		}
		if idgenSegmentAllocationDuration, idgenMetricsErr = prometheusx.Register(idgenSegmentAllocationDuration); idgenMetricsErr != nil {
			return
		}
		if idgenSegmentRemaining, idgenMetricsErr = prometheusx.Register(idgenSegmentRemaining); idgenMetricsErr != nil {
			return
		}
		idgenSnowflakeLeaseEventsTotal, idgenMetricsErr = prometheusx.Register(idgenSnowflakeLeaseEventsTotal)
	})
	return errors.Tag(idgenMetricsErr)
}

// recordIDGenerate 记录单次 ID 生成结果。
func recordIDGenerate(namespace string, strategy string, err error, elapsed time.Duration) {
	if RegisterMetrics() != nil {
		return
	}
	result := "success"
	if err != nil {
		result = "failed"
	}
	idgenGeneratedTotal.WithLabelValues(metricLabel(namespace), metricLabel(strategy), result).Inc()
	idgenGenerateDuration.WithLabelValues(metricLabel(namespace), metricLabel(strategy), result).Observe(elapsed.Seconds())
}

// RecordSegmentAllocation 记录 Redis Segment 号段分配结果。
func RecordSegmentAllocation(namespace string, result string, elapsed time.Duration) {
	if RegisterMetrics() != nil {
		return
	}
	idgenSegmentAllocationsTotal.WithLabelValues(metricLabel(namespace), metricLabel(result)).Inc()
	idgenSegmentAllocationDuration.WithLabelValues(metricLabel(namespace), metricLabel(result)).Observe(elapsed.Seconds())
}

// RecordSegmentRemaining 记录当前进程本地 Segment 剩余容量。
func RecordSegmentRemaining(namespace string, remaining int64) {
	if RegisterMetrics() != nil {
		return
	}
	if remaining < 0 {
		remaining = 0
	}
	idgenSegmentRemaining.WithLabelValues(metricLabel(namespace)).Set(float64(remaining))
}

// RecordSnowflakeLeaseEvent 记录 Snowflake Redis 租约事件。
func RecordSnowflakeLeaseEvent(namespace string, event string) {
	if RegisterMetrics() != nil {
		return
	}
	idgenSnowflakeLeaseEventsTotal.WithLabelValues(metricLabel(namespace), metricLabel(event)).Inc()
}

// metricLabel 清洗指标标签，避免空值和过长高基数值进入指标。
func metricLabel(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	if len(value) > 80 {
		return "other"
	}
	return value
}
