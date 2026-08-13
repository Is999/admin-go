// Package limits 统一任务投递和工作流覆盖参数的生产硬上限。
package limits

import (
	"fmt"
	"strings"
)

const (
	// MinPeriodicEverySeconds 限制固定间隔周期任务的最小秒数，避免高频误配置打爆队列。
	MinPeriodicEverySeconds = 5
	// MaxRetry 与 Asynq 默认最大重试边界一致，避免人工覆盖制造重试风暴。
	MaxRetry = 25
	// MaxShardTotal 限制单个工作流可生成的分片任务数，避免误配置放大队列和状态查询负载。
	MaxShardTotal = 128
	// MaxTimeoutSeconds 允许现有两小时任务并把单次执行硬限制在一天内。
	MaxTimeoutSeconds = 24 * 60 * 60
	// MaxUniqueTTLSeconds 限制任务去重记录最长保留三十天，避免误配置长期占用 Redis。
	MaxUniqueTTLSeconds = 30 * 24 * 60 * 60
	// MaxScheduleDelaySeconds 限制一次性任务最长提前三十天进入 Redis scheduled 状态。
	MaxScheduleDelaySeconds = 30 * 24 * 60 * 60
	// MaxPeriodicCount 限制单应用周期任务草稿和发布快照为一万条，超过后拒绝新增、全量覆盖、回滚和发布，不做静默截断。
	MaxPeriodicCount = 10000
	// MaxArchiveJobCount 限制单应用归档任务草稿和发布快照为一万条，超过后拒绝新增、全量覆盖、回滚和发布，不做静默截断。
	MaxArchiveJobCount = 10000
	// MaxPayloadBytes 限制单个通用任务负载为一 MiB，大数据必须改传对象引用或游标。
	MaxPayloadBytes = 1 << 20
	// MaxWorkflowTargets 限制单次工作流目标数量，避免请求解析和任务扇出被异常列表放大。
	MaxWorkflowTargets = 11000
	// MaxWorkflowTargetBytes 限制单个工作流目标的 UTF-8 字节数。
	MaxWorkflowTargetBytes = 1024
	// MaxWorkflowTargetsBytes 限制工作流目标总字节数，分片节点仍须按需拆分目标。
	MaxWorkflowTargetsBytes = 256 << 10
	// MaxUniqueKeyBytes 限制工作流幂等键长度，避免生成超长 Redis key。
	MaxUniqueKeyBytes = 256
)

// ValidateWorkflowTargets 校验工作流目标数量和体积，空目标不计入有效数量。
func ValidateWorkflowTargets(targets []string) error {
	count := 0
	totalBytes := 0
	for _, target := range targets {
		target = strings.TrimSpace(target)
		if target == "" {
			continue
		}
		count++
		if len(target) > MaxWorkflowTargetBytes {
			return fmt.Errorf("单个工作流目标不能超过 %d 字节", MaxWorkflowTargetBytes)
		}
		totalBytes += len(target)
	}
	if count > MaxWorkflowTargets {
		return fmt.Errorf("工作流目标不能超过 %d 个", MaxWorkflowTargets)
	}
	if totalBytes > MaxWorkflowTargetsBytes {
		return fmt.Errorf("工作流目标总长度不能超过 %d 字节", MaxWorkflowTargetsBytes)
	}
	return nil
}
