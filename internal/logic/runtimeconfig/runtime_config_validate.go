package runtimeconfig

import (
	"fmt"
	"strings"

	"admin/internal/config"
	tasklimits "admin/internal/task/limits"
	taskqueue "admin/internal/task/queue"

	"github.com/Is999/go-utils/errors"
)

// ValidateSnapshot 校验发布快照的周期任务和归档任务唯一性。
func ValidateSnapshot(snapshot ReleaseSnapshot) ([]string, error) {
	messages := []string{
		fmt.Sprintf("周期任务 %d 条", len(snapshot.TaskPeriodic)),
		fmt.Sprintf("归档任务 %d 条", len(snapshot.ArchiveJobs)),
	}
	if err := validatePeriodicConfigs(snapshot.TaskPeriodic); err != nil {
		return messages, errors.Tag(err)
	}
	if err := validateArchiveJobConfigs(snapshot.ArchiveJobs); err != nil {
		return messages, errors.Tag(err)
	}
	return messages, nil
}

// validatePeriodicConfigs 校验周期任务快照，避免发布后才暴露重复调度或无效计划。
func validatePeriodicConfigs(items []config.TaskPeriodicConfig) error {
	if len(items) > tasklimits.MaxPeriodicCount {
		return errors.Errorf("周期任务不能超过 %d 条", tasklimits.MaxPeriodicCount)
	}
	taskKeys := make(map[string]struct{}, len(items))
	uniqueKeys := make(map[string]struct{}, len(items))
	for _, item := range items {
		if strings.TrimSpace(item.Workflow) == "" {
			return errors.Errorf("周期任务 workflow 不能为空 name=%s", strings.TrimSpace(item.Name))
		}
		if strings.TrimSpace(item.Cron) == "" && item.EverySeconds <= 0 {
			return errors.Errorf("周期任务必须配置 cron 或 every_seconds name=%s", strings.TrimSpace(item.Name))
		}
		if strings.TrimSpace(item.Cron) != "" && item.EverySeconds > 0 {
			return errors.Errorf("周期任务 cron 与 every_seconds 不能同时配置 name=%s", strings.TrimSpace(item.Name))
		}
		if item.GrayPercent < 0 || item.GrayPercent > 100 {
			return errors.Errorf("周期任务 gray_percent 必须在 0 到 100 之间 name=%s", strings.TrimSpace(item.Name))
		}
		if item.Retry < 0 || item.TimeoutSeconds < 0 || item.ShardTotal < 0 || item.UniqueTTLSeconds < 0 {
			return errors.Errorf("周期任务数值参数不能小于 0 name=%s", strings.TrimSpace(item.Name))
		}
		if item.Retry > tasklimits.MaxRetry {
			return errors.Errorf("周期任务 retry 不能超过 %d name=%s", tasklimits.MaxRetry, strings.TrimSpace(item.Name))
		}
		if item.TimeoutSeconds > tasklimits.MaxTimeoutSeconds {
			return errors.Errorf("周期任务 timeout_seconds 不能超过 %d name=%s", tasklimits.MaxTimeoutSeconds, strings.TrimSpace(item.Name))
		}
		if item.ShardTotal > tasklimits.MaxShardTotal {
			return errors.Errorf("周期任务 shard_total 不能超过 %d name=%s", tasklimits.MaxShardTotal, strings.TrimSpace(item.Name))
		}
		if item.UniqueTTLSeconds > tasklimits.MaxUniqueTTLSeconds {
			return errors.Errorf("周期任务 unique_ttl_seconds 不能超过 %d name=%s", tasklimits.MaxUniqueTTLSeconds, strings.TrimSpace(item.Name))
		}
		taskKey, err := taskqueue.PeriodicTaskConfigKey(item)
		if err != nil {
			return errors.Wrapf(err, "周期任务配置无效 name=%s workflow=%s", strings.TrimSpace(item.Name), strings.TrimSpace(item.Workflow))
		}
		if _, ok := taskKeys[taskKey]; ok {
			return errors.Errorf("周期任务稳定键重复: %s", taskKey)
		}
		taskKeys[taskKey] = struct{}{}
		uniqueKey := strings.TrimSpace(item.UniqueKey)
		if uniqueKey == "" {
			continue
		}
		if _, ok := uniqueKeys[uniqueKey]; ok {
			return errors.Errorf("周期任务 unique_key 重复: %s", uniqueKey)
		}
		uniqueKeys[uniqueKey] = struct{}{}
	}
	return nil
}

// validateArchiveJobConfigs 校验归档任务快照，避免发布重复任务或负数运行参数。
func validateArchiveJobConfigs(items []config.ArchiveJobConfig) error {
	if len(items) > tasklimits.MaxArchiveJobCount {
		return errors.Errorf("归档任务不能超过 %d 条", tasklimits.MaxArchiveJobCount)
	}
	names := make(map[string]struct{}, len(items))
	for _, item := range items {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			return errors.Errorf("归档任务 name 不能为空")
		}
		if _, ok := names[name]; ok {
			return errors.Errorf("归档任务名称重复: %s", name)
		}
		names[name] = struct{}{}
		if strings.TrimSpace(item.TableName) == "" {
			return errors.Errorf("归档任务 table_name 不能为空 name=%s", name)
		}
		if item.CustomDays < 0 || item.HotKeepDays < 0 || item.ArchiveDelayDays < 0 ||
			item.ArchiveWindowSeconds < 0 || item.ArchiveMaxWindowsPerRun < 0 ||
			item.ArchiveAutoMaxWindows < 0 || item.ArchiveAutoLightRows < 0 ||
			item.ArchiveAutoLightMs < 0 || item.DeleteDelayDays < 0 ||
			item.DeleteWindowSeconds < 0 || item.DeleteMaxWindowsPerRun < 0 ||
			item.BatchSize < 0 || item.DeleteBatchSize < 0 || item.MaxHistoryTables < 0 {
			return errors.Errorf("归档任务数值参数不能小于 0 name=%s", name)
		}
	}
	return nil
}
