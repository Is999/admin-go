package runtimeconfig

import (
	"strings"

	"admin/internal/config"
	corelogic "admin/internal/logic"
	"admin/internal/model"
	"admin/internal/types"
)

// periodicReqToModel 把周期任务保存请求转换为草稿表模型。
func periodicReqToModel(req *types.SaveRuntimeTaskPeriodicReq, adminID int) model.RuntimeTaskPeriodic {
	return model.RuntimeTaskPeriodic{
		Name:             req.Name,
		Enabled:          req.Enabled,
		Cron:             req.Cron,
		EverySeconds:     req.EverySeconds,
		Workflow:         req.Workflow,
		Queue:            req.Queue,
		Targets:          model.StringSlice(req.Targets),
		ShardTotal:       req.ShardTotal,
		GrayPercent:      req.GrayPercent,
		Retry:            req.Retry,
		TimeoutSeconds:   req.TimeoutSeconds,
		Deadline:         req.Deadline,
		UniqueKey:        req.UniqueKey,
		UniqueTTLSeconds: req.UniqueTTLSeconds,
		SortOrder:        req.SortOrder,
		Remark:           req.Remark,
		CreatedByAdminID: adminID,
		UpdatedByAdminID: adminID,
	}
}

// periodicModelUpdateMap 返回周期任务草稿更新字段，避免覆盖创建人等稳定字段。
func periodicModelUpdateMap(row model.RuntimeTaskPeriodic) map[string]any {
	return map[string]any{
		"name":                row.Name,
		"enabled":             row.Enabled,
		"cron":                row.Cron,
		"every_seconds":       row.EverySeconds,
		"workflow":            row.Workflow,
		"queue":               row.Queue,
		"targets_json":        row.Targets,
		"shard_total":         row.ShardTotal,
		"gray_percent":        row.GrayPercent,
		"retry":               row.Retry,
		"timeout_seconds":     row.TimeoutSeconds,
		"deadline":            row.Deadline,
		"unique_key":          row.UniqueKey,
		"unique_ttl_seconds":  row.UniqueTTLSeconds,
		"sort_order":          row.SortOrder,
		"remark":              row.Remark,
		"updated_by_admin_id": row.UpdatedByAdminID,
	}
}

// periodicModelToConfig 把周期任务草稿模型转换为发布快照配置。
func periodicModelToConfig(row model.RuntimeTaskPeriodic) config.TaskPeriodicConfig {
	enabled := row.Enabled
	return config.TaskPeriodicConfig{
		Enabled:          &enabled,
		Name:             row.Name,
		Cron:             row.Cron,
		EverySeconds:     row.EverySeconds,
		Workflow:         row.Workflow,
		Queue:            row.Queue,
		Targets:          []string(row.Targets),
		ShardTotal:       row.ShardTotal,
		GrayPercent:      row.GrayPercent,
		Retry:            row.Retry,
		TimeoutSeconds:   row.TimeoutSeconds,
		Deadline:         row.Deadline,
		UniqueKey:        row.UniqueKey,
		UniqueTTLSeconds: row.UniqueTTLSeconds,
	}
}

// periodicConfigToModel 把发布快照中的周期任务转换为草稿模型，供回滚版本写回草稿使用。
func periodicConfigToModel(item config.TaskPeriodicConfig, adminID int, index int) model.RuntimeTaskPeriodic {
	return model.RuntimeTaskPeriodic{
		Name:             strings.TrimSpace(item.Name),
		Enabled:          item.EnabledOrDefault(),
		Cron:             strings.TrimSpace(item.Cron),
		EverySeconds:     item.EverySeconds,
		Workflow:         strings.TrimSpace(item.Workflow),
		Queue:            strings.TrimSpace(item.Queue),
		Targets:          model.StringSlice(uniqueStrings(item.Targets)),
		ShardTotal:       item.ShardTotal,
		GrayPercent:      item.GrayPercent,
		Retry:            item.Retry,
		TimeoutSeconds:   item.TimeoutSeconds,
		Deadline:         strings.TrimSpace(item.Deadline),
		UniqueKey:        strings.TrimSpace(item.UniqueKey),
		UniqueTTLSeconds: item.UniqueTTLSeconds,
		SortOrder:        index + 1,
		CreatedByAdminID: adminID,
		UpdatedByAdminID: adminID,
	}
}

// periodicModelToItem 把周期任务草稿模型转换为管理端列表项。
func periodicModelToItem(row model.RuntimeTaskPeriodic) types.RuntimeTaskPeriodicItem {
	return types.RuntimeTaskPeriodicItem{
		ID:               row.ID,
		Enabled:          row.Enabled,
		Name:             row.Name,
		Cron:             row.Cron,
		EverySeconds:     row.EverySeconds,
		Workflow:         row.Workflow,
		Queue:            row.Queue,
		Targets:          []string(row.Targets),
		ShardTotal:       row.ShardTotal,
		GrayPercent:      row.GrayPercent,
		Retry:            row.Retry,
		TimeoutSeconds:   row.TimeoutSeconds,
		Deadline:         row.Deadline,
		UniqueKey:        row.UniqueKey,
		UniqueTTLSeconds: row.UniqueTTLSeconds,
		SortOrder:        row.SortOrder,
		Remark:           row.Remark,
		CreatedAt:        corelogic.FormatDateTime(row.CreatedAt),
		UpdatedAt:        corelogic.FormatDateTime(row.UpdatedAt),
	}
}

// archiveReqToModel 把归档任务保存请求转换为草稿表模型。
func archiveReqToModel(req *types.SaveRuntimeArchiveJobReq, adminID int) model.RuntimeArchiveJob {
	archiveDelayDays := archiveDelayWithHotKeepDefault(req.HotKeepDays, req.ArchiveDelayDays)
	deleteDelayDays := archiveDelayWithHotKeepDefault(req.HotKeepDays, req.DeleteDelayDays)
	return model.RuntimeArchiveJob{
		Name:                    req.Name,
		Enabled:                 req.Enabled,
		Database:                req.Database,
		HotTableName:            req.TableName,
		TimeColumn:              req.TimeColumn,
		TimeColumnType:          req.TimeColumnType,
		TimeColumnFormat:        req.TimeColumnFormat,
		TimeColumnUnixUnit:      req.TimeColumnUnixUnit,
		PrimaryKey:              req.PrimaryKey,
		ArchiveCondition:        req.ArchiveCondition,
		DeleteCondition:         req.DeleteCondition,
		SplitUnit:               req.SplitUnit,
		CustomDays:              req.CustomDays,
		HotKeepDays:             req.HotKeepDays,
		ArchiveDelayDays:        archiveDelayDays,
		ArchiveWindowSeconds:    req.ArchiveWindowSeconds,
		ArchiveWindowMode:       req.ArchiveWindowMode,
		ArchiveMaxWindowsPerRun: req.ArchiveMaxWindowsPerRun,
		ArchiveAutoMaxWindows:   req.ArchiveAutoMaxWindows,
		ArchiveAutoLightRows:    req.ArchiveAutoLightRows,
		ArchiveAutoLightMs:      req.ArchiveAutoLightMs,
		DeleteDisabled:          req.DeleteDisabled,
		DeleteDelayDays:         deleteDelayDays,
		DeleteWindowSeconds:     req.DeleteWindowSeconds,
		DeleteMaxWindowsPerRun:  req.DeleteMaxWindowsPerRun,
		BatchSize:               req.BatchSize,
		DeleteBatchSize:         req.DeleteBatchSize,
		MaxHistoryTables:        req.MaxHistoryTables,
		HistoryTablePrefix:      req.HistoryTablePrefix,
		HistoryTableNameRule:    req.HistoryTableNameRule,
		StartAt:                 req.StartAt,
		QueryWriteDB:            req.QueryWriteDB,
		SortOrder:               req.SortOrder,
		Remark:                  req.Remark,
		CreatedByAdminID:        adminID,
		UpdatedByAdminID:        adminID,
	}
}

// archiveModelUpdateMap 返回归档任务草稿更新字段，保持创建信息不被覆盖。
func archiveModelUpdateMap(row model.RuntimeArchiveJob) map[string]any {
	return map[string]any{
		"name":                        row.Name,
		"enabled":                     row.Enabled,
		"database_name":               row.Database,
		"table_name":                  row.HotTableName,
		"time_column":                 row.TimeColumn,
		"time_column_type":            row.TimeColumnType,
		"time_column_format":          row.TimeColumnFormat,
		"time_column_unix_unit":       row.TimeColumnUnixUnit,
		"primary_key":                 row.PrimaryKey,
		"archive_condition":           row.ArchiveCondition,
		"delete_condition":            row.DeleteCondition,
		"split_unit":                  row.SplitUnit,
		"custom_days":                 row.CustomDays,
		"hot_keep_days":               row.HotKeepDays,
		"archive_delay_days":          row.ArchiveDelayDays,
		"archive_window_seconds":      row.ArchiveWindowSeconds,
		"archive_window_mode":         row.ArchiveWindowMode,
		"archive_max_windows_per_run": row.ArchiveMaxWindowsPerRun,
		"archive_auto_max_windows":    row.ArchiveAutoMaxWindows,
		"archive_auto_light_rows":     row.ArchiveAutoLightRows,
		"archive_auto_light_ms":       row.ArchiveAutoLightMs,
		"delete_disabled":             row.DeleteDisabled,
		"delete_delay_days":           row.DeleteDelayDays,
		"delete_window_seconds":       row.DeleteWindowSeconds,
		"delete_max_windows_per_run":  row.DeleteMaxWindowsPerRun,
		"batch_size":                  row.BatchSize,
		"delete_batch_size":           row.DeleteBatchSize,
		"max_history_tables":          row.MaxHistoryTables,
		"history_table_prefix":        row.HistoryTablePrefix,
		"history_table_name_rule":     row.HistoryTableNameRule,
		"start_at":                    row.StartAt,
		"query_write_db":              row.QueryWriteDB,
		"sort_order":                  row.SortOrder,
		"remark":                      row.Remark,
		"updated_by_admin_id":         row.UpdatedByAdminID,
	}
}

// archiveModelToConfig 把归档任务草稿模型转换为发布快照配置。
func archiveModelToConfig(row model.RuntimeArchiveJob) config.ArchiveJobConfig {
	return config.ArchiveJobConfig{
		Name:                    row.Name,
		Enabled:                 row.Enabled,
		Database:                row.Database,
		TableName:               row.HotTableName,
		TimeColumn:              row.TimeColumn,
		TimeColumnType:          row.TimeColumnType,
		TimeColumnFormat:        row.TimeColumnFormat,
		TimeColumnUnixUnit:      row.TimeColumnUnixUnit,
		PrimaryKey:              row.PrimaryKey,
		ArchiveCondition:        row.ArchiveCondition,
		DeleteCondition:         row.DeleteCondition,
		SplitUnit:               row.SplitUnit,
		CustomDays:              row.CustomDays,
		HotKeepDays:             row.HotKeepDays,
		ArchiveDelayDays:        row.ArchiveDelayDays,
		ArchiveWindowSeconds:    row.ArchiveWindowSeconds,
		ArchiveWindowMode:       row.ArchiveWindowMode,
		ArchiveMaxWindowsPerRun: row.ArchiveMaxWindowsPerRun,
		ArchiveAutoMaxWindows:   row.ArchiveAutoMaxWindows,
		ArchiveAutoLightRows:    row.ArchiveAutoLightRows,
		ArchiveAutoLightMs:      row.ArchiveAutoLightMs,
		DeleteDisabled:          row.DeleteDisabled,
		DeleteDelayDays:         row.DeleteDelayDays,
		DeleteWindowSeconds:     row.DeleteWindowSeconds,
		DeleteMaxWindowsPerRun:  row.DeleteMaxWindowsPerRun,
		BatchSize:               row.BatchSize,
		DeleteBatchSize:         row.DeleteBatchSize,
		MaxHistoryTables:        row.MaxHistoryTables,
		HistoryTablePrefix:      row.HistoryTablePrefix,
		HistoryTableNameRule:    row.HistoryTableNameRule,
		StartAt:                 row.StartAt,
		QueryWriteDB:            row.QueryWriteDB,
	}
}

// archiveConfigToModel 把发布快照中的归档任务转换为草稿模型，供回滚版本写回草稿使用。
func archiveConfigToModel(item config.ArchiveJobConfig, adminID int, index int) model.RuntimeArchiveJob {
	item = normalizeArchiveConfigDefaults(item)
	database := strings.TrimSpace(item.Database)
	if database == "" {
		database = "main"
	}
	return model.RuntimeArchiveJob{
		Name:                    strings.TrimSpace(item.Name),
		Enabled:                 item.Enabled,
		Database:                database,
		HotTableName:            strings.TrimSpace(item.TableName),
		TimeColumn:              strings.TrimSpace(item.TimeColumn),
		TimeColumnType:          strings.TrimSpace(item.TimeColumnType),
		TimeColumnFormat:        strings.TrimSpace(item.TimeColumnFormat),
		TimeColumnUnixUnit:      strings.TrimSpace(item.TimeColumnUnixUnit),
		PrimaryKey:              strings.TrimSpace(item.PrimaryKey),
		ArchiveCondition:        strings.TrimSpace(item.ArchiveCondition),
		DeleteCondition:         strings.TrimSpace(item.DeleteCondition),
		SplitUnit:               strings.TrimSpace(item.SplitUnit),
		CustomDays:              item.CustomDays,
		HotKeepDays:             item.HotKeepDays,
		ArchiveDelayDays:        item.ArchiveDelayDays,
		ArchiveWindowSeconds:    item.ArchiveWindowSeconds,
		ArchiveWindowMode:       strings.TrimSpace(item.ArchiveWindowMode),
		ArchiveMaxWindowsPerRun: item.ArchiveMaxWindowsPerRun,
		ArchiveAutoMaxWindows:   item.ArchiveAutoMaxWindows,
		ArchiveAutoLightRows:    item.ArchiveAutoLightRows,
		ArchiveAutoLightMs:      item.ArchiveAutoLightMs,
		DeleteDisabled:          item.DeleteDisabled,
		DeleteDelayDays:         item.DeleteDelayDays,
		DeleteWindowSeconds:     item.DeleteWindowSeconds,
		DeleteMaxWindowsPerRun:  item.DeleteMaxWindowsPerRun,
		BatchSize:               item.BatchSize,
		DeleteBatchSize:         item.DeleteBatchSize,
		MaxHistoryTables:        item.MaxHistoryTables,
		HistoryTablePrefix:      strings.TrimSpace(item.HistoryTablePrefix),
		HistoryTableNameRule:    strings.TrimSpace(item.HistoryTableNameRule),
		StartAt:                 strings.TrimSpace(item.StartAt),
		QueryWriteDB:            item.QueryWriteDB,
		SortOrder:               index + 1,
		CreatedByAdminID:        adminID,
		UpdatedByAdminID:        adminID,
	}
}

// normalizeReleaseSnapshot 补齐发布快照中的运行默认值，避免 YAML 导入、草稿表和 active release 语义漂移。
func normalizeReleaseSnapshot(snapshot ReleaseSnapshot) ReleaseSnapshot {
	snapshot.ArchiveJobs = append([]config.ArchiveJobConfig(nil), snapshot.ArchiveJobs...)
	for index := range snapshot.ArchiveJobs {
		snapshot.ArchiveJobs[index] = normalizeArchiveConfigDefaults(snapshot.ArchiveJobs[index])
	}
	snapshot.TaskPeriodic = append([]config.TaskPeriodicConfig(nil), snapshot.TaskPeriodic...)
	for index := range snapshot.TaskPeriodic {
		snapshot.TaskPeriodic[index] = normalizePeriodicConfigDefaults(snapshot.TaskPeriodic[index])
	}
	return snapshot
}

// normalizePeriodicConfigDefaults 补齐周期任务默认启用语义，保持 YAML 导入、草稿和 active release 一致。
func normalizePeriodicConfigDefaults(item config.TaskPeriodicConfig) config.TaskPeriodicConfig {
	if item.Enabled == nil {
		enabled := true
		item.Enabled = &enabled
	}
	return item
}

// normalizeArchiveConfigDefaults 补齐归档任务默认值，保持草稿、发布快照和执行服务口径一致。
func normalizeArchiveConfigDefaults(item config.ArchiveJobConfig) config.ArchiveJobConfig {
	item.ArchiveDelayDays = archiveDelayWithHotKeepDefault(item.HotKeepDays, item.ArchiveDelayDays)
	item.DeleteDelayDays = archiveDelayWithHotKeepDefault(item.HotKeepDays, item.DeleteDelayDays)
	return item
}

// archiveDelayWithHotKeepDefault 补齐归档/删除延迟天数，避免草稿表与归档执行态默认值不一致。
func archiveDelayWithHotKeepDefault(hotKeepDays, delayDays int) int {
	if hotKeepDays > 0 && delayDays <= 0 {
		return hotKeepDays
	}
	return delayDays
}

// archiveModelToItem 把归档任务草稿模型转换为管理端列表项。
func archiveModelToItem(row model.RuntimeArchiveJob) types.RuntimeArchiveJobItem {
	return types.RuntimeArchiveJobItem{
		ID:                      row.ID,
		Enabled:                 row.Enabled,
		Name:                    row.Name,
		Database:                row.Database,
		TableName:               row.HotTableName,
		TimeColumn:              row.TimeColumn,
		TimeColumnType:          row.TimeColumnType,
		TimeColumnFormat:        row.TimeColumnFormat,
		TimeColumnUnixUnit:      row.TimeColumnUnixUnit,
		PrimaryKey:              row.PrimaryKey,
		ArchiveCondition:        row.ArchiveCondition,
		DeleteCondition:         row.DeleteCondition,
		SplitUnit:               row.SplitUnit,
		CustomDays:              row.CustomDays,
		HotKeepDays:             row.HotKeepDays,
		ArchiveDelayDays:        row.ArchiveDelayDays,
		ArchiveWindowSeconds:    row.ArchiveWindowSeconds,
		ArchiveWindowMode:       row.ArchiveWindowMode,
		ArchiveMaxWindowsPerRun: row.ArchiveMaxWindowsPerRun,
		ArchiveAutoMaxWindows:   row.ArchiveAutoMaxWindows,
		ArchiveAutoLightRows:    row.ArchiveAutoLightRows,
		ArchiveAutoLightMs:      row.ArchiveAutoLightMs,
		DeleteDisabled:          row.DeleteDisabled,
		DeleteDelayDays:         row.DeleteDelayDays,
		DeleteWindowSeconds:     row.DeleteWindowSeconds,
		DeleteMaxWindowsPerRun:  row.DeleteMaxWindowsPerRun,
		BatchSize:               row.BatchSize,
		DeleteBatchSize:         row.DeleteBatchSize,
		MaxHistoryTables:        row.MaxHistoryTables,
		HistoryTablePrefix:      row.HistoryTablePrefix,
		HistoryTableNameRule:    row.HistoryTableNameRule,
		StartAt:                 row.StartAt,
		QueryWriteDB:            row.QueryWriteDB,
		SortOrder:               row.SortOrder,
		Remark:                  row.Remark,
		CreatedAt:               corelogic.FormatDateTime(row.CreatedAt),
		UpdatedAt:               corelogic.FormatDateTime(row.UpdatedAt),
	}
}

// stateCacheToItem 把 active 状态缓存转换为管理端展示项。
func stateCacheToItem(state StateCache) types.RuntimeConfigStateItem {
	return types.RuntimeConfigStateItem{
		ActiveReleaseID: state.ActiveReleaseID,
		ActiveVersion:   state.ActiveVersion,
		ActiveChecksum:  state.ActiveChecksum,
		PublishedAt:     formatUnix(state.PublishedAtUnix),
	}
}

// releaseModelToItem 把发布记录模型转换为管理端列表项。
func releaseModelToItem(row model.RuntimeConfigRelease) types.RuntimeConfigReleaseItem {
	return types.RuntimeConfigReleaseItem{
		ID:                 row.ID,
		VersionNo:          row.VersionNo,
		Checksum:           row.Checksum,
		BaseReleaseID:      row.BaseReleaseID,
		RestartRequired:    row.RestartRequired,
		RestartReason:      row.RestartReason,
		Remark:             row.Remark,
		PublishedByAdminID: row.PublishedByAdminID,
		PublishedByName:    row.PublishedByName,
		PublishedAt:        corelogic.FormatDateTime(row.PublishedAt),
	}
}

// snapshotToResp 把发布快照转换为管理端预览响应。
func snapshotToResp(snapshot ReleaseSnapshot) types.RuntimeConfigSnapshot {
	resp := types.RuntimeConfigSnapshot{
		ArchiveJobs:  make([]types.RuntimeArchiveJobItem, 0, len(snapshot.ArchiveJobs)),
		TaskPeriodic: make([]types.RuntimeTaskPeriodicItem, 0, len(snapshot.TaskPeriodic)),
	}
	for index, item := range snapshot.ArchiveJobs {
		resp.ArchiveJobs = append(resp.ArchiveJobs, archiveModelToItem(archiveConfigToModel(item, 0, index)))
	}
	for index, item := range snapshot.TaskPeriodic {
		resp.TaskPeriodic = append(resp.TaskPeriodic, periodicModelToItem(periodicConfigToModel(item, 0, index)))
	}
	return resp
}
