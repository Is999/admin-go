//lint:file-ignore SA5008 ignore go-zero optional tag

package types

import (
	"strings"
	"time"

	"admin/helper"
	tasklimits "admin/internal/task/limits"

	"github.com/Is999/go-utils/errors"
)

const (
	// runtimeConfigMaxPageSize 仅放宽周期任务和归档任务草稿列表到五百条，发布历史等通用列表仍使用一百条上限。
	runtimeConfigMaxPageSize = 500
)

// RuntimeConfigOverviewReq 查询运行配置概览；完整快照只在对比或详情场景显式请求。
type RuntimeConfigOverviewReq struct {
	IncludeSnapshots bool `form:"includeSnapshots,optional"` // 是否返回 active 与草稿全量快照；默认 false，最大可能各含一万条任务
}

// RuntimeConfigOverviewResp 返回运行配置来源、active 版本、草稿统计和快照对比数据。
type RuntimeConfigOverviewResp struct {
	Source              string                  `json:"source"`              // 配置来源：file 或 database
	PollIntervalSeconds int                     `json:"pollIntervalSeconds"` // DB 模式轻量轮询间隔秒数
	State               RuntimeConfigStateItem  `json:"state"`               // 当前 active 版本状态
	Draft               RuntimeConfigDraftCount `json:"draft"`               // 草稿配置数量
	CurrentSnapshot     RuntimeConfigSnapshot   `json:"currentSnapshot"`     // includeSnapshots=true 时返回当前运行态全量快照，否则为空列表
	DraftSnapshot       RuntimeConfigSnapshot   `json:"draftSnapshot"`       // includeSnapshots=true 时返回当前草稿全量快照，否则为空列表
	DraftChecksum       string                  `json:"draftChecksum"`       // includeSnapshots=true 时返回草稿快照 SHA256
	DraftChanged        bool                    `json:"draftChanged"`        // includeSnapshots=true 时表示草稿 checksum 是否不同于 active
}

// RuntimeConfigDraftCount 表示当前草稿配置数量。
type RuntimeConfigDraftCount struct {
	PeriodicTasks int64 `json:"periodicTasks"` // 周期任务草稿数量
	ArchiveJobs   int64 `json:"archiveJobs"`   // 归档任务草稿数量
}

// RuntimeConfigStateItem 表示运行配置 active 版本状态。
type RuntimeConfigStateItem struct {
	ActiveReleaseID uint64 `json:"activeReleaseId"` // 当前发布 ID
	ActiveVersion   uint64 `json:"activeVersion"`   // 当前版本号
	ActiveChecksum  string `json:"activeChecksum"`  // 当前快照 SHA256
	PublishedAt     string `json:"publishedAt"`     // 最近发布时间
}

// RuntimeConfigSnapshot 是发布快照的接口展示结构。
type RuntimeConfigSnapshot struct {
	ArchiveJobs  []RuntimeArchiveJobItem   `json:"archiveJobs"`  // 归档任务配置列表，按 sortOrder/id 稳定排序
	TaskPeriodic []RuntimeTaskPeriodicItem `json:"taskPeriodic"` // 周期任务配置列表，按 sortOrder/id 稳定排序
}

// RuntimeTaskPeriodicQueryReq 查询周期任务草稿。
type RuntimeTaskPeriodicQueryReq struct {
	GetPageReq        // GetPageReq 表示分页参数。
	Workflow   string `form:"workflow,optional"` // 工作流过滤
	Enabled    *bool  `form:"enabled,optional"`  // 启用状态过滤
	Keyword    string `form:"keyword,optional"`  // 名称/队列关键字
}

// Validate 校验并归一化周期任务查询参数。
func (r *RuntimeTaskPeriodicQueryReq) Validate() error {
	r.Workflow = strings.TrimSpace(r.Workflow)
	r.Keyword = strings.TrimSpace(r.Keyword)
	r.Page, r.PageSize = normalizePageWithMax(r.Page, r.PageSize, defaultPageSize, runtimeConfigMaxPageSize)
	return nil
}

// SaveRuntimeTaskPeriodicReq 保存周期任务草稿。
type SaveRuntimeTaskPeriodicReq struct {
	ID               uint64   `json:"id,optional"`               // 草稿 ID；为空时新增
	Enabled          bool     `json:"enabled"`                   // 是否启用
	Name             string   `json:"name"`                      // 周期任务名称
	Cron             string   `json:"cron,optional"`             // cron 表达式
	EverySeconds     int      `json:"everySeconds,optional"`     // 固定间隔秒数
	Workflow         string   `json:"workflow"`                  // 工作流名称
	Queue            string   `json:"queue,optional"`            // 投递队列
	Targets          []string `json:"targets,optional"`          // 执行目标列表
	ShardTotal       int      `json:"shardTotal,optional"`       // 分片总数
	GrayPercent      int      `json:"grayPercent,optional"`      // 灰度比例
	Retry            int      `json:"retry,optional"`            // 覆盖重试次数
	TimeoutSeconds   int      `json:"timeoutSeconds,optional"`   // 任务超时秒数
	Deadline         string   `json:"deadline,optional"`         // 截止时间 RFC3339
	UniqueKey        string   `json:"uniqueKey,optional"`        // 去重键
	UniqueTTLSeconds int      `json:"uniqueTtlSeconds,optional"` // 去重 TTL 秒数
	SortOrder        int      `json:"sortOrder,optional"`        // 排序值
	Remark           string   `json:"remark,optional"`           // 备注
}

// Validate 校验并归一化周期任务保存参数。
func (r *SaveRuntimeTaskPeriodicReq) Validate() error {
	r.Name = strings.TrimSpace(r.Name)
	r.Cron = strings.TrimSpace(r.Cron)
	r.Workflow = strings.TrimSpace(r.Workflow)
	r.Queue = strings.TrimSpace(r.Queue)
	r.Deadline = strings.TrimSpace(r.Deadline)
	r.UniqueKey = strings.TrimSpace(r.UniqueKey)
	r.Remark = strings.TrimSpace(r.Remark)
	if r.Name == "" {
		return errors.Errorf("name 不能为空")
	}
	if r.Workflow == "" {
		return errors.Errorf("workflow 不能为空")
	}
	if r.Cron == "" && r.EverySeconds <= 0 {
		return errors.Errorf("cron 和 everySeconds 至少配置一项")
	}
	if r.Cron != "" && r.EverySeconds > 0 {
		return errors.Errorf("cron 和 everySeconds 不能同时配置")
	}
	if r.EverySeconds > 0 && r.EverySeconds < tasklimits.MinPeriodicEverySeconds {
		return errors.Errorf("everySeconds 不能小于 %d", tasklimits.MinPeriodicEverySeconds)
	}
	if r.GrayPercent < 0 || r.GrayPercent > 100 {
		return errors.Errorf("grayPercent 必须在 0 到 100 之间")
	}
	if r.Retry < 0 || r.TimeoutSeconds < 0 || r.UniqueTTLSeconds < 0 || r.ShardTotal < 0 {
		return errors.Errorf("retry、timeoutSeconds、uniqueTtlSeconds、shardTotal 不能小于 0")
	}
	if r.Retry > tasklimits.MaxRetry {
		return errors.Errorf("retry 不能超过 %d", tasklimits.MaxRetry)
	}
	if r.TimeoutSeconds > tasklimits.MaxTimeoutSeconds {
		return errors.Errorf("timeoutSeconds 不能超过 %d", tasklimits.MaxTimeoutSeconds)
	}
	if r.ShardTotal > tasklimits.MaxShardTotal {
		return errors.Errorf("shardTotal 不能超过 %d", tasklimits.MaxShardTotal)
	}
	if r.UniqueTTLSeconds > tasklimits.MaxUniqueTTLSeconds {
		return errors.Errorf("uniqueTtlSeconds 不能超过 %d", tasklimits.MaxUniqueTTLSeconds)
	}
	if r.Deadline != "" {
		if _, err := time.Parse(time.RFC3339, r.Deadline); err != nil {
			return errors.Errorf("deadline 必须为 RFC3339 时间格式")
		}
	}
	r.Targets = helper.UniqueNonEmptyStrings(r.Targets)
	if err := tasklimits.ValidateWorkflowTargets(r.Targets); err != nil {
		return errors.Tag(err)
	}
	if len(r.Targets) == 0 {
		r.Targets = nil
	}
	if len(r.UniqueKey) > tasklimits.MaxUniqueKeyBytes {
		return errors.Errorf("uniqueKey 不能超过 %d 字节", tasklimits.MaxUniqueKeyBytes)
	}
	return nil
}

// RuntimeArchiveJobQueryReq 查询归档任务草稿。
type RuntimeArchiveJobQueryReq struct {
	GetPageReq        // GetPageReq 表示分页参数。
	Enabled    *bool  `form:"enabled,optional"`  // 启用状态过滤
	Database   string `form:"database,optional"` // 数据库过滤
	Keyword    string `form:"keyword,optional"`  // 名称/表名关键字
}

// Validate 校验并归一化归档任务查询参数。
func (r *RuntimeArchiveJobQueryReq) Validate() error {
	r.Database = strings.TrimSpace(r.Database)
	r.Keyword = strings.TrimSpace(r.Keyword)
	r.Page, r.PageSize = normalizePageWithMax(r.Page, r.PageSize, defaultPageSize, runtimeConfigMaxPageSize)
	return nil
}

// SaveRuntimeArchiveJobReq 保存归档任务草稿。
type SaveRuntimeArchiveJobReq struct {
	ID                      uint64 `json:"id,optional"`                      // 草稿 ID；为空时新增
	Enabled                 bool   `json:"enabled"`                          // 是否启用
	Name                    string `json:"name"`                             // 归档任务名称
	Database                string `json:"database"`                         // 热表数据库
	TableName               string `json:"tableName"`                        // 热表名
	TimeColumn              string `json:"timeColumn,optional"`              // 归档时间列
	TimeColumnType          string `json:"timeColumnType,optional"`          // 时间列类型
	TimeColumnFormat        string `json:"timeColumnFormat,optional"`        // 字符串时间格式
	TimeColumnUnixUnit      string `json:"timeColumnUnixUnit,optional"`      // Unix 时间单位
	PrimaryKey              string `json:"primaryKey,optional"`              // 主键列
	ArchiveCondition        string `json:"archiveCondition,optional"`        // 归档过滤条件
	DeleteCondition         string `json:"deleteCondition,optional"`         // 清理过滤条件
	SplitUnit               string `json:"splitUnit,optional"`               // 历史表拆分粒度
	CustomDays              int    `json:"customDays,optional"`              // 自定义分段天数
	HotKeepDays             int    `json:"hotKeepDays,optional"`             // 热表保留天数
	ArchiveDelayDays        int    `json:"archiveDelayDays,optional"`        // 归档延迟天数
	ArchiveWindowSeconds    int    `json:"archiveWindowSeconds,optional"`    // 归档窗口秒数
	ArchiveWindowMode       string `json:"archiveWindowMode,optional"`       // 归档窗口模式
	ArchiveMaxWindowsPerRun int    `json:"archiveMaxWindowsPerRun,optional"` // 单次最大归档窗口数
	ArchiveAutoMaxWindows   int    `json:"archiveAutoMaxWindows,optional"`   // auto 最大追赶窗口数
	ArchiveAutoLightRows    int    `json:"archiveAutoLightRows,optional"`    // auto 轻量行数阈值
	ArchiveAutoLightMs      int    `json:"archiveAutoLightMs,optional"`      // auto 轻量耗时阈值毫秒
	DeleteDisabled          bool   `json:"deleteDisabled,optional"`          // 是否禁用删除
	DeleteDelayDays         int    `json:"deleteDelayDays,optional"`         // 删除延迟天数
	DeleteWindowSeconds     int    `json:"deleteWindowSeconds,optional"`     // 删除窗口秒数
	DeleteMaxWindowsPerRun  int    `json:"deleteMaxWindowsPerRun,optional"`  // 单次最大删除窗口数
	BatchSize               int    `json:"batchSize,optional"`               // 归档批次大小
	DeleteBatchSize         int    `json:"deleteBatchSize,optional"`         // 删除批次大小
	MaxHistoryTables        int    `json:"maxHistoryTables,optional"`        // 最大历史表数量
	HistoryTablePrefix      string `json:"historyTablePrefix,optional"`      // 历史表前缀
	HistoryTableNameRule    string `json:"historyTableNameRule,optional"`    // 历史表命名规则
	StartAt                 string `json:"startAt,optional"`                 // 首次归档起点
	QueryWriteDB            bool   `json:"queryWriteDb,optional"`            // 查询是否强制走主库
	SortOrder               int    `json:"sortOrder,optional"`               // 排序值
	Remark                  string `json:"remark,optional"`                  // 备注
}

// Validate 校验并归一化归档任务保存参数。
func (r *SaveRuntimeArchiveJobReq) Validate() error {
	r.Name = strings.TrimSpace(r.Name)
	r.Database = strings.TrimSpace(r.Database)
	r.TableName = strings.TrimSpace(r.TableName)
	r.TimeColumn = strings.TrimSpace(r.TimeColumn)
	r.TimeColumnType = strings.TrimSpace(r.TimeColumnType)
	r.TimeColumnFormat = strings.TrimSpace(r.TimeColumnFormat)
	r.TimeColumnUnixUnit = strings.TrimSpace(r.TimeColumnUnixUnit)
	r.PrimaryKey = strings.TrimSpace(r.PrimaryKey)
	r.ArchiveWindowMode = strings.TrimSpace(r.ArchiveWindowMode)
	r.SplitUnit = strings.TrimSpace(r.SplitUnit)
	r.HistoryTablePrefix = strings.TrimSpace(r.HistoryTablePrefix)
	r.HistoryTableNameRule = strings.TrimSpace(r.HistoryTableNameRule)
	r.StartAt = strings.TrimSpace(r.StartAt)
	r.Remark = strings.TrimSpace(r.Remark)
	if r.Name == "" {
		return errors.Errorf("name 不能为空")
	}
	if r.Database == "" {
		r.Database = "main"
	}
	if r.TableName == "" {
		return errors.Errorf("tableName 不能为空")
	}
	if runtimeArchiveNegative(r) {
		return errors.Errorf("归档数值参数不能小于 0")
	}
	return nil
}

// RuntimeConfigIDReq 表示基于 ID 的运行配置操作请求。
type RuntimeConfigIDReq struct {
	ID uint64 `path:"id"` // 记录 ID
}

// Validate 校验 ID 参数。
func (r *RuntimeConfigIDReq) Validate() error {
	if r.ID == 0 {
		return errors.Errorf("id 不能为空")
	}
	return nil
}

// RuntimeConfigReleaseIDReq 表示基于发布 ID 的操作请求。
type RuntimeConfigReleaseIDReq struct {
	ReleaseID uint64 `path:"releaseId"` // 发布 ID
}

// Validate 校验发布 ID 参数。
func (r *RuntimeConfigReleaseIDReq) Validate() error {
	if r.ReleaseID == 0 {
		return errors.Errorf("releaseId 不能为空")
	}
	return nil
}

// RuntimeConfigPublishReq 发布运行配置草稿。
type RuntimeConfigPublishReq struct {
	Remark       string `json:"remark,optional"`       // 发布备注
	TwoStepKey   string `json:"twoStepKey,optional"`   // MFA 二次票据 key
	TwoStepValue string `json:"twoStepValue,optional"` // MFA 二次票据 value
}

// Validate 校验发布参数。
func (r *RuntimeConfigPublishReq) Validate() error {
	r.Remark = strings.TrimSpace(r.Remark)
	r.TwoStepKey = strings.TrimSpace(r.TwoStepKey)
	r.TwoStepValue = strings.TrimSpace(r.TwoStepValue)
	return nil
}

// RuntimeConfigRollbackReq 回滚到指定发布快照。
type RuntimeConfigRollbackReq struct {
	ReleaseID    uint64 `json:"releaseId"`             // 目标发布 ID
	Remark       string `json:"remark,optional"`       // 回滚备注
	TwoStepKey   string `json:"twoStepKey,optional"`   // MFA 二次票据 key
	TwoStepValue string `json:"twoStepValue,optional"` // MFA 二次票据 value
}

// Validate 校验回滚参数。
func (r *RuntimeConfigRollbackReq) Validate() error {
	r.Remark = strings.TrimSpace(r.Remark)
	r.TwoStepKey = strings.TrimSpace(r.TwoStepKey)
	r.TwoStepValue = strings.TrimSpace(r.TwoStepValue)
	if r.ReleaseID == 0 {
		return errors.Errorf("releaseId 不能为空")
	}
	return nil
}

// RuntimeConfigValidateResp 表示运行配置预检结果。
type RuntimeConfigValidateResp struct {
	Valid    bool     `json:"valid"`    // 是否通过预检
	Messages []string `json:"messages"` // 预检信息列表
	Checksum string   `json:"checksum"` // 草稿快照 SHA256
}

// RuntimeConfigPublishResp 表示发布和回滚的回执。
type RuntimeConfigPublishResp struct {
	ReleaseID       uint64 `json:"releaseId"`       // 新发布 ID
	VersionNo       uint64 `json:"versionNo"`       // 新版本号
	Checksum        string `json:"checksum"`        // 快照 SHA256
	Applied         bool   `json:"applied"`         // 当前实例是否已完成运行态应用
	RestartRequired bool   `json:"restartRequired"` // 是否需要重启才能完全生效
	RestartReason   string `json:"restartReason"`   // 重启原因
}

// RuntimeConfigReleaseQueryReq 查询发布历史。
type RuntimeConfigReleaseQueryReq struct {
	GetPageReq // GetPageReq 表示分页参数。
}

// Validate 校验发布历史查询参数。
func (r *RuntimeConfigReleaseQueryReq) Validate() error {
	return r.GetPageReq.Validate()
}

// RuntimeConfigReleaseItem 表示发布历史列表项。
type RuntimeConfigReleaseItem struct {
	ID                 uint64 `json:"id"`                 // 发布 ID
	VersionNo          uint64 `json:"versionNo"`          // 发布版本号
	Checksum           string `json:"checksum"`           // 快照 SHA256
	BaseReleaseID      uint64 `json:"baseReleaseId"`      // 来源发布 ID
	RestartRequired    bool   `json:"restartRequired"`    // 是否需要重启
	RestartReason      string `json:"restartReason"`      // 重启原因
	Remark             string `json:"remark"`             // 发布备注
	PublishedByAdminID int    `json:"publishedByAdminId"` // 发布管理员 ID
	PublishedByName    string `json:"publishedByName"`    // 发布管理员账号
	PublishedAt        string `json:"publishedAt"`        // 发布时间
}

// RuntimeConfigReleaseDetailResp 表示发布快照详情。
type RuntimeConfigReleaseDetailResp struct {
	RuntimeConfigReleaseItem        // RuntimeConfigReleaseItem 表示发布历史基础信息。
	SnapshotJSON             string `json:"snapshotJson"` // 发布快照 JSON
	SnapshotYAML             string `json:"snapshotYaml"` // 发布快照 YAML
}

// RuntimeTaskPeriodicItem 表示周期任务配置项。
type RuntimeTaskPeriodicItem struct {
	ID               uint64   `json:"id"`               // 草稿 ID；发布快照中为 0
	Enabled          bool     `json:"enabled"`          // 是否启用
	Name             string   `json:"name"`             // 周期任务名称
	Cron             string   `json:"cron"`             // cron 表达式
	EverySeconds     int      `json:"everySeconds"`     // 固定间隔秒数
	Workflow         string   `json:"workflow"`         // 工作流名称
	Queue            string   `json:"queue"`            // 投递队列
	Targets          []string `json:"targets"`          // 执行目标列表
	ShardTotal       int      `json:"shardTotal"`       // 分片总数
	GrayPercent      int      `json:"grayPercent"`      // 灰度比例
	Retry            int      `json:"retry"`            // 覆盖重试次数
	TimeoutSeconds   int      `json:"timeoutSeconds"`   // 任务超时秒数
	Deadline         string   `json:"deadline"`         // 截止时间 RFC3339
	UniqueKey        string   `json:"uniqueKey"`        // 去重键
	UniqueTTLSeconds int      `json:"uniqueTtlSeconds"` // 去重 TTL 秒数
	SortOrder        int      `json:"sortOrder"`        // 排序值
	Remark           string   `json:"remark"`           // 备注
	CreatedAt        string   `json:"createdAt"`        // 创建时间
	UpdatedAt        string   `json:"updatedAt"`        // 更新时间
}

// RuntimeArchiveJobItem 表示归档任务配置项。
type RuntimeArchiveJobItem struct {
	ID                      uint64 `json:"id"`                      // 草稿 ID；发布快照中为 0
	Enabled                 bool   `json:"enabled"`                 // 是否启用
	Name                    string `json:"name"`                    // 归档任务名称
	Database                string `json:"database"`                // 热表数据库
	TableName               string `json:"tableName"`               // 热表名
	TimeColumn              string `json:"timeColumn"`              // 归档时间列
	TimeColumnType          string `json:"timeColumnType"`          // 时间列类型
	TimeColumnFormat        string `json:"timeColumnFormat"`        // 字符串时间格式
	TimeColumnUnixUnit      string `json:"timeColumnUnixUnit"`      // Unix 时间单位
	PrimaryKey              string `json:"primaryKey"`              // 主键列
	ArchiveCondition        string `json:"archiveCondition"`        // 归档过滤条件
	DeleteCondition         string `json:"deleteCondition"`         // 清理过滤条件
	SplitUnit               string `json:"splitUnit"`               // 历史表拆分粒度
	CustomDays              int    `json:"customDays"`              // 自定义分段天数
	HotKeepDays             int    `json:"hotKeepDays"`             // 热表保留天数
	ArchiveDelayDays        int    `json:"archiveDelayDays"`        // 归档延迟天数
	ArchiveWindowSeconds    int    `json:"archiveWindowSeconds"`    // 归档窗口秒数
	ArchiveWindowMode       string `json:"archiveWindowMode"`       // 归档窗口模式
	ArchiveMaxWindowsPerRun int    `json:"archiveMaxWindowsPerRun"` // 单次最大归档窗口数
	ArchiveAutoMaxWindows   int    `json:"archiveAutoMaxWindows"`   // auto 最大追赶窗口数
	ArchiveAutoLightRows    int    `json:"archiveAutoLightRows"`    // auto 轻量行数阈值
	ArchiveAutoLightMs      int    `json:"archiveAutoLightMs"`      // auto 轻量耗时阈值毫秒
	DeleteDisabled          bool   `json:"deleteDisabled"`          // 是否禁用删除
	DeleteDelayDays         int    `json:"deleteDelayDays"`         // 删除延迟天数
	DeleteWindowSeconds     int    `json:"deleteWindowSeconds"`     // 删除窗口秒数
	DeleteMaxWindowsPerRun  int    `json:"deleteMaxWindowsPerRun"`  // 单次最大删除窗口数
	BatchSize               int    `json:"batchSize"`               // 归档批次大小
	DeleteBatchSize         int    `json:"deleteBatchSize"`         // 删除批次大小
	MaxHistoryTables        int    `json:"maxHistoryTables"`        // 最大历史表数量
	HistoryTablePrefix      string `json:"historyTablePrefix"`      // 历史表前缀
	HistoryTableNameRule    string `json:"historyTableNameRule"`    // 历史表命名规则
	StartAt                 string `json:"startAt"`                 // 首次归档起点
	QueryWriteDB            bool   `json:"queryWriteDb"`            // 查询是否强制走主库
	SortOrder               int    `json:"sortOrder"`               // 排序值
	Remark                  string `json:"remark"`                  // 备注
	CreatedAt               string `json:"createdAt"`               // 创建时间
	UpdatedAt               string `json:"updatedAt"`               // 更新时间
}

// RuntimeArchiveProgressResp 表示归档任务执行详情。
type RuntimeArchiveProgressResp struct {
	JobID              uint64                       `json:"jobId"`              // 归档任务草稿 ID
	JobName            string                       `json:"jobName"`            // 归档任务名
	RuntimeMatched     bool                         `json:"runtimeMatched"`     // 当前运行态是否存在同名任务
	RuntimeEnabled     bool                         `json:"runtimeEnabled"`     // 当前运行态归档模块和同名任务是否均已启用
	SchemaReady        bool                         `json:"schemaReady"`        // 归档水位表和区间表是否均已创建
	Phase              string                       `json:"phase"`              // 当前执行阶段
	WatermarkTime      string                       `json:"watermarkTime"`      // 已完整复制到历史表的排他上界；无水位时为空
	WatermarkUpdatedAt string                       `json:"watermarkUpdatedAt"` // 水位最近更新时间；无水位时为空
	EligibleUntil      string                       `json:"eligibleUntil"`      // 当前允许归档到的排他上界；任务未启用时为空
	PlannedUntil       string                       `json:"plannedUntil"`       // 已规划区间的最远排他上界；无区间时为空
	LagSeconds         *int64                       `json:"lagSeconds"`         // 有可靠水位或区间基线时的滞后秒数；无法计算时为空
	Counts             RuntimeArchiveProgressCounts `json:"counts"`             // 各区间状态数量
	CurrentSegment     *RuntimeArchiveSegmentItem   `json:"currentSegment"`     // 当前活动或最近租约过期的执行区间；无执行记录时为空
	RecentSegments     []RuntimeArchiveSegmentItem  `json:"recentSegments"`     // 最近区间按起点倒序排列，最多 20 条
	FetchedAt          string                       `json:"fetchedAt"`          // 本次运行态快照生成时间
}

// RuntimeArchiveProgressCounts 表示归档区间状态数量。
type RuntimeArchiveProgressCounts struct {
	Total    int64 `json:"total"`    // 区间总数
	Pending  int64 `json:"pending"`  // 待领取区间数
	Running  int64 `json:"running"`  // 正在归档区间数
	Done     int64 `json:"done"`     // 已归档待删除区间数
	Deleting int64 `json:"deleting"` // 正在删除区间数
	Deleted  int64 `json:"deleted"`  // 已完成热表删除区间数
	Failed   int64 `json:"failed"`   // 归档失败待重试区间数
}

// RuntimeArchiveSegmentItem 表示单个归档区间的执行详情。
type RuntimeArchiveSegmentItem struct {
	ID                       uint64   `json:"id"`                       // 区间 ID
	HistoryTableName         string   `json:"historyTableName"`         // 历史表名
	RangeStart               string   `json:"rangeStart"`               // 区间起点（含）
	RangeEnd                 string   `json:"rangeEnd"`                 // 区间终点（不含）
	Status                   string   `json:"status"`                   // 区间状态
	WorkerID                 string   `json:"workerId"`                 // 当前持有 worker；非执行态为空
	LeaseExpiresAt           string   `json:"leaseExpiresAt"`           // 当前租约过期时间；非执行态为空
	LastArchivedID           string   `json:"lastArchivedId"`           // 最近归档主键游标；字符串避免前端整数精度丢失
	LastArchivedTime         string   `json:"lastArchivedTime"`         // 最近归档时间游标；尚未推进时为空
	RowsArchived             int64    `json:"rowsArchived"`             // 累计归档行数
	AttemptCount             int      `json:"attemptCount"`             // 领取次数
	UpdatedAt                string   `json:"updatedAt"`                // 最近更新时间
	CompletedAt              string   `json:"completedAt"`              // 归档完成时间；未完成时为空
	EstimatedProgressPercent *float64 `json:"estimatedProgressPercent"` // 复制阶段按时间游标估算的区间进度百分比；非复制阶段为空
}

// runtimeArchiveNegative 判断归档请求中是否存在负数运行参数。
func runtimeArchiveNegative(r *SaveRuntimeArchiveJobReq) bool {
	return r.CustomDays < 0 || r.HotKeepDays < 0 || r.ArchiveDelayDays < 0 ||
		r.ArchiveWindowSeconds < 0 || r.ArchiveMaxWindowsPerRun < 0 ||
		r.ArchiveAutoMaxWindows < 0 || r.ArchiveAutoLightRows < 0 ||
		r.ArchiveAutoLightMs < 0 || r.DeleteDelayDays < 0 ||
		r.DeleteWindowSeconds < 0 || r.DeleteMaxWindowsPerRun < 0 ||
		r.BatchSize < 0 || r.DeleteBatchSize < 0 || r.MaxHistoryTables < 0
}
