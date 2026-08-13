package runtimeconfig

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	codes "admin/common/codes"
	i18n "admin/common/i18n"
	keys "admin/common/rediskeys"
	"admin/internal/config"
	corelogic "admin/internal/logic"
	adminlogic "admin/internal/logic/admin"
	cachelogic "admin/internal/logic/cache"
	securitylogic "admin/internal/logic/security"
	"admin/internal/model"
	"admin/internal/svc"
	tasklimits "admin/internal/task/limits"
	"admin/internal/types"

	"github.com/Is999/go-utils/errors"
	tablecache "github.com/Is999/table-cache"
	yaml "go.yaml.in/yaml/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	// SourceFile 表示运行期大列表配置继续来自 YAML 文件。
	SourceFile = "file"
	// SourceDatabase 表示运行期大列表配置来自数据库发布快照。
	SourceDatabase = "database"
	// defaultPollIntervalSeconds 表示 DB 模式轻量版本轮询默认间隔。
	defaultPollIntervalSeconds = 30
	// minPollIntervalSeconds 表示 DB 模式轮询最小间隔，避免配置错误刷 Redis。
	minPollIntervalSeconds = 5
	// runtimeConfigDraftWriteBatchSize 限制草稿覆盖时每批写入二百条，避免一万条快照产生逐行往返或超大单条 SQL。
	runtimeConfigDraftWriteBatchSize = 200
	// maxRuntimeConfigSnapshotBytes 把 JSON/YAML 单份快照限制在十五 MiB，低于 MySQL MEDIUMTEXT 上限并限制 Redis 回源大对象。
	maxRuntimeConfigSnapshotBytes = 15 << 20
)

var (
	// errRuntimeConfigCountLimit 标识草稿新增达到业务总量上限，调用层据此返回参数错误而不是数据库故障。
	errRuntimeConfigCountLimit = errors.New("运行配置草稿数量达到上限")
	// errRuntimeConfigInitialReleaseExists 表示其它实例已先完成首次发布，当前实例应加载现有 active 版本而不是重复发布。
	errRuntimeConfigInitialReleaseExists = errors.New("运行配置初始版本已由其它实例发布")
)

// ReleaseSnapshot 是运行态发布快照，只包含数据库化的大列表配置。
type ReleaseSnapshot struct {
	ArchiveJobs  []config.ArchiveJobConfig   `json:"archive_jobs" yaml:"archive_jobs"`   // 归档任务列表
	TaskPeriodic []config.TaskPeriodicConfig `json:"task_periodic" yaml:"task_periodic"` // 周期任务列表
}

// preparedRuntimeConfigSnapshot 保存已经完成归一化、校验和序列化的发布内容，事务内只负责原子写入草稿与版本状态。
type preparedRuntimeConfigSnapshot struct {
	Snapshot ReleaseSnapshot // 归一化后的周期任务和归档任务快照
	JSON     string          // 写入发布表和 table-cache 的 JSON 快照
	YAML     string          // 提供给运维查看的 YAML 快照
	Checksum string          // 按 JSON 快照计算的 SHA256
}

// StateCache 是 table-cache 中运行配置 active 版本状态。
type StateCache struct {
	ActiveReleaseID uint64 `json:"active_release_id,string"` // 当前发布 ID，按 Redis Hash 字符串读取
	ActiveVersion   uint64 `json:"active_version,string"`    // 当前版本号，按 Redis Hash 字符串读取
	ActiveChecksum  string `json:"active_checksum"`          // 当前快照 SHA256
	PublishedAtUnix int64  `json:"published_at_unix,string"` // 最近发布时间戳，按 Redis Hash 字符串读取
}

// ActiveRelease 保存一次 active 版本和对应快照。
type ActiveRelease struct {
	State    StateCache                 // 当前 active 状态
	Release  model.RuntimeConfigRelease // 发布记录
	Snapshot ReleaseSnapshot            // 发布快照
}

// RuntimeConfigLogic 封装运行期大列表配置管理逻辑。
type RuntimeConfigLogic struct {
	*corelogic.BaseLogic // 复用上下文、DB、Redis、审计和管理员上下文能力
}

// NewRuntimeConfigLogic 创建运行配置管理逻辑对象。
func NewRuntimeConfigLogic(r *http.Request, svcCtx *svc.ServiceContext) *RuntimeConfigLogic {
	return &RuntimeConfigLogic{BaseLogic: corelogic.NewBaseLogic(r, svcCtx)}
}

// NewRuntimeConfigLogicWithContext 创建绑定上下文的运行配置管理逻辑对象。
func NewRuntimeConfigLogicWithContext(ctx context.Context, svcCtx *svc.ServiceContext) *RuntimeConfigLogic {
	return &RuntimeConfigLogic{BaseLogic: corelogic.NewBaseLogicWithContext(ctx, svcCtx)}
}

// NormalizeSource 归一化运行配置来源。
func NormalizeSource(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case SourceDatabase:
		return SourceDatabase
	default:
		return SourceFile
	}
}

// IsDatabaseSource 判断当前配置是否启用数据库发布快照。
func IsDatabaseSource(cfg config.Config) bool {
	return NormalizeSource(cfg.RuntimeConfig.Source) == SourceDatabase
}

// PollIntervalSeconds 返回数据库模式轻量版本轮询间隔秒数。
func PollIntervalSeconds(cfg config.Config) int {
	if cfg.RuntimeConfig.PollIntervalSeconds <= 0 {
		return defaultPollIntervalSeconds
	}
	if cfg.RuntimeConfig.PollIntervalSeconds < minPollIntervalSeconds {
		return minPollIntervalSeconds
	}
	return cfg.RuntimeConfig.PollIntervalSeconds
}

// CurrentSnapshotFromConfig 从当前配置提取数据库化的大列表片段。
func CurrentSnapshotFromConfig(cfg config.Config) ReleaseSnapshot {
	return normalizeReleaseSnapshot(ReleaseSnapshot{
		ArchiveJobs:  append([]config.ArchiveJobConfig(nil), cfg.Archive.Jobs...),
		TaskPeriodic: append([]config.TaskPeriodicConfig(nil), cfg.Task.Periodic...),
	})
}

// runtimeConfigSnapshotEmpty 判断发布快照是否未包含任何运行期大列表配置。
func runtimeConfigSnapshotEmpty(snapshot ReleaseSnapshot) bool {
	return len(snapshot.ArchiveJobs) == 0 && len(snapshot.TaskPeriodic) == 0
}

// ApplySnapshot 把发布快照覆盖到完整配置；不会修改 YAML-only 段。
func ApplySnapshot(cfg *config.Config, snapshot ReleaseSnapshot) {
	if cfg == nil {
		return
	}
	snapshot = normalizeReleaseSnapshot(snapshot)
	cfg.Archive.Jobs = append([]config.ArchiveJobConfig(nil), snapshot.ArchiveJobs...)
	cfg.Task.Periodic = append([]config.TaskPeriodicConfig(nil), snapshot.TaskPeriodic...)
}

// DecodeSnapshotJSON 解析发布快照 JSON。
func DecodeSnapshotJSON(snapshotJSON string) (ReleaseSnapshot, error) {
	var snapshot ReleaseSnapshot
	if err := json.Unmarshal([]byte(snapshotJSON), &snapshot); err != nil {
		return snapshot, errors.Tag(err)
	}
	return snapshot, nil
}

// EncodeSnapshot 生成发布快照 JSON、YAML 和校验和。
func EncodeSnapshot(snapshot ReleaseSnapshot) (string, string, string, error) {
	jsonBytes, err := json.Marshal(snapshot)
	if err != nil {
		return "", "", "", errors.Tag(err)
	}
	if len(jsonBytes) > maxRuntimeConfigSnapshotBytes {
		return "", "", "", errors.Errorf("运行配置 JSON 快照不能超过 %d 字节", maxRuntimeConfigSnapshotBytes)
	}
	yamlBytes, err := yaml.Marshal(snapshot)
	if err != nil {
		return "", "", "", errors.Tag(err)
	}
	if len(yamlBytes) > maxRuntimeConfigSnapshotBytes {
		return "", "", "", errors.Errorf("运行配置 YAML 快照不能超过 %d 字节", maxRuntimeConfigSnapshotBytes)
	}
	return string(jsonBytes), string(yamlBytes), sha256Hex(jsonBytes), nil
}

// encodeReleaseSnapshot 补齐发布默认值后生成快照文本和校验和，供概览、预检和发布复用同一口径。
func encodeReleaseSnapshot(snapshot ReleaseSnapshot) (ReleaseSnapshot, string, string, string, error) {
	normalized := normalizeReleaseSnapshot(snapshot)
	jsonText, yamlText, checksum, err := EncodeSnapshot(normalized)
	if err != nil {
		return normalized, "", "", "", errors.Tag(err)
	}
	return normalized, jsonText, yamlText, checksum, nil
}

// LoadActiveSnapshotCached 从 table-cache 读取当前 active 发布快照。
func LoadActiveSnapshotCached(ctx context.Context, svcCtx *svc.ServiceContext) (*ActiveRelease, error) {
	logicObj := NewRuntimeConfigLogicWithContext(ctx, svcCtx)
	return logicObj.loadActiveSnapshotCached()
}

// EnsureInitialRelease 确保 DB 模式首次启动时已有 active 发布快照；初始内容只允许来自数据库初始化草稿表。
func EnsureInitialRelease(ctx context.Context, svcCtx *svc.ServiceContext) (*ActiveRelease, error) {
	logicObj := NewRuntimeConfigLogicWithContext(ctx, svcCtx)
	state, err := logicObj.loadActiveStateCached()
	if err != nil {
		return nil, errors.Tag(err)
	}
	if state.ActiveReleaseID != 0 {
		return logicObj.loadActiveSnapshotCached()
	}
	if _, err = logicObj.publishInitialDraft(); err != nil {
		if errors.Is(err, errRuntimeConfigInitialReleaseExists) {
			return logicObj.loadActiveSnapshotCached()
		}
		if active, loadErr := logicObj.loadActiveSnapshotCached(); loadErr == nil {
			return active, nil
		}
		return nil, errors.Wrap(err, "发布运行配置初始版本失败")
	}
	return logicObj.loadActiveSnapshotCached()
}

// LoadActiveStateCached 从 table-cache 读取当前 active 版本状态，不触碰发布快照。
func LoadActiveStateCached(ctx context.Context, svcCtx *svc.ServiceContext) (StateCache, error) {
	logicObj := NewRuntimeConfigLogicWithContext(ctx, svcCtx)
	return logicObj.loadActiveStateCached()
}

// Overview 查询运行配置来源、active 版本和草稿数量；全量快照只在调用方显式请求时读取和序列化。
func (l *RuntimeConfigLogic) Overview(req *types.RuntimeConfigOverviewReq) *types.BizResult {
	cfg := l.currentConfig()
	state, err := l.loadActiveStateCached()
	if err != nil {
		return types.ServerError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.Overview 读取 active 状态失败").ToBizResult()
	}
	periodicCount, archiveCount, err := l.draftCounts()
	if err != nil {
		return types.DBError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.Overview 查询草稿数量失败").ToBizResult()
	}
	resp := &types.RuntimeConfigOverviewResp{
		Source:              NormalizeSource(cfg.RuntimeConfig.Source),
		PollIntervalSeconds: PollIntervalSeconds(cfg),
		State:               stateCacheToItem(state),
		Draft: types.RuntimeConfigDraftCount{
			PeriodicTasks: periodicCount,
			ArchiveJobs:   archiveCount,
		},
		CurrentSnapshot: snapshotToResp(ReleaseSnapshot{}),
		DraftSnapshot:   snapshotToResp(ReleaseSnapshot{}),
	}
	if req == nil || !req.IncludeSnapshots {
		return types.NewBizResult(codes.FetchSuccess).WithData(resp)
	}
	draftSnapshot, err := l.buildDraftSnapshot()
	if err != nil {
		return types.DBError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.Overview 读取草稿快照失败").ToBizResult()
	}
	draftSnapshot, _, _, draftChecksum, err := encodeReleaseSnapshot(draftSnapshot)
	if err != nil {
		return types.ServerError(i18n.MsgKeyInternalError, err, "RuntimeConfigLogic.Overview 生成草稿快照失败").ToBizResult()
	}
	activeChecksum := strings.TrimSpace(state.ActiveChecksum)
	resp.CurrentSnapshot = snapshotToResp(CurrentSnapshotFromConfig(cfg))
	resp.DraftSnapshot = snapshotToResp(draftSnapshot)
	resp.DraftChecksum = draftChecksum
	resp.DraftChanged = draftChecksum != activeChecksum
	return types.NewBizResult(codes.FetchSuccess).WithData(resp)
}

// ListPeriodicTasks 分页查询周期任务草稿。
func (l *RuntimeConfigLogic) ListPeriodicTasks(req *types.RuntimeTaskPeriodicQueryReq) *types.BizResult {
	if err := req.Validate(); err != nil {
		return types.ParamError(err).ToBizResult()
	}
	db, err := l.writeDB()
	if err != nil {
		return types.DBError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.ListPeriodicTasks DB未初始化").ToBizResult()
	}
	q := db.Model(&model.RuntimeTaskPeriodic{})
	if req.Workflow != "" {
		q = q.Where("workflow = ?", req.Workflow)
	}
	if req.Enabled != nil {
		q = q.Where("enabled = ?", *req.Enabled)
	}
	if req.Keyword != "" {
		keyword := "%" + req.Keyword + "%"
		q = q.Where("name LIKE ? OR queue LIKE ?", keyword, keyword)
	}
	var total int64
	if err = q.Count(&total).Error; err != nil {
		return types.DBError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.ListPeriodicTasks Count失败").ToBizResult()
	}
	var rows []model.RuntimeTaskPeriodic
	if total > 0 {
		err = q.Order("sort_order ASC").Order("id ASC").
			Limit(req.PageSize).Offset((req.Page - 1) * req.PageSize).
			Find(&rows).Error
		if err != nil {
			return types.DBError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.ListPeriodicTasks 查询失败").ToBizResult()
		}
	}
	items := make([]types.RuntimeTaskPeriodicItem, 0, len(rows))
	for _, row := range rows {
		items = append(items, periodicModelToItem(row))
	}
	return types.NewBizResult(codes.FetchSuccess).WithData(&types.ListResp[types.RuntimeTaskPeriodicItem]{List: items, Total: total})
}

// SavePeriodicTask 保存周期任务草稿。
func (l *RuntimeConfigLogic) SavePeriodicTask(req *types.SaveRuntimeTaskPeriodicReq) *types.BizResult {
	if err := req.Validate(); err != nil {
		return types.ParamError(err).ToBizResult()
	}
	db, err := l.writeDB()
	if err != nil {
		return types.DBError(i18n.MsgKeySaveFail, err, "RuntimeConfigLogic.SavePeriodicTask DB未初始化").ToBizResult()
	}
	adminID := l.adminID()
	row := periodicReqToModel(req, adminID)
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := l.lockRuntimeConfigDraftTx(tx); err != nil {
			return errors.Tag(err)
		}
		if req.ID > 0 {
			row.ID = req.ID
			result := tx.Model(&model.RuntimeTaskPeriodic{}).
				Where("id = ?", req.ID).
				Updates(periodicModelUpdateMap(row))
			return checkRuntimeConfigUpdated(result, req.ID, "周期任务草稿")
		}
		if err := l.ensureDraftCapacityTx(tx, &model.RuntimeTaskPeriodic{}, tasklimits.MaxPeriodicCount, "周期任务"); err != nil {
			return errors.Tag(err)
		}
		return errors.Tag(tx.Create(&row).Error)
	})
	if err != nil {
		if errors.Is(err, errRuntimeConfigCountLimit) {
			return types.ParamError(err).ToBizResult()
		}
		return types.DBError(i18n.MsgKeySaveFail, err, "RuntimeConfigLogic.SavePeriodicTask 保存失败").ToBizResult()
	}
	return types.NewBizResult(codes.SaveSuccess).SetI18nMessage(i18n.MsgKeySaveSuccess)
}

// DeletePeriodicTask 删除周期任务草稿。
func (l *RuntimeConfigLogic) DeletePeriodicTask(req *types.RuntimeConfigIDReq) *types.BizResult {
	if err := req.Validate(); err != nil {
		return types.ParamError(err).ToBizResult()
	}
	if err := l.deleteByID(&model.RuntimeTaskPeriodic{}, req.ID); err != nil {
		return types.DBError(i18n.MsgKeyDeleteFail, err, "RuntimeConfigLogic.DeletePeriodicTask 删除失败").ToBizResult()
	}
	return types.NewBizResult(codes.DeleteSuccess).SetI18nMessage(i18n.MsgKeyDeleteSuccess)
}

// ListArchiveJobs 分页查询归档任务草稿。
func (l *RuntimeConfigLogic) ListArchiveJobs(req *types.RuntimeArchiveJobQueryReq) *types.BizResult {
	if err := req.Validate(); err != nil {
		return types.ParamError(err).ToBizResult()
	}
	db, err := l.writeDB()
	if err != nil {
		return types.DBError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.ListArchiveJobs DB未初始化").ToBizResult()
	}
	q := db.Model(&model.RuntimeArchiveJob{})
	if req.Enabled != nil {
		q = q.Where("enabled = ?", *req.Enabled)
	}
	if req.Database != "" {
		q = q.Where("database_name = ?", req.Database)
	}
	if req.Keyword != "" {
		keyword := "%" + req.Keyword + "%"
		q = q.Where("name LIKE ? OR table_name LIKE ?", keyword, keyword)
	}
	var total int64
	if err = q.Count(&total).Error; err != nil {
		return types.DBError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.ListArchiveJobs Count失败").ToBizResult()
	}
	var rows []model.RuntimeArchiveJob
	if total > 0 {
		err = q.Order("sort_order ASC").Order("id ASC").
			Limit(req.PageSize).Offset((req.Page - 1) * req.PageSize).
			Find(&rows).Error
		if err != nil {
			return types.DBError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.ListArchiveJobs 查询失败").ToBizResult()
		}
	}
	items := make([]types.RuntimeArchiveJobItem, 0, len(rows))
	for _, row := range rows {
		items = append(items, archiveModelToItem(row))
	}
	return types.NewBizResult(codes.FetchSuccess).WithData(&types.ListResp[types.RuntimeArchiveJobItem]{List: items, Total: total})
}

// SaveArchiveJob 保存归档任务草稿。
func (l *RuntimeConfigLogic) SaveArchiveJob(req *types.SaveRuntimeArchiveJobReq) *types.BizResult {
	if err := req.Validate(); err != nil {
		return types.ParamError(err).ToBizResult()
	}
	db, err := l.writeDB()
	if err != nil {
		return types.DBError(i18n.MsgKeySaveFail, err, "RuntimeConfigLogic.SaveArchiveJob DB未初始化").ToBizResult()
	}
	adminID := l.adminID()
	row := archiveReqToModel(req, adminID)
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := l.lockRuntimeConfigDraftTx(tx); err != nil {
			return errors.Tag(err)
		}
		if req.ID > 0 {
			row.ID = req.ID
			result := tx.Model(&model.RuntimeArchiveJob{}).
				Where("id = ?", req.ID).
				Updates(archiveModelUpdateMap(row))
			return checkRuntimeConfigUpdated(result, req.ID, "归档任务草稿")
		}
		if err := l.ensureDraftCapacityTx(tx, &model.RuntimeArchiveJob{}, tasklimits.MaxArchiveJobCount, "归档任务"); err != nil {
			return errors.Tag(err)
		}
		return errors.Tag(tx.Create(&row).Error)
	})
	if err != nil {
		if errors.Is(err, errRuntimeConfigCountLimit) {
			return types.ParamError(err).ToBizResult()
		}
		return types.DBError(i18n.MsgKeySaveFail, err, "RuntimeConfigLogic.SaveArchiveJob 保存失败").ToBizResult()
	}
	return types.NewBizResult(codes.SaveSuccess).SetI18nMessage(i18n.MsgKeySaveSuccess)
}

// DeleteArchiveJob 删除归档任务草稿。
func (l *RuntimeConfigLogic) DeleteArchiveJob(req *types.RuntimeConfigIDReq) *types.BizResult {
	if err := req.Validate(); err != nil {
		return types.ParamError(err).ToBizResult()
	}
	if err := l.deleteByID(&model.RuntimeArchiveJob{}, req.ID); err != nil {
		return types.DBError(i18n.MsgKeyDeleteFail, err, "RuntimeConfigLogic.DeleteArchiveJob 删除失败").ToBizResult()
	}
	return types.NewBizResult(codes.DeleteSuccess).SetI18nMessage(i18n.MsgKeyDeleteSuccess)
}

// ValidateDraft 预检当前草稿并返回快照校验和。
func (l *RuntimeConfigLogic) ValidateDraft() *types.BizResult {
	snapshot, err := l.buildDraftSnapshot()
	if err != nil {
		return types.ServerError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.ValidateDraft 读取草稿失败").ToBizResult()
	}
	snapshot = normalizeReleaseSnapshot(snapshot)
	messages, err := l.validateSnapshot(snapshot)
	if err != nil {
		return types.NewBizResult(codes.Success).
			WithData(&types.RuntimeConfigValidateResp{Valid: false, Messages: append(messages, err.Error())})
	}
	_, _, _, checksum, err := encodeReleaseSnapshot(snapshot)
	if err != nil {
		return types.ServerError(i18n.MsgKeyInternalError, err, "RuntimeConfigLogic.ValidateDraft 生成快照失败").ToBizResult()
	}
	return types.NewBizResult(codes.Success).WithData(&types.RuntimeConfigValidateResp{Valid: true, Messages: messages, Checksum: checksum})
}

// Publish 发布当前草稿。
func (l *RuntimeConfigLogic) Publish(req *types.RuntimeConfigPublishReq) *types.BizResult {
	if err := req.Validate(); err != nil {
		return types.ParamError(err).ToBizResult()
	}
	if err := l.requireMFA(req.TwoStepKey, req.TwoStepValue); err != nil {
		return (&adminlogic.AdminLogic{BaseLogic: l.BaseLogic}).MFABizResult(err)
	}
	resp, err := l.publishDraft(req.Remark, 0)
	if err != nil {
		return types.ServerError(i18n.MsgKeySaveFail, err, "RuntimeConfigLogic.Publish 发布失败").ToBizResult()
	}
	return types.NewBizResult(codes.SaveSuccess).SetI18nMessage(i18n.MsgKeySaveSuccess).WithData(resp)
}

// Rollback 把指定历史快照写回草稿并发布为新版本。
func (l *RuntimeConfigLogic) Rollback(req *types.RuntimeConfigRollbackReq) *types.BizResult {
	if err := req.Validate(); err != nil {
		return types.ParamError(err).ToBizResult()
	}
	if err := l.requireMFA(req.TwoStepKey, req.TwoStepValue); err != nil {
		return (&adminlogic.AdminLogic{BaseLogic: l.BaseLogic}).MFABizResult(err)
	}
	release, snapshot, err := l.loadReleaseByID(req.ReleaseID)
	if err != nil {
		return types.ServerError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.Rollback 查询发布快照失败").ToBizResult()
	}
	remark := req.Remark
	if remark == "" {
		remark = fmt.Sprintf("rollback to release %d", req.ReleaseID)
	}
	resp, err := l.publishRollbackSnapshot(snapshot, remark, release.ID)
	if err != nil {
		return types.ServerError(i18n.MsgKeySaveFail, err, "RuntimeConfigLogic.Rollback 写回草稿并发布回滚版本失败").ToBizResult()
	}
	return types.NewBizResult(codes.SaveSuccess).SetI18nMessage(i18n.MsgKeySaveSuccess).WithData(resp)
}

// ListReleases 分页查询发布历史。
func (l *RuntimeConfigLogic) ListReleases(req *types.RuntimeConfigReleaseQueryReq) *types.BizResult {
	if err := req.Validate(); err != nil {
		return types.ParamError(err).ToBizResult()
	}
	db, err := l.writeDB()
	if err != nil {
		return types.DBError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.ListReleases DB未初始化").ToBizResult()
	}
	q := db.Model(&model.RuntimeConfigRelease{})
	var total int64
	if err = q.Count(&total).Error; err != nil {
		return types.DBError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.ListReleases Count失败").ToBizResult()
	}
	var rows []model.RuntimeConfigRelease
	if total > 0 {
		err = q.Order("version_no DESC").Limit(req.PageSize).Offset((req.Page - 1) * req.PageSize).Find(&rows).Error
		if err != nil {
			return types.DBError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.ListReleases 查询失败").ToBizResult()
		}
	}
	items := make([]types.RuntimeConfigReleaseItem, 0, len(rows))
	for _, row := range rows {
		items = append(items, releaseModelToItem(row))
	}
	return types.NewBizResult(codes.FetchSuccess).WithData(&types.ListResp[types.RuntimeConfigReleaseItem]{List: items, Total: total})
}

// GetRelease 查询发布快照详情。
func (l *RuntimeConfigLogic) GetRelease(req *types.RuntimeConfigReleaseIDReq) *types.BizResult {
	if err := req.Validate(); err != nil {
		return types.ParamError(err).ToBizResult()
	}
	release, _, err := l.loadReleaseByID(req.ReleaseID)
	if err != nil {
		return types.ServerError(i18n.MsgKeyQueryFail, err, "RuntimeConfigLogic.GetRelease 查询发布快照失败").ToBizResult()
	}
	item := releaseModelToItem(release)
	return types.NewBizResult(codes.FetchSuccess).WithData(&types.RuntimeConfigReleaseDetailResp{
		RuntimeConfigReleaseItem: item,
		SnapshotJSON:             release.SnapshotJSON,
		SnapshotYAML:             release.SnapshotYAML,
	})
}

// publishDraft 在状态行锁保护下读取草稿并发布，确保并发保存不能插入“读取草稿”和“更新 active 版本”之间。
func (l *RuntimeConfigLogic) publishDraft(remark string, baseReleaseID uint64) (*types.RuntimeConfigPublishResp, error) {
	return l.publishPreparedSnapshot(func(tx *gorm.DB) (preparedRuntimeConfigSnapshot, error) {
		snapshot, err := l.buildDraftSnapshotDB(tx)
		if err != nil {
			return preparedRuntimeConfigSnapshot{}, errors.Tag(err)
		}
		return l.prepareSnapshot(snapshot)
	}, nil, remark, baseReleaseID)
}

// publishInitialDraft 只在状态行锁内确认 active 仍为空后发布初始化草稿，避免多实例首次启动产生重复版本。
func (l *RuntimeConfigLogic) publishInitialDraft() (*types.RuntimeConfigPublishResp, error) {
	return l.publishPreparedSnapshot(func(tx *gorm.DB) (preparedRuntimeConfigSnapshot, error) {
		state, err := l.loadStateForUpdate(tx)
		if err != nil {
			return preparedRuntimeConfigSnapshot{}, errors.Tag(err)
		}
		if state.ActiveReleaseID != 0 {
			return preparedRuntimeConfigSnapshot{}, errRuntimeConfigInitialReleaseExists
		}
		snapshot, err := l.buildDraftSnapshotDB(tx)
		if err != nil {
			return preparedRuntimeConfigSnapshot{}, errors.Wrap(err, "读取运行配置初始草稿失败")
		}
		if runtimeConfigSnapshotEmpty(snapshot) {
			return preparedRuntimeConfigSnapshot{}, errors.Errorf("运行配置未发布 active release，且数据库初始化草稿表没有 task_periodic/archive_jobs")
		}
		return l.prepareSnapshot(snapshot)
	}, nil, "bootstrap publish runtime config draft", 0)
}

// publishRollbackSnapshot 在同一状态行锁和事务内覆盖草稿、写入回滚版本并切换 active，避免并发编辑拆开两个副作用。
func (l *RuntimeConfigLogic) publishRollbackSnapshot(snapshot ReleaseSnapshot, remark string, baseReleaseID uint64) (*types.RuntimeConfigPublishResp, error) {
	prepared, err := l.prepareSnapshot(snapshot)
	if err != nil {
		return nil, errors.Tag(err)
	}
	return l.publishPreparedSnapshot(func(*gorm.DB) (preparedRuntimeConfigSnapshot, error) {
		return prepared, nil
	}, func(tx *gorm.DB, normalized ReleaseSnapshot) error {
		return l.replaceDraftTx(tx, normalized)
	}, remark, baseReleaseID)
}

// publishSnapshot 校验指定快照并持久化为 active 版本，供启动期初始化等不读取可编辑草稿的路径使用。
func (l *RuntimeConfigLogic) publishSnapshot(snapshot ReleaseSnapshot, remark string, baseReleaseID uint64) (*types.RuntimeConfigPublishResp, error) {
	prepared, err := l.prepareSnapshot(snapshot)
	if err != nil {
		return nil, errors.Tag(err)
	}
	return l.publishPreparedSnapshot(func(*gorm.DB) (preparedRuntimeConfigSnapshot, error) {
		return prepared, nil
	}, nil, remark, baseReleaseID)
}

// prepareSnapshot 在数据库写事务前完成静态快照校验；草稿发布会在持有状态行锁后调用，保证读取内容与发布版本一致。
func (l *RuntimeConfigLogic) prepareSnapshot(snapshot ReleaseSnapshot) (preparedRuntimeConfigSnapshot, error) {
	snapshot = normalizeReleaseSnapshot(snapshot)
	if _, err := l.validateSnapshot(snapshot); err != nil {
		return preparedRuntimeConfigSnapshot{}, errors.Tag(err)
	}
	_, jsonText, yamlText, checksum, err := encodeReleaseSnapshot(snapshot)
	if err != nil {
		return preparedRuntimeConfigSnapshot{}, errors.Tag(err)
	}
	return preparedRuntimeConfigSnapshot{Snapshot: snapshot, JSON: jsonText, YAML: yamlText, Checksum: checksum}, nil
}

// publishPreparedSnapshot 在单个事务内锁定版本状态、取得待发布内容、执行可选草稿覆盖并切换 active 版本。
func (l *RuntimeConfigLogic) publishPreparedSnapshot(
	source func(*gorm.DB) (preparedRuntimeConfigSnapshot, error),
	applyDraft func(*gorm.DB, ReleaseSnapshot) error,
	remark string,
	baseReleaseID uint64,
) (*types.RuntimeConfigPublishResp, error) {
	db, err := l.writeDB()
	if err != nil {
		return nil, errors.Tag(err)
	}
	admin := l.GetCtxAdmin()
	publishedByName := admin.Name
	if strings.TrimSpace(publishedByName) == "" {
		publishedByName = "system"
	}
	var release model.RuntimeConfigRelease
	var previousReleaseID uint64
	err = db.Transaction(func(tx *gorm.DB) error {
		state, lockErr := l.loadStateForUpdate(tx)
		if lockErr != nil {
			return errors.Tag(lockErr)
		}
		prepared, prepareErr := source(tx)
		if prepareErr != nil {
			return errors.Tag(prepareErr)
		}
		if applyDraft != nil {
			if applyErr := applyDraft(tx, prepared.Snapshot); applyErr != nil {
				return errors.Tag(applyErr)
			}
		}
		version := state.ActiveVersion + 1
		now := time.Now()
		release = model.RuntimeConfigRelease{
			VersionNo:          version,
			SnapshotJSON:       prepared.JSON,
			SnapshotYAML:       prepared.YAML,
			Checksum:           prepared.Checksum,
			BaseReleaseID:      baseReleaseID,
			Remark:             strings.TrimSpace(remark),
			PublishedByAdminID: admin.ID,
			PublishedByName:    publishedByName,
			PublishedAt:        now,
		}
		if err = tx.Create(&release).Error; err != nil {
			return errors.Tag(err)
		}
		previousReleaseID = state.ActiveReleaseID
		state.ActiveReleaseID = release.ID
		state.ActiveVersion = version
		state.ActiveChecksum = prepared.Checksum
		state.PublishedAt = now
		if state.ID == 0 {
			if err = tx.Create(&state).Error; err != nil {
				return errors.Tag(err)
			}
			return nil
		}
		return errors.Tag(tx.Save(&state).Error)
	})
	if err != nil {
		return nil, errors.Tag(err)
	}
	resp := &types.RuntimeConfigPublishResp{
		ReleaseID: release.ID,
		VersionNo: release.VersionNo,
		Checksum:  release.Checksum,
	}
	if err = l.invalidateRuntimeConfigCache(release.ID, previousReleaseID); err != nil {
		corelogic.LogWrappedError(l.Logger, err,
			"RuntimeConfigLogic.publishSnapshot 发布ID[%d]已提交但缓存失效失败", release.ID)
		return resp, nil
	}
	reload := svc.RuntimeConfigReloadResult{}
	if l.Svc != nil && l.Svc.ConfigReload != nil {
		reload, err = l.Svc.ConfigReload.ReloadRuntimeConfig(l.Ctx, "runtime_config_publish")
		if err != nil {
			resp.Applied = false
			corelogic.LogWrappedError(l.Logger, err,
				"RuntimeConfigLogic.publishSnapshot 发布ID[%d]已提交但运行态应用失败", release.ID)
			return resp, nil
		}
		if runtimeConfigReloadMatchesRelease(release, reload) {
			resp.Applied = true
			resp.RestartRequired = reload.RestartRequired
			resp.RestartReason = reload.RestartReason
		}
	}
	if resp.Applied && (reload.RestartRequired || reload.RestartReason != "") {
		if updateErr := db.Model(&model.RuntimeConfigRelease{}).Where("id = ?", release.ID).Updates(map[string]any{
			"restart_required": reload.RestartRequired,
			"restart_reason":   reload.RestartReason,
		}).Error; updateErr != nil {
			corelogic.LogWrappedError(l.Logger, updateErr,
				"RuntimeConfigLogic.publishSnapshot 更新发布ID[%d]重启提示失败", release.ID)
		}
	}
	return resp, nil
}

// validateSnapshot 校验快照结构及当前运行时可执行性；任务系统未启用时只校验静态边界。
func (l *RuntimeConfigLogic) validateSnapshot(snapshot ReleaseSnapshot) ([]string, error) {
	messages, err := ValidateSnapshot(snapshot)
	if err != nil {
		return messages, errors.Tag(err)
	}
	if l == nil || l.Svc == nil || l.Svc.Task == nil || !l.Svc.Task.IsEnabled() {
		return messages, nil
	}
	if err = l.Svc.Task.ValidatePeriodicTaskConfigs(snapshot.TaskPeriodic); err != nil {
		return messages, errors.Tag(err)
	}
	return messages, nil
}

// runtimeConfigReloadMatchesRelease 判断重载回执是否精确对应本次发布。
func runtimeConfigReloadMatchesRelease(release model.RuntimeConfigRelease, reload svc.RuntimeConfigReloadResult) bool {
	return reload.ReleaseID == release.ID &&
		reload.VersionNo == release.VersionNo &&
		reload.Checksum == release.Checksum
}

// buildDraftSnapshot 从当前草稿表组装可发布快照。
func (l *RuntimeConfigLogic) buildDraftSnapshot() (ReleaseSnapshot, error) {
	db, err := l.writeDB()
	if err != nil {
		return ReleaseSnapshot{}, errors.Tag(err)
	}
	return l.buildDraftSnapshotDB(db)
}

// buildDraftSnapshotDB 使用调用方提供的数据库句柄读取完整草稿；发布路径传入已持有状态行锁的事务句柄。
func (l *RuntimeConfigLogic) buildDraftSnapshotDB(db *gorm.DB) (ReleaseSnapshot, error) {
	var periodicRows []model.RuntimeTaskPeriodic
	// writeDB 带路由 clause，跨模型查询必须新开 Session，避免沿用上一个 Statement。
	if err := db.Session(&gorm.Session{NewDB: true}).
		Order("sort_order ASC").Order("id ASC").Limit(tasklimits.MaxPeriodicCount + 1).Find(&periodicRows).Error; err != nil {
		return ReleaseSnapshot{}, errors.Tag(err)
	}
	if len(periodicRows) > tasklimits.MaxPeriodicCount {
		return ReleaseSnapshot{}, errors.Errorf("周期任务草稿不能超过 %d 条", tasklimits.MaxPeriodicCount)
	}
	var archiveRows []model.RuntimeArchiveJob
	if err := db.Session(&gorm.Session{NewDB: true}).
		Order("sort_order ASC").Order("id ASC").Limit(tasklimits.MaxArchiveJobCount + 1).Find(&archiveRows).Error; err != nil {
		return ReleaseSnapshot{}, errors.Tag(err)
	}
	if len(archiveRows) > tasklimits.MaxArchiveJobCount {
		return ReleaseSnapshot{}, errors.Errorf("归档任务草稿不能超过 %d 条", tasklimits.MaxArchiveJobCount)
	}
	snapshot := ReleaseSnapshot{
		ArchiveJobs:  make([]config.ArchiveJobConfig, 0, len(archiveRows)),
		TaskPeriodic: make([]config.TaskPeriodicConfig, 0, len(periodicRows)),
	}
	for _, row := range archiveRows {
		snapshot.ArchiveJobs = append(snapshot.ArchiveJobs, archiveModelToConfig(row))
	}
	for _, row := range periodicRows {
		snapshot.TaskPeriodic = append(snapshot.TaskPeriodic, periodicModelToConfig(row))
	}
	return snapshot, nil
}

// replaceDraftTx 在调用方持有状态行锁的事务内覆盖草稿；输入已经过发布快照归一化和校验。
func (l *RuntimeConfigLogic) replaceDraftTx(tx *gorm.DB, snapshot ReleaseSnapshot) error {
	adminID := l.adminID()
	periodicRows := make([]model.RuntimeTaskPeriodic, 0, len(snapshot.TaskPeriodic))
	for index, item := range snapshot.TaskPeriodic {
		periodicRows = append(periodicRows, periodicConfigToModel(item, adminID, index))
	}
	archiveRows := make([]model.RuntimeArchiveJob, 0, len(snapshot.ArchiveJobs))
	for index, item := range snapshot.ArchiveJobs {
		archiveRows = append(archiveRows, archiveConfigToModel(item, adminID, index))
	}
	// 覆盖单库草稿表；同一事务内跨模型操作也要清理 Statement，事务连接仍由 tx 保持。
	if err := tx.Session(&gorm.Session{NewDB: true, AllowGlobalUpdate: true}).Delete(&model.RuntimeTaskPeriodic{}).Error; err != nil {
		return errors.Tag(err)
	}
	if err := tx.Session(&gorm.Session{NewDB: true, AllowGlobalUpdate: true}).Delete(&model.RuntimeArchiveJob{}).Error; err != nil {
		return errors.Tag(err)
	}
	if len(periodicRows) > 0 {
		if err := tx.Session(&gorm.Session{NewDB: true}).CreateInBatches(&periodicRows, runtimeConfigDraftWriteBatchSize).Error; err != nil {
			return errors.Wrap(err, "批量写入周期任务草稿失败")
		}
	}
	if len(archiveRows) > 0 {
		if err := tx.Session(&gorm.Session{NewDB: true}).CreateInBatches(&archiveRows, runtimeConfigDraftWriteBatchSize).Error; err != nil {
			return errors.Wrap(err, "批量写入归档任务草稿失败")
		}
	}
	return nil
}

// loadActiveSnapshotCached 从缓存回源读取当前 active 发布快照。
func (l *RuntimeConfigLogic) loadActiveSnapshotCached() (*ActiveRelease, error) {
	state, err := l.loadActiveStateCached()
	if err != nil {
		return nil, errors.Tag(err)
	}
	if state.ActiveReleaseID == 0 {
		return nil, errors.Errorf("运行配置未发布 active release")
	}
	var snapshotJSON string
	manager, err := cachelogic.TableCacheManager(l.BaseLogic)
	if err != nil {
		return nil, errors.Tag(err)
	}
	releaseKey := cachelogic.TableCachePhysicalKey(l.BaseLogic, keys.RuntimeConfigReleaseKey(state.ActiveReleaseID))
	result, err := manager.LoadThrough(l.Ctx, releaseKey, &snapshotJSON, nil)
	if err != nil {
		return nil, errors.Tag(err)
	}
	if result.State == tablecache.LookupStateEmpty || strings.TrimSpace(snapshotJSON) == "" {
		return nil, errors.Errorf("运行配置发布快照不存在 release_id=%d", state.ActiveReleaseID)
	}
	snapshot, err := DecodeSnapshotJSON(snapshotJSON)
	if err != nil {
		return nil, errors.Tag(err)
	}
	release := model.RuntimeConfigRelease{
		ID:        state.ActiveReleaseID,
		VersionNo: state.ActiveVersion,
		Checksum:  state.ActiveChecksum,
	}
	return &ActiveRelease{State: state, Release: release, Snapshot: snapshot}, nil
}

// loadActiveStateCached 从缓存回源读取当前 active 状态。
func (l *RuntimeConfigLogic) loadActiveStateCached() (StateCache, error) {
	manager, err := cachelogic.TableCacheManager(l.BaseLogic)
	if err != nil {
		return StateCache{}, errors.Tag(err)
	}
	stateKey := cachelogic.TableCachePhysicalKey(l.BaseLogic, keys.RuntimeConfigStateKey())
	var state StateCache
	result, err := manager.LoadThrough(l.Ctx, stateKey, &state, nil)
	if err != nil {
		return StateCache{}, errors.Tag(err)
	}
	if result.State == tablecache.LookupStateEmpty {
		return StateCache{}, nil
	}
	return state, nil
}

// loadStateForUpdate 在事务内锁定运行配置状态行。
func (l *RuntimeConfigLogic) loadStateForUpdate(tx *gorm.DB) (model.RuntimeConfigState, error) {
	var state model.RuntimeConfigState
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		First(&state).Error
	if err == nil {
		return state, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return model.RuntimeConfigState{}, nil
	}
	return model.RuntimeConfigState{}, errors.Tag(err)
}

// lockRuntimeConfigDraftTx 锁定运行配置状态行；保存、删除和全量覆盖共用该顺序，避免并发写入互相覆盖。
func (l *RuntimeConfigLogic) lockRuntimeConfigDraftTx(tx *gorm.DB) error {
	state, err := l.loadStateForUpdate(tx)
	if err != nil {
		return errors.Wrap(err, "锁定运行配置草稿失败")
	}
	if state.ID == 0 {
		return errors.Errorf("运行配置状态未初始化")
	}
	return nil
}

// ensureDraftCapacityTx 在调用方持有运行配置状态行锁后统计目标草稿表，保证并发新增不会共同越过数量上限。
func (l *RuntimeConfigLogic) ensureDraftCapacityTx(tx *gorm.DB, modelPtr any, maxCount int, subject string) error {
	var count int64
	if err := tx.Session(&gorm.Session{NewDB: true}).Model(modelPtr).Count(&count).Error; err != nil {
		return errors.Wrapf(err, "统计%s草稿数量失败", subject)
	}
	if count >= int64(maxCount) {
		return errors.Wrapf(errRuntimeConfigCountLimit, "%s不能超过 %d 条", subject, maxCount)
	}
	return nil
}

// loadReleaseByID 按发布 ID 读取发布记录和快照。
func (l *RuntimeConfigLogic) loadReleaseByID(releaseID uint64) (model.RuntimeConfigRelease, ReleaseSnapshot, error) {
	db, err := l.writeDB()
	if err != nil {
		return model.RuntimeConfigRelease{}, ReleaseSnapshot{}, errors.Tag(err)
	}
	var row model.RuntimeConfigRelease
	if err = db.Where("id = ?", releaseID).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return row, ReleaseSnapshot{}, errors.Errorf("发布快照不存在: %d", releaseID)
		}
		return row, ReleaseSnapshot{}, errors.Tag(err)
	}
	snapshot, err := DecodeSnapshotJSON(row.SnapshotJSON)
	return row, snapshot, errors.Tag(err)
}

// deleteByID 按 ID 删除草稿记录，并校验记录真实存在。
func (l *RuntimeConfigLogic) deleteByID(modelPtr any, id uint64) error {
	db, err := l.writeDB()
	if err != nil {
		return errors.Tag(err)
	}
	return db.Transaction(func(tx *gorm.DB) error {
		if err := l.lockRuntimeConfigDraftTx(tx); err != nil {
			return errors.Tag(err)
		}
		result := tx.Where("id = ?", id).Delete(modelPtr)
		if result.Error != nil {
			return errors.Tag(result.Error)
		}
		if result.RowsAffected == 0 {
			return errors.Errorf("记录不存在: %d", id)
		}
		return nil
	})
}

// checkRuntimeConfigUpdated 区分数据库错误和按 ID 更新不到草稿的假成功。
func checkRuntimeConfigUpdated(result *gorm.DB, id uint64, subject string) error {
	if result == nil {
		return errors.Errorf("%s更新结果为空: %d", subject, id)
	}
	if result.Error != nil {
		return errors.Tag(result.Error)
	}
	if result.RowsAffected == 0 {
		return errors.Errorf("%s不存在: %d", subject, id)
	}
	return nil
}

// invalidateRuntimeConfigCache 删除运行配置状态和指定发布快照缓存。
func (l *RuntimeConfigLogic) invalidateRuntimeConfigCache(releaseIDs ...uint64) error {
	manager, err := cachelogic.TableCacheManager(l.BaseLogic)
	if err != nil {
		return errors.Tag(err)
	}
	cacheKeys := []string{cachelogic.TableCachePhysicalKey(l.BaseLogic, keys.RuntimeConfigStateKey())}
	for _, releaseID := range releaseIDs {
		if releaseID == 0 {
			continue
		}
		cacheKeys = append(cacheKeys, cachelogic.TableCachePhysicalKey(l.BaseLogic, keys.RuntimeConfigReleaseKey(releaseID)))
	}
	for _, cacheKey := range uniqueStrings(cacheKeys) {
		if cacheKey == "" {
			continue
		}
		if err = manager.DeleteByKey(l.Ctx, cacheKey); err != nil {
			return errors.Tag(err)
		}
	}
	return nil
}

// requireMFA 校验运行配置敏感操作的二次认证。
func (l *RuntimeConfigLogic) requireMFA(twoStepKey string, twoStepValue string) error {
	return (&adminlogic.AdminLogic{BaseLogic: l.BaseLogic}).RequireOperateMFATwoStep(securitylogic.MFAScenarioRuntimeConfigManage, twoStepKey, twoStepValue)
}

// draftCounts 统计周期任务和归档任务草稿数量。
func (l *RuntimeConfigLogic) draftCounts() (int64, int64, error) {
	db, err := l.writeDB()
	if err != nil {
		return 0, 0, errors.Tag(err)
	}
	var periodicCount int64
	// 统计两个草稿表时不复用 Statement，避免表名被上一条 Count 污染。
	if err = db.Session(&gorm.Session{NewDB: true}).Model(&model.RuntimeTaskPeriodic{}).Count(&periodicCount).Error; err != nil {
		return 0, 0, errors.Tag(err)
	}
	var archiveCount int64
	if err = db.Session(&gorm.Session{NewDB: true}).Model(&model.RuntimeArchiveJob{}).Count(&archiveCount).Error; err != nil {
		return 0, 0, errors.Tag(err)
	}
	return periodicCount, archiveCount, nil
}

// writeDB 返回运行配置写库连接。
func (l *RuntimeConfigLogic) writeDB() (*gorm.DB, error) {
	if l == nil || l.Svc == nil {
		return nil, errors.Errorf("服务上下文未初始化")
	}
	db := l.Svc.WriteDB(svc.DatabaseMain)
	if db == nil {
		return nil, errors.Errorf("main主库未初始化")
	}
	return db, nil
}

// currentConfig 返回当前服务配置快照。
func (l *RuntimeConfigLogic) currentConfig() config.Config {
	if l == nil || l.Svc == nil {
		return config.Config{}
	}
	return l.Svc.CurrentConfig()
}

// adminID 返回当前操作管理员 ID，缺失时返回 0。
func (l *RuntimeConfigLogic) adminID() int {
	admin := l.GetCtxAdmin()
	if admin == nil {
		return 0
	}
	return admin.ID
}
