package admin

import (
	corelogic "admin/internal/logic"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	i18n "admin/common/i18n"
	keys "admin/common/rediskeys"
	"admin/helper"
	redislock "admin/internal/infra/redsync"
	"admin/internal/model"
	"admin/internal/svc"
	"admin/internal/task/queue"
	"admin/internal/types"
	"admin/pkg/excel"
	"admin/pkg/storage"

	utils "github.com/Is999/go-utils"
	"github.com/Is999/go-utils/errors"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/xuri/excelize/v2"
	"gorm.io/gorm"
)

const (
	// adminExportStatusTTL 表示管理员导出任务状态在 Redis 中的保留时长。
	adminExportStatusTTL = 24 * time.Hour
	// adminExportBatchSize 表示管理员导出游标分页每批读取行数。
	adminExportBatchSize = 200
	// adminExportMinWorkerCount 表示管理员导出最小并发格式化 worker 数。
	adminExportMinWorkerCount = 2
	// adminExportMaxWorkerCount 表示管理员导出最大并发格式化 worker 数。
	adminExportMaxWorkerCount = 4
	// adminExportDefaultFileNamePrefix 表示管理员导出文件名前缀。
	adminExportDefaultFileNamePrefix = "admin_export"
	// adminExportSheetName 表示导出 Excel 工作表名称。
	adminExportSheetName = "管理员列表"
	// adminExportMaxConcurrentShards 表示管理员导出最大并发分片数。
	adminExportMaxConcurrentShards = 4
	// adminExportProgressMinInterval 表示管理员导出进度最小上报时间间隔。
	adminExportProgressMinInterval = 500 * time.Millisecond
	// adminExportRunningMaxProgress 表示运行态百分比上限，完成状态统一写入 100%。
	adminExportRunningMaxProgress = 99
	// adminExportTriggerLockTTL 表示管理员导出同条件触发锁 TTL。
	adminExportTriggerLockTTL = 15 * time.Second
	// adminExportReuseIndexTTL 表示管理员导出同条件复用索引保留时间。
	adminExportReuseIndexTTL = 24 * time.Hour
)

// AdminExportLogic 承载管理员列表异步导出、进度查询和文件下载准备逻辑。
type AdminExportLogic struct {
	*corelogic.BaseLogic // 复用请求上下文、数据库、Redis 和审计能力
}

// adminExportStatusSnapshot 表示写入 Redis 的管理员导出任务完整快照。
// 对前端隐藏的内部字段不能直接依赖 `types.AdminExportStatusResp` 的 JSON 标签持久化，
// 这里单独展开，避免下载阶段丢失对象 key、本地路径和权限校验所需信息。
type adminExportStatusSnapshot struct {
	types.AdminExportStatusResp        // 对外导出状态响应字段
	FilePath                    string `json:"filePath"`           // 服务器本地文件路径
	ObjectKey                   string `json:"objectKey"`          // 统一存储对象 key
	StorageType                 string `json:"storageType"`        // 统一存储类型
	ContentType                 string `json:"contentType"`        // 文件 MIME
	LastCursorID                int    `json:"lastCursorId"`       // 最近处理到的管理员 ID
	OperatorID                  int    `json:"operatorId"`         // 发起导出的管理员 ID
	RequestFingerprint          string `json:"requestFingerprint"` // 归一化导出条件指纹
	AuthorizedAdminIDs          []int  `json:"authorizedAdminIds"` // 允许复用该导出结果的管理员 ID 列表
}

// NewAdminExportLogic 创建绑定 HTTP 请求上下文的管理员导出逻辑对象。
func NewAdminExportLogic(r *http.Request, svcCtx *svc.ServiceContext) *AdminExportLogic {
	return &AdminExportLogic{
		BaseLogic: corelogic.NewBaseLogic(r, svcCtx),
	}
}

// NewAdminExportLogicWithContext 创建绑定任意上下文的管理员导出逻辑对象。
func NewAdminExportLogicWithContext(ctx context.Context, svcCtx *svc.ServiceContext) *AdminExportLogic {
	return &AdminExportLogic{
		BaseLogic: corelogic.NewBaseLogicWithContext(ctx, svcCtx),
	}
}

// Trigger 提交管理员列表导出任务，并立即返回 jobId 供前端轮询进度。
func (l *AdminExportLogic) Trigger(req *types.AdminExportReq) *types.BizResult {
	if err := l.ensureTaskQueueEnabled(); err != nil {
		return types.ServerError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminExportLogic.Trigger 任务系统未启用").ToBizResult()
	}
	if err := l.ensureRedisEnabled(); err != nil {
		return types.ServerError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminExportLogic.Trigger Redis 未启用").ToBizResult()
	}

	ctxAdmin := l.GetCtxAdmin()
	if ctxAdmin.ID <= 0 {
		return types.NewBizResult(401).
			SetI18nMessage(i18n.MsgKeyNeedLogin).
			WithError(errors.Errorf("AdminExportLogic.Trigger 当前请求未登录"))
	}

	triggerResp, err := l.triggerWithUniqueLock(req, ctxAdmin.ID, ctxAdmin.Name)
	if err != nil {
		return types.ServerError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminExportLogic.Trigger 提交管理员导出任务失败").ToBizResult()
	}

	return types.NewBizResult(1).
		SetI18nMessage(i18n.MsgKeySuccess).
		WithData(triggerResp)
}

// GetStatus 查询管理员导出任务的当前进度。
func (l *AdminExportLogic) GetStatus(req *types.AdminExportJobReq) *types.BizResult {
	status, err := l.loadStatus(req.JobID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return types.NotFound(i18n.MsgKeyNotFound, err,
				"AdminExportLogic.GetStatus 管理员导出任务[%s]不存在", req.JobID).ToBizResult()
		}
		return types.ServerError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminExportLogic.GetStatus 查询管理员导出任务[%s]失败", req.JobID).ToBizResult()
	}
	if err := l.ensureStatusOwner(status); err != nil {
		return types.Forbidden(i18n.MsgKeyForbidden).
			ToBizResult().
			WithError(errors.Wrapf(err, "AdminExportLogic.GetStatus 无权查看管理员导出任务[%s]", req.JobID))
	}
	// 已完成任务在对外返回前补做一次对象可用性探测，避免 Redis 状态与真实下载对象脱节。
	if err := l.refreshDownloadAvailability(status); err != nil {
		corelogic.LogWrappedError(l, err, "AdminExportLogic.GetStatus 刷新管理员导出任务[%s]下载可用性失败", req.JobID)
	}
	return types.NewBizResult(1).
		SetI18nMessage(i18n.MsgKeyQuerySuccess).
		WithData(l.publicStatus(status))
}

// PrepareDownload 校验管理员导出任务状态，并返回用于下载的文件信息。
func (l *AdminExportLogic) PrepareDownload(req *types.AdminExportJobReq) (*types.AdminExportStatusResp, *types.BizResult) {
	status, err := l.loadStatus(req.JobID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, types.NotFound(i18n.MsgKeyNotFound, err,
				"AdminExportLogic.PrepareDownload 管理员导出任务[%s]不存在", req.JobID).ToBizResult()
		}
		return nil, types.ServerError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminExportLogic.PrepareDownload 查询管理员导出任务[%s]失败", req.JobID).ToBizResult()
	}
	if err := l.ensureStatusOwner(status); err != nil {
		return nil, types.Forbidden(i18n.MsgKeyForbidden).
			ToBizResult().
			WithError(errors.Wrapf(err, "AdminExportLogic.PrepareDownload 无权下载管理员导出任务[%s]", req.JobID))
	}
	if status.Status != types.AdminExportStatusSucceeded || !status.DownloadReady {
		return nil, types.NewBizResult(0).
			SetI18nMessage(i18n.MsgKeyFail).
			WithError(errors.Errorf("AdminExportLogic.PrepareDownload 管理员导出任务[%s]尚未完成", req.JobID))
	}
	// 下载前再次确认真实对象仍然存在，避免对象被清理后仍返回假完成态。
	if err := l.refreshDownloadAvailability(status); err != nil || !status.DownloadReady {
		return nil, types.NewBizResult(0).
			SetI18nMessage(i18n.MsgKeyFail).
			WithError(errors.Wrapf(firstNonNilError(err, errors.Errorf("导出文件已失效")),
				"AdminExportLogic.PrepareDownload 管理员导出任务[%s]导出文件已失效", req.JobID))
	}
	return status, nil
}

// triggerWithUniqueLock 在同一导出条件上串行化创建或复用任务，避免重复导出消耗服务器资源。
func (l *AdminExportLogic) triggerWithUniqueLock(req *types.AdminExportReq, operatorID int, operatorName string) (*types.AdminExportTriggerResp, error) {
	fingerprint := buildAdminExportRequestFingerprint(req)
	lockKey := l.adminExportRequestLockKey(fingerprint)
	var triggerResp *types.AdminExportTriggerResp
	err := redislock.WithLock(l.Ctx, l.Redis(), lockKey, adminExportTriggerLockTTL, func(lockCtx context.Context) error {
		// lockedLogic 让锁丢失取消信号贯穿任务状态和投递操作，避免失锁后继续执行。
		lockedLogic := NewAdminExportLogicWithContext(lockCtx, l.Svc)
		var innerErr error
		triggerResp, innerErr = lockedLogic.triggerOrReuseUnderLock(req, fingerprint, operatorID, operatorName)
		return errors.Tag(innerErr)
	})
	if err == nil {
		return triggerResp, nil
	}
	if reusedResp, reuseErr := l.tryReuseExistingJob(fingerprint, operatorID); reuseErr == nil && reusedResp != nil {
		return reusedResp, nil
	}
	return nil, errors.Tag(err)
}

// triggerOrReuseUnderLock 在持有同条件互斥锁时创建或复用导出任务。
func (l *AdminExportLogic) triggerOrReuseUnderLock(req *types.AdminExportReq, fingerprint string, operatorID int, operatorName string) (*types.AdminExportTriggerResp, error) {
	if reusedResp, err := l.tryReuseExistingJob(fingerprint, operatorID); err != nil || reusedResp != nil {
		return reusedResp, errors.Tag(err)
	}
	jobID := uuid.NewString()
	now := time.Now()
	fileName := buildAdminExportFileName(jobID, now)
	status := &types.AdminExportStatusResp{
		JobID:              jobID,
		Queue:              taskqueue.QueueMaintenance,
		Status:             types.AdminExportStatusQueued,
		Progress:           0,
		Processed:          0,
		Total:              0,
		EstimatedSeconds:   0,
		FileName:           fileName,
		DownloadReady:      false,
		CreatedAt:          corelogic.FormatDateTime(now),
		UpdatedAt:          corelogic.FormatDateTime(now),
		FilePath:           buildAdminExportFilePath(fileName),
		OperatorID:         operatorID,
		DownloadURL:        buildAdminExportDownloadURL(jobID),
		RequestFingerprint: fingerprint,
		AuthorizedAdminIDs: []int{operatorID},
	}
	if err := l.saveStatus(status); err != nil {
		return nil, errors.Wrap(err, "初始化管理员导出任务失败")
	}
	if err := l.saveRequestIndex(fingerprint, jobID); err != nil {
		return nil, errors.Wrap(err, "保存管理员导出复用索引失败")
	}
	payloadBody, err := json.Marshal(types.AdminExportTaskPayload{
		JobID:        jobID,
		OperatorID:   operatorID,
		OperatorName: operatorName,
		Request:      *req,
	})
	if err != nil {
		_ = l.clearRequestIndex(fingerprint)
		_ = l.markFailed(status, errors.Wrap(err, "构造管理员导出任务载荷失败"))
		return nil, errors.Wrap(err, "构造管理员导出任务载荷失败")
	}
	defaultTimeoutSeconds := l.Svc.CurrentConfig().Task.DefaultTimeoutSeconds
	if defaultTimeoutSeconds <= 0 {
		defaultTimeoutSeconds = 220
	}
	enqueueResp, err := l.Svc.Task.EnqueueRegisteredTask(l.Ctx, &types.EnqueueTaskReq{
		TaskType:         types.AdminExportTaskType,
		Payload:          payloadBody,
		Queue:            taskqueue.QueueMaintenance,
		TimeoutSeconds:   &defaultTimeoutSeconds,
		UniqueTTLSeconds: new(int(adminExportReuseIndexTTL.Seconds())),
	})
	if err != nil {
		_ = l.clearRequestIndex(fingerprint)
		_ = l.markFailed(status, errors.Wrap(err, "投递管理员导出任务失败"))
		return nil, errors.Wrap(err, "投递管理员导出任务失败")
	}
	status.TaskID = enqueueResp.TaskID
	status.Queue = helper.FirstNonEmptyString(enqueueResp.Queue, taskqueue.QueueMaintenance)
	status.ProcessAt = enqueueResp.ProcessAt
	if err := l.saveStatus(status); err != nil {
		return nil, errors.Wrap(err, "更新管理员导出任务回执失败")
	}
	return l.buildTriggerResp(status), nil
}

// tryReuseExistingJob 复用同一导出条件下仍可用的任务，避免重复工作。
func (l *AdminExportLogic) tryReuseExistingJob(fingerprint string, operatorID int) (*types.AdminExportTriggerResp, error) {
	jobID, err := l.loadRequestIndex(fingerprint)
	if err != nil || jobID == "" {
		return nil, errors.Tag(err)
	}
	status, err := l.loadStatus(jobID)
	if err != nil {
		_ = l.clearRequestIndex(fingerprint)
		return nil, nil
	}
	switch status.Status {
	case types.AdminExportStatusQueued, types.AdminExportStatusRunning:
		if addAuthorizedAdmin(status, operatorID) {
			if saveErr := l.saveStatus(status); saveErr != nil {
				return nil, errors.Wrapf(saveErr, "保存管理员导出授权人失败 job_id=%s operator_id=%d", status.JobID, operatorID)
			}
		}
		return l.buildTriggerResp(status), nil
	case types.AdminExportStatusSucceeded:
		// 成功任务优先复用对象存储结果，避免统一存储落地后本地文件删除导致复用失效。
		if !l.hasReusableDownloadObject(status) {
			_ = l.clearRequestIndex(fingerprint)
			return nil, nil
		}
		if addAuthorizedAdmin(status, operatorID) {
			if saveErr := l.saveStatus(status); saveErr != nil {
				return nil, errors.Wrapf(saveErr, "保存管理员导出授权人失败 job_id=%s operator_id=%d", status.JobID, operatorID)
			}
		}
		return l.buildTriggerResp(status), nil
	default:
		_ = l.clearRequestIndex(fingerprint)
		return nil, nil
	}
}

// RunExportTask 执行管理员列表异步导出任务。
func (l *AdminExportLogic) RunExportTask(payload *types.AdminExportTaskPayload) error {
	if payload == nil {
		return errors.Errorf("管理员导出任务载荷不能为空")
	}
	if strings.TrimSpace(payload.JobID) == "" {
		return errors.Errorf("管理员导出任务 jobId 不能为空")
	}

	status, err := l.loadStatus(payload.JobID)
	if err != nil {
		return errors.Wrapf(err, "AdminExportLogic.RunExportTask 查询管理员导出任务[%s]失败", payload.JobID)
	}
	if status.Status == types.AdminExportStatusSucceeded {
		return nil
	}
	if strings.TrimSpace(status.ObjectKey) != "" {
		if err := l.deleteExportObject(status.ObjectKey); err != nil {
			return errors.Wrapf(err, "AdminExportLogic.RunExportTask 清理管理员导出任务[%s]重试残留对象失败", payload.JobID)
		}
		status.ObjectKey = ""
		status.StorageType = ""
	}

	now := time.Now()
	status.Status = types.AdminExportStatusRunning
	status.Progress = 0
	status.Processed = 0
	status.Total = 0
	status.EstimatedSeconds = 0
	status.AverageRowsPerSec = 0
	status.LastProcessedAt = ""
	status.FinishedAt = ""
	status.DownloadReady = false
	status.ErrorMessage = ""
	status.OperatorID = payload.OperatorID
	status.StartedAt = excel.FormatDateTime(now)
	if status.FileName == "" {
		status.FileName = buildAdminExportFileName(payload.JobID, now)
	}
	status.FilePath = buildAdminExportFilePath(status.FileName)
	status.ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	status.DownloadURL = buildAdminExportDownloadURL(status.JobID)
	if err := l.saveStatus(status); err != nil {
		return errors.Wrapf(err, "AdminExportLogic.RunExportTask 初始化管理员导出任务[%s]运行态失败", payload.JobID)
	}

	if err := os.MkdirAll(filepath.Dir(status.FilePath), 0o755); err != nil {
		_ = l.markFailed(status, errors.Wrap(err, "创建管理员导出目录失败"))
		return errors.Wrapf(err, "AdminExportLogic.RunExportTask 创建管理员导出目录失败 jobId=%s", payload.JobID)
	}
	status.Total, err = l.countAdmins(&payload.Request)
	if err != nil {
		_ = l.markFailed(status, errors.Wrap(err, "统计管理员导出总量失败"))
		return errors.Wrapf(err, "AdminExportLogic.RunExportTask 统计管理员导出总量失败 jobId=%s", payload.JobID)
	}
	if err := l.saveStatus(status); err != nil {
		return errors.Wrapf(err, "AdminExportLogic.RunExportTask 保存管理员导出总量失败 jobId=%s", payload.JobID)
	}

	shards, err := l.buildExportShards(&payload.Request, status.Total)
	if err != nil {
		_ = l.markFailed(status, errors.Wrap(err, "构造管理员导出分片失败"))
		return errors.Wrapf(err, "AdminExportLogic.RunExportTask 构造管理员导出分片失败 jobId=%s", payload.JobID)
	}
	startedAt, err := excel.ParseDateTime(status.StartedAt)
	if err != nil {
		_ = l.markFailed(status, errors.Wrap(err, "解析管理员导出开始时间失败"))
		return errors.Wrapf(err, "AdminExportLogic.RunExportTask 解析管理员导出开始时间失败 jobId=%s", payload.JobID)
	}
	if startedAt.IsZero() {
		startedAt = now
	}

	exportOpt := excel.ApplyStreamExportShardedOpts(
		excel.StreamExportShardedOptions[model.Admin, int, excel.OrderedIDShard]{
			FilePath: status.FilePath,
			Query: func(ctx context.Context, shard excel.OrderedIDShard, cursorID int, limit int) (*excel.CursorPage[model.Admin, int], error) {
				admins, queryErr := l.listAdminShardBatch(ctx, &payload.Request, shard, cursorID, limit)
				if queryErr != nil {
					return nil, errors.Wrap(queryErr, "查询管理员导出批次失败")
				}
				page := &excel.CursorPage[model.Admin, int]{
					Total: status.Total,
					Items: admins,
				}
				if len(admins) > 0 {
					page.NextCursor = admins[len(admins)-1].ID
					page.HasMore = len(admins) >= limit && (shard.EndID <= 0 || admins[len(admins)-1].ID < shard.EndID)
				}
				return page, nil
			},
			BuildRows: func(admins []model.Admin) ([][]any, error) {
				roleMap, queryErr := l.adminManageLogic().adminRoleMap(admins)
				if queryErr != nil {
					return nil, errors.Wrap(queryErr, "查询管理员导出角色映射失败")
				}
				return buildAdminExportRows(admins, roleMap)
			},
		},
		excel.WithStreamExportSheetName[model.Admin, int, excel.OrderedIDShard](adminExportSheetName),
		excel.WithStreamExportHeader[model.Admin, int, excel.OrderedIDShard](buildAdminExportHeader()),
		excel.WithStreamExportHeaderStyle[model.Admin, int, excel.OrderedIDShard](&excelize.Style{
			Font:      &excelize.Font{Bold: true},
			Alignment: &excelize.Alignment{Horizontal: "center", Vertical: "center"},
		}),
		excel.WithStreamExportBatchSize[model.Admin, int, excel.OrderedIDShard](adminExportBatchSize),
		excel.WithStreamExportMaxConcurrentShards[model.Admin, int, excel.OrderedIDShard](adminExportMaxConcurrentShards),
		excel.WithStreamExportChunkBufferSize[model.Admin, int, excel.OrderedIDShard](adminExportMaxConcurrentShards*2),
		excel.WithStreamExportRetry[model.Admin, int, excel.OrderedIDShard](3, 100*time.Millisecond),
		excel.WithStreamExportShards[model.Admin, int, excel.OrderedIDShard](shards),
		excel.WithStreamExportInitialCursor[model.Admin, int, excel.OrderedIDShard](func(shard excel.OrderedIDShard) int {
			return shard.StartID - 1
		}),
		excel.WithStreamExportProgress[model.Admin, int, excel.OrderedIDShard](func(progress excel.ExportProgress) error {
			applyAdminExportProgress(status, startedAt, progress)
			return l.saveStatus(status)
		}),
		// 进度仅按时间节流，避免高速导出按行数频繁写 Redis。
		excel.WithStreamExportProgressThrottle[model.Admin, int, excel.OrderedIDShard](adminExportProgressMinInterval, 0),
	)

	if err := excel.StreamExportSharded(l.Ctx, exportOpt); err != nil {
		_ = l.markFailed(status, err)
		_ = os.Remove(status.FilePath)
		return errors.Wrapf(err, "AdminExportLogic.RunExportTask 执行通用流式导出失败 jobId=%s", payload.JobID)
	}
	if err := l.persistExportObject(status); err != nil {
		_ = l.markFailed(status, err)
		_ = os.Remove(status.FilePath)
		return errors.Wrapf(err, "AdminExportLogic.RunExportTask 持久化导出对象失败 jobId=%s", payload.JobID)
	}
	_ = os.Remove(status.FilePath)

	finishedAt := time.Now()
	status.Status = types.AdminExportStatusSucceeded
	status.Progress = 100
	status.Total = status.Processed
	status.EstimatedSeconds = 0
	status.DownloadReady = true
	status.FinishedAt = excel.FormatDateTime(finishedAt)
	status.LastProcessedAt = excel.FormatDateTime(finishedAt)
	status.DownloadURL = buildAdminExportDownloadURL(status.JobID)
	if err := l.scheduleExportCleanup(status); err != nil {
		cleanupErr := l.deleteExportObject(status.ObjectKey)
		if cleanupErr == nil {
			status.ObjectKey = ""
			status.StorageType = ""
		}
		status.DownloadReady = false
		wrappedErr := errors.Wrap(err, "投递管理员导出文件清理任务失败")
		_ = l.markFailed(status, firstNonNilError(cleanupErr, wrappedErr))
		return errors.Tag(wrappedErr)
	}
	if err := l.saveStatus(status); err != nil {
		return errors.Wrapf(err, "AdminExportLogic.RunExportTask 保存管理员导出完成状态失败 jobId=%s", payload.JobID)
	}
	return nil
}

// CleanupExportFile 删除已到保留期限的管理员导出对象。
func (l *AdminExportLogic) CleanupExportFile(payload *types.AdminExportCleanupTaskPayload) error {
	if payload == nil || strings.TrimSpace(payload.JobID) == "" || strings.TrimSpace(payload.ObjectKey) == "" {
		return errors.Errorf("管理员导出文件清理任务参数不完整")
	}
	return l.deleteExportObject(payload.ObjectKey)
}

// scheduleExportCleanup 投递导出完成 24 小时后的文件清理任务。
func (l *AdminExportLogic) scheduleExportCleanup(status *types.AdminExportStatusResp) error {
	if status == nil || l == nil || l.Svc == nil || l.Svc.Task == nil {
		return errors.Errorf("管理员导出文件清理任务系统未初始化")
	}
	objectKey := strings.TrimSpace(status.ObjectKey)
	if objectKey == "" {
		return errors.Errorf("管理员导出文件清理任务缺少对象 key")
	}
	payload, err := json.Marshal(types.AdminExportCleanupTaskPayload{
		JobID:     status.JobID,
		ObjectKey: objectKey,
	})
	if err != nil {
		return errors.Wrap(err, "序列化管理员导出文件清理任务失败")
	}
	return l.Svc.Task.EnqueueTask(l.Ctx, types.AdminExportCleanupTaskType, payload,
		svc.WithTaskQueue(taskqueue.QueueMaintenance),
		svc.WithTaskRetry(5),
		svc.WithTaskTimeout(10*time.Minute),
		svc.WithTaskDelay(adminExportStatusTTL),
	)
}

// deleteExportObject 删除管理员导出对象，删除失败时保留任务重试机会。
func (l *AdminExportLogic) deleteExportObject(objectKey string) error {
	objectKey = strings.TrimSpace(objectKey)
	if objectKey == "" {
		return nil
	}
	objectStorage, err := l.objectStorage()
	if err != nil {
		return errors.Tag(err)
	}
	if err := objectStorage.Delete(l.Ctx, objectKey); err != nil {
		return errors.Wrapf(err, "删除管理员导出对象失败 object_key=%s", objectKey)
	}
	return nil
}

// applyAdminExportProgress 更新管理员导出的全局进度、平均速度和预计剩余时间。
func applyAdminExportProgress(status *types.AdminExportStatusResp, startedAt time.Time, progress excel.ExportProgress) {
	status.Processed = progress.Processed
	status.LastProcessedAt = excel.FormatDateTime(progress.LastProcessedAt)
	percent, averageRowsPerSec, estimatedSeconds := excel.BuildMetrics(status.Total, status.Processed, startedAt, progress.LastProcessedAt)
	status.AverageRowsPerSec = averageRowsPerSec
	if status.Total <= 0 {
		status.Progress = 0
		status.EstimatedSeconds = 0
		return
	}
	status.Progress = min(percent, adminExportRunningMaxProgress)
	status.EstimatedSeconds = estimatedSeconds
}

// OpenDownloadObject 打开管理员导出结果对象流，支持本地文件和统一存储对象。
func (l *AdminExportLogic) OpenDownloadObject(status *types.AdminExportStatusResp, rangeHeader string) (*storage.OpenObjectResult, error) {
	if status == nil {
		return nil, errors.Errorf("导出任务状态不能为空")
	}
	if strings.TrimSpace(status.ObjectKey) != "" {
		objectStorage, err := l.objectStorage()
		if err != nil {
			return nil, errors.Tag(err)
		}
		// 统一存储优先打开对象，并支持 S3 与本地对象两种实现。
		return objectStorage.Open(l.Ctx, storage.OpenObjectReq{
			ObjectKey:   status.ObjectKey,
			RangeHeader: rangeHeader,
		})
	}
	if strings.TrimSpace(status.FilePath) == "" {
		return nil, errors.Errorf("导出文件不存在")
	}
	file, err := os.Open(status.FilePath)
	if err != nil {
		return nil, errors.Wrap(err, "打开本地导出文件失败")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, errors.Wrap(err, "读取本地导出文件信息失败")
	}
	return &storage.OpenObjectResult{
		Reader:        file,
		ContentType:   helper.FirstNonEmptyString(status.ContentType, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
		ContentLength: info.Size(),
		FileName:      helper.FirstNonEmptyString(status.FileName, info.Name()),
		LastModified:  info.ModTime(),
		AcceptRanges:  true,
	}, nil
}

// adminManageLogic 返回复用当前上下文的管理员管理逻辑对象。
func (l *AdminExportLogic) adminManageLogic() *AdminManageLogic {
	return &AdminManageLogic{
		AdminLogic: &AdminLogic{BaseLogic: l.BaseLogic},
	}
}

// ensureTaskQueueEnabled 校验任务系统是否可用。
func (l *AdminExportLogic) ensureTaskQueueEnabled() error {
	if l == nil || l.Svc == nil || l.Svc.Task == nil || !l.Svc.Task.IsEnabled() {
		return errors.Errorf("任务系统未启用")
	}
	return nil
}

// ensureRedisEnabled 校验 Redis 是否可用。
func (l *AdminExportLogic) ensureRedisEnabled() error {
	if l == nil || l.Redis() == nil {
		return errors.Errorf("Redis 未初始化")
	}
	return nil
}

// loadRequestIndex 读取同条件导出复用索引。
func (l *AdminExportLogic) loadRequestIndex(fingerprint string) (string, error) {
	if err := l.ensureRedisEnabled(); err != nil {
		return "", errors.Tag(err)
	}
	fingerprint = strings.TrimSpace(fingerprint)
	if fingerprint == "" {
		return "", nil
	}
	value, err := l.Redis().Get(l.Ctx, l.adminExportRequestIndexKey(fingerprint)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", errors.Wrap(err, "读取管理员导出复用索引失败")
	}
	return strings.TrimSpace(value), nil
}

// saveRequestIndex 保存同条件导出复用索引。
func (l *AdminExportLogic) saveRequestIndex(fingerprint string, jobID string) error {
	if err := l.ensureRedisEnabled(); err != nil {
		return errors.Tag(err)
	}
	fingerprint = strings.TrimSpace(fingerprint)
	jobID = strings.TrimSpace(jobID)
	if fingerprint == "" || jobID == "" {
		return errors.Errorf("导出复用索引参数不能为空")
	}
	if err := l.Redis().Set(l.Ctx, l.adminExportRequestIndexKey(fingerprint), jobID, adminExportReuseIndexTTL).Err(); err != nil {
		return errors.Wrap(err, "保存管理员导出复用索引失败")
	}
	return nil
}

// clearRequestIndex 清理同条件导出复用索引。
func (l *AdminExportLogic) clearRequestIndex(fingerprint string) error {
	if err := l.ensureRedisEnabled(); err != nil {
		return errors.Tag(err)
	}
	fingerprint = strings.TrimSpace(fingerprint)
	if fingerprint == "" {
		return nil
	}
	if err := l.Redis().Del(l.Ctx, l.adminExportRequestIndexKey(fingerprint)).Err(); err != nil {
		return errors.Wrap(err, "清理管理员导出复用索引失败")
	}
	return nil
}

// countAdmins 统计当前筛选条件下的管理员总数。
func (l *AdminExportLogic) countAdmins(req *types.AdminExportReq) (int64, error) {
	var total int64
	if err := l.adminQuery(l.Ctx, req).Count(&total).Error; err != nil {
		return 0, errors.Wrap(err, "AdminExportLogic.countAdmins 统计管理员总数失败")
	}
	return total, nil
}

// listAdminShardBatch 按分片范围和 ID 游标顺序读取一批导出数据。
func (l *AdminExportLogic) listAdminShardBatch(ctx context.Context, req *types.AdminExportReq, shard excel.OrderedIDShard, cursorID int, limit int) ([]model.Admin, error) {
	if limit <= 0 {
		limit = adminExportBatchSize
	}
	admins := make([]model.Admin, 0, limit)
	dbq := l.adminQuery(ctx, req).
		Where("admin.id >= ?", shard.StartID).
		Order("admin.id ASC").
		Limit(limit)
	if shard.EndID > 0 {
		dbq = dbq.Where("admin.id <= ?", shard.EndID)
	}
	if cursorID >= shard.StartID {
		dbq = dbq.Where("admin.id > ?", cursorID)
	}
	if err := dbq.Find(&admins).Error; err != nil {
		return nil, errors.Wrap(err, "AdminExportLogic.listAdminShardBatch 查询管理员分片批次失败")
	}
	return admins, nil
}

// buildExportShards 根据筛选后的管理员 ID 顺序构造导出分片，保证分片顺序与全局导出顺序一致。
func (l *AdminExportLogic) buildExportShards(req *types.AdminExportReq, total int64) ([]excel.ExportShard[excel.OrderedIDShard], error) {
	if total <= 0 {
		return []excel.ExportShard[excel.OrderedIDShard]{
			{Meta: excel.OrderedIDShard{StartID: 1, EndID: 0}},
		}, nil
	}
	shardCount := excel.RecommendShardCount(total, adminExportBatchSize, adminExportMaxConcurrentShards, 2)
	if shardCount <= 1 {
		return []excel.ExportShard[excel.OrderedIDShard]{
			{Meta: excel.OrderedIDShard{StartID: 1, EndID: 0}},
		}, nil
	}

	minID, maxID, err := l.queryAdminIDRange(req)
	if err != nil {
		return nil, errors.Tag(err)
	}
	return excel.BuildOrderedIDShards(minID, maxID, shardCount), nil
}

// adminIDRange 表示当前筛选条件命中的管理员主键范围。
type adminIDRange struct {
	MinID int `gorm:"column:min_id"` // 最小管理员 ID
	MaxID int `gorm:"column:max_id"` // 最大管理员 ID
}

// queryAdminIDRange 查询当前筛选结果的主键范围，避免分片边界计算使用高成本 OFFSET。
func (l *AdminExportLogic) queryAdminIDRange(req *types.AdminExportReq) (int, int, error) {
	var result adminIDRange
	if err := l.adminQuery(l.Ctx, req).
		Select("COALESCE(MIN(admin.id), 0) AS min_id, COALESCE(MAX(admin.id), 0) AS max_id").
		Scan(&result).Error; err != nil {
		return 0, 0, errors.Wrap(err, "AdminExportLogic.queryAdminIDRange 查询管理员主键范围失败")
	}
	return result.MinID, result.MaxID, nil
}

// adminQuery 构造管理员导出统一查询条件，保持与列表页筛选逻辑一致。
func (l *AdminExportLogic) adminQuery(ctx context.Context, req *types.AdminExportReq) *gorm.DB {
	if ctx == nil {
		ctx = context.Background()
	}
	dbq := l.Svc.ReadDB(svc.DatabaseMain).WithContext(ctx).Model(&model.Admin{})
	if req == nil {
		return dbq
	}
	if req.Username != "" {
		dbq = dbq.Where("admin.name LIKE ?", "%"+req.Username+"%")
	}
	if req.RealName != "" {
		dbq = dbq.Where("admin.real_name LIKE ?", "%"+req.RealName+"%")
	}
	if req.Status != nil {
		dbq = dbq.Where("admin.status = ?", *req.Status)
	}
	if req.RoleID != nil && *req.RoleID > 0 {
		roleID := *req.RoleID
		roleSubQuery := l.Svc.ReadDB(svc.DatabaseMain).WithContext(ctx).
			Model(&model.AdminRoleRel{}).
			Select("user_id").
			Where("role_id = ?", roleID)
		dbq = dbq.Where("admin.id IN (?)", roleSubQuery)
	}
	return dbq
}

// loadStatus 从 Redis 读取管理员导出任务状态。
func (l *AdminExportLogic) loadStatus(jobID string) (*types.AdminExportStatusResp, error) {
	if err := l.ensureRedisEnabled(); err != nil {
		return nil, errors.Tag(err)
	}
	snapshot := &adminExportStatusSnapshot{}
	if err := l.statusStore().Load(l.Ctx, jobID, snapshot); err != nil {
		return nil, errors.Wrapf(err, "AdminExportLogic.loadStatus 读取管理员导出任务[%s]失败", jobID)
	}
	status := snapshot.toStatus()
	if status.DownloadURL == "" {
		status.DownloadURL = buildAdminExportDownloadURL(status.JobID)
	}
	return status, nil
}

// saveStatus 将管理员导出任务状态写回 Redis。
func (l *AdminExportLogic) saveStatus(status *types.AdminExportStatusResp) error {
	if status == nil {
		return errors.Errorf("管理员导出任务状态不能为空")
	}
	if err := l.ensureRedisEnabled(); err != nil {
		return errors.Tag(err)
	}
	now := time.Now()
	if status.CreatedAt == "" {
		status.CreatedAt = corelogic.FormatDateTime(now)
	}
	status.UpdatedAt = corelogic.FormatDateTime(now)
	if status.DownloadURL == "" {
		status.DownloadURL = buildAdminExportDownloadURL(status.JobID)
	}
	if err := l.statusStore().Save(l.Ctx, status.JobID, newAdminExportStatusSnapshot(status)); err != nil {
		return errors.Wrapf(err, "AdminExportLogic.saveStatus 保存管理员导出任务[%s]失败", status.JobID)
	}
	return nil
}

// markFailed 把管理员导出任务更新为失败状态。
func (l *AdminExportLogic) markFailed(status *types.AdminExportStatusResp, err error) error {
	if status == nil {
		return nil
	}
	status.Status = types.AdminExportStatusFailed
	status.DownloadReady = false
	status.Progress = excel.ClampProgress(status.Progress)
	if err != nil {
		status.ErrorMessage = err.Error()
	}
	status.FinishedAt = excel.FormatDateTime(time.Now())
	if clearErr := l.clearRequestIndex(status.RequestFingerprint); clearErr != nil {
		corelogic.LogWrappedError(l, clearErr, "AdminExportLogic.markFailed 清理管理员导出复用索引失败")
	}
	saveErr := l.saveStatus(status)
	l.notifyExportFailed(status, firstNonNilError(err, saveErr))
	return saveErr
}

// notifyExportFailed 上报管理员 Excel 异步导出失败，避免后台任务状态只停留在 Redis 中无人感知。
func (l *AdminExportLogic) notifyExportFailed(status *types.AdminExportStatusResp, err error) {
	if l == nil || l.Svc == nil || status == nil || err == nil {
		return
	}
	svc.NotifyTaskRuntimeAlert(l.Ctx, l.Svc.RuntimeAlerter, svc.TaskRuntimeAlert{
		Kind:      svc.TaskRuntimeAlertKindAdminExcelExportFailed,
		Title:     "【P1 Excel 异步导出失败】",
		Status:    "导出任务已标记失败，文件不可下载",
		Component: "excel_export",
		Operation: "admin_export",
		TaskName:  "管理员列表导出",
		TaskType:  types.AdminExportTaskType,
		TaskQueue: helper.FirstNonEmptyString(status.Queue, taskqueue.QueueMaintenance),
		UniqueKey: "admin_export",
		Reason:    adminExportAlertReason(status, err),
		Advice:    "请在导出任务状态中按 jobId 定位请求条件，并在任务中心按 taskId 查看执行日志；确认查询范围、对象存储和 Redis 状态后再重新触发导出。",
	})
}

// adminExportAlertReason 生成导出告警错误摘要，保留 jobId/taskId 但不参与告警指纹。
func adminExportAlertReason(status *types.AdminExportStatusResp, err error) string {
	parts := make([]string, 0, 3)
	if status != nil && strings.TrimSpace(status.JobID) != "" {
		parts = append(parts, "jobId="+strings.TrimSpace(status.JobID))
	}
	if status != nil && strings.TrimSpace(status.TaskID) != "" {
		parts = append(parts, "taskId="+strings.TrimSpace(status.TaskID))
	}
	if err != nil {
		parts = append(parts, err.Error())
	}
	return strings.Join(parts, "；")
}

// ensureStatusOwner 校验当前管理员只能查看和下载自己发起或被授权复用的导出任务。
func (l *AdminExportLogic) ensureStatusOwner(status *types.AdminExportStatusResp) error {
	if status == nil {
		return gorm.ErrRecordNotFound
	}
	ctxAdmin := l.GetCtxAdmin()
	if ctxAdmin.ID <= 0 {
		return errors.Errorf("当前请求未登录")
	}
	if status.OperatorID == ctxAdmin.ID {
		return nil
	}
	for _, adminID := range status.AuthorizedAdminIDs {
		if adminID == ctxAdmin.ID {
			return nil
		}
	}
	if status.OperatorID > 0 && status.OperatorID != ctxAdmin.ID {
		return errors.Errorf("无权访问其他管理员的导出任务")
	}
	return nil
}

// publicStatus 返回对前端开放的管理员导出状态快照。
func (l *AdminExportLogic) publicStatus(status *types.AdminExportStatusResp) *types.AdminExportStatusResp {
	if status == nil {
		return &types.AdminExportStatusResp{}
	}
	resp := *status
	resp.Progress = excel.ClampProgress(resp.Progress)
	resp.DownloadURL = buildAdminExportDownloadURL(resp.JobID)
	return &resp
}

// statusStore 返回管理员导出复用的通用任务状态存储。
func (l *AdminExportLogic) statusStore() *excel.RedisStore {
	return excel.NewRedisStore(l.Redis(), l.adminExportJobKeyPattern(), adminExportStatusTTL)
}

// adminExportRequestLockKey 返回当前 app_id 作用域下的管理员导出条件互斥锁。
func (l *AdminExportLogic) adminExportRequestLockKey(fingerprint string) string {
	return l.AppRedisKey(fmt.Sprintf(keys.AdminExportRequestLock, strings.TrimSpace(fingerprint)))
}

// adminExportRequestIndexKey 返回当前 app_id 作用域下的管理员导出条件复用索引。
func (l *AdminExportLogic) adminExportRequestIndexKey(fingerprint string) string {
	return l.AppRedisKey(fmt.Sprintf(keys.AdminExportRequestIndex, strings.TrimSpace(fingerprint)))
}

// adminExportJobKeyPattern 返回当前 app_id 作用域下的管理员导出任务状态 key 模板。
func (l *AdminExportLogic) adminExportJobKeyPattern() string {
	return l.AppRedisKey(keys.AdminExportJob)
}

// buildAdminExportHeader 返回管理员导出的标准表头。
func buildAdminExportHeader() []any {
	return []any{
		"ID",
		"登录用户名",
		"真实姓名",
		"邮箱",
		"手机号",
		"账号状态",
		"MFA状态",
		"首次改密",
		"角色 ID",
		"角色名称",
		"最近登录时间",
		"最近登录 IP",
		"最近登录归属地",
		"创建时间",
		"更新时间",
		"备注",
	}
}

// buildAdminExportRows 并发构造当前批次管理员导出数据行，减少流式写入前的串行格式化开销。
func buildAdminExportRows(admins []model.Admin, roleMap map[int][]types.AdminRoleItem) ([][]any, error) {
	if len(admins) == 0 {
		return [][]any{}, nil
	}
	return excel.BuildRowsConcurrentlyWithOpt(
		admins,
		func(admin model.Admin) ([]any, error) {
			roles := roleMap[admin.ID]
			roleIDs := make([]string, 0, len(roles))
			roleTitles := make([]string, 0, len(roles))
			for _, role := range roles {
				roleIDs = append(roleIDs, strconv.Itoa(role.ID))
				roleTitles = append(roleTitles, role.Title)
			}
			return []any{
				admin.ID,
				admin.Name,
				admin.RealName,
				admin.Email,
				admin.Phone,
				adminStatusText(admin.Status),
				adminMFAStatusText(admin.MfaStatus),
				adminNeedResetPasswordText(admin.NeedResetPassword),
				strings.Join(roleIDs, ","),
				strings.Join(roleTitles, ","),
				corelogic.FormatOptionalDateTime(admin.LastLoginTime),
				admin.LastLoginIP,
				admin.LastLoginIPAddr,
				corelogic.FormatDateTime(admin.CreatedAt),
				corelogic.FormatDateTime(admin.UpdatedAt),
				admin.Description,
			}, nil
		},
		excel.WithConcurrentRowsWorkersRange(adminExportMinWorkerCount, adminExportMaxWorkerCount),
	)
}

// buildAdminExportFileName 构造管理员导出文件名。
func buildAdminExportFileName(jobID string, now time.Time) string {
	return fmt.Sprintf("%s_%s_%s.xlsx",
		adminExportDefaultFileNamePrefix,
		now.Format("20060102150405"),
		strings.ReplaceAll(jobID, "-", ""))
}

// buildAdminExportFilePath 构造管理员导出文件的本地存储路径。
func buildAdminExportFilePath(fileName string) string {
	return filepath.Join(os.TempDir(), "admin", "exports", "admin", fileName)
}

// buildAdminExportDownloadURL 构造管理员导出结果下载地址。
func buildAdminExportDownloadURL(jobID string) string {
	return fmt.Sprintf("/api/admins/exports/download/%s", url.PathEscape(strings.TrimSpace(jobID)))
}

// buildAdminExportRequestFingerprint 计算管理员导出筛选条件的稳定指纹。
func buildAdminExportRequestFingerprint(req *types.AdminExportReq) string {
	if req == nil {
		req = &types.AdminExportReq{}
	}
	statusText := ""
	if req.Status != nil {
		statusText = strconv.Itoa(*req.Status)
	}
	roleID := 0
	if req.RoleID != nil && *req.RoleID > 0 {
		roleID = *req.RoleID
	}
	raw := strings.Join([]string{
		strings.TrimSpace(req.Username),
		strings.TrimSpace(req.RealName),
		statusText,
		strconv.Itoa(roleID),
	}, "|")
	return utils.SHA256(raw)
}

// buildTriggerResp 从任务状态构造提交导出后的统一回执。
func (l *AdminExportLogic) buildTriggerResp(status *types.AdminExportStatusResp) *types.AdminExportTriggerResp {
	if status == nil {
		return &types.AdminExportTriggerResp{}
	}
	return &types.AdminExportTriggerResp{
		JobID:  status.JobID,
		TaskID: status.TaskID,
		Queue:  status.Queue,
		Status: status.Status,
	}
}

// addAuthorizedAdmin 把管理员加入可复用任务访问名单。
func addAuthorizedAdmin(status *types.AdminExportStatusResp, adminID int) bool {
	if status == nil || adminID <= 0 {
		return false
	}
	for _, item := range status.AuthorizedAdminIDs {
		if item == adminID {
			return false
		}
	}
	status.AuthorizedAdminIDs = append(status.AuthorizedAdminIDs, adminID)
	return true
}

// fileExists 判断文件是否存在。
func fileExists(filePath string) bool {
	if strings.TrimSpace(filePath) == "" {
		return false
	}
	_, err := os.Stat(filePath)
	return err == nil
}

// persistExportObject 将导出文件保存到对象存储。
func (l *AdminExportLogic) persistExportObject(status *types.AdminExportStatusResp) error {
	if status == nil {
		return errors.Errorf("导出任务状态不能为空")
	}
	objectStorage, err := l.objectStorage()
	if err != nil {
		return errors.Tag(err)
	}
	storedObject, err := objectStorage.SaveLocalFile(l.Ctx, storage.SaveLocalFileReq{
		BizType:          "admin-export",
		ContentType:      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		LocalPath:        status.FilePath,
		OriginalFileName: status.FileName,
		StoredFileName:   storage.BuildStoredFileName(buildAdminExportStorageID(status.JobID), status.FileName),
		Visibility:       storage.VisibilityPrivate,
	})
	if err != nil {
		return errors.Wrap(err, "上传管理员导出文件到统一存储失败")
	}
	status.ObjectKey = storedObject.ObjectKey
	status.StorageType = storedObject.StorageType
	status.ContentType = storedObject.ContentType
	return nil
}

// buildAdminExportStorageID 生成单次导出尝试的唯一存储 ID，避免失败重试覆盖仍待清理的旧对象。
func buildAdminExportStorageID(jobID string) string {
	return fmt.Sprintf("%s_%s",
		strings.ReplaceAll(strings.TrimSpace(jobID), "-", ""),
		strings.ReplaceAll(uuid.NewString(), "-", ""),
	)
}

// hasReusableDownloadObject 判断当前导出任务是否仍保有可复用的下载对象。
func (l *AdminExportLogic) hasReusableDownloadObject(status *types.AdminExportStatusResp) bool {
	if status == nil {
		return false
	}
	return l.probeDownloadObject(status) == nil
}

// refreshDownloadAvailability 刷新已完成导出任务的真实下载可用性，避免对象失效后前端仍看到可下载状态。
func (l *AdminExportLogic) refreshDownloadAvailability(status *types.AdminExportStatusResp) error {
	if status == nil || status.Status != types.AdminExportStatusSucceeded || !status.DownloadReady {
		return nil
	}
	if err := l.probeDownloadObject(status); err != nil {
		status.DownloadReady = false
		status.Status = types.AdminExportStatusFailed
		status.ErrorMessage = l.Message(i18n.MsgKeyAdminExportFileExpired)
		if clearErr := l.clearRequestIndex(status.RequestFingerprint); clearErr != nil {
			corelogic.LogWrappedError(l, clearErr, "AdminExportLogic.refreshDownloadAvailability 清理管理员导出复用索引失败")
		}
		if saveErr := l.saveStatus(status); saveErr != nil {
			return errors.Wrap(saveErr, "保存导出文件失效状态失败")
		}
		return errors.Wrap(err, "导出文件不存在或已失效")
	}
	return nil
}

// probeDownloadObject 探测管理员导出结果对象是否仍可读取，支持统一存储对象与本地文件。
func (l *AdminExportLogic) probeDownloadObject(status *types.AdminExportStatusResp) error {
	if status == nil {
		return errors.Errorf("导出任务状态不能为空")
	}
	if strings.TrimSpace(status.ObjectKey) != "" {
		objectStorage, err := l.objectStorage()
		if err != nil {
			return errors.Tag(err)
		}
		objectStream, err := objectStorage.Open(l.Ctx, storage.OpenObjectReq{
			ObjectKey:   status.ObjectKey,
			RangeHeader: "bytes=0-0",
		})
		if err != nil {
			return errors.Wrap(err, "打开统一存储导出对象失败")
		}
		_ = objectStream.Reader.Close()
		return nil
	}
	if strings.TrimSpace(status.FilePath) == "" || !fileExists(status.FilePath) {
		return errors.Errorf("导出文件不存在")
	}
	return nil
}

// objectStorage 获取统一对象存储实例，避免导出链路重复创建底层存储客户端。
func (l *AdminExportLogic) objectStorage() (storage.ObjectStorage, error) {
	if l == nil || l.Svc == nil {
		return nil, errors.Errorf("管理员导出服务上下文为空")
	}
	return l.Svc.ObjectStorage()
}

// firstNonNilError 返回首个非空错误，便于统一拼接业务错误链。
func firstNonNilError(values ...error) error {
	for _, err := range values {
		if err != nil {
			return errors.Tag(err)
		}
	}
	return nil
}

// adminStatusText 把管理员状态转换为导出文本。
func adminStatusText(status int) string {
	if status == 1 {
		return "启用"
	}
	return "禁用"
}

// adminMFAStatusText 把管理员 MFA 状态转换为导出文本。
func adminMFAStatusText(status int) string {
	if status == 1 {
		return "已启用"
	}
	return "未启用"
}

// adminNeedResetPasswordText 把是否必须首次改密转换为导出文本。
func adminNeedResetPasswordText(status int) string {
	if status == 1 {
		return "是"
	}
	return "否"
}

// newAdminExportStatusSnapshot 把运行态状态对象转换成 Redis 持久化快照。
func newAdminExportStatusSnapshot(status *types.AdminExportStatusResp) *adminExportStatusSnapshot {
	if status == nil {
		return &adminExportStatusSnapshot{}
	}
	snapshot := &adminExportStatusSnapshot{
		AdminExportStatusResp: *status,
		FilePath:              status.FilePath,
		ObjectKey:             status.ObjectKey,
		StorageType:           status.StorageType,
		ContentType:           status.ContentType,
		LastCursorID:          status.LastCursorID,
		OperatorID:            status.OperatorID,
		RequestFingerprint:    status.RequestFingerprint,
	}
	if len(status.AuthorizedAdminIDs) > 0 {
		snapshot.AuthorizedAdminIDs = append([]int(nil), status.AuthorizedAdminIDs...)
	}
	return snapshot
}

// toStatus 把 Redis 快照还原成运行态状态对象。
func (s *adminExportStatusSnapshot) toStatus() *types.AdminExportStatusResp {
	if s == nil {
		return &types.AdminExportStatusResp{}
	}
	status := s.AdminExportStatusResp
	status.FilePath = s.FilePath
	status.ObjectKey = s.ObjectKey
	status.StorageType = s.StorageType
	status.ContentType = s.ContentType
	status.LastCursorID = s.LastCursorID
	status.OperatorID = s.OperatorID
	status.RequestFingerprint = s.RequestFingerprint
	if len(s.AuthorizedAdminIDs) > 0 {
		status.AuthorizedAdminIDs = append([]int(nil), s.AuthorizedAdminIDs...)
	} else {
		status.AuthorizedAdminIDs = nil
	}
	return &status
}
