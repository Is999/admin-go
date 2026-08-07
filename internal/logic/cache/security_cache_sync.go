package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	keys "admin/common/rediskeys"
	"admin/helper"
	"admin/internal/infra/loggerx"
	"admin/internal/infra/redsync"
	corelogic "admin/internal/logic"
	"admin/internal/model"
	"admin/internal/svc"
	"admin/internal/types"

	"github.com/Is999/go-utils/errors"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	// securityCacheSyncPollInterval 表示补偿任务轮询间隔。
	securityCacheSyncPollInterval = time.Second
	// securityCacheSyncReconcileInterval 表示无阻断信号时数据库兜底对账间隔。
	securityCacheSyncReconcileInterval = 30 * time.Second
	// securityCacheSyncPersistTimeout 限制请求结束后补偿任务落库时间。
	securityCacheSyncPersistTimeout = 2 * time.Second
	// securityCacheSyncLockRetryInterval 表示补偿任务写入后等待 worker 释放锁的重试间隔。
	securityCacheSyncLockRetryInterval = 25 * time.Millisecond
	// securityCacheSyncLockTTL 表示单轮补偿任务的分布式锁租约。
	securityCacheSyncLockTTL = 10 * time.Second
	// securityCacheSyncBatchSize 限制单轮读取任务数量。
	securityCacheSyncBatchSize = 50
	// securityCacheSyncDeleteBatchSize 限制单批 Redis DEL 命令数量。
	securityCacheSyncDeleteBatchSize = 200
	// securityCacheSyncMaxBackoff 表示失败任务最大重试间隔。
	securityCacheSyncMaxBackoff = time.Minute
	// securityCacheSyncMaxErrorRunes 限制任务保存的错误摘要长度。
	securityCacheSyncMaxErrorRunes = 1000
)

// securityCacheSyncPlan 保存一次精确、幂等的安全缓存失效计划。
type securityCacheSyncPlan struct {
	TableKeys   []string `json:"tableKeys"`   // table-cache 物理键，按字典序去重
	RedisKeys   []string `json:"redisKeys"`   // 直接 Redis 键，按字典序去重
	MFAAdminIDs []int    `json:"mfaAdminIds"` // 需要清理 MFA 标记和票据的管理员 ID
}

// securityCacheBarrierSnapshot 保存一次安全缓存阻断键读取结果。
type securityCacheBarrierSnapshot struct {
	Exists bool   // Exists 表示读取时阻断键是否存在。
	Token  string // Token 表示读取时阻断键的唯一版本值。
}

// SecurityCacheSyncWorker 自动重试数据库已提交但尚未完成的安全缓存失效任务。
type SecurityCacheSyncWorker struct {
	svc     *svc.ServiceContext // worker 使用的数据库、Redis 和运行配置
	running atomic.Bool         // 是否已经启动
	cancel  context.CancelFunc  // 停止后台轮询
	wg      sync.WaitGroup      // 等待后台协程退出
}

// NewSecurityCacheSyncWorker 创建安全缓存失效补偿 worker。
func NewSecurityCacheSyncWorker(svcCtx *svc.ServiceContext) *SecurityCacheSyncWorker {
	return &SecurityCacheSyncWorker{svc: svcCtx}
}

// Start 先同步处理历史任务，再启动安全缓存失效补偿轮询。
func (w *SecurityCacheSyncWorker) Start(ctx context.Context) error {
	if w == nil || w.svc == nil || !w.running.CompareAndSwap(false, true) {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := w.runOnce(ctx); err != nil {
		w.running.Store(false)
		return errors.Wrap(err, "启动前处理安全缓存失效补偿失败")
	}
	runCtx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.run(runCtx)
	}()
	return nil
}

// Stop 停止安全缓存失效补偿轮询。
func (w *SecurityCacheSyncWorker) Stop(ctx context.Context) error {
	if w == nil || !w.running.CompareAndSwap(true, false) {
		return nil
	}
	if w.cancel != nil {
		w.cancel()
	}
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "等待安全缓存失效补偿 worker 退出超时")
	}
}

// run 每秒检查 Redis 阻断信号，并低频对账数据库中的漏信号任务。
func (w *SecurityCacheSyncWorker) run(ctx context.Context) {
	ticker := time.NewTicker(securityCacheSyncPollInterval)
	defer ticker.Stop()
	nextReconcileAt := time.Now().Add(securityCacheSyncReconcileInterval)
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			reconcile := !now.Before(nextReconcileAt)
			var err error
			if reconcile {
				// 对账失败也按固定周期重试，阻断信号仍由每秒快速路径处理。
				nextReconcileAt = now.Add(securityCacheSyncReconcileInterval)
				err = w.runOnce(ctx)
			} else {
				err = w.runPendingOnce(ctx)
			}
			if err != nil {
				loggerx.Errorw(ctx, "安全缓存失效补偿失败", err)
				continue
			}
		case <-ctx.Done():
			return
		}
	}
}

// runPendingOnce 仅在进程或 Redis 已标记待补偿时查询数据库并执行补偿。
func (w *SecurityCacheSyncWorker) runPendingOnce(ctx context.Context) error {
	if w.svc.SecurityCacheSyncPending() {
		return errors.Tag(w.runOnce(ctx))
	}
	snapshot, err := w.barrierSnapshot(ctx)
	if err != nil {
		return errors.Tag(err)
	}
	if !snapshot.Exists {
		return nil
	}
	// 先关闭当前进程缓存鉴权，避免数据库检查期间继续使用待失效缓存。
	w.svc.SetSecurityCacheSyncPending(true)
	return errors.Tag(w.runOnce(ctx))
}

// runOnce 检查待补偿状态，并在分布式锁内处理一批到期任务。
func (w *SecurityCacheSyncWorker) runOnce(ctx context.Context) error {
	db := w.svc.WriteDB(svc.DatabaseMain)
	if db == nil {
		return errors.Errorf("安全缓存失效补偿数据库未初始化")
	}
	appID := strings.TrimSpace(w.svc.CurrentConfig().AppID)
	if appID == "" {
		return errors.Errorf("安全缓存失效补偿 app_id 为空")
	}
	err := redsync.WithLockOnce(ctx, w.svc.Rds, keys.SecurityCacheSyncLockRedisKey(), securityCacheSyncLockTTL, func(lockCtx context.Context) error {
		snapshot, err := w.barrierSnapshot(lockCtx)
		if err != nil {
			return errors.Tag(err)
		}
		pending, err := securityCacheSyncTaskExists(lockCtx, db, appID)
		if err != nil {
			return errors.Tag(err)
		}
		if !pending {
			_, err = w.clearBarrierIfUnchanged(lockCtx, snapshot)
			return errors.Tag(err)
		}
		if err := w.markBarrier(lockCtx); err != nil {
			return errors.Tag(err)
		}
		return w.processBatch(lockCtx, db, appID)
	})
	if err != nil {
		if redsync.IsLockTaken(err) {
			return nil
		}
		return errors.Wrap(err, "执行安全缓存失效补偿锁任务失败")
	}
	return nil
}

// processBatch 在锁生命周期上下文内处理一批到期任务。
func (w *SecurityCacheSyncWorker) processBatch(ctx context.Context, db *gorm.DB, appID string) error {
	var tasks []model.SecurityCacheSyncTask
	if err := db.WithContext(ctx).
		Where("app_id = ?", appID).
		Where("next_retry_at <= ?", time.Now()).
		Order("next_retry_at ASC, id ASC").
		Limit(securityCacheSyncBatchSize).
		Find(&tasks).Error; err != nil {
		return errors.Wrap(err, "查询安全缓存失效补偿任务失败")
	}
	base := corelogic.NewBaseLogicWithContext(ctx, w.svc)
	for index := range tasks {
		if err := retrySecurityCacheSyncTask(base, db, &tasks[index]); err != nil {
			loggerx.Errorw(ctx, "安全缓存失效任务重试失败", err,
				logx.Field("task_id", tasks[index].ID),
				logx.Field("attempts", tasks[index].Attempts+1),
			)
		}
	}
	return errors.Tag(w.syncBarrierWithTasks(ctx, db, appID))
}

// barrierSnapshot 读取安全缓存阻断键快照。
func (w *SecurityCacheSyncWorker) barrierSnapshot(ctx context.Context) (securityCacheBarrierSnapshot, error) {
	if w.svc.Rds == nil {
		return securityCacheBarrierSnapshot{}, WrapRedisUnavailable(nil, "读取安全缓存失效阻断状态失败")
	}
	key := keys.SecurityCacheSyncBarrierRedisKey()
	if key == "" {
		return securityCacheBarrierSnapshot{}, errors.Errorf("安全缓存失效阻断键为空")
	}
	token, err := w.svc.Rds.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return securityCacheBarrierSnapshot{}, nil
		}
		return securityCacheBarrierSnapshot{}, WrapRedisUnavailable(err, "读取安全缓存失效阻断状态失败")
	}
	return securityCacheBarrierSnapshot{Exists: true, Token: token}, nil
}

// markBarrier 写入唯一版本的全局阻断键并关闭当前进程缓存鉴权。
func (w *SecurityCacheSyncWorker) markBarrier(ctx context.Context) error {
	w.svc.SetSecurityCacheSyncPending(true)
	if w.svc.Rds == nil {
		return WrapRedisUnavailable(nil, "写入安全缓存失效阻断状态失败")
	}
	key := keys.SecurityCacheSyncBarrierRedisKey()
	if key == "" {
		return errors.Errorf("安全缓存失效阻断键为空")
	}
	if err := w.svc.Rds.Set(ctx, key, uuid.NewString(), 0).Err(); err != nil {
		return WrapRedisUnavailable(err, "写入安全缓存失效阻断状态失败")
	}
	return nil
}

// clearBarrierIfUnchanged 仅在阻断键未被并发改写时清理全局和进程内阻断状态。
func (w *SecurityCacheSyncWorker) clearBarrierIfUnchanged(ctx context.Context, snapshot securityCacheBarrierSnapshot) (bool, error) {
	if w.svc.Rds == nil {
		return false, WrapRedisUnavailable(nil, "清理安全缓存失效阻断状态失败")
	}
	key := keys.SecurityCacheSyncBarrierRedisKey()
	if key == "" {
		return false, errors.Errorf("安全缓存失效阻断键为空")
	}
	expectedExists := "0"
	if snapshot.Exists {
		expectedExists = "1"
	}
	cleared, err := clearSecurityCacheBarrierScript.Run(ctx, w.svc.Rds, []string{key}, expectedExists, snapshot.Token).Int64()
	if err != nil {
		return false, WrapRedisUnavailable(err, "清理安全缓存失效阻断状态失败")
	}
	if cleared == 0 {
		w.svc.SetSecurityCacheSyncPending(true)
		return false, nil
	}
	w.svc.SetSecurityCacheSyncPending(false)
	return true, nil
}

// syncBarrierWithTasks 按数据库任务状态同步阻断键，并保护并发写入的新阻断版本。
func (w *SecurityCacheSyncWorker) syncBarrierWithTasks(ctx context.Context, db *gorm.DB, appID string) error {
	snapshot, err := w.barrierSnapshot(ctx)
	if err != nil {
		return errors.Tag(err)
	}
	pending, err := securityCacheSyncTaskExists(ctx, db, appID)
	if err != nil {
		return errors.Tag(err)
	}
	if pending {
		return errors.Tag(w.markBarrier(ctx))
	}
	_, err = w.clearBarrierIfUnchanged(ctx, snapshot)
	return errors.Tag(err)
}

// executeSecurityCacheSync 执行失效计划；失败时持久化任务供 worker 自动重试。
func executeSecurityCacheSync(base *corelogic.BaseLogic, title string, plan securityCacheSyncPlan) error {
	plan = normalizeSecurityCacheSyncPlan(plan)
	if plan.empty() {
		return nil
	}
	err := applySecurityCacheSyncPlan(base, title, plan)
	if err == nil {
		return nil
	}
	// 当前进程立即失败关闭，避免补偿任务落库期间仍使用尚未失效的旧鉴权缓存。
	if base != nil && base.Svc != nil {
		base.Svc.SetSecurityCacheSyncPending(true)
	}
	if persistErr := persistSecurityCacheSyncTask(base, plan, err); persistErr != nil {
		return errors.Join(err, persistErr)
	}
	return errors.Tag(err)
}

// applySecurityCacheSyncPlan 执行精确表缓存、Redis 键和 MFA 状态失效。
func applySecurityCacheSyncPlan(base *corelogic.BaseLogic, title string, plan securityCacheSyncPlan) error {
	if base == nil || base.Svc == nil {
		return errors.Errorf("%s 缓存上下文未初始化", strings.TrimSpace(title))
	}
	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = errors.Tag(err)
		}
	}
	record(deleteTableCacheKeysExact(base, title, plan.TableKeys))
	record(deleteRedisKeysExact(base, plan.RedisKeys))
	for _, adminID := range plan.MFAAdminIDs {
		record(deleteAdminMFAKeysExact(base, adminID))
	}
	return errors.Tag(firstErr)
}

// deleteTableCacheKeysExact 通过 table-cache 精确失效缓存，不创建补偿任务。
func deleteTableCacheKeysExact(base *corelogic.BaseLogic, title string, cacheKeys []string) error {
	if base == nil {
		return errors.Errorf("%s 缓存上下文未初始化", strings.TrimSpace(title))
	}
	if base.Redis() == nil {
		return WrapRedisUnavailable(nil, strings.TrimSpace(title)+" Redis缓存失效失败")
	}
	cacheKeys = helper.UniqueNonEmptyStrings(cacheKeys)
	if len(cacheKeys) == 0 {
		return nil
	}
	manager, err := TableCacheManager(base)
	if err != nil {
		return errors.Wrapf(err, "%s 初始化表缓存管理器失败", strings.TrimSpace(title))
	}
	var firstErr error
	for index, key := range cacheKeys {
		if err := manager.DeleteByKey(base.Ctx, key); err != nil && firstErr == nil {
			firstErr = errors.Wrapf(err, "%s 精确失效表缓存失败 key_index=%d", strings.TrimSpace(title), index)
		}
	}
	return errors.Tag(firstErr)
}

// deleteRedisKeysExact 分批删除直接 Redis 键。
func deleteRedisKeysExact(base *corelogic.BaseLogic, redisKeys []string) error {
	redisKeys = helper.UniqueNonEmptyStrings(redisKeys)
	for start := 0; start < len(redisKeys); start += securityCacheSyncDeleteBatchSize {
		end := start + securityCacheSyncDeleteBatchSize
		if end > len(redisKeys) {
			end = len(redisKeys)
		}
		if err := base.RdsDelKeys(redisKeys[start:end]...); err != nil {
			return WrapRedisUnavailable(err, "精确删除安全缓存失败")
		}
	}
	return nil
}

// deleteAdminMFAKeysExact 删除管理员登录 MFA 标记和二次票据 Hash。
func deleteAdminMFAKeysExact(base *corelogic.BaseLogic, adminID int) error {
	if adminID <= 0 {
		return nil
	}
	if base.Redis() == nil {
		return WrapRedisUnavailable(nil, "清理管理员MFA缓存失败")
	}
	return errors.Tag(deleteRedisKeysExact(base, []string{
		keys.LoginCheckMFAFlagRedisKey(adminID),
		keys.AdminMFATwoStepRedisKey(adminID),
	}))
}

// persistSecurityCacheSyncTask 按计划摘要幂等写入补偿任务。
func persistSecurityCacheSyncTask(base *corelogic.BaseLogic, plan securityCacheSyncPlan, cause error) error {
	if base == nil || base.Svc == nil {
		return errors.Errorf("安全缓存失效补偿上下文未初始化")
	}
	db := base.Svc.WriteDB(svc.DatabaseMain)
	if db == nil {
		return errors.Errorf("安全缓存失效补偿数据库未初始化")
	}
	payload, err := json.Marshal(plan)
	if err != nil {
		return errors.Wrap(err, "序列化安全缓存失效计划失败")
	}
	sum := sha256.Sum256(payload)
	now := time.Now()
	appID := strings.TrimSpace(base.AppID())
	if appID == "" {
		return errors.Errorf("安全缓存失效补偿 app_id 为空")
	}
	persistCtx := context.Background()
	if base.Ctx != nil {
		persistCtx = context.WithoutCancel(base.Ctx)
	}
	persistCtx, cancel := context.WithTimeout(persistCtx, securityCacheSyncPersistTimeout)
	defer cancel()
	task := model.SecurityCacheSyncTask{
		AppID:       appID,
		Digest:      hex.EncodeToString(sum[:]),
		PayloadJSON: string(payload),
		Revision:    1,
		NextRetryAt: now,
		LastError:   securityCacheSyncErrorText(cause),
	}
	worker := &SecurityCacheSyncWorker{svc: base.Svc}
	// 先发布全局阻断版本，避免任务落库与 worker 清理旧阻断键之间出现放行窗口。
	barrierErr := worker.markBarrier(persistCtx)
	if err := db.WithContext(persistCtx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "app_id"}, {Name: "digest"}},
		DoUpdates: clause.Assignments(map[string]any{
			"payload_json":  task.PayloadJSON,
			"revision":      gorm.Expr("revision + 1"),
			"attempts":      0,
			"next_retry_at": now,
			"last_error":    task.LastError,
			"updated_at":    now,
		}),
	}).Create(&task).Error; err != nil {
		return errors.Join(barrierErr, errors.Wrap(err, "写入安全缓存失效补偿任务失败"))
	}
	base.Svc.SetSecurityCacheSyncPending(true)
	if err := syncSecurityCacheBarrierAfterPersist(persistCtx, base, db, appID); err != nil {
		return errors.Join(barrierErr, err)
	}
	return nil
}

// syncSecurityCacheBarrierAfterPersist 在任务落库后同步全局阻断键，并与 worker 的清理动作串行化。
func syncSecurityCacheBarrierAfterPersist(ctx context.Context, base *corelogic.BaseLogic, db *gorm.DB, appID string) error {
	if base.Redis() == nil {
		return WrapRedisUnavailable(nil, "写入安全缓存失效阻断状态失败")
	}
	worker := &SecurityCacheSyncWorker{svc: base.Svc}
	for {
		err := redsync.WithLock(ctx, base.Redis(), keys.SecurityCacheSyncLockRedisKey(), securityCacheSyncLockTTL, func(lockCtx context.Context) error {
			return errors.Tag(worker.syncBarrierWithTasks(lockCtx, db, appID))
		})
		if err == nil {
			return nil
		}
		if !redsync.IsLockTaken(err) {
			return errors.Wrap(err, "同步安全缓存失效阻断状态失败")
		}
		timer := time.NewTimer(securityCacheSyncLockRetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Wrap(ctx.Err(), "等待同步安全缓存失效阻断状态超时")
		case <-timer.C:
		}
	}
}

// retrySecurityCacheSyncTask 执行单个补偿任务并更新重试状态。
func retrySecurityCacheSyncTask(base *corelogic.BaseLogic, db *gorm.DB, task *model.SecurityCacheSyncTask) error {
	if task == nil {
		return nil
	}
	var plan securityCacheSyncPlan
	if err := json.Unmarshal([]byte(task.PayloadJSON), &plan); err != nil {
		return updateSecurityCacheSyncTaskFailure(base.Ctx, db, task, errors.Wrap(err, "解析安全缓存失效计划失败"))
	}
	if err := applySecurityCacheSyncPlan(base, "SecurityCacheSyncWorker", normalizeSecurityCacheSyncPlan(plan)); err != nil {
		return updateSecurityCacheSyncTaskFailure(base.Ctx, db, task, err)
	}
	result := db.WithContext(base.Ctx).
		Where("id = ? AND app_id = ? AND digest = ? AND revision = ?", task.ID, task.AppID, task.Digest, task.Revision).
		Delete(&model.SecurityCacheSyncTask{})
	if result.Error != nil {
		return errors.Wrapf(result.Error, "删除已完成安全缓存失效任务失败 task_id=%d", task.ID)
	}
	// 删除未命中表示同摘要计划在执行期间再次写入；保留新修订任务供下一轮重试。
	return nil
}

// updateSecurityCacheSyncTaskFailure 记录失败次数和下一次指数退避时间。
func updateSecurityCacheSyncTaskFailure(ctx context.Context, db *gorm.DB, task *model.SecurityCacheSyncTask, cause error) error {
	attempts := task.Attempts + 1
	nextRetryAt := time.Now().Add(securityCacheSyncBackoff(attempts))
	result := db.WithContext(ctx).Model(&model.SecurityCacheSyncTask{}).
		Where("id = ? AND app_id = ? AND digest = ? AND revision = ?", task.ID, task.AppID, task.Digest, task.Revision).
		Updates(map[string]any{
			"attempts":      attempts,
			"next_retry_at": nextRetryAt,
			"last_error":    securityCacheSyncErrorText(cause),
			"updated_at":    time.Now(),
		})
	if result.Error != nil {
		return errors.Join(cause, errors.Wrapf(result.Error, "更新安全缓存失效任务失败 task_id=%d", task.ID))
	}
	// 更新未命中表示已有更新的修订任务，其 next_retry_at 已由写入方推进到当前时间。
	return errors.Tag(cause)
}

// securityCacheSyncTaskExists 按应用索引确认是否存在尚未完成的补偿任务。
func securityCacheSyncTaskExists(ctx context.Context, db *gorm.DB, appID string) (bool, error) {
	var task model.SecurityCacheSyncTask
	if err := db.WithContext(ctx).Select("id").Where("app_id = ?", strings.TrimSpace(appID)).Take(&task).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, errors.Wrap(err, "检查安全缓存失效补偿任务失败")
	}
	return true, nil
}

// securityCacheSyncBackoff 根据已失败次数计算最大一分钟的指数退避。
func securityCacheSyncBackoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	if attempts > 7 {
		attempts = 7
	}
	delay := time.Second << (attempts - 1)
	if delay > securityCacheSyncMaxBackoff {
		return securityCacheSyncMaxBackoff
	}
	return delay
}

// securityCacheSyncErrorText 返回适合落库的错误摘要。
func securityCacheSyncErrorText(err error) string {
	if err == nil {
		return ""
	}
	runes := []rune(strings.TrimSpace(err.Error()))
	if len(runes) > securityCacheSyncMaxErrorRunes {
		runes = runes[:securityCacheSyncMaxErrorRunes]
	}
	return string(runes)
}

// normalizeSecurityCacheSyncPlan 归一化计划，保证摘要稳定且重试范围准确。
func normalizeSecurityCacheSyncPlan(plan securityCacheSyncPlan) securityCacheSyncPlan {
	plan.TableKeys = helper.UniqueNonEmptyStrings(plan.TableKeys)
	plan.RedisKeys = helper.UniqueNonEmptyStrings(plan.RedisKeys)
	plan.MFAAdminIDs = types.UniquePositiveInts(plan.MFAAdminIDs)
	sort.Strings(plan.TableKeys)
	sort.Strings(plan.RedisKeys)
	sort.Ints(plan.MFAAdminIDs)
	return plan
}

// empty 判断当前计划是否没有任何失效目标。
func (p securityCacheSyncPlan) empty() bool {
	return len(p.TableKeys) == 0 && len(p.RedisKeys) == 0 && len(p.MFAAdminIDs) == 0
}
