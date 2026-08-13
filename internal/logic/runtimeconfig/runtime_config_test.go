package runtimeconfig

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"admin/internal/config"
	"admin/internal/jobs/archive"
	"admin/internal/model"
	"admin/internal/svc"
	tasklimits "admin/internal/task/limits"
	"admin/internal/types"

	mysqldriver "github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// runtimeConfigTaskQueueStub 注入运行配置测试需要的周期任务校验结果。
type runtimeConfigTaskQueueStub struct {
	svc.TaskQueue       // 其它任务能力不参与当前测试
	validationErr error // validationErr 是周期任务运行时校验错误
	validateCalls int   // validateCalls 记录周期任务校验调用次数
}

// IsEnabled 返回测试任务系统启用状态。
func (s *runtimeConfigTaskQueueStub) IsEnabled() bool { return true }

// ValidatePeriodicTaskConfigs 返回注入的运行时校验结果。
func (s *runtimeConfigTaskQueueStub) ValidatePeriodicTaskConfigs([]config.TaskPeriodicConfig) error {
	s.validateCalls++
	return s.validationErr
}

// TestCheckRuntimeConfigUpdatedRejectsMissingDraft 验证更新草稿不存在时返回明确错误。
func TestCheckRuntimeConfigUpdatedRejectsMissingDraft(t *testing.T) {
	err := checkRuntimeConfigUpdated(&gorm.DB{RowsAffected: 0}, 42, "周期任务草稿")
	if err == nil || !strings.Contains(err.Error(), "周期任务草稿不存在: 42") {
		t.Fatalf("checkRuntimeConfigUpdated() error = %v", err)
	}
}

// TestCheckRuntimeConfigUpdatedPropagatesDatabaseError 验证数据库错误不会被行数判断吞掉。
func TestCheckRuntimeConfigUpdatedPropagatesDatabaseError(t *testing.T) {
	want := errors.New("db down")
	err := checkRuntimeConfigUpdated(&gorm.DB{Error: want, RowsAffected: 1}, 42, "归档任务草稿")
	if err == nil || !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("checkRuntimeConfigUpdated() error = %v", err)
	}
}

// TestCheckRuntimeConfigUpdatedAcceptsAffectedRow 验证成功更新一行草稿时不返回错误。
func TestCheckRuntimeConfigUpdatedAcceptsAffectedRow(t *testing.T) {
	if err := checkRuntimeConfigUpdated(&gorm.DB{RowsAffected: 1}, 42, "周期任务草稿"); err != nil {
		t.Fatalf("checkRuntimeConfigUpdated() error = %v", err)
	}
}

// TestRuntimeConfigSnapshotEmpty 验证运行时快照空值判断覆盖周期任务和归档任务。
func TestRuntimeConfigSnapshotEmpty(t *testing.T) {
	if !runtimeConfigSnapshotEmpty(ReleaseSnapshot{}) {
		t.Fatal("空快照应判定为空")
	}
	if runtimeConfigSnapshotEmpty(ReleaseSnapshot{TaskPeriodic: []config.TaskPeriodicConfig{{Name: "demo"}}}) {
		t.Fatal("包含周期任务的快照不应判定为空")
	}
	if runtimeConfigSnapshotEmpty(ReleaseSnapshot{ArchiveJobs: []config.ArchiveJobConfig{{Name: "archive"}}}) {
		t.Fatal("包含归档任务的快照不应判定为空")
	}
}

// TestOverviewRejectsUnavailableActiveState 验证 active 状态依赖不可用时返回受控失败，不能以零版本概览伪装查询成功。
func TestOverviewRejectsUnavailableActiveState(t *testing.T) {
	svcCtx := svc.NewServiceContext(config.Config{}, svc.Dependencies{})
	logicObj := NewRuntimeConfigLogicWithContext(context.Background(), svcCtx)
	result := logicObj.Overview(&types.RuntimeConfigOverviewReq{})
	if result == nil || !result.IsFailure() {
		t.Fatalf("Overview() result = %+v, want controlled failure", result)
	}
	if result.Error == nil || !strings.Contains(result.Error.Error(), "active 状态") {
		t.Fatalf("Overview() error = %v, want active state dependency error", result.Error)
	}
}

// TestArchiveProgressToRespPreservesWatermarkAndEstimate 验证执行详情映射保留水位、滞后和区间估算进度。
func TestArchiveProgressToRespPreservesWatermarkAndEstimate(t *testing.T) {
	now := time.Date(2026, time.August, 1, 9, 30, 0, 0, time.Local)
	estimate := 50.0
	resp := archiveProgressToResp(5, archive.Progress{
		JobName:        "admin_log",
		RuntimeMatched: true,
		RuntimeEnabled: true,
		SchemaReady:    true,
		Phase:          archive.ProgressPhaseRunning,
		WatermarkTime:  sql.NullTime{Time: now.Add(-24 * time.Hour), Valid: true},
		LagSeconds:     sql.NullInt64{Int64: 86_400, Valid: true},
		CurrentSegment: &archive.ProgressSegment{
			ID:                       18,
			Status:                   "running",
			RangeStart:               now.Add(-24 * time.Hour),
			RangeEnd:                 now,
			LastArchivedID:           9_007_199_254_740_993,
			EstimatedProgressPercent: &estimate,
		},
		FetchedAt: now,
	})
	if resp.JobID != 5 || resp.JobName != "admin_log" || resp.WatermarkTime == "" {
		t.Fatalf("执行详情基础字段映射异常: %+v", resp)
	}
	if resp.LagSeconds == nil || *resp.LagSeconds != 86_400 {
		t.Fatalf("LagSeconds=%v want 86400", resp.LagSeconds)
	}
	if resp.CurrentSegment == nil || resp.CurrentSegment.EstimatedProgressPercent == nil || *resp.CurrentSegment.EstimatedProgressPercent != 50 {
		t.Fatalf("CurrentSegment=%+v want estimate 50", resp.CurrentSegment)
	}
	if resp.CurrentSegment.LastArchivedID != "9007199254740993" {
		t.Fatalf("LastArchivedID=%q want exact bigint string", resp.CurrentSegment.LastArchivedID)
	}
	if resp.RecentSegments == nil {
		t.Fatal("RecentSegments 应返回空数组而不是 null")
	}
	segmentJSON, err := json.Marshal(archiveSegmentToItem(archive.ProgressSegment{}))
	if err != nil {
		t.Fatalf("序列化空归档区间失败: %v", err)
	}
	if !strings.Contains(string(segmentJSON), `"estimatedProgressPercent":null`) {
		t.Fatalf("非复制阶段应显式返回 null 估算进度: %s", segmentJSON)
	}
}

// TestValidateSnapshotRejectsPeriodicResourceOverridesAboveHardLimits 校验发布预检不会接受无界周期任务参数。
func TestValidateSnapshotRejectsPeriodicResourceOverridesAboveHardLimits(t *testing.T) {
	base := config.TaskPeriodicConfig{
		Name:     "oversized-resource",
		Cron:     "0 * * * *",
		Workflow: "demo.workflow",
	}
	tests := []struct {
		name    string                           // 测试场景
		update  func(*config.TaskPeriodicConfig) // 设置越界参数
		wantErr string                           // 期望错误片段
	}{
		{
			name: "retry",
			update: func(item *config.TaskPeriodicConfig) {
				item.Retry = tasklimits.MaxRetry + 1
			},
			wantErr: "retry",
		},
		{
			name: "timeout",
			update: func(item *config.TaskPeriodicConfig) {
				item.TimeoutSeconds = tasklimits.MaxTimeoutSeconds + 1
			},
			wantErr: "timeout_seconds",
		},
		{
			name: "shard total",
			update: func(item *config.TaskPeriodicConfig) {
				item.ShardTotal = tasklimits.MaxShardTotal + 1
			},
			wantErr: "shard_total",
		},
		{
			name: "unique ttl",
			update: func(item *config.TaskPeriodicConfig) {
				item.UniqueTTLSeconds = tasklimits.MaxUniqueTTLSeconds + 1
			},
			wantErr: "unique_ttl_seconds",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := base
			tt.update(&item)
			_, err := ValidateSnapshot(ReleaseSnapshot{TaskPeriodic: []config.TaskPeriodicConfig{item}})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateSnapshot() error = %v, want %q limit", err, tt.wantErr)
			}
		})
	}
}

// TestValidateSnapshotRejectsTooManyPeriodicTasks 校验发布快照不会绕过周期任务总量上限。
func TestValidateSnapshotRejectsTooManyPeriodicTasks(t *testing.T) {
	items := make([]config.TaskPeriodicConfig, tasklimits.MaxPeriodicCount+1)
	if _, err := ValidateSnapshot(ReleaseSnapshot{TaskPeriodic: items}); err == nil || !strings.Contains(err.Error(), "周期任务不能超过") {
		t.Fatalf("ValidateSnapshot() error = %v, want periodic count limit", err)
	}
}

// TestValidateSnapshotAcceptsExactTaskLimits 验证一万条周期任务和一万条归档任务是可发布边界而不是越界值。
func TestValidateSnapshotAcceptsExactTaskLimits(t *testing.T) {
	periodicItems := make([]config.TaskPeriodicConfig, tasklimits.MaxPeriodicCount)
	for index := range periodicItems {
		periodicItems[index] = config.TaskPeriodicConfig{
			Name:     fmt.Sprintf("periodic-%d", index),
			Cron:     "0 * * * *",
			Workflow: "demo.workflow",
		}
	}
	archiveItems := make([]config.ArchiveJobConfig, tasklimits.MaxArchiveJobCount)
	for index := range archiveItems {
		archiveItems[index] = config.ArchiveJobConfig{
			Name:      fmt.Sprintf("archive-%d", index),
			TableName: "demo_table",
		}
	}
	if _, err := ValidateSnapshot(ReleaseSnapshot{TaskPeriodic: periodicItems, ArchiveJobs: archiveItems}); err != nil {
		t.Fatalf("ValidateSnapshot() exact limits error = %v", err)
	}
	_, jsonText, yamlText, _, err := encodeReleaseSnapshot(ReleaseSnapshot{TaskPeriodic: periodicItems, ArchiveJobs: archiveItems})
	if err != nil {
		t.Fatalf("encodeReleaseSnapshot() exact limits error = %v", err)
	}
	if len(jsonText) > maxRuntimeConfigSnapshotBytes || len(yamlText) > maxRuntimeConfigSnapshotBytes {
		t.Fatalf("exact limit snapshot exceeds storage boundary: json=%d yaml=%d", len(jsonText), len(yamlText))
	}
	t.Logf("exact limit snapshot bytes: json=%d yaml=%d", len(jsonText), len(yamlText))
}

// TestEncodeSnapshotRejectsOversizedSerializedPayload 验证字段内容过大时在写 MySQL 和 Redis 前返回明确边界错误。
func TestEncodeSnapshotRejectsOversizedSerializedPayload(t *testing.T) {
	snapshot := ReleaseSnapshot{ArchiveJobs: []config.ArchiveJobConfig{{
		Name:             "oversized-archive",
		TableName:        "demo_table",
		ArchiveCondition: strings.Repeat("x", maxRuntimeConfigSnapshotBytes),
	}}}
	if _, _, _, err := EncodeSnapshot(snapshot); err == nil || !strings.Contains(err.Error(), "JSON 快照不能超过") {
		t.Fatalf("EncodeSnapshot() error = %v, want encoded size limit", err)
	}
}

// TestValidateSnapshotRejectsTooManyArchiveJobs 校验归档任务数量不能绕过草稿覆盖、回滚和发布共用的快照边界。
func TestValidateSnapshotRejectsTooManyArchiveJobs(t *testing.T) {
	items := make([]config.ArchiveJobConfig, tasklimits.MaxArchiveJobCount+1)
	if _, err := ValidateSnapshot(ReleaseSnapshot{ArchiveJobs: items}); err == nil || !strings.Contains(err.Error(), "归档任务不能超过") {
		t.Fatalf("ValidateSnapshot() error = %v, want archive count limit", err)
	}
}

// TestPublishSnapshotRejectsOversizedSnapshotBeforeDatabase 验证回滚或启动发布的越界快照在打开写事务前失败。
func TestPublishSnapshotRejectsOversizedSnapshotBeforeDatabase(t *testing.T) {
	logicObj := &RuntimeConfigLogic{}
	snapshot := ReleaseSnapshot{ArchiveJobs: make([]config.ArchiveJobConfig, tasklimits.MaxArchiveJobCount+1)}
	if _, err := logicObj.publishSnapshot(snapshot, "test", 0); err == nil || !strings.Contains(err.Error(), "归档任务不能超过") {
		t.Fatalf("publishSnapshot() error = %v, want archive count limit", err)
	}
}

// TestPublishSnapshotRejectsRuntimeInvalidPeriodicConfig 验证发布写库前会执行任务运行时校验。
func TestPublishSnapshotRejectsRuntimeInvalidPeriodicConfig(t *testing.T) {
	validator := &runtimeConfigTaskQueueStub{validationErr: errors.New("workflow shard_total max=1")}
	svcCtx := svc.NewServiceContext(config.Config{}, svc.Dependencies{})
	svcCtx.Task = validator
	logicObj := NewRuntimeConfigLogicWithContext(context.Background(), svcCtx)
	snapshot := ReleaseSnapshot{TaskPeriodic: []config.TaskPeriodicConfig{{
		Name:       "single-shard-periodic",
		Cron:       "*/5 * * * *",
		Workflow:   "single-shard.workflow",
		ShardTotal: 2,
	}}}

	if _, err := logicObj.publishSnapshot(snapshot, "test", 0); err == nil || !strings.Contains(err.Error(), "max=1") {
		t.Fatalf("publishSnapshot() error = %v, want runtime validation error", err)
	}
	if validator.validateCalls != 1 {
		t.Fatalf("运行时周期任务校验调用次数 = %d, want 1", validator.validateCalls)
	}
}

// TestStateCacheDecodesTableCacheHashStrings 验证 table-cache 字符串哈希值可解码为状态缓存。
func TestStateCacheDecodesTableCacheHashStrings(t *testing.T) {
	payload, err := json.Marshal(map[string]string{
		"active_release_id": "1",
		"active_version":    "2",
		"active_checksum":   "abc",
		"published_at_unix": "1782215750",
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var got StateCache
	if err = json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("json.Unmarshal(StateCache) error = %v", err)
	}
	if got.ActiveReleaseID != 1 || got.ActiveVersion != 2 || got.ActiveChecksum != "abc" || got.PublishedAtUnix != 1782215750 {
		t.Fatalf("StateCache decoded mismatch: %+v", got)
	}
}

// TestArchiveConfigToModelDefaultsDelayDays 验证归档配置入库时删除延迟天数默认跟随热数据保留天数。
func TestArchiveConfigToModelDefaultsDelayDays(t *testing.T) {
	row := archiveConfigToModel(config.ArchiveJobConfig{
		Name:              "admin_log",
		TableName:         "admin_log",
		HotKeepDays:       32,
		ArchiveDelayDays:  2,
		DeleteDelayDays:   0,
		DeleteBatchSize:   1000,
		MaxHistoryTables:  12,
		ArchiveWindowMode: "auto",
	}, 0, 0)
	if row.ArchiveDelayDays != 2 {
		t.Fatalf("ArchiveDelayDays=%d want 2", row.ArchiveDelayDays)
	}
	if row.DeleteDelayDays != 32 {
		t.Fatalf("DeleteDelayDays=%d want 32", row.DeleteDelayDays)
	}
}

// TestCurrentSnapshotFromConfigDefaultsArchiveDelayDays 验证静态配置生成快照时补齐归档延迟默认值。
func TestCurrentSnapshotFromConfigDefaultsArchiveDelayDays(t *testing.T) {
	snapshot := CurrentSnapshotFromConfig(config.Config{
		Archive: config.ArchiveConfig{Jobs: []config.ArchiveJobConfig{
			{Name: "admin_log", TableName: "admin_log", HotKeepDays: 32},
		}},
	})
	if len(snapshot.ArchiveJobs) != 1 {
		t.Fatalf("ArchiveJobs len=%d want 1", len(snapshot.ArchiveJobs))
	}
	job := snapshot.ArchiveJobs[0]
	if job.ArchiveDelayDays != 32 {
		t.Fatalf("ArchiveDelayDays=%d want 32", job.ArchiveDelayDays)
	}
	if job.DeleteDelayDays != 32 {
		t.Fatalf("DeleteDelayDays=%d want 32", job.DeleteDelayDays)
	}
}

// TestCurrentSnapshotFromConfigDefaultsPeriodicEnabled 验证周期任务快照缺省 enabled 时按启用处理。
func TestCurrentSnapshotFromConfigDefaultsPeriodicEnabled(t *testing.T) {
	snapshot := CurrentSnapshotFromConfig(config.Config{
		Task: config.TaskQueueConfig{Periodic: []config.TaskPeriodicConfig{
			{Name: "archive-admin-log-hourly", Cron: "5 * * * *", Workflow: "archive.run"},
		}},
	})
	if len(snapshot.TaskPeriodic) != 1 {
		t.Fatalf("TaskPeriodic len=%d want 1", len(snapshot.TaskPeriodic))
	}
	if snapshot.TaskPeriodic[0].Enabled == nil || !*snapshot.TaskPeriodic[0].Enabled {
		t.Fatalf("期望周期任务默认启用，实际=%v", snapshot.TaskPeriodic[0].Enabled)
	}
}

// TestEncodeSnapshotUsesNormalizedArchiveDelayDays 验证编码快照前会写入归一化后的归档延迟字段。
func TestEncodeSnapshotUsesNormalizedArchiveDelayDays(t *testing.T) {
	snapshot := normalizeReleaseSnapshot(ReleaseSnapshot{ArchiveJobs: []config.ArchiveJobConfig{
		{Name: "admin_log", TableName: "admin_log", HotKeepDays: 32},
	}})
	jsonText, _, _, err := EncodeSnapshot(snapshot)
	if err != nil {
		t.Fatalf("EncodeSnapshot() error = %v", err)
	}
	if !strings.Contains(jsonText, `"archive_delay_days":32`) || !strings.Contains(jsonText, `"delete_delay_days":32`) {
		t.Fatalf("encoded snapshot missing normalized delay days: %s", jsonText)
	}
}

// TestEncodeReleaseSnapshotNormalizesDefaults 验证概览、预检和发布共用的快照编码会先补齐默认值。
func TestEncodeReleaseSnapshotNormalizesDefaults(t *testing.T) {
	snapshot, jsonText, _, checksum, err := encodeReleaseSnapshot(ReleaseSnapshot{
		ArchiveJobs: []config.ArchiveJobConfig{
			{Name: "admin_log", TableName: "admin_log", HotKeepDays: 32},
		},
		TaskPeriodic: []config.TaskPeriodicConfig{
			{Name: "archive-admin-log-hourly", Cron: "5 * * * *", Workflow: "archive.run"},
		},
	})
	if err != nil {
		t.Fatalf("encodeReleaseSnapshot() error = %v", err)
	}
	if checksum == "" {
		t.Fatal("checksum 不能为空")
	}
	if snapshot.ArchiveJobs[0].ArchiveDelayDays != 32 || snapshot.ArchiveJobs[0].DeleteDelayDays != 32 {
		t.Fatalf("归档默认值未补齐: %+v", snapshot.ArchiveJobs[0])
	}
	if snapshot.TaskPeriodic[0].Enabled == nil || !*snapshot.TaskPeriodic[0].Enabled {
		t.Fatalf("周期任务默认启用未补齐: %+v", snapshot.TaskPeriodic[0].Enabled)
	}
	if !strings.Contains(jsonText, `"archive_delay_days":32`) || !strings.Contains(jsonText, `"enabled":true`) {
		t.Fatalf("encoded snapshot missing normalized defaults: %s", jsonText)
	}
}

// TestPeriodicConfigToModelDefaultsEnabled 验证全量写入运行配置草稿时周期任务默认启用。
func TestPeriodicConfigToModelDefaultsEnabled(t *testing.T) {
	row := periodicConfigToModel(config.TaskPeriodicConfig{
		Name:     "archive-admin-log-hourly",
		Cron:     "5 * * * *",
		Workflow: "archive.run",
	}, 7, 0)
	if !row.Enabled {
		t.Fatal("期望缺省 enabled 的周期任务写入草稿时默认启用")
	}
}

// TestArchiveReqToModelDefaultsDelayDays 验证后台保存归档任务时补齐归档和删除延迟默认值。
func TestArchiveReqToModelDefaultsDelayDays(t *testing.T) {
	row := archiveReqToModel(&types.SaveRuntimeArchiveJobReq{
		Name:        "admin_log",
		TableName:   "admin_log",
		HotKeepDays: 45,
	}, 7)
	if row.ArchiveDelayDays != 45 {
		t.Fatalf("ArchiveDelayDays=%d want 45", row.ArchiveDelayDays)
	}
	if row.DeleteDelayDays != 45 {
		t.Fatalf("DeleteDelayDays=%d want 45", row.DeleteDelayDays)
	}
}

// TestRuntimeConfigReloadMatchesRelease 验证只有完整匹配本次发布的重载回执才能标记已应用。
func TestRuntimeConfigReloadMatchesRelease(t *testing.T) {
	// release 表示本次已持久化的发布记录。
	release := model.RuntimeConfigRelease{ID: 13, VersionNo: 7, Checksum: "checksum-7"}
	// tests 覆盖无回执和各个字段不匹配的场景。
	tests := []struct {
		name   string                        // name 表示测试场景。
		reload svc.RuntimeConfigReloadResult // reload 表示运行态重载回执。
		want   bool                          // want 表示是否应标记已应用。
	}{
		{name: "missing receipt"},
		{name: "release mismatch", reload: svc.RuntimeConfigReloadResult{ReleaseID: 12, VersionNo: 7, Checksum: "checksum-7"}},
		{name: "version mismatch", reload: svc.RuntimeConfigReloadResult{ReleaseID: 13, VersionNo: 6, Checksum: "checksum-7"}},
		{name: "checksum mismatch", reload: svc.RuntimeConfigReloadResult{ReleaseID: 13, VersionNo: 7, Checksum: "checksum-6"}},
		{name: "exact match", reload: svc.RuntimeConfigReloadResult{ReleaseID: 13, VersionNo: 7, Checksum: "checksum-7"}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runtimeConfigReloadMatchesRelease(release, tt.reload); got != tt.want {
				t.Fatalf("runtimeConfigReloadMatchesRelease() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestPublishPreparedSnapshotSerializesConcurrentDraftSave 验证发布持有状态行锁期间，并发保存必须等待到版本提交后再修改草稿。
func TestPublishPreparedSnapshotSerializesConcurrentDraftSave(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("INTEGRATION_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("INTEGRATION_MYSQL_DSN 未配置")
	}
	db := openRuntimeConfigIntegrationDB(t, dsn)
	if err := db.AutoMigrate(
		&model.RuntimeConfigState{},
		&model.RuntimeConfigRelease{},
		&model.RuntimeTaskPeriodic{},
		&model.RuntimeArchiveJob{},
	); err != nil {
		t.Fatalf("初始化运行配置并发测试表失败: %v", err)
	}
	if err := db.Create(&model.RuntimeConfigState{ID: 1, PublishedAt: time.Now()}).Error; err != nil {
		t.Fatalf("写入运行配置状态失败: %v", err)
	}
	if err := db.Create(&model.RuntimeTaskPeriodic{
		Name:     "before-publish",
		Enabled:  true,
		Cron:     "0 * * * *",
		Workflow: "demo.workflow",
	}).Error; err != nil {
		t.Fatalf("写入发布前草稿失败: %v", err)
	}

	svcCtx := svc.NewServiceContext(config.Config{}, svc.Dependencies{
		SiteDBs: svc.SiteDatabases{MainDB: db},
	})
	logicObj := NewRuntimeConfigLogicWithContext(context.Background(), svcCtx)
	lockHeld := make(chan struct{})
	continuePublish := make(chan struct{})
	publishDone := make(chan error, 1)
	go func() {
		_, publishErr := logicObj.publishPreparedSnapshot(func(tx *gorm.DB) (preparedRuntimeConfigSnapshot, error) {
			close(lockHeld)
			<-continuePublish
			snapshot, buildErr := logicObj.buildDraftSnapshotDB(tx)
			if buildErr != nil {
				return preparedRuntimeConfigSnapshot{}, buildErr
			}
			return logicObj.prepareSnapshot(snapshot)
		}, nil, "atomic publish test", 0)
		publishDone <- publishErr
	}()
	select {
	case <-lockHeld:
	case <-time.After(5 * time.Second):
		t.Fatal("发布事务未在五秒内取得状态行锁")
	}

	saveDone := make(chan *types.BizResult, 1)
	go func() {
		saveDone <- logicObj.SavePeriodicTask(&types.SaveRuntimeTaskPeriodicReq{
			Name:     "after-publish",
			Enabled:  true,
			Cron:     "5 * * * *",
			Workflow: "demo.workflow",
		})
	}()
	select {
	case result := <-saveDone:
		t.Fatalf("并发保存越过发布状态行锁: %+v", result)
	case <-time.After(200 * time.Millisecond):
	}
	close(continuePublish)
	if err := <-publishDone; err != nil {
		t.Fatalf("发布事务失败: %v", err)
	}
	select {
	case result := <-saveDone:
		if result == nil || result.IsFailure() {
			t.Fatalf("发布提交后的并发保存失败: %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("并发保存未在发布提交后五秒内完成")
	}

	var release model.RuntimeConfigRelease
	if err := db.Order("version_no DESC").First(&release).Error; err != nil {
		t.Fatalf("读取发布版本失败: %v", err)
	}
	snapshot, err := DecodeSnapshotJSON(release.SnapshotJSON)
	if err != nil {
		t.Fatalf("解析发布快照失败: %v", err)
	}
	if len(snapshot.TaskPeriodic) != 1 || snapshot.TaskPeriodic[0].Name != "before-publish" {
		t.Fatalf("发布快照混入锁后并发保存内容: %+v", snapshot.TaskPeriodic)
	}
	var draftCount int64
	if err = db.Model(&model.RuntimeTaskPeriodic{}).Count(&draftCount).Error; err != nil {
		t.Fatalf("统计发布后草稿失败: %v", err)
	}
	if draftCount != 2 {
		t.Fatalf("发布后草稿数量=%d want 2", draftCount)
	}
}

// TestPublishInitialDraftAvoidsDuplicateRelease 验证两个实例并发首次发布时只有一个实例创建 release，另一个识别既有 active 版本。
func TestPublishInitialDraftAvoidsDuplicateRelease(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("INTEGRATION_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("INTEGRATION_MYSQL_DSN 未配置")
	}
	db := openRuntimeConfigIntegrationDB(t, dsn)
	if err := db.AutoMigrate(
		&model.RuntimeConfigState{},
		&model.RuntimeConfigRelease{},
		&model.RuntimeTaskPeriodic{},
		&model.RuntimeArchiveJob{},
	); err != nil {
		t.Fatalf("初始化运行配置首次发布测试表失败: %v", err)
	}
	if err := db.Create(&model.RuntimeConfigState{ID: 1, PublishedAt: time.Now()}).Error; err != nil {
		t.Fatalf("写入运行配置状态失败: %v", err)
	}
	if err := db.Create(&model.RuntimeTaskPeriodic{
		Name:     "initial-periodic",
		Enabled:  true,
		Cron:     "0 * * * *",
		Workflow: "demo.workflow",
	}).Error; err != nil {
		t.Fatalf("写入初始化草稿失败: %v", err)
	}

	svcCtx := svc.NewServiceContext(config.Config{}, svc.Dependencies{
		SiteDBs: svc.SiteDatabases{MainDB: db},
	})
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			logicObj := NewRuntimeConfigLogicWithContext(context.Background(), svcCtx)
			_, err := logicObj.publishInitialDraft()
			results <- err
		}()
	}
	close(start)
	successes := 0
	alreadyPublished := 0
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			successes++
		case errors.Is(err, errRuntimeConfigInitialReleaseExists):
			alreadyPublished++
		default:
			t.Fatalf("并发首次发布返回非预期错误: %v", err)
		}
	}
	if successes != 1 || alreadyPublished != 1 {
		t.Fatalf("首次发布结果 success=%d already_published=%d want 1/1", successes, alreadyPublished)
	}
	var releaseCount int64
	if err := db.Model(&model.RuntimeConfigRelease{}).Count(&releaseCount).Error; err != nil {
		t.Fatalf("统计初始化 release 失败: %v", err)
	}
	if releaseCount != 1 {
		t.Fatalf("初始化 release 数量=%d want 1", releaseCount)
	}
	var state model.RuntimeConfigState
	if err := db.First(&state, 1).Error; err != nil {
		t.Fatalf("读取初始化 active 状态失败: %v", err)
	}
	if state.ActiveReleaseID == 0 || state.ActiveVersion != 1 {
		t.Fatalf("初始化 active 状态=%+v want release_id>0 version=1", state)
	}
}

// openRuntimeConfigIntegrationDB 为并发测试创建独立数据库，结束后关闭连接并删除，不修改共享测试库中的业务表。
func openRuntimeConfigIntegrationDB(t *testing.T, dsn string) *gorm.DB {
	t.Helper()
	parsed, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("解析集成测试 DSN 失败: %v", err)
	}
	databaseName := fmt.Sprintf("runtime_config_atomic_%d", time.Now().UnixNano())
	adminConfig := *parsed
	adminConfig.DBName = ""
	adminDB, err := sql.Open("mysql", adminConfig.FormatDSN())
	if err != nil {
		t.Fatalf("打开集成测试管理连接失败: %v", err)
	}
	if _, err = adminDB.Exec("CREATE DATABASE `" + databaseName + "` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		_ = adminDB.Close()
		t.Fatalf("创建独立运行配置测试库失败: %v", err)
	}
	testConfig := *parsed
	testConfig.DBName = databaseName
	db, err := gorm.Open(gormmysql.Open(testConfig.FormatDSN()), &gorm.Config{})
	if err != nil {
		_, _ = adminDB.Exec("DROP DATABASE IF EXISTS `" + databaseName + "`")
		_ = adminDB.Close()
		t.Fatalf("打开独立运行配置测试库失败: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		_, _ = adminDB.Exec("DROP DATABASE IF EXISTS `" + databaseName + "`")
		_ = adminDB.Close()
		t.Fatalf("获取独立运行配置 SQL 连接失败: %v", err)
	}
	t.Cleanup(func() {
		_ = sqlDB.Close()
		_, _ = adminDB.Exec("DROP DATABASE IF EXISTS `" + databaseName + "`")
		_ = adminDB.Close()
	})
	return db
}
