package user

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	i18n "admin/common/i18n"
	"admin/common/idgen"
	"admin/internal/config"
	"admin/internal/model"
	"admin/internal/svc"
	"admin/internal/types"
	"admin/pkg/excel"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/dbresolver"
)

// TestBuildUserProfileUpdatesKeepsExplicitEmptyValue 验证空字符串是显式清空资料，不应被忽略。
func TestBuildUserProfileUpdatesKeepsExplicitEmptyValue(t *testing.T) {
	nickname := "  新昵称  "
	email := "  "
	req := &types.UpdateUserReq{
		Nickname: &nickname,
		Email:    &email,
	}
	updates := buildUserProfileUpdates(req)
	if updates["nickname"] != "新昵称" {
		t.Fatalf("nickname update = %v, want trimmed nickname", updates["nickname"])
	}
	if value, ok := updates["email"]; !ok || value != "" {
		t.Fatalf("email update = %v ok=%v, want explicit empty value", value, ok)
	}
	if _, ok := updates["phone"]; ok {
		t.Fatalf("phone should not be updated when request field is nil")
	}
}

// TestUserDatabaseUsesMainDB 验证前台用户管理固定使用默认主库。
func TestUserDatabaseUsesMainDB(t *testing.T) {
	if userDatabase != svc.DatabaseMain {
		t.Fatalf("userDatabase = %q, want %q", userDatabase, svc.DatabaseMain)
	}
}

// TestUserModelUsesUserTable 验证后台管理前台用户时读取统一 user 表。
func TestUserModelUsesUserTable(t *testing.T) {
	if model.TableNameUser != "user" {
		t.Fatalf("TableNameUser = %q, want user", model.TableNameUser)
	}
	if tableName := (&model.User{}).TableName(); tableName != "user" {
		t.Fatalf("User.TableName() = %q, want user", tableName)
	}
}

// TestValidateUserIdentityListReq 验证身份目录列表不会退化为扫描全部用户物理表。
func TestValidateUserIdentityListReq(t *testing.T) {
	req := &types.UserListReq{Username: "demo"}
	if err := validateUserIdentityListReq(req); err != nil {
		t.Fatalf("validateUserIdentityListReq() error = %v", err)
	}
	req.Email = "demo@example.com"
	if err := validateUserIdentityListReq(req); err != nil {
		t.Fatalf("validateUserIdentityListReq(email) error = %v", err)
	}
	req.Phone = "13800000000"
	if err := validateUserIdentityListReq(req); err == nil {
		t.Fatal("expected email and phone combined filter to be rejected")
	}
	req.Email = ""
	req.Phone = ""
	req.Status = new(int)
	if err := validateUserIdentityListReq(req); err == nil {
		t.Fatal("expected status filter to be rejected")
	}
	req.Status = nil
	req.OrderBy = "lastLoginAt"
	if err := validateUserIdentityListReq(req); err == nil {
		t.Fatal("expected unsupported order field to be rejected")
	}
	req.OrderBy = "id"
	req.Page = 2
	if err := validateUserIdentityListReq(req); err == nil {
		t.Fatal("expected page after first to require cursor")
	}
	req.CursorID = 100
	if err := validateUserIdentityListReq(req); err != nil {
		t.Fatalf("cursor page should pass validation: %v", err)
	}
}

// TestListByUserIdentityUsesBoundedIdentityQuery 验证二百条列表只多读一行判断后续页，且身份目录排序不会污染用户表查询。
func TestListByUserIdentityUsesBoundedIdentityQuery(t *testing.T) {
	buffer := &bytes.Buffer{}
	db := newUserLogicDryRunDB(t).Session(&gorm.Session{
		Logger: logger.New(log.New(buffer, "", 0), logger.Config{LogLevel: logger.Info}),
	})
	userID := int64(1)
	shardNo := idgen.ShardNo(userID)
	if err := db.Callback().Query().Before("gorm:query").Register("test:inject_user_list_rows", func(tx *gorm.DB) {
		switch rows := tx.Statement.Dest.(type) {
		case *[]model.UserIdentity:
			*rows = []model.UserIdentity{{
				IdentityValue: "demo",
				UserID:        userID,
				UserShardNo:   shardNo,
			}}
		case *[]model.User:
			*rows = []model.User{{ID: userID, ShardNo: shardNo, Username: "demo"}}
		}
	}); err != nil {
		t.Fatalf("注册用户列表测试查询回调失败: %v", err)
	}
	logicObj := NewLogicWithContext(context.Background(), svc.NewServiceContext(config.Config{}, svc.Dependencies{
		SiteDBs: svc.SiteDatabases{MainDB: db},
	}))
	resp := logicObj.listByUserIdentity(db.Clauses(dbresolver.Read), &types.UserListReq{
		Username: "demo",
		GetOrderReq: types.GetOrderReq{
			Order: "desc",
		},
		GetPageReq: types.GetPageReq{
			Page:     1,
			PageSize: 200,
		},
	})
	if resp.IsFailure() {
		t.Fatalf("用户列表查询失败: %+v", resp)
	}
	identityBounded := false
	identityOrdered := false
	userQueried := false
	for _, line := range strings.Split(buffer.String(), "\n") {
		if strings.Contains(line, "FROM `user_identity_username`") && strings.Contains(line, "ORDER BY user_id desc") {
			identityOrdered = true
			identityBounded = strings.Contains(line, "LIMIT 201")
		}
		if strings.Contains(line, "FROM `user` ") {
			userQueried = true
			if strings.Contains(line, "ORDER BY user_id") {
				t.Fatalf("用户主表查询继承了身份目录排序: %s", line)
			}
		}
	}
	if !identityBounded || !identityOrdered || !userQueried {
		t.Fatalf("用户列表 SQL 未完整覆盖身份目录上限、排序和用户表读取: %s", buffer.String())
	}
}

// TestListByUserIdentityKeepsTwoHundredRowPagination 验证一百零一条不会被旧上限截断，超过二百条时用占位总数开放下一页。
func TestListByUserIdentityKeepsTwoHundredRowPagination(t *testing.T) {
	tests := []struct {
		name           string
		rowCount       int
		wantItems      int
		wantTotal      int64
		wantHasMore    bool
		wantNextCursor bool
	}{
		{name: "一百零一条完整返回", rowCount: 101, wantItems: 101, wantTotal: 101},
		{name: "二百零一条开放下一页", rowCount: 201, wantItems: 200, wantTotal: 201, wantHasMore: true, wantNextCursor: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newUserLogicDryRunDB(t)
			identities := make([]model.UserIdentity, 0, tt.rowCount)
			users := make([]model.User, 0, tt.wantItems)
			for index := 1; index <= tt.rowCount; index++ {
				userID := int64(index)
				identities = append(identities, model.UserIdentity{IdentityValue: "demo", UserID: userID, UserShardNo: idgen.ShardNo(userID)})
				if index <= tt.wantItems {
					users = append(users, model.User{ID: userID, ShardNo: idgen.ShardNo(userID), Username: "demo"})
				}
			}
			if err := db.Callback().Query().Before("gorm:query").Register("test:inject_user_pagination_rows", func(tx *gorm.DB) {
				switch rows := tx.Statement.Dest.(type) {
				case *[]model.UserIdentity:
					*rows = identities
				case *[]model.User:
					*rows = users
				}
			}); err != nil {
				t.Fatalf("注册用户分页测试查询回调失败: %v", err)
			}
			logicObj := NewLogicWithContext(context.Background(), svc.NewServiceContext(config.Config{}, svc.Dependencies{
				SiteDBs: svc.SiteDatabases{MainDB: db},
			}))
			resp := logicObj.listByUserIdentity(db.Clauses(dbresolver.Read), &types.UserListReq{
				GetOrderReq: types.GetOrderReq{Order: "asc"},
				GetPageReq:  types.GetPageReq{Page: 1, PageSize: 200},
			})
			if resp.IsFailure() {
				t.Fatalf("用户列表查询失败: %+v", resp)
			}
			data, ok := resp.Data.(types.ListResp[types.UserItem])
			if !ok {
				t.Fatalf("用户列表响应类型 = %T", resp.Data)
			}
			meta, ok := data.Meta.(types.UserListMeta)
			if !ok {
				t.Fatalf("用户列表分页元数据类型 = %T", data.Meta)
			}
			if len(data.List) != tt.wantItems || data.Total != tt.wantTotal || meta.HasMore != tt.wantHasMore {
				t.Fatalf("用户列表分页 list=%d total=%d hasMore=%v", len(data.List), data.Total, meta.HasMore)
			}
			if (meta.NextCursorID > 0) != tt.wantNextCursor {
				t.Fatalf("用户列表下一页游标 = %d, wantPresent=%v", meta.NextCursorID, tt.wantNextCursor)
			}
		})
	}
}

// TestBuildUserExportPartFileNameIncludesPartNo 验证导出文件名带固定宽度编号，便于按文件名确认顺序。
func TestBuildUserExportPartFileNameIncludesPartNo(t *testing.T) {
	now := time.Date(2026, 7, 9, 12, 34, 56, 0, time.UTC)
	jobID := "11111111-2222-3333-4444-555555555555"
	got := buildUserExportPartFileName(jobID, now, 12)
	wantSuffix := "_part_0012.xlsx"
	if !strings.HasSuffix(got, wantSuffix) {
		t.Fatalf("file name = %q, want suffix %q", got, wantSuffix)
	}
	if strings.Contains(got, "-") {
		t.Fatalf("file name = %q, job id hyphen should be removed", got)
	}
}

// TestUserExportSplitHelpers 验证用户导出拆分阈值不会超过配置单文件行数。
func TestUserExportSplitHelpers(t *testing.T) {
	if got := userExportPartQueryLimit(200, 50); got != 50 {
		t.Fatalf("query limit = %d, want remaining rows", got)
	}
	if got := userExportPartQueryLimit(200, 0); got != 1 {
		t.Fatalf("query limit = %d, want lookahead minimum", got)
	}
	if got := userExportSheetRowLimit(500000); got != 500001 {
		t.Fatalf("sheet row limit = %d, want split rows plus header", got)
	}
	if got := userExportSheetRowLimit(userExportMaxSplitRows + 1); got != excel.MaxExcelSheetRows {
		t.Fatalf("sheet row limit = %d, want excel max rows", got)
	}
}

// TestApplyUserExportProgress 验证多文件导出按全局处理量更新百分比、速度和预计剩余时间。
func TestApplyUserExportProgress(t *testing.T) {
	startedAt := time.Date(2026, 7, 16, 12, 0, 0, 0, time.Local)
	status := &types.UserExportStatusResp{Total: 1000000}
	applyUserExportProgress(status, 500000, startedAt, excel.ExportProgress{
		Processed:       100000,
		LastProcessedAt: startedAt.Add(10 * time.Second),
	})
	if status.Processed != 600000 || status.Progress != 60 {
		t.Fatalf("progress = %d processed = %d, want 60/600000", status.Progress, status.Processed)
	}
	if status.AverageRowsPerSec != 60000 || status.EstimatedSeconds != 7 {
		t.Fatalf("speed = %d eta = %d, want 60000/7", status.AverageRowsPerSec, status.EstimatedSeconds)
	}

	applyUserExportProgress(status, 500000, startedAt, excel.ExportProgress{
		Processed:       500000,
		LastProcessedAt: startedAt.Add(20 * time.Second),
	})
	if status.Progress != userExportRunningMaxProgress || status.EstimatedSeconds != 0 {
		t.Fatalf("finished running progress = %d eta = %d, want 99/0", status.Progress, status.EstimatedSeconds)
	}

	unknown := &types.UserExportStatusResp{}
	applyUserExportProgress(unknown, 0, startedAt, excel.ExportProgress{
		Processed:       200,
		LastProcessedAt: startedAt.Add(2 * time.Second),
	})
	if unknown.Progress != 0 || unknown.AverageRowsPerSec != 100 || unknown.EstimatedSeconds != 0 {
		t.Fatalf("unknown total progress = %+v, want progress 0 speed 100 eta 0", unknown)
	}
}

// TestResetUserExportFilesClearsRetryProgress 验证首个文件生成前失败也会清空旧进度和预计时间。
func TestResetUserExportFilesClearsRetryProgress(t *testing.T) {
	status := &types.UserExportStatusResp{
		Processed:         100,
		Total:             1000,
		Progress:          10,
		EstimatedSeconds:  9,
		AverageRowsPerSec: 100,
		StartedAt:         "2026-07-16 12:00:00",
		LastProcessedAt:   "2026-07-16 12:00:01",
	}
	resetUserExportFiles(status)
	if status.Processed != 0 || status.Total != 0 || status.Progress != 0 || status.EstimatedSeconds != 0 || status.AverageRowsPerSec != 0 {
		t.Fatalf("重试进度未清空: %+v", status)
	}
	if status.StartedAt != "" || status.LastProcessedAt != "" {
		t.Fatalf("重试时间未清空: %+v", status)
	}
}

// TestCountUserExportRowsUsesIdentityDirectory 验证总量统计固定使用有界身份目录查询。
func TestCountUserExportRowsUsesIdentityDirectory(t *testing.T) {
	buffer := &bytes.Buffer{}
	db := newUserLogicDryRunDB(t).Session(&gorm.Session{Logger: logger.New(log.New(buffer, "", 0), logger.Config{LogLevel: logger.Info})})
	logicObj := NewLogicWithContext(context.Background(), svc.NewServiceContext(config.Config{}, svc.Dependencies{
		SiteDBs: svc.SiteDatabases{MainDB: db},
	}))
	if _, err := logicObj.countUserExportRows(context.Background(), &types.UserExportReq{Username: "demo"}); err != nil {
		t.Fatalf("count identity rows: %v", err)
	}
	if sqlText := buffer.String(); !strings.Contains(sqlText, "FROM `user_identity_username` WHERE identity_value LIKE 'demo%'") || strings.Contains(sqlText, "FORCE INDEX") {
		t.Fatalf("identity count sql = %q, want optimizer-selected username identity query", sqlText)
	}
}

// TestNextUserExportCursorAfterEmptyPage 验证身份索引空页只能在游标推进时跳过。
func TestNextUserExportCursorAfterEmptyPage(t *testing.T) {
	nextCursor, skipped, err := nextUserExportCursorAfterEmptyPage(&excel.CursorPage[userExportRow, int64]{
		HasMore:    true,
		NextCursor: 20,
	}, 10)
	if err != nil {
		t.Fatalf("nextUserExportCursorAfterEmptyPage() error = %v", err)
	}
	if !skipped || nextCursor != 20 {
		t.Fatalf("next cursor = %d skipped=%v, want 20/true", nextCursor, skipped)
	}

	_, skipped, err = nextUserExportCursorAfterEmptyPage(&excel.CursorPage[userExportRow, int64]{
		Items:   []userExportRow{{CursorID: 11}},
		HasMore: true,
	}, 10)
	if err != nil || skipped {
		t.Fatalf("non-empty page skipped=%v err=%v, want no skip", skipped, err)
	}

	_, skipped, err = nextUserExportCursorAfterEmptyPage(&excel.CursorPage[userExportRow, int64]{
		HasMore:    true,
		NextCursor: 10,
	}, 10)
	if err == nil || skipped {
		t.Fatalf("stalled empty page skipped=%v err=%v, want error", skipped, err)
	}
}

// TestBuildUserExportRequestFingerprintIncludesSplitRows 验证拆分配置变化不会复用旧导出文件。
func TestBuildUserExportRequestFingerprintIncludesSplitRows(t *testing.T) {
	req := &types.UserExportReq{Username: "demo"}
	left := buildUserExportRequestFingerprint(req, 500000)
	right := buildUserExportRequestFingerprint(req, 250000)
	if left == right {
		t.Fatal("export fingerprint should include split rows")
	}
}

// TestUserExportStatusSnapshotKeepsFileObjects 验证 Redis 快照保留内部对象信息，对外响应隐藏内部路径。
func TestUserExportStatusSnapshotKeepsFileObjects(t *testing.T) {
	status := &types.UserExportStatusResp{
		JobID: "job-1",
		Files: []types.UserExportFileItem{{
			PartNo:        2,
			FileName:      "user_export_part_0002.xlsx",
			RowCount:      100,
			ProcessedFrom: 101,
			ProcessedTo:   200,
			DownloadReady: true,
			DownloadURL:   "internal",
			FilePath:      "/tmp/user_export_part_0002.xlsx",
			ObjectKey:     "exports/user/part-2.xlsx",
			StorageType:   "local",
			ContentType:   userExportContentType,
		}},
		AuthorizedAdminIDs: []int{1, 2},
	}
	snapshot := newUserExportStatusSnapshot(status)
	if len(snapshot.UserExportStatusResp.Files) != 0 {
		t.Fatal("embedded response files should be cleared to avoid duplicate stale snapshots")
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	decoded := &userExportStatusSnapshot{}
	if err := json.Unmarshal(payload, decoded); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	restored := decoded.toStatus()
	if len(restored.Files) != 1 || restored.Files[0].ObjectKey != "exports/user/part-2.xlsx" {
		t.Fatalf("restored files = %+v, want object key preserved", restored.Files)
	}
	selected, err := selectUserExportDownloadFile(restored, 2)
	if err != nil {
		t.Fatalf("select export file: %v", err)
	}
	if selected.FileName != "user_export_part_0002.xlsx" {
		t.Fatalf("selected file = %q, want part 2", selected.FileName)
	}
	publicFiles := publicUserExportFiles(restored.JobID, restored.Files)
	if publicFiles[0].ObjectKey != "" || publicFiles[0].FilePath != "" {
		t.Fatalf("public files leaked internal storage fields: %+v", publicFiles[0])
	}
	if !strings.HasSuffix(publicFiles[0].DownloadURL, "?partNo=2") {
		t.Fatalf("download url = %q, want partNo query", publicFiles[0].DownloadURL)
	}
}

// TestUserExportReusableObjectHonorsFilesList 验证多分片任务只按分片列表判断复用可用性。
func TestUserExportReusableObjectHonorsFilesList(t *testing.T) {
	filePath := t.TempDir() + "/user_export_part_0001.xlsx"
	if err := os.WriteFile(filePath, []byte("xlsx"), 0o644); err != nil {
		t.Fatalf("write temp export file: %v", err)
	}
	logicObj := NewLogicWithContext(context.Background(), svc.NewServiceContext(config.Config{}, svc.Dependencies{}))
	status := &types.UserExportStatusResp{
		FilePath:      filePath,
		DownloadReady: true,
		Files: []types.UserExportFileItem{{
			PartNo:        1,
			DownloadReady: false,
		}},
	}
	if logicObj.hasReusableUserExportDownloadObject(status) {
		t.Fatal("multi-part status should not fallback to top-level file when no part is ready")
	}
	status.Files = nil
	if !logicObj.hasReusableUserExportDownloadObject(status) {
		t.Fatal("legacy single-file status should fallback to top-level file")
	}
}

// TestAPIRuntimeSyncWarningPreservesDBSuccessSemantics 验证写库后的同步失败只作为可重试告警返回。
func TestAPIRuntimeSyncWarningPreservesDBSuccessSemantics(t *testing.T) {
	logicObj := NewLogic(nil, svc.NewServiceContext(config.Config{}, svc.Dependencies{}))
	resp := logicObj.apiRuntimeSyncWarning(7, types.UserRuntimeSyncResp{Enabled: true}, i18n.MsgKeyAPIRuntimeProfileSyncWarning, assertError("timeout"))
	if resp.Success {
		t.Fatal("sync warning should mark success false")
	}
	if !resp.Enabled || resp.UserID != 7 {
		t.Fatalf("sync warning response = %+v, want enabled user 7", resp)
	}
	if !strings.Contains(resp.Message, "资料已更新") || strings.Contains(resp.Message, "timeout") {
		t.Fatalf("sync warning message = %q, want safe fallback without internal cause", resp.Message)
	}
}

// assertError 是测试用固定错误文本。
type assertError string

// Error 返回固定错误文本。
func (e assertError) Error() string {
	return string(e)
}

// newUserLogicDryRunDB 创建用户管理逻辑测试使用的 MySQL DryRun 连接。
func newUserLogicDryRunDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN:                       "gorm:gorm@tcp(localhost:9910)/gorm?charset=utf8mb4&parseTime=True&loc=Local",
		SkipInitializeWithVersion: true,
	}), &gorm.Config{DryRun: true, DisableAutomaticPing: true})
	if err != nil {
		t.Fatalf("open dry run db: %v", err)
	}
	return db
}
