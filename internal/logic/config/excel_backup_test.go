package config

import (
	codes "admin/common/codes"
	cachelogic "admin/internal/logic/cache"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	redislock "admin/internal/infra/redsync"
	"admin/internal/model"

	"github.com/Is999/go-utils/errors"
	"github.com/xuri/excelize/v2"
)

// TestWriteSysConfigSnapshotStableAndComplete 验证字典快照摘要稳定且会覆盖非导入列。
func TestWriteSysConfigSnapshotStableAndComplete(t *testing.T) {
	base := []model.SysConfig{{
		ID:        1,
		UUID:      "demoConfig",
		Title:     "测试配置",
		Type:      3,
		Value:     `"value"`,
		Example:   `"example"`,
		Remark:    "备注",
		Page:      "/system/config",
		Pid:       2,
		Pids:      "0,2",
		Version:   3,
		CreatedAt: time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 7, 26, 11, 0, 0, 0, time.UTC),
	}}
	hashRows := func(rows []model.SysConfig) string {
		t.Helper()
		digest := sha256.New()
		if err := writeSysConfigSnapshot(digest, rows); err != nil {
			t.Fatalf("计算字典快照摘要失败: %v", err)
		}
		return hex.EncodeToString(digest.Sum(nil))
	}

	first := hashRows(base)
	if second := hashRows(base); second != first {
		t.Fatalf("相同字典数据摘要不稳定: first=%s second=%s", first, second)
	}
	changed := append([]model.SysConfig(nil), base...)
	changed[0].Pids = "0,9"
	if got := hashRows(changed); got == first {
		t.Fatal("配置族谱变化后摘要未变化")
	}
}

// TestFileSHA256DetectsContentChange 验证待导入文件内容变化会产生不同摘要。
func TestFileSHA256DetectsContentChange(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "import.xlsx")
	if err := os.WriteFile(filePath, []byte("first"), 0o600); err != nil {
		t.Fatalf("写入测试文件失败: %v", err)
	}
	first, err := fileSHA256(filePath)
	if err != nil {
		t.Fatalf("计算首个文件摘要失败: %v", err)
	}
	if err := os.WriteFile(filePath, []byte("second"), 0o600); err != nil {
		t.Fatalf("覆盖测试文件失败: %v", err)
	}
	second, err := fileSHA256(filePath)
	if err != nil {
		t.Fatalf("计算第二个文件摘要失败: %v", err)
	}
	if first == second {
		t.Fatal("文件内容变化后摘要未变化")
	}
}

// TestFinishSysConfigTransaction 验证只有明确回滚才允许重用备份消费标记。
func TestFinishSysConfigTransaction(t *testing.T) {
	workErr := errors.New("导入失败")
	commitErr := errors.New("提交失败")
	rollbackErr := errors.New("回滚失败")
	tests := []struct {
		name        string             // name 标识当前事务收尾场景。
		workErr     error              // workErr 模拟导入主体返回的错误。
		rollbackErr error              // rollbackErr 模拟事务回滚结果不确定。
		commitErr   error              // commitErr 模拟事务提交结果不确定。
		wantOutcome sysConfigTxOutcome // wantOutcome 是消费标记允许采用的最终状态。
		wantErr     error              // wantErr 是调用方必须收到的首要事务错误。
	}{
		{name: "提交成功", wantOutcome: sysConfigTxCommitted},
		{name: "提交结果不确定", commitErr: commitErr, wantOutcome: sysConfigTxUncertain, wantErr: commitErr},
		{name: "明确回滚", workErr: workErr, wantOutcome: sysConfigTxRolledBack, wantErr: workErr},
		{name: "回滚结果不确定", workErr: workErr, rollbackErr: rollbackErr, wantOutcome: sysConfigTxUncertain, wantErr: rollbackErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rollbackCalls := 0
			commitCalls := 0
			outcome, err := finishSysConfigTransaction(
				test.workErr,
				func() error {
					rollbackCalls++
					return test.rollbackErr
				},
				func() error {
					commitCalls++
					return test.commitErr
				},
			)
			if outcome != test.wantOutcome {
				t.Fatalf("事务结果错误: got=%d want=%d", outcome, test.wantOutcome)
			}
			if test.wantErr == nil {
				if err != nil {
					t.Fatalf("事务结束返回意外错误: %v", err)
				}
			} else if !errors.Is(err, test.wantErr) {
				t.Fatalf("事务错误链错误: got=%v want=%v", err, test.wantErr)
			}
			if test.workErr == nil && (rollbackCalls != 0 || commitCalls != 1) {
				t.Fatalf("提交分支调用次数错误: rollback=%d commit=%d", rollbackCalls, commitCalls)
			}
			if test.workErr != nil && (rollbackCalls != 1 || commitCalls != 0) {
				t.Fatalf("回滚分支调用次数错误: rollback=%d commit=%d", rollbackCalls, commitCalls)
			}
		})
	}
}

// TestSysConfigInfrastructureResult 验证 Redis 依赖错误返回 503 业务码。
func TestSysConfigInfrastructureResult(t *testing.T) {
	result := sysConfigInfrastructureResult(
		cachelogic.WrapRedisUnavailable(nil, "测试 Redis 故障"),
		"读取字典备份失败",
	)
	if result == nil || result.Code != codes.RedisUnavailable {
		t.Fatalf("Redis 故障业务码错误: %+v", result)
	}
	result = sysConfigInfrastructureResult(errSysConfigBackupTaskUnavailable, "投递清理任务失败")
	if result == nil || result.Code != codes.TaskQueueUnavailable {
		t.Fatalf("任务队列故障业务码错误: %+v", result)
	}
	result = sysConfigInfrastructureResult(redislock.ErrLockTaken, "获取字典写入锁失败")
	if result == nil || result.Code != codes.ServiceBusy {
		t.Fatalf("锁竞争业务码错误: %+v", result)
	}
}

// TestValidateSysConfigImportFileRejectsEquivalentUUID 验证预检按数据库大小写不敏感语义拒绝重复 UUID。
func TestValidateSysConfigImportFileRejectsEquivalentUUID(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "duplicate.xlsx")
	workbook := excelize.NewFile()
	defer workbook.Close()
	defaultSheet := workbook.GetSheetName(0)
	if err := workbook.SetSheetName(defaultSheet, sysConfigExcelSheetName); err != nil {
		t.Fatalf("重命名测试工作表失败: %v", err)
	}
	if err := workbook.SetSheetRow(sysConfigExcelSheetName, "A1", &sysConfigExcelHeaders); err != nil {
		t.Fatalf("写入测试表头失败: %v", err)
	}
	first := []any{1, "duplicateUUID", "配置一", 3, "value-1", "", "", 0, ""}
	second := []any{2, "DuplicateUUID", "配置二", 3, "value-2", "", "", 0, ""}
	if err := workbook.SetSheetRow(sysConfigExcelSheetName, "A2", &first); err != nil {
		t.Fatalf("写入首行测试数据失败: %v", err)
	}
	if err := workbook.SetSheetRow(sysConfigExcelSheetName, "A3", &second); err != nil {
		t.Fatalf("写入重复测试数据失败: %v", err)
	}
	if err := workbook.SaveAs(filePath); err != nil {
		t.Fatalf("保存测试 Excel 失败: %v", err)
	}
	if err := validateSysConfigImportFile(context.Background(), filePath); err == nil {
		t.Fatal("重复 UUID 未被导入预检拒绝")
	}
}

// TestSysConfigInputResult 验证字典业务数据错误不会被误报为数据库或服务端故障。
func TestSysConfigInputResult(t *testing.T) {
	logicObj := &SysConfigLogic{}
	_, err := logicObj.sysConfigPidsTx(nil, 7, 7)
	result := sysConfigInputResult(err, "测试字典配置数据校验")
	if result == nil || result.Code != codes.ParamError {
		t.Fatalf("字典业务数据错误业务码错误: %+v", result)
	}
}

// TestIsSysConfigBackupObjectKey 校验清理任务只能删除 backupId 对应的备份目录对象。
func TestIsSysConfigBackupObjectKey(t *testing.T) {
	const (
		backupID = "c1bc972b-8a57-4df3-bab9-6716b63763a1"
		prefix   = "tenant/sys-config-excel-backup"
	)
	validKey := prefix + "/202607/26/c1bc972b8a574df3bab96716b63763a1.xlsx"
	if !isSysConfigBackupObjectKey(validKey, prefix, backupID) {
		t.Fatal("合法字典备份对象被拒绝")
	}
	invalidKeys := []string{
		"tenant/other/202607/26/c1bc972b8a574df3bab96716b63763a1.xlsx",
		prefix + "/202607/26/11111111111111111111111111111111.xlsx",
	}
	for _, objectKey := range invalidKeys {
		if isSysConfigBackupObjectKey(objectKey, prefix, backupID) {
			t.Fatalf("非法字典备份对象被放行: %s", objectKey)
		}
	}
}
