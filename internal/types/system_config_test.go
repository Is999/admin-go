package types

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zeromicro/go-zero/rest/httpx"
)

// TestSysConfigExcelImportJSONBinding 校验备份准备和正式导入请求可从 JSON body 绑定字段。
func TestSysConfigExcelImportJSONBinding(t *testing.T) {
	t.Run("prepare backup", func(t *testing.T) {
		httpReq := httptest.NewRequest(http.MethodPost, "/api/dicts/import/backup", strings.NewReader(`{"uploadId":"upload-1"}`))
		httpReq.Header.Set("Content-Type", "application/json;charset=UTF-8")
		var req SysConfigExcelBackupReq
		if err := httpx.Parse(httpReq, &req); err != nil {
			t.Fatalf("httpx.Parse() error = %v", err)
		}
		if req.UploadID != "upload-1" {
			t.Fatalf("UploadID = %q, want upload-1", req.UploadID)
		}
	})

	t.Run("import", func(t *testing.T) {
		httpReq := httptest.NewRequest(http.MethodPost, "/api/dicts/import", strings.NewReader(`{"uploadId":"upload-1","backupId":"backup-1"}`))
		httpReq.Header.Set("Content-Type", "application/json;charset=UTF-8")
		var req SysConfigExcelImportReq
		if err := httpx.Parse(httpReq, &req); err != nil {
			t.Fatalf("httpx.Parse() error = %v", err)
		}
		if req.UploadID != "upload-1" || req.BackupID != "backup-1" {
			t.Fatalf("request = %+v", req)
		}
	})
}

// TestSysConfigExcelBackupDownloadReqValidate 校验下载路径与签名 query 必须绑定同一备份。
func TestSysConfigExcelBackupDownloadReqValidate(t *testing.T) {
	tests := []struct {
		name    string                          // name 表示测试场景。
		req     SysConfigExcelBackupDownloadReq // req 同时携带路径参数和签名查询参数。
		wantErr bool                            // wantErr 表示两个备份 ID 不一致时必须拒绝下载。
	}{
		{
			name: "matched",
			req: SysConfigExcelBackupDownloadReq{
				BackupID:     "backup-1",
				PathBackupID: "backup-1",
			},
		},
		{
			name: "missing query",
			req: SysConfigExcelBackupDownloadReq{
				PathBackupID: "backup-1",
			},
			wantErr: true,
		},
		{
			name: "mismatched",
			req: SysConfigExcelBackupDownloadReq{
				BackupID:     "backup-2",
				PathBackupID: "backup-1",
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.req.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}
