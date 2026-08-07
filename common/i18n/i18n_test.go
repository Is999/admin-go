package i18n

import (
	"testing"

	"admin/common/codes"
)

// TestValidateCatalog 确保发布物内嵌的全部语言资产可在应用启动前完成解析和注册。
func TestValidateCatalog(t *testing.T) {
	if err := ValidateCatalog(); err != nil {
		t.Fatalf("ValidateCatalog() error = %v", err)
	}
}

// TestNormalizeLocale 验证对应场景符合预期。
func TestNormalizeLocale(t *testing.T) {
	cases := []struct {
		in   string // in 表示输入值。
		want string // want 表示期望结果。
	}{
		{"", LocaleZHCN},
		{"zh-CN,zh;q=0.9,en;q=0.8", LocaleZHCN},
		{"en-US,en;q=0.9", LocaleENUS},
		{"en", LocaleENUS},
		{"fr-FR", LocaleZHCN},
	}

	for _, c := range cases {
		if got := NormalizeLocale(c.in); got != c.want {
			t.Fatalf("NormalizeLocale(%q)=%q, want %q", c.in, got, c.want)
		}
	}
}

// TestMessageByCode 验证对应场景符合预期。
func TestMessageByCode(t *testing.T) {
	if got := MessageByCode(codes.Success, LocaleENUS); got != "Success" {
		t.Fatalf("MessageByCode(Success,en)=%q", got)
	}
	if got := MessageByCode(codes.DeleteSuccess, LocaleENUS); got != "Deleted successfully" {
		t.Fatalf("MessageByCode(DeleteSuccess,en)=%q", got)
	}
	if got := MessageByCode(codes.CheckMFABind, LocaleENUS); got != "MFA binding and enablement are required" {
		t.Fatalf("MessageByCode(CheckMFABind,en)=%q", got)
	}
	if got := MessageByCode(codes.CheckPasswordReset, LocaleENUS); got != "Password must be changed before continuing" {
		t.Fatalf("MessageByCode(CheckPasswordReset,en)=%q", got)
	}
	if got := MessageByCode(codes.UserTagWorkflowLeaseNotFound, LocaleZHCN); got != "用户标签工作流互斥锁不存在或已释放" {
		t.Fatalf("MessageByCode(UserTagWorkflowLeaseNotFound,zh-CN)=%q", got)
	}
	if got := MessageByCode(999999, LocaleENUS); got != "Failed" {
		t.Fatalf("MessageByCode(unknown,en)=%q", got)
	}
}

// TestMessageCatalogParity 校验中英文语言包 key 完整一致，避免新增文案只补一个语种。
func TestMessageCatalogParity(t *testing.T) {
	zhCNMessageCatalog := messageCatalog[LocaleZHCN]
	enUSMessageCatalog := messageCatalog[LocaleENUS]
	for key := range zhCNMessageCatalog {
		if enUSMessageCatalog[key] == "" {
			t.Fatalf("en-US catalog missing key %q", key)
		}
	}
	for key := range enUSMessageCatalog {
		if zhCNMessageCatalog[key] == "" {
			t.Fatalf("zh-CN catalog missing key %q", key)
		}
	}
}

// TestCodeContractsCatalogCoverage 校验业务码契约声明的 key 在所有语言包中都有文案。
func TestCodeContractsCatalogCoverage(t *testing.T) {
	for _, contract := range codes.DefaultCodeContracts() {
		key := contract.MessageKey
		for locale, catalog := range messageCatalog {
			if catalog[key] == "" {
				t.Fatalf("code %d key %q missing locale %s message", contract.Code, key, locale)
			}
		}
	}
}
