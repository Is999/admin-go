package secretkey

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"admin/internal/config"
	"admin/internal/model"
	"admin/internal/svc"
	"admin/internal/types"
)

// TestValidateSecretKeyVersionCapacity 验证每个 AppID 的第一百个版本可存在，但不能再创建第一百零一个版本。
func TestValidateSecretKeyVersionCapacity(t *testing.T) {
	if err := validateSecretKeyVersionCapacity(maxSecretKeyVersionCount - 1); err != nil {
		t.Fatalf("第 %d 个现有版本后仍应允许创建新版本: %v", maxSecretKeyVersionCount-1, err)
	}
	err := validateSecretKeyVersionCapacity(maxSecretKeyVersionCount)
	if err == nil || !errors.Is(err, errSecretKeyVersionCountLimit) {
		t.Fatalf("现有版本达到 %d 时应返回数量上限错误: %v", maxSecretKeyVersionCount, err)
	}
}

// TestCheckSecretKeyPayloadKeepsValidationSemantics 验证拆分后的校验流程保持字段清洗、分项顺序和启用判断。
func TestCheckSecretKeyPayloadKeepsValidationSemantics(t *testing.T) {
	logicObj := NewSecretKeyLogic(context.Background(), svc.NewServiceContext(config.Config{}, svc.Dependencies{}))
	result := logicObj.checkSecretKeyPayload(&types.SaveSecretKeyReq{
		UUID:          " app.demo ",
		Title:         " 演示秘钥 ",
		KeyVersion:    " v1 ",
		StableVersion: "v1",
	}, nil, false, false)
	if result.UUID != "app.demo" || result.Title != "演示秘钥" || result.KeyVersion != "v1" {
		t.Fatalf("sanitized result = %+v", result)
	}
	if !result.AllPassed || !result.CanSave || result.CanEnable {
		t.Fatalf("disabled validation result = %+v", result)
	}
	wantKeys := []string{"route.version", "crypto_status", "sign_status"}
	if len(result.Items) != len(wantKeys) {
		t.Fatalf("validation items = %+v", result.Items)
	}
	for index, wantKey := range wantKeys {
		if result.Items[index].Key != wantKey {
			t.Fatalf("validation item[%d] = %q, want %q", index, result.Items[index].Key, wantKey)
		}
	}
}

// TestCheckSecretKeyPayloadRejectsMissingFields 验证空请求仍返回可展示的完整失败结果而不会继续启用。
func TestCheckSecretKeyPayloadRejectsMissingFields(t *testing.T) {
	logicObj := NewSecretKeyLogic(context.Background(), svc.NewServiceContext(config.Config{}, svc.Dependencies{}))
	result := logicObj.checkSecretKeyPayload(nil, nil, false, true)
	if result.Mode != "self_check" || !result.RuntimeChecked {
		t.Fatalf("runtime flags = %+v", result)
	}
	if result.AllPassed || result.CanSave || result.CanEnable || len(result.Items) == 0 {
		t.Fatalf("missing field result = %+v", result)
	}
}

// TestSecretKeySignFailureFieldsExcludeSensitiveValues 验证签名失败日志只保留非敏感定位字段。
func TestSecretKeySignFailureFieldsExcludeSensitiveValues(t *testing.T) {
	fields := secretKeySignFailureFields(" app.demo ", " v1 ", " runtime.rsa.verify ", " server_public_key ")
	want := map[string]string{
		"uuid":        "app.demo",
		"key_version": "v1",
		"stage":       "runtime.rsa.verify",
		"secret_type": "server_public_key",
	}
	if len(fields) != len(want) {
		t.Fatalf("log fields = %+v", fields)
	}
	for _, field := range fields {
		value, ok := field.Value.(string)
		if !ok || want[field.Key] != value {
			t.Fatalf("log field %q = %#v, want %q", field.Key, field.Value, want[field.Key])
		}
		delete(want, field.Key)
	}
	if len(want) != 0 {
		t.Fatalf("missing log fields = %+v", want)
	}
}

// TestCheckSecretKeyPayloadRuntimeModes 验证签名、加解密及组合模式的真实 AES/RSA 自检链路。
func TestCheckSecretKeyPayloadRuntimeModes(t *testing.T) {
	tempDir := t.TempDir()
	serverPrivatePEM, serverPublicPEM := generateTestRSAPEMPair(t)
	_, userPublicPEM := generateTestRSAPEMPair(t)
	paths := map[string]string{
		"aes_key":            "12345678901234567890123456789012",
		"aes_iv":             "1234567890123456",
		"server_private.pem": serverPrivatePEM,
		"server_public.pem":  serverPublicPEM,
		"user_public.pem":    userPublicPEM,
	}
	for name, value := range paths {
		filePath := filepath.Join(tempDir, name)
		if err := os.WriteFile(filePath, []byte(value), 0o600); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", filePath, err)
		}
		paths[name] = filePath
	}

	tests := []struct {
		name         string   // name 表示运行态校验场景。
		signStatus   int      // signStatus 表示是否启用签名验签。
		cryptoStatus int      // cryptoStatus 表示是否启用加密解密。
		wantItems    []string // wantItems 表示必须通过的关键运行态校验项。
	}{
		{
			name:         "签名与加解密同时启用",
			signStatus:   1,
			cryptoStatus: 1,
			wantItems:    []string{"rsa_server_pair.match", "runtime.aes.decrypt", "runtime.rsa.verify", "runtime.rsa.decrypt"},
		},
		{
			name:       "仅启用签名验签",
			signStatus: 1,
			wantItems:  []string{"rsa_server_pair.match", "runtime.rsa.verify"},
		},
		{
			name:         "仅启用加密解密",
			cryptoStatus: 1,
			wantItems:    []string{"runtime.aes.decrypt", "runtime.rsa.decrypt"},
		},
	}

	logicObj := NewSecretKeyLogic(context.Background(), svc.NewServiceContext(config.Config{}, svc.Dependencies{}))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := logicObj.checkSecretKeyPayload(&types.SaveSecretKeyReq{
				UUID:                   "app.demo",
				Title:                  "演示秘钥",
				KeyVersion:             "v1",
				AESKeyRef:              paths["aes_key"],
				AESIVRef:               paths["aes_iv"],
				RSAPublicKeyUserRef:    paths["user_public.pem"],
				RSAPublicKeyServerRef:  paths["server_public.pem"],
				RSAPrivateKeyServerRef: paths["server_private.pem"],
				Status:                 1,
				SignStatus:             tt.signStatus,
				CryptoStatus:           tt.cryptoStatus,
				VersionStatus:          1,
				StableVersion:          "v1",
			}, nil, false, true)
			if !result.AllPassed || !result.CanSave || !result.CanEnable || !result.RuntimeChecked {
				t.Fatalf("runtime result = %+v", result)
			}
			for _, wantKey := range tt.wantItems {
				found := false
				for _, item := range result.Items {
					if item.Key == wantKey {
						found = item.Passed
						break
					}
				}
				if !found {
					t.Fatalf("runtime item %q did not pass: %+v", wantKey, result.Items)
				}
			}
		})
	}
}

// TestCheckSecretKeyPayloadRejectsMismatchedServerPair 验证不配对的服务端公私钥无法启用。
func TestCheckSecretKeyPayloadRejectsMismatchedServerPair(t *testing.T) {
	tempDir := t.TempDir()
	serverPrivatePEM, _ := generateTestRSAPEMPair(t)
	_, mismatchedServerPublicPEM := generateTestRSAPEMPair(t)
	_, userPublicPEM := generateTestRSAPEMPair(t)
	paths := map[string]string{
		"server_private.pem": serverPrivatePEM,
		"server_public.pem":  mismatchedServerPublicPEM,
		"user_public.pem":    userPublicPEM,
	}
	for name, value := range paths {
		filePath := filepath.Join(tempDir, name)
		if err := os.WriteFile(filePath, []byte(value), 0o600); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", filePath, err)
		}
		paths[name] = filePath
	}

	logicObj := NewSecretKeyLogic(context.Background(), svc.NewServiceContext(config.Config{}, svc.Dependencies{}))
	result := logicObj.checkSecretKeyPayload(&types.SaveSecretKeyReq{
		UUID:                   "app.demo",
		Title:                  "演示秘钥",
		KeyVersion:             "v1",
		RSAPublicKeyUserRef:    paths["user_public.pem"],
		RSAPublicKeyServerRef:  paths["server_public.pem"],
		RSAPrivateKeyServerRef: paths["server_private.pem"],
		Status:                 1,
		SignStatus:             1,
		VersionStatus:          1,
		StableVersion:          "v1",
	}, nil, false, true)
	if result.AllPassed || result.CanEnable {
		t.Fatalf("mismatched RSA pair should not be enabled: %+v", result)
	}
	if !result.CanSave {
		t.Fatalf("structurally valid draft should remain savable: %+v", result)
	}
	for _, item := range result.Items {
		if item.Key == "rsa_server_pair.match" && !item.Passed {
			return
		}
	}
	t.Fatalf("missing failed rsa pair item: %+v", result.Items)
}

// TestMaskSecretKeyValue 验证秘钥列表脱敏规则，避免敏感字段明文暴露。
func TestMaskSecretKeyValue(t *testing.T) {
	tests := []struct {
		name  string // name 表示测试场景名称。
		input string // input 表示输入值。
		want  string // want 表示期望结果。
	}{
		{
			name:  "短文本保留前缀",
			input: "12345678",
			want:  "12****",
		},
		{
			name:  "文件路径仅展示文件名摘要",
			input: "/etc/admin/keys/app/server_private.pem",
			want:  "serv****.pem",
		},
		{
			name:  "短文件名保留前缀",
			input: "/tmp/aes_iv",
			want:  "ae****",
		},
		{
			name:  "空值保持为空",
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := maskSecretKeyValue(tt.input); got != tt.want {
				t.Fatalf("maskSecretKeyValue() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestSecretKeyModelToItem 验证列表与详情场景的秘钥返回语义不同。
func TestSecretKeyModelToItem(t *testing.T) {
	row := model.SecretKey{
		ID:            1,
		UUID:          "app.demo",
		Title:         "测试秘钥",
		StableVersion: "v1",
		GrayVersion:   "v2",
		GrayPercent:   30,
		Status:        1,
		Remark:        "remark",
		CreatedAt:     time.Date(2026, 1, 2, 3, 4, 5, 0, time.Local),
		UpdatedAt:     time.Date(2026, 1, 2, 3, 4, 6, 0, time.Local),
	}
	selected := model.SecretKeyVersion{
		ID:                     11,
		KeyVersion:             "v1",
		AESKeyRef:              "/etc/admin/keys/app.demo/aes_key",
		AESIVRef:               "/etc/admin/keys/app.demo/aes_iv",
		RSAPublicKeyUserRef:    "/etc/admin/keys/app.demo/user_public.pem",
		RSAPublicKeyServerRef:  "/etc/admin/keys/app.demo/server_public.pem",
		RSAPrivateKeyServerRef: "/etc/admin/keys/app.demo/server_private.pem",
		Status:                 1,
		Remark:                 "stable",
		CreatedAt:              time.Date(2026, 1, 2, 3, 4, 5, 0, time.Local),
		UpdatedAt:              time.Date(2026, 1, 2, 3, 4, 6, 0, time.Local),
	}
	grayVersion := model.SecretKeyVersion{
		ID:                     12,
		KeyVersion:             "v2",
		AESKeyRef:              "/etc/admin/keys/app.demo/aes_key_v2",
		AESIVRef:               "/etc/admin/keys/app.demo/aes_iv_v2",
		RSAPublicKeyUserRef:    "/etc/admin/keys/app.demo/user_public_v2.pem",
		RSAPublicKeyServerRef:  "/etc/admin/keys/app.demo/server_public_v2.pem",
		RSAPrivateKeyServerRef: "/etc/admin/keys/app.demo/server_private_v2.pem",
		Status:                 1,
		Remark:                 "gray",
		CreatedAt:              time.Date(2026, 1, 2, 3, 4, 7, 0, time.Local),
		UpdatedAt:              time.Date(2026, 1, 2, 3, 4, 8, 0, time.Local),
	}
	versions := []model.SecretKeyVersion{selected, grayVersion}

	listItem := secretKeyModelToItem(row, versions, &selected, true)
	if !listItem.SecretMasked {
		t.Fatalf("list item should be marked masked")
	}
	if listItem.AESKeyRef != "ae****" {
		t.Fatalf("masked AESKeyRef mismatch: %s", listItem.AESKeyRef)
	}
	if listItem.RSAPublicKeyUserRef != "user****.pem" {
		t.Fatalf("masked RSAPublicKeyUserRef mismatch: %s", listItem.RSAPublicKeyUserRef)
	}
	if listItem.RSAPrivateKeyServerRef != "serv****.pem" {
		t.Fatalf("masked private key ref mismatch: %q", listItem.RSAPrivateKeyServerRef)
	}
	if listItem.VersionCount != 2 {
		t.Fatalf("list version count mismatch: %d", listItem.VersionCount)
	}

	detailItem := secretKeyModelToItem(row, versions, &selected, false)
	if detailItem.SecretMasked {
		t.Fatalf("detail item should not be marked masked")
	}
	if detailItem.AESKeyRef != selected.AESKeyRef {
		t.Fatalf("detail AESKeyRef mismatch: %s", detailItem.AESKeyRef)
	}
	if detailItem.RSAPrivateKeyServerRef != selected.RSAPrivateKeyServerRef {
		t.Fatalf("detail private key ref mismatch: %s", detailItem.RSAPrivateKeyServerRef)
	}
	if len(detailItem.VersionList) != 2 {
		t.Fatalf("detail version list mismatch: %d", len(detailItem.VersionList))
	}
	if !detailItem.VersionList[0].IsStable {
		t.Fatalf("stable version flag mismatch")
	}
	if !detailItem.VersionList[1].IsGray {
		t.Fatalf("gray version flag mismatch")
	}
}

// TestResolvePEMTextFromFile 验证 RSA PEM 会从绝对路径文件中读取。
func TestResolvePEMTextFromFile(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "server_private.pem")
	want := "-----BEGIN RSA PRIVATE KEY-----\nline1\nline2\n-----END RSA PRIVATE KEY-----"
	if err := os.WriteFile(filePath, []byte(want), 0o600); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", filePath, err)
	}
	got, err := resolvePEMText(filePath)
	if err != nil {
		t.Fatalf("resolvePEMText(%s) failed: %v", filePath, err)
	}
	if got != want {
		t.Fatalf("resolvePEMText(%s) = %q, want %q", filePath, got, want)
	}
}

// TestResolvePEMTextRejectInlinePEM 验证当前项目不再允许直接录入 PEM 文本。
func TestResolvePEMTextRejectInlinePEM(t *testing.T) {
	if _, err := resolvePEMText("-----BEGIN RSA PRIVATE KEY-----\nabc"); err == nil {
		t.Fatal("resolvePEMText should reject inline pem text")
	}
}

// TestRunSecretKeyRSARequestDecryptSelfCheckUsesDerivedServerPublic 验证解密自检使用服务端密钥。
func TestRunSecretKeyRSARequestDecryptSelfCheckUsesDerivedServerPublic(t *testing.T) {
	serverPrivatePEM, _ := generateTestRSAPEMPair(t)
	passed, err := runSecretKeyRSARequestDecryptSelfCheck(serverPrivatePEM)
	if err != nil {
		t.Fatalf("runSecretKeyRSARequestDecryptSelfCheck() error = %v", err)
	}
	if !passed {
		t.Fatal("runSecretKeyRSARequestDecryptSelfCheck() should pass with derived server public key")
	}
}

// TestValidateSecretKeyEnabledValuesAllowsDerivedServerPublic 验证服务端公钥路径可为空并由私钥派生。
func TestValidateSecretKeyEnabledValuesAllowsDerivedServerPublic(t *testing.T) {
	tempDir := t.TempDir()
	serverPrivatePEM, _ := generateTestRSAPEMPair(t)
	_, userPublicPEM := generateTestRSAPEMPair(t)
	serverPrivatePath := filepath.Join(tempDir, "server_private.pem")
	userPublicPath := filepath.Join(tempDir, "user_public.pem")
	if err := os.WriteFile(serverPrivatePath, []byte(serverPrivatePEM), 0o600); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", serverPrivatePath, err)
	}
	if err := os.WriteFile(userPublicPath, []byte(userPublicPEM), 0o600); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", userPublicPath, err)
	}

	err := validateSecretKeyEnabledValues(&types.SaveSecretKeyReq{
		SignStatus:             1,
		VersionStatus:          1,
		RSAPublicKeyUserRef:    userPublicPath,
		RSAPrivateKeyServerRef: serverPrivatePath,
	})
	if err != nil {
		t.Fatalf("validateSecretKeyEnabledValues() error = %v", err)
	}
}

// generateTestRSAPEMPair 生成测试用 RSA 公私钥 PEM，避免重复散落构造逻辑。
func generateTestRSAPEMPair(t *testing.T) (string, string) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() error = %v", err)
	}
	privatePEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	publicASN1, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		t.Fatalf("x509.MarshalPKIXPublicKey() error = %v", err)
	}
	publicPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicASN1,
	})
	return string(privatePEM), string(publicPEM)
}
