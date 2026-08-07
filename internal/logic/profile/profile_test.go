package profile

import (
	cachelogic "admin/internal/logic/cache"
	securitylogic "admin/internal/logic/security"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	keys "admin/common/rediskeys"
	"admin/common/runtimecfg"
	"admin/internal/config"
	"admin/internal/model"
	"admin/internal/svc"
	"admin/internal/types"

	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

// runProfileStandaloneRedis 模拟真实单机 Redis 的拓扑探测响应。
func runProfileStandaloneRedis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	previous := runtimecfg.Get()
	runtimecfg.Set(config.Config{AppID: "site-a"})
	t.Cleanup(func() {
		runtimecfg.Restore(previous)
	})
	server := miniredis.RunT(t)
	server.Server().SetPreHook(func(peer *miniredisserver.Peer, command string, args ...string) bool {
		if !strings.EqualFold(command, "cluster") || len(args) != 1 || !strings.EqualFold(args[0], "info") {
			return false
		}
		peer.WriteError("ERR This instance has cluster support disabled")
		return true
	})
	return server
}

// newTestProfileSecurityLogic 构造测试依赖。
func newTestProfileSecurityLogic() *securitylogic.SecurityLogic {
	svcCtx := svc.NewServiceContext(config.Config{
		AppID:        "site-a",
		AppKey:       "unit-test-app-key",
		JwtExpiresIn: 3600,
	}, svc.Dependencies{})
	return securitylogic.NewSecurityLogic(context.Background(), svcCtx)
}

// testLoginMFAFlagKey 表示测试辅助逻辑。
func testLoginMFAFlagKey(logicObj *securitylogic.SecurityLogic, adminID int) string {
	return keys.LoginCheckMFAFlagRedisKey(adminID)
}

// seedBoolSecurityConfig 在 Redis 中写入布尔型安全配置缓存，供单测直接复用读取链路。
func seedBoolSecurityConfig(t *testing.T, client *redis.Client, uuid string, enabled bool) {
	t.Helper()
	if client == nil {
		t.Fatalf("seedBoolSecurityConfig client is nil")
	}
	value := "false"
	if enabled {
		value = "true"
	}
	previous := runtimecfg.Get()
	runtimecfg.Set(config.Config{AppID: "site-a"})
	t.Cleanup(func() {
		runtimecfg.Restore(previous)
	})
	cacheKey := keys.TableCachePrefix() + fmt.Sprintf(keys.SysConfigUUID, uuid)
	if err := client.HSet(context.Background(), cacheKey, map[string]any{
		"type":  "6",
		"value": value,
	}).Err(); err != nil {
		t.Fatalf("seedBoolSecurityConfig HSet failed: %v", err)
	}
}

// TestBuildProfileInfoReturnsMFAURLWhenDisabled 验证未启用 MFA 但已有秘钥时，个人中心仍返回二维码地址供继续绑定。
func TestBuildProfileInfoReturnsMFAURLWhenDisabled(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	securityLogic := newTestProfileSecurityLogic()
	securityLogic.Svc.Rds = client
	seedBoolSecurityConfig(t, client, securitylogic.ConfigAdminMFACheckEnable, false)
	oldSecret := "RCABDVITFNQJJ4VJ"
	cipherText, err := securityLogic.EncryptAdminMFASecret(oldSecret)
	if err != nil {
		t.Fatalf("EncryptAdminMFASecret failed: %v", err)
	}
	info, err := securityLogic.BuildProfileInfo(&model.Admin{
		ID:           9,
		Name:         "admin999",
		MfaSecureKey: cipherText,
		MfaStatus:    0,
	}, "test-token")
	if err != nil {
		t.Fatalf("buildProfileInfo failed: %v", err)
	}
	if info == nil || info.BuildMFAURL == "" {
		t.Fatalf("buildProfileInfo() BuildMFAURL empty, want non-empty")
	}
	if strings.Contains(info.BuildMFAURL, oldSecret) {
		t.Fatalf("buildProfileInfo() should issue fresh MFA url, got reused old secret url %q", info.BuildMFAURL)
	}
}

// TestBuildProfileInfoSkipsLoginMFAWhenNeedResetPassword 验证必须改密时不会在登录后先触发 MFA 校验。
func TestBuildProfileInfoSkipsLoginMFAWhenNeedResetPassword(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	securityLogic := newTestProfileSecurityLogic()
	securityLogic.Svc.Rds = client
	seedBoolSecurityConfig(t, client, securitylogic.ConfigAdminMFACheckEnable, false)
	cipherText, err := securityLogic.EncryptAdminMFASecret("RCABDVITFNQJJ4VJ")
	if err != nil {
		t.Fatalf("EncryptAdminMFASecret failed: %v", err)
	}
	info, err := securityLogic.BuildProfileInfo(&model.Admin{
		ID:                9,
		Name:              "admin999",
		MfaSecureKey:      cipherText,
		MfaStatus:         1,
		NeedResetPassword: 1,
	}, "test-token")
	if err != nil {
		t.Fatalf("buildProfileInfo failed: %v", err)
	}
	if info.NeedResetPassword != 1 {
		t.Fatalf("buildProfileInfo() needResetPassword = %d, want 1", info.NeedResetPassword)
	}
	if info.MFACheck != 0 {
		t.Fatalf("buildProfileInfo() mfaCheck = %d, want 0", info.MFACheck)
	}
}

// TestBuildProfileInfoRequiresBindWhenForceMFAEnabled 验证系统强制启用 MFA 时，未启用账号登录会被要求先绑定并校验。
func TestBuildProfileInfoRequiresBindWhenForceMFAEnabled(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	securityLogic := newTestProfileSecurityLogic()
	securityLogic.Svc.Rds = client
	seedBoolSecurityConfig(t, client, securitylogic.ConfigAdminMFACheckEnable, true)
	cipherText, err := securityLogic.EncryptAdminMFASecret("RCABDVITFNQJJ4VJ")
	if err != nil {
		t.Fatalf("EncryptAdminMFASecret failed: %v", err)
	}

	info, err := securityLogic.BuildProfileInfo(&model.Admin{
		ID:           9,
		Name:         "admin999",
		MfaSecureKey: cipherText,
		MfaStatus:    0,
	}, "test-token")
	if err != nil {
		t.Fatalf("buildProfileInfo failed: %v", err)
	}
	if info == nil {
		t.Fatalf("buildProfileInfo() returned nil info")
	}
	if !info.ForceMFAEnabled {
		t.Fatalf("buildProfileInfo() forceMFAEnabled = false, want true")
	}
	if !info.MFABindRequired {
		t.Fatalf("buildProfileInfo() mfaBindRequired = false, want true")
	}
	if info.MFACheck != 1 {
		t.Fatalf("buildProfileInfo() mfaCheck = %d, want 1", info.MFACheck)
	}
	if info.BuildMFAURL == "" {
		t.Fatalf("buildProfileInfo() BuildMFAURL empty, want non-empty")
	}
}

// TestBuildProfileInfoRequiresBindWhenEnabledMFASecretMissing 验证账号已启用但秘钥不可用时，登录资料会进入绑定流程。
func TestBuildProfileInfoRequiresBindWhenEnabledMFASecretMissing(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	securityLogic := newTestProfileSecurityLogic()
	securityLogic.Svc.Rds = client
	seedBoolSecurityConfig(t, client, securitylogic.ConfigAdminMFACheckEnable, false)

	info, err := securityLogic.BuildProfileInfo(&model.Admin{
		ID:           10,
		Name:         "admin-enabled",
		MfaSecureKey: "broken-secret",
		MfaStatus:    1,
	}, "test-token")
	if err != nil {
		t.Fatalf("buildProfileInfo failed: %v", err)
	}
	if info == nil {
		t.Fatalf("buildProfileInfo() returned nil info")
	}
	if info.ExistMFA {
		t.Fatalf("buildProfileInfo() existMFA = true, want false")
	}
	if !info.MFABindRequired {
		t.Fatalf("buildProfileInfo() mfaBindRequired = false, want true")
	}
	if info.BuildMFAURL == "" {
		t.Fatalf("buildProfileInfo() BuildMFAURL empty, want non-empty")
	}
	if info.MFACheck != 1 {
		t.Fatalf("buildProfileInfo() mfaCheck = %d, want 1", info.MFACheck)
	}
}

// TestMarkLoginMFACompletedAfterEnable 验证启用 MFA 后补写登录 MFA 完成标记时，后续个人资料查询不会立刻被自己拦截。
func TestMarkLoginMFACompletedAfterEnable(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	svcCtx := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client})
	logicObj := securitylogic.NewSecurityLogic(context.Background(), svcCtx)
	lastLoginTime := time.Now()
	admin := &model.Admin{
		ID:            2,
		Name:          "admin999",
		MfaStatus:     1,
		LastLoginTime: &lastLoginTime,
	}
	if err := logicObj.MarkLoginMFACompleted(admin.ID); err != nil {
		t.Fatalf("MarkLoginMFACompleted failed: %v", err)
	}
	passed, err := logicObj.HasPassedLoginMFA(admin)
	if err != nil || !passed {
		t.Fatalf("HasPassedLoginMFA() = %v, error=%v, want true", passed, err)
	}
	cacheKey := testLoginMFAFlagKey(logicObj, admin.ID)
	if !server.Exists(cacheKey) {
		t.Fatalf("login MFA flag key %s not found", cacheKey)
	}
}

// TestSyncLoginMFAAfterPasswordUpdateForForcedReset 验证首次登录改密完成后会补写登录 MFA 完成标记。
func TestSyncLoginMFAAfterPasswordUpdateForForcedReset(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	securityLogic := newTestProfileSecurityLogic()
	securityLogic.Svc.Rds = client
	userLogic := &ProfileLogic{BaseLogic: securityLogic.BaseLogic}

	userLogic.syncLoginMFAAfterPasswordUpdate(&model.Admin{
		ID:                21,
		Name:              "admin-reset",
		NeedResetPassword: 1,
	})

	cacheKey := testLoginMFAFlagKey(securityLogic, 21)
	if !server.Exists(cacheKey) {
		t.Fatalf("login MFA flag key %s not found after forced reset password update", cacheKey)
	}
}

// TestSyncLoginMFAAfterPasswordUpdateKeepsExistingFlag 验证普通改自己密码不会清空当前会话已完成的登录 MFA 标记。
func TestSyncLoginMFAAfterPasswordUpdateKeepsExistingFlag(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	securityLogic := newTestProfileSecurityLogic()
	securityLogic.Svc.Rds = client
	userLogic := &ProfileLogic{BaseLogic: securityLogic.BaseLogic}

	cacheKey := testLoginMFAFlagKey(securityLogic, 22)
	if err := client.Set(context.Background(), cacheKey, 123456789, time.Minute).Err(); err != nil {
		t.Fatalf("seed login MFA flag failed: %v", err)
	}

	userLogic.syncLoginMFAAfterPasswordUpdate(&model.Admin{
		ID:                22,
		Name:              "admin-normal",
		NeedResetPassword: 0,
	})

	value, err := client.Get(context.Background(), cacheKey).Result()
	if err != nil {
		t.Fatalf("Get(%s) error = %v", cacheKey, err)
	}
	if value != "123456789" {
		t.Fatalf("login MFA flag = %s, want 123456789", value)
	}
}

// TestSyncCurrentAdminNeedResetPassword 验证个人中心改密成功后，会立即把当前登录态缓存里的 needResetPassword 同步为 0。
func TestSyncCurrentAdminNeedResetPassword(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	securityLogic := newTestProfileSecurityLogic()
	securityLogic.Svc.Rds = client
	userLogic := &ProfileLogic{BaseLogic: securityLogic.BaseLogic}

	cacheLogic := cachelogic.NewCacheLogic(context.Background(), securityLogic.Svc)
	if err := cacheLogic.SetAdminSession(26, &types.AdminSession{
		ID:                26,
		UserName:          "admin-reset-flag",
		NeedResetPassword: 1,
		Token:             "token-26",
	}); err != nil {
		t.Fatalf("SetAdminSession failed: %v", err)
	}

	if err := userLogic.syncCurrentAdminNeedResetPassword(26, 0); err != nil {
		t.Fatalf("syncCurrentAdminNeedResetPassword() error = %v", err)
	}

	info, err := cacheLogic.GetAdminSession(26)
	if err != nil {
		t.Fatalf("GetAdminSession failed: %v", err)
	}
	if info.NeedResetPassword != 0 {
		t.Fatalf("needResetPassword = %d, want 0", info.NeedResetPassword)
	}
}

// TestSyncLoginMFAAfterDisableKeepsCurrentSessionWhenForceEnabled 验证强制 MFA 下当前会话保持有效。
func TestSyncLoginMFAAfterDisableKeepsCurrentSessionWhenForceEnabled(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	securityLogic := newTestProfileSecurityLogic()
	securityLogic.Svc.Rds = client
	seedBoolSecurityConfig(t, client, securitylogic.ConfigAdminMFACheckEnable, true)
	userLogic := &ProfileLogic{BaseLogic: securityLogic.BaseLogic}

	userLogic.syncLoginMFAAfterBindingChange(securityLogic, &model.Admin{
		ID:   23,
		Name: "admin-disable-force",
	})

	cacheKey := testLoginMFAFlagKey(securityLogic, 23)
	if !server.Exists(cacheKey) {
		t.Fatalf("login MFA flag key %s not found after disabling MFA in force mode", cacheKey)
	}
}

// TestSyncLoginMFAAfterDisableKeepsCurrentSessionWhenForceDisabled 验证非强制 MFA 下当前会话保持有效。
func TestSyncLoginMFAAfterDisableKeepsCurrentSessionWhenForceDisabled(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	securityLogic := newTestProfileSecurityLogic()
	securityLogic.Svc.Rds = client
	userLogic := &ProfileLogic{BaseLogic: securityLogic.BaseLogic}

	cacheKey := testLoginMFAFlagKey(securityLogic, 24)
	if err := client.Set(context.Background(), cacheKey, 123456789, time.Minute).Err(); err != nil {
		t.Fatalf("seed login MFA flag failed: %v", err)
	}

	userLogic.syncLoginMFAAfterBindingChange(securityLogic, &model.Admin{
		ID:   24,
		Name: "admin-disable-normal",
	})

	if !server.Exists(cacheKey) {
		t.Fatalf("login MFA flag key %s should keep existing after disabling MFA without force mode", cacheKey)
	}
}

// TestSyncLoginMFAAfterEnableKeepsCurrentSession 验证启用 MFA 后补写当前会话标记。
func TestSyncLoginMFAAfterEnableKeepsCurrentSession(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	securityLogic := newTestProfileSecurityLogic()
	securityLogic.Svc.Rds = client
	userLogic := &ProfileLogic{BaseLogic: securityLogic.BaseLogic}

	userLogic.syncLoginMFAAfterBindingChange(securityLogic, &model.Admin{
		ID:   25,
		Name: "admin-enable",
	})

	cacheKey := testLoginMFAFlagKey(securityLogic, 25)
	if !server.Exists(cacheKey) {
		t.Fatalf("login MFA flag key %s not found after enabling MFA", cacheKey)
	}
}

// TestResolveEnableMFASecretUsesRequestSecret 验证启用 MFA 时优先使用请求新秘钥。
func TestResolveEnableMFASecretUsesRequestSecret(t *testing.T) {
	securityLogic := newTestProfileSecurityLogic()
	oldSecret := "RCABDVITFNQJJ4VJ"
	requestSecret := "JBSWY3DPEHPK3PXP"
	cipherText, err := securityLogic.EncryptAdminMFASecret(oldSecret)
	if err != nil {
		t.Fatalf("EncryptAdminMFASecret failed: %v", err)
	}
	userLogic := &ProfileLogic{BaseLogic: securityLogic.BaseLogic}
	secret, err := userLogic.resolveEnableMFASecret(&model.Admin{
		ID:           11,
		Name:         "admin-request",
		MfaSecureKey: cipherText,
		MfaStatus:    0,
	}, requestSecret, &securitylogic.MFATwoStepTicketPayload{
		Scenario:     securitylogic.MFAScenarioStatus,
		Value:        "ticket-value",
		SecretSource: securitylogic.MFATwoStepSecretSourceRequest,
		SecretDigest: securitylogic.HashMFASecret(requestSecret),
	})
	if err != nil {
		t.Fatalf("resolveEnableMFASecret failed: %v", err)
	}
	if secret != requestSecret {
		t.Fatalf("resolveEnableMFASecret() = %q, want %q", secret, requestSecret)
	}
}

// TestResolveEnableMFASecretUsesCurrentSecret 验证启用 MFA 时可保留数据库当前秘钥。
func TestResolveEnableMFASecretUsesCurrentSecret(t *testing.T) {
	securityLogic := newTestProfileSecurityLogic()
	currentSecret := "RCABDVITFNQJJ4VJ"
	requestSecret := "JBSWY3DPEHPK3PXP"
	cipherText, err := securityLogic.EncryptAdminMFASecret(currentSecret)
	if err != nil {
		t.Fatalf("EncryptAdminMFASecret failed: %v", err)
	}
	userLogic := &ProfileLogic{BaseLogic: securityLogic.BaseLogic}
	secret, err := userLogic.resolveEnableMFASecret(&model.Admin{
		ID:           12,
		Name:         "admin-current",
		MfaSecureKey: cipherText,
		MfaStatus:    0,
	}, requestSecret, &securitylogic.MFATwoStepTicketPayload{
		Scenario:     securitylogic.MFAScenarioStatus,
		Value:        "ticket-value",
		SecretSource: securitylogic.MFATwoStepSecretSourceCurrent,
		SecretDigest: securitylogic.HashMFASecret(currentSecret),
	})
	if err != nil {
		t.Fatalf("resolveEnableMFASecret failed: %v", err)
	}
	if secret != currentSecret {
		t.Fatalf("resolveEnableMFASecret() = %q, want %q", secret, currentSecret)
	}
}

// TestCheckAdminMFARequiresWhenForceMFAEnabledAndDisabled 验证系统强制启用 MFA 时，未启用账号访问会被登录态 MFA 拦截。
func TestCheckAdminMFARequiresWhenForceMFAEnabledAndDisabled(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	securityLogic := newTestProfileSecurityLogic()
	securityLogic.Svc.Rds = client
	seedBoolSecurityConfig(t, client, securitylogic.ConfigAdminMFACheckEnable, true)

	admin := &model.Admin{
		ID:        9,
		Name:      "admin999",
		MfaStatus: 0,
	}
	// needLoginMFA 表示当前账号是否必须完成登录 MFA。
	needLoginMFA, err := securityLogic.NeedLoginMFA(admin)
	if err != nil {
		t.Fatalf("NeedLoginMFA() error = %v", err)
	}
	if !needLoginMFA {
		t.Fatalf("NeedLoginMFA() = false, want true")
	}
	// needBindMFA 表示当前账号是否需要先绑定 MFA。
	needBindMFA, err := securityLogic.NeedBindMFAOnLogin(admin)
	if err != nil {
		t.Fatalf("NeedBindMFAOnLogin() error = %v", err)
	}
	if !needBindMFA {
		t.Fatalf("NeedBindMFAOnLogin() = false, want true")
	}
}

// TestNeedBindMFAOnLoginRequiresEnabledAccountSecret 验证已启用 MFA 但秘钥缺失的账号会进入登录绑定流程。
func TestNeedBindMFAOnLoginRequiresEnabledAccountSecret(t *testing.T) {
	securityLogic := newTestProfileSecurityLogic()
	admin := &model.Admin{
		ID:           10,
		Name:         "admin-enabled",
		MfaSecureKey: "broken-secret",
		MfaStatus:    1,
	}
	// needLoginMFA 表示秘钥损坏账号是否仍需登录 MFA。
	needLoginMFA, err := securityLogic.NeedLoginMFA(admin)
	if err != nil {
		t.Fatalf("NeedLoginMFA() error = %v", err)
	}
	if !needLoginMFA {
		t.Fatalf("NeedLoginMFA() = false, want true")
	}
	// needBindMFA 表示秘钥损坏账号是否进入重新绑定流程。
	needBindMFA, err := securityLogic.NeedBindMFAOnLogin(admin)
	if err != nil {
		t.Fatalf("NeedBindMFAOnLogin() error = %v", err)
	}
	if !needBindMFA {
		t.Fatalf("NeedBindMFAOnLogin() = false, want true")
	}
}

// TestSyncCurrentAdminSessionFieldsPreservesToken 验证个人资料同步只更新公开字段，不轮换或删除当前 token。
func TestSyncCurrentAdminSessionFieldsPreservesToken(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	cfg := config.Config{AppID: "site-a", JwtExpiresIn: 3600}
	previous := runtimecfg.Get()
	runtimecfg.Set(cfg)
	t.Cleanup(func() { runtimecfg.Restore(previous) })
	svcCtx := svc.NewServiceContext(cfg, svc.Dependencies{Rds: client})
	cacheLogic := cachelogic.NewCacheLogic(context.Background(), svcCtx)
	if err := cacheLogic.SetAdminSession(7, &types.AdminSession{
		ID:          7,
		UserName:    "admin",
		RealName:    "旧姓名",
		Email:       "old@example.com",
		Phone:       "13000000000",
		Avatar:      "old-avatar",
		Description: "旧说明",
		Token:       "current-token",
	}); err != nil {
		t.Fatalf("SetAdminSession() error = %v", err)
	}
	logicObj := &ProfileLogic{BaseLogic: cacheLogic.BaseLogic}

	if err := logicObj.syncCurrentAdminSessionFields(7, map[string]any{
		"avatar":      "new-avatar",
		"description": "新说明",
		"email":       "new@example.com",
		"phone":       "13800000000",
		"realName":    "新姓名",
	}); err != nil {
		t.Fatalf("syncCurrentAdminSessionFields() error = %v", err)
	}
	info, err := cacheLogic.GetAdminSession(7)
	if err != nil {
		t.Fatalf("GetAdminSession() error = %v", err)
	}
	if info.Token != "current-token" || info.RealName != "新姓名" || info.Email != "new@example.com" ||
		info.Phone != "13800000000" || info.Avatar != "new-avatar" || info.Description != "新说明" {
		t.Fatalf("个人资料同步后会话字段异常: %+v", info)
	}
}

// TestSyncCurrentAdminSessionFieldsDoesNotRecreateMissingSession 验证已撤销会话不会被个人资料更新重新创建。
func TestSyncCurrentAdminSessionFieldsDoesNotRecreateMissingSession(t *testing.T) {
	server := runProfileStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	cfg := config.Config{AppID: "site-a", JwtExpiresIn: 3600}
	previous := runtimecfg.Get()
	runtimecfg.Set(cfg)
	t.Cleanup(func() { runtimecfg.Restore(previous) })
	svcCtx := svc.NewServiceContext(cfg, svc.Dependencies{Rds: client})
	logicObj := &ProfileLogic{BaseLogic: cachelogic.NewCacheLogic(context.Background(), svcCtx).BaseLogic}

	if err := logicObj.syncCurrentAdminSessionFields(7, map[string]any{"avatar": "new-avatar"}); err != nil {
		t.Fatalf("syncCurrentAdminSessionFields() error = %v", err)
	}
	if server.Exists(keys.AdminSessionRedisKey(7)) {
		t.Fatal("个人资料同步不应复活已撤销会话")
	}
}

// TestSyncCurrentAdminSessionFieldsFailsClosedWithoutRedis 验证会话字段无法同步时进入安全缓存阻断状态。
func TestSyncCurrentAdminSessionFieldsFailsClosedWithoutRedis(t *testing.T) {
	previous := runtimecfg.Get()
	runtimecfg.Set(config.Config{AppID: "site-a"})
	t.Cleanup(func() { runtimecfg.Restore(previous) })
	svcCtx := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{})
	logicObj := &ProfileLogic{BaseLogic: cachelogic.NewCacheLogic(context.Background(), svcCtx).BaseLogic}

	if err := logicObj.syncCurrentAdminSessionFields(7, map[string]any{"mfaStatus": 1}); err == nil {
		t.Fatal("syncCurrentAdminSessionFields() error = nil, want Redis unavailable failure")
	}
	if !svcCtx.SecurityCacheSyncPending() {
		t.Fatal("会话字段同步失败后应立即关闭当前进程缓存鉴权")
	}
}
