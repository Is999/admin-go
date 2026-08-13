package security

import (
	corelogic "admin/internal/logic"
	cachelogic "admin/internal/logic/cache"
	configlogic "admin/internal/logic/config"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"fmt"
	"math/big"
	"net/url"
	"strconv"
	"strings"
	"time"

	keys "admin/common/rediskeys"
	"admin/internal/model"
	"admin/internal/types"

	"github.com/Is999/go-utils"
	"github.com/Is999/go-utils/errors"
	"github.com/google/uuid"
	"github.com/pquerna/otp/totp"
	"github.com/redis/go-redis/v9"
)

const (
	// mfaDefaultIssuer 表示默认的 MFA 发行方名称。
	mfaDefaultIssuer = "admin"
	// mfaSecretCipherPrefix 表示 MFA 秘钥密文前缀，便于快速识别当前密文版本。
	mfaSecretCipherPrefix = "mfa:v1:"
	// mfaTwoStepDefaultTTL 表示未配置校验频率时，二次校验票据默认沿用 300 秒窗口。
	mfaTwoStepDefaultTTL = 300 * time.Second
	// loginMFAFlagToleranceSeconds 为登录时间与 MFA 完成标记之间允许的秒级容差。
	// 登录请求写库时间与随后 MFA 校验写 Redis 时间可能落在相邻秒，容差用于消除该类抖动。
	loginMFAFlagToleranceSeconds int64 = 1

	// mfaTwoStepSecretSourceRequest 表示本次 MFA 校验通过的是前端当前携带的新秘钥。
	mfaTwoStepSecretSourceRequest = "request"
	// mfaTwoStepSecretSourceCurrent 表示本次 MFA 校验通过的是数据库当前保留的当前秘钥。
	mfaTwoStepSecretSourceCurrent = "current"
)

const (
	// MFATwoStepSecretSourceRequest 表示二次票据校验通过的是前端本次携带的新秘钥。
	MFATwoStepSecretSourceRequest = mfaTwoStepSecretSourceRequest
	// MFATwoStepSecretSourceCurrent 表示二次票据校验通过的是数据库当前保留的秘钥。
	MFATwoStepSecretSourceCurrent = mfaTwoStepSecretSourceCurrent
)

// MFA 校验场景常量，保持与前端 `CheckMfaScenariosEnum` 一致。
const (
	// MFAScenarioLogin 表示登录后的 MFA 校验场景。
	MFAScenarioLogin = 0
	// MFAScenarioChangePassword 表示修改密码场景。
	MFAScenarioChangePassword = 1
	// MFAScenarioStatus 表示修改 MFA 状态场景。
	MFAScenarioStatus = 2
	// MFAScenarioSecret 表示修改 MFA 秘钥场景。
	MFAScenarioSecret = 3
	// MFAScenarioUserStatus 表示修改管理员状态场景。
	MFAScenarioUserStatus = 4
	// MFAScenarioAddUser 表示新增管理员场景。
	MFAScenarioAddUser = 5
	// MFAScenarioEditUser 表示编辑管理员场景。
	MFAScenarioEditUser = 6
	// MFAScenarioResetUserPassword 表示后台重置管理员密码场景。
	MFAScenarioResetUserPassword = 7
	// MFAScenarioResetUserInitialState 表示后台重置管理员首次状态场景。
	MFAScenarioResetUserInitialState = 8
	// MFAScenarioDeleteUser 表示后台删除管理员场景。
	MFAScenarioDeleteUser = 9
	// MFAScenarioUserTagLeaseRelease 表示释放用户标签工作流互斥租约场景。
	MFAScenarioUserTagLeaseRelease = 10
	// MFAScenarioSecretKeyManage 表示秘钥管理敏感操作场景。
	MFAScenarioSecretKeyManage = 11
	// MFAScenarioRuntimeConfigManage 表示运行配置发布和回滚场景。
	MFAScenarioRuntimeConfigManage = 12
	// MFAScenarioUserManage 表示前台用户管理敏感操作场景。
	MFAScenarioUserManage = 13
	// MFAScenarioAPIRuntimeManage 表示 API 运行态热加载管理场景。
	MFAScenarioAPIRuntimeManage = 14
)

// mfaBindingVerifyResult 表示绑定流程里实际校验通过的秘钥来源与摘要。
type mfaBindingVerifyResult struct {
	SecretSource string // 通过校验的秘钥来源：request/current
	SecretDigest string // 通过校验的归一化秘钥摘要，用于二次票据防篡改
}

// mfaTwoStepTicketPayload 表示 Redis 中缓存的 MFA 二次票据扩展内容。
type mfaTwoStepTicketPayload struct {
	Scenario     int    // 票据对应的 MFA 场景
	Value        string // 票据随机值
	ExpiresAt    int64  // 票据过期时间，Unix 毫秒
	SecretSource string // 绑定校验通过的秘钥来源
	SecretDigest string // 绑定校验通过的秘钥摘要
}

// MFATwoStepTicketPayload 是 MFA 二次票据的跨领域只读载荷。
type MFATwoStepTicketPayload = mfaTwoStepTicketPayload

// HasPassedLoginMFA 判断当前管理员是否已经完成本次登录后的 MFA 校验。
func (l *SecurityLogic) HasPassedLoginMFA(admin *model.Admin) (bool, error) {
	if admin == nil {
		return false, nil
	}
	if l.Redis() == nil {
		return false, cachelogic.WrapRedisUnavailable(nil, "读取管理员登录MFA状态失败")
	}
	cacheKey := l.loginMFAFlagKey(admin.ID)
	if cacheKey == "" {
		return false, cachelogic.WrapRedisUnavailable(nil, "读取管理员登录MFA状态失败：缓存 Key 未初始化")
	}
	flag, err := l.Redis().Get(l.Ctx, cacheKey).Int64()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, cachelogic.WrapRedisUnavailable(err, "读取管理员登录MFA状态失败")
	}
	return loginMFAFlagMatches(flag, admin.LastLoginTime), nil
}

// MarkLoginMFACompleted 标记当前管理员已经完成本次登录后的 MFA 校验。
func (l *SecurityLogic) MarkLoginMFACompleted(adminID int) error {
	if adminID <= 0 {
		return nil
	}
	if l.Redis() == nil {
		return cachelogic.WrapRedisUnavailable(nil, "写入管理员登录MFA状态失败")
	}
	cacheKey := l.loginMFAFlagKey(adminID)
	if cacheKey == "" {
		return cachelogic.WrapRedisUnavailable(nil, "写入管理员登录MFA状态失败：缓存 Key 未初始化")
	}
	if err := l.Redis().Set(l.Ctx, cacheKey, time.Now().Unix(), loginCheckMFAFlagTTL()).Err(); err != nil {
		return cachelogic.WrapRedisUnavailable(err, "写入管理员登录MFA状态失败")
	}
	return nil
}

// loginMFAFlagMatches 判断 Redis 中的登录 MFA 完成标记是否覆盖当前登录会话。
func loginMFAFlagMatches(flag int64, lastLoginTime *time.Time) bool {
	if lastLoginTime == nil || lastLoginTime.IsZero() {
		return flag > 0
	}
	return flag+loginMFAFlagToleranceSeconds >= lastLoginTime.Unix()
}

// ClearLoginMFACompleted 清理当前管理员登录后的 MFA 校验标记。
func (l *SecurityLogic) ClearLoginMFACompleted(adminID int) error {
	if adminID <= 0 {
		return nil
	}
	if l.Redis() == nil {
		return cachelogic.WrapRedisUnavailable(nil, "清理管理员登录MFA状态失败")
	}
	cacheKey := l.loginMFAFlagKey(adminID)
	if cacheKey == "" {
		return cachelogic.WrapRedisUnavailable(nil, "清理管理员登录MFA状态失败：缓存 Key 未初始化")
	}
	if err := l.Redis().Del(l.Ctx, cacheKey).Err(); err != nil {
		return cachelogic.WrapRedisUnavailable(err, "清理管理员登录MFA状态失败")
	}
	return nil
}

// verifyMFACodeBySecret 按给定 MFA 秘钥校验动态码。
func verifyMFACodeBySecret(secret string, code string) error {
	secret = NormalizeMFASecret(secret)
	if !IsUsableMFASecret(secret) {
		return ErrAdminMFACodeInvalid
	}
	ok, err := totp.ValidateCustom(strings.TrimSpace(code), secret, time.Now(), totp.ValidateOpts{
		Period:    30,
		Skew:      1,
		Digits:    6,
		Algorithm: 0,
	})
	if err != nil {
		return errors.Tag(err)
	}
	if !ok {
		return ErrAdminMFACodeInvalid
	}
	return nil
}

// VerifyMFACode 校验管理员当前输入的 MFA 动态验证码。
func (l *SecurityLogic) VerifyMFACode(admin *model.Admin, code string) error {
	if admin == nil {
		return ErrAdminNotFound
	}
	secret, err := l.LoadAdminMFASecret(admin)
	if err != nil {
		return errors.Tag(err)
	}
	return verifyMFACodeBySecret(secret, code)
}

// VerifyBindingMFACode 在未启用绑定流程下优先校验前端回传的待绑定秘钥，
// 避免刷新二维码时提前替换库内正式秘钥。
func (l *SecurityLogic) VerifyBindingMFACode(admin *model.Admin, secret string, code string) error {
	_, err := l.VerifyBindingMFACodeDetail(admin, secret, code)
	return errors.Tag(err)
}

// VerifyBindingMFACodeDetail 校验绑定流程动态码，并返回实际通过校验的秘钥来源。
func (l *SecurityLogic) VerifyBindingMFACodeDetail(admin *model.Admin, secret string, code string) (*mfaBindingVerifyResult, error) {
	if admin == nil {
		return nil, ErrAdminNotFound
	}
	requestSecret := NormalizeMFASecret(secret)
	currentSecret, err := l.LoadAdminMFASecret(admin)
	if err != nil {
		return nil, errors.Tag(err)
	}
	currentSecretUsable := IsUsableMFASecret(currentSecret)
	if admin.MfaStatus == 1 && currentSecretUsable {
		if err := verifyMFACodeBySecret(currentSecret, code); err != nil {
			return nil, errors.Tag(err)
		}
		return &mfaBindingVerifyResult{
			SecretSource: mfaTwoStepSecretSourceCurrent,
			SecretDigest: HashMFASecret(currentSecret),
		}, nil
	}
	if IsUsableMFASecret(requestSecret) {
		if err := verifyMFACodeBySecret(requestSecret, code); err == nil {
			return &mfaBindingVerifyResult{
				SecretSource: mfaTwoStepSecretSourceRequest,
				SecretDigest: HashMFASecret(requestSecret),
			}, nil
		}
	}
	if currentSecretUsable && currentSecret != requestSecret {
		if err := verifyMFACodeBySecret(currentSecret, code); err == nil {
			return &mfaBindingVerifyResult{
				SecretSource: mfaTwoStepSecretSourceCurrent,
				SecretDigest: HashMFASecret(currentSecret),
			}, nil
		}
	}
	return nil, ErrAdminMFACodeInvalid
}

// IssueMFATwoStepTicket 生成当前管理员指定场景的二次校验票据。
func (l *SecurityLogic) IssueMFATwoStepTicket(adminID int, scenario int) (*types.ProfileTwoStepResp, error) {
	return l.issueMFATwoStepTicket(adminID, scenario, nil)
}

// IssueMFATwoStepTicketWithVerifyResult 按指定的秘钥校验结果生成二次校验票据。
func (l *SecurityLogic) IssueMFATwoStepTicketWithVerifyResult(adminID int, scenario int, verifyResult *mfaBindingVerifyResult) (*types.ProfileTwoStepResp, error) {
	return l.issueMFATwoStepTicket(adminID, scenario, verifyResult)
}

// issueMFATwoStepTicket 生成当前管理员指定场景的二次校验票据，并按需附带绑定秘钥来源元信息。
func (l *SecurityLogic) issueMFATwoStepTicket(adminID int, scenario int, verifyResult *mfaBindingVerifyResult) (*types.ProfileTwoStepResp, error) {
	if adminID <= 0 {
		return nil, errors.Errorf("管理员ID不能为空")
	}
	if l.Redis() == nil {
		return nil, cachelogic.WrapRedisUnavailable(nil, "签发MFA二次校验票据失败")
	}
	key := uuid.NewString()
	value, err := utils.SecureRandomLetters(32)
	if err != nil {
		return nil, errors.Wrap(err, "生成MFA二次校验票据失败")
	}
	ttl := l.mfaFrequencyTTL()
	expiresAt := time.Now().Add(ttl).UnixMilli()
	cacheValue := encodeMFATwoStepTicketPayload(&mfaTwoStepTicketPayload{
		Scenario:  scenario,
		Value:     value,
		ExpiresAt: expiresAt,
	})
	if verifyResult != nil {
		cacheValue = encodeMFATwoStepTicketPayload(&mfaTwoStepTicketPayload{
			Scenario:     scenario,
			Value:        value,
			ExpiresAt:    expiresAt,
			SecretSource: strings.TrimSpace(verifyResult.SecretSource),
			SecretDigest: strings.TrimSpace(verifyResult.SecretDigest),
		})
	}
	cacheKey := l.mfaTwoStepKey(adminID)
	if cacheKey == "" {
		return nil, cachelogic.WrapRedisUnavailable(nil, "签发MFA二次校验票据失败：缓存 Key 未初始化")
	}
	if _, err := setMFATwoStepTicketScript.Run(
		l.Ctx,
		l.Redis(),
		[]string{cacheKey},
		key,
		cacheValue,
		ttl.Milliseconds(),
	).Result(); err != nil {
		return nil, cachelogic.WrapRedisUnavailable(err, "签发MFA二次校验票据失败")
	}
	return &types.ProfileTwoStepResp{
		Key:    key,
		Time:   time.Now().Unix(),
		Expire: int64(ttl.Seconds()),
		Value:  value,
	}, nil
}

// VerifyMFATwoStepTicket 校验指定场景的二次校验票据。
// 普通敏感操作允许在窗口内复用票据；MFA 状态、MFA 秘钥和秘钥管理必须同场景校验。
func (l *SecurityLogic) VerifyMFATwoStepTicket(adminID int, scenario int, key string, value string) error {
	_, err := l.verifyMFATwoStepTicketPayload(adminID, scenario, key, value, false)
	return errors.Tag(err)
}

// ConsumeMFATwoStepTicket 严格按指定场景校验二次校验票据，并返回票据携带的元信息。
// 当前频率窗口允许重复复用同一张票据，因此这里不再删除 Redis 中的原票据。
func (l *SecurityLogic) ConsumeMFATwoStepTicket(adminID int, scenario int, key string, value string) (*mfaTwoStepTicketPayload, error) {
	return l.verifyMFATwoStepTicketPayload(adminID, scenario, key, value, true)
}

// ClearAdminMFATwoStepTickets 按管理员维度精确清理 MFA 二次校验票据 Hash。
func (l *SecurityLogic) ClearAdminMFATwoStepTickets(adminID int) error {
	if adminID <= 0 {
		return nil
	}
	if l.Redis() == nil {
		return cachelogic.WrapRedisUnavailable(nil, "清理管理员MFA二次校验票据失败")
	}
	cacheKey := l.mfaTwoStepKey(adminID)
	if cacheKey == "" {
		return cachelogic.WrapRedisUnavailable(nil, "清理管理员MFA二次校验票据失败：缓存 Key 未初始化")
	}
	if err := l.RdsDelKeys(cacheKey); err != nil {
		return cachelogic.WrapRedisUnavailable(err, "清理管理员MFA二次校验票据失败")
	}
	return nil
}

// verifyMFATwoStepTicketPayload 校验指定场景的二次校验票据，并按需返回票据携带的元信息。
func (l *SecurityLogic) verifyMFATwoStepTicketPayload(adminID int, scenario int, key string, value string, strictScenario bool) (*mfaTwoStepTicketPayload, error) {
	if adminID <= 0 {
		return nil, ErrAdminMFATwoStepExpired
	}
	if l.Redis() == nil {
		return nil, cachelogic.WrapRedisUnavailable(nil, "校验MFA二次校验票据失败")
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" {
		return nil, ErrAdminMFATwoStepExpired
	}
	cacheKey := l.mfaTwoStepKey(adminID)
	if cacheKey == "" {
		return nil, cachelogic.WrapRedisUnavailable(nil, "校验MFA二次校验票据失败：缓存 Key 未初始化")
	}
	cacheValue, err := l.Redis().HGet(l.Ctx, cacheKey, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrAdminMFATwoStepExpired
		}
		return nil, cachelogic.WrapRedisUnavailable(err, "读取MFA二次校验票据失败")
	}
	payload, err := decodeMFATwoStepTicketPayload(cacheValue)
	if err != nil || payload == nil {
		return nil, ErrAdminMFATwoStepExpired
	}
	if payload.ExpiresAt <= time.Now().UnixMilli() {
		_ = l.Redis().HDel(l.Ctx, cacheKey, key).Err()
		return nil, ErrAdminMFATwoStepExpired
	}
	if payload.Value != value {
		return nil, ErrAdminMFATwoStepExpired
	}
	if !mfaTwoStepScenarioMatches(scenario, payload.Scenario, strictScenario) {
		return nil, ErrAdminMFATwoStepExpired
	}
	return payload, nil
}

// MFAFrequency 返回后台 MFA 校验频率，单位秒。
func (l *SecurityLogic) MFAFrequency() int {
	configCtx, cancel := optionalSecurityConfigContext(l.Ctx)
	defer cancel()
	value, err := configlogic.NewSysConfigLogicWithContext(configCtx, l.Svc).GetCachedValue(ConfigAdminMFACheckFrequency)
	if err != nil {
		return 0
	}
	switch v := value.(type) {
	case int:
		if v > 0 {
			return v
		}
	case float64:
		if int(v) > 0 {
			return int(v)
		}
	case string:
		number, convErr := strconv.Atoi(strings.TrimSpace(v))
		if convErr == nil && number > 0 {
			return number
		}
	}
	return 0
}

// MFADisabledScenarios 返回禁用 MFA 校验的场景列表。
func (l *SecurityLogic) MFADisabledScenarios() []int {
	items := SecurityConfigStringSlice(l.Ctx, l.Svc, ConfigAdminDisableMFACheckScenario, nil)
	result := make([]int, 0, len(items))
	for _, item := range items {
		number, err := strconv.Atoi(strings.TrimSpace(item))
		if err == nil && number >= 0 {
			result = append(result, number)
		}
	}
	return types.UniquePositiveInts(result)
}

// IsMFAScenarioDisabled 判断指定场景是否禁用 MFA 校验。
func (l *SecurityLogic) IsMFAScenarioDisabled(scenario int) bool {
	if scenario <= MFAScenarioLogin {
		return false
	}
	for _, item := range l.MFADisabledScenarios() {
		if item == scenario {
			return true
		}
	}
	return false
}

// NeedMFATwoStep 判断指定管理员在当前场景下是否需要二次校验票据。
func (l *SecurityLogic) NeedMFATwoStep(admin *model.Admin, scenario int) (bool, error) {
	if admin == nil {
		return false, nil
	}
	// 首次登录或管理员重置后的临时密码阶段，用户必须先完成改密，MFA 允许稍后再设置。
	if scenario == MFAScenarioChangePassword && admin.NeedResetPassword == 1 {
		return false, nil
	}
	forceMFA, err := l.ForceLoginMFAEnabled()
	if err != nil {
		return false, errors.Tag(err)
	}
	if !forceMFA && admin.MfaStatus != 1 {
		return false, nil
	}
	if l.IsMFAScenarioDisabled(scenario) {
		return false, nil
	}
	return true, nil
}

// NeedOperateMFATwoStep 判断后台管理类敏感操作是否需要 MFA 二次校验票据。
// 这类“代操作”场景统一只受系统强制开关和禁用场景配置控制，不再额外要求操作者自己已启用 MFA。
func (l *SecurityLogic) NeedOperateMFATwoStep(scenario int) (bool, error) {
	if scenario <= MFAScenarioLogin {
		return false, nil
	}
	forceMFA, err := l.ForceLoginMFAEnabled()
	if err != nil {
		return false, errors.Tag(err)
	}
	if !forceMFA {
		return false, nil
	}
	if l.IsMFAScenarioDisabled(scenario) {
		return false, nil
	}
	return true, nil
}

// buildAdminMFAURLBySecret 使用给定秘钥拼装管理员 MFA 绑定地址。
func buildAdminMFAURLBySecret(admin *model.Admin, secret string, issuer string) (string, error) {
	if admin == nil {
		return "", ErrAdminNotFound
	}
	account := sanitizeMFALabelValue(admin.Name)
	if account == "" {
		account = fmt.Sprintf("admin-%d", admin.ID)
	}
	issuer = normalizeMFAIssuer(issuer)
	secret = NormalizeMFASecret(secret)
	if _, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(secret); err != nil {
		return "", errors.Wrap(err, "解析管理员MFA秘钥失败")
	}
	return buildMFAOtpauthURL(issuer, account, secret), nil
}

// BuildAdminMFAURLBySecret 使用给定秘钥拼装管理员 MFA 绑定地址。
func BuildAdminMFAURLBySecret(admin *model.Admin, secret string) (string, error) {
	return buildAdminMFAURLBySecret(admin, secret, buildMFAIssuer(""))
}

// BuildAdminMFAURLBySecret 使用当前站点发行方拼装管理员 MFA 绑定地址。
func (l *SecurityLogic) BuildAdminMFAURLBySecret(admin *model.Admin, secret string) (string, error) {
	return buildAdminMFAURLBySecret(admin, secret, l.mfaIssuer())
}

// BuildAdminMFAURL 生成管理员 MFA 绑定地址。
func (l *SecurityLogic) BuildAdminMFAURL(admin *model.Admin) (string, error) {
	if admin == nil {
		return "", ErrAdminNotFound
	}
	currentSecret, err := l.LoadAdminMFASecret(admin)
	if err != nil {
		return "", errors.Tag(err)
	}
	if IsUsableMFASecret(currentSecret) {
		return l.BuildAdminMFAURLBySecret(admin, currentSecret)
	}
	return l.BuildFreshAdminMFAURL(admin)
}

// BuildFreshAdminMFAURL 生成一张仅用于本次绑定流程的 MFA 二维码，不会提前写入数据库。
func (l *SecurityLogic) BuildFreshAdminMFAURL(admin *model.Admin) (string, error) {
	if admin == nil {
		return "", ErrAdminNotFound
	}
	secret, err := generateMFASecret()
	if err != nil {
		return "", errors.Tag(err)
	}
	return l.BuildAdminMFAURLBySecret(admin, secret)
}

// mfaIssuer 返回当前站点 MFA 发行方，便于多 app_id 后台在身份验证器中区分账号。
func (l *SecurityLogic) mfaIssuer() string {
	if l == nil || l.Svc == nil {
		return buildMFAIssuer("")
	}
	return buildMFAIssuer(l.Svc.CurrentConfig().AppID)
}

// buildMFAIssuer 将固定产品名与 app_id 合成身份验证器发行方。
func buildMFAIssuer(appID string) string {
	base := normalizeMFAIssuer(mfaDefaultIssuer)
	appID = sanitizeMFALabelValue(appID)
	if appID == "" {
		return base
	}
	return fmt.Sprintf("%s-%s", base, appID)
}

// normalizeMFAIssuer 归一化 MFA 发行方名称，避免生成空 issuer。
func normalizeMFAIssuer(issuer string) string {
	issuer = sanitizeMFALabelValue(issuer)
	if issuer == "" {
		return "admin"
	}
	return issuer
}

// buildMFAOtpauthURL 按微软 Authenticator 可识别格式拼装 otpauth 地址。
// 这里显式对 label 做 URL 编码，并固定 query 参数顺序，避免部分身份验证器对二维码解析过于挑剔。
func buildMFAOtpauthURL(issuer string, account string, secret string) string {
	label := fmt.Sprintf("%s:%s", issuer, account)
	escapedLabel := strings.ReplaceAll(url.QueryEscape(label), "+", "%20")
	return fmt.Sprintf(
		"otpauth://totp/%s?secret=%s&issuer=%s&algorithm=SHA1&digits=6&period=30",
		escapedLabel,
		url.QueryEscape(secret),
		url.QueryEscape(issuer),
	)
}

// HasUsableAdminMFASecret 判断管理员当前是否存在可用的 MFA 秘钥。
func (l *SecurityLogic) HasUsableAdminMFASecret(admin *model.Admin) bool {
	secret, err := l.LoadAdminMFASecret(admin)
	if err != nil {
		return false
	}
	return IsUsableMFASecret(secret)
}

// sanitizeMFALabelValue 归一化 otpauth 标签字段，避免多余分隔符影响身份验证器解析。
func sanitizeMFALabelValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = strings.ReplaceAll(value, ":", " ")
	return strings.Join(strings.Fields(value), " ")
}

// mfaFrequencyTTL 返回 MFA 校验频率对应的缓存 TTL。
func (l *SecurityLogic) mfaFrequencyTTL() time.Duration {
	frequency := l.MFAFrequency()
	if frequency <= 0 {
		return mfaTwoStepDefaultTTL
	}
	return time.Duration(frequency) * time.Second
}

// loginMFAFlagKey 返回当前 app_id 作用域下的登录 MFA 完成标记 key。
func (l *SecurityLogic) loginMFAFlagKey(adminID int) string {
	if l == nil || l.BaseLogic == nil {
		return ""
	}
	return l.AppRedisKey(fmt.Sprintf(keys.LoginCheckMFAFlag, adminID))
}

// mfaTwoStepKey 返回当前 app_id 作用域下的管理员 MFA 二次票据 Hash key。
func (l *SecurityLogic) mfaTwoStepKey(adminID int) string {
	if l == nil || l.BaseLogic == nil {
		return ""
	}
	return l.AppRedisKey(fmt.Sprintf(keys.AdminMFATwoStep, adminID))
}

// mfaTwoStepScenarioMatches 判断目标场景是否允许复用当前二次票据。
// MFA 状态、MFA 秘钥和秘钥管理场景必须严格使用同场景票据；
// 其它普通敏感操作在频率窗口内允许复用最近一次已签发的二次票据，减少重复弹框。
func mfaTwoStepScenarioMatches(expectScenario int, ticketScenario int, strictScenario bool) bool {
	if expectScenario == ticketScenario {
		return true
	}
	if strictScenario {
		return false
	}
	return isReusableMFATwoStepScenario(expectScenario) && isReusableMFATwoStepScenario(ticketScenario)
}

// isReusableMFATwoStepScenario 判断指定场景是否允许在频率窗口内复用最近一次 MFA 二次票据。
func isReusableMFATwoStepScenario(scenario int) bool {
	switch scenario {
	case MFAScenarioLogin, MFAScenarioStatus, MFAScenarioSecret, MFAScenarioSecretKeyManage, MFAScenarioRuntimeConfigManage:
		return false
	default:
		return scenario > MFAScenarioLogin
	}
}

// EncryptAdminMFASecret 把 MFA 明文秘钥加密后再写入数据库，避免库内直接暴露种子。
func (l *SecurityLogic) EncryptAdminMFASecret(secret string) (string, error) {
	secret = NormalizeMFASecret(secret)
	if secret == "" {
		return "", nil
	}
	key, err := l.mfaSecretCipherKey()
	if err != nil {
		return "", errors.Tag(err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", errors.Wrap(err, "初始化MFA秘钥加密器失败")
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", errors.Wrap(err, "初始化MFA秘钥GCM失败")
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", errors.Wrap(err, "生成MFA秘钥加密随机数失败")
	}
	cipherText := gcm.Seal(nil, nonce, []byte(secret), nil)
	payload := append(nonce, cipherText...)
	return mfaSecretCipherPrefix + base64.StdEncoding.EncodeToString(payload), nil
}

// decryptAdminMFASecret 把数据库中的 MFA 密文解密成 TOTP 所需的 Base32 明文种子。
func (l *SecurityLogic) decryptAdminMFASecret(cipherText string) (string, error) {
	cipherText = strings.TrimSpace(cipherText)
	if cipherText == "" {
		return "", nil
	}
	if !strings.HasPrefix(cipherText, mfaSecretCipherPrefix) {
		return "", errors.Errorf("MFA秘钥密文格式不正确")
	}
	key, err := l.mfaSecretCipherKey()
	if err != nil {
		return "", errors.Tag(err)
	}
	payload, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(cipherText, mfaSecretCipherPrefix))
	if err != nil {
		return "", errors.Wrap(err, "解码MFA秘钥密文失败")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", errors.Wrap(err, "初始化MFA秘钥解密器失败")
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", errors.Wrap(err, "初始化MFA秘钥GCM失败")
	}
	if len(payload) < gcm.NonceSize() {
		return "", errors.Errorf("MFA秘钥密文长度不合法")
	}
	nonce := payload[:gcm.NonceSize()]
	data := payload[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, data, nil)
	if err != nil {
		return "", errors.Wrap(err, "解密MFA秘钥失败")
	}
	return NormalizeMFASecret(string(plain)), nil
}

// LoadAdminMFASecret 读取管理员当前可用的 MFA 明文秘钥；数据异常时按未绑定处理，避免异常密文继续参与校验。
func (l *SecurityLogic) LoadAdminMFASecret(admin *model.Admin) (string, error) {
	if admin == nil {
		return "", ErrAdminNotFound
	}
	secret, err := l.decryptAdminMFASecret(admin.MfaSecureKey)
	if err != nil {
		corelogic.LogWrappedError(l, err, "SecurityLogic.LoadAdminMFASecret 解析管理员ID[%d]MFA秘钥失败", admin.ID)
		return "", nil
	}
	return secret, nil
}

// mfaSecretCipherKey 从运行时配置派生 32 字节密钥，用于管理员 MFA 秘钥库内加密。
func (l *SecurityLogic) mfaSecretCipherKey() ([]byte, error) {
	if l == nil || l.Svc == nil {
		return nil, errors.Errorf("服务上下文未初始化")
	}
	raw := strings.TrimSpace(l.Svc.CurrentConfig().AppKey)
	if raw == "" {
		return nil, errors.Errorf("app_key 未配置")
	}
	sum := sha256.Sum256([]byte(raw))
	return sum[:], nil
}

// NormalizeMFASecret 归一化 MFA 秘钥，统一转成大写并去掉空白字符。
func NormalizeMFASecret(secret string) string {
	secret = strings.ToUpper(strings.TrimSpace(secret))
	secret = strings.ReplaceAll(secret, " ", "")
	return secret
}

// IsUsableMFASecret 判断当前秘钥是否满足 TOTP 所需的基础格式。
func IsUsableMFASecret(secret string) bool {
	if len(secret) != 16 {
		return false
	}
	for _, ch := range secret {
		if (ch < 'A' || ch > 'Z') && (ch < '2' || ch > '7') {
			return false
		}
	}
	return true
}

// HashMFASecret 返回归一化 MFA 秘钥的稳定摘要，用于二次票据防止后续请求篡改秘钥。
func HashMFASecret(secret string) string {
	secret = NormalizeMFASecret(secret)
	if secret == "" {
		return ""
	}
	return utils.SHA256(secret)
}

// encodeMFATwoStepTicketPayload 把 MFA 二次票据内容编码为 Redis 字符串。
func encodeMFATwoStepTicketPayload(payload *mfaTwoStepTicketPayload) string {
	if payload == nil {
		return ""
	}
	parts := []string{
		strconv.Itoa(payload.Scenario),
		strings.TrimSpace(payload.Value),
		strconv.FormatInt(payload.ExpiresAt, 10),
	}
	if strings.TrimSpace(payload.SecretSource) == "" && strings.TrimSpace(payload.SecretDigest) == "" {
		return strings.Join(parts, ":")
	}
	parts = append(parts, strings.TrimSpace(payload.SecretSource), strings.TrimSpace(payload.SecretDigest))
	return strings.Join(parts, ":")
}

// decodeMFATwoStepTicketPayload 解析 Redis 中缓存的 MFA 二次票据内容。
func decodeMFATwoStepTicketPayload(raw string) (*mfaTwoStepTicketPayload, error) {
	parts := strings.SplitN(strings.TrimSpace(raw), ":", 5)
	if len(parts) != 3 && len(parts) != 5 {
		return nil, errors.Errorf("MFA二次票据格式不正确")
	}
	scenario, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, errors.Wrap(err, "解析MFA二次票据场景失败")
	}
	expiresAt, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return nil, errors.Wrap(err, "解析MFA二次票据过期时间失败")
	}
	if expiresAt <= 0 {
		return nil, errors.Errorf("MFA二次票据过期时间不正确")
	}
	payload := &mfaTwoStepTicketPayload{
		Scenario:  scenario,
		Value:     parts[1],
		ExpiresAt: expiresAt,
	}
	if len(parts) == 5 {
		payload.SecretSource = strings.TrimSpace(parts[3])
		payload.SecretDigest = strings.TrimSpace(parts[4])
	}
	return payload, nil
}

// generateMFASecret 使用加密安全随机数生成 16 位 Base32 秘钥，前端当前输入限制。
func generateMFASecret() (string, error) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
	var builder strings.Builder
	builder.Grow(16)
	for i := 0; i < 16; i++ {
		index, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			return "", errors.Wrap(err, "生成MFA秘钥随机数失败")
		}
		builder.WriteByte(alphabet[index.Int64()])
	}
	return builder.String(), nil
}
