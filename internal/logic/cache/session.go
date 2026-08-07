package cache

import (
	keys "admin/common/rediskeys"
	corelogic "admin/internal/logic"
	"admin/internal/model"
	"admin/internal/svc"
	"admin/internal/types"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"github.com/Is999/go-utils/errors"
	"github.com/redis/go-redis/v9"
)

// CacheLogic 负责登录态与轻量级业务缓存的读写封装。
type CacheLogic struct {
	*corelogic.BaseLogic // 复用上下文和 Redis 操作能力
}

const (
	// adminRefreshTokenGraceSeconds 是刷新接口对上一枚 token 的幂等宽限窗口，普通业务接口不使用该窗口。
	adminRefreshTokenGraceSeconds = int64(30)
	// adminRefreshPreviousTokenDigestField 保存上一枚刷新 token 的 SHA-256 摘要，不在 Redis 中留存原 token。
	adminRefreshPreviousTokenDigestField = "refreshPreviousTokenDigest"
	// adminRefreshGraceUntilField 保存上一枚刷新 token 的宽限截止时间戳。
	adminRefreshGraceUntilField = "refreshGraceUntil"
)

// adminSessionMutableFields 是个人中心允许原子同步到当前会话的公开资料字段。
var adminSessionMutableFields = map[string]struct{}{
	"avatar":            {},
	"description":       {},
	"email":             {},
	"mfaStatus":         {},
	"needResetPassword": {},
	"phone":             {},
	"realName":          {},
}

// NewCacheLogic 为缓存相关逻辑绑定请求上下文，确保 Redis 操作日志能带上 trace_id。
func NewCacheLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CacheLogic {
	return &CacheLogic{
		BaseLogic: corelogic.NewBaseLogicWithContext(ctx, svcCtx),
	}
}

// adminSessionKey 统一管理员会话 key 格式，避免业务层散落拼接字符串。
func (l *CacheLogic) adminSessionKey(adminID int) string {
	if l == nil || l.BaseLogic == nil {
		return ""
	}
	return l.AppRedisKey(keys.AdminSessionLogicalKey(adminID))
}

// sessionRedis 返回管理员会话链路使用的 Redis 客户端；依赖或 app_id 命名空间缺失时直接失败。
func (l *CacheLogic) sessionRedis(operation string) (redis.UniversalClient, error) {
	if l == nil || l.BaseLogic == nil || l.Svc == nil || l.Svc.Rds == nil {
		return nil, WrapRedisUnavailable(nil, operation)
	}
	if l.adminSessionKey(1) == "" {
		return nil, WrapRedisUnavailable(nil, operation+"：app_id 缓存命名空间未初始化")
	}
	return l.Svc.Rds, nil
}

// sessionRedisError 保留缓存未命中语义，其它命令错误统一标记为 Redis 不可用。
func sessionRedisError(err error, operation string) error {
	if err == nil || errors.Is(err, redis.Nil) {
		return err
	}
	return WrapRedisUnavailable(err, operation)
}

// SetAdminSession 整体写入管理员会话，并设置统一过期时间。
func (l *CacheLogic) SetAdminSession(adminID int, session *types.AdminSession) error {
	if adminID <= 0 || session == nil {
		return errors.Errorf("管理员会话写入参数不完整")
	}
	redisClient, err := l.sessionRedis("写入管理员会话失败")
	if err != nil {
		return errors.Tag(err)
	}
	ctx := l.Ctx
	key := l.adminSessionKey(adminID)
	expiresIn := l.Svc.CurrentConfig().JwtExpiresIn
	if expiresIn <= 0 {
		// 支持测试与轻量 ServiceContext 场景：若未显式注入 JwtExpiresIn，则按默认 24 小时处理，避免缓存立即过期。
		expiresIn = 86400
	}
	// 登录资料、token、宽限字段清理和 TTL 必须在同 key MULTI/EXEC 中提交，避免并发登出穿插 HSET/EXPIRE。
	_, err = redisClient.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, key, session.ToMap())
		pipe.HDel(ctx, key, adminRefreshPreviousTokenDigestField, adminRefreshGraceUntilField)
		pipe.Expire(ctx, key, time.Duration(expiresIn)*time.Second)
		return nil
	})
	return sessionRedisError(err, "写入管理员会话失败")
}

// SetAdminSessionFields 原子更新已有会话的受控字段；字段不完整时删除异常会话，不重建已撤销会话。
func (l *CacheLogic) SetAdminSessionFields(adminID int, fields map[string]any) error {
	if adminID <= 0 || len(fields) == 0 {
		return errors.Errorf("管理员会话字段更新参数不完整")
	}
	redisClient, err := l.sessionRedis("更新管理员会话字段失败")
	if err != nil {
		return errors.Tag(err)
	}
	fieldNames := make([]string, 0, len(fields))
	for rawField := range fields {
		field := strings.TrimSpace(rawField)
		if field != rawField {
			return errors.Errorf("管理员会话字段格式不合法: %s", rawField)
		}
		if _, ok := adminSessionMutableFields[field]; !ok {
			return errors.Errorf("管理员会话字段不允许更新: %s", field)
		}
		fieldNames = append(fieldNames, field)
	}
	sort.Strings(fieldNames)
	args := make([]any, 0, len(fieldNames)*2)
	for _, field := range fieldNames {
		args = append(args, field, fields[field])
	}
	result, err := setAdminSessionFieldsScript.Run(l.Ctx, redisClient, []string{l.adminSessionKey(adminID)}, args...).Int64()
	if err != nil {
		return sessionRedisError(err, "更新管理员会话字段失败")
	}
	switch result {
	case -1, 0, 1:
		// 会话不存在或字段不完整时不重建；字段不完整的异常会话已由 Lua 原子删除。
		return nil
	default:
		return errors.Errorf("管理员会话字段更新返回未知状态: %d", result)
	}
}

// GetAdminSession 读取管理员会话，并反序列化成结构体。
func (l *CacheLogic) GetAdminSession(adminID int) (*types.AdminSession, error) {
	if adminID <= 0 {
		return nil, errors.Errorf("管理员ID不能为空")
	}
	redisClient, err := l.sessionRedis("读取管理员会话失败")
	if err != nil {
		return nil, errors.Tag(err)
	}
	if l.Svc.SecurityCacheSyncPending() {
		return nil, ErrSecurityCacheSyncPending
	}
	ctx := l.Ctx
	key := l.adminSessionKey(adminID)
	barrierKey := keys.SecurityCacheSyncBarrierRedisKey()
	if key == "" || barrierKey == "" {
		return nil, WrapRedisUnavailable(nil, "读取管理员会话失败：Redis key 未初始化")
	}
	var session types.AdminSession
	pipe := redisClient.Pipeline()
	sessionCmd := pipe.HGetAll(ctx, key)
	barrierCmd := pipe.Exists(ctx, barrierKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, WrapRedisUnavailable(err, "读取管理员会话失败")
	}
	barrier, err := barrierCmd.Result()
	if err != nil {
		return nil, sessionRedisError(err, "读取安全缓存同步状态失败")
	}
	if barrier > 0 {
		return nil, ErrSecurityCacheSyncPending
	}
	result, err := sessionCmd.Result()
	if err != nil {
		return nil, sessionRedisError(err, "读取管理员会话失败")
	}
	if len(result) == 0 {
		return nil, redis.Nil
	}
	err = session.FromMap(result)
	if err != nil {
		return nil, errors.Tag(err)
	}
	return &session, nil
}

// GetAdminToken 读取管理员会话中的访问令牌。
func (l *CacheLogic) GetAdminToken(adminID int) (string, error) {
	if adminID <= 0 {
		return "", errors.Errorf("管理员ID不能为空")
	}
	redisClient, err := l.sessionRedis("读取管理员会话 token 失败")
	if err != nil {
		return "", errors.Tag(err)
	}
	ctx := l.Ctx
	key := l.adminSessionKey(adminID)
	token, err := redisClient.HGet(ctx, key, "token").Result()
	return token, sessionRedisError(err, "读取管理员会话 token 失败")
}

// RotateAdminToken 原子轮换当前 token；同一旧 token 在短暂宽限期内重试时幂等返回已轮换 token。
// 返回空字符串表示会话不存在、已登出、已重新登录或请求 token 已超出宽限期。
func (l *CacheLogic) RotateAdminToken(adminID int, expectedToken string, newToken string) (string, error) {
	expectedToken = strings.TrimSpace(expectedToken)
	newToken = strings.TrimSpace(newToken)
	if adminID <= 0 || expectedToken == "" || newToken == "" {
		return "", errors.Errorf("管理员会话 token 轮换参数不完整")
	}
	redisClient, err := l.sessionRedis("轮换管理员会话 token 失败")
	if err != nil {
		return "", errors.Tag(err)
	}
	expiresIn := l.Svc.CurrentConfig().JwtExpiresIn
	if expiresIn <= 0 {
		expiresIn = 86400
	}
	result, err := rotateAdminTokenScript.Run(
		l.Ctx,
		redisClient,
		[]string{l.adminSessionKey(adminID)},
		expectedToken,
		newToken,
		adminTokenDigest(expectedToken),
		adminRefreshTokenGraceSeconds,
		expiresIn,
	).Text()
	if err != nil {
		return "", sessionRedisError(err, "轮换管理员会话 token 失败")
	}
	return strings.TrimSpace(result), nil
}

// CanUseAdminSessionToken 判断 token 是否为当前 token，或仍处于会话刷新宽限期。
// 该能力只允许鉴权中间件用于 auth.refresh 和 auth.logout，普通业务路由仍只接受当前 token。
func (l *CacheLogic) CanUseAdminSessionToken(adminID int, token string) (bool, error) {
	token = strings.TrimSpace(token)
	if adminID <= 0 || token == "" {
		return false, nil
	}
	redisClient, err := l.sessionRedis("校验管理员会话 token 失败")
	if err != nil {
		return false, errors.Tag(err)
	}
	result, err := canUseAdminSessionTokenScript.Run(
		l.Ctx,
		redisClient,
		[]string{l.adminSessionKey(adminID)},
		token,
		adminTokenDigest(token),
	).Int64()
	if err != nil {
		return false, sessionRedisError(err, "校验管理员会话 token 失败")
	}
	return result == 1, nil
}

// adminTokenDigest 返回 token 的不可逆摘要，供短暂刷新宽限匹配使用。
func adminTokenDigest(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return hex.EncodeToString(sum[:])
}

// DeleteAdminSession 删除指定管理员的缓存会话。
func (l *CacheLogic) DeleteAdminSession(adminID int) error {
	if adminID <= 0 {
		return errors.Errorf("管理员ID不能为空")
	}
	redisClient, err := l.sessionRedis("删除管理员会话失败")
	if err != nil {
		return errors.Tag(err)
	}
	key := l.adminSessionKey(adminID)
	return sessionRedisError(redisClient.Del(l.Ctx, key).Err(), "删除管理员会话失败")
}

// DeleteAdminSessionForLogout 删除当前请求持有的会话。
// 刷新并发窗口内接受上一枚 token；重新登录会清空宽限字段，因此旧登录请求不能删除新会话。
func (l *CacheLogic) DeleteAdminSessionForLogout(adminID int, token string) (bool, error) {
	token = strings.TrimSpace(token)
	if adminID <= 0 || token == "" {
		return false, nil
	}
	redisClient, err := l.sessionRedis("退出管理员会话失败")
	if err != nil {
		return false, errors.Tag(err)
	}
	deleted, err := deleteAdminSessionForLogoutScript.Run(
		l.Ctx,
		redisClient,
		[]string{l.adminSessionKey(adminID)},
		token,
		adminTokenDigest(token),
	).Int64()
	if err != nil {
		return false, sessionRedisError(err, "退出管理员会话失败")
	}
	return deleted == 1, nil
}

// RebuildAdminSession 从主库回源管理员资料并重建会话缓存。
func (l *CacheLogic) RebuildAdminSession(adminID int, token string) (*types.AdminSession, error) {
	if adminID <= 0 {
		return nil, errors.Errorf("管理员ID不能为空")
	}
	if strings.TrimSpace(token) == "" {
		return nil, errors.Errorf("管理员会话Token不能为空")
	}
	if _, err := l.sessionRedis("重建管理员会话失败"); err != nil {
		return nil, errors.Tag(err)
	}
	var admin model.Admin
	if err := l.Svc.WriteDB(svc.DatabaseMain).Where("id = ?", adminID).First(&admin).Error; err != nil {
		return nil, errors.Tag(err)
	}
	session := BuildAdminSession(&admin, token)
	if err := l.SetAdminSession(adminID, session); err != nil {
		return nil, errors.Tag(err)
	}
	return session, nil
}

// RebuildAdminSessionByKey 根据管理员会话键重建缓存；仅当原缓存仍携带 token 时允许重建。
func (l *CacheLogic) RebuildAdminSessionByKey(key string) error {
	key = strings.TrimSpace(key)
	adminID, ok := keys.AdminSessionIDFromRedisKey(key)
	if !ok {
		return errors.Errorf("管理员会话缓存key不合法: %s", key)
	}
	token, err := l.GetAdminToken(adminID)
	if err != nil {
		if err == redis.Nil {
			return errors.Errorf("管理员会话缓存不存在，无法重建: %s", key)
		}
		return errors.Tag(err)
	}
	_, err = l.RebuildAdminSession(adminID, token)
	return errors.Tag(err)
}

// BuildAdminSession 把管理员模型和当前 token 转换成会话缓存结构。
func BuildAdminSession(admin *model.Admin, token string) *types.AdminSession {
	if admin == nil {
		return &types.AdminSession{Token: token}
	}
	return &types.AdminSession{
		ID:                admin.ID,
		UserName:          admin.Name,
		RealName:          admin.RealName,
		NeedResetPassword: admin.NeedResetPassword,
		Email:             admin.Email,
		Phone:             admin.Phone,
		MfaStatus:         admin.MfaStatus,
		Status:            admin.Status,
		Avatar:            admin.Avatar,
		Description:       admin.Description,
		LastLoginTime:     corelogic.FormatOptionalDateTime(admin.LastLoginTime),
		LastLoginIP:       admin.LastLoginIP,
		LastLoginIPAddr:   admin.LastLoginIPAddr,
		Token:             token,
	}
}
