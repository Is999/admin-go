package security

import (
	corelogic "admin/internal/logic"
	cachelogic "admin/internal/logic/cache"
	configlogic "admin/internal/logic/config"
	rbaclogic "admin/internal/logic/rbac"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"admin/common/codes"
	i18n "admin/common/i18n"
	"admin/helper"

	utils "github.com/Is999/go-utils"
	"github.com/Is999/go-utils/errors"

	keys "admin/common/rediskeys"
	"admin/internal/model"
	"admin/internal/requestctx"
	"admin/internal/routealias"
	"admin/internal/svc"
	"admin/internal/types"

	tablecache "github.com/Is999/table-cache"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const (
	// ConfigAdminIPWhitelistEnabled 表示是否启用后台 IP 白名单。
	ConfigAdminIPWhitelistEnabled = "adminIpWhitelistEnabled"
	// ConfigAdminIPWhitelist 表示后台 IP 白名单配置。
	ConfigAdminIPWhitelist = "adminIpWhitelist"
	// ConfigAdminCheckChangeIP 表示是否校验后台登录 IP 变更。
	ConfigAdminCheckChangeIP = "adminCheckChangeIp"
	// ConfigAdminMFACheckEnable 表示是否强制启用后台 MFA 校验。
	ConfigAdminMFACheckEnable = "adminMFACheckEnable"
	// ConfigAdminMFACheckFrequency 表示后台 MFA 校验频率，单位秒。
	ConfigAdminMFACheckFrequency = "adminMFACheckFrequency"
	// ConfigAdminDisableMFACheckScenario 表示后台禁用 MFA 校验的场景配置。
	ConfigAdminDisableMFACheckScenario = "adminDisableMFACheckScenario"
)

const (
	// routePermissionBypassAlias 必须与 middleware.Ignore 保持一致，用于已登录但不绑定业务权限码的通用接口。
	routePermissionBypassAlias = routealias.Ignore
	// adminAccessMFASecretUnknown 表示登录态缓存不保存 MFA 秘钥，只有未完成登录 MFA 时才按需回源判断。
	adminAccessMFASecretUnknown = "__cache_unknown__"
	// securityOptionalConfigTimeout 表示安全链路读取“可回退”的系统配置时使用的独立短超时。
	// 登录、鉴权等主链路不应因为这类兜底配置读取抖动而整体超时。
	securityOptionalConfigTimeout = 500 * time.Millisecond
)

var (
	// ErrAdminNotFound 表示管理员不存在。
	ErrAdminNotFound = errors.New("管理员不存在")
	// ErrAdminDisabled 表示管理员账号已被禁用。
	ErrAdminDisabled = errors.New("管理员账号已被禁用")
	// ErrAdminIPChanged 表示当前请求 IP 与 token 登录 IP 不一致。
	ErrAdminIPChanged = errors.New("管理员登录IP已变更")
	// ErrAdminIPNotAllowed 表示当前请求 IP 不在后台白名单。
	ErrAdminIPNotAllowed = errors.New("当前IP不在后台白名单")
	// ErrAdminPermissionDenied 表示当前管理员没有路由访问权限。
	ErrAdminPermissionDenied = errors.New("管理员权限不足")
	// ErrAdminPasswordResetRequired 表示当前管理员必须先修改登录密码。
	ErrAdminPasswordResetRequired = errors.New("需要先修改登录密码")
	// ErrAdminMFARequired 表示当前管理员需要先完成 MFA 登录校验。
	ErrAdminMFARequired = errors.New("需要完成MFA验证")
	// ErrAdminMFABindRequired 表示当前管理员必须先完成 MFA 绑定与启用。
	ErrAdminMFABindRequired = errors.New("需要先绑定并启用MFA")
	// ErrAdminMFACodeInvalid 表示当前提交的 MFA 动态验证码不正确。
	ErrAdminMFACodeInvalid = errors.New("MFA动态验证码错误")
	// ErrAdminMFATwoStepExpired 表示当前二次校验票据已过期或无效。
	ErrAdminMFATwoStepExpired = errors.New("MFA校验已过期")
)

// SecurityLogic 承载鉴权链路中的账号状态、IP 白名单、MFA 与权限校验逻辑。
type SecurityLogic struct {
	*corelogic.BaseLogic // 复用上下文、数据库、Redis 和日志能力
}

// NewSecurityLogic 创建后台安全校验逻辑对象。
func NewSecurityLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SecurityLogic {
	return &SecurityLogic{
		BaseLogic: corelogic.NewBaseLogicWithContext(ctx, svcCtx),
	}
}

// CheckAdminAccess 复用已校验会话，按账号状态、IP、MFA 与业务权限顺序完成访问校验。
func (l *SecurityLogic) CheckAdminAccess(session *types.AdminSession, routeAlias string, currentIP string, loginIP string) error {
	routeAlias = strings.TrimSpace(routeAlias)
	if routeAlias == "" {
		return errors.Tag(ErrAdminPermissionDenied)
	}
	admin, err := adminForAccess(session)
	if err != nil {
		return errors.Tag(err)
	}
	if admin.Status != 1 {
		return ErrAdminDisabled
	}
	if err := l.checkAdminIP(currentIP, loginIP); err != nil {
		return errors.Tag(err)
	}
	if err := l.checkAdminNeedResetPassword(admin, routeAlias); err != nil {
		return errors.Tag(err)
	}
	if !shouldBypassLoginMFACheck(admin, routeAlias) {
		if err := l.checkAdminMFA(admin); err != nil {
			return errors.Tag(err)
		}
	}
	allowed, err := l.CheckRoutePermission(admin.ID, routeAlias)
	if err != nil {
		return errors.Tag(err)
	}
	if !allowed {
		return ErrAdminPermissionDenied
	}
	return nil
}

// CheckRoutePermission 使用规范化角色关系缓存校验路由权限。
func (l *SecurityLogic) CheckRoutePermission(userID int, routeAlias string) (bool, error) {
	routeAlias = strings.TrimSpace(routeAlias)
	alias := routeAliasKey(routeAlias)
	if alias == routePermissionBypassAlias {
		// 显式挂 middleware.Ignore 的路由只跳过业务权限表；JWT、账号状态、IP 与 MFA 仍在 CheckAdminAccess 前置校验。
		return true, nil
	}
	if permissionAllowlist[alias] {
		return true, nil
	}
	if userID <= 0 || routeAlias == "" {
		return false, nil
	}
	if l.Redis() == nil {
		return false, cachelogic.WrapRedisUnavailable(nil, "校验管理员路由权限失败")
	}
	roleIDs, err := l.EnabledRoleIDs(userID)
	if err != nil {
		return false, errors.Tag(err)
	}
	if containsSuperRole(roleIDs) {
		return true, nil
	}
	permissionIDs, err := l.routePermissionIDsWithCache(routeAlias)
	if err != nil {
		return false, errors.Tag(err)
	}
	return l.rolePermissionSetsContain(roleIDs, keys.RolePermission, cachelogic.TableCacheIndexRolePermission, permissionIDs)
}

// CheckDocPermission 使用规范化角色关系缓存校验单篇文档权限。
func (l *SecurityLogic) CheckDocPermission(userID int, resource routealias.DocResource) (bool, error) {
	resourceKey := resource.Key()
	if userID <= 0 || resourceKey == "" {
		return false, nil
	}
	if l.Redis() == nil {
		return false, cachelogic.WrapRedisUnavailable(nil, "校验管理员文档权限失败")
	}
	roleIDs, err := l.EnabledRoleIDs(userID)
	if err != nil {
		return false, errors.Tag(err)
	}
	if containsSuperRole(roleIDs) {
		return true, nil
	}
	permissionID, err := l.docResourcePermissionIDWithCache(resourceKey)
	if err != nil {
		return false, errors.Tag(err)
	}
	return l.rolePermissionSetsContain(
		roleIDs,
		keys.RoleDocPermission,
		cachelogic.TableCacheIndexRoleDocPermission,
		[]int{permissionID},
	)
}

// AllowedDocResources 通过角色文档关系缓存计算账号可见资源。
func (l *SecurityLogic) AllowedDocResources(userID int) ([]routealias.DocResource, error) {
	if userID <= 0 {
		return []routealias.DocResource{}, nil
	}
	if l.Redis() == nil {
		return nil, cachelogic.WrapRedisUnavailable(nil, "读取管理员可见文档失败")
	}
	roleIDs, err := l.EnabledRoleIDs(userID)
	if err != nil {
		return nil, errors.Tag(err)
	}
	permissions, err := l.docPermissionsWithCache()
	if err != nil {
		return nil, errors.Tag(err)
	}
	isSuper := containsSuperRole(roleIDs)
	allowedPermissionIDs := make(map[int]struct{})
	if !isSuper {
		permissionIDs, err := l.roleRelationPermissionIDsWithCache(roleIDs, keys.RoleDocPermission, "角色文档权限关系")
		if err != nil {
			return nil, errors.Tag(err)
		}
		allowedPermissionIDs = make(map[int]struct{}, len(permissionIDs))
		for _, permissionID := range permissionIDs {
			allowedPermissionIDs[permissionID] = struct{}{}
		}
	}
	resources := make([]routealias.DocResource, 0, len(permissions))
	for _, permission := range permissions {
		if permission.Status != 1 {
			continue
		}
		if !isSuper {
			if _, ok := allowedPermissionIDs[permission.ID]; !ok {
				continue
			}
		}
		resource := routealias.DocResource{Site: permission.Site, Path: permission.Path}
		if resource.Key() == "" {
			return nil, errors.Errorf("文档权限缓存资源不合法")
		}
		resources = append(resources, resource)
	}
	return resources, nil
}

// containsSuperRole 判断启用角色集合是否包含超级管理员角色。
func containsSuperRole(roleIDs []int) bool {
	return slices.Contains(roleIDs, corelogic.AdminSuperRoleID)
}

// routePermissionIDsWithCache 根据路由别名读取启用权限 ID。
func (l *SecurityLogic) routePermissionIDsWithCache(routeAlias string) ([]int, error) {
	cache, err := cachelogic.StringHashFieldsWithCache(l.BaseLogic, keys.RoutePermissionIDs, []string{routeAlias})
	if err != nil {
		return nil, errors.Tag(err)
	}
	value := strings.TrimSpace(cache[routeAlias])
	if value == "" {
		return []int{}, nil
	}
	return cachelogic.ParsePositiveIntStrings(strings.Split(value, ","), "路由权限索引")
}

// docResourcePermissionIDWithCache 根据文档资源键读取启用文档权限 ID。
func (l *SecurityLogic) docResourcePermissionIDWithCache(resourceKey string) (int, error) {
	cache, err := cachelogic.StringHashFieldsWithCache(l.BaseLogic, keys.DocResourcePermissionID, []string{resourceKey})
	if err != nil {
		return 0, errors.Tag(err)
	}
	value := strings.TrimSpace(cache[resourceKey])
	if value == "" {
		return 0, nil
	}
	permissionID, err := strconv.Atoi(value)
	if err != nil {
		return 0, errors.Wrapf(err, "解析文档权限索引失败 value=%s", value)
	}
	if permissionID <= 0 {
		return 0, errors.Errorf("文档权限索引不是正整数 value=%s", value)
	}
	return permissionID, nil
}

// rolePermissionSetsContain 判断任一启用角色是否拥有目标权限 ID。
func (l *SecurityLogic) rolePermissionSetsContain(roleIDs []int, keyFormat string, cacheIndex string, permissionIDs []int) (bool, error) {
	roleIDs = types.UniquePositiveInts(roleIDs)
	permissionIDs = types.UniquePositiveInts(permissionIDs)
	if len(roleIDs) == 0 || len(permissionIDs) == 0 {
		return false, nil
	}
	if l.Redis() == nil {
		return false, cachelogic.WrapRedisUnavailable(nil, "读取角色权限关系失败")
	}
	members := make([]any, 0, len(permissionIDs))
	for _, permissionID := range permissionIDs {
		members = append(members, permissionID)
	}
	type setLookup struct {
		cacheKey string              // table-cache 逻辑 Key
		matches  *redis.BoolSliceCmd // 目标权限成员判断结果
		exists   *redis.IntCmd       // 物理集合是否存在
	}
	lookups := make([]setLookup, 0, len(roleIDs))
	pipe := l.Redis().Pipeline()
	for _, roleID := range roleIDs {
		cacheKey := fmt.Sprintf(keyFormat, roleID)
		physicalKey := cachelogic.TableCachePhysicalKey(l.BaseLogic, cacheKey)
		if physicalKey == "" {
			return false, cachelogic.WrapRedisUnavailable(nil, "角色权限关系缓存 Key 未初始化")
		}
		lookups = append(lookups, setLookup{
			cacheKey: cacheKey,
			matches:  pipe.SMIsMember(l.Ctx, physicalKey, members...),
			exists:   pipe.Exists(l.Ctx, physicalKey),
		})
	}
	if _, err := pipe.Exec(l.Ctx); err != nil {
		return false, cachelogic.WrapRedisUnavailable(err, "批量读取角色权限关系失败")
	}
	missingKeys := make([]string, 0)
	for _, lookup := range lookups {
		matches, err := lookup.matches.Result()
		if err != nil {
			return false, cachelogic.WrapRedisUnavailable(err, "解析角色权限匹配结果失败")
		}
		if slices.Contains(matches, true) {
			cachelogic.RecordTableCacheHit(l.BaseLogic, cacheIndex)
			return true, nil
		}
		exists, err := lookup.exists.Result()
		if err != nil {
			return false, cachelogic.WrapRedisUnavailable(err, "解析角色权限缓存状态失败")
		}
		if exists == 0 {
			missingKeys = append(missingKeys, lookup.cacheKey)
		}
	}
	if len(missingKeys) == 0 {
		cachelogic.RecordTableCacheHit(l.BaseLogic, cacheIndex)
		return false, nil
	}
	manager, err := cachelogic.TableCacheManager(l.BaseLogic)
	if err != nil {
		return false, errors.Tag(err)
	}
	permissionSet := make(map[int]struct{}, len(permissionIDs))
	for _, permissionID := range permissionIDs {
		permissionSet[permissionID] = struct{}{}
	}
	for _, cacheKey := range missingKeys {
		var values []string
		result, err := manager.LoadThrough(l.Ctx, cachelogic.TableCachePhysicalKey(l.BaseLogic, cacheKey), &values, nil)
		if err != nil {
			return false, errors.Tag(err)
		}
		if result.State == tablecache.LookupStateEmpty {
			continue
		}
		cachedPermissionIDs, err := cachelogic.ParsePositiveIntStrings(values, "角色权限关系缓存")
		if err != nil {
			return false, errors.Tag(err)
		}
		for _, permissionID := range cachedPermissionIDs {
			if _, ok := permissionSet[permissionID]; ok {
				return true, nil
			}
		}
	}
	return false, nil
}

// roleRelationPermissionIDsWithCache 合并少量角色的原始权限关系 ID。
func (l *SecurityLogic) roleRelationPermissionIDsWithCache(roleIDs []int, keyFormat string, title string) ([]int, error) {
	roleIDs = types.UniquePositiveInts(roleIDs)
	if len(roleIDs) == 0 {
		return []int{}, nil
	}
	manager, err := cachelogic.TableCacheManager(l.BaseLogic)
	if err != nil {
		return nil, errors.Tag(err)
	}
	permissionIDs := make([]int, 0)
	for _, roleID := range roleIDs {
		physicalKey := cachelogic.TableCachePhysicalKey(l.BaseLogic, fmt.Sprintf(keyFormat, roleID))
		var values []string
		result, err := manager.LoadThrough(l.Ctx, physicalKey, &values, nil)
		if err != nil {
			return nil, errors.Tag(err)
		}
		if result.State == tablecache.LookupStateEmpty {
			continue
		}
		rolePermissionIDs, err := cachelogic.ParsePositiveIntStrings(values, title)
		if err != nil {
			return nil, errors.Tag(err)
		}
		permissionIDs = append(permissionIDs, rolePermissionIDs...)
	}
	return types.UniquePositiveInts(permissionIDs), nil
}

// docPermissionsWithCache 读取完整文档权限快照，供导航按角色关系投影。
func (l *SecurityLogic) docPermissionsWithCache() ([]model.AdminDocPermission, error) {
	manager, err := cachelogic.TableCacheManager(l.BaseLogic)
	if err != nil {
		return nil, errors.Tag(err)
	}
	var permissions []model.AdminDocPermission
	result, err := manager.LoadThrough(
		l.Ctx,
		cachelogic.TableCachePhysicalKey(l.BaseLogic, keys.DocPermissionList),
		&permissions,
		nil,
	)
	if err != nil {
		return nil, errors.Tag(err)
	}
	if result.State == tablecache.LookupStateEmpty {
		return []model.AdminDocPermission{}, nil
	}
	return permissions, nil
}

// SecurityConfigBool 读取可选布尔型系统配置，读取失败时使用调用方给出的默认值。
func SecurityConfigBool(ctx context.Context, svcCtx *svc.ServiceContext, uuid string, defaultValue bool) bool {
	result, err := securityConfigBool(ctx, svcCtx, uuid)
	if err != nil {
		return defaultValue
	}
	return result
}

// securityConfigBool 读取安全链路布尔配置，缺失、类型错误或依赖异常时返回错误。
func securityConfigBool(ctx context.Context, svcCtx *svc.ServiceContext, uuid string) (bool, error) {
	if svcCtx == nil {
		return false, errors.Errorf("安全配置服务上下文未初始化")
	}
	configCtx, cancel := optionalSecurityConfigContext(ctx)
	defer cancel()
	value, err := configlogic.NewSysConfigLogicWithContext(configCtx, svcCtx).GetCachedValue(uuid)
	if err != nil {
		return false, errors.Wrapf(err, "读取安全配置[%s]失败", uuid)
	}
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		value := strings.TrimSpace(v)
		if value == "1" || strings.EqualFold(value, "true") {
			return true, nil
		}
		if value == "0" || strings.EqualFold(value, "false") {
			return false, nil
		}
	case int:
		if v == 0 || v == 1 {
			return v == 1, nil
		}
	case float64:
		if v == 0 || v == 1 {
			return v == 1, nil
		}
	}
	return false, errors.Errorf("安全配置[%s]不是合法布尔值", uuid)
}

// SecurityConfigStringSlice 读取字符串数组配置，读取失败时使用调用方给出的默认值。
func SecurityConfigStringSlice(ctx context.Context, svcCtx *svc.ServiceContext, uuid string, defaultValue []string) []string {
	result, err := securityConfigStringSlice(ctx, svcCtx, uuid)
	if err != nil {
		return defaultValue
	}
	return result
}

// securityConfigStringSlice 读取安全链路字符串数组配置，缺失、类型错误或依赖异常时返回错误。
func securityConfigStringSlice(ctx context.Context, svcCtx *svc.ServiceContext, uuid string) ([]string, error) {
	if svcCtx == nil {
		return nil, errors.Errorf("安全配置服务上下文未初始化")
	}
	configCtx, cancel := optionalSecurityConfigContext(ctx)
	defer cancel()
	value, err := configlogic.NewSysConfigLogicWithContext(configCtx, svcCtx).GetCachedValue(uuid)
	if err != nil {
		return nil, errors.Wrapf(err, "读取安全配置[%s]失败", uuid)
	}
	switch v := value.(type) {
	case []string:
		return helper.UniqueNonEmptyStrings(v), nil
	case []any:
		result := make([]string, 0, len(v))
		for _, item := range v {
			text := strings.TrimSpace(fmt.Sprint(item))
			if text != "" {
				result = append(result, text)
			}
		}
		return helper.UniqueNonEmptyStrings(result), nil
	case map[string]any:
		result := make([]string, 0, len(v))
		for key := range v {
			if strings.TrimSpace(key) != "" {
				result = append(result, strings.TrimSpace(key))
			}
		}
		return helper.UniqueNonEmptyStrings(result), nil
	case string:
		if strings.TrimSpace(v) == "" {
			return []string{}, nil
		}
		parts := strings.Split(v, ",")
		result := make([]string, 0, len(parts))
		for _, item := range parts {
			text := strings.TrimSpace(item)
			if text != "" {
				result = append(result, text)
			}
		}
		return helper.UniqueNonEmptyStrings(result), nil
	}
	return nil, errors.Errorf("安全配置[%s]不是合法字符串数组", uuid)
}

// optionalSecurityConfigContext 为安全链路中的“可失败可回退”配置读取创建独立短超时上下文。
// 这里故意不复用请求上下文的 deadline，避免登录成功后仅因配置缓存慢读而整条请求超时。
func optionalSecurityConfigContext(context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), securityOptionalConfigTimeout)
}

// adminForAccess 把已校验会话转换为安全校验所需的最小管理员模型。
func adminForAccess(session *types.AdminSession) (*model.Admin, error) {
	if session == nil || session.ID <= 0 {
		return nil, ErrAdminNotFound
	}
	var lastLoginTime *time.Time
	if value := strings.TrimSpace(session.LastLoginTime); value != "" {
		parsed, err := time.ParseInLocation(time.DateTime, value, time.Local)
		if err != nil {
			return nil, errors.Wrap(err, "解析管理员登录态最后登录时间失败")
		}
		lastLoginTime = &parsed
	}
	return &model.Admin{
		ID:                session.ID,
		Name:              session.UserName,
		Status:            session.Status,
		MfaStatus:         session.MfaStatus,
		MfaSecureKey:      adminAccessMFASecretUnknown,
		NeedResetPassword: session.NeedResetPassword,
		LastLoginTime:     lastLoginTime,
	}, nil
}

// checkAdminIP 校验 token 登录 IP、当前请求 IP 与白名单配置。
func (l *SecurityLogic) checkAdminIP(currentIP string, loginIP string) error {
	whitelistEnabled, err := securityConfigBool(l.Ctx, l.Svc, ConfigAdminIPWhitelistEnabled)
	if err != nil {
		return errors.Tag(err)
	}
	currentIP = strings.TrimSpace(currentIP)
	loginIP = strings.TrimSpace(loginIP)
	ipChanged := loginIP != "" && currentIP != "" && loginIP != currentIP
	if ipChanged {
		checkChangeIP, err := securityConfigBool(l.Ctx, l.Svc, ConfigAdminCheckChangeIP)
		if err != nil {
			return errors.Tag(err)
		}
		if checkChangeIP {
			return ErrAdminIPChanged
		}
	}
	if whitelistEnabled {
		whitelist, err := securityConfigStringSlice(l.Ctx, l.Svc, ConfigAdminIPWhitelist)
		if err != nil {
			return errors.Tag(err)
		}
		if currentIP == "" || !utils.Contains(currentIP, whitelist) {
			return ErrAdminIPNotAllowed
		}
	}
	return nil
}

// CheckAdminLoginIP 在签发登录 token 前校验当前来源 IP 与后台白名单。
func (l *SecurityLogic) CheckAdminLoginIP(currentIP string) error {
	return l.checkAdminIP(currentIP, "")
}

// ForceLoginMFAEnabled 判断系统是否开启了登录阶段强制 MFA；配置不可用时返回错误并阻断敏感操作。
func (l *SecurityLogic) ForceLoginMFAEnabled() (bool, error) {
	return securityConfigBool(l.Ctx, l.Svc, ConfigAdminMFACheckEnable)
}

// NeedLoginMFA 判断当前管理员登录态是否必须完成 MFA 校验。
func (l *SecurityLogic) NeedLoginMFA(admin *model.Admin) (bool, error) {
	if admin == nil {
		return false, nil
	}
	// 首次登录或重置密码后的临时密码阶段，优先放行改密流程，MFA 允许后续再补。
	if admin.NeedResetPassword == 1 {
		return false, nil
	}
	if admin.MfaStatus == 1 {
		return true, nil
	}
	return l.ForceLoginMFAEnabled()
}

// NeedBindMFAOnLogin 判断当前管理员在登录阶段是否需要先完成 MFA 绑定并启用。
// 账号状态已启用但秘钥不可用时，同样走登录绑定流程，避免账号被卡死在不可登录状态。
func (l *SecurityLogic) NeedBindMFAOnLogin(admin *model.Admin) (bool, error) {
	if admin == nil || admin.NeedResetPassword == 1 {
		return false, nil
	}
	needMFA, err := l.NeedLoginMFA(admin)
	if err != nil {
		return false, errors.Tag(err)
	}
	return needMFA && (admin.MfaStatus != 1 || !l.HasUsableAdminMFASecret(admin)), nil
}

// checkAdminMFA 校验登录阶段是否已经完成 MFA；正常请求命中完成标记时不读取管理员表。
func (l *SecurityLogic) checkAdminMFA(admin *model.Admin) error {
	if admin == nil {
		return ErrAdminNotFound
	}
	needMFA, err := l.NeedLoginMFA(admin)
	if err != nil {
		return errors.Tag(err)
	}
	if !needMFA {
		return nil
	}
	if l.Redis() == nil {
		return cachelogic.WrapRedisUnavailable(nil, "校验管理员登录MFA失败")
	}
	cacheKey := l.loginMFAFlagKey(admin.ID)
	if cacheKey == "" {
		return cachelogic.WrapRedisUnavailable(nil, "校验管理员登录MFA失败：缓存 Key 未初始化")
	}
	flag, err := l.Redis().Get(l.Ctx, cacheKey).Int64()
	if err == nil && loginMFAFlagMatches(flag, admin.LastLoginTime) {
		return nil
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		return cachelogic.WrapRedisUnavailable(err, "读取管理员登录MFA状态失败")
	}
	if admin.MfaStatus != 1 {
		return ErrAdminMFABindRequired
	}
	if admin.MfaSecureKey == adminAccessMFASecretUnknown {
		var credential struct {
			MfaSecureKey string `gorm:"column:mfa_secure_key"` // MFA 加密秘钥
		}
		if err := l.Svc.WriteDB(svc.DatabaseMain).
			Model(&model.Admin{}).
			Select("mfa_secure_key").
			Where("id = ?", admin.ID).
			Take(&credential).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrAdminNotFound
			}
			return errors.Tag(err)
		}
		admin.MfaSecureKey = credential.MfaSecureKey
	}
	if !l.HasUsableAdminMFASecret(admin) {
		return ErrAdminMFABindRequired
	}
	return ErrAdminMFARequired
}

// EnabledRoleIDs 查询用户拥有的启用角色 ID。
func (l *SecurityLogic) EnabledRoleIDs(userID int) ([]int, error) {
	if roleIDs, ok := requestctx.EnabledRoleIDs(l.Ctx); ok {
		return roleIDs, nil
	}
	roleIDs, err := (&rbaclogic.AdminRoleLogic{BaseLogic: l.BaseLogic}).EnabledRoleIDsByUserWithCache(userID)
	if err != nil {
		return nil, errors.Tag(err)
	}
	requestctx.SetEnabledRoleIDs(l.Ctx, roleIDs)
	return roleIDs, nil
}

// UserPermissionUUIDsWithCache 通过规范化角色关系缓存计算管理员权限码。
func (l *SecurityLogic) UserPermissionUUIDsWithCache(userID int) ([]string, error) {
	if userID <= 0 {
		return []string{}, nil
	}
	if l.Redis() == nil {
		return nil, cachelogic.WrapRedisUnavailable(nil, "读取管理员权限码失败")
	}
	roleIDs, err := l.EnabledRoleIDs(userID)
	if err != nil {
		return nil, errors.Tag(err)
	}
	permissionLogic := &rbaclogic.AdminPermissionLogic{BaseLogic: l.BaseLogic}
	if containsSuperRole(roleIDs) {
		return permissionLogic.AllEnabledPermissionUUIDsWithCache()
	}
	permissionIDs, err := l.roleRelationPermissionIDsWithCache(roleIDs, keys.RolePermission, "角色路由权限关系")
	if err != nil {
		return nil, errors.Tag(err)
	}
	return permissionLogic.PermissionUUIDsByIDsWithCache(permissionIDs)
}

// OperateMFABizResult 把后台敏感操作中的 MFA 错误转换成统一业务响应。
func OperateMFABizResult(err error, operation string) *types.BizResult {
	if err == nil {
		return nil
	}
	operation = strings.TrimSpace(operation)
	if operation == "" {
		operation = "SecurityLogic.OperateMFABizResult"
	}
	if errors.Is(err, cachelogic.ErrRedisUnavailable) {
		return types.NewBizResult(codes.RedisUnavailable).
			SetI18nMessage(i18n.MsgKeyRedisUnavailable).
			WithError(corelogic.WrapLogicError(err, "%s Redis不可用", operation))
	}
	if errors.Is(err, ErrAdminMFATwoStepExpired) || errors.Is(err, ErrAdminMFARequired) {
		return types.NewBizResult(codes.CheckMFAAgain).
			SetI18nMessage(i18n.MsgKeyMFAExpired).
			WithError(corelogic.WrapLogicError(err, "%s MFA二次校验已失效", operation))
	}
	if errors.Is(err, types.Nil) {
		return types.NewBizResult(codes.Unauthorized).
			SetI18nMessage(i18n.MsgKeyNeedLogin).
			WithError(corelogic.WrapLogicError(err, "%s 当前请求未登录", operation))
	}
	return types.NewBizResult(codes.DBError).
		SetI18nMessage(i18n.MsgKeyDBError).
		WithError(corelogic.WrapLogicError(err, "%s MFA校验失败", operation))
}

// permissionAllowlist 显式列出只依赖登录态和账号状态的个人/会话接口。
// 这些接口不走权限表，但仍会经过 token、账号状态、IP 与 MFA 校验，不属于未配置权限的默认放行。
var permissionAllowlist = map[routealias.Alias]bool{
	routealias.AuthRefresh:            true, // 刷新访问令牌不要求后台权限码。
	routealias.AuthLogout:             true, // 管理员退出登录不要求后台权限码。
	routealias.AuthCodes:              true, // 获取当前用户权限码不要求后台权限码。
	routealias.AuthProfile:            true, // 获取当前登录资料不要求后台权限码。
	routealias.RoleTreeOptions:        true, // 查询角色树下拉不要求后台权限码。
	routealias.ProfileMine:            true, // 获取当前管理员资料不要求后台权限码。
	routealias.ProfileCheckSecure:     true, // 校验当前管理员密码不要求后台权限码。
	routealias.ProfileCheckMFA:        true, // 校验当前管理员MFA动态码不要求后台权限码。
	routealias.ProfileUpdatePassword:  true, // 个人中心修改密码不要求后台权限码。
	routealias.ProfileUpdateMine:      true, // 个人中心修改资料不要求后台权限码。
	routealias.ProfileUpdateMFAStatus: true, // 个人中心修改MFA状态不要求后台权限码。
	routealias.ProfileUpdateMFASecret: true, // 个人中心修改MFA秘钥不要求后台权限码。
	// 个人中心刷新 MFA 绑定秘钥属于当前登录账号的自助安全操作，
	// 只依赖登录态、账号状态和后续 MFA 二次校验，不额外绑定后台业务权限码。
	routealias.ProfileRefreshMFASecret: true, // 个人中心重新生成MFA秘钥不要求后台权限码。
	routealias.ProfileUpdateAvatar:     true, // 个人中心修改头像不要求后台权限码。
	// 权限 UUID 预览只生成候选值，不写权限表；新增/编辑权限仍由权限保存接口做权限控制。
	routealias.PermissionMaxUUID: true, // 查询下一个权限UUID不要求后台权限码。
	// 消息中心属于个人收件箱能力，仅依赖登录态与账号安全校验，不绑定后台权限码。
	routealias.AdminMessageList:            true, // 查询管理员消息收件箱不要求后台权限码。
	routealias.AdminMessageSentList:        true, // 查询管理员已发送消息不要求后台权限码。
	routealias.AdminMessageReceiverOptions: true, // 查询管理员消息可用收件人选项不要求后台权限码。
	routealias.AdminMessageReceivers:       true, // 查询管理员消息收件人明细不要求后台权限码。
	routealias.AdminMessageUnreadCount:     true, // 查询管理员未读消息数量不要求后台权限码。
	routealias.AdminMessageNotifications:   true, // 查询管理员通知列表不要求后台权限码。
	routealias.AdminMessageMarkRead:        true, // 标记管理员消息已读不要求后台权限码。
	routealias.AdminMessageDelete:          true, // 删除管理员消息不要求后台权限码。
	routealias.AdminMessageSend:            true, // 发送管理员消息不要求后台权限码。
	routealias.AdminMessageHandle:          true, // 标记管理员消息已处理不要求后台权限码。
}

// passwordResetAllowlist 显式列出“必须先修改密码”状态下仍允许访问的自助接口。
var passwordResetAllowlist = map[routealias.Alias]bool{
	routealias.AuthRefresh:               true, // 刷新访问令牌在强制改密阶段允许访问。
	routealias.AuthLogout:                true, // 管理员退出登录在强制改密阶段允许访问。
	routealias.AuthCodes:                 true, // 获取当前用户权限码在强制改密阶段允许访问。
	routealias.AuthProfile:               true, // 获取当前登录资料在强制改密阶段允许访问。
	routealias.ProfileMine:               true, // 获取当前管理员资料在强制改密阶段允许访问。
	routealias.ProfileCheckSecure:        true, // 校验当前管理员密码在强制改密阶段允许访问。
	routealias.ProfileCheckMFA:           true, // 校验当前管理员MFA动态码在强制改密阶段允许访问。
	routealias.ProfileUpdatePassword:     true, // 个人中心修改密码在强制改密阶段允许访问。
	routealias.ProfileUpdateMine:         true, // 个人中心修改资料在强制改密阶段允许访问。
	routealias.ProfileUpdateMFAStatus:    true, // 个人中心修改MFA状态在强制改密阶段允许访问。
	routealias.ProfileUpdateMFASecret:    true, // 个人中心修改MFA秘钥在强制改密阶段允许访问。
	routealias.ProfileRefreshMFASecret:   true, // 个人中心重新生成MFA秘钥在强制改密阶段允许访问。
	routealias.ProfileUpdateAvatar:       true, // 个人中心修改头像在强制改密阶段允许访问。
	routealias.AdminMessageNotifications: true, // 查询管理员通知列表在强制改密阶段允许访问。
}

// loginMFAAllowlist 显式列出“正在完成登录 MFA 校验”时允许访问的会话接口。
// 这里必须至少放行 MFA 动态码校验接口本身，避免“校验 MFA 的接口自己又被要求先完成 MFA”形成递归拦截。
var loginMFAAllowlist = map[routealias.Alias]bool{
	routealias.AuthRefresh:               true, // 刷新访问令牌在登录 MFA 阶段允许访问。
	routealias.AuthLogout:                true, // 管理员退出登录在登录 MFA 阶段允许访问。
	routealias.AuthCodes:                 true, // 获取当前用户权限码在登录 MFA 阶段允许访问。
	routealias.ProfileCheckMFA:           true, // 校验当前管理员MFA动态码在登录 MFA 阶段允许访问。
	routealias.ProfileRefreshMFASecret:   true, // 个人中心重新生成MFA秘钥在登录 MFA 阶段允许访问。
	routealias.ProfileUpdateMFAStatus:    true, // 个人中心修改MFA状态在登录 MFA 阶段允许访问。
	routealias.ProfileMine:               true, // 获取当前管理员资料在登录 MFA 阶段允许访问。
	routealias.AdminMessageNotifications: true, // 查询管理员通知列表在登录 MFA 阶段允许访问。
}

// routeAliasKey 归一化外部传入的路由别名，避免白名单判断受空白字符影响。
func routeAliasKey(routeAlias string) routealias.Alias {
	return routealias.Alias(strings.TrimSpace(routeAlias))
}

// checkAdminNeedResetPassword 校验管理员是否处于必须先修改登录密码状态。
func (l *SecurityLogic) checkAdminNeedResetPassword(admin *model.Admin, routeAlias string) error {
	if admin == nil || admin.NeedResetPassword != 1 {
		return nil
	}
	if passwordResetAllowlist[routeAliasKey(routeAlias)] {
		return nil
	}
	return ErrAdminPasswordResetRequired
}

// shouldSkipMFAForPasswordReset 判断必须改密阶段是否允许当前路由先跳过登录 MFA 校验。
func shouldSkipMFAForPasswordReset(admin *model.Admin, routeAlias string) bool {
	if admin == nil || admin.NeedResetPassword != 1 {
		return false
	}
	return passwordResetAllowlist[routeAliasKey(routeAlias)]
}

// shouldBypassLoginMFACheck 判断当前路由是否允许跳过“登录态尚未完成 MFA”的前置拦截。
// 该判断只用于让 MFA 自助完成链路先跑通，不影响普通业务接口仍受登录 MFA 约束。
func shouldBypassLoginMFACheck(admin *model.Admin, routeAlias string) bool {
	routeAlias = strings.TrimSpace(routeAlias)
	if shouldSkipMFAForPasswordReset(admin, routeAlias) {
		return true
	}
	return loginMFAAllowlist[routeAliasKey(routeAlias)]
}

// loginCheckMFAFlagTTL 返回 MFA 登录校验标记的默认过期时间，后续 MFA 接口可复用。
func loginCheckMFAFlagTTL() time.Duration {
	return 4 * time.Hour
}
