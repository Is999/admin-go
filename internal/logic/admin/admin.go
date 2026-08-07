package admin

import (
	"context"
	"net/http"
	"strings"
	"time"

	"admin/common/codes"
	i18n "admin/common/i18n"
	"admin/helper"
	corelogic "admin/internal/logic"
	cachelogic "admin/internal/logic/cache"
	filelogic "admin/internal/logic/file"
	rbaclogic "admin/internal/logic/rbac"
	securitylogic "admin/internal/logic/security"
	"admin/internal/model"
	"admin/internal/requestctx"
	"admin/internal/svc"
	"admin/internal/types"

	"github.com/Is999/go-utils/errors"
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

const (
	// adminLoginDummyPasswordHash 用于不存在账号的等时 bcrypt 校验，避免通过响应耗时枚举管理员账号。
	adminLoginDummyPasswordHash = "$2y$10$ory3FZfUy1VExaUHmEkeluYtVtP/4CiCCfeSPfD12T9dbpWqO52Eq"
	// adminLoginCleanupTimeout 限制数据库提交失败后清理本次 Redis 会话的独立等待时间。
	adminLoginCleanupTimeout = 3 * time.Second
)

// AdminLogic 承载管理员登录、会话、账号创建和权限码查询等核心逻辑。
type AdminLogic struct {
	*corelogic.BaseLogic // 复用上下文、日志、数据库和缓存等公共能力
}

// NewAdminLogic 创建管理员业务逻辑对象，继承带请求上下文的 BaseLogic。
func NewAdminLogic(r *http.Request, svcCtx *svc.ServiceContext) *AdminLogic {
	return &AdminLogic{
		BaseLogic: corelogic.NewBaseLogic(r, svcCtx),
	}
}

// buildAdminSession 把管理员模型和当前 token 统一转换成会话缓存结构。
func buildAdminSession(admin *model.Admin, token string) *types.AdminSession {
	return cachelogic.BuildAdminSession(admin, token)
}

// Login 校验管理员账号密码，更新登录态并写入缓存会话信息。
func (l *AdminLogic) Login(req *types.LoginReq) *types.BizResult {
	if err := securitylogic.NewSecurityLogic(l.Ctx, l.Svc).CheckAdminLoginIP(req.IP); err != nil {
		if errors.Is(err, securitylogic.ErrAdminIPNotAllowed) {
			return types.Forbidden(i18n.MsgKeyAdminIPNotAllowed).
				ToBizResult().
				WithError(err)
		}
		return types.ServerError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminLogic.Login 校验登录IP失败").ToBizResult()
	}
	// 登录属于强一致鉴权链路，必须直接查主库，避免主从延迟导致禁用/改密状态未及时生效。
	admin, err := model.FindUserByName(l.Svc.WriteDB(svc.DatabaseMain), req.Username)
	if err != nil {
		return types.DBError(i18n.MsgKeyDBError, err,
			"AdminLogic.Login 账号[%s]", req.Username).ToBizResult()
	}

	if admin == nil {
		// 账号不存在时仍执行固定哈希校验，使失败路径耗时接近，避免通过响应时间枚举管理员账号。
		_ = bcrypt.CompareHashAndPassword([]byte(adminLoginDummyPasswordHash), []byte(req.Password))
		return invalidAdminPasswordResult(errors.Errorf("AdminLogic.Login 账号[%s]不存在", req.Username))
	}

	if err = bcrypt.CompareHashAndPassword([]byte(admin.Password), []byte(req.Password)); err != nil {
		return invalidAdminPasswordResult(errors.Errorf("AdminLogic.Login 账号[%s]密码错误", req.Username))
	}

	// 检查用户状态
	if admin.Status != 1 {
		return types.NewBizResult(codes.UserDisabled).
			SetI18nMessage(i18n.MsgKeyUserDisabled).
			WithError(errors.Errorf("AdminLogic.Login 账号[%s]已被禁用", req.Username))
	}

	// 更新最后登录时间、IP 与离线归属地；归属地查询异常不影响登录主流程。
	l.setLastLoginIP(admin, req.IP)
	admin.LastLoginTime = time.Now()
	admin.UpdatedAt = time.Now()

	// 生成 JWT 令牌
	token, err := l.generateJWT(admin.ID, admin.Name, admin.LastLoginIP)
	if err != nil {
		return types.ServerError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminLogic.Login 账号[%s]生成令牌失败", req.Username).ToBizResult()
	}

	session := buildAdminSession(admin, token)

	cacheLogic := cachelogic.NewCacheLogic(l.Ctx, l.Svc)
	// 先完成依赖 Redis 的资料构造，失败时不得留下可鉴权会话或虚假的数据库登录记录。
	userInfo, err := securitylogic.NewSecurityLogic(l.Ctx, l.Svc).BuildProfileInfo(admin, token)
	if err != nil {
		if errors.Is(err, cachelogic.ErrRedisUnavailable) {
			return redisUnavailableBizResult(err, "AdminLogic.Login 账号[%s]构造登录用户上下文失败", req.Username)
		}
		return types.ServerError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminLogic.Login 账号[%s]构造登录用户上下文失败", req.Username).ToBizResult()
	}

	// 会话成功写入后再提交最后登录信息；数据库失败时按 token 所有权删除本次会话，不影响并发产生的新会话。
	if err = cacheLogic.SetAdminSession(admin.ID, session); err != nil {
		if errors.Is(err, cachelogic.ErrRedisUnavailable) {
			return redisUnavailableBizResult(err, "AdminLogic.Login 账号[%s]缓存用户信息失败", req.Username)
		}
		return types.ServerError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminLogic.Login 账号[%s]缓存用户信息失败", req.Username).ToBizResult()
	}
	update := map[string]any{
		"last_login_time":   admin.LastLoginTime,
		"last_login_ip":     admin.LastLoginIP,
		"last_login_ipaddr": admin.LastLoginIPAddr,
		"updated_at":        admin.UpdatedAt,
	}
	if err = model.UpdateAdmin(l.Svc.WriteDB(svc.DatabaseMain), admin.ID, update); err != nil {
		if cleanupErr := l.discardCreatedAdminSession(admin.ID, token); cleanupErr != nil {
			err = errors.Wrapf(err, "更新最后登录信息失败且清理本次会话失败 cleanup_error=%v", cleanupErr)
		}
		return types.DBError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminLogic.Login 账号[%s]更新最后登录信息失败", req.Username).ToBizResult()
	}

	return types.NewBizResult(codes.Success).
		SetI18nMessage(i18n.MsgKeySuccess).
		WithData(&types.ProfileLoginResp{
			Token: token,
			User:  userInfo,
		})
}

// discardCreatedAdminSession 使用独立上下文按 token 所有权清理失败登录创建的会话。
func (l *AdminLogic) discardCreatedAdminSession(adminID int, token string) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), adminLoginCleanupTimeout)
	defer cancel()
	_, err := cachelogic.NewCacheLogic(cleanupCtx, l.Svc).DeleteAdminSessionForLogout(adminID, token)
	return errors.Tag(err)
}

// setLastLoginIP 同步更新最后登录 IP 与其归属地；未启用或未命中时清空旧归属地。
func (l *AdminLogic) setLastLoginIP(admin *model.Admin, ip string) {
	ip = requestctx.NormalizeClientIP(ip)
	admin.LastLoginIP = ip
	admin.LastLoginIPAddr = ""
	if ip != "" && l.Svc.IPRegion != nil {
		admin.LastLoginIPAddr = l.Svc.IPRegion.Lookup(ip)
	}
	if ip != "" {
		requestctx.SetClientIPRegion(l.Ctx, ip, admin.LastLoginIPAddr)
	}
}

// invalidAdminPasswordResult 返回统一的账号或密码错误，避免向外暴露管理员账号是否存在。
func invalidAdminPasswordResult(err error) *types.BizResult {
	return types.NewBizResult(codes.InvalidPassword).
		SetI18nMessage(i18n.MsgKeyAccountPwdInvalid).
		WithError(err)
}

// redisUnavailableBizResult 把核心会话与鉴权缓存故障统一映射为 HTTP 503 业务码。
func redisUnavailableBizResult(err error, format string, args ...any) *types.BizResult {
	return &types.BizResult{
		Code:       codes.RedisUnavailable,
		MessageKey: i18n.MsgKeyRedisUnavailable,
		Error:      errors.Wrapf(err, format, args...),
	}
}

// Logout 清理当前管理员缓存登录态，完成显式登出。
func (l *AdminLogic) Logout(ctxAdmin *helper.CtxAdmin) *types.BizResult {
	cacheLogic := cachelogic.NewCacheLogic(l.Ctx, l.Svc)
	_, err := cacheLogic.DeleteAdminSessionForLogout(ctxAdmin.ID, l.AccessToken())
	if err != nil {
		if errors.Is(err, cachelogic.ErrRedisUnavailable) {
			return redisUnavailableBizResult(err, "AdminLogic.Logout 账号[%s]原子清理当前会话失败", ctxAdmin.Name)
		}
		return types.ServerError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminLogic.Logout 账号[%s]原子清理当前会话失败", ctxAdmin.Name).ToBizResult()
	}

	return types.NewBizResult(codes.Success).
		SetI18nMessage(i18n.MsgKeyLogoutSuccess)
}

// generateJWT 生成 JWT 令牌，sub/username/ip 会被后续鉴权中间件解析并回填到请求上下文。
func (l *AdminLogic) generateJWT(userID int, username string, IP string) (string, error) {
	cfg := l.Svc.CurrentConfig()
	expiresIn := cfg.JwtExpiresIn
	if expiresIn <= 0 {
		expiresIn = 86400
	}
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":      userID,
		"username": username,
		"ip":       IP,
		"jti":      uuid.NewString(),
		"iat":      now.Unix(),
		"exp":      now.Add(time.Duration(expiresIn) * time.Second).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(cfg.JwtSecret))
}

// Create 新增管理员账号，并在同一事务内完成落库和密码摘要回写。
func (l *AdminLogic) Create(req *types.AddAdminReq) *types.BizResult {
	if err := l.RequireOperateMFATwoStep(securitylogic.MFAScenarioAddUser, req.TwoStepKey, req.TwoStepValue); err != nil {
		return l.MFABizResult(err)
	}

	// 创建前唯一性校验也走主库，避免从库延迟导致“刚创建完又查不到”而误判可重复创建。
	q := model.NewQuery(l.Svc.WriteDB(svc.DatabaseMain), &model.Admin{})
	exists, err := q.Exists(func(db *gorm.DB) *gorm.DB {
		return db.Where("name = ?", req.Username)
	})
	if err != nil {
		return types.DBError(i18n.MsgKeyDBError, err,
			"AdminLogic.Create 账号[%s]", req.Username).ToBizResult()
	}
	if exists {
		return AdminNameAlreadyExistsResult(req.Username, ErrAdminNameAlreadyExists)
	}
	avatar, err := filelogic.NewFileTransferLogicWithContext(l.Ctx, l.Svc).ValidateAdminAvatar(req.Avatar)
	if err != nil {
		return types.ParamErrorResult(err).
			WithError(errors.Wrapf(err, "AdminLogic.Create 账号[%s]头像校验失败", req.Username))
	}
	password, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return types.ServerError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminLogic.Create 账号[%s]生成密码哈希失败", req.Username).ToBizResult()
	}
	encryptedMFASecret := ""
	if strings.TrimSpace(req.MfaSecureKey) != "" {
		encryptedMFASecret, err = securitylogic.NewSecurityLogic(l.Ctx, l.Svc).EncryptAdminMFASecret(req.MfaSecureKey)
		if err != nil {
			return types.NewBizResult(codes.ServerError).
				SetI18nMessage(i18n.MsgKeyInternalError).
				WithError(errors.Wrapf(err, "AdminLogic.Create 账号[%s]加密MFA秘钥失败", req.Username))
		}
	}

	admin := model.Admin{
		ID:                0,
		Name:              req.Username,
		RealName:          req.RealName,
		Password:          string(password),
		NeedResetPassword: 1,
		Email:             req.Email,
		Phone:             req.Phone,
		MfaSecureKey:      encryptedMFASecret,
		// 首次登录阶段允许用户先改密、后续再自行决定是否完成 MFA 绑定，因此新建账号默认保持待启用状态。
		MfaStatus:       0,
		Status:          1,
		Avatar:          avatar,
		Description:     req.Description,
		LastLoginTime:   time.Time{},
		LastLoginIP:     "",
		LastLoginIPAddr: "",
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}
	// 管理员和初始角色必须在同一事务提交，避免账号已创建但角色未绑定。
	roleIDs := types.UniquePositiveInts(req.RoleIDs)
	if len(roleIDs) == 0 {
		return l.createAdminWithRoles(&admin, roleIDs)
	}
	return rbaclogic.WithRBACWriteLock(l.BaseLogic, "AdminLogic.Create", func(lockedBaseLogic *corelogic.BaseLogic) *types.BizResult {
		originalBaseLogic := l.BaseLogic
		l.BaseLogic = lockedBaseLogic
		defer func() {
			l.BaseLogic = originalBaseLogic
		}()
		return l.createAdminWithRoles(&admin, roleIDs)
	})
}

// createAdminWithRoles 校验初始角色范围，并在同一事务创建管理员和角色关系。
func (l *AdminLogic) createAdminWithRoles(admin *model.Admin, roleIDs []int) *types.BizResult {
	if len(roleIDs) > 0 {
		if err := (&rbaclogic.AdminRoleLogic{BaseLogic: l.BaseLogic}).EnsureRolesWithinManageScope(roleIDs); err != nil {
			return types.Forbidden(i18n.MsgKeyForbidden).
				ToBizResult().
				WithError(errors.Wrapf(err, "AdminLogic.Create 账号[%s]初始角色超出可操作范围", admin.Name))
		}
	}
	if err := l.Svc.WriteDB(svc.DatabaseMain).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(admin).Error; err != nil {
			return errors.Wrap(err, "tx.Create 创建用户失败")
		}
		if len(roleIDs) > 0 {
			if err := (&AdminManageLogic{AdminLogic: l}).replaceAdminRolesTx(tx, admin.ID, roleIDs); err != nil {
				return errors.Wrap(err, "绑定用户初始角色失败")
			}
		}
		return nil
	}); err != nil {
		if corelogic.IsMySQLDuplicateEntryError(err) {
			return AdminNameAlreadyExistsResult(admin.Name, err)
		}
		return types.DBError(i18n.MsgKeyInternalErrorFormat, err,
			"AdminLogic.Create 账号[%s]事务执行失败", admin.Name).ToBizResult()
	}

	return types.NewBizResult(codes.AddSuccess).
		SetI18nMessage(i18n.MsgKeyAddSuccess)
}

// RequireOperateMFATwoStep 校验后台敏感操作的 MFA 二次票据。
func (l *AdminLogic) RequireOperateMFATwoStep(scenario int, twoStepKey string, twoStepValue string) error {
	ctxAdmin := l.GetCtxAdmin()
	if ctxAdmin == nil || ctxAdmin.ID <= 0 {
		return types.Nil
	}
	securityLogic := securitylogic.NewSecurityLogic(l.Ctx, l.Svc)
	needTwoStep, err := securityLogic.NeedOperateMFATwoStep(scenario)
	if err != nil {
		return errors.Tag(err)
	}
	if !needTwoStep {
		return nil
	}
	return securityLogic.VerifyMFATwoStepTicket(ctxAdmin.ID, scenario, twoStepKey, twoStepValue)
}

// MFABizResult 把后台敏感操作中的 MFA 错误转换成统一业务响应。
func (l *AdminLogic) MFABizResult(err error) *types.BizResult {
	return securitylogic.OperateMFABizResult(err, "AdminLogic.MFABizResult")
}

// GetAdminByID 通过ID获取管理员详细信息。
// 管理与会话链路需要读取即时账号状态，因此固定走主库保证强一致。
func (l *AdminLogic) GetAdminByID(id int) (*model.Admin, error) {
	var admin model.Admin
	err := l.Svc.WriteDB(svc.DatabaseMain).Where("id = ?", id).First(&admin).Error
	if err != nil {
		return nil, errors.Wrapf(err, "AdminLogic.GetAdminByID 查询管理员ID[%d]失败", id)
	}
	return &admin, nil
}

// GetLoginAfterInfo 返回前端登录后初始化所需的管理员资料与基础角色信息。
func (l *AdminLogic) GetLoginAfterInfo(ctxAdmin *helper.CtxAdmin) *types.BizResult {
	session, ok := cachelogic.AdminSessionFromContext(l.Ctx)
	if !ok {
		return &types.BizResult{
			Code:       codes.Unauthorized,
			MessageKey: i18n.MsgKeyNeedLogin,
			Error:      errors.Errorf("AdminLogic.GetLoginAfterInfo 账号[%s]缺少已校验会话", ctxAdmin.Name),
		}
	}

	// 登录后初始化复用鉴权阶段已经解析的角色，避免同一请求重复访问角色缓存。
	roleIDs, err := securitylogic.NewSecurityLogic(l.Ctx, l.Svc).EnabledRoleIDs(ctxAdmin.ID)
	if err != nil {
		return &types.BizResult{
			Code:       codes.DBError,
			MessageKey: i18n.MsgKeyRoleFetchFail,
			Error:      errors.Wrapf(err, "AdminLogic.GetLoginAfterInfo 账号[%s]获取用户角色 ID失败", ctxAdmin.Name),
		}
	}
	roles, err := (&rbaclogic.AdminRoleRelLogic{BaseLogic: l.BaseLogic}).GetRolesByUserID(int64(ctxAdmin.ID))
	if err != nil {
		return &types.BizResult{
			Code:       codes.DBError,
			MessageKey: i18n.MsgKeyRoleFetchFail,
			Error:      errors.Wrapf(err, "AdminLogic.GetLoginAfterInfo 账号[%s]获取用户角色失败", ctxAdmin.Name),
		}
	}
	// 角色 ID 为 1 的账号统一视为超级管理员，前端会基于该标记做权限码兜底。
	isSuperAdmin := false
	for _, roleID := range roleIDs {
		if roleID == corelogic.AdminSuperRoleID {
			isSuperAdmin = true
			break
		}
	}

	// 构造响应
	resp := types.AdminLoginAfterInfoResp{
		ID:                session.ID,
		UserName:          session.UserName,
		RealName:          session.RealName,
		NeedResetPassword: session.NeedResetPassword,
		IsSuperAdmin:      isSuperAdmin,
		Email:             session.Email,
		Phone:             session.Phone,
		MfaStatus:         session.MfaStatus,
		Status:            session.Status,
		Avatar:            session.Avatar,
		Description:       session.Description,
		LastLoginTime:     session.LastLoginTime,
		LastLoginIP:       session.LastLoginIP,
		LastLoginIPAddr:   session.LastLoginIPAddr,
		RoleIDs:           roleIDs,
		Roles:             roles,
		Token:             session.Token,
	}

	return &types.BizResult{
		Code:       codes.Success,
		MessageKey: i18n.MsgKeySuccess,
		Data:       resp,
	}
}

// GetUserPermissionCodes 汇总当前管理员拥有的全部权限码集合。
func (l *AdminLogic) GetUserPermissionCodes(userID int) *types.BizResult {
	// `/auth/codes` 继续保持缓存优先；缓存 miss 时由下层统一回源数据库并更新缓存。
	securityLogic := securitylogic.NewSecurityLogic(l.Ctx, l.Svc)
	codesArr, err := securityLogic.UserPermissionUUIDsWithCache(userID)
	if err != nil {
		if errors.Is(err, cachelogic.ErrRedisUnavailable) {
			return &types.BizResult{
				Code:       codes.RedisUnavailable,
				MessageKey: i18n.MsgKeyRedisUnavailable,
				Error:      err,
			}
		}
		return &types.BizResult{
			Code:       codes.DependencyUnavailable,
			MessageKey: i18n.MsgKeyDependencyUnavailable,
			Error:      err,
		}
	}
	if len(codesArr) == 0 {
		return &types.BizResult{
			Code:       codes.Success,
			MessageKey: i18n.MsgKeySuccess,
			Data:       []string{},
		}
	}
	return &types.BizResult{
		Code:       codes.Success,
		MessageKey: i18n.MsgKeySuccess,
		Data:       codesArr,
	}
}

// RefreshAccessToken 为当前有效会话主动续签访问令牌，并原子续写缓存 token 与 TTL。
func (l *AdminLogic) RefreshAccessToken(ctxAdmin *helper.CtxAdmin) *types.BizResult {
	cacheLogic := cachelogic.NewCacheLogic(l.Ctx, l.Svc)
	expectedToken := strings.TrimSpace(l.AccessToken())
	if expectedToken == "" {
		return &types.BizResult{
			Code:       codes.Unauthorized,
			MessageKey: i18n.MsgKeyNeedLogin,
			Error:      errors.New("AdminLogic.RefreshAccessToken 当前请求 token 为空"),
		}
	}
	session, ok := cachelogic.AdminSessionFromContext(l.Ctx)
	ip := l.ClientIP()
	if !ok {
		return &types.BizResult{
			Code:       codes.Unauthorized,
			MessageKey: i18n.MsgKeyNeedLogin,
			Error:      errors.New("AdminLogic.RefreshAccessToken 缺少已校验会话"),
		}
	}
	token, err := l.generateJWT(session.ID, session.UserName, ip)
	if err != nil {
		return &types.BizResult{
			Code:       codes.InternalError,
			MessageKey: i18n.MsgKeyTokenGenerateFail,
			Error:      errors.Wrapf(err, "AdminLogic.RefreshAccessToken 账号[%s]l.generateJWT 生成新Token失败", session.UserName),
		}
	}
	activeToken, err := cacheLogic.RotateAdminToken(session.ID, expectedToken, token)
	if err != nil {
		if errors.Is(err, cachelogic.ErrRedisUnavailable) {
			return redisUnavailableBizResult(err,
				"AdminLogic.RefreshAccessToken 账号[%s]cacheLogic.RotateAdminToken 轮换缓存 token 失败", session.UserName)
		}
		return &types.BizResult{
			Code:       codes.InternalError,
			MessageKey: i18n.MsgKeyTokenCacheFail,
			Error:      errors.Wrapf(err, "AdminLogic.RefreshAccessToken 账号[%s]cacheLogic.RotateAdminToken 轮换缓存 token 失败", session.UserName),
		}
	}
	if activeToken == "" {
		return &types.BizResult{
			Code:       codes.Unauthorized,
			MessageKey: i18n.MsgKeyNeedLogin,
			Error:      errors.Errorf("AdminLogic.RefreshAccessToken 账号[%s]Redis 会话已撤销或已被并发轮换", session.UserName),
		}
	}
	return &types.BizResult{
		Code:       codes.Success,
		MessageKey: i18n.MsgKeySuccess,
		Data:       map[string]interface{}{"token": activeToken, "isRefresh": true},
	}
}
