package admin

import (
	corelogic "admin/internal/logic"
	cachelogic "admin/internal/logic/cache"
	filelogic "admin/internal/logic/file"
	"context"
	"strings"
	"time"

	codes "admin/common/codes"
	i18n "admin/common/i18n"
	"admin/internal/model"
	"admin/internal/svc"
	"admin/internal/types"

	"github.com/Is999/go-utils/errors"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

const (
	// adminBootstrapOperationReset 表示本次内网接口仅执行既有超级管理员账号重置。
	adminBootstrapOperationReset = "reset"
)

// AdminBootstrapLogic 负责仅内网可调用的管理员自举初始化逻辑。
type AdminBootstrapLogic struct {
	*corelogic.BaseLogic // 复用上下文、数据库、Redis 与日志能力
}

// NewAdminBootstrapLogic 创建管理员自举逻辑对象。
func NewAdminBootstrapLogic(ctx context.Context, svcCtx *svc.ServiceContext) *AdminBootstrapLogic {
	return &AdminBootstrapLogic{
		BaseLogic: corelogic.NewBaseLogicWithContext(ctx, svcCtx),
	}
}

// InitAdminBootstrap 通过内网接口重置既有超级管理员账号到首次登录前状态。
func (l *AdminBootstrapLogic) InitAdminBootstrap(req *types.InitAdminBootstrapReq) *types.BizResult {
	if req == nil {
		return types.ParamError(errors.Errorf("初始化请求不能为空")).ToBizResult()
	}
	if err := req.Validate(); err != nil {
		return types.ParamError(err).ToBizResult()
	}

	admin, err := l.resetExistingSuperAdmin(req)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return types.NotFound(i18n.MsgKeyUserNotFound, err,
				"AdminBootstrapLogic.InitAdminBootstrap 管理员账号[%s]不存在", req.Username).ToBizResult()
		}
		if errors.Is(err, errAdminBootstrapTargetNotSuperAdmin) {
			return types.NewBizResult(codes.Forbidden).
				SetI18nMessage(i18n.MsgKeyForbidden).
				WithError(err)
		}
		return types.DBError(i18n.MsgKeyDBError, err,
			"AdminBootstrapLogic.InitAdminBootstrap 重置管理员账号[%s]失败", req.Username).ToBizResult()
	}

	resp := &types.InitAdminBootstrapResp{
		ID:                admin.ID,
		Username:          admin.Name,
		RoleID:            corelogic.AdminSuperRoleID,
		NeedResetPassword: admin.NeedResetPassword,
		MfaStatus:         admin.MfaStatus,
		Status:            admin.Status,
		Operation:         adminBootstrapOperationReset,
	}
	if err := l.finalizeBootstrapAdminRuntime(admin.ID); err != nil {
		resp.SyncPending = true
		return corelogic.CacheSyncPendingResult(l.Logger, codes.Success, i18n.MsgKeyAdminCacheInvalidationPending, err,
			"AdminBootstrapLogic.InitAdminBootstrap 超级管理员账号[%s]ID[%d]安全缓存失效失败", req.Username, admin.ID).
			WithData(resp)
	}
	return types.NewBizResult(codes.Success).
		SetI18nMessage(i18n.MsgKeySuccess).
		WithData(resp)
}

var (
	// errAdminBootstrapTargetNotSuperAdmin 表示目标账号不是超级管理员，禁止通过内网接口重置。
	errAdminBootstrapTargetNotSuperAdmin = errors.Errorf("仅允许重置已有超级管理员账号")
)

// resetExistingSuperAdmin 在事务内只允许重置既有超级管理员账号，不允许新建账号或调整角色关系。
func (l *AdminBootstrapLogic) resetExistingSuperAdmin(req *types.InitAdminBootstrapReq) (*model.Admin, error) {
	var admin *model.Admin
	if err := l.Svc.WriteDB(svc.DatabaseMain).Transaction(func(tx *gorm.DB) error {
		existing, err := model.FindUserByName(tx, req.Username)
		if err != nil {
			return errors.Wrapf(err, "AdminBootstrapLogic.resetExistingSuperAdmin 查询账号[%s]失败", req.Username)
		}

		if existing == nil {
			return gorm.ErrRecordNotFound
		}
		isSuperAdmin, checkErr := bootstrapTargetIsSuperAdminTx(tx, existing.ID)
		if checkErr != nil {
			return errors.Wrapf(checkErr, "AdminBootstrapLogic.resetExistingSuperAdmin 校验账号[%s]超级管理员角色失败", req.Username)
		}
		if !isSuperAdmin {
			return errAdminBootstrapTargetNotSuperAdmin
		}
		// 重置可能替换托管头像，先投递引用复查型清理任务；事务失败时旧引用会阻止删除。
		if err := filelogic.NewFileTransferLogicWithContext(l.Ctx, l.Svc).
			ScheduleReplacedAdminAvatarCleanup(existing.Avatar, chooseBootstrapString(req.Avatar, existing.Avatar)); err != nil {
			return errors.Wrapf(err, "投递超级管理员账号[%s]旧头像清理任务失败", req.Username)
		}

		resetAdmin, resetErr := l.resetBootstrapAdminTx(tx, existing, req)
		if resetErr != nil {
			return errors.Wrapf(resetErr, "重置超级管理员账号失败 username=%s", req.Username)
		}
		admin = resetAdmin
		return nil
	}); err != nil {
		return nil, errors.Tag(err)
	}

	if admin == nil {
		return nil, errors.Errorf("AdminBootstrapLogic.resetExistingSuperAdmin 未生成管理员结果")
	}

	return admin, nil
}

// finalizeBootstrapAdminRuntime 清理已重置管理员的会话和 MFA 票据。
func (l *AdminBootstrapLogic) finalizeBootstrapAdminRuntime(adminID int) error {
	var firstErr error
	recordFailure := func(err error, message string) {
		if err == nil {
			return
		}
		corelogic.LogWrappedError(l.Logger, err, "%s admin_id=%d", message, adminID)
		if firstErr == nil {
			firstErr = errors.Wrapf(err, "%s admin_id=%d", message, adminID)
		}
	}

	recordFailure(cachelogic.InvalidateAdminSecurityCache(l.BaseLogic, adminID), "重置超级管理员后失效安全缓存失败")
	return errors.Tag(firstErr)
}

// resetBootstrapAdminTx 在事务内把既有超级管理员重置为首次登录前状态，不修改角色关系。
func (l *AdminBootstrapLogic) resetBootstrapAdminTx(tx *gorm.DB, admin *model.Admin, req *types.InitAdminBootstrapReq) (*model.Admin, error) {
	if admin == nil {
		return nil, errors.Errorf("管理员不存在")
	}

	passwordHash, err := bcrypt.GenerateFromPassword([]byte(strings.TrimSpace(req.Password)), bcrypt.DefaultCost)
	if err != nil {
		return nil, errors.Wrapf(err, "AdminBootstrapLogic.resetBootstrapAdminTx 生成账号[%s]密码哈希失败", req.Username)
	}

	updates := map[string]any{
		"password":            string(passwordHash),
		"real_name":           chooseBootstrapString(req.RealName, admin.RealName),
		"need_reset_password": 1,
		"email":               chooseBootstrapString(req.Email, admin.Email),
		"phone":               chooseBootstrapString(req.Phone, admin.Phone),
		"mfa_status":          0,
		"mfa_secure_key":      "",
		"status":              1,
		"avatar":              chooseBootstrapString(req.Avatar, admin.Avatar),
		"description":         chooseBootstrapString(req.Description, admin.Description),
		"last_login_time":     nil,
		"last_login_ip":       "",
		"last_login_ipaddr":   "",
		"updated_at":          time.Now(),
	}
	if err := tx.Model(&model.Admin{}).Where("id = ?", admin.ID).Updates(updates).Error; err != nil {
		return nil, errors.Wrapf(err, "AdminBootstrapLogic.resetBootstrapAdminTx 重置账号[%s]首次状态失败", req.Username)
	}

	admin.RealName = chooseBootstrapString(req.RealName, admin.RealName)
	admin.Password = string(passwordHash)
	admin.NeedResetPassword = 1
	admin.Email = chooseBootstrapString(req.Email, admin.Email)
	admin.Phone = chooseBootstrapString(req.Phone, admin.Phone)
	admin.MfaStatus = 0
	admin.MfaSecureKey = ""
	admin.Status = 1
	admin.Avatar = chooseBootstrapString(req.Avatar, admin.Avatar)
	admin.Description = chooseBootstrapString(req.Description, admin.Description)
	admin.LastLoginTime = nil
	admin.LastLoginIP = ""
	admin.LastLoginIPAddr = ""
	admin.UpdatedAt = time.Now()
	return admin, nil
}

// bootstrapTargetIsSuperAdminTx 在当前写事务内校验目标账号仍绑定启用的超级管理员角色。
func bootstrapTargetIsSuperAdminTx(tx *gorm.DB, adminID int) (bool, error) {
	if tx == nil || adminID <= 0 {
		return false, nil
	}
	var row struct {
		RoleID int `gorm:"column:role_id"` // 超级管理员角色 ID
	}
	// admin_role_rel 的联合主键从 user_id 起始，admin_role 使用主键 id；该查询最多读取一行。
	err := tx.Table(model.TableNameAdminRoleRel+" AS rel").
		Select("rel.role_id").
		Joins("JOIN "+model.TableNameAdminRole+" AS role ON role.id = rel.role_id").
		Where("rel.user_id = ? AND rel.role_id = ? AND role.status = 1 AND role.is_delete = 0", adminID, corelogic.AdminSuperRoleID).
		Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, errors.Tag(err)
	}
	return row.RoleID == corelogic.AdminSuperRoleID, nil
}

// chooseBootstrapString 在内网初始化时优先采用显式传入的新值，否则保留既有字段值。
func chooseBootstrapString(next string, fallback string) string {
	next = strings.TrimSpace(next)
	if next != "" {
		return next
	}
	return strings.TrimSpace(fallback)
}
