package security

import (
	corelogic "admin/internal/logic"

	"admin/internal/model"
	"admin/internal/types"

	"github.com/Is999/go-utils/errors"
)

// BuildProfileInfo 构造前端登录态用户资料。
func (l *SecurityLogic) BuildProfileInfo(admin *model.Admin, token string) (*types.ProfileInfo, error) {
	if admin == nil {
		return nil, errors.Errorf("管理员信息不能为空")
	}
	forceMFAEnabled, err := l.ForceLoginMFAEnabled()
	if err != nil {
		return nil, errors.Wrap(err, "读取后台强制MFA配置失败")
	}
	existMFA := l.HasUsableAdminMFASecret(admin)
	// needLoginMFA 直接复用本次已读取的强制开关，避免构造个人资料时重复读取 Redis。
	needLoginMFA := admin.NeedResetPassword != 1 && (admin.MfaStatus == 1 || forceMFAEnabled)
	needBindMFA := needLoginMFA && (admin.MfaStatus != 1 || !existMFA)
	buildURL := ""
	if admin.MfaStatus != 1 || needBindMFA {
		buildURL, err = l.BuildFreshAdminMFAURL(admin)
		if err != nil {
			return nil, errors.Wrapf(err, "SecurityLogic.BuildProfileInfo 生成管理员ID[%d]MFA绑定地址失败", admin.ID)
		}
	}
	mfaCheck := 0
	if needLoginMFA {
		passed, passedErr := l.HasPassedLoginMFA(admin)
		if passedErr != nil {
			return nil, errors.Wrap(passedErr, "读取管理员登录MFA状态失败")
		}
		if !passed {
			mfaCheck = 1
		}
	}
	return &types.ProfileInfo{
		ID:                admin.ID,
		Username:          admin.Name,
		RealName:          admin.RealName,
		NeedResetPassword: admin.NeedResetPassword,
		Email:             admin.Email,
		Phone:             admin.Phone,
		Status:            admin.Status,
		MfaStatus:         admin.MfaStatus,
		GroupID:           0,
		ExistMFA:          existMFA,
		BuildMFAURL:       buildURL,
		ForceMFAEnabled:   forceMFAEnabled,
		MFABindRequired:   needBindMFA,
		Avatar:            admin.Avatar,
		Description:       admin.Description,
		LastLoginTime:     corelogic.FormatOptionalDateTime(admin.LastLoginTime),
		LastLoginIP:       admin.LastLoginIP,
		LastLoginAddr:     admin.LastLoginIPAddr,
		CreatedAt:         corelogic.FormatDateTime(admin.CreatedAt),
		UpdatedAt:         corelogic.FormatDateTime(admin.UpdatedAt),
		MFACheck:          mfaCheck,
		Frequency:         l.MFAFrequency(),
	}, nil
}
