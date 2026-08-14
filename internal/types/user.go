package types

import (
	"strings"
	"unicode"

	"github.com/Is999/go-utils/errors"
)

const (
	// userPasswordMinLength 表示业务用户后台重置密码的最小长度。
	userPasswordMinLength = 8
	// userPasswordMaxLength 表示业务用户后台重置密码的最大长度。
	userPasswordMaxLength = 64
	// userShardNoMod 表示用户 ID 哈希分片上限，与 idgen.ShardMod 保持一致。
	userShardNoMod = 1024
	// userListMaxPageSize 对齐用户列表分页器的最大档位；身份目录采用有界游标查询，单次最多额外读取一行判断后续页。
	userListMaxPageSize = 200
)

// UserListReq 表示业务用户列表查询请求。
type UserListReq struct {
	ID          int64  `json:"id,optional" form:"id,optional"`             // 用户 ID 精确筛选
	CursorID    int64  `json:"cursorId,optional" form:"cursorId,optional"` // 身份目录下一页用户 ID 游标，首页为 0
	ShardNo     *int   `json:"shardNo,optional" form:"shardNo,optional"`   // ID 哈希分片筛选：0-1023
	Username    string `json:"username,optional" form:"username,optional"` // 用户名筛选
	Email       string `json:"email,optional" form:"email,optional"`       // 邮箱筛选
	Phone       string `json:"phone,optional" form:"phone,optional"`       // 手机号筛选
	Status      *int   `json:"status,optional" form:"status,optional"`     // 状态筛选：1 正常，0 禁用
	GetOrderReq        // GetOrderReq 表示排序参数。
	GetPageReq         // GetPageReq 表示分页参数。
}

// UserIDReq 表示业务用户详情路径请求。
type UserIDReq struct {
	ID int64 `path:"id" json:"id,optional" form:"id,optional"` // 用户 ID
}

// Validate 校验业务用户详情路径请求。
func (r *UserIDReq) Validate() error {
	if r.ID <= 0 {
		return errors.Errorf("用户 ID 不能为空")
	}
	return nil
}

// Validate 校验业务用户列表查询请求。
func (r *UserListReq) Validate() error {
	r.Username = strings.TrimSpace(r.Username)
	r.Email = strings.TrimSpace(r.Email)
	r.Phone = strings.TrimSpace(r.Phone)
	if r.Status != nil && (*r.Status != 0 && *r.Status != 1) {
		return errors.Errorf("用户状态不合法")
	}
	if r.ShardNo != nil && (*r.ShardNo < 0 || *r.ShardNo >= userShardNoMod) {
		return errors.Errorf("用户分片号必须在0-%d之间", userShardNoMod-1)
	}
	if err := r.GetOrderReq.Validate(); err != nil {
		return errors.Tag(err)
	}
	r.Page, r.PageSize = normalizePageWithMax(r.Page, r.PageSize, defaultPageSize, userListMaxPageSize)
	return nil
}

// CreateUserReq 表示后台新增前台用户请求。
type CreateUserReq struct {
	Username     string `json:"username"`              // 用户名，3-32 位
	Password     string `json:"password"`              // 登录密码
	Nickname     string `json:"nickname,optional"`     // 昵称，为空时使用用户名
	Email        string `json:"email,optional"`        // 邮箱
	Phone        string `json:"phone,optional"`        // 手机号
	Avatar       string `json:"avatar,optional"`       // 头像地址
	Status       *int   `json:"status,optional"`       // 状态：1 正常，0 禁用
	TwoStepKey   string `json:"twoStepKey,optional"`   // MFA 二次票据 key
	TwoStepValue string `json:"twoStepValue,optional"` // MFA 二次票据 value
}

// Validate 校验后台新增前台用户请求。
func (r *CreateUserReq) Validate() error {
	r.Username = strings.TrimSpace(r.Username)
	r.Password = strings.TrimSpace(r.Password)
	r.Nickname = strings.TrimSpace(r.Nickname)
	r.Email = strings.TrimSpace(r.Email)
	r.Phone = strings.TrimSpace(r.Phone)
	r.Avatar = strings.TrimSpace(r.Avatar)
	if err := validateUsername(r.Username); err != nil {
		return errors.Tag(err)
	}
	if err := validateUserPassword(r.Password, true); err != nil {
		return errors.Tag(err)
	}
	if r.Status != nil && (*r.Status != 0 && *r.Status != 1) {
		return errors.Errorf("用户状态不合法")
	}
	return nil
}

// UpdateUserReq 表示后台编辑前台用户资料请求。
type UpdateUserReq struct {
	ID           int64   `path:"id" json:"id,optional" form:"id,optional"` // 用户 ID
	Nickname     *string `json:"nickname,optional"`                        // 昵称
	Email        *string `json:"email,optional"`                           // 邮箱
	Phone        *string `json:"phone,optional"`                           // 手机号
	Avatar       *string `json:"avatar,optional"`                          // 头像地址
	TwoStepKey   string  `json:"twoStepKey,optional"`                      // MFA 二次票据 key
	TwoStepValue string  `json:"twoStepValue,optional"`                    // MFA 二次票据 value
}

// Validate 校验后台编辑前台用户资料请求。
func (r *UpdateUserReq) Validate() error {
	if r.ID <= 0 {
		return errors.Errorf("用户 ID 不能为空")
	}
	if r.Nickname != nil {
		value := strings.TrimSpace(*r.Nickname)
		r.Nickname = &value
	}
	if r.Email != nil {
		value := strings.TrimSpace(*r.Email)
		r.Email = &value
	}
	if r.Phone != nil {
		value := strings.TrimSpace(*r.Phone)
		r.Phone = &value
	}
	if r.Avatar != nil {
		value := strings.TrimSpace(*r.Avatar)
		r.Avatar = &value
	}
	return nil
}

// UserStatusReq 表示后台修改前台用户状态请求。
type UserStatusReq struct {
	ID           int64  `path:"id" json:"id,optional" form:"id,optional"`           // 用户 ID
	Status       *int   `json:"status,optional" form:"status,optional"`             // 状态：1 正常，0 禁用
	TwoStepKey   string `json:"twoStepKey,optional" form:"twoStepKey,optional"`     // MFA 二次票据 key
	TwoStepValue string `json:"twoStepValue,optional" form:"twoStepValue,optional"` // MFA 二次票据 value
}

// Validate 校验后台修改前台用户状态请求。
func (r *UserStatusReq) Validate() error {
	if r.ID <= 0 {
		return errors.Errorf("用户 ID 不能为空")
	}
	if r.Status == nil || (*r.Status != 0 && *r.Status != 1) {
		return errors.Errorf("用户状态不合法")
	}
	return nil
}

// ResetUserPasswordReq 表示后台重置前台用户密码请求。
type ResetUserPasswordReq struct {
	ID           int64  `path:"id" json:"id,optional" form:"id,optional"` // 用户 ID
	Password     string `json:"password"`                                 // 新密码
	TwoStepKey   string `json:"twoStepKey,optional"`                      // MFA 二次票据 key
	TwoStepValue string `json:"twoStepValue,optional"`                    // MFA 二次票据 value
}

// Validate 校验后台重置前台用户密码请求。
func (r *ResetUserPasswordReq) Validate() error {
	if r.ID <= 0 {
		return errors.Errorf("用户 ID 不能为空")
	}
	return validateUserPassword(r.Password, true)
}

// SyncUserRuntimeReq 表示手动同步前台用户 API 运行态请求。
type SyncUserRuntimeReq struct {
	ID           int64  `path:"id" json:"id,optional" form:"id,optional"` // 用户 ID
	Profile      bool   `json:"profile,optional"`                         // 是否同步资料缓存
	Sessions     bool   `json:"sessions,optional"`                        // 是否失效登录态
	TwoStepKey   string `json:"twoStepKey,optional"`                      // MFA 二次票据 key
	TwoStepValue string `json:"twoStepValue,optional"`                    // MFA 二次票据 value
}

// Validate 校验手动同步前台用户 API 运行态请求。
func (r *SyncUserRuntimeReq) Validate() error {
	if r.ID <= 0 {
		return errors.Errorf("用户 ID 不能为空")
	}
	if !r.Profile && !r.Sessions {
		r.Profile = true
	}
	return nil
}

// UserItem 表示前台用户列表和详情项。
type UserItem struct {
	ID          int64  `json:"id,string"`   // 用户 ID，JSON 以字符串返回，避免前端丢失精度
	ShardNo     int    `json:"shardNo"`     // ID 哈希分片，来源 CRC32(id字符串)%1024，便于分表和分片游标查询
	Username    string `json:"username"`    // 用户名
	Nickname    string `json:"nickname"`    // 昵称
	EmailMasked string `json:"emailMasked"` // 邮箱脱敏展示值
	PhoneMasked string `json:"phoneMasked"` // 手机号脱敏展示值
	Avatar      string `json:"avatar"`      // 头像
	Status      int    `json:"status"`      // 状态：1 正常，0 禁用
	LastLoginAt string `json:"lastLoginAt"` // 最后登录时间
	LastLoginIP string `json:"lastLoginIP"` // 最后登录 IP
	CreatedAt   string `json:"createdAt"`   // 创建时间
	UpdatedAt   string `json:"updatedAt"`   // 更新时间
}

// UserListMeta 表示业务用户列表的附加分页口径。
type UserListMeta struct {
	ExactTotal            bool  `json:"exactTotal"`            // 是否返回精确总数；身份目录分页固定为 false
	HasMore               bool  `json:"hasMore"`               // 身份目录是否仍有下一页
	NextCursorID          int64 `json:"nextCursorId,string"`   // 下一页用户 ID 游标，末页为 0
	StatusFilterSupported bool  `json:"statusFilterSupported"` // 是否支持状态筛选；身份目录当前为 false
}

// UserRuntimeSyncResp 表示 admin 调用 API 运行态同步后的回执。
type UserRuntimeSyncResp struct {
	Enabled                 bool   `json:"enabled"`                 // 是否已配置 API 内网同步
	Success                 bool   `json:"success"`                 // 本次同步是否成功
	UserID                  int64  `json:"userId,string"`           // 用户 ID，JSON 以字符串返回，避免前端丢失精度
	ProfileCacheInvalidated bool   `json:"profileCacheInvalidated"` // 是否已处理资料缓存
	SessionsInvalidated     bool   `json:"sessionsInvalidated"`     // 是否已处理登录态
	AuthVersion             uint64 `json:"authVersion,string"`      // 本次登录态失效使用的已提交认证版本，JSON 以字符串返回
	Message                 string `json:"message"`                 // 同步结果说明
}

// UserMutationResp 表示前台用户写操作响应。
type UserMutationResp struct {
	Item *UserItem           `json:"item,omitempty"` // 最新用户资料
	Sync UserRuntimeSyncResp `json:"sync"`           // API 运行态同步结果
}

// APIRuntimeConfigReloadReq 表示后台触发 API 配置热加载请求。
type APIRuntimeConfigReloadReq struct {
	TwoStepKey   string `json:"twoStepKey,optional"`   // MFA 二次票据 key
	TwoStepValue string `json:"twoStepValue,optional"` // MFA 二次票据 value
}

// Validate 校验后台触发 API 配置热加载请求。
func (r *APIRuntimeConfigReloadReq) Validate() error {
	return nil
}

// APIRuntimeConfigReloadResp 表示 admin 调用 API 热加载接口后的回执。
type APIRuntimeConfigReloadResp struct {
	Connected bool                        `json:"connected"` // 是否已配置并成功访问 API 内网接口
	Status    *TaskConfigReloadStatusResp `json:"status"`    // API 返回的热加载状态快照
	Message   string                      `json:"message"`   // 调用结果说明
}

// APIRuntimeConfigItemsResp 表示 admin 查询 API 运行态配置项后的回执。
type APIRuntimeConfigItemsResp struct {
	Connected bool                     `json:"connected"` // 是否已配置并成功访问 API 内网接口
	Items     *TaskConfigItemQueryResp `json:"items"`     // API 返回的运行态配置项快照
	Message   string                   `json:"message"`   // 调用结果说明
}

// validateUsername 校验前台用户名基础长度。
func validateUsername(username string) error {
	if len(username) < 3 || len(username) > 32 {
		return errors.Errorf("用户名必须为3-32位")
	}
	return nil
}

// validateUserPassword 校验前台用户后台密码。
func validateUserPassword(password string, required bool) error {
	password = strings.TrimSpace(password)
	if password == "" {
		if required {
			return errors.Errorf("密码不能为空")
		}
		return nil
	}
	if len(password) < userPasswordMinLength || len(password) > userPasswordMaxLength {
		return errors.Errorf("密码必须为8-64位")
	}
	for _, item := range password {
		if unicode.Is(unicode.Han, item) {
			return errors.Errorf("密码不能包含中文")
		}
	}
	return nil
}
