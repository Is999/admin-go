package model

import (
	"time"

	"github.com/Is999/go-utils/errors"

	"gorm.io/gorm"
)

// TableNameAdmin 管理员表名常量。
const TableNameAdmin = "admin"

// Admin 表示后台管理员实体。
type Admin struct {
	ID                int        `gorm:"column:id;type:int;primaryKey;autoIncrement:true;comment:主键" json:"id"`                                                     // 主键
	Name              string     `gorm:"column:name;type:varchar(20);not null;uniqueIndex:uk_name,priority:1;comment:用户账号" json:"name"`                             // 用户账号
	RealName          string     `gorm:"column:real_name;type:varchar(20);not null;comment:用户名" json:"real_name"`                                                   // 用户名
	Password          string     `gorm:"column:password;type:varchar(255);not null;comment:密码哈希" json:"password"`                                                   // 密码哈希
	NeedResetPassword int        `gorm:"column:need_reset_password;type:tinyint unsigned;not null;default:0;comment:是否必须修改登录密码：0 否，1 是" json:"need_reset_password"` // 是否必须修改登录密码：0 否，1 是
	Email             string     `gorm:"column:email;type:varchar(100);not null;comment:邮箱" json:"email"`                                                           // 邮箱
	Phone             string     `gorm:"column:phone;type:varchar(30);not null;comment:电话" json:"phone"`                                                            // 电话
	MfaSecureKey      string     `gorm:"column:mfa_secure_key;type:varchar(255);not null;comment:基于时间的一次性密码密钥" json:"mfa_secure_key"`                               // 基于时间的一次性密码密钥
	MfaStatus         int        `gorm:"column:mfa_status;type:tinyint unsigned;not null;comment:MFA 状态：0 未启用，1 已启用" json:"mfa_status"`                             // MFA 状态：0 未启用，1 已启用
	Status            int        `gorm:"column:status;type:tinyint;not null;default:1;comment:账户状态：1 正常，0 禁用" json:"status"`                                        // 账户状态：1 正常，0 禁用
	Avatar            string     `gorm:"column:avatar;type:varchar(255);not null;index:idx_avatar,priority:1;comment:头像" json:"avatar"`                             // 头像访问地址，公开读取按该索引确认已保存引用
	Description       string     `gorm:"column:description;type:varchar(255);not null;comment:简介描述" json:"description"`                                             // 简介描述
	LastLoginTime     *time.Time `gorm:"column:last_login_time;type:datetime;default:null;comment:最后登录时间，NULL 表示从未登录" json:"last_login_time"`                       // 最后登录时间；从未成功登录时为 nil
	LastLoginIP       string     `gorm:"column:last_login_ip;type:varchar(45);not null;comment:最后登录 IP" json:"last_login_ip"`                                       // 最后登录 IP
	LastLoginIPAddr   string     `gorm:"column:last_login_ipaddr;type:varchar(255);not null;comment:最后登录 IP 归属地" json:"last_login_ipaddr"`                          // 最后登录 IP 归属地
	CreatedAt         time.Time  `gorm:"column:created_at;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:添加时间" json:"created_at"`                         // 添加时间
	UpdatedAt         time.Time  `gorm:"column:updated_at;type:datetime;not null;default:CURRENT_TIMESTAMP;comment:修改时间" json:"updated_at"`                         // 修改时间
}

// TableName 返回管理员表名。
func (*Admin) TableName() string {
	return TableNameAdmin
}

// FindUserByName 根据用户名查询管理员；未命中时返回 `nil, nil`。
func FindUserByName(db *gorm.DB, name string) (*Admin, error) {
	var row Admin
	if err := db.Where("name = ?", name).First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, errors.Wrapf(err, "Admin.FindUserByName 查询账号[%s]失败", name)
	}
	return &row, nil
}

// UpdateAdmin 按主键更新管理员可变字段。
func UpdateAdmin(db *gorm.DB, id int, admin map[string]interface{}) error {
	if len(admin) == 0 {
		return nil
	}
	return db.Model(Admin{}).
		Omit("id", "name", "created_at").
		Where("id = ?", id).
		Updates(admin).Error
}
