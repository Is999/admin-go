package logic

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

const (
	// AdminSuperRoleID 表示超级管理员角色 ID。
	AdminSuperRoleID = 1
)

// FormatDateTime 把数据库时间统一转换成前端使用的日期时间字符串。
func FormatDateTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.DateTime)
}

// FormatOptionalDateTime 把可空数据库时间转换成前端日期时间；nil 表示业务事件从未发生，返回空字符串。
func FormatOptionalDateTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return FormatDateTime(*t)
}

// BuildTreePids 根据父级 ID 和父级族谱生成当前节点族谱。
func BuildTreePids(parentID int, parentPids string) string {
	if parentID <= 0 {
		return ""
	}
	parentPids = strings.TrimSpace(parentPids)
	if parentPids == "" {
		return strconv.Itoa(parentID)
	}
	return fmt.Sprintf("%s,%d", parentPids, parentID)
}

// ApplyGenealogyScopeFilter 使用 MySQL `FIND_IN_SET` 过滤指定祖先节点下的全部子孙节点。
// 当前项目的 `pids` 以逗号分隔祖先链存储，统一收口到这里，避免各业务线继续散落低效模糊匹配。
func ApplyGenealogyScopeFilter(db *gorm.DB, genealogyField string, id int) *gorm.DB {
	if db == nil || id <= 0 {
		return db
	}
	genealogyField = strings.TrimSpace(genealogyField)
	if genealogyField == "" {
		genealogyField = "pids"
	}
	return db.Where(fmt.Sprintf("FIND_IN_SET(?, %s)", genealogyField), id)
}

// ContainsTreeID 判断逗号分隔族谱中是否包含指定 ID。
func ContainsTreeID(pids string, id int) bool {
	if id <= 0 {
		return false
	}
	target := strconv.Itoa(id)
	for _, part := range strings.Split(pids, ",") {
		if strings.TrimSpace(part) == target {
			return true
		}
	}
	return false
}

// NormalizedOrderField 把前端常见小驼峰排序字段映射成数据库字段。
func NormalizedOrderField(orderBy string, fallback string) string {
	switch strings.TrimSpace(orderBy) {
	case "createdAt":
		return "created_at"
	case "updatedAt":
		return "updated_at"
	case "realName":
		return "real_name"
	case "lastLoginTime":
		return "last_login_time"
	case "lastLoginIP":
		return "last_login_ip"
	case "":
		return fallback
	default:
		return orderBy
	}
}

// NormalizedOrderDirection 归一化排序方向，非法值统一回落到降序。
func NormalizedOrderDirection(order string) string {
	if strings.ToLower(strings.TrimSpace(order)) == "asc" {
		return "asc"
	}
	return "desc"
}

// IntPtrDefault 返回指针整数值；指针为空时使用默认值。
func IntPtrDefault(value *int, defaultValue int) int {
	if value == nil {
		return defaultValue
	}
	return *value
}
