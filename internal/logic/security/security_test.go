package security

import (
	"admin/internal/database"
	corelogic "admin/internal/logic"
	cachelogic "admin/internal/logic/cache"
	rbaclogic "admin/internal/logic/rbac"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"admin/common/codes"
	keys "admin/common/rediskeys"
	"admin/internal/config"
	"admin/internal/model"
	"admin/internal/requestctx"
	"admin/internal/routealias"
	"admin/internal/svc"
	"admin/internal/types"

	"github.com/Is999/go-utils/errors"
	"github.com/alicebob/miniredis/v2"
	miniredisserver "github.com/alicebob/miniredis/v2/server"
	"github.com/redis/go-redis/v9"
)

// TestEnabledRoleIDsReusesRequestResult 验证同一请求复用已解析角色，不再访问 Redis 或数据库。
func TestEnabledRoleIDsReusesRequestResult(t *testing.T) {
	ctx, _ := requestctx.New(context.Background())
	requestctx.SetEnabledRoleIDs(ctx, []int{2, 3})
	logicObj := NewSecurityLogic(ctx, svc.NewServiceContext(config.Config{}, svc.Dependencies{}))
	roleIDs, err := logicObj.EnabledRoleIDs(7)
	if err != nil {
		t.Fatalf("EnabledRoleIDs() error = %v", err)
	}
	if fmt.Sprint(roleIDs) != "[2 3]" {
		t.Fatalf("EnabledRoleIDs() = %v, want [2 3]", roleIDs)
	}
}

// runSecurityStandaloneRedis 模拟真实单机 Redis 的拓扑探测响应。
func runSecurityStandaloneRedis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
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

// TestPermissionSQLContainsAllFrontendCodes 验证初始化 SQL 已覆盖前端当前维护的全部权限码。
func TestPermissionSQLContainsAllFrontendCodes(t *testing.T) {
	sqlUUIDSet, _, err := loadPermissionSQLSnapshot()
	if err != nil {
		t.Fatalf("loadPermissionSQLSnapshot() error = %v", err)
	}
	frontendFile := frontendPermissionCodesFilePath()
	if _, statErr := os.Stat(frontendFile); statErr != nil {
		if os.IsNotExist(statErr) {
			t.Skipf("前端权限码文件不存在，跳过联动校验: %s", frontendFile)
		}
		t.Fatalf("Stat(permission-codes.ts) error = %v", statErr)
	}
	frontendCodes, err := loadFrontendPermissionCodes()
	if err != nil {
		t.Fatalf("loadFrontendPermissionCodes() error = %v", err)
	}
	var missing []string
	for _, code := range frontendCodes {
		if _, ok := sqlUUIDSet[code]; !ok {
			missing = append(missing, code)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("database permission SQL missing frontend permission codes: %v", missing)
	}
}

// TestFrontendPermissionCodesAreUnique 验证前端当前维护的权限码不存在“一码多义”重复配置。
func TestFrontendPermissionCodesAreUnique(t *testing.T) {
	frontendFile := frontendPermissionCodesFilePath()
	if _, statErr := os.Stat(frontendFile); statErr != nil {
		if os.IsNotExist(statErr) {
			t.Skipf("前端权限码文件不存在，跳过联动校验: %s", frontendFile)
		}
		t.Fatalf("Stat(permission-codes.ts) error = %v", statErr)
	}
	content, err := os.ReadFile(frontendFile)
	if err != nil {
		t.Fatalf("ReadFile(permission-codes.ts) error = %v", err)
	}
	codePattern := regexp.MustCompile(`'(\d{6})'`)
	matches := codePattern.FindAllStringSubmatch(string(content), -1)
	counts := make(map[string]int, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		counts[match[1]]++
	}
	var duplicates []string
	for code, count := range counts {
		if count > 1 {
			duplicates = append(duplicates, code)
		}
	}
	sort.Strings(duplicates)
	if len(duplicates) > 0 {
		t.Fatalf("frontend permission codes duplicated: %v", duplicates)
	}
}

// TestPermissionSQLContainsRequiredCurrentModules 验证初始化 SQL 已包含当前项目核心路由别名对应的模块权限。
func TestPermissionSQLContainsRequiredCurrentModules(t *testing.T) {
	_, sqlModuleSet, err := loadPermissionSQLSnapshot()
	if err != nil {
		t.Fatalf("loadPermissionSQLSnapshot() error = %v", err)
	}
	requiredModules := []string{
		"admin.list",
		string(routealias.AdminAdd),
		"admin.info",
		string(routealias.AdminUpdate),
		string(routealias.AdminDelete),
		"admin.export",
		string(routealias.AdminPasswordReset),
		string(routealias.AdminResetInitialState),
		string(routealias.AdminMFAStatus),
		string(routealias.AdminBuildMFAURL),
		"role.list",
		"role.tree.list",
		"role.permission.tree",
		"permission.list",
		"permission.tree.list",
		"system.config.list",
		"cache.list",
		"admin.log.query",
		"secretKey.index",
		string(routealias.SecretKeyGet),
		"task.console.index",
		"task.workflow.status.index",
		"task.config.reload.index",
		"task.config.reload.items",
		"task.queue.list",
		"task.items.list",
		"user_tag.index",
		string(routealias.UserList),
		string(routealias.UserAdd),
		string(routealias.UserUpdate),
		string(routealias.UserStatusUpdate),
		string(routealias.UserPasswordReset),
		string(routealias.UserRuntimeSync),
		string(routealias.APIRuntimeConfigReloadStatus),
		string(routealias.APIRuntimeConfigReloadItems),
		string(routealias.APIRuntimeConfigReloadRun),
		"runtime.config.index",
		"runtime.config.list",
		"runtime.config.overview",
		"runtime.config.save",
		"runtime.config.validate",
		"runtime.config.publish",
		"runtime.config.rollback",
		"runtime.config.import",
		"security.debug.index",
	}
	requiredModules = append(requiredModules, string(routealias.DocsIndex), string(routealias.DocsAPIServiceIndex))
	var missing []string
	for _, module := range requiredModules {
		if _, ok := sqlModuleSet[module]; !ok {
			missing = append(missing, module)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("database permission SQL missing required modules: %v", missing)
	}
}

// TestMFAResultByErrorRecognizesWrappedErrors 验证 MFA 业务错误经过 errors.Tag 包装后仍能映射为前端可识别的业务码。
func TestMFAResultByErrorRecognizesWrappedErrors(t *testing.T) {
	// cases 表示不同 MFA 错误转换入口对已包装错误的期望业务码。
	cases := []struct {
		name string           // name 表示测试场景名称。
		got  *types.BizResult // got 表示实际结果。
		want int              // want 表示期望结果。
	}{
		{
			name: "后台敏感操作MFA二次票据过期",
			got:  OperateMFABizResult(errors.Tag(ErrAdminMFATwoStepExpired), "test"),
			want: codes.CheckMFAAgain,
		},
		{
			name: "后台敏感操作Redis不可用",
			got:  OperateMFABizResult(cachelogic.WrapRedisUnavailable(nil, "test"), "test"),
			want: codes.RedisUnavailable,
		},
	}
	for _, item := range cases {
		if item.got == nil {
			t.Fatalf("%s got nil result", item.name)
		}
		if item.got.Code != item.want {
			t.Fatalf("%s code = %d, want %d", item.name, item.got.Code, item.want)
		}
	}
}

// TestPermissionAllowlistContainsSelfServiceRoutes 验证个人中心与会话接口不要求额外业务权限码。
func TestPermissionAllowlistContainsSelfServiceRoutes(t *testing.T) {
	// routeAliases 表示只依赖登录态与账号安全状态的个人中心/会话接口集合。
	routeAliases := []routealias.Alias{
		routealias.AuthRefresh,
		routealias.AuthLogout,
		routealias.AuthCodes,
		routealias.AuthProfile,
		routealias.ProfileMine,
		routealias.ProfileCheckSecure,
		routealias.ProfileCheckMFA,
		routealias.ProfileUpdatePassword,
		routealias.ProfileUpdateMine,
		routealias.ProfileUpdateMFAStatus,
		routealias.ProfileUpdateMFASecret,
		routealias.ProfileRefreshMFASecret,
		routealias.ProfileUpdateAvatar,
	}
	for _, routeAlias := range routeAliases {
		if !permissionAllowlist[routeAlias] {
			t.Fatalf("permissionAllowlist missing %s", routeAlias)
		}
	}
}

// TestPermissionAllowlistContainsSessionVerifyRoutes 验证锁屏解锁校验接口不要求额外业务权限码。
func TestPermissionAllowlistContainsSessionVerifyRoutes(t *testing.T) {
	// routeAliases 表示只依赖登录态与账号状态的会话校验路由别名集合。
	routeAliases := []routealias.Alias{
		routealias.ProfileCheckSecure,
		routealias.ProfileCheckMFA,
	}
	for _, routeAlias := range routeAliases {
		if !permissionAllowlist[routeAlias] {
			t.Fatalf("permissionAllowlist missing %s", routeAlias)
		}
	}
}

// TestCheckRoutePermissionAllowsSelfServiceWithoutPermissionStore 验证个人中心接口不依赖角色/权限缓存即可通过权限层。
func TestCheckRoutePermissionAllowsSelfServiceWithoutPermissionStore(t *testing.T) {
	logicObj := NewSecurityLogic(context.Background(), svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{}))
	// routeAliases 表示不需要查询权限表的自助接口集合。
	routeAliases := []routealias.Alias{
		routealias.ProfileMine,
		routealias.ProfileUpdatePassword,
		routealias.ProfileUpdateMine,
		routealias.ProfileCheckSecure,
		routealias.ProfileCheckMFA,
		routealias.ProfileUpdateMFAStatus,
		routealias.ProfileRefreshMFASecret,
		routealias.ProfileUpdateAvatar,
	}
	for _, routeAlias := range routeAliases {
		allowed, err := logicObj.CheckRoutePermission(999, string(routeAlias))
		if err != nil {
			t.Fatalf("CheckRoutePermission(%s) error = %v", routeAlias, err)
		}
		if !allowed {
			t.Fatalf("CheckRoutePermission(%s) allowed = false, want true", routeAlias)
		}
	}
}

// TestCheckRoutePermissionAllowsMiddlewareIgnoreWithoutPermissionStore 验证通用上传等 Ignore 路由只校验登录态，不查询业务权限表。
func TestCheckRoutePermissionAllowsMiddlewareIgnoreWithoutPermissionStore(t *testing.T) {
	logicObj := NewSecurityLogic(context.Background(), svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{}))
	allowed, err := logicObj.CheckRoutePermission(999, string(routePermissionBypassAlias))
	if err != nil {
		t.Fatalf("CheckRoutePermission(%s) error = %v", routePermissionBypassAlias, err)
	}
	if !allowed {
		t.Fatalf("CheckRoutePermission(%s) allowed = false, want true", routePermissionBypassAlias)
	}
}

// TestPermissionReadsFailWithoutRedis 验证 Redis 未初始化时权限读取直接失败，不绕过缓存查询数据库。
func TestPermissionReadsFailWithoutRedis(t *testing.T) {
	logicObj := NewSecurityLogic(context.Background(), svc.NewServiceContext(config.Config{}, svc.Dependencies{}))
	resource := routealias.DocResource{Site: routealias.DocSiteAPI, Path: "接口文档/前台系统/系统接口.md"}
	checks := []struct {
		name string       // 测试链路名称
		run  func() error // 待执行的权限读取
	}{
		{name: "路由权限", run: func() error {
			_, err := logicObj.CheckRoutePermission(7, "admin.list")
			return err
		}},
		{name: "文档权限", run: func() error {
			_, err := logicObj.CheckDocPermission(7, resource)
			return err
		}},
		{name: "可见文档", run: func() error {
			_, err := logicObj.AllowedDocResources(7)
			return err
		}},
		{name: "权限码", run: func() error {
			_, err := logicObj.UserPermissionUUIDsWithCache(7)
			return err
		}},
	}
	for _, check := range checks {
		if err := check.run(); !errors.Is(err, cachelogic.ErrRedisUnavailable) {
			t.Fatalf("%s error=%v, want ErrRedisUnavailable", check.name, err)
		}
	}
}

// TestCheckPermissionsUseNormalizedCaches 验证路由与文档鉴权命中规范化 Redis 缓存时不依赖数据库。
func TestCheckPermissionsUseNormalizedCaches(t *testing.T) {
	server := runSecurityStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	svcCtx := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client})
	logicObj := NewSecurityLogic(context.Background(), svcCtx)
	base := logicObj.BaseLogic
	ctx := context.Background()
	adminID := 7
	roleID := 3
	routePermissionID := 9
	docPermissionID := 10
	docResource := routealias.DocResource{Site: routealias.DocSiteAPI, Path: "接口文档/前台系统/系统接口.md"}

	if err := client.SAdd(ctx, cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.AdminRoleIDs, adminID)), roleID).Err(); err != nil {
		t.Fatalf("写入管理员角色关系缓存失败: %v", err)
	}
	if err := client.HSet(ctx, cachelogic.TableCachePhysicalKey(base, keys.RoleStatus), strconv.Itoa(roleID), "1").Err(); err != nil {
		t.Fatalf("写入角色状态缓存失败: %v", err)
	}
	if err := client.HSet(ctx, cachelogic.TableCachePhysicalKey(base, keys.RoutePermissionIDs), "admin.list", "8,"+strconv.Itoa(routePermissionID)).Err(); err != nil {
		t.Fatalf("写入路由权限索引失败: %v", err)
	}
	if err := client.SAdd(ctx, cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.RolePermission, roleID)), routePermissionID).Err(); err != nil {
		t.Fatalf("写入角色路由权限关系失败: %v", err)
	}
	if err := client.HSet(ctx, cachelogic.TableCachePhysicalKey(base, keys.DocResourcePermissionID), docResource.Key(), docPermissionID).Err(); err != nil {
		t.Fatalf("写入文档资源索引失败: %v", err)
	}
	if err := client.SAdd(ctx, cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.RoleDocPermission, roleID)), docPermissionID).Err(); err != nil {
		t.Fatalf("写入角色文档权限关系失败: %v", err)
	}
	docPermissionsJSON, err := json.Marshal([]model.AdminDocPermission{{
		ID: docPermissionID, Site: docResource.Site, Path: docResource.Path, Status: 1,
	}})
	if err != nil {
		t.Fatalf("编码文档权限缓存失败: %v", err)
	}
	if err := client.Set(ctx, cachelogic.TableCachePhysicalKey(base, keys.DocPermissionList), docPermissionsJSON, 0).Err(); err != nil {
		t.Fatalf("写入文档权限列表缓存失败: %v", err)
	}

	allowed, err := logicObj.CheckRoutePermission(adminID, "admin.list")
	if err != nil || !allowed {
		t.Fatalf("CheckRoutePermission(admin.list) allowed=%v error=%v", allowed, err)
	}
	allowed, err = logicObj.CheckRoutePermission(adminID, "admin.delete")
	if err != nil || allowed {
		t.Fatalf("CheckRoutePermission(admin.delete) allowed=%v error=%v", allowed, err)
	}
	allowed, err = logicObj.CheckDocPermission(adminID, docResource)
	if err != nil || !allowed {
		t.Fatalf("CheckDocPermission(allowed) allowed=%v error=%v", allowed, err)
	}
	allowed, err = logicObj.CheckDocPermission(adminID, routealias.DocResource{
		Site: routealias.DocSiteAPI,
		Path: "接口文档/前台系统/用户接口.md",
	})
	if err != nil || allowed {
		t.Fatalf("CheckDocPermission(denied) allowed=%v error=%v", allowed, err)
	}
	resources, err := logicObj.AllowedDocResources(adminID)
	if err != nil {
		t.Fatalf("AllowedDocResources() error=%v", err)
	}
	if len(resources) != 1 || resources[0] != docResource {
		t.Fatalf("AllowedDocResources() resources=%v, want [%v]", resources, docResource)
	}
}

// TestAdminForAccessUsesVerifiedSession 验证账号安全前置校验直接复用中间件已读取的会话。
func TestAdminForAccessUsesVerifiedSession(t *testing.T) {
	lastLoginTime := time.Date(2026, 7, 23, 15, 30, 0, 0, time.Local)
	session := &types.AdminSession{
		ID:                7,
		UserName:          "admin007",
		Status:            1,
		MfaStatus:         1,
		NeedResetPassword: 0,
		LastLoginTime:     lastLoginTime.Format(time.DateTime),
		Token:             "token",
	}

	admin, err := adminForAccess(session)
	if err != nil {
		t.Fatalf("adminForAccess() error=%v", err)
	}
	if admin.ID != 7 || admin.Name != "admin007" || admin.Status != 1 || admin.MfaStatus != 1 {
		t.Fatalf("adminForAccess() admin=%+v", admin)
	}
	if admin.LastLoginTime == nil || !admin.LastLoginTime.Equal(lastLoginTime) || admin.MfaSecureKey != adminAccessMFASecretUnknown {
		t.Fatalf("adminForAccess() session state=%+v", admin)
	}
}

// TestCheckPermissionsRejectDisabledRole 验证管理员关系和权限关系仍存在时，禁用角色会统一阻断路由与文档权限。
func TestCheckPermissionsRejectDisabledRole(t *testing.T) {
	server := runSecurityStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	svcCtx := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client})
	logicObj := NewSecurityLogic(context.Background(), svcCtx)
	base := logicObj.BaseLogic
	ctx := context.Background()
	adminID := 7
	roleID := 3
	routePermissionID := 9
	docPermissionID := 10
	resource := routealias.DocResource{Site: routealias.DocSiteAPI, Path: "接口文档/前台系统/系统接口.md"}

	if err := client.SAdd(ctx, cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.AdminRoleIDs, adminID)), roleID).Err(); err != nil {
		t.Fatalf("写入管理员角色关系缓存失败: %v", err)
	}
	if err := client.HSet(ctx, cachelogic.TableCachePhysicalKey(base, keys.RoleStatus), strconv.Itoa(roleID), "0").Err(); err != nil {
		t.Fatalf("写入角色禁用状态失败: %v", err)
	}
	if err := client.HSet(ctx, cachelogic.TableCachePhysicalKey(base, keys.RoutePermissionIDs), "admin.list", routePermissionID).Err(); err != nil {
		t.Fatalf("写入路由权限索引失败: %v", err)
	}
	if err := client.SAdd(ctx, cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.RolePermission, roleID)), routePermissionID).Err(); err != nil {
		t.Fatalf("写入角色路由权限关系失败: %v", err)
	}
	if err := client.HSet(ctx, cachelogic.TableCachePhysicalKey(base, keys.DocResourcePermissionID), resource.Key(), docPermissionID).Err(); err != nil {
		t.Fatalf("写入文档资源索引失败: %v", err)
	}
	if err := client.SAdd(ctx, cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.RoleDocPermission, roleID)), docPermissionID).Err(); err != nil {
		t.Fatalf("写入角色文档权限关系失败: %v", err)
	}
	docPermissionsJSON, err := json.Marshal([]model.AdminDocPermission{{
		ID: docPermissionID, Site: resource.Site, Path: resource.Path, Status: 1,
	}})
	if err != nil {
		t.Fatalf("编码文档权限缓存失败: %v", err)
	}
	if err := client.Set(ctx, cachelogic.TableCachePhysicalKey(base, keys.DocPermissionList), docPermissionsJSON, 0).Err(); err != nil {
		t.Fatalf("写入文档权限列表缓存失败: %v", err)
	}

	if allowed, err := logicObj.CheckRoutePermission(adminID, "admin.list"); err != nil || allowed {
		t.Fatalf("CheckRoutePermission(disabled role) allowed=%v error=%v", allowed, err)
	}
	if allowed, err := logicObj.CheckDocPermission(adminID, resource); err != nil || allowed {
		t.Fatalf("CheckDocPermission(disabled role) allowed=%v error=%v", allowed, err)
	}
	if resources, err := logicObj.AllowedDocResources(adminID); err != nil || len(resources) != 0 {
		t.Fatalf("AllowedDocResources(disabled role) resources=%v error=%v", resources, err)
	}
}

// TestCheckPermissionsUseEnabledSuperRole 验证启用超级角色跳过普通权限匹配。
func TestCheckPermissionsUseEnabledSuperRole(t *testing.T) {
	server := runSecurityStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	svcCtx := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client})
	logicObj := NewSecurityLogic(context.Background(), svcCtx)
	base := logicObj.BaseLogic
	ctx := context.Background()
	adminID := 7
	resource := routealias.DocResource{Site: routealias.DocSiteAPI, Path: "不存在于权限表但格式合法.md"}
	if err := client.SAdd(ctx, cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.AdminRoleIDs, adminID)), corelogic.AdminSuperRoleID).Err(); err != nil {
		t.Fatalf("写入超级管理员角色关系失败: %v", err)
	}
	if err := client.HSet(ctx, cachelogic.TableCachePhysicalKey(base, keys.RoleStatus), strconv.Itoa(corelogic.AdminSuperRoleID), "1").Err(); err != nil {
		t.Fatalf("写入超级管理员角色状态失败: %v", err)
	}
	docPermissionsJSON, err := json.Marshal([]model.AdminDocPermission{{
		ID: 10, Site: routealias.DocSiteAPI, Path: "接口文档/前台系统/系统接口.md", Status: 1,
	}})
	if err != nil {
		t.Fatalf("编码文档权限缓存失败: %v", err)
	}
	if err := client.Set(ctx, cachelogic.TableCachePhysicalKey(base, keys.DocPermissionList), docPermissionsJSON, 0).Err(); err != nil {
		t.Fatalf("写入文档权限列表缓存失败: %v", err)
	}

	allowed, err := logicObj.CheckRoutePermission(adminID, "permission.not_registered")
	if err != nil || !allowed {
		t.Fatalf("CheckRoutePermission(super) allowed=%v error=%v", allowed, err)
	}
	allowed, err = logicObj.CheckDocPermission(adminID, resource)
	if err != nil || !allowed {
		t.Fatalf("CheckDocPermission(super) allowed=%v error=%v", allowed, err)
	}
	resources, err := logicObj.AllowedDocResources(adminID)
	if err != nil {
		t.Fatalf("AllowedDocResources(super) error=%v", err)
	}
	if len(resources) != 1 || resources[0].Path != "接口文档/前台系统/系统接口.md" {
		t.Fatalf("AllowedDocResources(super) resources=%v, want all enabled docs", resources)
	}
}

// TestPermissionAllowlistContainsRoleTreeOptions 验证角色树下拉接口只要求登录态与账号状态，不额外绑定角色管理权限。
func TestPermissionAllowlistContainsRoleTreeOptions(t *testing.T) {
	if !permissionAllowlist[routealias.RoleTreeOptions] {
		t.Fatalf("permissionAllowlist missing role.tree.options")
	}
}

// TestPermissionAllowlistContainsPermissionMaxUUID 验证权限 UUID 预览接口只要求登录态，不额外绑定权限管理权限。
func TestPermissionAllowlistContainsPermissionMaxUUID(t *testing.T) {
	if !permissionAllowlist[routealias.PermissionMaxUUID] {
		t.Fatalf("permissionAllowlist missing permission.max_uuid")
	}
}

// TestPermissionAllowlistContainsPersonalMessageRoutes 验证消息中心属于个人收件箱能力，只依赖登录态和账号安全状态。
func TestPermissionAllowlistContainsPersonalMessageRoutes(t *testing.T) {
	// routeAliases 表示个人消息收发、已读和处理接口集合；这些接口不按后台角色权限码二次授权。
	routeAliases := []routealias.Alias{
		routealias.AdminMessageList,
		routealias.AdminMessageSentList,
		routealias.AdminMessageReceivers,
		routealias.AdminMessageUnreadCount,
		routealias.AdminMessageNotifications,
		routealias.AdminMessageMarkRead,
		routealias.AdminMessageDelete,
		routealias.AdminMessageSend,
		routealias.AdminMessageHandle,
	}
	for _, routeAlias := range routeAliases {
		if !permissionAllowlist[routeAlias] {
			t.Fatalf("permissionAllowlist missing personal message route %s", routeAlias)
		}
	}
}

// TestPasswordResetAllowlistContainsForcedResetFlow 验证首次/强制改密阶段不会拦截个人中心必要接口。
func TestPasswordResetAllowlistContainsForcedResetFlow(t *testing.T) {
	// routeAliases 表示必须改密状态下仍可访问的自助闭环接口集合。
	routeAliases := []routealias.Alias{
		routealias.AuthRefresh,
		routealias.AuthLogout,
		routealias.AuthCodes,
		routealias.AuthProfile,
		routealias.ProfileMine,
		routealias.ProfileCheckSecure,
		routealias.ProfileCheckMFA,
		routealias.ProfileUpdatePassword,
		routealias.ProfileUpdateMine,
		routealias.ProfileUpdateMFAStatus,
		routealias.ProfileUpdateMFASecret,
		routealias.ProfileRefreshMFASecret,
		routealias.ProfileUpdateAvatar,
	}
	for _, routeAlias := range routeAliases {
		if !passwordResetAllowlist[routeAlias] {
			t.Fatalf("passwordResetAllowlist missing %s", routeAlias)
		}
	}
}

// TestLoginMFAAllowlistContainsBindFlow 验证登录 MFA 未完成时，绑定/校验 MFA 的闭环接口不会被自己递归拦截。
func TestLoginMFAAllowlistContainsBindFlow(t *testing.T) {
	// routeAliases 表示登录 MFA 前置拦截期间允许访问的最小接口集合。
	routeAliases := []routealias.Alias{
		routealias.AuthRefresh,
		routealias.AuthLogout,
		routealias.AuthCodes,
		routealias.ProfileCheckMFA,
		routealias.ProfileRefreshMFASecret,
		routealias.ProfileUpdateMFAStatus,
		routealias.ProfileMine,
		routealias.AdminMessageNotifications,
	}
	for _, routeAlias := range routeAliases {
		if !loginMFAAllowlist[routeAlias] {
			t.Fatalf("loginMFAAllowlist missing %s", routeAlias)
		}
	}
}

// TestAdminSensitiveRoutesRemainPermissionProtected 验证后台代操作敏感接口仍必须走权限表，不被个人中心白名单误放行。
func TestAdminSensitiveRoutesRemainPermissionProtected(t *testing.T) {
	// routeAliases 表示管理员管理与后台代操作类敏感接口集合。
	routeAliases := []routealias.Alias{
		routealias.AdminAdd,
		routealias.AdminUpdate,
		routealias.AdminDelete,
		routealias.AdminStatusUpdate,
		routealias.AdminPasswordReset,
		routealias.AdminResetInitialState,
		routealias.AdminMFAStatus,
		routealias.AdminBuildMFAURL,
	}
	for _, routeAlias := range routeAliases {
		if permissionAllowlist[routeAlias] {
			t.Fatalf("permissionAllowlist should not contain sensitive route %s", routeAlias)
		}
		if passwordResetAllowlist[routeAlias] {
			t.Fatalf("passwordResetAllowlist should not contain sensitive route %s", routeAlias)
		}
		if loginMFAAllowlist[routeAlias] {
			t.Fatalf("loginMFAAllowlist should not contain sensitive route %s", routeAlias)
		}
	}
}

// TestCheckAdminNeedResetPassword 验证必须改密状态会拦截非白名单接口，但放行个人中心改密接口。
func TestCheckAdminNeedResetPassword(t *testing.T) {
	logicObj := NewSecurityLogic(context.Background(), svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{}))
	admin := &model.Admin{ID: 8, Name: "admin999", NeedResetPassword: 1}
	if err := logicObj.checkAdminNeedResetPassword(admin, "admin.list"); err != ErrAdminPasswordResetRequired {
		t.Fatalf("checkAdminNeedResetPassword(admin.list) = %v, want %v", err, ErrAdminPasswordResetRequired)
	}
	if err := logicObj.checkAdminNeedResetPassword(admin, string(routealias.ProfileUpdatePassword)); err != nil {
		t.Fatalf("checkAdminNeedResetPassword(profile.update_password) = %v, want nil", err)
	}
	if err := logicObj.checkAdminNeedResetPassword(admin, string(routealias.ProfileUpdateMFAStatus)); err != nil {
		t.Fatalf("checkAdminNeedResetPassword(profile.update_mfa_status) = %v, want nil", err)
	}
	if err := logicObj.checkAdminNeedResetPassword(admin, string(routealias.AdminPasswordReset)); err != ErrAdminPasswordResetRequired {
		t.Fatalf("checkAdminNeedResetPassword(admin.password.reset) = %v, want %v", err, ErrAdminPasswordResetRequired)
	}
}

// TestShouldSkipMFAForPasswordReset 验证必须改密阶段允许白名单路由先跳过登录 MFA 校验。
func TestShouldSkipMFAForPasswordReset(t *testing.T) {
	admin := &model.Admin{ID: 8, Name: "admin999", NeedResetPassword: 1}
	for _, routeAlias := range []routealias.Alias{routealias.ProfileUpdatePassword, routealias.ProfileCheckMFA, routealias.ProfileUpdateMFAStatus} {
		if !shouldSkipMFAForPasswordReset(admin, string(routeAlias)) {
			t.Fatalf("shouldSkipMFAForPasswordReset(%s) = false, want true", routeAlias)
		}
	}
	if shouldSkipMFAForPasswordReset(admin, "admin.list") {
		t.Fatalf("shouldSkipMFAForPasswordReset(admin.list) = true, want false")
	}
	if shouldSkipMFAForPasswordReset(admin, string(routealias.AdminPasswordReset)) {
		t.Fatalf("shouldSkipMFAForPasswordReset(admin.password.reset) = true, want false")
	}
	if shouldSkipMFAForPasswordReset(&model.Admin{ID: 8, Name: "admin999"}, string(routealias.ProfileUpdatePassword)) {
		t.Fatalf("shouldSkipMFAForPasswordReset(non-reset profile.update_password) = true, want false")
	}
}

// TestShouldBypassLoginMFACheck 验证登录后首次绑定 MFA 所需的自助接口可以跳过登录态 MFA 前置拦截。
func TestShouldBypassLoginMFACheck(t *testing.T) {
	admin := &model.Admin{ID: 8, Name: "admin999"}
	for _, routeAlias := range []routealias.Alias{routealias.ProfileCheckMFA, routealias.ProfileRefreshMFASecret, routealias.ProfileUpdateMFAStatus} {
		if !shouldBypassLoginMFACheck(admin, string(routeAlias)) {
			t.Fatalf("shouldBypassLoginMFACheck(%s) = false, want true", routeAlias)
		}
	}
	if shouldBypassLoginMFACheck(admin, "admin.list") {
		t.Fatalf("shouldBypassLoginMFACheck(admin.list) = true, want false")
	}
	if shouldBypassLoginMFACheck(admin, string(routealias.AdminPasswordReset)) {
		t.Fatalf("shouldBypassLoginMFACheck(admin.password.reset) = true, want false")
	}
}

// TestCheckAdminLoginIPRejectsEmptyEnabledWhitelist 验证白名单启用但未配置 IP 时会拒绝登录。
func TestCheckAdminLoginIPRejectsEmptyEnabledWhitelist(t *testing.T) {
	server := runSecurityStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})
	seedBoolSecurityConfig(t, client, ConfigAdminIPWhitelistEnabled, true)
	seedStringSliceSecurityConfig(t, client, ConfigAdminIPWhitelist, "[]")
	logicObj := NewSecurityLogic(context.Background(), svc.NewServiceContext(
		config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client},
	))

	if err := logicObj.CheckAdminLoginIP("127.0.0.1"); !errors.Is(err, ErrAdminIPNotAllowed) {
		t.Fatalf("CheckAdminLoginIP error = %v, want %v", err, ErrAdminIPNotAllowed)
	}
}

// TestForceLoginMFAEnabledFailsWhenConfigUnavailable 验证强制 MFA 配置不可用时不会静默放行。
func TestForceLoginMFAEnabledFailsWhenConfigUnavailable(t *testing.T) {
	if _, err := newTestSecurityLogic().ForceLoginMFAEnabled(); err == nil {
		t.Fatal("ForceLoginMFAEnabled error = nil, want config error")
	}
}

// TestRefreshPermissionRelatedCacheDeletesCoreAndAffectedRoleCaches 验证权限缓存刷新会清理共享索引和受影响角色关系缓存。
func TestRefreshPermissionRelatedCacheDeletesCoreAndAffectedRoleCaches(t *testing.T) {
	server := runSecurityStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	svcCtx := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client})
	base := corelogic.NewBaseLogicWithContext(context.Background(), svcCtx)
	logicObj := &rbaclogic.AdminPermissionLogic{BaseLogic: base}
	ctx := context.Background()
	affectedRoleKey := cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.RolePermission, 7))
	unaffectedRoleKey := cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.RolePermission, 8))

	keysToPrepare := []string{
		cachelogic.TableCachePhysicalKey(base, keys.PermissionTree),
		cachelogic.TableCachePhysicalKey(base, keys.RoutePermissionIDs),
		cachelogic.TableCachePhysicalKey(base, keys.PermissionUUID),
		affectedRoleKey,
	}
	for _, key := range keysToPrepare {
		if key == cachelogic.TableCachePhysicalKey(base, keys.RoutePermissionIDs) || key == cachelogic.TableCachePhysicalKey(base, keys.PermissionUUID) {
			if err := client.HSet(ctx, key, "1", "value").Err(); err != nil {
				t.Fatalf("HSet(%s) error = %v", key, err)
			}
			continue
		}
		if key == affectedRoleKey {
			if err := client.SAdd(ctx, key, "100001").Err(); err != nil {
				t.Fatalf("SAdd(%s) error = %v", key, err)
			}
			continue
		}
		if err := client.Set(ctx, key, "value", 0).Err(); err != nil {
			t.Fatalf("Set(%s) error = %v", key, err)
		}
	}
	if err := client.SAdd(ctx, unaffectedRoleKey, "100002").Err(); err != nil {
		t.Fatalf("SAdd(%s) error = %v", unaffectedRoleKey, err)
	}

	if err := logicObj.RefreshPermissionRelatedCache(7); err != nil {
		t.Fatalf("RefreshPermissionRelatedCache() error = %v", err)
	}

	for _, key := range keysToPrepare {
		if server.Exists(key) {
			t.Fatalf("refreshPermissionRelatedCache() key %s still exists", key)
		}
	}
	if !server.Exists(unaffectedRoleKey) {
		t.Fatalf("refreshPermissionRelatedCache() unrelated role key %s should be kept", unaffectedRoleKey)
	}
}

// TestBusinessLogicDoesNotUseRedisScanOrPrefixDelete 验证业务逻辑不再通过 Redis 扫描或 table-cache 前缀删除处理高基数 key。
func TestBusinessLogicDoesNotUseRedisScanOrPrefixDelete(t *testing.T) {
	root := testFilePath("../../../internal/logic")
	forbidden := []string{"DeleteByPrefix(", "DeletePattern(", "HScan(", "SScan(", "ZScan(", "ForEachMaster(", "scanDeletePattern"}
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return errors.Tag(walkErr)
		}
		if info == nil || info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return errors.Tag(readErr)
		}
		for _, keyword := range forbidden {
			if strings.Contains(string(content), keyword) {
				t.Fatalf("业务逻辑禁止使用Redis前缀/模板删除: file=%s keyword=%s", path, keyword)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Walk(internal/logic) error = %v", err)
	}
}

// TestInvalidateDeletedAdminCacheDeletesSessionAndRoleCaches 验证删除管理员会清理登录态和角色缓存。
func TestInvalidateDeletedAdminCacheDeletesSessionAndRoleCaches(t *testing.T) {
	server := runSecurityStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	svcCtx := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client})
	base := corelogic.NewBaseLogicWithContext(context.Background(), svcCtx)
	ctx := context.Background()
	adminID := 7

	stringKeys := []string{
		keys.AdminSessionRedisKey(adminID),
	}
	setKeys := []string{
		cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.AdminRoleIDs, adminID)),
		cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.AdminRolesDetail, adminID)),
	}
	for _, key := range stringKeys {
		if err := client.Set(ctx, key, "value", 0).Err(); err != nil {
			t.Fatalf("Set(%s) error = %v", key, err)
		}
	}
	for _, key := range setKeys {
		if err := client.SAdd(ctx, key, "value").Err(); err != nil {
			t.Fatalf("SAdd(%s) error = %v", key, err)
		}
	}

	if err := cachelogic.InvalidateDeletedAdminCache(base, adminID); err != nil {
		t.Fatalf("cachelogic.InvalidateDeletedAdminCache() error = %v", err)
	}

	for _, key := range append(stringKeys, setKeys...) {
		if server.Exists(key) {
			t.Fatalf("cachelogic.InvalidateDeletedAdminCache() key %s still exists", key)
		}
	}
}

// TestGetUserPermissionCodesUsesNormalizedCaches 验证权限码查询通过角色关系与共享 UUID 索引完成。
func TestGetUserPermissionCodesUsesNormalizedCaches(t *testing.T) {
	server := runSecurityStandaloneRedis(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	svcCtx := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client})
	ctx := context.Background()

	base := corelogic.NewBaseLogicWithContext(ctx, svcCtx)
	if err := client.SAdd(ctx, cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.AdminRoleIDs, 7)), "3").Err(); err != nil {
		t.Fatalf("SAdd(admin_role_ids:7) error = %v", err)
	}
	if err := client.HSet(ctx, cachelogic.TableCachePhysicalKey(base, keys.RoleStatus), "3", "1").Err(); err != nil {
		t.Fatalf("HSet(role_status) error = %v", err)
	}
	if err := client.SAdd(ctx, cachelogic.TableCachePhysicalKey(base, fmt.Sprintf(keys.RolePermission, 3)), "2").Err(); err != nil {
		t.Fatalf("SAdd(role_permission:3) error = %v", err)
	}
	if err := client.HSet(ctx, cachelogic.TableCachePhysicalKey(base, keys.PermissionUUID), "2", "100002").Err(); err != nil {
		t.Fatalf("HSet(permission_uuid) error = %v", err)
	}

	values, err := (&SecurityLogic{BaseLogic: base}).UserPermissionUUIDsWithCache(7)
	if err != nil {
		t.Fatalf("UserPermissionUUIDsWithCache(7) error = %v", err)
	}
	if len(values) != 1 || values[0] != "100002" {
		t.Fatalf("GetUserPermissionCodes(7) values = %v, want [100002]", values)
	}
}

// loadPermissionSQLSnapshot 读取数据库迁移 SQL 中的权限 UUID 与 module 集合，供权限清单回归测试复用。
func loadPermissionSQLSnapshot() (map[string]struct{}, map[string]struct{}, error) {
	statementPattern := regexp.MustCompile("^\\s*(?:INSERT(?: IGNORE)? INTO `admin_permission` .* VALUES\\s*)?\\((\\d+),\\s*'([^']+)',\\s*'[^']*',\\s*'([^']*)'")
	uuidSet := make(map[string]struct{})
	moduleSet := make(map[string]struct{})
	for _, migration := range database.DefaultMigrations() {
		inPermissionInsert := false
		for _, line := range strings.Split(migration.SQL, "\n") {
			if strings.Contains(line, "INSERT") && strings.Contains(line, "`admin_permission`") {
				inPermissionInsert = true
			}
			if !inPermissionInsert {
				continue
			}
			match := statementPattern.FindStringSubmatch(line)
			if len(match) >= 4 {
				uuidSet[match[2]] = struct{}{}
				if match[3] != "" {
					moduleSet[match[3]] = struct{}{}
				}
			}
			if strings.Contains(line, ";") {
				inPermissionInsert = false
			}
		}
	}
	return uuidSet, moduleSet, nil
}

// loadFrontendPermissionCodes 读取前端常量文件中的全部 6 位权限码，确保 SQL 与前端显隐权限保持一致。
func loadFrontendPermissionCodes() ([]string, error) {
	content, err := os.ReadFile(frontendPermissionCodesFilePath())
	if err != nil {
		return nil, errors.Tag(err)
	}
	codePattern := regexp.MustCompile(`'\d{6}'`)
	matches := codePattern.FindAllString(string(content), -1)
	codeSet := map[string]struct{}{}
	for _, match := range matches {
		codeSet[match[1:len(match)-1]] = struct{}{}
	}
	codes := make([]string, 0, len(codeSet))
	for code := range codeSet {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	return codes, nil
}

// frontendPermissionCodesFilePath 返回前端权限码常量文件路径，供联动测试统一复用。
func frontendPermissionCodesFilePath() string {
	return testFilePath("../../../../gozero-admin-vue/apps/web-antd/src/constants/permission-codes.ts")
}

// testFilePath 基于当前测试文件计算仓库内/相邻仓库文件路径，避免依赖 `go test` 执行目录。
func testFilePath(relativePath string) string {
	_, currentFile, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), relativePath))
}
