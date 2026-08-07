package handler_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	codes "admin/common/codes"
	keys "admin/common/rediskeys"
	"admin/internal/bootstrap"
	"admin/internal/bootstrap/configload"
	corelogic "admin/internal/logic"
	cachelogic "admin/internal/logic/cache"
	secretkeylogic "admin/internal/logic/secretkey"
	securitylogic "admin/internal/logic/security"
	"admin/internal/model"
	"admin/internal/routealias"
	"admin/internal/security"
	"admin/internal/svc"
	"admin/internal/types"

	"github.com/Is999/go-utils/errors"
	"gorm.io/gorm"
)

// roleAdminIntegrationResp 表示接口统一业务响应结构，便于集成测试解码 code/message/data。
type roleAdminIntegrationResp struct {
	Code    int             `json:"code"`    // Code 表示响应业务码。
	Message string          `json:"message"` // Message 表示响应消息。
	Data    json.RawMessage `json:"data"`    // Data 表示响应数据。
}

// roleAdminIntegrationLoginResp 表示登录返回的令牌结构。
type roleAdminIntegrationLoginResp struct {
	Token string `json:"token"` // Token 表示登录令牌。
}

// roleAdminIntegrationCaptchaResp 表示图形验证码返回结构。
type roleAdminIntegrationCaptchaResp struct {
	Key   string `json:"key"`   // Key 表示测试 key。
	Image string `json:"image"` // Image 表示验证码图片。
}

// roleAdminIntegrationPermissionItem 表示权限树节点结构。
type roleAdminIntegrationPermissionItem struct {
	ID       int                                  `json:"id"`       // ID 表示测试记录 ID。
	UUID     string                               `json:"uuid"`     // UUID 表示测试 UUID。
	Module   string                               `json:"module"`   // Module 表示权限模块。
	Status   int                                  `json:"status"`   // Status 表示状态值。
	Checked  bool                                 `json:"checked"`  // Checked 表示权限是否选中。
	Children []roleAdminIntegrationPermissionItem `json:"children"` // Children 表示子节点列表。
}

// roleAdminIntegrationRoleItem 表示角色树节点结构。
type roleAdminIntegrationRoleItem struct {
	ID       int                            `json:"id"`       // ID 表示测试记录 ID。
	Title    string                         `json:"title"`    // Title 表示标题。
	Status   int                            `json:"status"`   // Status 表示状态值。
	Children []roleAdminIntegrationRoleItem `json:"children"` // Children 表示子节点列表。
}

// roleAdminIntegrationPermissionTreeResp 表示角色的两类权限树响应。
type roleAdminIntegrationPermissionTreeResp struct {
	RoutePermissions []roleAdminIntegrationPermissionItem `json:"routePermissions"` // RoutePermissions 表示正常路由权限树。
	Writable         bool                                 `json:"writable"`         // Writable 表示当前角色权限是否允许修改。
}

// roleAdminIntegrationAdminRoleItem 表示管理员已绑定角色项。
type roleAdminIntegrationAdminRoleItem struct {
	ID     int    `json:"id"`     // ID 表示测试记录 ID。
	RoleID int    `json:"roleID"` // RoleID 表示角色 ID。
	Title  string `json:"title"`  // Title 表示标题。
}

// roleAdminIntegrationAdminItem 表示管理员列表项。
type roleAdminIntegrationAdminItem struct {
	ID       int    `json:"id"`       // ID 表示测试记录 ID。
	Username string `json:"username"` // Username 表示用户名。
}

// roleAdminIntegrationClient 封装集成测试 HTTP 客户端和 AES 签名器。
type roleAdminIntegrationClient struct {
	*http.Client                 // Client 发起集成测试 HTTP 请求
	signer       security.Signer // signer 按生产安全策略生成 AES 请求签名
}

const (
	// integrationAppID 表示集成测试读取 AES 密钥时使用的应用 ID。
	integrationAppID = "1"
	// integrationSuperAdminID 表示初始化数据中超级管理员账号的固定 ID。
	integrationSuperAdminID = 1
	// integrationSuperAdminName 表示真实登录流程使用的初始化超级管理员账号。
	integrationSuperAdminName = "super999"
	// integrationConfigEnv 指向完整且已通过启动校验的配置文件；测试会在加载后覆盖基础服务地址。
	integrationConfigEnv = "CRON_ADMIN_TEST_CONFIG"
	// integrationMySQLDSNEnv 指向隔离 MySQL 数据库；测试会创建、修改并删除业务数据。
	integrationMySQLDSNEnv = "CRON_ADMIN_TEST_MYSQL_DSN"
	// integrationRedisAddrEnv 指向隔离 Redis 单机地址；测试会创建并清理登录态与业务缓存。
	integrationRedisAddrEnv = "CRON_ADMIN_TEST_REDIS_ADDR"
	// integrationRedisPasswordEnv 是隔离 Redis 密码；空值表示测试 Redis 未启用认证。
	integrationRedisPasswordEnv = "CRON_ADMIN_TEST_REDIS_PASSWORD"
	// integrationDisposableEnv 必须显式为 1，防止集成测试误连开发或生产基础服务。
	integrationDisposableEnv = "CRON_ADMIN_TEST_DISPOSABLE"
)

const (
	// integrationReplicaWaitTimeout 限制零结果再次查询的等待窗口；单次 HTTP 超时仍由测试客户端控制。
	integrationReplicaWaitTimeout = 3 * time.Second
	// integrationReplicaPollInterval 控制从库可见性重试频率，避免短暂复制延迟造成高频查询。
	integrationReplicaPollInterval = 100 * time.Millisecond
)

// TestRoleAdminIntegrationFlows 验证登录、角色父子权限收敛、状态切换和管理员角色绑定过滤链路。
func TestRoleAdminIntegrationFlows(t *testing.T) {
	configFile := strings.TrimSpace(os.Getenv(integrationConfigEnv))
	testPassword := strings.TrimSpace(os.Getenv("CRON_ADMIN_TEST_PASSWORD"))
	mysqlDSN := strings.TrimSpace(os.Getenv(integrationMySQLDSNEnv))
	redisAddr := strings.TrimSpace(os.Getenv(integrationRedisAddrEnv))
	if configFile == "" || testPassword == "" || mysqlDSN == "" || redisAddr == "" || strings.TrimSpace(os.Getenv(integrationDisposableEnv)) != "1" {
		t.Skipf("未显式启用隔离集成环境；必须同时设置 %s、CRON_ADMIN_TEST_PASSWORD、%s、%s 和 %s=1", integrationConfigEnv, integrationMySQLDSNEnv, integrationRedisAddrEnv, integrationDisposableEnv)
	}
	if _, err := os.Stat(configFile); err != nil {
		t.Fatalf("隔离集成测试配置不存在: %v", err)
	}

	cfg, err := bootstrap.LoadConfig(configFile)
	if err != nil {
		t.Fatalf("读取运行时配置失败: %v", err)
	}
	cfg.Host = "127.0.0.1"
	cfg.Port = integrationFreePort(t)
	cfg.InternalServer.Host = "127.0.0.1"
	cfg.InternalServer.Port = integrationFreePort(t)
	cfg.Task.Enabled = false
	cfg.HotReload.Enabled = false
	cfg.Observability.TraceEnabled = false
	cfg.MySQL.WriteDataSource = mysqlDSN
	cfg.MySQL.ReadDataSources = nil
	cfg.SiteMySQL = nil
	cfg.Redis.Type = "single"
	cfg.Redis.Addrs = []string{redisAddr}
	cfg.Redis.AddrMap = nil
	cfg.Redis.Password = os.Getenv(integrationRedisPasswordEnv)
	cfg.Redis.DB = 0
	cfg.Redis.TLS = false
	cfg.Redis.TLSInsecureSkipVerify = false
	cfg.Kafka.Enabled = false
	cfg.Collector.Enabled = false
	cfg.CDC.Enabled = false
	cfg.Archive.Enabled = false
	cfg.Alert.Lark.Enabled = false
	if err = configload.Validate(cfg); err != nil {
		t.Fatalf("隔离集成测试覆盖基础服务地址后配置校验失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	app, err := bootstrap.New(ctx, cfg, bootstrap.ModeAPI)
	if err != nil {
		t.Fatalf("隔离集成环境依赖初始化失败: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		if stopErr := app.Stop(stopCtx); stopErr != nil {
			t.Errorf("停止应用失败: %v", stopErr)
		}
	})

	startErrCh := make(chan error, 1)
	go func() {
		startErrCh <- app.Start()
	}()

	baseURL := fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port)
	integrationWaitForServer(t, baseURL, startErrCh)

	client := &roleAdminIntegrationClient{
		Client: &http.Client{Timeout: 20 * time.Second},
		signer: integrationNewAESCipher(t, app.ServiceContext),
	}
	superSnapshot, superLoginMessageCursor := integrationAdminLoginSnapshot(t, app.ServiceContext, integrationSuperAdminID, integrationSuperAdminName)
	superToken := ""
	superLoginMessageCount := 0
	// 在发起登录前登记 Cleanup；即使登录响应解析失败，也会恢复超级管理员的密码和最后登录状态。
	t.Cleanup(func() {
		integrationCleanupAdminLogins(t, app.ServiceContext, superSnapshot, superLoginMessageCursor, superLoginMessageCount)
	})
	superToken = integrationLogin(t, client, baseURL, app.ServiceContext, integrationSuperAdminName, testPassword)
	superLoginMessageCount++

	// 先验证登录后初始化和权限码接口可正常返回，确保前端初始化链路可用。
	afterInfoResp := integrationDo(t, client, http.MethodGet, baseURL+"/api/auth/profile", "auth.profile", superToken, nil)
	if afterInfoResp.Code == codes.CheckMFACode || afterInfoResp.Code == codes.CheckMFABind || afterInfoResp.Code == codes.CheckMFAAgain {
		t.Skipf("当前环境登录态要求 MFA 校验，集成测试无法自动完成 MFA，跳过: %s", afterInfoResp.Message)
	}
	if !codes.IsSuccess(afterInfoResp.Code) {
		t.Fatalf("接口返回失败: method=%s url=%s code=%d message=%s", http.MethodGet, baseURL+"/api/auth/profile", afterInfoResp.Code, afterInfoResp.Message)
	}
	var superProfile struct {
		NeedResetPassword int `json:"needResetPassword"` // NeedResetPassword 为 1 时只允许访问强制改密入口。
	}
	if err = json.Unmarshal(afterInfoResp.Data, &superProfile); err != nil {
		t.Fatalf("解析超级管理员资料失败: %v", err)
	}
	if superProfile.NeedResetPassword != 1 {
		t.Fatalf("隔离初始化库的超级管理员必须处于强制改密状态: got=%d want=1", superProfile.NeedResetPassword)
	}
	changedSuperPassword := "PassWord4!"
	if testPassword == changedSuperPassword {
		changedSuperPassword = "PassWord5!"
	}
	integrationMustDo(t, client, http.MethodPatch, baseURL+"/api/profile/password", "profile.update_password", superToken, map[string]any{
		"passwordOld":     testPassword,
		"passwordNew":     changedSuperPassword,
		"confirmPassword": changedSuperPassword,
	}, nil)
	superToken = integrationLogin(t, client, baseURL, app.ServiceContext, integrationSuperAdminName, changedSuperPassword)
	superLoginMessageCount++
	var changedSuperProfile struct {
		NeedResetPassword int `json:"needResetPassword"` // NeedResetPassword 必须在成功改密后清零。
	}
	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/auth/profile", "auth.profile", superToken, nil, &changedSuperProfile)
	if changedSuperProfile.NeedResetPassword != 0 {
		t.Fatalf("超级管理员成功改密后仍被标记为强制改密: got=%d want=0", changedSuperProfile.NeedResetPassword)
	}
	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/auth/codes", "auth.codes", superToken, nil, nil)

	var permissionTree []roleAdminIntegrationPermissionItem
	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/permissions/tree", "permission.tree.list", superToken, nil, &permissionTree)
	var superRolePermissionTree roleAdminIntegrationPermissionTreeResp
	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/roles/permissions/tree/1", "role.permission.tree", superToken, nil, &superRolePermissionTree)
	if superRolePermissionTree.Writable {
		t.Fatal("超级管理员角色权限树不应允许修改")
	}

	parentPermissionIDs := integrationPickPermissionIDs(t, permissionTree, "role.add", "role.update", "role.status.update")
	childPermissionIDs := append([]int(nil), parentPermissionIDs[:2]...)
	removedPermissionID := childPermissionIDs[1]

	suffix := time.Now().UnixNano()
	parentTitle := fmt.Sprintf("自动化父角色-%d", suffix)
	childTitle := fmt.Sprintf("自动化子角色-%d", suffix)
	adminUsername := fmt.Sprintf("ar%d", suffix%1_000_000_000)
	cleanupAdminID := 0
	// 清理函数按随机业务标识回查精确 ID；创建成功后即使测试中断，也不会遗留角色、管理员或关系数据。
	t.Cleanup(func() {
		integrationCleanupRoleAdminData(t, app.ServiceContext, parentTitle, childTitle, adminUsername, cleanupAdminID)
	})

	integrationMustDo(t, client, http.MethodPost, baseURL+"/api/roles", "role.add", superToken, map[string]any{
		"title":       parentTitle,
		"pid":         0,
		"description": "集成测试父角色",
	}, nil)

	var roleTree []roleAdminIntegrationRoleItem
	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/roles/tree", "role.tree.list", superToken, nil, &roleTree)
	parentRole := integrationFindRoleByTitle(t, roleTree, parentTitle)
	integrationMustDo(t, client, http.MethodPatch, fmt.Sprintf("%s/api/roles/permissions/%d", baseURL, parentRole.ID), "role.permission.update", superToken, map[string]any{
		"routePermissionIds": parentPermissionIDs,
		"docPermissionIds":   []int{},
	}, nil)

	integrationMustDo(t, client, http.MethodPost, fmt.Sprintf("%s/api/roles", baseURL), "role.add", superToken, map[string]any{
		"title":       childTitle,
		"pid":         parentRole.ID,
		"description": "集成测试子角色",
	}, nil)

	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/roles/tree", "role.tree.list", superToken, nil, &roleTree)
	parentRole = integrationFindRoleByTitle(t, roleTree, parentTitle)
	childRole := integrationFindRoleByTitle(t, roleTree, childTitle)
	integrationMustDo(t, client, http.MethodPatch, fmt.Sprintf("%s/api/roles/permissions/%d", baseURL, childRole.ID), "role.permission.update", superToken, map[string]any{
		"routePermissionIds": childPermissionIDs,
		"docPermissionIds":   []int{},
	}, nil)
	parentCheckedPermissionIDs := integrationGetCheckedPermissionIDs(t, client, baseURL, superToken, parentRole.ID)
	childCheckedPermissionIDs := integrationGetCheckedPermissionIDs(t, client, baseURL, superToken, childRole.ID)
	parentExpectedPermissionIDs := integrationPermissionClosureIDs(permissionTree, parentPermissionIDs)
	childExpectedPermissionIDs := integrationPermissionClosureIDs(permissionTree, childPermissionIDs)
	if !slices.Equal(parentCheckedPermissionIDs, parentExpectedPermissionIDs) {
		t.Fatalf("父角色初始权限及祖先闭包不符合预期: got=%v want=%v", parentCheckedPermissionIDs, parentExpectedPermissionIDs)
	}
	if !slices.Equal(childCheckedPermissionIDs, childExpectedPermissionIDs) {
		t.Fatalf("子角色初始权限及祖先闭包不符合预期: got=%v want=%v", childCheckedPermissionIDs, childExpectedPermissionIDs)
	}

	// 通过专用授权接口移除父角色权限，校验子角色越权权限会被同步清理。
	updatedParentPermissionIDs := []int{parentPermissionIDs[0], parentPermissionIDs[2]}
	integrationMustDo(t, client, http.MethodPatch, fmt.Sprintf("%s/api/roles/permissions/%d", baseURL, parentRole.ID), "role.permission.update", superToken, map[string]any{
		"routePermissionIds": updatedParentPermissionIDs,
		"docPermissionIds":   []int{},
	}, nil)

	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/roles/tree", "role.tree.list", superToken, nil, &roleTree)
	parentRole = integrationFindRoleByTitle(t, roleTree, parentTitle)
	childRole = integrationFindRoleByTitle(t, roleTree, childTitle)
	childCheckedPermissionIDs = integrationGetCheckedPermissionIDs(t, client, baseURL, superToken, childRole.ID)
	if slices.Contains(childCheckedPermissionIDs, removedPermissionID) {
		t.Fatalf("父角色移除权限后，子角色仍保留越权权限: %d", removedPermissionID)
	}

	// 创建管理员并同时提交父子角色，后端应自动过滤子角色，仅保留父角色绑定。
	addUserTwoStep := integrationIssueMFATwoStep(t, app.ServiceContext, integrationSuperAdminID, securitylogic.MFAScenarioAddUser)
	integrationMustDo(t, client, http.MethodPost, baseURL+"/api/admins", "admin.add", superToken, map[string]any{
		"username":     adminUsername,
		"realName":     "集成测试管理员",
		"password":     "PassWord3!",
		"email":        fmt.Sprintf("%s@example.com", adminUsername),
		"phone":        "13800138000",
		"avatar":       "",
		"description":  "集成测试管理员",
		"roleIDs":      []int{parentRole.ID, childRole.ID},
		"twoStepKey":   addUserTwoStep.Key,
		"twoStepValue": addUserTwoStep.Value,
	}, nil)

	var adminList struct {
		List []roleAdminIntegrationAdminItem `json:"list"` // List 表示列表数据。
	}
	adminListURL := fmt.Sprintf("%s/api/admins?username=%s", baseURL, url.QueryEscape(adminUsername))
	replicaDeadline := time.Now().Add(integrationReplicaWaitTimeout)
	for {
		adminList.List = nil
		integrationMustDo(t, client, http.MethodGet, adminListURL, "admin.list", superToken, nil, &adminList)
		if len(adminList.List) == 1 {
			break
		}
		if len(adminList.List) > 1 {
			t.Fatalf("按唯一用户名查询管理员返回多条记录: got=%d want=1", len(adminList.List))
		}
		if time.Now().After(replicaDeadline) {
			t.Fatalf("新增管理员从库可见性重试窗口 %s 已耗尽: username=%s", integrationReplicaWaitTimeout, adminUsername)
		}
		time.Sleep(integrationReplicaPollInterval)
	}
	adminID := adminList.List[0].ID
	cleanupAdminID = adminID

	var adminRoles []roleAdminIntegrationAdminRoleItem
	integrationMustDo(t, client, http.MethodGet, fmt.Sprintf("%s/api/admins/roles/%d", baseURL, adminID), "admin.role.list", superToken, nil, &adminRoles)
	if len(adminRoles) != 1 {
		t.Fatalf("管理员绑定角色过滤失败: got=%d want=1", len(adminRoles))
	}
	if adminRoles[0].ID != parentRole.ID && adminRoles[0].RoleID != parentRole.ID {
		t.Fatalf("管理员最终绑定角色不是父角色: %+v", adminRoles[0])
	}

	// 覆盖保存管理员角色会对角色数组签名；成功后超级管理员会话必须继续有效。
	editUserTwoStep := integrationIssueMFATwoStep(t, app.ServiceContext, integrationSuperAdminID, securitylogic.MFAScenarioEditUser)
	integrationMustDo(t, client, http.MethodPatch, fmt.Sprintf("%s/api/admins/roles/%d", baseURL, adminID), "admin.role.update", superToken, map[string]any{
		"roleIDs":      []int{parentRole.ID},
		"twoStepKey":   editUserTwoStep.Key,
		"twoStepValue": editUserTwoStep.Value,
	}, nil)
	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/auth/codes", "auth.codes", superToken, nil, nil)

	// 新增管理员首次登录只允许访问强制改密链路，普通业务入口必须返回明确业务码。
	adminToken := integrationLogin(t, client, baseURL, app.ServiceContext, adminUsername, "PassWord3!")
	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/auth/profile", "auth.profile", adminToken, nil, nil)
	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/auth/codes", "auth.codes", adminToken, nil, nil)
	forcedPasswordResp := integrationDo(t, client, http.MethodPost, baseURL+"/api/docs/session", string(routealias.Ignore), adminToken, nil)
	if forcedPasswordResp.Code != codes.CheckPasswordReset {
		t.Fatalf("首次登录访问文档会话未被强制改密拦截: got=%d want=%d message=%s", forcedPasswordResp.Code, codes.CheckPasswordReset, forcedPasswordResp.Message)
	}

	changedPassword := "PassWord4!"
	integrationMustDo(t, client, http.MethodPatch, baseURL+"/api/profile/password", "profile.update_password", adminToken, map[string]any{
		"passwordOld":     "PassWord3!",
		"passwordNew":     changedPassword,
		"confirmPassword": changedPassword,
	}, nil)
	adminToken = integrationLogin(t, client, baseURL, app.ServiceContext, adminUsername, changedPassword)
	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/auth/profile", "auth.profile", adminToken, nil, nil)
	// 改密后文档会话只承载登录凭证，不要求角色同时拥有两个文档站中的任一入口权限。
	integrationMustDo(t, client, http.MethodPost, baseURL+"/api/docs/session", string(routealias.Ignore), adminToken, nil, nil)

	// 最后再验证禁用角色接口和列表状态回写，避免影响前面的“管理员绑定角色”校验。
	integrationMustDo(t, client, http.MethodPatch, fmt.Sprintf("%s/api/roles/status/%d", baseURL, childRole.ID), "role.status.update", superToken, map[string]any{
		"status": 0,
	}, nil)

	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/roles/tree", "role.tree.list", superToken, nil, &roleTree)
	childRole = integrationFindRoleByTitle(t, roleTree, childTitle)
	if childRole.Status != 0 {
		t.Fatalf("子角色状态更新失败: got=%d want=0", childRole.Status)
	}

	// 删除接口本身仍属于真实业务流覆盖；Cleanup 只负责物理清理软删除角色和中断时的残留数据。
	deleteUserTwoStep := integrationIssueMFATwoStep(t, app.ServiceContext, integrationSuperAdminID, securitylogic.MFAScenarioDeleteUser)
	integrationMustDo(t, client, http.MethodDelete, fmt.Sprintf("%s/api/admins/%d", baseURL, adminID), "admin.delete", superToken, map[string]any{
		"twoStepKey":   deleteUserTwoStep.Key,
		"twoStepValue": deleteUserTwoStep.Value,
	}, nil)
	integrationMustDo(t, client, http.MethodDelete, fmt.Sprintf("%s/api/roles/%d", baseURL, childRole.ID), "role.delete", superToken, nil, nil)
	integrationMustDo(t, client, http.MethodDelete, fmt.Sprintf("%s/api/roles/%d", baseURL, parentRole.ID), "role.delete", superToken, nil, nil)

}

// TestIntegrationPermissionClosureIDs 验证集成断言会保留叶子权限并补齐完整祖先路径。
func TestIntegrationPermissionClosureIDs(t *testing.T) {
	tree := []roleAdminIntegrationPermissionItem{{
		ID: 1,
		Children: []roleAdminIntegrationPermissionItem{{
			ID: 2,
			Children: []roleAdminIntegrationPermissionItem{{
				ID: 3,
			}},
		}, {
			ID: 4,
		}},
	}}

	got := integrationPermissionClosureIDs(tree, []int{3, 4})
	want := []int{1, 2, 3, 4}
	if !slices.Equal(got, want) {
		t.Fatalf("权限祖先闭包不符合预期: got=%v want=%v", got, want)
	}
}

// integrationAdminLoginSnapshot 保存超级管理员登录前的持久状态和消息游标，供 Cleanup 精确恢复。
func integrationAdminLoginSnapshot(t *testing.T, svcCtx *svc.ServiceContext, adminID int, adminName string) (model.Admin, int64) {
	t.Helper()
	if svcCtx == nil || svcCtx.WriteDB(svc.DatabaseMain) == nil {
		t.Fatal("读取管理员登录前快照失败: MySQL 写库未初始化")
	}
	writeDB := svcCtx.WriteDB(svc.DatabaseMain)
	var snapshot model.Admin
	if err := writeDB.Session(&gorm.Session{}).Where("id = ? AND name = ?", adminID, adminName).Take(&snapshot).Error; err != nil {
		t.Fatalf("读取管理员登录前快照失败: adminID=%d err=%v", adminID, err)
	}

	var message model.AdminMessage
	err := writeDB.Session(&gorm.Session{}).
		Select("id").
		Where("sender_admin_id = ? AND sender_admin_name = ? AND type = ?", adminID, adminName, types.AdminMessageTypeAdminLogin).
		Order("id DESC").
		Take(&message).Error
	switch {
	case err == nil:
		return snapshot, message.ID
	case errors.Is(err, gorm.ErrRecordNotFound):
		return snapshot, 0
	default:
		t.Fatalf("读取管理员登录消息游标失败: adminID=%d err=%v", adminID, err)
		return model.Admin{}, 0
	}
}

// integrationCleanupAdminLogins 清理本轮登录消息和会话，并恢复超级管理员的密码、强制改密与最后登录字段。
// 该操作仅允许在 CRON_ADMIN_TEST_DISPOSABLE=1 的隔离环境运行；恢复期间不能存在外部并发登录。
func integrationCleanupAdminLogins(t *testing.T, svcCtx *svc.ServiceContext, snapshot model.Admin, messageCursor int64, expectedMessageCount int) {
	t.Helper()
	if svcCtx == nil || snapshot.ID <= 0 {
		t.Error("清理管理员登录副作用失败: ServiceContext 为空")
		return
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	backgroundStopped := true
	if err := svcCtx.StopBackground(cleanupCtx); err != nil {
		backgroundStopped = false
		t.Errorf("等待管理员登录消息写入完成失败: %v", err)
	}

	if backgroundStopped {
		integrationDeleteAdminLoginMessages(t, svcCtx.WriteDB(svc.DatabaseMain), snapshot.ID, snapshot.Name, messageCursor, expectedMessageCount)
	}
	writeDB := svcCtx.WriteDB(svc.DatabaseMain)
	if writeDB == nil {
		t.Error("恢复管理员登录前状态失败: MySQL 写库未初始化")
		return
	}
	updates := map[string]any{
		"password":            snapshot.Password,
		"need_reset_password": snapshot.NeedResetPassword,
		"last_login_time":     snapshot.LastLoginTime,
		"last_login_ip":       snapshot.LastLoginIP,
		"last_login_ipaddr":   snapshot.LastLoginIPAddr,
		"updated_at":          snapshot.UpdatedAt,
	}
	result := writeDB.Session(&gorm.Session{}).Model(&model.Admin{}).
		Where("id = ? AND name = ?", snapshot.ID, snapshot.Name).
		Updates(updates)
	if result.Error != nil || result.RowsAffected != 1 {
		t.Errorf("恢复管理员登录前状态失败: adminID=%d rows=%d err=%v", snapshot.ID, result.RowsAffected, result.Error)
		return
	}
	base := corelogic.NewBaseLogicWithContext(cleanupCtx, svcCtx)
	if err := cachelogic.InvalidateAdminSecurityCache(base, snapshot.ID); err != nil {
		t.Errorf("恢复管理员状态后清理登录态与 MFA 缓存失败: adminID=%d err=%v", snapshot.ID, err)
	}
}

// integrationDeleteAdminLoginMessages 只删除游标后数量与本轮成功登录次数一致的消息；数量不符时拒绝删除。
func integrationDeleteAdminLoginMessages(t *testing.T, writeDB *gorm.DB, adminID int, adminName string, messageCursor int64, expectedMessageCount int) {
	t.Helper()
	if writeDB == nil {
		t.Error("清理管理员登录消息失败: MySQL 写库未初始化")
		return
	}

	var messageIDs []int64
	if err := writeDB.Session(&gorm.Session{}).Model(&model.AdminMessage{}).
		Where("id > ? AND sender_admin_id = ? AND sender_admin_name = ? AND type = ?", messageCursor, adminID, adminName, types.AdminMessageTypeAdminLogin).
		Order("id ASC").
		Pluck("id", &messageIDs).Error; err != nil {
		t.Errorf("查询本轮管理员登录消息失败: adminID=%d err=%v", adminID, err)
		return
	}
	if len(messageIDs) != expectedMessageCount {
		t.Errorf("本轮管理员登录消息数量不符合已完成登录次数，拒绝删除: adminID=%d cursor=%d candidates=%d expected=%d", adminID, messageCursor, len(messageIDs), expectedMessageCount)
		return
	}
	if len(messageIDs) == 0 {
		return
	}

	if err := writeDB.Session(&gorm.Session{}).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("message_id IN ?", messageIDs).Delete(&model.AdminMessageReceiver{}).Error; err != nil {
			return errors.Wrap(err, "删除管理员登录消息收件关系失败")
		}
		result := tx.Where("id IN ? AND sender_admin_id = ? AND sender_admin_name = ? AND type = ?", messageIDs, adminID, adminName, types.AdminMessageTypeAdminLogin).
			Delete(&model.AdminMessage{})
		if result.Error != nil {
			return errors.Wrap(result.Error, "删除管理员登录消息失败")
		}
		if result.RowsAffected != int64(expectedMessageCount) {
			return errors.Errorf("删除管理员登录消息影响行数异常: messageIDs=%v rows=%d expected=%d", messageIDs, result.RowsAffected, expectedMessageCount)
		}
		return nil
	}); err != nil {
		t.Errorf("清理管理员登录消息失败: adminID=%d messageIDs=%v err=%v", adminID, messageIDs, err)
	}
}

// integrationCleanupRoleAdminData 硬删除本轮随机标识对应的管理员、角色和关系，并精确失效相关缓存。
// 生产删除接口保留角色软删除审计语义；集成测试必须物理移除随机数据，避免重复执行污染真实环境。
func integrationCleanupRoleAdminData(t *testing.T, svcCtx *svc.ServiceContext, parentTitle, childTitle, adminUsername string, knownAdminID int) {
	t.Helper()
	if svcCtx == nil {
		t.Error("清理角色管理员集成测试数据失败: ServiceContext 为空")
		return
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Cleanup 也会在 Fatal/Skip 后运行；先阻止新短后台任务并等待已登记任务落库，避免清理后又写回测试消息。
	if err := svcCtx.StopBackground(cleanupCtx); err != nil {
		t.Errorf("等待集成测试短后台任务完成失败: %v", err)
		return
	}
	base := corelogic.NewBaseLogicWithContext(cleanupCtx, svcCtx)
	if base == nil || base.Svc == nil {
		t.Error("清理角色管理员集成测试数据失败: BaseLogic 未初始化")
		return
	}
	writeDB := base.Svc.WriteDB(svc.DatabaseMain)
	if writeDB == nil {
		t.Error("清理角色管理员集成测试数据失败: MySQL 写库未初始化")
		return
	}

	var roles []model.AdminRole
	if err := writeDB.Session(&gorm.Session{}).
		Where("title IN ?", []string{parentTitle, childTitle}).
		Find(&roles).Error; err != nil {
		t.Errorf("查询待清理测试角色失败: %v", err)
		return
	}
	roleIDs := make([]int, 0, len(roles))
	for _, role := range roles {
		roleIDs = append(roleIDs, role.ID)
	}
	roleIDs = types.UniquePositiveInts(roleIDs)

	var adminRow model.Admin
	adminID := knownAdminID
	adminExists := false
	adminErr := writeDB.Session(&gorm.Session{}).
		Where("name = ?", adminUsername).
		Take(&adminRow).Error
	switch {
	case adminErr == nil:
		adminID = adminRow.ID
		adminExists = true
	case errors.Is(adminErr, gorm.ErrRecordNotFound):
	case adminErr != nil:
		t.Errorf("查询待清理测试管理员失败: %v", adminErr)
		return
	}

	err := writeDB.Session(&gorm.Session{}).Transaction(func(tx *gorm.DB) error {
		if adminID > 0 {
			var messageIDs []int64
			if err := tx.Model(&model.AdminMessage{}).
				Where("sender_admin_id = ? AND type = ?", adminID, types.AdminMessageTypeAdminLogin).
				Pluck("id", &messageIDs).Error; err != nil {
				return errors.Wrap(err, "查询测试管理员登录消息失败")
			}
			if len(messageIDs) > 0 {
				if err := tx.Where("message_id IN ?", messageIDs).Delete(&model.AdminMessageReceiver{}).Error; err != nil {
					return errors.Wrap(err, "删除测试管理员登录消息收件关系失败")
				}
				if err := tx.Where("id IN ? AND sender_admin_id = ?", messageIDs, adminID).Delete(&model.AdminMessage{}).Error; err != nil {
					return errors.Wrap(err, "删除测试管理员登录消息失败")
				}
			}
			if err := tx.Where("user_id = ?", adminID).Delete(&model.AdminRoleRel{}).Error; err != nil {
				return errors.Wrap(err, "删除测试管理员角色关系失败")
			}
			if adminExists {
				if err := tx.Where("id = ? AND name = ?", adminID, adminUsername).Delete(&model.Admin{}).Error; err != nil {
					return errors.Wrap(err, "删除测试管理员失败")
				}
			}
		}
		if len(roleIDs) == 0 {
			return nil
		}
		if err := tx.Where("role_id IN ?", roleIDs).Delete(&model.AdminRoleRel{}).Error; err != nil {
			return errors.Wrap(err, "删除测试角色管理员关系失败")
		}
		if err := tx.Where("role_id IN ?", roleIDs).Delete(&model.AdminRolePermissionRel{}).Error; err != nil {
			return errors.Wrap(err, "删除测试角色路由权限关系失败")
		}
		if err := tx.Where("role_id IN ?", roleIDs).Delete(&model.AdminRoleDocPermissionRel{}).Error; err != nil {
			return errors.Wrap(err, "删除测试角色文档权限关系失败")
		}
		if err := tx.Where("id IN ? AND title IN ?", roleIDs, []string{parentTitle, childTitle}).Delete(&model.AdminRole{}).Error; err != nil {
			return errors.Wrap(err, "删除测试角色失败")
		}
		return nil
	})
	if err != nil {
		t.Errorf("清理角色管理员集成测试数据失败: %v", err)
		return
	}

	if adminID > 0 {
		if err := cachelogic.InvalidateDeletedAdminCache(base, adminID); err != nil {
			t.Errorf("清理测试管理员缓存失败: %v", err)
		}
	}
	roleCacheKeys := []string{keys.RoleTree, keys.RoleStatus}
	for _, roleID := range roleIDs {
		roleCacheKeys = append(roleCacheKeys,
			fmt.Sprintf(keys.RolePermission, roleID),
			fmt.Sprintf(keys.RoleDocPermission, roleID),
		)
	}
	if err := cachelogic.DeleteTableCacheKeysExact(
		base,
		"integrationCleanupRoleAdminData 清理测试角色缓存",
		cachelogic.TableCachePhysicalKeys(base, roleCacheKeys...),
	); err != nil {
		t.Errorf("清理测试角色缓存失败: %v", err)
	}
}

// integrationFreePort 申请一个本地空闲端口，避免和开发中的服务端口冲突。
func integrationFreePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("申请空闲端口失败: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// integrationWaitForServer 轮询等待测试服务启动成功。
func integrationWaitForServer(t *testing.T, baseURL string, startErrCh <-chan error) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-startErrCh:
			if err != nil {
				t.Fatalf("启动测试服务失败: %v", err)
			}
			t.Fatalf("测试服务过早退出")
		default:
		}

		resp, err := client.Get(baseURL + "/health")
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("等待测试服务启动超时: %s", baseURL)
}

// integrationMFATwoStepTicket 表示测试里直接签发的 MFA 二次校验票据。
type integrationMFATwoStepTicket struct {
	Key   string // Key 表示测试 key。
	Value string // Value 表示字段值。
}

// integrationIssueMFATwoStep 为指定管理员直接签发指定场景的 MFA 二次票据，避免集成测试依赖固定 TOTP 秘钥。
func integrationIssueMFATwoStep(t *testing.T, svcCtx *svc.ServiceContext, adminID int, scenario int) integrationMFATwoStepTicket {
	t.Helper()
	if svcCtx == nil {
		t.Fatalf("签发 MFA 二次票据失败: service context 为空")
	}
	twoStep, err := securitylogic.NewSecurityLogic(context.Background(), svcCtx).IssueMFATwoStepTicket(adminID, scenario)
	if err != nil {
		t.Fatalf("签发 MFA 二次票据失败: adminID=%d scenario=%d err=%v", adminID, scenario, err)
	}
	if twoStep == nil || strings.TrimSpace(twoStep.Key) == "" || strings.TrimSpace(twoStep.Value) == "" {
		t.Fatalf("签发 MFA 二次票据返回为空: adminID=%d scenario=%d resp=%+v", adminID, scenario, twoStep)
	}
	return integrationMFATwoStepTicket{
		Key:   twoStep.Key,
		Value: twoStep.Value,
	}
}

// integrationLogin 通过验证码登录接口获取访问令牌。
// 注意：auth.login 响应可能包含加密后的 token 字段，集成测试需要在本地解密后再作为 Bearer token 使用。
func integrationLogin(t *testing.T, client *roleAdminIntegrationClient, baseURL string, svcCtx *svc.ServiceContext, username string, password string) string {
	t.Helper()

	var captcha roleAdminIntegrationCaptchaResp
	integrationMustDo(t, client, http.MethodGet, baseURL+"/api/auth/captcha", "auth.captcha", "", nil, &captcha)
	cacheKey := keys.WithPrefix(fmt.Sprintf(keys.LoginCaptcha, captcha.Key))
	code, err := svcCtx.Rds.Get(context.Background(), cacheKey).Result()
	if err != nil {
		t.Fatalf("读取集成测试验证码失败: %v", err)
	}

	var loginResp roleAdminIntegrationLoginResp
	integrationMustDo(t, client, http.MethodPost, baseURL+"/api/auth/login", "auth.login", "", map[string]any{
		"username": username,
		"password": password,
		"key":      captcha.Key,
		"captcha":  code,
	}, &loginResp)
	if strings.TrimSpace(loginResp.Token) == "" {
		t.Fatalf("登录成功但返回 token 为空: username=%s", username)
	}
	return integrationNormalizeBearerToken(t, svcCtx, loginResp.Token)
}

// integrationNormalizeBearerToken 把登录响应中的 token 统一转换为可直接用于 Authorization 的 JWT。
// 当 token 已经是 JWT 时直接返回；否则按当前默认加密算法（AES）解密。
func integrationNormalizeBearerToken(t *testing.T, svcCtx *svc.ServiceContext, token string) string {
	t.Helper()

	token = strings.TrimSpace(token)
	if token == "" {
		return ""
	}
	if integrationLooksLikeJWT(token) {
		return token
	}
	if svcCtx == nil {
		t.Fatalf("登录 token 需要解密但 ServiceContext 为空")
	}
	cryptor := integrationNewAESCipher(t, svcCtx)
	plain, err := cryptor.Decrypt(token)
	if err != nil {
		t.Fatalf("登录 token 解密失败: %v", err)
	}
	if !integrationLooksLikeJWT(plain) {
		t.Fatalf("登录 token 解密后仍不是 JWT: %s", plain)
	}
	return plain
}

// integrationNewAESCipher 使用当前应用秘钥创建集成测试 AES 签名和加解密实现。
func integrationNewAESCipher(t *testing.T, svcCtx *svc.ServiceContext) *security.AESCipher {
	t.Helper()
	if svcCtx == nil {
		t.Fatal("初始化 AES 实现失败: ServiceContext 为空")
	}
	aesKey, _, err := secretkeylogic.NewSecretKeyLogic(context.Background(), svcCtx).GetAESKey(integrationAppID, "", "")
	if err != nil || aesKey == nil {
		t.Fatalf("读取 AES Key 失败: %v", err)
	}
	cipherObj, err := security.NewAESCipher(aesKey.Key, aesKey.IV)
	if err != nil {
		t.Fatalf("初始化 AES 实现失败: %v", err)
	}
	return cipherObj
}

// integrationLooksLikeJWT 判断字符串是否形如 `header.payload.signature` 的 JWT 结构。
func integrationLooksLikeJWT(token string) bool {
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	return parts[0] != "" && parts[1] != "" && parts[2] != ""
}

// integrationShouldSign 返回集成测试辅助数据。
func integrationShouldSign(alias string) bool {
	alias = strings.TrimSpace(alias)
	if alias == "" || strings.EqualFold(alias, "ignore") {
		return false
	}
	policy := security.PolicyByRoute(alias)
	return policy.RequestSign != nil || policy.ResponseSign != nil
}

// integrationAppHeader 返回集成测试辅助数据。
func integrationAppHeader() string {
	return base64.StdEncoding.EncodeToString([]byte(integrationAppID))
}

// integrationTraceID 返回集成测试辅助数据。
func integrationTraceID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// integrationTimestamp 返回集成测试辅助数据。
func integrationTimestamp() string {
	return fmt.Sprintf("%d", time.Now().Unix())
}

// integrationSignValue 使用 AES 签名器生成集成测试请求签名。
func integrationSignValue(t *testing.T, signer security.Signer, signText string) string {
	t.Helper()
	if signer == nil {
		t.Fatal("生成集成测试签名失败: 签名器为空")
	}
	sign, err := signer.Sign(signText)
	if err != nil {
		t.Fatalf("生成集成测试签名失败: %v", err)
	}
	return sign
}

// integrationAttachSignature 为集成测试请求参数附加 AES 签名。
func integrationAttachSignature(t *testing.T, signer security.Signer, alias string, payload map[string]any, traceID string, timestamp string) map[string]any {
	t.Helper()
	next := make(map[string]any, len(payload)+1)
	for k, v := range payload {
		next[k] = v
	}
	policy := security.PolicyByRoute(alias)
	signText := security.BuildSignString(next, policy.RequestSign, traceID, timestamp, integrationAppID)
	next["sign"] = integrationSignValue(t, signer, signText)
	return next
}

// integrationMustDo 发起一次接口请求，并断言业务响应为成功。
func integrationMustDo(t *testing.T, client *roleAdminIntegrationClient, method string, urlText string, alias string, token string, payload any, out any) {
	t.Helper()
	signEnabled := integrationShouldSign(alias)
	traceID := ""
	timestamp := ""

	payloadMap := map[string]any{}
	if payload != nil {
		candidate, ok := payload.(map[string]any)
		if !ok {
			t.Fatalf("请求参数必须是 map[string]any: got=%T", payload)
		}
		for k, v := range candidate {
			payloadMap[k] = v
		}
	}

	parsedURL, err := url.Parse(urlText)
	if err != nil {
		t.Fatalf("解析请求地址失败: %v", err)
	}

	queryCarrier := method == http.MethodGet || method == http.MethodDelete
	queryParams := parsedURL.Query()
	signParams := map[string]any{}
	for k, values := range queryParams {
		if len(values) == 0 {
			continue
		}
		signParams[k] = values[len(values)-1]
	}
	if queryCarrier {
		for k, v := range payloadMap {
			queryParams.Set(k, fmt.Sprint(v))
			signParams[k] = v
		}
	}
	if signEnabled {
		traceID = integrationTraceID()
		timestamp = integrationTimestamp()
		// GET/DELETE 走 query 参数承载签名；POST/PUT/PATCH 走 body 承载签名。
		// 这里必须使用“最终待提交的业务参数”参与签名，避免出现“签名只覆盖空参数”的误判。
		if queryCarrier {
			signed := integrationAttachSignature(t, client.signer, alias, signParams, traceID, timestamp)
			for k, v := range signed {
				queryParams.Set(k, fmt.Sprint(v))
			}
		} else {
			payloadMap = integrationAttachSignature(t, client.signer, alias, payloadMap, traceID, timestamp)
		}
	}
	parsedURL.RawQuery = queryParams.Encode()

	var bodyReader io.Reader
	if queryCarrier {
		bodyReader = bytes.NewReader(nil)
	} else {
		if payload == nil {
			bodyReader = bytes.NewReader(nil)
		} else {
			raw, err := json.Marshal(payloadMap)
			if err != nil {
				t.Fatalf("序列化请求参数失败: %v", err)
			}
			bodyReader = bytes.NewReader(raw)
		}
	}

	req, err := http.NewRequest(method, parsedURL.String(), bodyReader)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if signEnabled {
		req.Header.Set("X-App-Id", integrationAppHeader())
		req.Header.Set("X-Trace-Id", traceID)
		req.Header.Set("X-Timestamp", timestamp)
		req.Header.Set("X-Signature", security.SignatureTypeAES)
	}
	if strings.TrimSpace(token) != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("调用接口失败: method=%s url=%s err=%v", method, parsedURL.String(), err)
	}
	defer resp.Body.Close()
	t.Logf("api call: method=%s alias=%s path=%s status=%d latency=%s", method, alias, parsedURL.Path, resp.StatusCode, time.Since(start))

	var bizResp roleAdminIntegrationResp
	if err = json.NewDecoder(resp.Body).Decode(&bizResp); err != nil {
		t.Fatalf("解析响应失败: method=%s url=%s err=%v", method, parsedURL.String(), err)
	}
	if !codes.IsSuccess(bizResp.Code) {
		t.Fatalf("接口返回失败: method=%s url=%s code=%d message=%s", method, parsedURL.String(), bizResp.Code, bizResp.Message)
	}
	if out == nil || len(bizResp.Data) == 0 || string(bizResp.Data) == "null" {
		return
	}
	if err = json.Unmarshal(bizResp.Data, out); err != nil {
		t.Fatalf("解析业务数据失败: method=%s url=%s err=%v data=%s", method, parsedURL.String(), err, string(bizResp.Data))
	}
}

// integrationDo 发起一次接口请求并返回业务响应结构。
// 该方法主要用于集成测试的“环境探测”场景：部分环境可能强制 MFA 校验或存在运维限流，
// 这类场景不适合直接用 integrationMustDo 做强断言。
func integrationDo(t *testing.T, client *roleAdminIntegrationClient, method string, urlText string, alias string, token string, payload any) roleAdminIntegrationResp {
	t.Helper()

	signEnabled := integrationShouldSign(alias)
	traceID := ""
	timestamp := ""

	payloadMap := map[string]any{}
	if payload != nil {
		candidate, ok := payload.(map[string]any)
		if !ok {
			t.Fatalf("请求参数必须是 map[string]any: got=%T", payload)
		}
		for k, v := range candidate {
			payloadMap[k] = v
		}
	}

	parsedURL, err := url.Parse(urlText)
	if err != nil {
		t.Fatalf("解析请求地址失败: %v", err)
	}

	queryCarrier := method == http.MethodGet || method == http.MethodDelete
	queryParams := parsedURL.Query()
	signParams := map[string]any{}
	for k, values := range queryParams {
		if len(values) == 0 {
			continue
		}
		signParams[k] = values[len(values)-1]
	}
	if queryCarrier {
		for k, v := range payloadMap {
			queryParams.Set(k, fmt.Sprint(v))
			signParams[k] = v
		}
	}
	if signEnabled {
		traceID = integrationTraceID()
		timestamp = integrationTimestamp()
		if queryCarrier {
			signed := integrationAttachSignature(t, client.signer, alias, signParams, traceID, timestamp)
			for k, v := range signed {
				queryParams.Set(k, fmt.Sprint(v))
			}
		} else {
			payloadMap = integrationAttachSignature(t, client.signer, alias, payloadMap, traceID, timestamp)
		}
	}
	parsedURL.RawQuery = queryParams.Encode()

	var bodyReader io.Reader
	if queryCarrier {
		bodyReader = bytes.NewReader(nil)
	} else {
		if payload == nil {
			bodyReader = bytes.NewReader(nil)
		} else {
			raw, err := json.Marshal(payloadMap)
			if err != nil {
				t.Fatalf("序列化请求参数失败: %v", err)
			}
			bodyReader = bytes.NewReader(raw)
		}
	}

	req, err := http.NewRequest(method, parsedURL.String(), bodyReader)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if signEnabled {
		req.Header.Set("X-App-Id", integrationAppHeader())
		req.Header.Set("X-Trace-Id", traceID)
		req.Header.Set("X-Timestamp", timestamp)
		req.Header.Set("X-Signature", security.SignatureTypeAES)
	}
	if strings.TrimSpace(token) != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("调用接口失败: method=%s url=%s err=%v", method, parsedURL.String(), err)
	}
	defer resp.Body.Close()
	t.Logf("api call: method=%s alias=%s path=%s status=%d latency=%s", method, alias, parsedURL.Path, resp.StatusCode, time.Since(start))

	var bizResp roleAdminIntegrationResp
	if err = json.NewDecoder(resp.Body).Decode(&bizResp); err != nil {
		t.Fatalf("解析响应失败: method=%s url=%s err=%v", method, parsedURL.String(), err)
	}
	return bizResp
}

// integrationPickPermissionIDs 按模块名挑选测试所需的权限 ID。
func integrationPickPermissionIDs(t *testing.T, tree []roleAdminIntegrationPermissionItem, modules ...string) []int {
	t.Helper()
	moduleToID := make(map[string]int, len(modules))
	var walk func(items []roleAdminIntegrationPermissionItem)
	walk = func(items []roleAdminIntegrationPermissionItem) {
		for _, item := range items {
			if item.Status == 1 && item.Module != "" {
				moduleToID[item.Module] = item.ID
			}
			walk(item.Children)
		}
	}
	walk(tree)

	ids := make([]int, 0, len(modules))
	for _, module := range modules {
		id := moduleToID[module]
		if id <= 0 {
			t.Fatalf("权限树中未找到模块: %s", module)
		}
		ids = append(ids, id)
	}
	return ids
}

// integrationPermissionClosureIDs 根据权限树补齐已提交权限的祖先 ID，匹配生产授权保存语义。
func integrationPermissionClosureIDs(tree []roleAdminIntegrationPermissionItem, selectedIDs []int) []int {
	selected := make(map[int]struct{}, len(selectedIDs))
	for _, permissionID := range types.UniquePositiveInts(selectedIDs) {
		selected[permissionID] = struct{}{}
	}
	closure := make(map[int]struct{}, len(selected))
	var walk func(items []roleAdminIntegrationPermissionItem, ancestors []int)
	walk = func(items []roleAdminIntegrationPermissionItem, ancestors []int) {
		for _, item := range items {
			path := append(append([]int(nil), ancestors...), item.ID)
			if _, ok := selected[item.ID]; ok {
				for _, permissionID := range path {
					if permissionID > 0 {
						closure[permissionID] = struct{}{}
					}
				}
			}
			walk(item.Children, path)
		}
	}
	walk(tree, nil)

	result := make([]int, 0, len(closure))
	for permissionID := range closure {
		result = append(result, permissionID)
	}
	slices.Sort(result)
	return result
}

// integrationGetCheckedPermissionIDs 读取角色权限树里当前已勾选的权限 ID。
func integrationGetCheckedPermissionIDs(t *testing.T, client *roleAdminIntegrationClient, baseURL string, token string, roleID int) []int {
	t.Helper()
	var tree roleAdminIntegrationPermissionTreeResp
	integrationMustDo(t, client, http.MethodGet, fmt.Sprintf("%s/api/roles/permissions/tree/%d", baseURL, roleID), "role.permission.tree", token, nil, &tree)
	if !tree.Writable {
		t.Fatalf("角色 ID[%d]权限树应允许当前超级管理员修改", roleID)
	}

	ids := make([]int, 0, 16)
	var walk func(items []roleAdminIntegrationPermissionItem)
	walk = func(items []roleAdminIntegrationPermissionItem) {
		for _, item := range items {
			if item.Checked {
				ids = append(ids, item.ID)
			}
			walk(item.Children)
		}
	}
	walk(tree.RoutePermissions)
	slices.Sort(ids)
	return ids
}

// integrationFindRoleByTitle 按标题在角色树中查找目标角色。
func integrationFindRoleByTitle(t *testing.T, tree []roleAdminIntegrationRoleItem, title string) roleAdminIntegrationRoleItem {
	t.Helper()
	var walk func(items []roleAdminIntegrationRoleItem) *roleAdminIntegrationRoleItem
	walk = func(items []roleAdminIntegrationRoleItem) *roleAdminIntegrationRoleItem {
		for _, item := range items {
			if item.Title == title {
				return new(item)
			}
			if matched := walk(item.Children); matched != nil {
				return matched
			}
		}
		return nil
	}
	matched := walk(tree)
	if matched == nil {
		t.Fatalf("角色树中未找到角色: %s", title)
	}
	return *matched
}
