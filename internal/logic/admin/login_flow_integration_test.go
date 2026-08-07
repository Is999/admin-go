//go:build integration

package admin

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"admin/common/codes"
	keys "admin/common/rediskeys"
	"admin/common/runtimecfg"
	"admin/internal/config"
	corelogic "admin/internal/logic"
	securitylogic "admin/internal/logic/security"
	"admin/internal/model"
	"admin/internal/svc"
	"admin/internal/types"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestLoginRemovesCreatedSessionWhenDatabaseUpdateFails 在真实 MySQL 上验证登录信息提交失败后只清理本次新建会话。
func TestLoginRemovesCreatedSessionWhenDatabaseUpdateFails(t *testing.T) {
	db := openAdminLoginIntegrationDB(t)
	server := miniredis.RunT(t)
	rds := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rds.Close() })
	cfg := config.Config{
		AppID:        "site-a",
		AppKey:       "admin-login-flow-test-key",
		JwtSecret:    "admin-login-flow-test-jwt-secret",
		JwtExpiresIn: 3600,
	}
	previous := runtimecfg.Get()
	runtimecfg.Set(cfg)
	t.Cleanup(func() { runtimecfg.Restore(previous) })
	svcCtx := svc.NewServiceContext(cfg, svc.Dependencies{
		SiteDBs: svc.SiteDatabases{MainDB: db},
		Rds:     rds,
	})
	seedAdminLoginSecurityConfig(t, rds, securitylogic.ConfigAdminIPWhitelistEnabled, false)
	seedAdminLoginSecurityConfig(t, rds, securitylogic.ConfigAdminMFACheckEnable, false)

	passwordHash, err := bcrypt.GenerateFromPassword([]byte("P@ssw0rd!"), bcrypt.MinCost)
	if err != nil {
		t.Fatalf("生成测试密码哈希失败: %v", err)
	}
	admin := &model.Admin{
		ID:                7,
		Name:              "login_failure_admin",
		RealName:          "登录失败测试",
		Password:          string(passwordHash),
		NeedResetPassword: 0,
		Status:            1,
		LastLoginTime:     time.Unix(1_700_000_000, 0),
		CreatedAt:         time.Unix(1_700_000_000, 0),
		UpdatedAt:         time.Unix(1_700_000_000, 0),
	}
	if err = db.Create(admin).Error; err != nil {
		t.Fatalf("创建测试管理员失败: %v", err)
	}

	const callbackName = "test:reject_admin_login_update"
	if err = db.Callback().Update().Before("gorm:update").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement.Table == model.TableNameAdmin {
			tx.AddError(fmt.Errorf("injected admin login update failure"))
		}
	}); err != nil {
		t.Fatalf("注册登录更新失败回调失败: %v", err)
	}
	t.Cleanup(func() { _ = db.Callback().Update().Remove(callbackName) })

	logicObj := &AdminLogic{BaseLogic: corelogic.NewBaseLogicWithContext(context.Background(), svcCtx)}
	result := logicObj.Login(&types.LoginReq{
		Username: admin.Name,
		Password: "P@ssw0rd!",
		IP:       "127.0.0.1",
	})
	if result == nil || result.Code != codes.DBError {
		t.Fatalf("Login() = %+v, want code=%d", result, codes.DBError)
	}
	if server.Exists(keys.AdminSessionRedisKey(admin.ID)) {
		t.Fatal("数据库更新失败后仍遗留本次登录创建的管理员会话")
	}
}

// openAdminLoginIntegrationDB 连接隔离 MySQL，并创建只服务于本测试的最小管理员表。
func openAdminLoginIntegrationDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("INTEGRATION_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("INTEGRATION_MYSQL_DSN 未配置，跳过管理员登录真实 MySQL 补偿测试")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("连接管理员登录集成 MySQL 失败: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("获取管理员登录集成 MySQL 连接失败: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err = db.Migrator().DropTable(&model.Admin{}); err != nil {
		t.Fatalf("清理管理员登录集成测试表失败: %v", err)
	}
	t.Cleanup(func() { _ = db.Migrator().DropTable(&model.Admin{}) })
	if err = db.AutoMigrate(&model.Admin{}); err != nil {
		t.Fatalf("创建管理员登录集成测试表失败: %v", err)
	}
	return db
}

// seedAdminLoginSecurityConfig 写入登录链路读取的布尔安全配置，避免测试回源数据库。
func seedAdminLoginSecurityConfig(t *testing.T, rds redis.UniversalClient, uuid string, enabled bool) {
	t.Helper()
	value := "false"
	if enabled {
		value = "true"
	}
	cacheKey := keys.TableCachePrefix() + fmt.Sprintf(keys.SysConfigUUID, uuid)
	if err := rds.HSet(context.Background(), cacheKey, map[string]any{"type": "6", "value": value}).Err(); err != nil {
		t.Fatalf("写入登录安全配置[%s]失败: %v", uuid, err)
	}
}
