package cache

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	keys "admin/common/rediskeys"
	"admin/internal/config"
	redislock "admin/internal/infra/redsync"
	corelogic "admin/internal/logic"
	"admin/internal/svc"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// TestNormalizeSecurityCacheSyncPlan 验证补偿摘要输入会稳定排序、去重并过滤无效值。
func TestNormalizeSecurityCacheSyncPlan(t *testing.T) {
	plan := normalizeSecurityCacheSyncPlan(securityCacheSyncPlan{
		TableKeys:   []string{"table:b", "", "table:a", "table:b"},
		RedisKeys:   []string{"redis:b", "redis:a", "redis:b"},
		MFAAdminIDs: []int{9, 0, 7, 9, -1},
	})
	if !slices.Equal(plan.TableKeys, []string{"table:a", "table:b"}) {
		t.Fatalf("TableKeys = %v", plan.TableKeys)
	}
	if !slices.Equal(plan.RedisKeys, []string{"redis:a", "redis:b"}) {
		t.Fatalf("RedisKeys = %v", plan.RedisKeys)
	}
	if !slices.Equal(plan.MFAAdminIDs, []int{7, 9}) {
		t.Fatalf("MFAAdminIDs = %v", plan.MFAAdminIDs)
	}
}

// TestSecurityCacheSyncBackoff 验证补偿重试按指数增长并在一分钟封顶。
func TestSecurityCacheSyncBackoff(t *testing.T) {
	tests := []struct {
		attempts int           // attempts 表示已失败次数。
		want     time.Duration // want 表示期望退避时间。
	}{
		{attempts: 0, want: time.Second},
		{attempts: 2, want: 2 * time.Second},
		{attempts: 6, want: 32 * time.Second},
		{attempts: 7, want: time.Minute},
		{attempts: 20, want: time.Minute},
	}
	for _, test := range tests {
		if got := securityCacheSyncBackoff(test.attempts); got != test.want {
			t.Fatalf("securityCacheSyncBackoff(%d) = %s, want %s", test.attempts, got, test.want)
		}
	}
}

// TestSecurityCacheSyncWorkerIntervals 锁定阻断快速补偿和空闲数据库对账频率。
func TestSecurityCacheSyncWorkerIntervals(t *testing.T) {
	if securityCacheSyncPollInterval != time.Second {
		t.Fatalf("poll interval = %s, want %s", securityCacheSyncPollInterval, time.Second)
	}
	if securityCacheSyncReconcileInterval != 30*time.Second {
		t.Fatalf("reconcile interval = %s, want %s", securityCacheSyncReconcileInterval, 30*time.Second)
	}
}

// TestSecurityCacheSyncErrorText 验证任务错误摘要按字符数截断。
func TestSecurityCacheSyncErrorText(t *testing.T) {
	message := strings.Repeat("错", securityCacheSyncMaxErrorRunes+1)
	got := securityCacheSyncErrorText(assertionError(message))
	if len([]rune(got)) != securityCacheSyncMaxErrorRunes {
		t.Fatalf("securityCacheSyncErrorText() rune length = %d", len([]rune(got)))
	}
}

// TestSecurityCacheSyncWorkerStartFailsWithoutDatabase 验证启动首轮失败时不会伪装 worker 已运行。
func TestSecurityCacheSyncWorkerStartFailsWithoutDatabase(t *testing.T) {
	worker := NewSecurityCacheSyncWorker(svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{}))
	if err := worker.Start(context.Background()); err == nil {
		t.Fatal("Start() error = nil, want database unavailable error")
	}
	if worker.running.Load() {
		t.Fatal("Start() failure must reset running state")
	}
}

// TestRunPendingSecurityCacheSyncSkipsDatabaseWithoutSignal 验证空闲轮询只检查 Redis，不访问数据库。
func TestRunPendingSecurityCacheSyncSkipsDatabaseWithoutSignal(t *testing.T) {
	useRuntimeAppID(t, "site-a")
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	worker := NewSecurityCacheSyncWorker(svc.NewServiceContext(
		config.Config{AppID: "site-a"},
		svc.Dependencies{Rds: client},
	))
	if err := worker.runPendingOnce(context.Background()); err != nil {
		t.Fatalf("runPendingOnce() error = %v", err)
	}
}

// TestRunPendingSecurityCacheSyncFailsClosedOnSignal 验证阻断信号会先关闭本进程鉴权再进入数据库补偿。
func TestRunPendingSecurityCacheSyncFailsClosedOnSignal(t *testing.T) {
	useRuntimeAppID(t, "site-a")
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	service := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client})
	worker := NewSecurityCacheSyncWorker(service)
	if err := client.Set(context.Background(), keys.SecurityCacheSyncBarrierRedisKey(), "pending", 0).Err(); err != nil {
		t.Fatalf("seed barrier error = %v", err)
	}
	if err := worker.runPendingOnce(context.Background()); err == nil {
		t.Fatal("runPendingOnce() error = nil, want database unavailable error")
	}
	if !service.SecurityCacheSyncPending() {
		t.Fatal("barrier signal must close local cache authentication")
	}
}

// TestRunSecurityCacheSyncSkipsHeldLockWithoutRetry 校验后台补偿遇到其它实例持锁时立即成功跳过。
func TestRunSecurityCacheSyncSkipsHeldLockWithoutRetry(t *testing.T) {
	useRuntimeAppID(t, "site-a")
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	service := svc.NewServiceContext(
		config.Config{AppID: "site-a"},
		svc.Dependencies{
			SiteDBs: svc.SiteDatabases{MainDB: &gorm.DB{}},
			Rds:     client,
		},
	)
	holder := redislock.NewLock(client, keys.SecurityCacheSyncLockRedisKey())
	if err := holder.TryLock(context.Background(), time.Minute); err != nil {
		t.Fatalf("holder.TryLock() error = %v", err)
	}
	t.Cleanup(func() { _ = holder.Unlock() })

	startedAt := time.Now()
	if err := NewSecurityCacheSyncWorker(service).runOnce(context.Background()); err != nil {
		t.Fatalf("runOnce() lock contention error = %v, want successful skip", err)
	}
	if elapsed := time.Since(startedAt); elapsed >= 500*time.Millisecond {
		t.Fatalf("runOnce() lock contention elapsed = %v, want no retry backoff", elapsed)
	}
}

// TestClearSecurityCacheBarrierIfUnchanged 验证阻断键只会按读取快照清理。
func TestClearSecurityCacheBarrierIfUnchanged(t *testing.T) {
	useRuntimeAppID(t, "site-a")
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	service := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client})
	worker := NewSecurityCacheSyncWorker(service)
	key := keys.SecurityCacheSyncBarrierRedisKey()

	if err := client.Set(context.Background(), key, "old", 0).Err(); err != nil {
		t.Fatalf("seed barrier error = %v", err)
	}
	snapshot, err := worker.barrierSnapshot(context.Background())
	if err != nil {
		t.Fatalf("barrierSnapshot() error = %v", err)
	}
	if err := client.Set(context.Background(), key, "new", 0).Err(); err != nil {
		t.Fatalf("replace barrier error = %v", err)
	}
	cleared, err := worker.clearBarrierIfUnchanged(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("clearBarrierIfUnchanged() error = %v", err)
	}
	if cleared {
		t.Fatal("clearBarrierIfUnchanged() = true, want concurrent barrier kept")
	}
	if got, _ := server.Get(key); got != "new" {
		t.Fatalf("barrier = %q, want new", got)
	}
	if !service.SecurityCacheSyncPending() {
		t.Fatal("concurrent barrier must keep local pending state")
	}

	snapshot, err = worker.barrierSnapshot(context.Background())
	if err != nil {
		t.Fatalf("barrierSnapshot() second error = %v", err)
	}
	cleared, err = worker.clearBarrierIfUnchanged(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("clearBarrierIfUnchanged() second error = %v", err)
	}
	if !cleared || server.Exists(key) {
		t.Fatal("unchanged barrier must be cleared")
	}
	if service.SecurityCacheSyncPending() {
		t.Fatal("cleared barrier must release local pending state")
	}
}

// TestClearAbsentSecurityCacheBarrierKeepsConcurrentWrite 验证空快照不会删除随后写入的阻断键。
func TestClearAbsentSecurityCacheBarrierKeepsConcurrentWrite(t *testing.T) {
	useRuntimeAppID(t, "site-a")
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	service := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client})
	worker := NewSecurityCacheSyncWorker(service)
	key := keys.SecurityCacheSyncBarrierRedisKey()

	snapshot, err := worker.barrierSnapshot(context.Background())
	if err != nil {
		t.Fatalf("barrierSnapshot() error = %v", err)
	}
	if err := client.Set(context.Background(), key, "new", 0).Err(); err != nil {
		t.Fatalf("seed concurrent barrier error = %v", err)
	}
	cleared, err := worker.clearBarrierIfUnchanged(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("clearBarrierIfUnchanged() error = %v", err)
	}
	got, _ := server.Get(key)
	if cleared || got != "new" {
		t.Fatal("absent snapshot must keep a concurrent barrier")
	}
}

// TestDeleteAdminMFAKeysExact 验证补偿清理会精确删除管理员登录标记和二次票据 Hash。
func TestDeleteAdminMFAKeysExact(t *testing.T) {
	useRuntimeAppID(t, "site-a")
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	service := svc.NewServiceContext(config.Config{AppID: "site-a"}, svc.Dependencies{Rds: client})
	base := corelogic.NewBaseLogicWithContext(context.Background(), service)
	adminID := 7
	twoStepKey := keys.AdminMFATwoStepRedisKey(adminID)
	loginFlagKey := keys.LoginCheckMFAFlagRedisKey(adminID)
	if err := client.HSet(base.Ctx, twoStepKey, "ticket-1", "payload").Err(); err != nil {
		t.Fatalf("seed ticket hash error = %v", err)
	}
	if err := client.Set(base.Ctx, loginFlagKey, "1", 0).Err(); err != nil {
		t.Fatalf("seed login flag error = %v", err)
	}

	if err := deleteAdminMFAKeysExact(base, adminID); err != nil {
		t.Fatalf("deleteAdminMFAKeysExact() error = %v", err)
	}
	for _, key := range []string{twoStepKey, loginFlagKey} {
		if server.Exists(key) {
			t.Fatalf("Redis key %q still exists", key)
		}
	}
}

// assertionError 为错误摘要测试提供最小 error 实现。
type assertionError string

// Error 返回测试错误文本。
func (e assertionError) Error() string {
	return string(e)
}
