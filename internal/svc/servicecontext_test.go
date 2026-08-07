package svc

import (
	"context"
	"net/http/httptest"
	"testing"

	"admin/internal/config"
	"admin/internal/infra/redislimit"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestScopedWithContextCopiesConfigSnapshot 验证对应场景符合预期。
func TestScopedWithContextCopiesConfigSnapshot(t *testing.T) {
	svcCtx := NewServiceContext(config.Config{AppID: "root"}, Dependencies{})
	svcCtx.configValue.Store(config.Config{AppID: "request"})

	scoped := svcCtx.ScopedWithContext(context.Background())
	if scoped == nil {
		t.Fatal("ScopedWithContext() = nil")
	}
	if got := scoped.CurrentConfig().AppID; got != "request" {
		t.Fatalf("scoped AppID = %q, want request", got)
	}
}

// TestScopedWithContextReusesRedisLimiter 验证请求作用域复用进程级限流器，避免同 key 产生多套本地等待队列。
func TestScopedWithContextReusesRedisLimiter(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	svcCtx := NewServiceContext(config.Config{AppID: "site-a"}, Dependencies{Rds: client})
	if svcCtx.RedisLimiter == nil {
		t.Fatal("ServiceContext 未自动初始化 RedisLimiter")
	}
	scoped := svcCtx.ScopedWithContext(context.Background())
	if scoped == nil || scoped.RedisLimiter != svcCtx.RedisLimiter {
		t.Fatal("请求作用域必须复用进程级 RedisLimiter 指针")
	}
}

// TestScopedWithContextReusesTableCacheStore 验证请求作用域共享已确认的 Redis 拓扑，避免重复执行 CLUSTER INFO。
func TestScopedWithContextReusesTableCacheStore(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	svcCtx := NewServiceContext(config.Config{AppID: "site-a"}, Dependencies{Rds: client})
	if svcCtx.TableCacheStore == nil {
		t.Fatal("ServiceContext 未自动初始化进程级 TableCacheStore")
	}
	scoped := svcCtx.ScopedWithContext(context.Background())
	if scoped == nil || scoped.TableCacheStore != svcCtx.TableCacheStore {
		t.Fatal("请求作用域必须复用进程级 TableCacheStore 指针")
	}
	if withoutRedis := NewServiceContext(config.Config{AppID: "site-a"}, Dependencies{}); withoutRedis.TableCacheStore != nil {
		t.Fatal("缺少 Redis 客户端时不应创建 TableCacheStore")
	}
}

// TestNewServiceContextPreservesInjectedRedisLimiter 验证显式注入优先且缺少 Redis 时不创建空组件。
func TestNewServiceContextPreservesInjectedRedisLimiter(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	injected := redislimit.New(client)

	svcCtx := NewServiceContext(config.Config{AppID: "site-a"}, Dependencies{Rds: client, RedisLimiter: injected})
	if svcCtx.RedisLimiter != injected {
		t.Fatal("ServiceContext 覆盖了显式注入的 RedisLimiter")
	}
	if scoped := svcCtx.ScopedWithContext(context.Background()); scoped == nil || scoped.RedisLimiter != injected {
		t.Fatal("请求作用域未复用显式注入的 RedisLimiter")
	}
	if withoutRedis := NewServiceContext(config.Config{AppID: "site-a"}, Dependencies{}); withoutRedis.RedisLimiter != nil {
		t.Fatal("缺少 Redis 客户端时不应创建 RedisLimiter")
	}
}

// TestClientIPHonorsExplicitTrustedProxies 验证只有显式可信代理才能提供转发客户端地址。
func TestClientIPHonorsExplicitTrustedProxies(t *testing.T) {
	svcCtx := NewServiceContext(config.Config{TrustedProxies: []string{"10.0.0.0/8"}}, Dependencies{})

	trustedRequest := httptest.NewRequest("GET", "/", nil)
	trustedRequest.RemoteAddr = "10.0.0.10:8080"
	trustedRequest.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.11")
	if got := svcCtx.ClientIP(trustedRequest); got != "203.0.113.9" {
		t.Fatalf("可信代理解析客户端 IP=%q，期望 203.0.113.9", got)
	}

	untrustedRequest := httptest.NewRequest("GET", "/", nil)
	untrustedRequest.RemoteAddr = "192.0.2.20:8080"
	untrustedRequest.Header.Set("X-Forwarded-For", "203.0.113.9")
	if got := svcCtx.ClientIP(untrustedRequest); got != "192.0.2.20" {
		t.Fatalf("非可信来源不应采用转发头，实际客户端 IP=%q", got)
	}

	zonedRequest := httptest.NewRequest("GET", "/", nil)
	zonedRequest.RemoteAddr = "[fe80::1%very-long-interface-name]:8080"
	if got := svcCtx.ClientIP(zonedRequest); got != "fe80::1" {
		t.Fatalf("客户端 IPv6 zone 未清理，实际客户端 IP=%q", got)
	}
}
