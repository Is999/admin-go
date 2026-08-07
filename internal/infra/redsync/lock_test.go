package redsync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Is999/go-utils/errors"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestLockRetryDelayUsesLinearJitteredBackoff 校验五次重试落在约定区间，超过第五轮后基础等待不再增长。
func TestLockRetryDelayUsesLinearJitteredBackoff(t *testing.T) {
	// cases 同时覆盖每轮上下界、非法抖动归一化和未来增加重试次数时的封顶行为。
	cases := []struct {
		name   string        // 当前边界场景
		tries  int           // go-redsync 传入的重试轮次，从 1 开始
		jitter time.Duration // 由随机源生成的抖动值
		want   time.Duration // 归一化后的最终等待
	}{
		{name: "first lower", tries: 1, jitter: 0, want: 100 * time.Millisecond},
		{name: "first upper", tries: 1, jitter: 100 * time.Millisecond, want: 200 * time.Millisecond},
		{name: "second lower", tries: 2, jitter: 0, want: 200 * time.Millisecond},
		{name: "second upper", tries: 2, jitter: 100 * time.Millisecond, want: 300 * time.Millisecond},
		{name: "third lower", tries: 3, jitter: 0, want: 300 * time.Millisecond},
		{name: "third upper", tries: 3, jitter: 100 * time.Millisecond, want: 400 * time.Millisecond},
		{name: "fourth lower", tries: 4, jitter: 0, want: 400 * time.Millisecond},
		{name: "fourth upper", tries: 4, jitter: 100 * time.Millisecond, want: 500 * time.Millisecond},
		{name: "fifth lower", tries: 5, jitter: 0, want: 500 * time.Millisecond},
		{name: "fifth upper", tries: 5, jitter: 100 * time.Millisecond, want: 600 * time.Millisecond},
		{name: "base capped", tries: 6, jitter: 0, want: 500 * time.Millisecond},
		{name: "jitter capped", tries: 6, jitter: time.Second, want: 600 * time.Millisecond},
		{name: "invalid values", tries: 0, jitter: -time.Second, want: 100 * time.Millisecond},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := lockRetryDelay(tt.tries, tt.jitter); got != tt.want {
				t.Fatalf("lockRetryDelay(%d, %v) = %v, want %v", tt.tries, tt.jitter, got, tt.want)
			}
		})
	}
}

// TestLockRetryDelayTotalRangeFitsAcquireTimeout 校验六次尝试对应五段等待，纯退避总计为 1.5–2s。
func TestLockRetryDelayTotalRangeFitsAcquireTimeout(t *testing.T) {
	// minTotal 和 maxTotal 分别表示无抖动与每轮最大抖动时的纯等待总和，不包含 Redis 请求耗时。
	var minTotal time.Duration
	var maxTotal time.Duration
	for tries := 1; tries < lockAcquireTries; tries++ {
		minTotal += lockRetryDelay(tries, 0)
		maxTotal += lockRetryDelay(tries, maxLockRetryJitter)
	}
	if minTotal != 1500*time.Millisecond || maxTotal != 2*time.Second {
		t.Fatalf("retry delay total range = %v–%v, want 1.5s–2s", minTotal, maxTotal)
	}
	if maxLockAcquireTimeout <= maxTotal {
		t.Fatalf("maxLockAcquireTimeout = %v, must exceed maximum backoff %v", maxLockAcquireTimeout, maxTotal)
	}
}

// TestTryLockWithoutContentionUsesSingleAcquireCommand 校验无竞争时首轮成功，不因退避策略增加 Redis 加锁请求。
func TestTryLockWithoutContentionUsesSingleAcquireCommand(t *testing.T) {
	// server 和 client 先用 PING 完成连接初始化，使命令差值只包含 RedSync 的加锁请求。
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis PING failed: %v", err)
	}

	before := server.CommandCount()
	lock := NewLock(client, "lock:no-contention")
	if err := lock.TryLock(context.Background(), time.Second); err != nil {
		t.Fatalf("TryLock() error = %v", err)
	}
	if got := server.CommandCount() - before; got != 1 {
		t.Fatalf("uncontended acquire command count = %d, want 1", got)
	}
	if err := lock.Unlock(); err != nil {
		t.Fatalf("Unlock() error = %v", err)
	}
}

// TestTryLockRejectsNilRedisClient 校验 nil Redis 客户端会返回明确错误，而不是在 redsync 内部 panic。
func TestTryLockRejectsNilRedisClient(t *testing.T) {
	// lock 模拟调用方传入 nil Redis 客户端时创建出来的锁实例。
	lock := NewLock(nil, "lock:nil-client")

	err := lock.TryLock(context.Background(), time.Second)
	if err == nil {
		t.Fatal("expected nil redis client lock error, got nil")
	}
	if !strings.Contains(err.Error(), "Redis 锁未初始化") {
		t.Fatalf("unexpected lock error: %v", err)
	}
}

// TestIsLockTakenDetectsContention 校验锁竞争错误能被识别为可跳过的互斥冲突。
func TestIsLockTakenDetectsContention(t *testing.T) {
	// server 和 client 使用 miniredis 构造同一把锁的竞争场景，避免依赖真实 Redis。
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	lock := NewLock(client, "lock:busy")
	if err := lock.TryLock(context.Background(), time.Second); err != nil {
		t.Fatalf("expected first lock success, got %v", err)
	}
	defer lock.Unlock()

	err := WithLock(context.Background(), client, "lock:busy", time.Second, func(context.Context) error {
		t.Fatal("second lock holder should not run")
		return nil
	})
	if err == nil {
		t.Fatal("expected lock contention error, got nil")
	}
	if !IsLockTaken(err) {
		t.Fatalf("expected lock taken error, got %v", err)
	}
}

// TestWithLockOnceReturnsContentionWithoutRetry 校验单次入口只执行一轮抢锁和清理，不进入全局退避。
func TestWithLockOnceReturnsContentionWithoutRetry(t *testing.T) {
	// server 和 client 构造稳定竞争；holder 的 TTL 足以避免测试期间触发续期命令干扰计数。
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	holder := NewLock(client, "lock:single-attempt")
	if err := holder.TryLock(context.Background(), 2*time.Second); err != nil {
		t.Fatalf("expected holder lock success, got %v", err)
	}
	defer holder.Unlock()

	before := server.CommandCount()
	startedAt := time.Now()
	callbackCalled := false
	err := WithLockOnce(context.Background(), client, "lock:single-attempt", 2*time.Second, func(context.Context) error {
		callbackCalled = true
		return nil
	})
	if !IsLockTaken(err) {
		t.Fatalf("WithLockOnce() error = %v, want ErrLockTaken", err)
	}
	if callbackCalled {
		t.Fatal("WithLockOnce() callback ran without owning the lock")
	}
	if elapsed := time.Since(startedAt); elapsed >= 500*time.Millisecond {
		t.Fatalf("WithLockOnce() contention elapsed = %v, want no retry backoff", elapsed)
	}
	// miniredis 会把 SETNX、脚本缓存回退和脚本内 owner 读取分别计数；单轮冷启动路径固定为 4，更多命令表示发生了重试。
	if got := server.CommandCount() - before; got != 4 {
		t.Fatalf("WithLockOnce() contention command count = %d, want 4", got)
	}
}

// TestWithLockReturnsUnlockError 校验释放锁失败时 WithLock 会把错误返回给调用方。
func TestWithLockReturnsUnlockError(t *testing.T) {
	// server 和 client 使用 miniredis 构造可控的 Redis 环境，便于模拟释放阶段断连。
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	err := WithLock(context.Background(), client, "lock:unlock-error", time.Second, func(context.Context) error {
		server.Close()
		return nil
	})
	if err == nil {
		t.Fatal("expected unlock error, got nil")
	}
	if !strings.Contains(err.Error(), "释放 Redis 锁失败") {
		t.Fatalf("expected release failure, got %v", err)
	}
}

// TestWithLockUnlocksOnPanic 校验业务 panic 时仍会释放锁，并保持 panic 向上抛出。
func TestWithLockUnlocksOnPanic(t *testing.T) {
	// server 和 client 构造可重新竞争同一锁 key 的本地 Redis 环境。
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	// panicValue 和 recovered 校验 WithLock 只负责清理资源，不吞掉业务 panic。
	const panicValue = "lock callback panic"
	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		_ = WithLock(context.Background(), client, "lock:panic", time.Second, func(context.Context) error {
			panic(panicValue)
		})
	}()
	if recovered != panicValue {
		t.Fatalf("recovered panic = %v, want %q", recovered, panicValue)
	}
	if err := WithLock(context.Background(), client, "lock:panic", time.Second, nil); err != nil {
		t.Fatalf("expected lock to be released after panic, got %v", err)
	}
}

// TestWithLockPreservesAcquireContextError 校验等待锁期间超时会保留 context 错误语义。
func TestWithLockPreservesAcquireContextError(t *testing.T) {
	// server 和 client 构造锁竞争环境，由较短的父 deadline 中止后续退避。
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	// holder 先持有目标锁，确保被测调用进入竞争等待。
	holder := NewLock(client, "lock:acquire-timeout")
	if err := holder.TryLock(context.Background(), time.Second); err != nil {
		t.Fatalf("expected holder lock success, got %v", err)
	}
	defer holder.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := WithLock(ctx, client, "lock:acquire-timeout", time.Second, func(context.Context) error {
		t.Fatal("timed out lock callback should not run")
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected acquisition deadline error, got %v", err)
	}
}

// TestWithLockCancelsContextOnRenewalFailure 校验续期失败后业务 context 会被主动取消。
func TestWithLockCancelsContextOnRenewalFailure(t *testing.T) {
	// server 和 client 在业务函数内主动关闭，用来触发后台续期失败。
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	err := WithLock(context.Background(), client, "lock:renewal-failure", 50*time.Millisecond, func(ctx context.Context) error {
		server.Close()
		select {
		case <-ctx.Done():
			return errors.Tag(ctx.Err())
		case <-time.After(2 * time.Second):
			return errors.New("lock context was not canceled")
		}
	})
	if err == nil {
		t.Fatal("expected renewal failure error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected protected context cancellation, got %v", err)
	}
	if !errors.Is(err, ErrLockLost) {
		t.Fatalf("expected lock lost error, got %v", err)
	}
}
