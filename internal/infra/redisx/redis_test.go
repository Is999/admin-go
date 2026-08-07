package redisx

import (
	"context"
	stderrors "errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"admin/internal/config"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestResolveAddrs 验证对应场景。
func TestResolveAddrs(t *testing.T) {
	t.Run("normalize and deduplicate addrs", func(t *testing.T) {
		addrs, err := resolveAddrs([]string{
			"",
			"127.0.0.1:7001",
			" 127.0.0.1:7002 ",
			"127.0.0.1:7001",
			"127.0.0.1:7003",
		})
		if err != nil {
			t.Fatalf("resolveAddrs returned error: %v", err)
		}

		expected := []string{
			"127.0.0.1:7001",
			"127.0.0.1:7002",
			"127.0.0.1:7003",
		}
		if !reflect.DeepEqual(addrs, expected) {
			t.Fatalf("expected addrs %+v, got %+v", expected, addrs)
		}
	})

	t.Run("missing addrs", func(t *testing.T) {
		_, err := resolveAddrs(nil)
		if err == nil {
			t.Fatalf("expected error when addrs are empty")
		}
	})

	t.Run("blank addrs only", func(t *testing.T) {
		_, err := resolveAddrs([]string{"", "   "})
		if err == nil {
			t.Fatalf("expected error when addrs contain only blanks")
		}
	})
}

// TestResolveAddrMap 验证对应场景。
func TestResolveAddrMap(t *testing.T) {
	addrMap := resolveAddrMap(map[string]string{
		"":                          "127.0.0.1",
		"redis-cluster-7001":        "",
		"redis-cluster-7002":        " 127.0.0.1 ",
		" redis-cluster-7003:7003 ": " 127.0.0.1:7003 ",
	})

	expected := map[string]string{
		"redis-cluster-7002":      "127.0.0.1",
		"redis-cluster-7003:7003": "127.0.0.1:7003",
	}
	if !reflect.DeepEqual(addrMap, expected) {
		t.Fatalf("expected addrMap %+v, got %+v", expected, addrMap)
	}
}

// TestRewriteClusterAddr 验证对应场景。
func TestRewriteClusterAddr(t *testing.T) {
	addrMap := map[string]string{
		"redis-cluster-7001":      "127.0.0.1",
		"redis-cluster-7002:7002": "127.0.0.1:7002",
	}

	tests := []struct {
		name string // name 表示测试场景名称。
		addr string // addr 表示测试连接地址。
		want string // want 表示期望结果。
	}{
		{
			name: "host only mapping",
			addr: "redis-cluster-7001:7001",
			want: "127.0.0.1:7001",
		},
		{
			name: "full addr mapping",
			addr: "redis-cluster-7002:7002",
			want: "127.0.0.1:7002",
		},
		{
			name: "no mapping",
			addr: "redis-cluster-7003:7003",
			want: "redis-cluster-7003:7003",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rewriteClusterAddr(tt.addr, addrMap)
			if got != tt.want {
				t.Fatalf("expected %s, got %s", tt.want, got)
			}
		})
	}
}

// TestPingConfiguredAddrsUsesClusterAddrMap 确保预探测阶段和正式 Cluster 客户端一样使用地址改写。
func TestPingConfiguredAddrsUsesClusterAddrMap(t *testing.T) {
	server := miniredis.RunT(t)
	err := pingConfiguredAddrs(
		context.Background(),
		config.RedisConfig{Type: "cluster"},
		[]string{"redis-cluster-7001:7001"},
		map[string]string{"redis-cluster-7001:7001": server.Addr()},
	)
	if err != nil {
		t.Fatalf("expected addr_map preflight ping success, got %v", err)
	}
}

// TestHookProcessHookPreservesSentinelError 验证 Redis hook 不会改写哨兵错误语义。
func TestHookProcessHookPreservesSentinelError(t *testing.T) {
	h := newHook(time.Second)
	wrapped := h.ProcessHook(func(context.Context, redis.Cmder) error {
		return redis.Nil
	})

	err := wrapped(context.Background(), redis.NewCmd(context.Background(), "eval"))
	if !stderrors.Is(err, redis.Nil) {
		t.Fatalf("expected redis.Nil to be preserved, got %v", err)
	}
	if err != redis.Nil {
		t.Fatalf("expected exact redis.Nil sentinel, got %v", err)
	}
}

// TestHookProcessPipelineHookPreservesSentinelError 验证管道 hook 仅调整日志判定，不改写空值语义。
func TestHookProcessPipelineHookPreservesSentinelError(t *testing.T) {
	h := newHook(time.Second)
	wrapped := h.ProcessPipelineHook(func(_ context.Context, cmds []redis.Cmder) error {
		cmds[0].SetErr(redis.Nil)
		return redis.Nil
	})

	cmd := redis.NewCmd(context.Background(), "get", "missing")
	err := wrapped(context.Background(), []redis.Cmder{cmd})
	if err != redis.Nil {
		t.Fatalf("expected exact redis.Nil sentinel, got %v", err)
	}
}

// TestPipelineLogError 验证管道空值不会误报，同时不隐藏后续真实命令错误。
func TestPipelineLogError(t *testing.T) {
	realErr := stderrors.New("connection reset")
	nilCmd := redis.NewCmd(context.Background(), "get", "missing")
	nilCmd.SetErr(redis.Nil)
	failedCmd := redis.NewCmd(context.Background(), "get", "failed")
	failedCmd.SetErr(realErr)

	tests := []struct {
		name string        // name 表示测试场景名称。
		err  error         // err 表示管道返回的聚合错误。
		cmds []redis.Cmder // cmds 表示管道内各命令及其执行结果。
		want error         // want 表示应写入错误日志的错误；nil 表示不记录。
	}{
		{name: "nil result only", err: redis.Nil, cmds: []redis.Cmder{nilCmd}},
		{name: "real command error after nil", err: redis.Nil, cmds: []redis.Cmder{nilCmd, failedCmd}, want: realErr},
		{name: "real pipeline error", err: realErr, cmds: []redis.Cmder{failedCmd}, want: realErr},
		{name: "success", cmds: []redis.Cmder{redis.NewCmd(context.Background(), "ping")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := pipelineLogError(tt.err, tt.cmds); !stderrors.Is(got, tt.want) {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

// TestIsRedisScriptCacheMiss 验证 Redis 脚本缓存缺失只匹配 EVALSHA 类命令。
func TestIsRedisScriptCacheMiss(t *testing.T) {
	tests := []struct {
		name string      // name 表示测试场景名称。
		cmd  redis.Cmder // cmd 表示待验证命令。
		err  error       // err 表示待验证错误。
		want bool        // want 表示期望结果。
	}{
		{
			name: "evalsha noscript sentinel",
			cmd:  redis.NewCmd(context.Background(), redisCommandEvalSHA, "sha", 0),
			err:  redis.ErrNoScript,
			want: true,
		},
		{
			name: "evalsha ro noscript string",
			cmd:  redis.NewCmd(context.Background(), redisCommandEvalSHARO, "sha", 0),
			err:  stderrors.New("NOSCRIPT No matching script. Please use EVAL."),
			want: true,
		},
		{
			name: "evalsha wrapped noscript",
			cmd:  redis.NewCmd(context.Background(), redisCommandEvalSHA, "sha", 0),
			err:  fmt.Errorf("redis script cache miss: %w", redis.ErrNoScript),
			want: true,
		},
		{
			name: "eval normal error",
			cmd:  redis.NewCmd(context.Background(), "eval", "return 1", 0),
			err:  redis.ErrNoScript,
			want: false,
		},
		{
			name: "other command noscript",
			cmd:  redis.NewCmd(context.Background(), "get", "key"),
			err:  redis.ErrNoScript,
			want: false,
		},
		{
			name: "evalsha redis nil",
			cmd:  redis.NewCmd(context.Background(), redisCommandEvalSHA, "sha", 0),
			err:  redis.Nil,
			want: false,
		},
		{
			name: "nil command",
			cmd:  nil,
			err:  redis.ErrNoScript,
			want: false,
		},
		{
			name: "nil error",
			cmd:  redis.NewCmd(context.Background(), redisCommandEvalSHA, "sha", 0),
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRedisScriptCacheMiss(tt.err, tt.cmd); got != tt.want {
				t.Fatalf("expected %v, got %v", tt.want, got)
			}
		})
	}
}

// TestIsRedisStandaloneTopologyProbe 验证只有单机 Redis 对 CLUSTER INFO 的标准响应被归类为预期拓扑探测。
func TestIsRedisStandaloneTopologyProbe(t *testing.T) {
	tests := []struct {
		name string
		cmd  redis.Cmder
		err  error
		want bool
	}{
		{
			name: "standalone response",
			cmd:  redis.NewCmd(context.Background(), "cluster", "info"),
			err:  stderrors.New("ERR This instance has cluster support disabled"),
			want: true,
		},
		{
			name: "cluster failure",
			cmd:  redis.NewCmd(context.Background(), "cluster", "info"),
			err:  stderrors.New("CLUSTERDOWN The cluster is down"),
		},
		{
			name: "other command",
			cmd:  redis.NewCmd(context.Background(), "get", "key"),
			err:  stderrors.New("ERR This instance has cluster support disabled"),
		},
		{
			name: "nil error",
			cmd:  redis.NewCmd(context.Background(), "cluster", "info"),
		},
		{
			name: "nil command",
			err:  stderrors.New("ERR This instance has cluster support disabled"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isRedisStandaloneTopologyProbe(tt.err, tt.cmd); got != tt.want {
				t.Fatalf("isRedisStandaloneTopologyProbe() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestNewPingsRedisAtStartup 确保 Redis 客户端创建阶段会完成联通性探测。
func TestNewPingsRedisAtStartup(t *testing.T) {
	// server 提供一个真实可 PING 的 Redis 单节点，验证 New 能完成启动探测。
	server := miniredis.RunT(t)
	client, err := New(context.Background(), config.RedisConfig{
		Type: "single",
		Addrs: []string{
			server.Addr(),
		},
		PoolSize: 1,
	}, config.ObservabilityConfig{})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	defer client.Close()
	singleClient, ok := client.(*redis.Client)
	if !ok || !singleClient.Options().ContextTimeoutEnabled {
		t.Fatalf("Redis 客户端必须启用 context deadline: type=%T", client)
	}
}

// TestNewRequiresEveryConfiguredRedisAddressReachable 确保每个配置的 Redis 地址都必须可连通。
func TestNewRequiresEveryConfiguredRedisAddressReachable(t *testing.T) {
	// server 表示可用节点，unusedTCPAddr 表示不可达节点，用于验证任一地址失败都会阻断启动。
	server := miniredis.RunT(t)
	_, err := New(context.Background(), config.RedisConfig{
		Type: "single",
		Addrs: []string{
			server.Addr(),
			unusedTCPAddr(t),
		},
		PoolSize: 1,
	}, config.ObservabilityConfig{})
	if err == nil {
		t.Fatalf("expected error when one Redis address is unreachable")
	}
	if !strings.Contains(err.Error(), "探测 Redis 地址[1]") {
		t.Fatalf("expected address index in error, got %v", err)
	}
}

// unusedTCPAddr 返回一个当前未监听的本地 TCP 地址，用于模拟 Redis 不可达。
func unusedTCPAddr(t *testing.T) string {
	t.Helper()
	// listener 先占用随机端口，关闭后该地址通常保持未监听状态。
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on unused TCP address failed: %v", err)
	}
	// addr 保存关闭 listener 后用于探测失败的地址。
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener failed: %v", err)
	}
	return addr
}
