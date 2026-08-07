package redisx

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"time"

	"admin/internal/config"
	"admin/internal/infra/loggerx"

	"github.com/Is999/go-utils/errors"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	// redisCommandEvalSHA 表示 Redis 脚本缓存执行命令，go-redis Script.Run 会先尝试该命令。
	redisCommandEvalSHA = "evalsha"
	// redisCommandEvalSHARO 表示 Redis 只读脚本缓存执行命令，go-redis RunRO 会先尝试该命令。
	redisCommandEvalSHARO = "evalsha_ro"
	// redisNoScriptPrefix 表示 Redis 脚本缓存缺失错误前缀，后续会由 go-redis 自动回退到 EVAL。
	redisNoScriptPrefix = "NOSCRIPT"
	// redisCommandClusterInfo 表示 table-cache 用单节点客户端确认实际 Redis 拓扑的探测命令。
	redisCommandClusterInfo = "cluster info"
	// redisClusterDisabledMessage 表示 Redis 单机模式对 CLUSTER INFO 的标准拒绝响应，该结果确认拓扑而非业务失败。
	redisClusterDisabledMessage = "cluster support disabled"
)

// New 创建 Redis 客户端，并注册统一的命令耗时/错误日志 hook。
func New(ctx context.Context, cfg config.RedisConfig, obs config.ObservabilityConfig) (redis.UniversalClient, error) {
	addrs, err := resolveAddrs(cfg.Addrs)
	if err != nil {
		return nil, errors.Tag(err)
	}
	addrMap := resolveAddrMap(cfg.AddrMap)
	if err := pingConfiguredAddrs(ctx, cfg, addrs, addrMap); err != nil {
		return nil, errors.Tag(err)
	}

	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = 100 // 提升默认连接池大小以应对高并发任务投递
	}

	var rdb redis.UniversalClient
	if !isClusterMode(cfg, addrs) {
		option := &redis.Options{
			Addr:                  addrs[0],
			Password:              cfg.Password,
			DB:                    cfg.DB,
			PoolSize:              poolSize,
			MinIdleConns:          poolSize / 5, // 保持一定的最小空闲连接，避免突发流量时建连耗时
			DisableIdentity:       true,         // 禁用 CLIENT SETINFO，避免代理 Redis 拒绝该命令
			ContextTimeoutEnabled: true,         // 让调用方 deadline 约束 Redis 等待，避免限流等链路越过请求预算
			Protocol:              2,
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode: maintnotifications.ModeDisabled,
			},
		}
		applyTLSConfig(option, cfg)
		rdb = redis.NewClient(option)
	} else {
		clusterOpts := &redis.ClusterOptions{
			Addrs:                 addrs,
			Password:              cfg.Password,
			PoolSize:              poolSize,
			MinIdleConns:          poolSize / 5, // 保持一定的最小空闲连接，避免突发流量时建连耗时
			DisableIdentity:       true,         // 禁用 CLIENT SETINFO，避免代理 Redis 拒绝该命令
			ContextTimeoutEnabled: true,         // Cluster 节点命令同样必须遵守调用方 deadline
			Protocol:              2,
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode: maintnotifications.ModeDisabled,
			},
		}
		applyClusterTLSConfig(clusterOpts, cfg, obs)
		if len(addrMap) > 0 {
			clusterOpts.NewClient = func(opt *redis.Options) *redis.Client {
				cloned := *opt
				cloned.Addr = rewriteClusterAddr(opt.Addr, addrMap)
				return redis.NewClient(&cloned)
			}
		}
		rdb = redis.NewClusterClient(clusterOpts)
	}

	rdb.AddHook(newHook(time.Duration(obs.RedisSlowMs) * time.Millisecond))

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, errors.Tag(err)
	}

	return rdb, nil
}

// resolveAddrs 解析并去重 Redis 地址列表，要求至少提供一个有效地址。
func resolveAddrs(addrs []string) ([]string, error) {
	if len(addrs) == 0 {
		return nil, errors.Errorf("缺少 Redis 地址配置")
	}

	result := make([]string, 0, len(addrs))
	seen := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		trimmed := strings.TrimSpace(addr)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}

	if len(result) == 0 {
		return nil, errors.Errorf("缺少 Redis 地址配置")
	}
	return result, nil
}

// pingConfiguredAddrs 在正式创建 Redis 客户端前逐个探测配置地址。
func pingConfiguredAddrs(ctx context.Context, cfg config.RedisConfig, addrs []string, addrMap map[string]string) error {
	// Cluster 模式不支持按 DB 编号隔离，探测时固定使用 0，保持和正式客户端行为一致。
	db := cfg.DB
	if isClusterMode(cfg, addrs) {
		db = 0
	}
	for idx, addr := range addrs {
		pingAddr := addr
		if isClusterMode(cfg, addrs) {
			// 本地访问 Docker Redis Cluster 时，配置地址常是容器内节点名；预探测必须和正式客户端一致地做地址改写。
			pingAddr = rewriteClusterAddr(addr, addrMap)
		}
		// 单地址探测使用独立短连接，避免失败节点污染后续正式客户端实例。
		option := &redis.Options{
			Addr:                  pingAddr,
			Password:              cfg.Password,
			DB:                    db,
			PoolSize:              1,
			DisableIdentity:       true,
			ContextTimeoutEnabled: true,
			Protocol:              2,
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode: maintnotifications.ModeDisabled,
			},
		}
		applyTLSConfig(option, cfg)
		// client 只用于当前地址 PING，探测完成后立即关闭释放连接。
		client := redis.NewClient(option)
		err := client.Ping(ctx).Err()
		_ = client.Close()
		if err != nil {
			return errors.Wrapf(err, "探测 Redis 地址[%d]=%s 失败", idx, pingAddr)
		}
	}
	return nil
}

// resolveAddrMap 归一化地址改写表，过滤空 key/value。
func resolveAddrMap(raw map[string]string) map[string]string {
	if len(raw) == 0 {
		return nil
	}

	result := make(map[string]string, len(raw))
	for key, value := range raw {
		trimmedKey := strings.TrimSpace(key)
		trimmedValue := strings.TrimSpace(value)
		if trimmedKey == "" || trimmedValue == "" {
			continue
		}
		result[trimmedKey] = trimmedValue
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// isClusterMode 根据显式 type 配置判断是否走 Redis Cluster。
// 未配置 type 时按地址数量推断，减少部署配置缺项导致的启动失败。
func isClusterMode(cfg config.RedisConfig, addrs []string) bool {
	switch strings.ToLower(strings.TrimSpace(cfg.Type)) {
	case "cluster":
		return true
	case "single", "standalone":
		return false
	default:
		return len(addrs) > 1
	}
}

// applyTLSConfig 根据配置为单机 Redis 客户端补充 TLS 参数。
func applyTLSConfig(option *redis.Options, cfg config.RedisConfig) {
	if option == nil || !cfg.TLS {
		return
	}
	option.TLSConfig = &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.TLSInsecureSkipVerify,
	}
}

// applyClusterTLSConfig 根据配置为 Redis Cluster 客户端补充 TLS 参数。
func applyClusterTLSConfig(option *redis.ClusterOptions, cfg config.RedisConfig, obs config.ObservabilityConfig) {
	if option == nil || !cfg.TLS {
		return
	}
	insecureSkipVerify := cfg.TLSInsecureSkipVerify
	if isDevEnvironment(obs) {
		insecureSkipVerify = true
	}
	option.TLSConfig = &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: insecureSkipVerify,
	}
}

// isDevEnvironment 判断当前是否为开发环境。
func isDevEnvironment(obs config.ObservabilityConfig) bool {
	return strings.EqualFold(strings.TrimSpace(obs.Environment), "dev")
}

// rewriteClusterAddr 把集群返回的节点地址改写成宿主机可访问地址。
// 支持两种映射形式：
// 1. "redis-cluster-7001:7001" -> "127.0.0.1:7001"
// 2. "redis-cluster-7001" -> "127.0.0.1"（保留原端口）
func rewriteClusterAddr(addr string, addrMap map[string]string) string {
	if len(addrMap) == 0 {
		return addr
	}
	if mapped, ok := addrMap[addr]; ok && mapped != "" {
		return mapped
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	mappedHost, ok := addrMap[host]
	if !ok || mappedHost == "" {
		return addr
	}
	if strings.Contains(mappedHost, ":") {
		return mappedHost
	}
	return net.JoinHostPort(mappedHost, port)
}

// hook 负责把 go-redis 命令执行结果转成结构化日志。
type hook struct {
	slowThreshold time.Duration // Redis 慢命令日志阈值
}

// newHook 根据慢查询阈值创建 Redis 日志 hook。
func newHook(slowThreshold time.Duration) hook {
	return hook{slowThreshold: slowThreshold}
}

// DialHook 这里直接透传，当前不额外采集 Redis 建连日志。
func (h hook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

// ProcessHook 记录单条 Redis 命令的执行耗时与错误。
func (h hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		begin := time.Now()
		err := next(ctx, cmd)
		h.logProcess(ctx, time.Since(begin), err, cmd)
		// hook 层只负责记录日志，必须原样透传底层错误，
		// 否则会破坏 redis.Nil 这类哨兵错误的直接比较，影响 Asynq 等依赖库判断空结果。
		return err
	}
}

// ProcessPipelineHook 记录 pipeline 执行结果，方便定位批量命令的慢请求与失败。
func (h hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		begin := time.Now()
		err := next(ctx, cmds)

		if err != nil {
			if logErr := pipelineLogError(err, cmds); logErr != nil {
				fields := []logx.LogField{
					logx.Field("latency_ms", time.Since(begin).Milliseconds()),
					logx.Field("commands", pipelineNames(cmds)),
				}
				loggerx.Errorw(ctx, "缓存 管道执行失败", logErr, fields...)
			}
			// pipeline hook 不能改写原始错误语义，避免上层依赖的 errors.Is/直接比较失效。
			return err
		}

		if h.slowThreshold > 0 && time.Since(begin) > h.slowThreshold {
			fields := []logx.LogField{
				logx.Field("latency_ms", time.Since(begin).Milliseconds()),
				logx.Field("commands", pipelineNames(cmds)),
			}
			loggerx.Sloww(ctx, "缓存 管道耗时较高", fields...)
		}
		return nil
	}
}

// pipelineLogError 忽略管道内正常的空值；存在真实命令错误时仍返回该错误用于记录。
func pipelineLogError(err error, cmds []redis.Cmder) error {
	if err == nil || !errors.Is(err, redis.Nil) {
		return err
	}
	for _, cmd := range cmds {
		if cmdErr := cmd.Err(); cmdErr != nil && !errors.Is(cmdErr, redis.Nil) {
			return cmdErr
		}
	}
	return nil
}

// logProcess 根据耗时和错误情况输出 Redis 单命令日志。
func (h hook) logProcess(ctx context.Context, duration time.Duration, err error, cmd redis.Cmder) {
	fields := []logx.LogField{
		logx.Field("latency_ms", duration.Milliseconds()),
		logx.Field("cmd", cmd.FullName()),
		logx.Field("arg_count", max(len(cmd.Args())-1, 0)),
	}

	switch {
	case isRedisScriptCacheMiss(err, cmd):
		// EVALSHA 的 NOSCRIPT 是脚本内容变更、Redis 重启或脚本缓存清理后的正常回退路径。
		// go-redis Script.Run 会继续执行 EVAL，这里避免把中间态误报为业务错误。
		return
	case isRedisStandaloneTopologyProbe(err, cmd):
		// table-cache 会缓存该拓扑结论，单机 Redis 的标准拒绝响应不应污染错误日志。
		return
	case err != nil && !errors.Is(err, redis.Nil):
		loggerx.Errorw(ctx, "缓存 命令执行失败", err, fields...)
	case h.slowThreshold > 0 && duration > h.slowThreshold:
		loggerx.Sloww(ctx, "缓存 命令耗时较高", fields...)
	}
}

// isRedisStandaloneTopologyProbe 判断 CLUSTER INFO 是否以标准响应确认当前节点为非 Cluster Redis。
func isRedisStandaloneTopologyProbe(err error, cmd redis.Cmder) bool {
	if err == nil || cmd == nil || !strings.EqualFold(strings.TrimSpace(cmd.FullName()), redisCommandClusterInfo) {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), redisClusterDisabledMessage)
}

// isRedisScriptCacheMiss 判断当前错误是否为 Redis 脚本缓存未命中。
func isRedisScriptCacheMiss(err error, cmd redis.Cmder) bool {
	if err == nil || cmd == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(cmd.FullName())) {
	case redisCommandEvalSHA, redisCommandEvalSHARO:
	default:
		return false
	}
	if errors.Is(err, redis.ErrNoScript) || redis.HasErrorPrefix(err, redisNoScriptPrefix) {
		return true
	}
	message := err.Error()
	return strings.HasPrefix(message, redisNoScriptPrefix)
}

// pipelineNames 提取 pipeline 内所有命令名称，方便日志快速定位命令组成。
func pipelineNames(cmds []redis.Cmder) []string {
	names := make([]string, 0, len(cmds))
	for _, cmd := range cmds {
		names = append(names, cmd.FullName())
	}
	return names
}
