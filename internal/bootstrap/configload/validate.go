package configload

import (
	"path/filepath"
	"strings"

	"admin/common/idgen"
	"admin/internal/bootstrap/configload/validators"
	"admin/internal/config"
	"admin/internal/sharding"
	tasklimits "admin/internal/task/limits"

	utils "github.com/Is999/go-utils"
	"github.com/Is999/go-utils/errors"
)

const (
	minAdminJWTSecretLength          = 16      // JWT 密钥最小长度，避免明显弱配置启动
	maxConfigKeySegmentBytes         = 64      // Redis key 配置片段最大长度
	maxHotReloadIntervalSeconds      = 3600    // 配置文件轮询最大间隔
	maxVirusScanTimeoutSeconds       = 600     // 单文件病毒扫描最大超时秒数
	maxTaskConcurrency               = 256     // Worker 并发硬上限，避免误配置瞬间耗尽连接和 CPU
	maxTaskQueueCount                = 32      // 单实例监听队列数量上限，限制巡检和 Worker 元数据规模
	maxTaskQueueWeight               = 100     // 单队列权重上限，避免异常权重破坏消费公平性
	maxTaskShutdownSeconds           = 300     // Worker 优雅停止最长等待时间
	maxTaskPollSeconds               = 60      // 普通任务和延迟任务最长轮询间隔
	maxTaskGroupGraceSeconds         = 300     // 聚合等待窗口最大五分钟
	maxTaskGroupDelaySeconds         = 3600    // 聚合任务最大延迟一小时
	maxTaskGroupSize                 = 1000    // 单次聚合任务数量硬上限
	maxTaskHistoryRetentionDays      = 30      // 任务历史表最长保留三十天
	maxTaskHistoryFlushSeconds       = 10      // 历史收集器最长刷新间隔，配合批量追赶保证吞吐余量
	maxTaskHistoryCleanupSeconds     = 3600    // 历史清理最长间隔，避免高频表长期追不上保留边界
	maxTaskSchedulerLeaseSeconds     = 300     // 调度 leader 最长租约时间
	maxTaskSchedulerSyncSeconds      = 3600    // 周期配置最长同步间隔
	maxTaskSchedulerHeartbeatSeconds = 300     // 调度器最长心跳间隔
	maxTaskQueueBacklog              = 100000  // 周期投递积压硬上限，避免配置放大到事故规模
	defaultUserRouteShardCount       = 1       // 用户物理路由默认保持单表
	defaultUserExportSplitRows       = 500000  // 用户导出默认单文件最大行数
	maxUserExportSplitRows           = 1048575 // Excel 单 Sheet 扣除表头后的最大数据行数
)

// Validate 校验启动与热加载阶段必须立即失败的基础配置。
// 这里放置跨组件安全约束，避免错误配置进入运行态后才在后台组件里表现为半生效。
func Validate(c config.Config) error {
	if err := validateCoreConfig(c); err != nil {
		return errors.Tag(err)
	}
	if err := validateSnowflakeConfig(c.Snowflake); err != nil {
		return errors.Tag(err)
	}
	if err := validateUserConfig(c.User); err != nil {
		return errors.Tag(err)
	}
	if err := validateFileStorageConfig(c.FileStorage); err != nil {
		return errors.Tag(err)
	}
	if err := validateIPRegionConfig(c.IPRegion); err != nil {
		return errors.Tag(err)
	}
	if err := validateOpsConfig(c.Ops); err != nil {
		return errors.Tag(err)
	}
	if err := validateInternalServer(c.RestConf, c.InternalServer, c.Mode); err != nil {
		return errors.Tag(err)
	}
	if err := validateAPIService(c.APIService, c.Mode); err != nil {
		return errors.Tag(err)
	}
	if err := ValidateTaskRedisConfig(c.Task.Redis); err != nil {
		return errors.Wrap(err, "校验任务系统 Redis 配置失败")
	}
	if err := validateTaskLimits(c.Task); err != nil {
		return errors.Tag(err)
	}
	if err := validateArchiveLimits(c.Archive); err != nil {
		return errors.Tag(err)
	}
	if err := validators.ValidateCollector(c); err != nil {
		return errors.Tag(err)
	}
	if err := validateAlertConfig(c.Alert); err != nil {
		return errors.Tag(err)
	}
	if err := validators.ValidateSecurity(c); err != nil {
		return errors.Tag(err)
	}
	if err := validators.ValidateProduction(c); err != nil {
		return errors.Tag(err)
	}
	return nil
}

// validateArchiveLimits 校验进程内归档任务总量；数据库 active 快照加载后也必须遵守与草稿相同的一万条上限。
func validateArchiveLimits(cfg config.ArchiveConfig) error {
	if len(cfg.Jobs) > tasklimits.MaxArchiveJobCount {
		return errors.Errorf("archive.jobs 不能超过 %d 条", tasklimits.MaxArchiveJobCount)
	}
	return nil
}

// validateIPRegionConfig 校验离线归属地启用时至少提供一种 IP 版本的数据文件绝对路径。
func validateIPRegionConfig(cfg config.IPRegionConfig) error {
	if !cfg.Enabled {
		return nil
	}
	ipv4Path := strings.TrimSpace(cfg.IPv4XDBPath)
	ipv6Path := strings.TrimSpace(cfg.IPv6XDBPath)
	if ipv4Path == "" && ipv6Path == "" {
		return errors.Errorf("ip_region.enabled=true 时必须配置 ipv4_xdb_path 或 ipv6_xdb_path")
	}
	if ipv4Path != "" && !filepath.IsAbs(ipv4Path) {
		return errors.Errorf("ip_region.ipv4_xdb_path 必须使用绝对路径")
	}
	if ipv6Path != "" && !filepath.IsAbs(ipv6Path) {
		return errors.Errorf("ip_region.ipv6_xdb_path 必须使用绝对路径")
	}
	return nil
}

// validateFileStorageConfig 校验文件存储启动参数，避免首次上传时才暴露配置错误。
func validateFileStorageConfig(cfg config.FileStorageConfig) error {
	storageType := strings.TrimSpace(cfg.Type)
	if storageType == "" {
		storageType = "local"
	}
	if storageType != strings.ToLower(storageType) || (storageType != "local" && storageType != "s3") {
		return errors.Errorf("file_storage.type 仅支持 local/s3 小写规范值")
	}
	uploadMode := strings.TrimSpace(cfg.UploadMode)
	if uploadMode == "" {
		uploadMode = "server"
	}
	if uploadMode != strings.ToLower(uploadMode) || (uploadMode != "server" && uploadMode != "direct") {
		return errors.Errorf("file_storage.upload_mode 仅支持 server/direct 小写规范值")
	}
	if uploadMode == "direct" && (storageType != "s3" || !cfg.S3.Enabled) {
		return errors.Errorf("file_storage.upload_mode=direct 时必须启用 s3 存储")
	}
	if storageType == "s3" {
		if !cfg.S3.Enabled {
			return errors.Errorf("file_storage.type=s3 时必须设置 s3.enabled=true")
		}
		if strings.TrimSpace(cfg.S3.Bucket) == "" || strings.TrimSpace(cfg.S3.Region) == "" {
			return errors.Errorf("file_storage.type=s3 时 bucket 和 region 不能为空")
		}
		accessKey := strings.TrimSpace(cfg.S3.AccessKey)
		secretKey := strings.TrimSpace(cfg.S3.SecretKey)
		if (accessKey == "") != (secretKey == "") {
			return errors.Errorf("file_storage.s3.access_key 和 secret_key 必须同时配置")
		}
	}
	name := strings.TrimSpace(cfg.VirusScanner.Name)
	if name != strings.ToLower(name) {
		return errors.Errorf("file_storage.virus_scanner.name 必须使用小写规范值")
	}
	if name != "" && name != config.VirusScannerNoop && name != config.VirusScannerClamAV {
		return errors.Errorf("file_storage.virus_scanner.name 仅支持 noop/clamav")
	}
	if timeout := cfg.VirusScanner.TimeoutSeconds; timeout < 0 || timeout > maxVirusScanTimeoutSeconds {
		return errors.Errorf("file_storage.virus_scanner.timeout_seconds 必须在 0-%d 之间", maxVirusScanTimeoutSeconds)
	}
	return nil
}

// validateTaskLimits 校验任务默认值和周期任务覆盖值，避免配置绕过接口硬上限。
func validateTaskLimits(cfg config.TaskQueueConfig) error {
	if cfg.Concurrency < 0 || cfg.Concurrency > maxTaskConcurrency {
		return errors.Errorf("task.concurrency 必须在 0-%d 之间", maxTaskConcurrency)
	}
	if len(cfg.Queues) > maxTaskQueueCount {
		return errors.Errorf("task.queues 不能超过 %d 个", maxTaskQueueCount)
	}
	for queue, weight := range cfg.Queues {
		if queue != strings.TrimSpace(queue) || !validConfigKeySegment(queue) {
			return errors.Errorf("task.queues 队列名只能包含字母、数字、点、下划线或短横线，且长度不能超过 %d", maxConfigKeySegmentBytes)
		}
		if weight <= 0 || weight > maxTaskQueueWeight {
			return errors.Errorf("task.queues[%s] 权重必须在 1-%d 之间", queue, maxTaskQueueWeight)
		}
	}
	if queue := strings.TrimSpace(cfg.DefaultQueue); queue != "" && !validConfigKeySegment(queue) {
		return errors.Errorf("task.default_queue 只能包含字母、数字、点、下划线或短横线，且长度不能超过 %d", maxConfigKeySegmentBytes)
	}
	defaultQueue := strings.TrimSpace(cfg.DefaultQueue)
	if defaultQueue == "" {
		defaultQueue = "default"
	}
	if len(cfg.Queues) > 0 {
		if _, ok := cfg.Queues[defaultQueue]; !ok {
			return errors.Errorf("task.default_queue=%s 未在 task.queues 中配置消费", defaultQueue)
		}
	}
	if cfg.DefaultRetry < 0 || cfg.DefaultRetry > tasklimits.MaxRetry {
		return errors.Errorf("task.default_retry 必须在 0-%d 之间", tasklimits.MaxRetry)
	}
	if cfg.DefaultTimeoutSeconds < 0 || cfg.DefaultTimeoutSeconds > tasklimits.MaxTimeoutSeconds {
		return errors.Errorf("task.default_timeout_seconds 必须在 0-%d 之间", tasklimits.MaxTimeoutSeconds)
	}
	if seconds := cfg.DefaultUniqueTTLSeconds; seconds < 0 || seconds > tasklimits.MaxUniqueTTLSeconds {
		return errors.Errorf("task.default_unique_ttl_seconds 必须在 0-%d 之间", tasklimits.MaxUniqueTTLSeconds)
	}
	if seconds := cfg.CompletedRetentionSeconds; seconds != 0 && (seconds < 600 || seconds > 7200) {
		return errors.Errorf("task.completed_retention_seconds 必须为 0 或 600-7200")
	}
	if seconds := cfg.ArchivedRetentionSeconds; seconds != 0 && (seconds < 86400 || seconds > 30*86400) {
		return errors.Errorf("task.archived_retention_seconds 必须为 0 或 86400-%d", 30*86400)
	}
	if seconds := cfg.ShutdownTimeoutSeconds; seconds < 0 || seconds > maxTaskShutdownSeconds {
		return errors.Errorf("task.shutdown_timeout_seconds 必须在 0-%d 之间", maxTaskShutdownSeconds)
	}
	if seconds := cfg.TaskCheckSeconds; seconds < 0 || seconds > maxTaskPollSeconds {
		return errors.Errorf("task.task_check_seconds 必须在 0-%d 之间", maxTaskPollSeconds)
	}
	if seconds := cfg.DelayedTaskCheckSeconds; seconds < 0 || seconds > maxTaskPollSeconds {
		return errors.Errorf("task.delayed_task_check_seconds 必须在 0-%d 之间", maxTaskPollSeconds)
	}
	if seconds := cfg.GroupGracePeriodSeconds; seconds < 0 || seconds > maxTaskGroupGraceSeconds {
		return errors.Errorf("task.group_grace_period_seconds 必须在 0-%d 之间", maxTaskGroupGraceSeconds)
	}
	if seconds := cfg.GroupMaxDelaySeconds; seconds < 0 || seconds > maxTaskGroupDelaySeconds {
		return errors.Errorf("task.group_max_delay_seconds 必须在 0-%d 之间", maxTaskGroupDelaySeconds)
	}
	if size := cfg.GroupMaxSize; size < 0 || size > maxTaskGroupSize {
		return errors.Errorf("task.group_max_size 必须在 0-%d 之间", maxTaskGroupSize)
	}
	graceSeconds := cfg.GroupGracePeriodSeconds
	if graceSeconds == 0 {
		graceSeconds = 5
	}
	delaySeconds := cfg.GroupMaxDelaySeconds
	if delaySeconds == 0 {
		delaySeconds = 20
	}
	if graceSeconds > delaySeconds {
		return errors.Errorf("task.group_grace_period_seconds 不能大于 group_max_delay_seconds")
	}
	if days := cfg.History.WorkflowRetentionDays; days < 0 || days > maxTaskHistoryRetentionDays {
		return errors.Errorf("task.history.workflow_retention_days 必须在 0-%d 之间", maxTaskHistoryRetentionDays)
	}
	if days := cfg.History.TaskRetentionDays; days < 0 || days > maxTaskHistoryRetentionDays {
		return errors.Errorf("task.history.task_retention_days 必须在 0-%d 之间", maxTaskHistoryRetentionDays)
	}
	if days := cfg.History.FailureRetentionDays; days < 0 || days > maxTaskHistoryRetentionDays {
		return errors.Errorf("task.history.failure_retention_days 必须在 0-%d 之间", maxTaskHistoryRetentionDays)
	}
	if limit := cfg.History.PendingLimit; limit < 0 || limit > 10000 {
		return errors.Errorf("task.history.pending_limit 必须在 0-10000 之间")
	}
	if seconds := cfg.History.FlushIntervalSeconds; seconds < 0 || seconds > maxTaskHistoryFlushSeconds {
		return errors.Errorf("task.history.flush_interval_seconds 必须在 0-%d 之间", maxTaskHistoryFlushSeconds)
	}
	if seconds := cfg.History.CleanupIntervalSeconds; seconds != 0 && (seconds < 30 || seconds > maxTaskHistoryCleanupSeconds) {
		return errors.Errorf("task.history.cleanup_interval_seconds 必须为 0 或 30-%d", maxTaskHistoryCleanupSeconds)
	}
	if seconds := cfg.Scheduler.LeaseTTLSeconds; seconds < 0 || seconds > maxTaskSchedulerLeaseSeconds {
		return errors.Errorf("task.scheduler.lease_ttl_seconds 必须在 0-%d 之间", maxTaskSchedulerLeaseSeconds)
	}
	if seconds := cfg.Scheduler.RenewIntervalSeconds; seconds < 0 || seconds > maxTaskSchedulerHeartbeatSeconds {
		return errors.Errorf("task.scheduler.renew_interval_seconds 必须在 0-%d 之间", maxTaskSchedulerHeartbeatSeconds)
	}
	leaseSeconds := cfg.Scheduler.LeaseTTLSeconds
	if leaseSeconds == 0 {
		leaseSeconds = 30
	}
	renewSeconds := cfg.Scheduler.RenewIntervalSeconds
	if renewSeconds == 0 {
		renewSeconds = 10
	}
	if renewSeconds >= leaseSeconds {
		return errors.Errorf("task.scheduler.renew_interval_seconds 必须小于 lease_ttl_seconds")
	}
	if seconds := cfg.Scheduler.SyncIntervalSeconds; seconds < 0 || seconds > maxTaskSchedulerSyncSeconds {
		return errors.Errorf("task.scheduler.sync_interval_seconds 必须在 0-%d 之间", maxTaskSchedulerSyncSeconds)
	}
	if seconds := cfg.Scheduler.HeartbeatIntervalSeconds; seconds < 0 || seconds > maxTaskSchedulerHeartbeatSeconds {
		return errors.Errorf("task.scheduler.heartbeat_interval_seconds 必须在 0-%d 之间", maxTaskSchedulerHeartbeatSeconds)
	}
	if backlog := cfg.Scheduler.MaxQueueBacklog; backlog < 0 || backlog > maxTaskQueueBacklog {
		return errors.Errorf("task.scheduler.max_queue_backlog 必须在 0-%d 之间", maxTaskQueueBacklog)
	}
	if len(cfg.Periodic) > tasklimits.MaxPeriodicCount {
		return errors.Errorf("task.periodic 不能超过 %d 条", tasklimits.MaxPeriodicCount)
	}
	for index, item := range cfg.Periodic {
		queue := strings.TrimSpace(item.Queue)
		if queue == "" {
			queue = defaultQueue
		}
		if len(cfg.Queues) > 0 {
			if _, ok := cfg.Queues[queue]; !ok {
				return errors.Errorf("task.periodic[%d].queue=%s 未在 task.queues 中配置消费", index, queue)
			}
		}
		if err := tasklimits.ValidateWorkflowTargets(item.Targets); err != nil {
			return errors.Wrapf(err, "task.periodic[%d].targets 非法", index)
		}
		if len(strings.TrimSpace(item.UniqueKey)) > tasklimits.MaxUniqueKeyBytes {
			return errors.Errorf("task.periodic[%d].unique_key 不能超过 %d 字节", index, tasklimits.MaxUniqueKeyBytes)
		}
		if item.Retry < 0 || item.Retry > tasklimits.MaxRetry {
			return errors.Errorf("task.periodic[%d].retry 必须在 0-%d 之间", index, tasklimits.MaxRetry)
		}
		if item.TimeoutSeconds < 0 || item.TimeoutSeconds > tasklimits.MaxTimeoutSeconds {
			return errors.Errorf("task.periodic[%d].timeout_seconds 必须在 0-%d 之间", index, tasklimits.MaxTimeoutSeconds)
		}
		if item.ShardTotal < 0 || item.ShardTotal > tasklimits.MaxShardTotal {
			return errors.Errorf("task.periodic[%d].shard_total 必须在 0-%d 之间", index, tasklimits.MaxShardTotal)
		}
		if item.GrayPercent < 0 || item.GrayPercent > 100 {
			return errors.Errorf("task.periodic[%d].gray_percent 必须在 0-100 之间", index)
		}
		if item.UniqueTTLSeconds < 0 || item.UniqueTTLSeconds > tasklimits.MaxUniqueTTLSeconds {
			return errors.Errorf("task.periodic[%d].unique_ttl_seconds 必须在 0-%d 之间", index, tasklimits.MaxUniqueTTLSeconds)
		}
		if item.EverySeconds < 0 || (item.EverySeconds > 0 && item.EverySeconds < tasklimits.MinPeriodicEverySeconds) {
			return errors.Errorf("task.periodic[%d].every_seconds 必须为 0 或不小于 %d", index, tasklimits.MinPeriodicEverySeconds)
		}
	}
	return nil
}

// validateCoreConfig 校验所有环境都必须满足的基础配置。
func validateCoreConfig(c config.Config) error {
	if len(strings.TrimSpace(c.JwtSecret)) < minAdminJWTSecretLength {
		return errors.Errorf("jwt_secret 长度不能小于 %d", minAdminJWTSecretLength)
	}
	if strings.TrimSpace(c.AppID) == "" {
		return errors.Errorf("app_id 不能为空")
	}
	if !validConfigKeySegment(c.AppID) {
		return errors.Errorf("app_id 只能包含字母、数字、点、下划线或短横线，且长度不能超过 %d", maxConfigKeySegmentBytes)
	}
	if _, err := utils.NewTrustedProxies(c.TrustedProxies...); err != nil {
		return errors.Wrap(err, "trusted_proxies 配置非法")
	}
	if err := validateMainRedisConfig(c.Redis); err != nil {
		return errors.Tag(err)
	}
	if interval := c.HotReload.CheckIntervalSeconds; interval < 0 || interval > maxHotReloadIntervalSeconds {
		return errors.Errorf("hot_reload.check_interval_seconds 必须为 0 或 1-%d", maxHotReloadIntervalSeconds)
	}
	if ratio := c.Observability.SampleRatio; ratio < 0 || ratio > 1 {
		return errors.Errorf("observability.sample_ratio 必须在 0-1 之间")
	}
	return nil
}

// validateMainRedisConfig 校验主 Redis 拓扑和连接池必填参数。
func validateMainRedisConfig(cfg config.RedisConfig) error {
	if err := validateRedisTopology("redis", cfg); err != nil {
		return errors.Tag(err)
	}
	if cfg.PoolSize <= 0 {
		return errors.Errorf("redis.pool_size 必须大于 0")
	}
	return nil
}

// validateRedisTopology 校验 Redis 模式、地址和数据库编号的一致性。
func validateRedisTopology(name string, cfg config.RedisConfig) error {
	addresses := make([]string, 0, len(cfg.Addrs))
	for _, rawAddress := range cfg.Addrs {
		address := strings.TrimSpace(rawAddress)
		if address == "" || address != rawAddress {
			return errors.Errorf("%s.addrs 不能包含空地址或首尾空白", name)
		}
		addresses = append(addresses, address)
	}
	if len(addresses) == 0 {
		return errors.Errorf("%s.addrs 不能为空", name)
	}

	redisType := strings.ToLower(strings.TrimSpace(cfg.Type))
	if strings.TrimSpace(cfg.Type) != redisType {
		return errors.Errorf("%s.type 必须使用小写规范值", name)
	}
	// admin 存量配置允许不填 type：单地址走单机，多地址走集群。
	if redisType == "" {
		redisType = "single"
		if len(addresses) > 1 {
			redisType = "cluster"
		}
	}
	switch redisType {
	case "single", "standalone":
		if len(addresses) != 1 {
			return errors.Errorf("%s.type=single 时 addrs 必须且只能配置一个地址", name)
		}
		if len(cfg.AddrMap) > 0 {
			return errors.Errorf("%s.addr_map 仅支持 cluster 模式", name)
		}
	case "cluster":
		if cfg.DB != 0 {
			return errors.Errorf("%s.type=cluster 时 db 必须为 0", name)
		}
		for rawAddress, rawMappedAddress := range cfg.AddrMap {
			address := strings.TrimSpace(rawAddress)
			mappedAddress := strings.TrimSpace(rawMappedAddress)
			if address == "" || mappedAddress == "" || address != rawAddress || mappedAddress != rawMappedAddress {
				return errors.Errorf("%s.addr_map 不能包含空 key/value 或首尾空白", name)
			}
		}
	default:
		return errors.Errorf("%s.type 仅支持 single/standalone/cluster", name)
	}
	if cfg.DB < 0 {
		return errors.Errorf("%s.db 不能小于 0", name)
	}
	return nil
}

// validateSnowflakeConfig 校验分布式雪花 ID worker 配置。
func validateSnowflakeConfig(cfg config.SnowflakeConfig) error {
	if cfg.Redis.Enabled {
		if err := validateSnowflakeRedisConfig(cfg); err != nil {
			return errors.Tag(err)
		}
	} else if _, err := resolveSnowflakeWorkerID(cfg); err != nil {
		return errors.Tag(err)
	}
	if err := validateIDSegmentConfig(cfg); err != nil {
		return errors.Tag(err)
	}
	return nil
}

// validateSnowflakeRedisConfig 校验 Redis 租约 node_id 分配参数。
func validateSnowflakeRedisConfig(cfg config.SnowflakeConfig) error {
	if cfg.WorkerID != nil {
		return errors.Errorf("snowflake.redis.enabled=true 时不能同时配置 snowflake.worker_id")
	}
	redisCfg := normalizeSnowflakeRedisConfig(cfg.Redis)
	if !validConfigKeySegment(redisCfg.Scope) {
		return errors.Errorf("snowflake.redis.scope 只能包含安全 key 字符且长度不能超过 %d", maxConfigKeySegmentBytes)
	}
	if redisCfg.LeaseSeconds < minSnowflakeRedisLeaseSeconds || redisCfg.LeaseSeconds > maxSnowflakeRedisLeaseSeconds {
		return errors.Errorf("snowflake.redis.lease_seconds 必须在 %d-%d 之间", minSnowflakeRedisLeaseSeconds, maxSnowflakeRedisLeaseSeconds)
	}
	if redisCfg.RenewIntervalSeconds <= 0 || redisCfg.RenewIntervalSeconds > maxSnowflakeRedisLeaseSeconds {
		return errors.Errorf("snowflake.redis.renew_interval_seconds 必须在 1-%d 之间", maxSnowflakeRedisLeaseSeconds)
	}
	if redisCfg.RenewIntervalSeconds >= redisCfg.LeaseSeconds-redisCfg.RenewIntervalSeconds {
		return errors.Errorf("snowflake.redis.renew_interval_seconds 必须小于 lease_seconds 的一半")
	}
	if err := validateSnowflakeRedisNamespaces(cfg.Redis.Namespaces); err != nil {
		return errors.Tag(err)
	}
	return nil
}

// validateSnowflakeRedisNamespaces 校验业务 namespace 的 node_id 池覆盖配置。
func validateSnowflakeRedisNamespaces(items map[string]config.SnowflakeRedisNamespaceConfig) error {
	seen := make(map[string]struct{}, len(items))
	for rawNamespace := range items {
		namespace := idgen.NormalizeNamespace(rawNamespace)
		if namespace == "" {
			return errors.Errorf("snowflake.redis.namespaces 包含空 namespace")
		}
		if _, exists := seen[namespace]; exists {
			return errors.Errorf("snowflake.redis.namespaces 规范化后重复: %s", namespace)
		}
		seen[namespace] = struct{}{}
	}
	for rawNamespace, item := range items {
		namespace := idgen.NormalizeNamespace(rawNamespace)
		if rawNamespace != namespace || namespace != strings.ToLower(namespace) {
			return errors.Errorf("snowflake.redis.namespaces.%s 必须使用无首尾空白的小写名称", rawNamespace)
		}
		if !validConfigKeySegment(namespace) {
			return errors.Errorf("snowflake.redis.namespaces.%s 包含非法 key 字符", rawNamespace)
		}
		if item.NodeCount < 0 || item.NodeCount > int(idgen.SnowflakeMaxWorkerID+1) {
			return errors.Errorf("snowflake.redis.namespaces.%s.node_count 必须在 0-%d 之间", namespace, idgen.SnowflakeMaxWorkerID+1)
		}
	}
	return nil
}

// validateIDSegmentConfig 校验高吞吐业务 Redis Segment 号段配置。
func validateIDSegmentConfig(cfg config.SnowflakeConfig) error {
	if !cfg.Segment.Enabled {
		return nil
	}
	segmentCfg := normalizeIDSegmentConfig(cfg.Segment, cfg.Redis)
	if !validConfigKeySegment(segmentCfg.Scope) {
		return errors.Errorf("snowflake.segment.scope 只能包含安全 key 字符且长度不能超过 %d", maxConfigKeySegmentBytes)
	}
	if segmentCfg.AllocateTimeoutSeconds <= 0 || segmentCfg.AllocateTimeoutSeconds > maxIDSegmentAllocateTimeoutSeconds {
		return errors.Errorf("snowflake.segment.allocate_timeout_seconds 必须在 1-%d 之间", maxIDSegmentAllocateTimeoutSeconds)
	}
	enabledNamespaces := 0
	seen := make(map[string]struct{}, len(cfg.Segment.Namespaces))
	for rawNamespace := range cfg.Segment.Namespaces {
		namespace := idgen.NormalizeNamespace(rawNamespace)
		if namespace == "" {
			return errors.Errorf("snowflake.segment.namespaces 包含空 namespace")
		}
		if _, exists := seen[namespace]; exists {
			return errors.Errorf("snowflake.segment.namespaces 规范化后重复: %s", namespace)
		}
		seen[namespace] = struct{}{}
	}
	for rawNamespace, rawItem := range cfg.Segment.Namespaces {
		namespace := idgen.NormalizeNamespace(rawNamespace)
		if rawNamespace != namespace || namespace != strings.ToLower(namespace) {
			return errors.Errorf("snowflake.segment.namespaces.%s 必须使用无首尾空白的小写名称", rawNamespace)
		}
		if !validConfigKeySegment(namespace) {
			return errors.Errorf("snowflake.segment.namespaces.%s 包含非法 key 字符", rawNamespace)
		}
		item := normalizeIDSegmentNamespaceConfig(rawItem)
		if !item.Enabled {
			continue
		}
		enabledNamespaces++
		if item.Step <= 0 || item.Step > maxIDSegmentStep {
			return errors.Errorf("snowflake.segment.namespaces.%s.step 必须在 1-%d 之间", namespace, maxIDSegmentStep)
		}
		if item.PrefetchThreshold < 0 || item.PrefetchThreshold >= item.Step {
			return errors.Errorf("snowflake.segment.namespaces.%s.prefetch_threshold 必须小于 step 且不能为负数", namespace)
		}
		if item.Start < 0 {
			return errors.Errorf("snowflake.segment.namespaces.%s.start 不能小于 0", namespace)
		}
	}
	if enabledNamespaces == 0 {
		return errors.Errorf("snowflake.segment.enabled=true 时必须至少启用一个 namespace")
	}
	return nil
}

// resolveSnowflakeWorkerID 解析配置或环境变量中的显式 worker_id。
func resolveSnowflakeWorkerID(cfg config.SnowflakeConfig) (int64, error) {
	if cfg.WorkerID == nil {
		return idgen.ResolveWorkerID(idgen.SnowflakeWorkerIDUnset)
	}
	return idgen.ResolveWorkerID(*cfg.WorkerID)
}

// ConfigureSnowflakeWorkerID 发布当前进程使用的雪花 ID worker 配置。
func ConfigureSnowflakeWorkerID(cfg config.SnowflakeConfig) error {
	if cfg.Redis.Enabled {
		return errors.New("snowflake.redis.enabled=true 时必须通过 Redis 租约配置雪花 node_id")
	}
	workerID, err := resolveSnowflakeWorkerID(cfg)
	if err != nil {
		return errors.Tag(err)
	}
	return idgen.ConfigureWorkerID(workerID)
}

// validateUserConfig 校验用户物理分片数和后台导出配置。
func validateUserConfig(cfg config.UserConfig) error {
	if cfg.RouteShardCount != 0 && !sharding.ValidCount(cfg.RouteShardCount) {
		return errors.Errorf("user.route_shard_count 仅支持 1/2/4/8/16/32/64/128/256/512/1024")
	}
	if cfg.ExportSplitRows < 0 {
		return errors.Errorf("user.export_split_rows 不能小于 0")
	}
	if cfg.ExportSplitRows > maxUserExportSplitRows {
		return errors.Errorf("user.export_split_rows 不能大于 %d", maxUserExportSplitRows)
	}
	return nil
}

// validConfigKeySegment 校验会参与 Redis key 的短标识。
func validConfigKeySegment(value string) bool {
	trimmed := strings.TrimSpace(value)
	if value != trimmed || trimmed == "" || len(trimmed) > maxConfigKeySegmentBytes {
		return false
	}
	for _, char := range trimmed {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '.' || char == '_' || char == '-' {
			continue
		}
		return false
	}
	return true
}

// HasTaskRedisConfig 判断是否为任务系统显式配置了独立 Redis。
func HasTaskRedisConfig(cfg config.RedisConfig) bool {
	for _, addr := range cfg.Addrs {
		if strings.TrimSpace(addr) != "" {
			return true
		}
	}
	return false
}

// ValidateTaskRedisConfig 校验任务系统独立 Redis 配置是否完整。
// task.redis 只要出现任一非地址字段，就必须显式配置 addrs，避免半配置静默复用主 Redis。
func ValidateTaskRedisConfig(cfg config.RedisConfig) error {
	if !taskRedisConfigTouched(cfg) {
		return nil
	}
	if !HasTaskRedisConfig(cfg) {
		return errors.Errorf("task.redis 已配置非地址字段时必须同时配置 addrs")
	}
	return errors.Tag(validateRedisTopology("task.redis", cfg))
}

// taskRedisConfigTouched 判断 task.redis 是否被显式配置过。
func taskRedisConfigTouched(cfg config.RedisConfig) bool {
	return strings.TrimSpace(cfg.Type) != "" ||
		len(cfg.Addrs) > 0 ||
		len(cfg.AddrMap) > 0 ||
		strings.TrimSpace(cfg.Password) != "" ||
		cfg.DB != 0 ||
		cfg.PoolSize != 0 ||
		cfg.TLS ||
		cfg.TLSInsecureSkipVerify
}

// validateAlertConfig 校验外部告警通道的最小可用配置。
func validateAlertConfig(cfg config.AlertConfig) error {
	if !cfg.Lark.Enabled {
		return nil
	}
	if strings.TrimSpace(cfg.Lark.WebhookURL) == "" && strings.TrimSpace(cfg.Lark.WebhookURLRef) == "" {
		return errors.Errorf("alert.lark.enabled=true 时必须配置 webhook_url 或 webhook_url_ref")
	}
	if cfg.Lark.TimeoutSeconds < 0 {
		return errors.Errorf("alert.lark.timeout_seconds 不能小于 0")
	}
	if cfg.Lark.MaxErrorBytes < 0 {
		return errors.Errorf("alert.lark.max_error_bytes 不能小于 0")
	}
	return nil
}
