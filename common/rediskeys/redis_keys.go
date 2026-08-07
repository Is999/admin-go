package keys

// Redis key 根前缀集中维护。
const (
	// ScopeRoot 表示 Redis 缓存和锁的 app_id 命名空间根前缀。
	// Redis 类型：命名空间前缀，TTL 过期规则：不直接写入 Redis。
	ScopeRoot = "app:"
	// rateLimitRedisRoot 表示项目分布式限流状态的二级业务前缀。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由具体限流算法管理状态和 TTL。
	// GCRA 逻辑 key 会由 redis_rate 在物理 key 前追加固定的 `rate:` 前缀。
	rateLimitRedisRoot = "rate_limit"
	// rateLimitFixedIntervalSegment 表示固定间隔限流状态的算法隔离段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由固定间隔限流令牌的 interval 控制。
	rateLimitFixedIntervalSegment = "fixed_interval"
)

// table-cache Redis key 二级前缀集中维护。
const (
	// tableCacheSegment 表示 table-cache 业务在应用命名空间下的二级前缀。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由具体 table-cache 业务 key 的调用方 TTL 控制。
	tableCacheSegment = "table"
)

// 任务队列 Redis key 根前缀和运行段集中维护。
const (
	// taskQueueRedisRoot 表示任务系统自管 key 的二级业务前缀。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由具体任务 key 的调用方 TTL 控制。
	taskQueueRedisRoot = "task"
	// taskRuntimeSegment 表示任务运行快照 key 的领域段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由任务运行快照 TTL 控制。
	taskRuntimeSegment = "runtime"
	// taskHistorySegment 表示终态历史异步落库缓冲区的领域段。
	// Redis 类型：Key 片段，TTL 过期规则：缓冲区由容量上限和消费确认主动清理。
	taskHistorySegment = "history"
)

// Collector Redis key 根前缀和运行段集中维护。
const (
	// collectorRedisRoot 表示 Collector 自管 key 的二级业务前缀。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由具体 Collector key 的调用方 TTL 控制。
	collectorRedisRoot = "collector"
	// collectorIdempotencySegment 表示 Collector 单任务 EventID 幂等去重 key 段。
	// Redis 类型：String，TTL 过期规则：由 Collector 幂等终态 TTL 或处理中租约 TTL 控制。
	collectorIdempotencySegment = "idempotency"
)

// Asynq Redis key 片段集中维护。
const (
	// taskAsynqRedisRoot 表示 Asynq 框架固定 Redis 根前缀。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由 Asynq 内部 key 生命周期控制。
	taskAsynqRedisRoot = "asynq"
	// taskAsynqStatePending 表示 Asynq pending 状态 list 段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由 Asynq pending 队列生命周期控制。
	taskAsynqStatePending = "pending"
	// taskAsynqStateActive 表示 Asynq active 状态 list 段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由 Asynq active 队列生命周期控制。
	taskAsynqStateActive = "active"
	// taskAsynqStateRetry 表示 Asynq retry 状态 zset 段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由 Asynq retry 队列生命周期控制。
	taskAsynqStateRetry = "retry"
	// taskAsynqStateArchived 表示 Asynq archived 状态 zset 段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由 Asynq archived 队列生命周期控制。
	taskAsynqStateArchived = "archived"
	// taskAsynqStateCompleted 表示 Asynq completed 状态 zset 段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由 Asynq completed 队列生命周期控制。
	taskAsynqStateCompleted = "completed"
	// taskAsynqStateScheduled 表示 Asynq scheduled 状态 zset 段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由 Asynq scheduled 队列生命周期控制。
	taskAsynqStateScheduled = "scheduled"
	// taskAsynqPausedSegment 表示 Asynq 队列暂停标记段。
	// Redis 类型：Key 片段，TTL 过期规则：由 Asynq 暂停和恢复操作管理。
	taskAsynqPausedSegment = "paused"
	// taskAsynqProcessedSegment 表示 Asynq 已处理计数段。
	// Redis 类型：Key 片段，TTL 过期规则：由 Asynq 统计生命周期管理。
	taskAsynqProcessedSegment = "processed"
	// taskAsynqFailedSegment 表示 Asynq 失败计数段。
	// Redis 类型：Key 片段，TTL 过期规则：由 Asynq 统计生命周期管理。
	taskAsynqFailedSegment = "failed"
	// taskAsynqTaskHashSegment 表示 Asynq 任务详情 hash 段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由 Asynq 任务详情生命周期控制。
	taskAsynqTaskHashSegment = "t"
	// taskAsynqGroupsSegment 表示 Asynq 聚合组集合段。
	// Redis 类型：Set，TTL 过期规则：由 Asynq 聚合生命周期控制。
	taskAsynqGroupsSegment = "groups"
	// taskAsynqGroupSegment 表示 Asynq 单个聚合组 ZSet 段。
	// Redis 类型：ZSet，TTL 过期规则：由 Asynq 聚合生命周期控制。
	taskAsynqGroupSegment = "g"
	// taskAsynqUniqueSegment 表示 Asynq 任务唯一锁 key 段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由 Asynq 唯一锁 TTL 控制。
	taskAsynqUniqueSegment = "unique"
)

// 任务工作流 Redis key 片段集中维护。
const (
	// taskWorkflowSegment 表示工作流状态 key 的领域段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由具体工作流 key 的调用方 TTL 控制。
	taskWorkflowSegment = "workflow"
	// taskWorkflowNodeSegment 表示工作流节点状态 key 段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由具体工作流节点 key 的调用方 TTL 控制。
	taskWorkflowNodeSegment = "node"
	// taskWorkflowMetaSegment 表示工作流主记录 key 段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由工作流主记录 key 的调用方 TTL 控制。
	taskWorkflowMetaSegment = "meta"
	// taskWorkflowNodesSegment 表示工作流节点集合 key 段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由工作流节点集合 key 的调用方 TTL 控制。
	taskWorkflowNodesSegment = "nodes"
	// taskWorkflowUniqueSegment 表示工作流幂等占位 key 段。
	// Redis 类型：Key 片段，TTL 过期规则：不直接写入 Redis，由工作流幂等 key TTL 控制。
	taskWorkflowUniqueSegment = "unique"
)

// 用户标签工作流 Redis key 片段集中维护。
const (
	// UserTagWorkflowUniqueSegment 表示用户标签工作流默认去重键片段模板。
	// Redis 类型：任务工作流唯一键片段，TTL 过期规则：不直接写入 Redis，由工作流唯一键 TTL 控制。
	// 第一个 `%s` 位置填充 mode，第二个 `%x` 位置填充规范化参数摘要。
	UserTagWorkflowUniqueSegment = "user_tag:%s:%x"
)

// Redis Key 模板集中维护，业务代码只能按模板精确读写。
const (
	// SnowflakeNodeLease 表示跨 admin/api 共享的雪花 node_id 租约 key 模板。
	// Redis 类型：String(owner)，TTL 过期规则：按 snowflake.redis.lease_seconds 自动过期并由实例续约。
	// 参数依次为部署级 scope、业务 namespace、node_id；该 key 不追加 app_id 前缀，确保同一业务统一互斥。
	SnowflakeNodeLease = "snowflake:node:%s:%s:%d"

	// IDSegmentCounter 表示跨 admin/api 共享的业务号段高水位 key 模板。
	// Redis 类型：String(integer)，TTL 过期规则：无 TTL；每次按业务 namespace 使用 INCRBY 分配本地号段。
	// 参数依次为部署级 scope、业务 namespace；该 key 不追加 app_id 前缀，确保同一业务统一递增。
	IDSegmentCounter = "idgen:segment:%s:%s"

	// AdminSession 表示管理员会话缓存业务段模板。
	// Redis 类型：Hash，TTL 过期规则：跟随 JwtExpiresIn，登录和刷新令牌时续期。
	// `%d` 位置填充管理员 ID，调用侧通过 WithPrefix 追加 app_id 前缀。
	AdminSession = "admin:session:%d"

	// AdminSessionPattern 表示管理员会话缓存业务段展示模板。
	// Redis 类型：Hash 模板，TTL 过期规则：不直接写入 Redis，仅用于展示或匹配真实 key。
	AdminSessionPattern = "admin:session:{adminID}"

	// SecurityCacheSyncBarrier 表示安全缓存存在待补偿失效任务。
	// Redis 类型：String，TTL 过期规则：无 TTL，待补偿任务全部完成后精确删除。
	SecurityCacheSyncBarrier = "security:cache_sync:barrier"

	// SecurityCacheSyncLock 表示安全缓存失效补偿 worker 的分布式锁。
	// Redis 类型：String（由 redsync 管理），TTL 过期规则：由 redsync 锁 TTL 控制，到期自动释放。
	SecurityCacheSyncLock = "security:cache_sync:lock"

	// RoleStatus 表示角色状态缓存键。
	// Redis 类型：Hash，TTL 过期规则：由 table-cache 鉴权目标统一控制。
	RoleStatus = "role_status"

	// RolePermission 表示角色路由权限关系缓存键模板。
	// Redis 类型：Set，成员为原始权限 ID；TTL 过期规则：由 table-cache 鉴权目标统一控制。
	// `%d` 位置填充角色 ID。
	RolePermission = "role_permission:%d"

	// RolePermissionPattern 表示角色路由权限关系缓存键展示模板。
	// Redis 类型：Set 模板，TTL 过期规则：不直接写入 Redis，仅用于展示或匹配真实 key。
	RolePermissionPattern = "role_permission:{roleID}"

	// RoleDocPermission 表示角色文档权限关系缓存键模板。
	// Redis 类型：Set，成员为原始文档权限 ID；TTL 过期规则：由 table-cache 鉴权目标统一控制。
	// `%d` 位置填充角色 ID。
	RoleDocPermission = "role_doc_permission:%d"

	// RoleDocPermissionPattern 表示角色文档权限关系缓存键展示模板。
	// Redis 类型：Set 模板，TTL 过期规则：不直接写入 Redis，仅用于展示或匹配真实 key。
	RoleDocPermissionPattern = "role_doc_permission:{roleID}"

	// RBACWriteLock 表示角色、权限和授权关系写操作互斥锁。
	// Redis 类型：String（由 redsync 管理），TTL 过期规则：由 redsync 锁 TTL 控制，到期自动释放。
	RBACWriteLock = "admin:rbac:write:lock"

	// RoleTree 表示角色树缓存键。
	// Redis 类型：String（JSON 文本），TTL 过期规则：无固定 TTL，按角色变更精确失效。
	RoleTree = "role_tree"

	// AdminRoleIDs 表示管理员角色关系缓存键模板。
	// Redis 类型：Set，成员为原始角色 ID；TTL 过期规则：由 table-cache 鉴权目标统一控制。
	// `%d` 位置填充管理员 ID。
	AdminRoleIDs = "admin_role_ids:%d"

	// AdminRoleIDsPattern 表示管理员角色关系缓存键展示模板。
	// Redis 类型：Set 模板，TTL 过期规则：不直接写入 Redis，仅用于展示或匹配真实 key。
	AdminRoleIDsPattern = "admin_role_ids:{adminID}"

	// AdminRolesDetail 表示管理员角色名称列表缓存键模板。
	// Redis 类型：String（JSON 文本），TTL 过期规则：由 table-cache 目标配置为 1 小时，并按管理员角色变更精确失效。
	// `%d` 位置填充管理员 ID。
	AdminRolesDetail = "admin_roles_detail:%d"

	// AdminRolesDetailPattern 表示管理员角色名称列表缓存键展示模板。
	// Redis 类型：String 模板，TTL 过期规则：不直接写入 Redis，仅用于展示或匹配真实 key。
	AdminRolesDetailPattern = "admin_roles_detail:{adminID}"

	// RoutePermissionIDs 表示启用路由别名到权限 ID 的反向索引缓存键。
	// Redis 类型：Hash，field 为路由别名，value 为逗号分隔的权限 ID；TTL 过期规则：由 table-cache 鉴权目标统一控制。
	RoutePermissionIDs = "route_permission_ids"

	// PermissionUUID 表示权限 UUID 缓存键。
	// Redis 类型：Hash，field 为启用权限 ID，value 为 UUID；TTL 过期规则：由 table-cache 鉴权目标统一控制。
	PermissionUUID = "permission_uuid"

	// PermissionTree 表示权限树缓存键。
	// Redis 类型：String（JSON 文本），TTL 过期规则：无固定 TTL，按权限变更精确失效。
	PermissionTree = "permission_tree"

	// DocPermissionList 表示全部文档权限节点缓存键。
	// Redis 类型：String（JSON 文本），TTL 过期规则：按 table-cache 目标配置过期。
	DocPermissionList = "doc_permission_list"

	// DocResourcePermissionID 表示启用文档资源反向索引缓存键。
	// Redis 类型：Hash，field 为 site 与 path 组成的资源键，value 为文档权限 ID；TTL 过期规则：由 table-cache 鉴权目标统一控制。
	DocResourcePermissionID = "doc_resource_permission_id"

	// SysConfigUUID 表示系统配置缓存键模板。
	// Redis 类型：Hash，TTL 过期规则：无固定 TTL，按业务更新或删除精确失效。
	// `%s` 位置填充系统配置 uuid。
	SysConfigUUID = "config_uuid:%s"

	// SysConfigUUIDPattern 表示系统配置缓存键展示模板。
	// Redis 类型：Hash 模板，TTL 过期规则：不直接写入 Redis，仅用于展示或匹配真实 key。
	SysConfigUUIDPattern = "config_uuid:{uuid}"

	// RuntimeConfigStatePattern 是运行配置 active 版本状态缓存模板。
	// Redis 类型：Hash 模板，TTL 过期规则：不直接写入 Redis，由 table-cache 目标配置控制。
	RuntimeConfigStatePattern = "runtime_config:state"

	// RuntimeConfigReleasePattern 是运行配置发布快照缓存模板。
	// Redis 类型：String 模板，TTL 过期规则：不直接写入 Redis，由 table-cache 目标配置控制。
	RuntimeConfigReleasePattern = "runtime_config:release:{releaseID}"

	// SecretKeyRoute 表示秘钥版本路由缓存键模板。
	// Redis 类型：Hash，TTL 过期规则：无固定 TTL，按业务更新或删除精确失效。
	// `%s` 位置填充 secret_key.uuid。
	SecretKeyRoute = "secret_key_route:%s"

	// SecretKeyRoutePattern 表示秘钥版本路由缓存键展示模板。
	// Redis 类型：Hash 模板，TTL 过期规则：不直接写入 Redis，仅用于展示或匹配真实 key。
	SecretKeyRoutePattern = "secret_key_route:{uuid}"

	// SecretKeyAESVersion 表示版本化 AES 秘钥配置缓存键模板。
	// Redis 类型：Hash，TTL 过期规则：无固定 TTL，按业务更新或删除精确失效。
	// 第一个 `%s` 位置填充 secret_key.uuid，第二个 `%s` 位置填充 key_version。
	SecretKeyAESVersion = "secret_key_aes:%s:%s"

	// SecretKeyAESVersionPattern 表示版本化 AES 秘钥配置缓存键展示模板。
	// Redis 类型：Hash 模板，TTL 过期规则：不直接写入 Redis，仅用于展示或匹配真实 key。
	SecretKeyAESVersionPattern = "secret_key_aes:{uuid}:{keyVersion}"

	// SecretKeyRSAVersion 表示版本化 RSA 秘钥配置缓存键模板。
	// Redis 类型：Hash，TTL 过期规则：无固定 TTL，按业务更新或删除精确失效。
	// 第一个 `%s` 位置填充 secret_key.uuid，第二个 `%s` 位置填充 key_version。
	SecretKeyRSAVersion = "secret_key_rsa:%s:%s"

	// SecretKeyRSAVersionPattern 表示版本化 RSA 秘钥配置缓存键展示模板。
	// Redis 类型：Hash 模板，TTL 过期规则：不直接写入 Redis，仅用于展示或匹配真实 key。
	SecretKeyRSAVersionPattern = "secret_key_rsa:{uuid}:{keyVersion}"

	// SecretKeyVersionIndex 表示指定 AppID 下版本材料缓存 key 的精确索引。
	// Redis 类型：Set，TTL 过期规则：无固定 TTL，成员按业务生命周期精确增删。
	// `%s` 位置填充 secret_key.uuid；成员为 `secret_key_aes:{uuid}:{keyVersion}` 与 `secret_key_rsa:{uuid}:{keyVersion}` 真实 key。
	SecretKeyVersionIndex = "secret_key_version:index:%s"

	// LoginCheckMFAFlag 表示管理员登录 MFA 校验标记业务段模板。
	// Redis 类型：String（Unix 时间戳），TTL 过期规则：由登录 MFA 流程 TTL 控制，过期后需重新校验。
	// `%d` 位置填充管理员 ID，调用侧通过 WithPrefix 追加 app_id 前缀。
	LoginCheckMFAFlag = "login_check_mfa_flag:%d"

	// AdminMFATwoStep 表示管理员二次校验票据 Hash 业务段模板。
	// Redis 类型：Hash，field 为票据 key，value 为带独立过期时间的票据内容；TTL 过期规则：Hash 保留最长票据窗口，value 独立校验真实过期时间。
	// `%d` 位置填充管理员 ID，调用侧通过 WithPrefix 追加 app_id 前缀。
	AdminMFATwoStep = "admin:mfa:two_step:%d"

	// AdminMFATwoStepPattern 表示管理员二次校验票据业务段展示模板。
	// Redis 类型：Hash 模板，TTL 过期规则：不直接写入 Redis，仅用于展示或匹配真实 key。
	AdminMFATwoStepPattern = "admin:mfa:two_step:{adminID}"

	// SysConfigExcelExportLock 表示字典配置导出条件互斥锁 key 模板。
	// Redis 类型：String（由 redsync 管理），TTL 过期规则：由 redsync 锁 TTL 控制，到期自动释放。
	// `%s` 位置填充导出条件指纹，避免同条件并发重复生成 Excel。
	SysConfigExcelExportLock = "sys_config:excel:export:%s"

	// SysConfigMutationLock 表示字典配置写入互斥锁。
	// Redis 类型：String（由 redsync 管理），TTL 过期规则：由 redsync 锁 TTL 控制，到期自动释放。
	// 新增、编辑和导入共用该锁，保证正式导入的快照校验与实际写入之间没有应用内并发变更。
	SysConfigMutationLock = "sys_config:mutation:lock"

	// SysConfigImportBackup 表示字典导入前备份状态缓存键模板。
	// Redis 类型：String（JSON 文本），TTL 过期规则：备份生成后保留 30 天。
	// `%s` 位置填充 backupId，状态记录文件对象、操作者、上传会话和数据快照。
	SysConfigImportBackup = "sys_config:excel:import:backup:%s"

	// SysConfigImportBackupUsed 表示字典导入备份消费标记键模板。
	// Redis 类型：String，TTL 过期规则：与对应备份剩余保留时间一致。
	// `%s` 位置填充 backupId，通过 SETNX 保证同一备份只能执行一次导入。
	SysConfigImportBackupUsed = "sys_config:excel:import:backup:used:%s"

	// AdminExportJob 表示管理员列表导出任务状态缓存键模板。
	// Redis 类型：String（JSON 文本），TTL 过期规则：按管理员导出任务状态 TTL 自动过期。
	// `%s` 位置填充导出任务 jobId。
	AdminExportJob = "admin:export:job:%s"

	// AdminExportRequestIndex 表示管理员导出条件到任务 ID 的复用索引。
	// Redis 类型：String，TTL 过期规则：由调用方 TTL 或业务精确删除控制。
	// `%s` 位置填充导出条件指纹。
	AdminExportRequestIndex = "admin:export:request:%s"

	// AdminExportRequestLock 表示管理员导出条件互斥锁 key 模板。
	// Redis 类型：String（由 redsync 管理），TTL 过期规则：由 redsync 锁 TTL 控制，到期自动释放。
	// `%s` 位置填充导出条件指纹。
	AdminExportRequestLock = "admin:export:request:lock:%s"

	// UserExportJob 表示前台用户列表导出任务状态缓存键模板。
	// Redis 类型：String（JSON 文本），TTL 过期规则：按用户导出任务状态 TTL 自动过期。
	// `%s` 位置填充导出任务 jobId。
	UserExportJob = "user:export:job:%s"

	// UserExportRequestIndex 表示前台用户导出条件到任务 ID 的复用索引。
	// Redis 类型：String，TTL 过期规则：由调用方 TTL 或业务精确删除控制。
	// `%s` 位置填充导出条件指纹。
	UserExportRequestIndex = "user:export:request:%s"

	// UserExportRequestLock 表示前台用户导出条件互斥锁 key 模板。
	// Redis 类型：String（由 redsync 管理），TTL 过期规则：由 redsync 锁 TTL 控制，到期自动释放。
	// `%s` 位置填充导出条件指纹。
	UserExportRequestLock = "user:export:request:lock:%s"

	// FileTransferUploadSession 表示断点续传上传会话缓存键模板。
	// Redis 类型：String（JSON 文本），TTL 过期规则：按上传会话 TTL 自动过期，上传进度刷新时续期。
	// `%s` 位置填充 uploadId。
	FileTransferUploadSession = "file_transfer:upload:session:%s"

	// FileTransferUploadChunks 表示断点续传上传分片完成集合键模板。
	// Redis 类型：Set，TTL 过期规则：按上传会话 TTL 自动过期，上传进度刷新时续期。
	// `%s` 位置填充 uploadId。
	FileTransferUploadChunks = "file_transfer:upload:chunks:%s"

	// FileTransferUploadFingerprint 表示断点续传上传文件指纹到 uploadId 的复用索引。
	// Redis 类型：String，TTL 过期规则：按上传会话 TTL 自动过期。
	// `%s` 位置填充文件指纹。
	FileTransferUploadFingerprint = "file_transfer:upload:fingerprint:%s"

	// FileTransferUploadObjectIndex 表示统一存储对象 key 到 uploadId 的反查索引。
	// Redis 类型：String，TTL 过期规则：按上传会话 TTL 自动过期。
	// `%s` 位置填充对象 key 指纹。
	FileTransferUploadObjectIndex = "file_transfer:upload:object:%s"

	// TaskQueueSchedulerLeaderKey 表示调度器默认 leader 租约 key 模板。
	// Redis 类型：String（由 redsync 管理），TTL 过期规则：由 redsync 锁 TTL 控制，到期自动释放。
	// 实际 Redis key 通过 TaskSchedulerLeaderRedisKey 生成。
	TaskQueueSchedulerLeaderKey = "task:scheduler:leader"

	// SignatureReplayRequest 表示请求签名防重放缓存键模板。
	// Redis 类型：String，TTL 过期规则：按签名防重放 TTL 自动过期。
	// `%s` 位置填充 RequestID，实际 Redis key 通过 WithPrefix 追加 app_id 前缀。
	SignatureReplayRequest = "signature:request:%s"

	// OpsReplayNonce 表示内网运维请求 nonce 防重放缓存键模板。
	// Redis 类型：String，TTL 过期规则：覆盖请求时间戳剩余有效窗口后自动过期。
	// `%s` 位置填充十六进制 nonce，实际 Redis key 通过 WithPrefix 追加 app_id 前缀。
	OpsReplayNonce = "ops:nonce:%s"

	// LoginCaptcha 表示登录图形验证码缓存键模板。
	// Redis 类型：String，TTL 过期规则：按登录验证码 TTL 自动过期，校验成功或失败后立即删除。
	// `%s` 位置填充验证码 key。
	LoginCaptcha = "login:captcha:%s"

	// UserTagWorkflowLeaseKey 表示用户标签写工作流全局互斥租约 key。
	// Redis 类型：String，TTL 过期规则：由调用方 TTL 或业务精确删除控制。
	// 实际 Redis key 通过 UserTagWorkflowLeaseRedisKey 生成，值为 `workflowID|mode`，释放时必须按完整 owner 精确比较。
	UserTagWorkflowLeaseKey = "user_tag:workflow:write_lock"

	// UserTagWorkflowFinalDoneKey 表示用户标签最终分片完成屏障 key 模板。
	// Redis 类型：Set，TTL 过期规则：无固定 TTL，成员按业务生命周期精确增删。
	// `%s` 位置填充 workflow_id，实际 Redis key 通过 UserTagWorkflowFinalDoneRedisKey 生成。
	UserTagWorkflowFinalDoneKey = "user_tag:workflow:final_done:%s"

	// UserTagRuntimeCleanupLock 表示用户标签运行期辅助表清理互斥锁。
	// Redis 类型：String（由 redsync 管理），TTL 过期规则：由 redsync 锁 TTL 控制，到期自动释放。
	// 实际 Redis key 通过 UserTagRuntimeCleanupRedisKey 生成，避免周期调度和人工补跑同时清理。
	UserTagRuntimeCleanupLock = "user_tag:runtime:cleanup:lock"

	// UserTagEventOutboxRetryScanLock 表示用户标签事件 outbox 异常扫描互斥锁。
	// Redis 类型：String（由 redsync 管理），TTL 过期规则：由 redsync 锁 TTL 控制，到期自动释放。
	// 实际 Redis key 通过 UserTagEventOutboxRetryScanRedisKey 生成，限制异常 outbox 单任务推进。
	UserTagEventOutboxRetryScanLock = "user_tag:event_outbox:retry_scan:lock"

	// ArchiveJobPlanLock 表示归档任务区间规划互斥锁 key 模板。
	// Redis 类型：String（由 redsync 管理），TTL 过期规则：由 redsync 锁 TTL 控制，到期自动释放。
	// `%s` 位置填充归档 job 名，实际 Redis key 通过 ArchiveJobPlanRedisKey 生成。
	ArchiveJobPlanLock = "archive:job:%s:plan"

	// ArchiveJobWatermarkLock 表示归档任务水位推进互斥锁 key 模板。
	// Redis 类型：String（由 redsync 管理），TTL 过期规则：由 redsync 锁 TTL 控制，到期自动释放。
	// `%s` 位置填充归档 job 名，实际 Redis key 通过 ArchiveJobWatermarkRedisKey 生成。
	ArchiveJobWatermarkLock = "archive:job:%s:watermark"

	// ArchiveJobCleanupLock 表示归档历史表清理互斥锁 key 模板。
	// Redis 类型：String（由 redsync 管理），TTL 过期规则：由 redsync 锁 TTL 控制，到期自动释放。
	// `%s` 位置填充归档 job 名，实际 Redis key 通过 ArchiveJobCleanupRedisKey 生成。
	ArchiveJobCleanupLock = "archive:job:%s:cleanup"
)
