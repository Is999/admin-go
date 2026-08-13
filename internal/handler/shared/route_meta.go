package shared

import (
	"admin/internal/middleware"
	"admin/internal/model"
	"admin/internal/routealias"
)

// RouteMeta 描述一条业务路由的统一元数据。
// 统一收口后，路由注册、审计日志、链路追踪都复用这一份定义，避免同一条路由维护多套信息。
type RouteMeta struct {
	Alias    middleware.RouteAlias // 统一路由别名，供鉴权/日志/trace 对齐
	Action   model.AdminLogAction  // 管理员审计动作；为空表示该路由不写管理员操作审计
	Describe string                // 中文业务说明，供审计日志直接复用
}

// defaultRouteMetas 按声明顺序登记内置路由元数据，供契约、测试和文档防漂移复用。
var defaultRouteMetas []RouteMeta

// newRouteMeta 创建并登记普通路由元数据，避免变量清单和默认清单双份维护。
func newRouteMeta(alias middleware.RouteAlias, describe string) RouteMeta {
	return registerRouteMeta(RouteMeta{
		Alias:    alias,
		Describe: describe,
	})
}

// newAuditRouteMeta 创建并登记带管理员审计动作的路由元数据。
func newAuditRouteMeta(alias middleware.RouteAlias, action model.AdminLogAction, describe string) RouteMeta {
	return registerRouteMeta(RouteMeta{
		Alias:    alias,
		Action:   action,
		Describe: describe,
	})
}

// registerRouteMeta 追加内置元数据并返回原值，保持声明点仍可直接赋给模块变量。
func registerRouteMeta(meta RouteMeta) RouteMeta {
	defaultRouteMetas = append(defaultRouteMetas, meta)
	return meta
}

// DefaultRouteMetas 返回内置路由元数据快照，调用方不能修改全局声明顺序。
func DefaultRouteMetas() []RouteMeta {
	out := make([]RouteMeta, len(defaultRouteMetas))
	copy(out, defaultRouteMetas)
	return out
}

// 路由元数据变量集中维护路由别名、审计动作和中文说明，是路由注册与审计的统一契约。
var (
	// 认证模块
	// AuthCaptcha 获取登录图形验证码。
	AuthCaptcha = newRouteMeta("auth.captcha", "获取登录图形验证码")
	// AuthLogin 管理员登录。
	AuthLogin = newRouteMeta(routealias.AuthLogin, "管理员登录")
	// AuthRefresh 刷新访问令牌。
	AuthRefresh = newRouteMeta(routealias.AuthRefresh, "刷新访问令牌")
	// AuthLogout 管理员退出登录。
	AuthLogout = newRouteMeta(routealias.AuthLogout, "管理员退出登录")
	// AuthCodes 获取当前用户权限码。
	AuthCodes = newRouteMeta(routealias.AuthCodes, "获取当前用户权限码")
	// AuthProfile 获取当前登录资料。
	AuthProfile = newRouteMeta(routealias.AuthProfile, "获取当前登录资料")
	// DocsSession 创建文档访问会话，只校验登录安全状态，文档资源另行鉴权。
	DocsSession = newRouteMeta(routealias.Ignore, "创建文档访问会话")
	// InternalInitAdminBootstrap 内网初始化管理员账号。
	InternalInitAdminBootstrap = newRouteMeta("internal.auth.init_admin_bootstrap", "内网初始化管理员账号")
	// ProfileMine 获取当前管理员资料。
	ProfileMine = newRouteMeta(routealias.ProfileMine, "获取当前管理员资料")
	// ProfileCheckSecure 校验当前管理员密码。
	ProfileCheckSecure = newRouteMeta(routealias.ProfileCheckSecure, "校验当前管理员密码")
	// ProfileCheckMFA 校验当前管理员MFA动态码。
	ProfileCheckMFA = newRouteMeta(routealias.ProfileCheckMFA, "校验当前管理员MFA动态码")
	// ProfileUpdatePassword 个人中心修改密码。
	ProfileUpdatePassword = newAuditRouteMeta(routealias.ProfileUpdatePassword, model.ActionAdminPasswordReset, "个人中心修改密码")
	// ProfileUpdateMine 个人中心修改资料。
	ProfileUpdateMine = newAuditRouteMeta(routealias.ProfileUpdateMine, model.ActionAdminUpdate, "个人中心修改资料")
	// ProfileUpdateMFA 个人中心修改MFA状态。
	ProfileUpdateMFA = newAuditRouteMeta(routealias.ProfileUpdateMFAStatus, model.ActionAdminUpdate, "个人中心修改MFA状态")
	// ProfileUpdateMFAKey 个人中心修改MFA秘钥。
	ProfileUpdateMFAKey = newAuditRouteMeta(routealias.ProfileUpdateMFASecret, model.ActionAdminUpdate, "个人中心修改MFA秘钥")
	// ProfileRefreshMFAKey 个人中心重新生成MFA秘钥。
	ProfileRefreshMFAKey = newAuditRouteMeta(routealias.ProfileRefreshMFASecret, model.ActionAdminUpdate, "个人中心重新生成MFA秘钥")
	// ProfileUpdateAvatar 个人中心修改头像。
	ProfileUpdateAvatar = newAuditRouteMeta(routealias.ProfileUpdateAvatar, model.ActionAdminUpdate, "个人中心修改头像")
	// AdminBuildMFAURL 生成管理员MFA绑定地址。
	AdminBuildMFAURL = newAuditRouteMeta(routealias.AdminBuildMFAURL, model.ActionAdminUpdate, "生成管理员MFA绑定地址")
	// AdminMFAStatus 修改管理员MFA状态。
	AdminMFAStatus = newAuditRouteMeta(routealias.AdminMFAStatus, model.ActionAdminUpdate, "修改管理员MFA状态")

	// 管理员模块
	// AdminLogQuery 查询管理员审计日志。
	AdminLogQuery = newAuditRouteMeta("admin.log.query", model.ActionAdminLogQuery, "查询管理员审计日志")
	// AdminAdd 新增管理员。
	AdminAdd = newAuditRouteMeta(routealias.AdminAdd, model.ActionAdminAdd, "新增管理员")
	// AdminList 查询管理员列表。
	AdminList = newAuditRouteMeta("admin.list", model.ActionAdminList, "查询管理员列表")
	// AdminInfo 查询管理员详情。
	AdminInfo = newAuditRouteMeta("admin.info", model.ActionAdminInfo, "查询管理员详情")
	// AdminUpdate 编辑管理员。
	AdminUpdate = newAuditRouteMeta(routealias.AdminUpdate, model.ActionAdminUpdate, "编辑管理员")
	// AdminDelete 删除管理员。
	AdminDelete = newAuditRouteMeta(routealias.AdminDelete, model.ActionAdminDelete, "删除管理员")
	// AdminStatusUpdate 修改管理员状态。
	AdminStatusUpdate = newAuditRouteMeta(routealias.AdminStatusUpdate, model.ActionAdminStatusUpdate, "修改管理员状态")
	// AdminPasswordReset 重置管理员密码。
	AdminPasswordReset = newAuditRouteMeta(routealias.AdminPasswordReset, model.ActionAdminPasswordReset, "重置管理员密码")
	// AdminResetInitialState 重置管理员到首次登录前状态。
	AdminResetInitialState = newAuditRouteMeta(routealias.AdminResetInitialState, model.ActionAdminPasswordReset, "重置管理员到首次登录前状态")
	// AdminRoleList 查询管理员角色。
	AdminRoleList = newAuditRouteMeta("admin.role.list", model.ActionAdminRoleList, "查询管理员角色")
	// AdminRoleUpdate 编辑管理员角色。
	AdminRoleUpdate = newAuditRouteMeta(routealias.AdminRoleUpdate, model.ActionAdminRoleUpdate, "编辑管理员角色")
	// AdminExportTrigger 异步导出管理员列表。
	AdminExportTrigger = newAuditRouteMeta("admin.export", model.ActionAdminExport, "异步导出管理员列表")
	// AdminExportStatus 查询管理员导出进度。
	AdminExportStatus = newAuditRouteMeta("admin.export.status", model.ActionAdminExportStatus, "查询管理员导出进度")
	// AdminExportDownload 下载管理员导出文件。
	AdminExportDownload = newAuditRouteMeta("admin.export.download", model.ActionAdminExportDownload, "下载管理员导出文件")

	// 前台用户模块
	// UserList 查询前台用户列表。
	UserList = newAuditRouteMeta(routealias.UserList, model.ActionUserList, "查询前台用户列表")
	// UserInfo 查询前台用户详情。
	UserInfo = newAuditRouteMeta(routealias.UserInfo, model.ActionUserInfo, "查询前台用户详情")
	// UserAdd 新增前台用户。
	UserAdd = newAuditRouteMeta(routealias.UserAdd, model.ActionUserAdd, "新增前台用户")
	// UserUpdate 编辑前台用户资料。
	UserUpdate = newAuditRouteMeta(routealias.UserUpdate, model.ActionUserUpdate, "编辑前台用户资料")
	// UserStatusUpdate 修改前台用户状态。
	UserStatusUpdate = newAuditRouteMeta(routealias.UserStatusUpdate, model.ActionUserStatusUpdate, "修改前台用户状态")
	// UserPasswordReset 重置前台用户密码。
	UserPasswordReset = newAuditRouteMeta(routealias.UserPasswordReset, model.ActionUserPasswordReset, "重置前台用户密码")
	// UserRuntimeSync 同步前台用户 API 运行态。
	UserRuntimeSync = newAuditRouteMeta(routealias.UserRuntimeSync, model.ActionUserRuntimeSync, "同步前台用户API运行态")
	// UserExportTrigger 异步导出前台用户列表。
	UserExportTrigger = newAuditRouteMeta(routealias.UserExport, model.ActionUserExport, "异步导出前台用户列表")
	// UserExportStatus 查询前台用户导出进度。
	UserExportStatus = newAuditRouteMeta(routealias.UserExportStatus, model.ActionUserExportStatus, "查询前台用户导出进度")
	// UserExportDownload 下载前台用户导出文件。
	UserExportDownload = newAuditRouteMeta(routealias.UserExportDownload, model.ActionUserExportDownload, "下载前台用户导出文件")
	// APIRuntimeConfigReloadStatus 查询 API 配置热加载状态。
	APIRuntimeConfigReloadStatus = newAuditRouteMeta(routealias.APIRuntimeConfigReloadStatus, model.ActionAPIRuntimeConfigReloadStatus, "查询API配置热加载状态")
	// APIRuntimeConfigReloadItems 查询 API 运行态配置项。
	APIRuntimeConfigReloadItems = newAuditRouteMeta(routealias.APIRuntimeConfigReloadItems, model.ActionAPIRuntimeConfigReloadItems, "查询API运行态配置项")
	// APIRuntimeConfigReloadRun 手动触发 API 配置热加载。
	APIRuntimeConfigReloadRun = newAuditRouteMeta(routealias.APIRuntimeConfigReloadRun, model.ActionAPIRuntimeConfigReloadRun, "手动触发API配置热加载")

	// 消息中心模块
	// AdminMessageList 查询管理员消息收件箱。
	AdminMessageList = newAuditRouteMeta(routealias.AdminMessageList, model.ActionAdminMessageList, "查询管理员消息收件箱")
	// AdminMessageSentList 查询管理员已发送消息。
	AdminMessageSentList = newAuditRouteMeta(routealias.AdminMessageSentList, model.ActionAdminMessageSentList, "查询管理员已发送消息")
	// AdminMessageReceiverOptions 查询管理员消息可用收件人选项。
	AdminMessageReceiverOptions = newRouteMeta(routealias.AdminMessageReceiverOptions, "查询管理员消息可用收件人选项")
	// AdminMessageReceivers 查询管理员消息收件人明细。
	AdminMessageReceivers = newAuditRouteMeta(routealias.AdminMessageReceivers, model.ActionAdminMessageReceivers, "查询管理员消息收件人明细")
	// AdminMessageSend 发送管理员消息。
	AdminMessageSend = newAuditRouteMeta(routealias.AdminMessageSend, model.ActionAdminMessageSend, "发送管理员消息")
	// AdminMessageMarkRead 标记管理员消息已读。
	AdminMessageMarkRead = newAuditRouteMeta(routealias.AdminMessageMarkRead, model.ActionAdminMessageMarkRead, "标记管理员消息已读")
	// AdminMessageDelete 删除管理员消息。
	AdminMessageDelete = newAuditRouteMeta(routealias.AdminMessageDelete, model.ActionAdminMessageDelete, "删除管理员消息")
	// AdminMessageHandle 标记管理员消息已处理。
	AdminMessageHandle = newAuditRouteMeta(routealias.AdminMessageHandle, model.ActionAdminMessageHandle, "标记管理员消息已处理")
	// AdminMessageUnreadCount 查询管理员未读消息数量。
	AdminMessageUnreadCount = newRouteMeta(routealias.AdminMessageUnreadCount, "查询管理员未读消息数量")
	// AdminMessageNotifications 查询管理员通知列表。
	AdminMessageNotifications = newRouteMeta(routealias.AdminMessageNotifications, "查询管理员通知列表")
	// RoleList 查询角色列表。
	RoleList = newAuditRouteMeta("role.list", model.ActionRoleList, "查询角色列表")
	// RoleTreeList 查询角色树。
	RoleTreeList = newRouteMeta("role.tree.list", "查询角色树")
	// RoleTreeOptions 查询角色树下拉。
	RoleTreeOptions = newRouteMeta(routealias.RoleTreeOptions, "查询角色树下拉")
	// RoleAdd 新增角色。
	RoleAdd = newAuditRouteMeta(routealias.RoleAdd, model.ActionRoleAdd, "新增角色")
	// RoleUpdate 编辑角色。
	RoleUpdate = newAuditRouteMeta(routealias.RoleUpdate, model.ActionRoleUpdate, "编辑角色")
	// RoleDelete 删除角色。
	RoleDelete = newAuditRouteMeta("role.delete", model.ActionRoleDelete, "删除角色")
	// RoleStatusUpdate 修改角色状态。
	RoleStatusUpdate = newAuditRouteMeta(routealias.RoleStatusUpdate, model.ActionRoleStatusUpdate, "修改角色状态")
	// RolePermissionTree 查询角色权限树。
	RolePermissionTree = newRouteMeta("role.permission.tree", "查询角色权限树")
	// RolePermissionUpdate 编辑角色权限。
	RolePermissionUpdate = newAuditRouteMeta(routealias.RolePermissionUpdate, model.ActionRolePermissionUpdate, "编辑角色权限")
	// PermissionList 查询权限列表。
	PermissionList = newAuditRouteMeta("permission.list", model.ActionPermissionList, "查询权限列表")
	// PermissionTreeList 查询权限树。
	PermissionTreeList = newRouteMeta("permission.tree.list", "查询权限树")
	// PermissionMaxUUID 查询下一个权限UUID。
	PermissionMaxUUID = newRouteMeta(routealias.PermissionMaxUUID, "查询下一个权限UUID")
	// PermissionAdd 新增权限。
	PermissionAdd = newAuditRouteMeta(routealias.PermissionAdd, model.ActionPermissionAdd, "新增权限")
	// PermissionUpdate 编辑权限。
	PermissionUpdate = newAuditRouteMeta(routealias.PermissionUpdate, model.ActionPermissionUpdate, "编辑权限")
	// PermissionDelete 删除权限。
	PermissionDelete = newAuditRouteMeta("permission.delete", model.ActionPermissionDelete, "删除权限")
	// PermissionStatus 修改权限状态。
	PermissionStatus = newAuditRouteMeta(routealias.PermissionStatusUpdate, model.ActionPermissionStatus, "修改权限状态")
	// DocPermissionList 查询文档权限列表。
	DocPermissionList = newAuditRouteMeta(routealias.DocPermissionList, model.ActionDocPermissionList, "查询文档权限列表")
	// DocPermissionStatus 修改文档权限状态。
	DocPermissionStatus = newAuditRouteMeta(routealias.DocPermissionStatusUpdate, model.ActionDocPermissionStatus, "修改文档权限状态")
	// SysConfigList 查询系统配置。
	SysConfigList = newAuditRouteMeta("system.config.list", model.ActionSysConfigList, "查询系统配置")
	// SysConfigAdd 新增系统配置。
	SysConfigAdd = newAuditRouteMeta("system.config.add", model.ActionSysConfigAdd, "新增系统配置")
	// SysConfigUpdate 编辑系统配置。
	SysConfigUpdate = newAuditRouteMeta("system.config.update", model.ActionSysConfigUpdate, "编辑系统配置")
	// SysConfigExport 导出系统配置。
	SysConfigExport = newAuditRouteMeta("system.config.export", model.ActionSysConfigExport, "导出系统配置")
	// SysConfigImport 导入系统配置。
	SysConfigImport = newAuditRouteMeta(routealias.SysConfigImport, model.ActionSysConfigImport, "导入系统配置")
	// SysConfigCache 查看系统配置缓存。
	SysConfigCache = newAuditRouteMeta("system.config.cache", model.ActionSysConfigCache, "查看系统配置缓存")
	// SysConfigRenew 刷新系统配置缓存。
	SysConfigRenew = newAuditRouteMeta("system.config.renew", model.ActionSysConfigRenew, "刷新系统配置缓存")
	// CacheList 查询缓存列表。
	CacheList = newAuditRouteMeta("cache.list", model.ActionCacheList, "查询缓存列表")
	// CacheServerInfo 查看缓存服务器信息。
	CacheServerInfo = newAuditRouteMeta("cache.server.info", model.ActionCacheInfo, "查看缓存服务器信息")
	// CacheKeyInfo 查看缓存键信息。
	CacheKeyInfo = newAuditRouteMeta("cache.key.info", model.ActionCacheInfo, "查看缓存键信息")
	// CacheSearch 搜索缓存键。
	CacheSearch = newAuditRouteMeta("cache.search", model.ActionCacheSearch, "搜索缓存键")
	// CacheRenew 刷新缓存。
	CacheRenew = newAuditRouteMeta("cache.renew", model.ActionCacheRenew, "刷新缓存")
	// CacheRenewAll 刷新全部缓存。
	CacheRenewAll = newAuditRouteMeta("cache.renew.all", model.ActionCacheRenewAll, "刷新全部缓存")
	// CacheWarmup 按模板预热缓存。
	CacheWarmup = newAuditRouteMeta("cache.warmup", model.ActionCacheWarmup, "按模板预热缓存")
	// SecretKeyList 查询秘钥列表。
	SecretKeyList = newAuditRouteMeta("secretKey.index", model.ActionSecretKeyList, "查询秘钥列表")
	// SecretKeyGet 查询秘钥详情。
	SecretKeyGet = newAuditRouteMeta(routealias.SecretKeyGet, model.ActionSecretKeyGet, "查询秘钥详情")
	// SecretKeyAdd 新增秘钥。
	SecretKeyAdd = newAuditRouteMeta(routealias.SecretKeyAdd, model.ActionSecretKeyAdd, "新增秘钥")
	// SecretKeyUpdate 编辑秘钥。
	SecretKeyUpdate = newAuditRouteMeta(routealias.SecretKeyUpdate, model.ActionSecretKeyUpdate, "编辑秘钥")
	// SecretKeyStatus 修改秘钥状态。
	SecretKeyStatus = newAuditRouteMeta(routealias.SecretKeyStatus, model.ActionSecretKeyStatus, "修改秘钥状态")
	// SecretKeyRenew 刷新秘钥缓存。
	SecretKeyRenew = newAuditRouteMeta(routealias.SecretKeyRenew, model.ActionSecretKeyRenew, "刷新秘钥缓存")
	// SecretKeyValidate 预检秘钥配置。
	SecretKeyValidate = newAuditRouteMeta(routealias.SecretKeyValidate, model.ActionSecretKeyValidate, "预检秘钥配置")
	// SecretKeySelfCheck 执行秘钥自检。
	SecretKeySelfCheck = newAuditRouteMeta(routealias.SecretKeySelfCheck, model.ActionSecretKeySelfCheck, "执行秘钥自检")
	// SecurityDebugSign 安全调试签名。
	SecurityDebugSign = newAuditRouteMeta(routealias.SecurityDebugSign, model.ActionSecurityDebugSign, "安全调试签名")
	// SecurityDebugVerify 安全调试验签。
	SecurityDebugVerify = newAuditRouteMeta(routealias.SecurityDebugVerify, model.ActionSecurityDebugVerify, "安全调试验签")
	// SecurityDebugEncrypt 安全调试加密。
	SecurityDebugEncrypt = newAuditRouteMeta(routealias.SecurityDebugEncrypt, model.ActionSecurityDebugEncrypt, "安全调试加密")
	// SecurityDebugDecrypt 安全调试解密。
	SecurityDebugDecrypt = newAuditRouteMeta(routealias.SecurityDebugDecrypt, model.ActionSecurityDebugDecrypt, "安全调试解密")

	// 任务系统模块
	// TaskEnqueue 手动投递任务。
	TaskEnqueue = newAuditRouteMeta("task.enqueue", model.ActionTaskEnqueue, "手动投递任务")
	// TaskInfoGet 查询任务详情。
	TaskInfoGet = newAuditRouteMeta("task.info.get", model.ActionTaskInfoGet, "查询任务详情")
	// TaskItemsList 查询任务列表。
	TaskItemsList = newAuditRouteMeta("task.items.list", model.ActionTaskItemsList, "查询任务列表")
	// TaskRun 立即执行任务。
	TaskRun = newAuditRouteMeta("task.run", model.ActionTaskRun, "立即执行任务")
	// TaskDelete 删除任务。
	TaskDelete = newAuditRouteMeta("task.delete", model.ActionTaskDelete, "删除任务")
	// TaskWorkflowTrigger 手动触发工作流。
	TaskWorkflowTrigger = newAuditRouteMeta("task.workflow.trigger", model.ActionTaskWorkflowTrigger, "手动触发工作流")
	// TaskWorkflowStatus 查询工作流状态。
	TaskWorkflowStatus = newAuditRouteMeta("task.workflow.status", model.ActionTaskWorkflowStatus, "查询工作流状态")
	// TaskQueueList 查询任务队列概览。
	TaskQueueList = newAuditRouteMeta("task.queue.list", model.ActionTaskQueueList, "查询任务队列概览")
	// TaskConfigItems 查询配置热加载配置项。
	TaskConfigItems = newAuditRouteMeta("task.config.reload.items", model.ActionTaskConfigReloadItems, "查询配置热加载配置项")
	// TaskConfigReload 查询配置热加载状态。
	TaskConfigReload = newAuditRouteMeta("task.config.reload.status", model.ActionTaskConfigReloadStatus, "查询配置热加载状态")
	// TaskConfigReloadRun 手动触发配置热加载。
	TaskConfigReloadRun = newAuditRouteMeta("task.config.reload.run", model.ActionTaskConfigReloadRun, "手动触发配置热加载")
	// RuntimeConfigOverview 查询运行配置概览。
	RuntimeConfigOverview = newAuditRouteMeta("runtime.config.overview", model.ActionRuntimeConfigOverview, "查询运行配置概览")
	// RuntimeConfigList 查询运行配置列表。
	RuntimeConfigList = newAuditRouteMeta("runtime.config.list", model.ActionRuntimeConfigList, "查询运行配置")
	// RuntimeConfigSave 保存运行配置草稿。
	RuntimeConfigSave = newAuditRouteMeta("runtime.config.save", model.ActionRuntimeConfigSave, "保存运行配置草稿")
	// RuntimeConfigValidate 预检运行配置草稿。
	RuntimeConfigValidate = newAuditRouteMeta("runtime.config.validate", model.ActionRuntimeConfigValidate, "预检运行配置")
	// RuntimeConfigPublish 发布运行配置。
	RuntimeConfigPublish = newAuditRouteMeta("runtime.config.publish", model.ActionRuntimeConfigPublish, "发布运行配置")
	// RuntimeConfigRollback 回滚运行配置。
	RuntimeConfigRollback = newAuditRouteMeta("runtime.config.rollback", model.ActionRuntimeConfigRollback, "回滚运行配置")
	// TaskQueuePause 暂停任务队列。
	TaskQueuePause = newAuditRouteMeta("task.queue.pause", model.ActionTaskQueuePause, "暂停任务队列")
	// TaskQueueResume 恢复任务队列。
	TaskQueueResume = newAuditRouteMeta("task.queue.resume", model.ActionTaskQueueResume, "恢复任务队列")

	// 通用收集器模块
	// CollectorOverview 查询Collector观测概览。
	CollectorOverview = newAuditRouteMeta("collector.overview", model.ActionCollectorOverview, "查询Collector观测概览")
	// CollectorFailureList 查询Collector失败事件。
	CollectorFailureList = newAuditRouteMeta("collector.failure.list", model.ActionCollectorFailureList, "查询Collector失败事件")
	// CollectorFailureRun 手动执行Collector失败重试。
	CollectorFailureRun = newAuditRouteMeta("collector.failure.run", model.ActionCollectorFailureRun, "手动执行Collector失败重试")
	// CollectorFailureRetry 手动重试Collector失败事件。
	CollectorFailureRetry = newAuditRouteMeta("collector.failure.retry", model.ActionCollectorFailureRetry, "手动重试Collector失败事件")

	// 用户标签模块
	// UserTagWorkflowTrigger 触发用户标签工作流。
	UserTagWorkflowTrigger = newAuditRouteMeta("user_tag.workflow.trigger", model.ActionUserTagWorkflowTrigger, "触发用户标签工作流")
	// UserTagRecalculate 指定标签重新计算。
	UserTagRecalculate = newAuditRouteMeta("user_tag.recalculate", model.ActionUserTagRecalculate, "指定标签重新计算")
	// UserTagWorkflowLeaseRelease 释放用户标签工作流互斥锁。
	UserTagWorkflowLeaseRelease = newAuditRouteMeta(routealias.UserTagWorkflowLeaseRelease, model.ActionUserTagWorkflowLeaseRelease, "释放用户标签工作流互斥锁")
)
