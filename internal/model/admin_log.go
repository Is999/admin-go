package model

import (
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// TableNameAdminLog 管理员审计日志表名常量，统一供模型与查询复用。
const TableNameAdminLog = "admin_log"

// AdminLog 管理员操作日志。
// 这一版除了传统审计字段，还补充了 trace/span、HTTP 结果和耗时，便于把审计记录和运行日志串起来。
type AdminLog struct {
	ID           int       `gorm:"column:id;type:int unsigned;primaryKey;autoIncrement:true" json:"id"`                                                               // 日志主键 ID
	EventID      string    `gorm:"column:event_id;type:varchar(64);not null;default:'';uniqueIndex:uk_event_id;comment:Collector事件ID" json:"event_id"`                // Collector 持久幂等事件 ID
	UserID       int       `gorm:"column:user_id;type:int unsigned;not null;comment:用户 ID" json:"user_id"`                                                            // 操作管理员 ID
	UserName     string    `gorm:"column:user_name;type:varchar(20);not null;index:idx_user_name,priority:1;comment:用户账户" json:"user_name"`                           // 操作管理员账号
	Action       string    `gorm:"column:action;type:varchar(100);not null;index:idx_action,priority:1;default:0;comment:动作名称" json:"action"`                         // 审计动作名称
	Route        string    `gorm:"column:route;type:varchar(255);not null;comment:路由名称" json:"route"`                                                                 // 请求路由
	Method       string    `gorm:"column:method;type:varchar(255);not null;comment:模块/类/方法" json:"method"`                                                            // 处理方法标识
	Describe     string    `gorm:"column:describe;type:varchar(255);not null;comment:描述" json:"describe"`                                                             // 操作描述
	Data         string    `gorm:"column:data;type:text;comment:操作数据" json:"data"`                                                                                    // 审计数据快照
	IP           string    `gorm:"column:ip;type:varchar(64);not null;comment:IP 地址" json:"ip"`                                                                       // 客户端 IP 地址
	Ipaddr       string    `gorm:"column:ipaddr;type:varchar(100);not null;comment:IP 地区信息" json:"ipaddr"`                                                            // IP 地区信息
	TraceID      string    `gorm:"column:trace_id;type:varchar(64);not null;default:'';index:idx_trace_id,priority:1;comment:Trace ID" json:"trace_id"`               // 链路追踪 ID
	SpanID       string    `gorm:"column:span_id;type:varchar(32);not null;default:'';comment:Span ID" json:"span_id"`                                                // 链路跨度 ID
	HTTPStatus   int       `gorm:"column:http_status;type:int;not null;default:200;comment:HTTP 状态码" json:"http_status"`                                              // HTTP 状态码
	BizCode      int       `gorm:"column:biz_code;type:int;not null;default:0;comment:业务码" json:"biz_code"`                                                           // 业务响应码
	LatencyMS    int64     `gorm:"column:latency_ms;type:bigint;not null;default:0;comment:请求耗时毫秒" json:"latency_ms"`                                                 // 请求耗时（毫秒）
	Success      bool      `gorm:"column:success;type:tinyint(1);not null;default:1;comment:是否成功" json:"success"`                                                     // 是否成功
	ErrorMessage string    `gorm:"column:error_message;type:varchar(500);not null;default:'';comment:错误信息" json:"error_message"`                                      // 错误摘要
	CreatedAt    time.Time `gorm:"column:created_at;type:datetime;not null;index:idx_created_at,priority:1;default:CURRENT_TIMESTAMP;comment:创建时间" json:"created_at"` // 创建时间
}

// TableName 返回管理员审计日志表名。
func (*AdminLog) TableName() string {
	return TableNameAdminLog
}

// AdminLogAction 定义管理员操作动作枚举，既用于审计落库，也用于后台筛选。
type AdminLogAction string

// 定义管理员操作日志的动作类型。
const (
	// 管理员登录相关。
	// ActionAdminLogin 管理员登录。
	ActionAdminLogin AdminLogAction = "管理员登录"
	// ActionAdminLogout 管理员登出。
	ActionAdminLogout AdminLogAction = "管理员登出"

	// 管理员管理相关。
	// ActionAdminAdd 新增管理员。
	ActionAdminAdd AdminLogAction = "新增管理员"
	// ActionAdminList 查询管理员列表。
	ActionAdminList AdminLogAction = "查询管理员列表"
	// ActionAdminInfo 查询管理员详情。
	ActionAdminInfo AdminLogAction = "查询管理员详情"
	// ActionAdminUpdate 编辑管理员。
	ActionAdminUpdate AdminLogAction = "编辑管理员"
	// ActionAdminDelete 删除管理员。
	ActionAdminDelete AdminLogAction = "删除管理员"
	// ActionAdminStatusUpdate 修改管理员状态。
	ActionAdminStatusUpdate AdminLogAction = "修改管理员状态"
	// ActionAdminPasswordReset 重置管理员密码。
	ActionAdminPasswordReset AdminLogAction = "重置管理员密码"
	// ActionAdminRoleList 查询管理员角色。
	ActionAdminRoleList AdminLogAction = "查询管理员角色"
	// ActionAdminRoleUpdate 编辑管理员角色。
	ActionAdminRoleUpdate AdminLogAction = "编辑管理员角色"
	// ActionAdminExport 导出管理员列表。
	ActionAdminExport AdminLogAction = "导出管理员列表"
	// ActionAdminExportStatus 查询管理员导出进度。
	ActionAdminExportStatus AdminLogAction = "查询管理员导出进度"
	// ActionAdminExportDownload 下载管理员导出文件。
	ActionAdminExportDownload AdminLogAction = "下载管理员导出文件"

	// 前台用户管理相关。
	// ActionUserList 查询前台用户列表。
	ActionUserList AdminLogAction = "查询前台用户列表"
	// ActionUserInfo 查询前台用户详情。
	ActionUserInfo AdminLogAction = "查询前台用户详情"
	// ActionUserAdd 新增前台用户。
	ActionUserAdd AdminLogAction = "新增前台用户"
	// ActionUserUpdate 编辑前台用户。
	ActionUserUpdate AdminLogAction = "编辑前台用户"
	// ActionUserStatusUpdate 修改前台用户状态。
	ActionUserStatusUpdate AdminLogAction = "修改前台用户状态"
	// ActionUserPasswordReset 重置前台用户密码。
	ActionUserPasswordReset AdminLogAction = "重置前台用户密码"
	// ActionUserRuntimeSync 同步前台用户运行态。
	ActionUserRuntimeSync AdminLogAction = "同步前台用户运行态"
	// ActionUserExport 导出前台用户列表。
	ActionUserExport AdminLogAction = "导出前台用户列表"
	// ActionUserExportStatus 查询前台用户导出进度。
	ActionUserExportStatus AdminLogAction = "查询前台用户导出进度"
	// ActionUserExportDownload 下载前台用户导出文件。
	ActionUserExportDownload AdminLogAction = "下载前台用户导出文件"
	// ActionAPIRuntimeConfigReloadStatus 查询 API 配置热加载状态。
	ActionAPIRuntimeConfigReloadStatus AdminLogAction = "查询API配置热加载状态"
	// ActionAPIRuntimeConfigReloadItems 查询 API 运行态配置项。
	ActionAPIRuntimeConfigReloadItems AdminLogAction = "查询API运行态配置项"
	// ActionAPIRuntimeConfigReloadRun 触发 API 配置热加载。
	ActionAPIRuntimeConfigReloadRun AdminLogAction = "触发API配置热加载"

	// 消息中心相关。
	// ActionAdminMessageList 查询消息列表。
	ActionAdminMessageList AdminLogAction = "查询消息列表"
	// ActionAdminMessageSentList 查询已发送消息。
	ActionAdminMessageSentList AdminLogAction = "查询已发送消息"
	// ActionAdminMessageReceivers 查询消息收件人明细。
	ActionAdminMessageReceivers AdminLogAction = "查询消息收件人明细"
	// ActionAdminMessageSend 发送消息。
	ActionAdminMessageSend AdminLogAction = "发送消息"
	// ActionAdminMessageMarkRead 标记消息已读。
	ActionAdminMessageMarkRead AdminLogAction = "标记消息已读"
	// ActionAdminMessageDelete 删除消息。
	ActionAdminMessageDelete AdminLogAction = "删除消息"
	// ActionAdminMessageHandle 标记消息已处理。
	ActionAdminMessageHandle AdminLogAction = "标记消息已处理"
	// ActionAdminMessageUnreadCount 查询未读消息数量。
	ActionAdminMessageUnreadCount AdminLogAction = "查询未读消息数量"
	// ActionAdminMessageNotifyList 查询通知列表。
	ActionAdminMessageNotifyList AdminLogAction = "查询通知列表"

	// 角色与权限管理相关。
	// ActionRoleList 查询角色列表。
	ActionRoleList AdminLogAction = "查询角色列表"
	// ActionRoleAdd 新增角色。
	ActionRoleAdd AdminLogAction = "新增角色"
	// ActionRoleUpdate 编辑角色。
	ActionRoleUpdate AdminLogAction = "编辑角色"
	// ActionRoleDelete 删除角色。
	ActionRoleDelete AdminLogAction = "删除角色"
	// ActionRoleStatusUpdate 修改角色状态。
	ActionRoleStatusUpdate AdminLogAction = "修改角色状态"
	// ActionRolePermissionUpdate 编辑角色权限。
	ActionRolePermissionUpdate AdminLogAction = "编辑角色权限"
	// ActionPermissionList 查询权限列表。
	ActionPermissionList AdminLogAction = "查询权限列表"
	// ActionPermissionAdd 新增权限。
	ActionPermissionAdd AdminLogAction = "新增权限"
	// ActionPermissionUpdate 编辑权限。
	ActionPermissionUpdate AdminLogAction = "编辑权限"
	// ActionPermissionDelete 删除权限。
	ActionPermissionDelete AdminLogAction = "删除权限"
	// ActionPermissionStatus 修改权限状态。
	ActionPermissionStatus AdminLogAction = "修改权限状态"
	// ActionDocPermissionList 查询文档权限列表。
	ActionDocPermissionList AdminLogAction = "查询文档权限列表"
	// ActionDocPermissionStatus 修改文档权限状态。
	ActionDocPermissionStatus AdminLogAction = "修改文档权限状态"

	// 系统配置与缓存管理相关。
	// ActionSysConfigList 查询系统配置。
	ActionSysConfigList AdminLogAction = "查询系统配置"
	// ActionSysConfigAdd 新增系统配置。
	ActionSysConfigAdd AdminLogAction = "新增系统配置"
	// ActionSysConfigUpdate 编辑系统配置。
	ActionSysConfigUpdate AdminLogAction = "编辑系统配置"
	// ActionSysConfigExport 导出系统配置。
	ActionSysConfigExport AdminLogAction = "导出系统配置"
	// ActionSysConfigImport 导入系统配置。
	ActionSysConfigImport AdminLogAction = "导入系统配置"
	// ActionSysConfigCache 查看系统配置缓存。
	ActionSysConfigCache AdminLogAction = "查看系统配置缓存"
	// ActionSysConfigRenew 刷新系统配置缓存。
	ActionSysConfigRenew AdminLogAction = "刷新系统配置缓存"
	// ActionCacheList 查询缓存列表。
	ActionCacheList AdminLogAction = "查询缓存列表"
	// ActionCacheInfo 查看缓存信息。
	ActionCacheInfo AdminLogAction = "查看缓存信息"
	// ActionCacheSearch 搜索缓存键。
	ActionCacheSearch AdminLogAction = "搜索缓存键"
	// ActionCacheRenew 刷新缓存。
	ActionCacheRenew AdminLogAction = "刷新缓存"
	// ActionCacheRenewAll 刷新全部缓存。
	ActionCacheRenewAll AdminLogAction = "刷新全部缓存"
	// ActionCacheWarmup 预热模板缓存。
	ActionCacheWarmup AdminLogAction = "预热模板缓存"
	// ActionSecretKeyList 查询秘钥列表。
	ActionSecretKeyList AdminLogAction = "查询秘钥列表"
	// ActionSecretKeyGet 查询秘钥详情。
	ActionSecretKeyGet AdminLogAction = "查询秘钥详情"
	// ActionSecretKeyAdd 新增秘钥。
	ActionSecretKeyAdd AdminLogAction = "新增秘钥"
	// ActionSecretKeyUpdate 编辑秘钥。
	ActionSecretKeyUpdate AdminLogAction = "编辑秘钥"
	// ActionSecretKeyStatus 修改秘钥状态。
	ActionSecretKeyStatus AdminLogAction = "修改秘钥状态"
	// ActionSecretKeyRenew 刷新秘钥缓存。
	ActionSecretKeyRenew AdminLogAction = "刷新秘钥缓存"
	// ActionSecretKeyValidate 预检秘钥配置。
	ActionSecretKeyValidate AdminLogAction = "预检秘钥配置"
	// ActionSecretKeySelfCheck 执行秘钥自检。
	ActionSecretKeySelfCheck AdminLogAction = "执行秘钥自检"
	// ActionSecurityDebugSign 安全调试签名。
	ActionSecurityDebugSign AdminLogAction = "安全调试签名"
	// ActionSecurityDebugVerify 安全调试验签。
	ActionSecurityDebugVerify AdminLogAction = "安全调试验签"
	// ActionSecurityDebugEncrypt 安全调试加密。
	ActionSecurityDebugEncrypt AdminLogAction = "安全调试加密"
	// ActionSecurityDebugDecrypt 安全调试解密。
	ActionSecurityDebugDecrypt AdminLogAction = "安全调试解密"

	// 任务、日志与用户标签相关。
	// ActionTaskEnqueue 手动投递任务。
	ActionTaskEnqueue AdminLogAction = "手动投递任务"
	// ActionAdminLogQuery 查询管理员操作日志。
	ActionAdminLogQuery AdminLogAction = "查询管理员操作日志"
	// ActionTaskInfoGet 查询任务详情。
	ActionTaskInfoGet AdminLogAction = "查询任务详情"
	// ActionTaskItemsList 查询任务列表。
	ActionTaskItemsList AdminLogAction = "查询任务列表"
	// ActionTaskRun 立即执行任务。
	ActionTaskRun AdminLogAction = "立即执行任务"
	// ActionTaskDelete 删除任务。
	ActionTaskDelete AdminLogAction = "删除任务"
	// ActionTaskWorkflowTrigger 手动触发工作流。
	ActionTaskWorkflowTrigger AdminLogAction = "手动触发工作流"
	// ActionTaskWorkflowStatus 查询工作流状态。
	ActionTaskWorkflowStatus AdminLogAction = "查询工作流状态"
	// ActionTaskQueueList 查询任务队列概览。
	ActionTaskQueueList AdminLogAction = "查询任务队列概览"
	// ActionTaskConfigReloadItems 查询配置热加载配置项。
	ActionTaskConfigReloadItems AdminLogAction = "查询配置热加载配置项"
	// ActionTaskConfigReloadStatus 查询配置热加载状态。
	ActionTaskConfigReloadStatus AdminLogAction = "查询配置热加载状态"
	// ActionTaskConfigReloadRun 手动触发配置热加载。
	ActionTaskConfigReloadRun AdminLogAction = "手动触发配置热加载"
	// ActionRuntimeConfigOverview 查询运行配置概览。
	ActionRuntimeConfigOverview AdminLogAction = "查询运行配置概览"
	// ActionRuntimeConfigList 查询运行配置。
	ActionRuntimeConfigList AdminLogAction = "查询运行配置"
	// ActionRuntimeConfigSave 保存运行配置草稿。
	ActionRuntimeConfigSave AdminLogAction = "保存运行配置草稿"
	// ActionRuntimeConfigValidate 预检运行配置。
	ActionRuntimeConfigValidate AdminLogAction = "预检运行配置"
	// ActionRuntimeConfigPublish 发布运行配置。
	ActionRuntimeConfigPublish AdminLogAction = "发布运行配置"
	// ActionRuntimeConfigRollback 回滚运行配置。
	ActionRuntimeConfigRollback AdminLogAction = "回滚运行配置"
	// ActionTaskQueuePause 暂停任务队列。
	ActionTaskQueuePause AdminLogAction = "暂停任务队列"
	// ActionTaskQueueResume 恢复任务队列。
	ActionTaskQueueResume AdminLogAction = "恢复任务队列"
	// ActionUserTagWorkflowTrigger 触发用户标签工作流。
	ActionUserTagWorkflowTrigger AdminLogAction = "触发用户标签工作流"
	// ActionUserTagRecalculate 指定标签重新计算。
	ActionUserTagRecalculate AdminLogAction = "指定标签重新计算"
	// ActionUserTagWorkflowLeaseRelease 释放用户标签工作流互斥锁。
	ActionUserTagWorkflowLeaseRelease AdminLogAction = "释放用户标签工作流互斥锁"

	// 通用收集器相关。
	// ActionCollectorOverview 查询Collector观测概览。
	ActionCollectorOverview AdminLogAction = "查询Collector观测概览"
	// ActionCollectorFailureList 查询Collector失败事件。
	ActionCollectorFailureList AdminLogAction = "查询Collector失败事件"
	// ActionCollectorFailureRun 手动执行Collector失败重试。
	ActionCollectorFailureRun AdminLogAction = "手动执行Collector失败重试"
	// ActionCollectorFailureRetry 手动重试Collector失败事件。
	ActionCollectorFailureRetry AdminLogAction = "手动重试Collector失败事件"
)

// CreateAdminLogs 批量创建管理员操作日志。
func CreateAdminLogs(db *gorm.DB, rows []AdminLog) error {
	if len(rows) == 0 {
		return nil
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(&rows, len(rows)).Error
}
