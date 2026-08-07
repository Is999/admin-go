# API 项目边界

## 真实链路

- HTTP 从 RouteSpec 进入 middleware，再到 `handler -> logic -> model/svc`，最终访问 DB、Redis 或外部服务。
- 启动、组件状态、配置和迁移位于 `cmd`、`internal/bootstrap`、`internal/svc`、`internal/config` 和 `internal/database`。
- API 是前台请求热路径，不复制 Admin 的任务队列、Scheduler 或运营工作流；只同步真正共享的安全、数据和缓存契约。
- `common/localcache`、`SysConfigLogic`、`SysConfigKey` 和 table-cache 目标若当前业务 route/logic 未调用，归类为基础能力/未激活：保留共享数据与缓存契约、聚焦测试和接入文档，但不得写成生产链路已启用，也不得再建重复实现。

## 简化重点

- Handler 只接收参数和写响应；Logic 直接表达业务规则，不为单一调用叠加 service、repository 或 mapper 包装。
- Middleware 保持顺序和短路语义；减少查询不能削弱鉴权、签名、加密、限流或即时撤销。
- 每请求 DB/Redis 操作先核对频率、索引、缓存一致性和撤销时效，再决定批量、缓存或合并。
- 迁移互斥、节点租约和 readiness 可与 Admin 对齐语义，但独立仓库不为消除少量重复增加共享发布依赖。
- 内部重构前后逐项比较错误码、HTTP 状态、响应字段/空值、安全字段点路径和接口文档示例；需求未授权契约变化时，任何新增、删除、改名或语义变化都视为回归。
- 冷路径不使用难读微优化；请求热路径优先减少外部 I/O、扫描和无界分配。

## 不可破坏边界

- JWT/session 生命周期、`auth_version`、Redis 原子操作和多实例竞态。
- 签名/加密字段子集、大小限制、流式响应和失败业务码。
- GORM 读写路由、事务、身份索引和表路由。
- Admin 运行态同步与内网文档代理边界。

## 验证

- Go 改动运行 gofmt；先运行改动文件归属包与全部直接调用包测试，再执行 `go test ./...` 和 `go vet ./...`。goroutine、锁、共享状态、取消或关闭逻辑变化时，对归属包和直接调用包运行 `go test -race -count=1`。
- 初始化执行器、初始化资产或 Model/DDL 契约改动补 `make integration-test`；安全契约变化补 manifest 与共享向量检查。
- 最终运行 `git diff --check`、`git diff --cached --check`（存在 staged 文件时）和 `git status --short`。
