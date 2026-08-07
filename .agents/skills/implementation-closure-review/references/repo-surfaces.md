# 仓库闭环检查面

只选择当前仓库真实存在且受影响的检查面，不要求项目具备不存在的技术栈。

## Go 后端

- 入口：route、handler、CLI、scheduler、worker、consumer、plugin、migration。
- 主链路：middleware、logic/service、repository/model、事务、cache、error/log、response/output。
- 协议：普通 JSON、download、Range、SSE、长轮询、批量、大响应和取消。
- 数据资产：GORM Model、SQL template、Lua、config YAML、embedded asset、表与索引假设。
- 契约：request/response、业务码、i18n、权限、RouteMeta、安全策略和接口文档。
- 运维：metrics/log、retry/idempotency、query plan、scan range、batch/pagination、timeout/concurrency、DB 压力、cache invalidation、migration/backfill/compensation。
- 验证：聚焦测试、全量测试、静态扫描、race、构建和 diff 检查。

## Vben 前端

- page、route、menu、component、drawer/modal、table action、form、store/composable、API wrapper 和 TypeScript type。
- 中英文 i18n、permission、MFA、signature、encryption、route guard 和 session。
- loading、empty、error、disabled、pagination、取消和重复点击状态。
- typecheck、lint/build、提交 hook、浏览器可见流程和响应式布局。

## 文档与配置

- README、文档首页、导航和专题文档的受众、事实、链接与权威来源。
- 配置结构、样例、默认值、热加载/重启边界和运维说明。
- 不保留无验证快照的“最近通过”或未完成能力冒充当前状态。

## 跨仓库

- 传播前逐文件比较，不假设兄弟仓库完全一致。
- 保留服务名、路由、配置、包名、部署和验证差异。
- 默认保持共享 cache/API/security 契约兼容。
