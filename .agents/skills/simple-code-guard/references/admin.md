# Admin 项目边界

## 真实链路

- 外部入口包括 HTTP route、CLI、Scheduler、Asynq Worker、Kafka Consumer 和 bootstrap component。
- HTTP 沿 `handler -> logic -> model/infra`；后台任务沿 `scheduler/queue -> jobs -> model/infra` 到 DB、Redis、MQ、存储或告警。
- 运行时接线集中在 `internal/bootstrap`、`internal/svc` 和 `internal/config`，不要增加平行容器或第二套生命周期框架。
- 队列、Collector、归档和周期任务是长生命周期并发组件；租约、幂等、原子状态、停止期限和 readiness 是已存在的生产边界。

## 简化重点

- `internal/task`：主流程直接呈现依赖检查、投递、结果归并和终态；Go 预检查不能与 Lua CAS 维护冲突的状态规则。
- `internal/infra/collectorx`：批量、分区顺序、失败账本和幂等只保留一套生产实现。
- `pkg/storage` 的对象存储注册、文件上传策略注册和 bootstrap Option 属于项目扩展基础能力；内置实现与外部注册入口必须分别标注启用状态，不能因标准命令没有调用外部注册方法而删除。
- `internal/jobs`：任务特有的复制、删除、checkpoint 和租约贴近批次循环，不为单一任务建立通用工作流框架。
- `internal/bootstrap`：组件只负责注册、启动、停止、健康与接线；业务逻辑留在所属包。
- readiness 只探测已启用且必要的底层能力，检查保持有界、无重查询、无业务副作用。
- 热路径优先减少 DB/Redis/MQ 往返；错误、告警、启动和关闭路径必须保留触发操作、失败状态、重试/恢复动作和最终返回，禁止用无状态 wrapper 或合并分支隐藏故障边界。

## 不可破坏边界

- RouteSpec、RouteMeta、权限、审计、安全字段、业务码、i18n 和接口文档同步。
- GORM 事务、读写路由、SQL/Lua 嵌入资产和 Redis Cluster 同槽语义。
- Worker 锁 owner、幂等、retry/terminal、取消、清理和补偿。
- 新增或重构的稳定契约和关键逻辑补中文注释，至少说明业务原因、数据来源、单位/范围、并发、失败或安全边界中的一项；只翻译标识符或逐行复述实现的注释必须删除或重写。

## 验证

- Go 改动运行 gofmt；先运行改动文件归属包与全部直接调用包测试，再执行 `go test ./...` 和 `go vet ./...`。goroutine、锁、共享状态、取消或关闭逻辑变化时，对归属包和直接调用包运行 `go test -race -count=1`。
- 初始化执行器、初始化资产或 Model/DDL 契约改动补 `make integration-test`；指标或告警改动补 `make promtool-check`。
- 最终运行 `git diff --check`、`git diff --cached --check`（存在 staged 文件时）和 `git status --short`。
