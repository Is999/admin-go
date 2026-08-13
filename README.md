# Admin 后台管理服务

`admin` 是后台管理服务，负责管理员认证与授权、审计、系统配置、任务调度、文件传输、前台用户管理和用户标签等后台能力。项目采用 go-zero 风格的模块化单体架构，通过同一二进制按运行模式组合启动 HTTP API、Worker 和 Scheduler。

本文面向首次接手项目的开发与运维人员，只保留工程定位、启动方式、核心边界和验证入口。接口字段、任务规则与发布细节以 [`docs/site`](docs/site/文档首页.md) 下的专题文档为准。

> 使用 AI 修改代码、配置、SQL、脚本或文档前，必须先阅读 [AGENTS.md](AGENTS.md)、[AI 开发规范](docs/site/角色文档/后端开发/AI开发规范.md) 和 [AI 开发提示词](docs/site/角色文档/后端开发/AI开发提示词.md)。
> 项目审查必须分栏报告正常逻辑缺陷、极端运行条件和验证环境阻塞；极端运行条件栏内再标明代码处理符合预期或存在韧性缺陷。服务运行路径禁止使用会终止进程的致命调用。完整标准见 [AI 开发规范](docs/site/角色文档/后端开发/AI开发规范.md#bug阻塞异常必须按触发条件分类)。

## 快速导航

| 目标 | 文档或入口 |
| --- | --- |
| 了解系统边界与运行组件 | [系统组件功能说明](docs/site/角色文档/后端开发/系统组件功能说明.md) |
| 开始本地开发 | [本地启动](#本地启动) |
| 新增路由、任务或组件 | [开发扩展指南](docs/site/角色文档/后端开发/开发扩展指南.md) |
| 查询接口契约 | [接口文档首页](docs/site/接口文档/接口文档首页.md) |
| 初始化新库或交付存量库 SQL | [数据库初始化与变更交付治理](docs/site/角色文档/运维/数据库迁移治理.md) |
| 准备发布 | [部署发布指南](docs/site/角色文档/运维/部署发布指南.md) |
| 运行和排查任务系统 | [任务系统运行与操作手册](docs/site/功能模块/任务系统/任务系统使用手册.md) |
| 操作或排查用户标签 | [用户标签操作与验收手册](docs/site/功能模块/用户标签/用户标签操作手册.md)、[故障处置手册](docs/site/功能模块/用户标签/任务系统与用户标签排障手册.md) |

## 服务边界

Admin 负责后台控制面和异步执行面：

- 管理员登录、MFA、RBAC、权限码和审计日志。
- 系统配置、运行配置、密钥、缓存、消息和安全调试。
- 前台用户资料、状态、密码和运行态同步。
- Asynq 队列、工作流、周期调度、任务监控和失败归档。
- 用户标签计算、事件 outbox、定向重算和排障入口。
- 本地或 S3 文件存储、断点续传和异步导出。
- Kafka Collector 消费、失败账本、重试和告警。

面向前台用户的认证和资料接口由同工作区的 `api-go` 负责。Admin 不应承载前台请求热路径，也不应复制 API 服务的登录态实现。

## 技术栈

- Go `1.26.6`、go-zero HTTP 框架。
- GORM + MySQL，支持主从读写路由和命名扩展库。
- Redis + redsync，用于缓存、分布式锁和运行态状态。
- Asynq + robfig/cron，用于 Worker、Scheduler 和工作流。
- Kafka Collector、OpenTelemetry Trace、Prometheus 指标和结构化日志。
- JWT、MFA、RBAC，以及可选的签名验签和字段级加解密。

## 分支与表路由

`admin-go` 与 `api-go` 必须成对使用同名分支和同一套路由配置。三条长期分支互斥，不能叠加：

| 分支 | 定位 | 数据路由职责 |
| --- | --- | --- |
| `main` | 唯一公共开发与交付基线 | 默认单表；`user.route_shard_count` 支持 `1/2/4/.../1024`，生产拆分需选用对应候选分支的迁移流程 |
| `table-sharding/shardingsphere-proxy-alternative` | ShardingSphere-Proxy 候选方案 | 应用访问逻辑表；Proxy 管理物理表和路由，Admin 提供部署、回填与规则工具 |
| `table-sharding/app-table-sharding` | 应用内固定桶分表候选方案 | Admin/API 计算物理表名；Admin 负责在线复制、校验和配置切换 |

Proxy 分支相对 `main` 只允许方案文档和 Proxy 部署资产差异；应用分表分支只允许物理表路由、复制/校验/切换工具及对应文档差异。合并后执行下列命令，退出码必须为 `0`，报告中不得出现允许清单之外的代码、配置、SQL 或接口差异：

```bash
make branch-drift-check
```

在候选分支可通过 `BRANCH_VARIANT=proxy` 或 `BRANCH_VARIANT=app` 指定检查目标。

## 运行架构

```text
cmd/admin
  -> bootstrap.WireWithConfigMode
  -> 加载并校验配置，初始化日志、Trace、MySQL、Redis、Kafka
  -> 创建 ServiceContext，注册启动组件和任务插件
  -> 按 mode 启动 API、Worker、Scheduler
  -> handler / worker / scheduler 接收请求或任务
  -> logic / jobs 编排业务、事务、缓存和队列
  -> model / infra / task / pkg 访问数据库、Redis、MQ、存储和外部服务
```

HTTP 响应统一为：

```json
{
  "status": true,
  "code": 1,
  "message": "成功",
  "data": {},
  "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",
  "spanId": "00f067aa0ba902b7"
}
```

`traceId` 关联完整请求链路，`spanId` 定位当前处理片段；中间件同时回传 `X-Trace-Id` 和 `X-Span-Id` 响应头。

## 运行模式

`-mode` 是位掩码，解析优先级为命令行 `-mode`、配置文件 `run_mode`、默认值 `7`。

| mode | 启动单元 |
| --- | --- |
| `1` | API |
| `2` | Worker |
| `3` | API + Worker |
| `4` | Scheduler |
| `5` | API + Scheduler |
| `6` | Worker + Scheduler |
| `7` | API + Worker + Scheduler |

生产环境通常拆分控制面和执行面：

```bash
./bin/admin -f ./etc/config.yaml -mode 5
./bin/admin -f ./etc/config.yaml -mode 2
```

## 目录结构

本分支使用 ShardingSphere-Proxy 管理物理拓扑。运行配置保持 `user.route_shard_count=1`，共享的 `internal/sharding` 路由函数返回逻辑表名；应用写入固定 `shard_no`，Proxy 根据规则定位物理表。

| 目录 | 职责 |
| --- | --- |
| `cmd` | Admin、迁移、固定桶回填和 ShardingSphere DistSQL 规则生成入口 |
| `common` | 业务码、i18n、Redis Key、嵌入资产和稳定公共契约 |
| `docs` | 文档站、接口文档、运维手册、Prometheus 与 Grafana 资产 |
| `deploy` | Docker、systemd、集成环境和 ShardingSphere-Proxy 部署资产 |
| `etc` | 配置样例、本地配置和运行期配置文件 |
| `internal/bootstrap` | 配置加载、组件装配、热加载和生命周期 |
| `internal/handler` | 路由规格、鉴权审计、参数解析和响应写出 |
| `internal/logic` | 用例编排、规则校验、事务和缓存边界 |
| `internal/jobs` | 归档、导出、用户标签等后台业务任务 |
| `internal/model` | GORM Model、固定分片字段、Proxy 逻辑表和数据访问 |
| `internal/sharding` | 与 main 共用固定桶映射；本分支配置为 1，仅返回逻辑表名 |
| `internal/task` | Asynq 队列、工作流、任务插件和调度运行时 |
| `internal/infra` | MySQL、Redis、Kafka、日志、Trace 和外部适配 |
| `internal/types` | 请求、响应、列表项和参数校验契约 |
| `pkg` | 文件存储、传输、Excel 和批处理等可复用能力 |

`data/` 和 `logs/` 是本地运行目录。不要提交本地密钥、上传文件、临时数据或日志。

## 统一注册点

| 对象 | 权威入口 |
| --- | --- |
| 启动组件 | `internal/bootstrap/components/builtin/specs.go:DefaultSpecs` |
| HTTP 路由 | `internal/handler/routes.go:builtinRouteModuleSpecs` 与各模块 `RouteSpecs` |
| RouteMeta | `internal/handler/shared/route_meta.go:DefaultRouteMetas` |
| 路由安全清单 | `internal/handler/route_security_manifest.go:DefaultRouteSecurityManifest` |
| 任务插件 | `internal/bootstrap/components/builtin/task.go:DefaultTaskPluginSpecs` 与 `internal/jobs/plugins.go:PluginSpecs` |
| 运行时扩展 | 各能力归属包的 `RuntimeRegistrySpecs`；组件文档逐项标明“生产已启用”或“基础能力/未激活” |
| 数据库初始化基线 | `internal/database/migrations.go:defaultMigrationSpecs` |
| Redis Key | `common/rediskeys` |

完整登记规则见[组件注册清单](docs/site/角色文档/后端开发/组件注册清单.md)。修改路由、RouteMeta 或安全字段后执行：

```bash
make update-route-security-manifest
```

## 本地启动

### 1. 准备依赖

必需依赖为 Go `1.26.6`、MySQL 和 Redis；Kafka、对象存储、OTLP 等依赖按启用能力准备。

```bash
cp etc/config.dnmp.sample.yaml etc/config.yaml
cp etc/config.d/runtime.sample.yaml etc/config.d/runtime.yaml
go mod download
```

修改本地配置中的数据库、Redis、`app_id`、`jwt_secret`、安全密钥、运维令牌和存储路径。不要把真实生产密钥写入样例文件。

### 2. 初始化本地空库

```bash
make migrate-status MIGRATE_CONFIG=./etc/config.yaml
make migrate-dry-run MIGRATE_CONFIG=./etc/config.yaml
make migrate-bootstrap MIGRATE_CONFIG=./etc/config.yaml
```

以上命令只用于确认无业务数据的全新空库。仓库内 DDL/DML 是完整初始化基线，不负责升级已有数据库；存量库变化由开发在被忽略的 `data/sql-changes/<change-id>/` 生成本地增量 SQL，通过发布工单交给 DBA/运维命令行执行，SQL 文件不提交仓库、不追加迁移版本。完整边界见[数据库初始化与变更交付治理](docs/site/角色文档/运维/数据库迁移治理.md)。

### 3. 启动服务

```bash
go run ./cmd/admin -f ./etc/config.yaml -mode 7
```

查看构建版本：

```bash
go run ./cmd/admin -version
go run ./cmd/migrate -version
```

## 配置边界

- `etc/config.sample.yaml`：标准配置样例。
- `etc/config.dnmp.sample.yaml`：本地 dnmp 环境样例。
- `etc/config.yaml`：本地实际配置，不应提交生产密钥。
- `etc/config.d/runtime.sample.yaml`：运行期工作流配置样例，不包含周期任务或归档任务。
- `etc/config.d/runtime.yaml`：本地运行期工作流配置。

项目自有 YAML 样例中的每个固定配置字段必须保留紧邻字段上方、与字段同缩进的中文注释。注释至少写明消费组件和用途，并如实补充源码已定义的取值/单位、缺省与空值、热加载或重启、敏感信息及跨字段约束；父节点概述不能替代子字段说明。动态 map 的重复数据项由父字段统一定义 key/value、空值和合并语义，第三方 schema 与纯数据文件按 [AI 开发规范](docs/site/角色文档/后端开发/AI开发规范.md#yaml-配置字段行级注释)记录排除依据。

当 `runtime_config.source=database` 时，`task_periodic` 和 `archive_jobs` 只由数据库草稿与 active release 管理；首次启动只发布初始化 SQL 写入的草稿，运行期文件仅加载 `workflows` 并参与热更新。

运行期参数可由已有应用器热加载；HTTP 监听、MySQL、Redis、Kafka、OTLP、路由、任务插件和 workflow 定义等启动期能力变更后必须重启。字段说明见[配置字段说明](docs/site/角色文档/后端开发/配置字段说明.md)。

## 开发与验证

日常开发按改动范围运行：

```bash
make fmt-check
make test
make vet
make build
make build-tools
git diff --check
```

发布候选运行完整门禁：

```bash
make ci
```

`make ci` 包含格式、全量测试、race、vet、构建、密钥扫描、依赖漏洞检查、Prometheus 规则、分支差异和 diff 检查。缺少 `promtool` 时会尝试使用 Docker 镜像。

开发时遵守以下分层：

- Handler 只负责路由、参数、鉴权审计和响应；业务流程进入 Logic。
- Logic 负责用例、事务、缓存和错误上下文，不临时创建基础设施连接。
- Model 优先使用 GORM 链式调用，事务内始终沿用同一个 `tx`。
- 原生 SQL 与 Redis Lua 必须作为可审查的嵌入资产维护。
- 新增接口同步 types、RouteMeta、权限、审计、安全字段、业务码、i18n、文档和前端契约。
- 新增任务或运行时能力必须接入统一注册点并补测试，不能只提交孤立实现。

## 文档与观测

- Admin 文档站：`/api/docs`。
- 存活探针：`/api/live`，不访问外部依赖。
- 就绪探针：`/api/ready`，检查启用的关键依赖和组件。
- Prometheus 指标：`/api/metrics`。

前台 API 文档通过 Admin 文档站的 `/api/docs/api` 独立入口展示，由 Admin 通过内网代理读取 `api-go` 文档资源，浏览器不直接访问 API 内网文档接口。

首次上线若无法登录，按[内网初始化管理员接口](docs/site/接口文档/后台系统/内网初始化管理员接口.md)重置已存在的超级管理员。该接口不创建账号、不提升角色，并要求 Ops HMAC、一次性 nonce、Redis 防重放；跨主机链路使用 mTLS。

## 发布检查

以下条目必须保留命令输出、接口响应、任务记录或发布工单作为证据，不能只填写“正常”或“已确认”：

- 新空库已按 `migrate-status -> migrate-dry-run -> migrate-bootstrap` 顺序初始化且进程退出码为 `0`；已有环境的 DBA/运维工单已记录 SQL SHA-256、执行顺序、影响行数和 `90_verify.sql` 或等价校验结果。候选版本中不存在一次性增量 SQL。
- `/api/live` 返回 HTTP 2xx 且不依赖外部服务；`/api/ready` 返回 HTTP 2xx，并且响应中所有已启用关键依赖与组件均为就绪；`/api/metrics` 可被目标 Prometheus 抓取；授权管理员可打开 `/api/docs`，未授权请求按鉴权策略拒绝。
- API、Worker、Scheduler 按目标 `mode` 启动。启用任务能力时，至少一条受控冒烟任务能够完成 enqueue/claim/execute/terminal 状态转换；启用周期任务时，active release 与目标发布版本一致且下一次触发时间可见；启用 Collector 时，目标 topic、consumer group、失败账本和告警通道均已验证。
- MySQL、Redis、Kafka、Lark、Trace 和文件存储的实际连接目标与发布环境清单一致；通过脱敏诊断、就绪检查或受控测试验证，禁止在交付记录中输出密码、token、私钥或完整 DSN。
- 配置与部署资产已通过密钥扫描；样例密钥、私钥、AES Key、对象存储密钥和运维令牌未进入 Git、镜像、日志或发布工单正文。真实密钥只从目标环境的密钥管理渠道注入。

发布资产和回滚流程见[部署发布指南](docs/site/角色文档/运维/部署发布指南.md)。

## License

Internal use only.
