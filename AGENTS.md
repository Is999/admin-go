# AGENTS.md

本文件是 Codex 参与 `admin-go` 开发的仓库入口。修改代码、配置、SQL、脚本或文档前，必须完整阅读：

- `docs/site/角色文档/后端开发/AI开发提示词.md`
- `docs/site/角色文档/后端开发/AI开发规范.md`

## 禁止概括性规则

- “简洁、清晰、闭环、同步、必要时、相关、验证通过、生产可用”等词不能单独作为实施要求或完成结论；必须按 AI 开发规范补出触发条件、检查对象、通过标准、禁止项和证据。
- “闭环”必须核对真实入口、主调用链、成功/失败分支、数据或外部副作用、注册与配置、同步面、测试和运维后续；helper、脚手架、DTO、单一 happy path 或孤立测试不能证明完成。
- “同步”必须先按源码变化和调用方搜索识别触发面，再记录源码依据、实际修改文件和验证结果；route、request/response、RouteMeta、权限/安全、业务码/i18n、配置、Model、初始化 DDL/DML、SQL/Lua、Redis Key、前端契约、文档、测试和运维步骤只作为发现清单，不能变成机械填表。
- “验证通过”必须记录最后一次修改后的命令、执行目录、范围和结果；旧快照、只测 helper 或只编译不能证明整项需求完成。

## 执行规则

1. 先确认用户要求属于分析、实现、审查、验证还是交接；先读现有目录、代码、调用链、测试和相关文档，再修改文件。
2. 需求含糊、存在多种解释、目标入口不清，或触及 API、数据、权限、安全、大表、缓存、迁移和补偿时，先说明理解、源码证据、执行边界与待确认点；未经确认不得猜测实现。
3. 优先延续现有分层、注册点和成熟依赖。新增抽象、目录、框架或依赖前，说明必要性、替代方案、影响范围和验证方式，并取得开发确认。
4. 每个修改文件和关键 diff 都必须映射到需求项、必要同步面或验证修复；无映射的重命名、格式化、目录调整和顺手重构不得混入本任务，也不得回滚用户已有改动。
5. 每个新增 helper、接口、包装层、状态和分支都必须能指出当前需求、真实调用方、稳定复用边界或故障证据；只转发参数且没有校验、转换、事务、重试或依赖隔离职责的包装应内联。
6. 目录、文件、变量、方法、常量、结构体、接口和字段使用短而准确的业务名称；职责变化时同步改名。
7. 新增或重构的变量、常量、方法、结构体、字段、map 字段、路由、GORM Model、接口和关键逻辑补中文注释。新增或修改的项目自有 YAML 固定配置字段必须在字段正上方以相同缩进补中文注释，至少说明消费组件和用途；源码存在枚举/范围/单位、缺省/空值/零值、normalize/getter、热加载/重启、敏感信息或跨字段约束时逐项写明。父节点概述不能替代子字段注释；动态 map 重复数据项只有在父字段已定义 key/value、空值和合并语义时才可不重复注释；第三方 schema、纯数据清单或生成文件必须记录具体排除依据。
8. 接口或行为变化时，先按源码 diff 和调用方搜索识别触发面。route、request/response、RouteMeta、权限/安全、业务码/i18n、配置、Model、初始化 DDL/DML、SQL/Lua、Redis Key、前端 wrapper/type、文档、测试和运维步骤是发现清单，不是固定填空表；每个触发面记录源码依据、修改文件和验证结果。只有常见风险面容易被误判为已处理时才写不适用及具体依据，禁止用一个“不适用”概括所有未检查项。
9. MySQL 默认使用 GORM 链式调用。原生 SQL 仅在 GORM 无法安全表达或会明显损害性能时使用，并说明索引、扫描范围、批量、超时、并发和数据库压力。
10. 大表、大结果集和高频查询先评估索引、执行计划、返回规模与并发；禁止在线接口执行无边界查询或高基数 Redis 通配扫描。
11. 仓库只维护用于全新空库的完整初始化 DDL/DML；需求变更直接修改原始初始化资产，不新增碎片化迁移 SQL，不向 `defaultMigrationSpecs` 追加日常版本。存量库增量 SQL 只能放在被忽略的 `data/sql-changes/<change-id>/`，通过发布工单交给 DBA/运维命令行执行，禁止提交或暂存。

## Skill 路由

非平凡开发、修复、重构、生产复核和交付前检查必须先使用 `$standard-development-flow`，再按场景使用专项 skill：

| 场景 | 必用 skill |
| --- | --- |
| 需求含糊或高风险边界 | `$requirement-intent-gate` |
| Go 实现、修复或重构 | `$go-ai-implementation-guard` |
| 代码优化、简化或性能复核 | `$simple-code-guard` |
| 全仓注释与可维护性审计 | `$go-comment-style-audit` |
| API、路由、业务码或安全字段变化 | `$api-contract-sync` |
| Redis Key、TTL、失效或扫描治理 | `$redis-key-governance` |
| GORM、SQL、索引或大查询 | `$go-db-query-performance-guard` |
| 测试、vet、race、构建或 CI 失败 | `$go-test-failure-debugger` |
| 配置、默认值、热加载或重启边界 | `$go-config-runtime-guard` |
| Worker、Scheduler、队列、锁或重试 | `$go-worker-concurrency-guard` |
| JWT、MFA、权限、签名或加密 | `$go-security-route-guard` |
| DDL、Model、迁移、回填或补偿 | `$go-migration-backfill-runbook` |
| 兄弟仓库比较与同步 | `$sibling-repo-sync-review` |
| 需求闭环、命名和交付完整性 | `$implementation-closure-review` |
| 最终验证、暂存与运维交接 | `$release-handoff-guard` |

## 必跑检查

- Go 改动优先运行 `go test ./...`；范围很小或环境受限时至少运行相关包测试，并说明未跑全量的原因。
- 前端类型或 API wrapper 变化时运行 `pnpm -F @vben/web-antd run typecheck`；页面、路由或交互变化时补浏览器冒烟，构建配置或依赖变化时补 build，源码格式或提交规则变化时补 lint 或提交 hook。
- 每轮结束前在本仓库运行 `git diff --check` 和 `git status --short`；存在 staged 文件时再运行 `git diff --cached --check`。
- 只暂存本轮新增且属于交付范围的文件，不添加本地数据、构建产物或历史无关文件。

## 交付要求

最终说明必须列出修改内容、同步面、验证命令与结果、跳过项和剩余风险，并明确是否需要迁移、回填、缓存失效、重启、回滚或补偿。
