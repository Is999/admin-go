---
name: go-migration-backfill-runbook
description: "规划并复核 Go 数据库初始化与存量变更交付。用于 DDL、DML、表/字段/索引、Model 对齐、seed、回填、数据修复、清理、DBA 执行、上线顺序、回滚和补偿，阻止碎片化迁移 SQL 进入仓库。"
---

# Go 数据库初始化与变更交付

## 硬边界

- 仓库只维护全新空库所需的完整初始化 DDL/DML；直接修改原始资产，不为需求增量新建迁移 SQL。
- `defaultMigrationSpecs` 只登记稳定初始化资产，不追加日期化、工单化或版本化的日常变更。
- 存量库增量 DDL/DML、回填、修复和补偿 SQL 只生成到被忽略的 `data/sql-changes/<change-id>/`，通过发布工单交给 DBA/运维命令行执行。
- 本地 SQL 不提交、不暂存、不登记到项目 `schema_migrations`；执行证据保存在工单或 DBA 平台。
- 运行时代码真实调用的 `*.sql.tmpl` 仍是代码资产；不要把它与一次性 DBA SQL 混淆。
- 已产品化的 CLI、分表或部署工具可维护稳定 SQL 资产，但必须有真实调用方、明确输入、测试和长期复用边界；不能借此提交单次需求执行包。

## 执行流程

1. 确认数据契约：目标环境、实例、库表、当前部署版本、真实表结构、数据量、Model、索引、读写路径、API/任务依赖和兼容窗口。
2. 判断资产类型：完整初始化基线、运行时 SQL 或存量库一次性变更；三者使用不同存放和交付方式。
3. 修改仓库原始初始化 DDL/DML，使新空库一次初始化即可得到当前完整结构和基线数据；同步 Model、代码、文档和测试。
4. 保持初始化清单稳定。禁止新增 `add_*`、`alter_*`、`sync_*`、`*_seed_*`、`*_repair_*`、`*_backfill_*` 迁移资产或日常迁移版本。
5. 根据“当前部署库真实状态 → 目标状态”生成本地 SQL，不直接把完整初始化资产用于已有库。
6. 在本地变更包中只创建真实需要的 precheck、DDL、DML、verify、compensate 和 runbook 文件，不生成空白占位模板。
7. 使用 `$go-db-query-performance-guard` 复核索引、扫描、锁、batch、timeout、concurrency、主从延迟和 DB 压力。
8. 在安全测试库验证前置检查、执行顺序、受影响行数、幂等/断点、结果校验和恢复路径。
9. 通过受控渠道向 DBA/运维交付 SQL SHA-256、目标范围、版本前提、顺序、风险、备份、停止条件、验证和恢复方案。
10. 检查 `git check-ignore`、`git status --short` 和暂存区，确保本地 SQL 没有进入仓库。

## 本地变更包

推荐目录为 `data/sql-changes/<change-id>/`。只在满足右侧触发条件时创建对应文件：

- `00_precheck.sql`：已有库状态会影响 SQL 安全执行，或需要确认目标库、版本、结构、索引、重复/空值数据时创建。
- `10_ddl.sql`：存量库需要新增、修改或删除表、字段、索引或约束时创建；只有 DML 时不创建。
- `20_dml.sql`：存量库需要回填、修复、清理或基线数据变更时创建；必须具有索引条件、影响行数保护或分批边界。
- `90_verify.sql`：DDL 或 DML 执行后需要核对结构、索引、行数、唯一性、空值或业务口径时创建；只要存在数据库变更，默认应提供等价验证证据。
- `99_compensate.sql`：存在可安全执行的前向补偿时创建；MySQL DDL 或不可逆 DML 无法脚本恢复时，在 RUNBOOK 中写备份恢复路径，不创建虚假回滚 SQL。
- `RUNBOOK.md`：涉及多个 SQL、顺序依赖、大表、分批、停写、观察/停止条件或跨服务发布时创建，写明责任人与恢复决策。

目录约定不能变成模板生成要求；无对应动作时不要创建空文件。

## DDL 复核

- 对齐 Model 与完整初始化 DDL 的表名、类型、长度、默认值、NULL、主键、索引、约束、时间和软删除语义。
- 确认 MySQL 版本、表大小、metadata lock、表重建、临时磁盘、写阻塞和主从延迟风险。
- 大表使用 DBA 批准的在线 DDL 工具和限速方案，不在 HTTP、Worker、Scheduler 或应用启动时动态改表。
- 删除字段、索引或表前，证明所有调用方已停止读写并经过兼容发布窗口。
- 不依赖事务自动回滚 MySQL DDL；准备备份恢复或前向补偿。

## DML、回填与修复复核

- `UPDATE`、`DELETE` 和回填使用可命中索引的确定性条件；先用同条件 `SELECT` 核对预计范围。
- 按主键游标或时间窗口分批，明确 batch、timeout、sleep/backoff、concurrency、checkpoint、取消和重跑幂等性。
- 禁止无界全表写、巨大单事务、深分页、超大 `IN/NOT IN` 和 HTTP 在线回填。
- 明确应用与数据顺序：先 SQL、先兼容代码，或扩展—回填—切换—清理；没有真实需要时不引入双轨。
- 数据清理优先归档或软删除；不可逆删除必须有备份、审批和影响行数保护。

## 验证与交付

- 初始化 DDL/DML 变化时运行 schema 渲染、Model/DDL 对齐和 seed 顺序测试；Model 或数据访问变化时运行 database/model 包及直接调用包测试；回填/修复任务变化时运行 task/jobs 测试；MySQL 方言、DDL 锁或事务语义变化时在隔离测试库执行集成验证。
- 使用 `git check-ignore -v data/sql-changes/<change-id>/<file>` 证明本地变更包被忽略。
- 使用 `git diff --check`、`git diff --cached --check` 和 `git status --short` 证明没有碎片化 SQL 进入候选版本。
- 最终说明仓库初始化资产改动、DBA/运维人工步骤、代码发布顺序、缓存影响、观察指标、停止条件、回滚/补偿和未验证风险。
