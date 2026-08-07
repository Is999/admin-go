---
name: go-db-query-performance-guard
description: "审查 Go 数据库查询的生产安全性。用于 GORM、原生 SQL、索引、读写路由、大表、分页、批处理、慢查询、EXPLAIN 和数据库压力评估。"
---

# Go 数据库查询性能护栏

## 执行流程

1. 定位查询真实归属：handler、logic、model/repository、task、SQL template、配置和文档。
2. 优先使用 GORM 链式调用；仅在无法安全表达或会明显损害性能时使用原生 SQL，并记录例外原因。
3. 原生 SQL 放入 `*.sql.tmpl`，通过 `go:embed` 加载并在执行前剥离文件头说明。
4. 核对逻辑库和事务路由：`ReadDB`、`WriteDB`、事务 `tx`、dbresolver，以及配置是否真的存在 replica。
5. 评估选择列、谓词、索引顺序、基数、排序/分组/JOIN、`IN` 大小、返回行数、游标、时间窗口、batch、timeout、concurrency 和 retry/backoff。
6. 阻止无界全表扫描、深分页 `OFFSET`、`SELECT *`、无索引排序/聚合/JOIN、巨大 `IN/NOT IN`、请求期 schema 探测和 HTTP 内重聚合。
7. 大聚合、修复、导出和对账使用后台任务、汇总表、ClickHouse/数仓或受控维护流程。
8. 查询、索引或表结构变化时同步 Model、完整初始化 DDL/DML、文档和测试；不新增碎片化迁移 SQL。存量库执行 SQL 放在被忽略的 `data/sql-changes/<change-id>/` 并按 DBA/运维变更单交付。

## 验证与交付

- 先运行查询归属包测试；共享 helper 或并发路径变化时补 vet、race 或编译检查。
- 只对安全的本地/测试数据库运行 `EXPLAIN`；不可用时说明缺失证据和索引推理。
- 交付时说明 GORM/原生 SQL 选择、索引依据、预估扫描与返回规模、批量、超时、并发、DB 压力和跳过项。

每条新增或改变形状的查询使用以下记录，不能只写“已优化 SQL”或“已有索引”：

调用方、查询形状、索引匹配、扫描/返回上限和资源边界是必填事实；原生 SQL 例外、`EXPLAIN`、降级和异步边界按实现与环境触发。没有安全测试库时记录缺失证据和补跑动作，不为填表连接未知数据库。

```text
调用方与频率:
逻辑库/ReadDB/WriteDB/tx:
GORM 链或原生 SQL 例外原因:
选择列:
谓词/order/group/join:
候选索引与最左前缀匹配:
预估扫描行数与返回行数:
分页或游标/batch 上限:
超时/并发/重试:
EXPLAIN 环境及 key/type/rows/Extra，或不可用原因:
降级/限流/异步边界:
测试与监控:
```

通过标准：在线请求具有确定扫描和返回上限，排序/JOIN/过滤能够解释索引选择，批处理可中止和重跑，超时与并发不会形成无界库压力。任一项未知时标记风险和上线前验证动作，不得宣称“生产安全”。
