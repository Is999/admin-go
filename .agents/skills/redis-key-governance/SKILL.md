---
name: redis-key-governance
description: "治理 Go Redis Key 与缓存契约。用于 Key 收口、共享缓存、TTL、失效、重建、SCAN/KEYS 风险、helper、registry、Lua/CAS 和跨服务兼容。"
---

# Redis Key 治理

## 执行流程

1. 读取生效的 `AGENTS.md`、AI 文档和 `references/project-map.md`，定位现有 Key helper、registry 和静态检查。
2. 修改调用点前确认 Key 归属、数据结构、基数、TTL、miss 语义、失效与重建路径。
3. 保持跨服务共享缓存兼容；除非用户明确要求隔离，不添加 repo-name 前缀或改变 hash tag。
4. 使用集中 helper 和模板 registry，禁止业务代码散落字符串拼接。
5. 把高基数 `SCAN`/`KEYS` 替换为精确 Key、索引集合、白名单模板、异步任务或静态 registry。
6. 多 Key Lua/CAS 逐项核对 Redis Cluster hash tag，保证同一原子操作涉及的 Hash、索引、版本、计数和锁位于同槽。
7. 覆盖旧版本覆盖、高并发精确计数、TTL、owner 校验、缓存穿透和失败补偿。
8. 需要长期保持的规则补静态测试或扫描。

## 扫描脚本

```bash
python3 <skill-dir>/scripts/redis_key_scan.py <repo-or-dir>
```

默认跳过 `*_test.go`，只检查生产 Go/Lua 中的 Redis/Rds 调用、`redis.call/pcall` 和 Key 变量赋值；需要复核测试 fixture 时显式增加 `--include-tests`。扫描结果只作为审查线索，修改前必须结合本地 helper 包确认每个命中。

脚本变更后运行 `python3 -m unittest discover -s <skill-dir>/scripts -p 'test_*.py'`。

## 交付证据

每个新增或改变语义的 Key 按以下契约记录：

模板、归属方、数据形状、基数、TTL、miss、写入和失效/重建是缓存正确性的必填事实；Cluster 同槽和旧 Key 兼容只在多 Key 原子操作或兼容窗口存在时展开。不得为填表臆造永久 Key、迁移期或清理任务。

```text
模板/helper/registry:
归属服务与调用方:
数据类型/字段/value 编码:
基数与上限:
TTL 来源/刷新规则/不过期原因:
miss 行为与写入顺序:
失效/重建/失败补偿:
Cluster hash tag 与 Lua/CAS Keys:
旧 Key 兼容与清理条件:
测试与静态扫描:
```

通过标准：业务代码没有散落拼接，同一语义只有一个 helper；高基数路径不依赖在线 `SCAN/KEYS`；TTL、miss、失效和重建形成确定链路；多 Key 原子操作可证明同槽；旧 Key 的兼容/清理有明确版本条件。任一项未知时标记风险，禁止只写“Redis Key 已收口”。

交付时说明修改文件、调用点、验证命令与结果，以及是否需要回填、清缓存、重建索引集合或补偿。
