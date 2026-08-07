---
name: api-contract-sync
description: "同步 Go 后端 API 契约。用于新增或修改 route、handler、request/response、业务码、权限、RouteMeta、审计、安全字段、接口文档、前端类型和 i18n。"
---

# API 契约同步

## 执行流程

1. 读取生效的 `AGENTS.md`、接口文档规范和 `references/project-map.md`，从当前仓库发现真实契约入口。
2. 沿 route、middleware、handler、logic、model 和 response wrapper 追踪行为，再核对接口文档与前端调用方。
3. 路由变化时同步 RouteMeta、权限码、审计动作、公开或白名单原因、鉴权链和安全策略；未被源码触发的面不展开，只有常见风险面容易被误判时才记录不适用及依据。
4. request/response 变化时同步 Go 类型、校验、接口示例、前端 wrapper、TypeScript 类型和用户可见 i18n。
5. retry、timeout、batch、concurrency 和 pagination 字段同时维护服务端硬上限、前端输入约束和文档；前端限制不能替代后端校验。
6. 业务错误使用独立业务码并补中英文消息；外部 message 不暴露 SQL、密钥、表名或真实下游错误。
7. 敏感契约保持 MFA、签名、加密、密钥版本、字段数量和大小限制一致；响应字段满足 `ResponseCipher ⊆ ResponseSign`。
8. 认证契约变化时完整复核 JWT claim、会话 ID、`auth_version`、Redis 原子语义、会话上限、DDL 和存量迁移说明，不能只修改单个 handler。

## 验证

- route、middleware、handler、request/response 或业务码变化时，运行直接归属包测试及路由/契约防漂移测试，并覆盖成功、校验失败、鉴权失败和新业务码。
- 只改变 TypeScript 类型或 API wrapper 时，读取前端仓库规则并运行真实 typecheck；改变页面字段、按钮、路由、权限可见性或交互时，再运行对应 lint 与浏览器冒烟。
- 权限或安全字段变化时，补未登录、无权限、MFA/签名/加密失败、字段超限和公开/内网路由隔离测试；普通成功响应不能替代安全失败验证。
- 运行 `git diff --check`，检查文档链接、业务码/i18n 覆盖和仓库状态。

## 契约影响记录

这份记录用于证明“源码中的契约变化已经传播到所有被触发的消费面”，不是运行时配置、接口 schema 或要求每个接口填写全部字段的固定表单。

先按一个可独立发布和验证的契约变化建立记录，必填内容只有：

```text
变更项与受影响接口:
源码依据:
变更类型:
已触发的同步面、修改文件与动作:
验证命令、执行目录、候选快照与结果:
未覆盖路径或阻塞:
数据与运行时后续:
```

再根据源码变化展开对应同步面；未触发的行不制造空字段：

| 触发条件 | 必须核对的同步面 |
| --- | --- |
| method、path、注册 server、route alias 或 middleware 变化 | 路由注册、RouteMeta、访问级别、权限、审计、公开/白名单依据、接口文档和路由测试 |
| request、response、校验或 wrapper 变化 | Go 类型、字段来源与空值、服务端上限、响应包装、接口示例、前端 wrapper/type 和契约测试 |
| HTTP 状态或业务错误变化 | 独立业务码、中文/英文消息、前端错误映射、日志脱敏和失败分支测试 |
| JWT/session、MFA、签名、加密或权限变化 | 安全策略、字段点路径与大小、密钥版本、nonce/重放、失败业务码、前端安全流程和安全测试 |
| 前端页面、按钮、路由或可见性受影响 | API wrapper、TypeScript 类型、i18n、权限状态、typecheck；可见交互变化再补浏览器冒烟 |
| Model、Redis、配置或运行时行为受影响 | 初始化 DDL/DML、被忽略的 DBA SQL 交付、Redis Key/失效、YAML、reload/restart、缓存或补偿步骤 |

同一记录只能合并 method/path、请求响应、安全策略、业务码、消费方和验证方式都相同的接口。任一项不同就拆分记录，避免用一个总括结论掩盖差异。只有某个常见风险面容易被误判为已处理时，才单独写“不适用”，并附 `rg` 结果、调用方搜索、配置或路由注册等具体依据；禁止使用一个汇总的“不适用”覆盖所有未检查项。

## 交付证据

按契约影响记录列出源码依据、触发面、修改文件、验证结果和未覆盖路径，并说明是否需要数据库初始化/DBA SQL、缓存失效、配置 reload、进程重启或补偿。禁止只交付“接口文档已更新”。
