---
name: go-security-route-guard
description: "审查 Go 路由安全与敏感契约。用于 auth、JWT/session、MFA、权限、RouteMeta、签名、加密、公开/白名单/内网路由、业务鉴权码和前端安全联动。"
---

# Go 路由安全护栏

## 执行流程

1. 从路由注册追到 middleware、handler、logic、response wrapper、配置、文档和前端调用方。
2. 分类每条路由：登录态、公开、内网、白名单 token、MFA、签名、加密或组合模式。
3. 同步核对 RouteMeta、权限码、审计动作、安全策略、业务码、i18n、接口文档和前端策略。
4. JWT/session 变化时复核 issuer、`jti`、稳定会话 ID、`auth_version`、用户状态、AppID/tenant、登录/刷新/退出生命周期、会话上限和重放边界。
5. Redis 会话 Hash、ZSET、version Key 和 Lua 原子操作保持同槽；覆盖同 token 单次刷新、refresh/logout 竞态、旧版本覆盖和多实例行为。
6. 登录失败对外保持不可枚举的 code/status/message 与成本；内部审计保留真实原因。匿名高成本入口同时评估可信客户端 IP、账号/IP 限流、原子计数、TTL、429 契约和 Key 基数。
7. token、ticket、nonce、验证码、重置 key、密钥和锁 owner 使用 CSPRNG，禁止时间种子或非安全随机 helper。
8. 签名与加密只处理明确字段，不默认处理完整 body、大对象、数组或分页列表；响应字段满足 `ResponseCipher ⊆ ResponseSign`，服务端先签后加密、客户端先解密后验签。
9. 响应安全中间件仅在需要改写时缓冲；下载、Range、SSE 和大响应保留流式输出与必要的 `Flusher`/`Hijacker`/`Pusher` 能力。
10. 错误消息不暴露 SQL、密钥、内部表名、真实下游错误或 key material。

## 验证与交付

- 运行 handler、middleware、security 聚焦测试；补公开路由、失败业务码、字段大小、响应签名子集、refresh/logout 并发终态和流式响应回归。
- 契约变化时使用 `$api-contract-sync`。
- 交付时说明路由分类、安全字段、权限/业务码/文档同步、测试结果和配置或前端后续。

每条新增或改变安全语义的路由必须记录：

接口标识、注册位置和访问分类是必填事实；JWT/session、权限/MFA、签名/加密、重放和流式响应只在该路由实际启用或本次改动触及时展开。未启用的机制不填空、不臆造策略；只有容易被误判为已覆盖的安全面才写不适用及路由注册、policy 或调用方依据。

```text
接口标识（method/path/route alias）:
注册 server 与 middleware:
访问分类（公开/登录/内网/白名单）:
JWT/session/AppID/tenant 校验:
权限/审计/MFA:
请求签名/加密字段及大小:
响应签名/加密字段及大小:
nonce/时间戳/重放边界:
失败业务码:
流式/Range/大响应行为:
前端与文档:
验证命令与结果:
```

通过标准不仅是合法请求成功：未登录、无权限、错误/过期 token、错误签名、错误密钥版本、重放、字段缺失/超限、refresh/logout 竞态和流式响应按路由适用面得到确定且不泄密的结果。未覆盖的安全分支必须明确标记，不能写“安全校验正常”。
