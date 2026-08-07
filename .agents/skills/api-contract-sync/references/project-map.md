# API 契约项目地图

## 发现入口

从当前 Git 仓库发现契约，不依赖其它 checkout 或本机绝对路径：

```bash
rg --files | rg '接口文档统一规范\.md$|docs/site/接口文档|route_security_manifest'
rg -n 'RouteSpec|RouteMeta|permission|Permission|MFA|signature|encrypt|cipher' --glob '*.go'
rg -n 'type .*Req|type .*Resp|type .*Item|Validate\(' internal common --glob '*.go'
```

存在统一接口文档规范时必须遵循；否则以同一业务域的 `RouteSpec`、handler 实际绑定的 request/response 类型、同目录接口文档和目标文件生效的 `AGENTS.md` 为准，禁止按修改时间或相邻文件名猜测契约。

## 同步面

- 路由：method、path、alias、middleware chain、公开/内网边界和文档路径。
- 后端：request/response、Validate、handler、logic、model、response wrapper。
- 契约：业务码、HTTP status、中文/英文消息、权限、审计、RouteMeta 和安全字段。
- 前端：仅在任务触达前端时同步 API wrapper、TypeScript 类型、i18n、route 和 permission UI。
- 配置与数据：行为变化时同步 YAML 样例、SQL/Lua、迁移和运维说明。

## 验证

- 后端：路由、middleware、handler、request/response 或业务码变化时运行改动文件归属包和路由/契约防漂移测试；共享 RouteSpec、类型或响应 wrapper 变化时，用 `rg`/`go list` 找出直接调用包并全部运行，随后执行 `go test ./...`。外部依赖阻塞全量命令时记录第一处错误和未覆盖包。
- 安全契约：route manifest、文档和共享测试向量防漂移检查。
- 前端：读取目标仓库规则后运行真实 typecheck；不要猜 package 命令。
- 必跑：`git diff --check` 和 `git status --short`。
