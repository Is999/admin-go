# Go 项目地图

只在仓库结构或验证入口不明确时读取本文件。

## 发现仓库规则

```bash
git rev-parse --show-toplevel
rg --files -uu | rg '(^|/)AGENTS\.md$|AI开发规范\.md$|AI开发提示词\.md$'
```

从仓库根目录到目标文件逐级读取 `AGENTS.md`。项目 AI 文档补充本地风格，但不能覆盖生效的上级规则。

## 发现后端面

- 外部入口：route、handler、CLI、scheduler、worker、consumer、plugin、migration。
- 业务路径：logic、service、jobs、task。
- 数据路径：model、repository、DAO、SQL/Lua 资产、Redis helper。
- 运行时接线：bootstrap、config、svc、middleware、infra、cmd。
- 契约：types、业务码、i18n、permission、RouteMeta、security policy 和接口文档。
- 验证：改动文件同包 `*_test.go`、通过导入或注册使用该契约的直接调用包、Makefile/CI 目标、静态脚本和提交 hook。

## 验证选择

- 优先运行仓库现有 Makefile 或 CI 入口。
- Go 后端先运行改动文件归属包；共享类型、helper、RouteSpec、配置、Model 或注册清单变化时，用 `rg`/`go list` 找出直接调用包并全部运行，随后执行 `go test ./...`。外部依赖阻塞全量命令时记录第一处错误、未覆盖包和上线前补跑命令。
- 并发状态补 race；共享契约补静态检查或跨仓库验证。
- 每个触达仓库运行 `git diff --check` 和 `git status --short`。
