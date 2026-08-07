---
name: go-test-failure-debugger
description: "复现并定位 Go 验证失败。用于 go test、全量测试、go vet、race、编译、CI、flaky、mock、fixture 和环境依赖问题。"
---

# Go 测试失败排查

## 执行流程

1. 原样复现失败命令，记录第一处真实错误，不只阅读最终 summary。
2. 分类失败：编译、vet、单测逻辑、集成依赖、race、timeout、flaky 顺序、缓存、fixture 或环境配置。
3. 区分本轮引入的问题与仓库已有健康噪声，不能用无关失败掩盖新回归。
4. 使用 package path、`-run`、`-count=1`、compile-only `-run '^$'` 和定向重跑逐步缩小范围。
5. 从生产源码、测试、fixture、生成数据或配置修复根因；除非用户明确要求且理由成立，不跳过、删除或削弱测试。
6. 测试断言业务行为和稳定契约，避免只绑定私有实现细节的脆弱 mock。
7. 先重跑最小失败命令，再运行改动文件归属包；公共类型、helper、配置、Model 或注册清单变化时，用 `rg`/`go list` 找出并运行全部直接调用包，最后执行 `go test ./...`。goroutine、锁、共享 map/cache 或取消逻辑变化时，对这些归属包和调用包补 `-race -count=1`。

## 常用命令

```bash
go list ./...
go test ./internal/handler -run TestName -count=1
go test ./internal/handler/...
go test ./...
go test -race -count=1 ./internal/handler/...
go vet ./internal/handler/...
git diff --check
```

示例使用两个仓库都存在的 `internal/handler`；实际执行前必须把它替换为失败输出或改动文件所属的真实 package path，不能把示例包当作固定范围。

## 交付证据

交付记录必须区分“现象消失”和“根因已修复”：

- 原始失败命令、执行目录、环境/seed/次数和第一处真实错误。
- 最小复现命令及其稳定失败次数；flaky 问题记录并发、顺序、时钟、随机种子或共享状态证据。
- 根因所在生产代码、测试、fixture、生成数据或配置，以及为何会产生原始错误；只延长 timeout、增加 retry 或跳过测试不算根因修复。
- 修复后先运行同一最小命令并证明通过，再运行直接调用包；共享状态、公共 helper 或构建配置变化时扩大到全量测试，goroutine/锁变化时补 race。
- 最后一次修改后的命令、结果和仍存在的无关失败；无法运行全量时说明缺少的依赖、未覆盖范围和 CI/上线前补跑命令。

禁止只写“测试已通过”而省略原始失败、复现条件、修复依据和扩大验证。
