# 结构化交付模板

非平凡交付按实际情况使用以下结构，不需要为不适用项制造内容。

```text
完成:
- 需求项:
- 真实入口/调用链:
- 修改文件:
- 同步面:
- 明确非目标:

验证:
- 候选快照: worktree | index
- 命令与结果:
- 跳过检查及原因:

状态:
- 仓库与分支:
- staged/unstaged/untracked:
- 保留未动的无关改动:

风险与后续:
- 剩余风险:
- 观察指标:
- 发布或人工步骤:

数据与运行时:
- database initialization baseline:
- DBA/Ops local SQL handoff (ignored, not committed):
- SQL checksum / target / order / verification / recovery:
- backfill/compensation:
- cache invalidation/rebuild:
- reload/restart:
- rollback/compensation:
```

最终说明只保留修改范围、最后一次验证命令与结果、未验证/跳过/阻塞项、数据库/缓存/重启/回滚动作和责任人；禁止用笼统“已完成”掩盖部分实现、旧验证或未接通的生产入口。
