---
name: go-worker-concurrency-guard
description: "审查 Go 后台任务与并发状态。用于 worker、scheduler、Kafka/Asynq、cron、lock、retry、timeout、幂等、归档、清理和告警。"
---

# Go Worker 并发护栏

## 执行流程

1. 确认真实入口：scheduler、worker、consumer、task plugin、queue handler、CLI 或维护路由。
2. 端到端追踪状态转换：claim、lock、execute、retry、success、failure、archive、cleanup、alert 和 compensation。
3. 核对幂等 Key、事务边界、lock owner、TTL、续租、retry/backoff、deadline、cancellation 和 shutdown。
4. 区分暂时重试、预期窗口耗尽与终态失败；只对需要人工介入的状态告警。
5. 为 retry、timeout、concurrency、batch 等外部参数设置服务端硬上限；前端 `max` 不能替代后端校验。
6. 避免无界并发、长事务、超大 batch、无限循环、忽略 context 和关闭时丢失状态。
7. 明确 worker/plugin 的注册、启停、热加载与重启边界。
8. 为锁竞争、owner 不匹配、超时取消、幂等重跑、越界参数、retrying/terminal 和 cleanup 补测试。

## 验证与交付

每条后台链路先建立状态转换表：

入口、输入身份、状态迁移和最终副作用是每条链路的必填事实；锁、续租、重试、DLQ、归档和补偿只在实现存在或故障模型要求时展开。未使用的机制不生成空字段，不能为填表而增加锁、重试或状态。

```text
入口与注册点:
输入身份与幂等 Key:
原状态 -> 事件 -> 目标状态:
事务与外部副作用顺序:
锁 Key/owner/TTL/续租/释放:
超时/取消/关闭:
可重试错误/最大次数/backoff:
终态失败/告警/DLQ/归档:
重放/清理/补偿:
指标与日志:
```

测试至少覆盖成功、重复投递、锁竞争、owner 不匹配、下游暂时失败、重试耗尽、超时取消、进程关闭和重放；修改共享状态、goroutine、锁或 cancellation 时运行 race。没有故障注入或并发终态证据时，不能只凭 happy path 声称“并发安全”或“幂等”。

交付时按状态转换表说明实际入口、硬上限、最终副作用、告警条件、验证命令与结果，以及是否需要重放、清理或补偿。
