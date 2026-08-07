---
name: go-config-runtime-guard
description: "审查 Go 配置契约与运行时边界。用于 config struct、YAML/env、默认值、normalize/getter、hot reload、feature flag、配置不生效和重启判断。"
---

# Go 配置运行时护栏

## 执行流程

1. 追踪完整来源：结构体 tag、默认值、normalize/getter、YAML 样例、env override、外置运行时文件、文档和 reload 应用器。
2. 区分运行期参数与启动期注册。源码没有应用器时，不声称 hot reload 能重建路由、listener、DB/Redis/Kafka client、任务插件或 worker 拓扑。
3. 解释 `0`、空字符串和 `false` 前，先走真实 normalize/getter 逻辑；不要凭字段名推断默认值。
4. 除非需求明确改变外部契约，保持 YAML、env 和 API 配置结构稳定。
5. feature flag 同时追踪执行行为、管理/API 入口、前端展示和可观测状态；关闭 worker 不等于路由消失。
6. 同步配置样例、字段文档、API 响应、前端展示和运维重启说明。
7. 为默认值、零值安全、reload merge、`restartRequired/restartReason` 和回退行为补测试。

## YAML 行级注释契约

- 适用对象：项目代码直接读取的 `etc/config*.sample.yaml`、外置运行期配置样例和项目自有部署配置模板。新增或修改固定字段、默认值、校验、来源优先级或生效方式时，必须同步更新对应注释。
- 位置要求：每个固定字段正上方必须有与字段相同缩进的中文 `#` 注释；父 mapping、父 list、章节标题和行尾注释不能替代子字段的行级说明。
- 内容要求：每个字段先写消费组件或业务用途。源码定义了类型/数据形状、枚举、范围、单位、缺省/空值/零值、normalize/getter 回退、reload 应用器、restart 边界、敏感信息来源/脱敏/轮换或跨字段依赖/互斥/顺序时，注释必须覆盖实际存在的行为；未从源码确认的内容不得写入。
- 容器要求：mapping/list 父字段说明 item 形状、唯一键、顺序、空集合和覆盖/合并规则。固定 item 字段逐项注释；动态 map 的重复业务 key、地址映射或同构列表项只有在父字段已完整定义 key/value 和边界时才可共享说明。
- 安全要求：密码、token、私钥、密钥和完整 DSN 只说明注入渠道、是否允许为空、最小约束、轮换与 restart/reload 边界；样例使用明确占位值，注释和交付证据不得泄露真实值。
- 排除要求：第三方 schema、纯数据清单和生成文件只有在记录具体路径、schema 所有者、文件职责以及项目代码不把其字段作为应用配置读取的证据后才能排除；其中项目自定义的环境变量或嵌套应用配置仍须说明。

## 验证与交付

按受影响层选择并记录测试，禁止只写“配置已同步”：

- struct/tag/default/normalize/getter 变化：覆盖字段缺失、显式零值、合法边界、越界和旧配置读取。
- runtime file、database release 或 reload 变化：覆盖 merge、删除/回退、应用器成功与失败、并发读取和可观测的 active version。
- bootstrap、listener、DB/Redis/Kafka client、路由、插件或 worker 拓扑变化：覆盖启动校验和关闭路径，并明确发布后必须 restart；没有运行期应用器时不得写 reload。
- API 或前端展示变化：同步字段、脱敏、`restartRequired/restartReason`、示例与类型，并运行对应后端契约测试和前端 typecheck。
- YAML 注释变化：逐字段核对注释与 config struct/tag、默认值、normalize/getter、校验器、消费方和 reload 应用器一致；运行 YAML 解析、配置样例加载测试和行级注释审计，确认纳入范围的固定字段没有缺失，排除项均有依据。
- 最后运行 `git diff --check`，核对所有样例 YAML/env、外置运行时文件和文档中的配置键及注释没有漂移。

交付时逐字段列出来源优先级、缺省/零值、normalize 后值、生效组件、reload 或 restart、注释位置、修改文件和验证结果；另列 YAML 纳入文件与逐文件排除依据。未测试的部署路径明确标记，不得用“配置可热加载”“重启即可”或“字段已有说明”概括。
