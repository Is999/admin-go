# ShardingSphere-Proxy 生产接入

> 本目录是从 `main` 独立维护的候选方案，不是当前主方案。只有全部生产准入门禁通过并经过架构、DBA、运维和业务共同评审后，才允许进入生产发布流程。

本目录固定使用 ShardingSphere-Proxy `5.5.3` 和 MySQL Connector/J `8.4.0`。应用只访问逻辑表，物理表数量和位置只由 Proxy 规则管理。

## 生产拓扑

- 至少两个 Proxy 实例，分布在不同宿主机或可用区，由四层负载均衡提供统一地址。
- 使用外部 ZooKeeper 奇数仲裁集群保存 Proxy 集群元数据；不同环境使用不同 `namespace`。
- 目标 MySQL 是新集群，不得把迁移目标指向现有源库或其只读副本。
- 应用流量只访问移除 DistSQL 插件的 `runtime` 镜像；`management` 镜像只在 DBA 私网变更窗口短时启动，不能接入应用负载均衡。
- Proxy 管理账号只用于 DistSQL 和迁移；Admin/API 使用仅映射业务逻辑库的应用账号。
- 应用 DSN 指向负载均衡地址，连接池总量按“所有应用实例 × 每实例连接池 × Proxy 后端放大系数”核算。
- 用户注册和资料修改会同时写身份单表与用户分片，生产模板使用 Atomikos XA 保持跨存储单元原子性；上线前必须做 prepare/commit/rollback 故障注入。

`compose.yaml` 只部署 `runtime` Proxy。生产在每个宿主机部署一份，不要用同一宿主机上的多个容器冒充高可用。镜像应在可信 CI 中分别构建 `runtime` 和 `management` target、发布到私有仓库并固定 digest；生产设置 `SHARDINGSPHERE_PROXY_IMAGE=运行镜像地址@sha256:...` 拉取同一不可变产物，不要在每台生产宿主机分别构建。

ShardingSphere 的 `DATABASE_PERMITTED` 只提供逻辑库级授权，不是 SQL 动词级 RBAC。实测 5.5.3 中 `admin: false` 的应用账号仍可执行规则 DistSQL，并可 `CREATE/DROP DATABASE`。`runtime` 镜像移除了 DistSQL 语法和处理插件，但核心仍接受 `CREATE/DROP DATABASE`；因此生产入口前必须有经过验证的 MySQL 协议 SQL 防火墙，按应用账号拒绝 DDL、DCL、DistSQL 和多语句请求。没有这层控制时，本方案不得上线。

## 构建和配置

生成口令时只使用渲染脚本允许的字符，并通过发布系统密钥注入：

```bash
cd deploy/shardingsphere
export PROXY_NAMESPACE=prod_user_sharding
export ZOOKEEPER_SERVERS=zk1.example:2181,zk2.example:2181,zk3.example:2181
export ZOOKEEPER_DIGEST='proxy_meta:<从密钥系统注入至少16位口令>'
export PROXY_ADMIN_USER=proxy_admin
export PROXY_ADMIN_PASSWORD='<从密钥系统注入至少16位口令>'
export PROXY_APP_USER=app_user
export PROXY_APP_PASSWORD='<从密钥系统注入至少16位口令>'
export PROXY_DATABASE=app_db
export PROXY_KERNEL_EXECUTOR_SIZE=32
export PROXY_FRONTEND_MAX_CONNECTIONS=2000
./render-global.sh
# 仅 CI 构建：分别构建并扫描两个 target
# docker build --pull --target runtime -t <runtime-image> .
# docker build --pull --target management -t <management-image> .
export SHARDINGSPHERE_PROXY_IMAGE='registry.example/shardingsphere-proxy@sha256:<发布摘要>'
export SHARDINGSPHERE_MANAGEMENT_IMAGE='registry.example/shardingsphere-management@sha256:<发布摘要>'
docker compose pull
docker compose up -d --no-build
```

默认命令不会启动管理入口。只有审批通过的变更窗口才能在 DBA 堡垒机执行 `docker compose --profile management up -d --no-build management`，并只通过回环地址或 SSH 隧道连接 3308；规则、迁移和核验完成后立即停止并删除该容器。管理实例使用独立 `proxy_management_logs` 卷，禁止与运行实例共享 Atomikos 恢复日志。

`runtime/global.yaml` 权限为 `0600` 且被 Git 和 Docker build context 忽略。构建基于固定镜像 digest，并用 SHA256 校验 Connector/J；升级版本时必须同时更新版本、摘要、兼容性验证和回滚镜像。`PROXY_KERNEL_EXECUTOR_SIZE` 和 `PROXY_FRONTEND_MAX_CONNECTIONS` 必须按 CPU、内存、后端连接预算和压测结果设置，禁止使用无上限配置。

`proxy_logs` 卷必须由每个 Proxy 实例独占、持久化并纳入容量告警，尤其不能删除 Atomikos 的 `logs/xa_tx*.log` 和锁文件，否则会破坏未决 XA 事务的崩溃恢复线索。ZooKeeper 必须启用 ACL、网络隔离和独立环境 namespace；`ZOOKEEPER_DIGEST` 是 ShardingSphere 创建和访问元数据节点使用的 `用户名:口令`，必须由 ZooKeeper 侧预先授权且不能与 Proxy 业务账号复用，缺失或格式非法时渲染脚本会失败。负载均衡健康检查使用受限账号执行 `SELECT 1`，不能只依赖容器 TCP 探针。客户端到 Proxy 的链路必须在受信负载均衡层终止 TLS 或部署等价的加密与双向访问控制，禁止把 3307 暴露到公网或不可信网络。

目标 MySQL 存储单元在建表和迁移完成后必须切换为运行账号，只授予业务所需的 `SELECT/INSERT/UPDATE/DELETE`、XA 恢复和经过验证的流水线权限，不授予 `CREATE/DROP/ALTER`。临时 DDL 账号必须在变更窗口后从存储单元配置移除并轮换口令。管理镜像、运行镜像和底层最小权限是三层独立边界，不能互相替代。

## 规则生成

先把目标存储单元模板复制到发布系统，替换占位符并由 Proxy 管理账号执行。真实口令不得落入仓库或发布日志。模板默认使用 `sslMode=VERIFY_IDENTITY` 校验 MySQL 服务端证书，并在注册目标节点时执行 `SELECT/XA/PIPELINE` 权限自检；失败必须补齐证书信任或最小权限后重试，不能通过关闭校验绕过。

生成 `user=2`、`user_tag=4`，以及未来大表 `user_log=4`、`balance_change=8` 的规则：

```bash
go run ./cmd/shardingctl \
  -database app_db \
  -storage-units ds_0,ds_1 \
  -user-shards 2 \
  -table-shards user_tag=4,user_log=4,balance_change=8 \
  -key-strategies user=application,user_tag=proxy:id,user_log=proxy:id,balance_change=proxy:id \
  > /tmp/app_db-sharding-rules.sql
```

工具只生成计划，不连接生产。执行前必须人工核对。首个存储单元会被设置为身份目录、系统表和运行期表的默认单表节点；已有单表迁入该节点后，由 DBA 渲染并执行 `sql/load-single-tables.sql.tmpl`，其中固定包含 `LOAD SINGLE TABLE ds_0.*` 和 `SHOW SINGLE TABLES` 核对命令。`user_tag` 必须在 `table-shards` 中显式声明自己的物理分片数，但不要求与 `user` 相同。

ShardingSphere 5.5.3 不能接收 MySQL 客户端随语句发送的独立 SQL 注释。通过 Proxy 执行本目录的 `.sql.tmpl` 时必须使用 `mysql --skip-comments`；否则会在首条注释处返回 `Can not accept SQL type 'TerminalNodeImpl'`。`shardingctl` 生成结果不含 SQL 注释，可直接送入管理入口。

默认不建立 table reference 关系。只有确实存在关联查询、分片数相同并已核对实际节点完全一致时，才使用 `-reference-tables table_a,table_b` 显式加入 `user_reference`；工具会拒绝未定义或分片数不同的 reference 表。

每张分片表都必须在 `key-strategies` 显式选择 `table=application` 或 `table=proxy:<主键列>`，缺失、重复、拼错表名都会失败。`application` 表示应用写入全局唯一 ID，不能依赖各物理表的自增值；`proxy` 会生成 `SNOWFLAKE` 规则。现有 `user` 固定为 `application`，`user_tag` 固定为 `proxy:id`。迁移期 `user_tag` 物理表仍保留与源表一致的 `AUTO_INCREMENT` 结构，但切流后的逻辑表插入由 Proxy 显式生成 ID，不依赖各节点自增序列。执行后必须逐表核对输出末尾的 `SHOW SHARDING KEY GENERATORS` 结果。

存储单元注册、逻辑库、分片规则、引用规则和目标建表都不使用 `IF NOT EXISTS`。它们只能在已确认的全新空 namespace 执行；任何重名或部分成功都必须停止，保留现场证据并销毁该目标代际后从空集群重建，禁止在未知旧状态上直接重跑。

存储单元数量也只能是 `1/2/4/.../1024`，并且至少一张规则表的物理分片数要覆盖全部已声明存储单元。表分片数大于存储单元数时必须能均匀分配；表分片数较小时只使用前 N 个存储单元。`shardingctl` 会拒绝 3 个存储单元造成的 `2/1/1` 落点，以及所有表都覆盖不到尾部存储单元的配置。新增存储单元前必须先在预发执行 `SHOW SHARDING TABLE NODES` 保存真实落点，不能只根据参数推测。

所有规则使用：

```text
logical_bucket = CRC32(decimal user id) % 1024
physical_shard = logical_bucket % table_shard_count
```

`user`、`user_tag` 和其他 UID 表保存相同的固定逻辑桶，但各表可按容量独立设置 `table_shard_count`。相同逻辑桶只保证能够由 UID 算出路由条件，不代表物理分片数相同，也不自动建立 table reference 关系。

## 当前表分类

| 表 | 业务 UID | 处理方式 |
| --- | --- | --- |
| `user` | `id` | 按 `shard_no` 分片，物理分片数独立配置 |
| `user_tag`（源表当前为 `user_tag_0`） | `uid=user.id` | 使用同一 UID 固定桶，物理分片数独立配置 |
| `user_identity_*` | `user_id=user.id` | 身份目录；登录先按身份查目录，再带 `user_shard_no` 查 `user`，不随 user 物理分片 |
| `user_tag_runtime_uid` | `uid=user.id` | 工作流运行期表，当前按 `workflow_id/shard_no/uid` 查询，默认保持单表 |
| `user_tag_event_outbox` | `uid=user.id` | 工作流 outbox，当前还存在按状态和 ID 的跨用户任务，默认保持单表 |
| `admin_log`、`admin_role_rel` | 后台管理员 ID | 不属于业务用户 UID，不纳入本方案 |

把一张新表加入分片规则前必须同时满足：

1. 有 `shard_no INT NOT NULL`，值严格等于 `CRC32(十进制 uid)%1024`；迁移期源表和目标表的列、索引、默认值、字符集及约束必须一致，不能只在目标端增加公式 `CHECK`。源表已有公式 `CHECK` 时目标必须保留；源表只有范围约束时，源表和全部目标物理表都安装同语义保护触发器，配合回填全量校验和 Proxy 写审计守住公式。
2. 所有 INSERT 都写 `shard_no`；所有 UPDATE/DELETE 都带可路由的 `shard_no` 等值或 IN 条件。
3. 高频读取带 `shard_no`，索引以真实过滤前缀设计；无 UID 的查询必须先查目录/二级索引或进入明确限流的离线任务。
4. 主键和唯一键语义已经评估。跨物理表的唯一约束不能依赖单节点 MySQL 自动保证；每张分片表必须通过 `key-strategies` 显式配置 Proxy 全局主键或声明由应用提供全局 ID。
5. 已通过 `PREVIEW SQL`、执行计划、单分片命中、批量上限、超时和压力验证。

规则启用 `DML_SHARDING_CONDITIONS` 且不允许 Hint 绕过，缺少分片条件的更新和删除会失败，避免误广播写。

## 可恢复在线回填

生产回填使用 `cmd/shardbackfill`，不再依赖发布脚本自行解释一条 `UPDATE ... LIMIT` 的影响行数来推进游标。它具备以下固定边界：

- DSN 只从 `SHARD_BACKFILL_DSN` 读取，强制要求校验证书的 TLS，拒绝 `multiStatements`，命令没有关闭 TLS 校验的绕过参数。
- 表名、主键列、UID 列和任务名只接受受限标识符；主键必须是正数、唯一、迁移期间不可更新且单调推进的整数，`range-start` 是不包含边界。
- 启动前从 `information_schema` 确认业务表是 InnoDB、主键游标有单列唯一索引，主键和 UID 是非空整数，`shard_no` 是非空且至少为 `smallint`；插入保护在数据库分配自增值后的 `AFTER INSERT` 阶段拒绝非正主键、非正 UID 和错误桶，每批更新前仍会复核非正 UID。
- `run/verify` 必须显式传入插入和更新保护触发器名。命令在启动和写入终态前校验触发器仍属于目标表、时机正确，并与批准的完整触发器体一致；缺失、重排或被替换时失败关闭。
- checkpoint 表启动时精确校验引擎、排序规则、列顺序和长度、默认值、时间精度、索引及全部强制 CHECK；已有同名表只要存在任何结构漂移就拒绝运行，不能借 `init` 静默兼容。
- 每批先按主键读取并锁定最多 `batch-size` 行，在同一事务内修正公式并推进源库 checkpoint；进程退出、连接断开或批次超时只会回滚当前批。
- 同一数据库同一表由 MySQL `GET_LOCK` 单写，发布机更换后使用相同任务名和参数会从已提交游标恢复。
- `verify` 逐页扫描范围内每一行并持久化独立校验游标；只有状态为 `verified` 才能进入下一阶段。

先从密钥系统注入只允许目标源库的运维账号 DSN，再显式创建一次 checkpoint 表。初始化窗口需要该表的 `CREATE`，完成后立即回收；持续回填账号只保留 checkpoint 表的 `SELECT/INSERT/UPDATE` 和目标业务表的 `SELECT/UPDATE`，不授予跨库或普通业务 DDL 权限。`GET_LOCK` 不需要额外 MySQL 权限：

```bash
export SHARD_BACKFILL_DSN='<运维账号>:<口令>@tcp(<证书匹配的源库主机名>)/<源库>?tls=true&timeout=5s'
go run ./cmd/shardbackfill -action init
```

`tls=true` 使用运行主机的系统 CA 并校验服务端证书；内部 CA 必须先纳入受控运行镜像或主机信任库，不能改成 `skip-verify` 或 `preferred`。

如果历史表还没有 `shard_no`，先审核并逐段执行 `sql/add-shard-no-column.sql.tmpl`。它以 `NOT NULL DEFAULT 0` 保持旧版本读写兼容，默认值只用于回填前过渡；DDL 明确要求 `INSTANT`/`LOCK=NONE`，数据库无法满足时会失败，不能让 MySQL 自动降级为阻塞写入。然后先把所有写入方升级为按固定桶写入的兼容版本；漏掉任一脚本、worker 或旁路写入方都禁止继续。当前 `user` 已由主线代码按固定桶写入，仍必须用回填器确认真实历史数据，不能根据 Model 推断线上数据正确。

兼容版本全量生效后，由 DBA 审核并为每张源表执行 `sql/source-insert-shard-no-guard.sql.tmpl` 和 `sql/source-update-shard-no-guard.sql.tmpl`，封住错误新记录、非正主键/UID、UID/桶号错误变更和主键游标变更。插入保护使用 `AFTER INSERT`，因此能在自增主键已经分配后校验正整数，并在失败时回滚整条写入；更新保护仍使用 `BEFORE UPDATE`，允许旧错桶行只修改无关业务字段，因此不会要求先停止写入。回填本身把桶号改为正确公式时可以正常通过。启用 binlog 且 `log_bin_trust_function_creators=OFF` 时，创建触发器需要具备相应管理权限；不得为了省事永久放宽全局安全配置。触发器获取元数据锁超过 5 秒会失败，排查长事务后重试，不能取消该超时。

以执行时的 `MAX(主键)` 作为固定 `range-end` 并记录到迁移账本。`user` 的 UID 列是 `id`：

```bash
go run ./cmd/shardbackfill \
  -action run -job user_20260722 -table user \
  -primary-key id -uid-column id \
  -insert-trigger user_shard_no_bi -update-trigger user_shard_no_bu \
  -range-start 0 -range-end <已记录的最大ID> \
  -batch-size 1000 -batch-timeout 30s -pause 100ms
```

`user_tag_0`、`user_tag_event_outbox` 等表把 `uid-column` 改为 `uid`，主键和两个触发器名仍按各表真实值指定；每张表使用不同任务名。中断后原命令可直接重跑。表、列、触发器或范围变化时禁止复用任务名。

回填状态为 `backfilled` 后执行全量校验：

```bash
go run ./cmd/shardbackfill \
  -action verify -job user_20260722 -table user \
  -primary-key id -uid-column id \
  -insert-trigger user_shard_no_bi -update-trigger user_shard_no_bu \
  -range-start 0 -range-end <同一最大ID> \
  -batch-size 1000 -batch-timeout 30s -pause 100ms
go run ./cmd/shardbackfill -action status -job user_20260722
```

首次达到 `verified` 后，切入 ShardingSphere 迁移前再用相同参数执行一次 `verify -restart`，确认保护触发器持续生效且没有旁路错误写入。若状态是 `mismatch`，先定位并修复仍在错误写入的调用方，再执行 `run -restart` 和 `verify -restart`；禁止手工改 checkpoint 跳过数据。

`sql/backfill-*.sql.tmpl` 和 `sql/verify-*.sql.tmpl` 只保留给 DBA 排障和小范围人工复核，生产全表回填以 CLI 的数据库 checkpoint 为准。`shard_backfill_checkpoint` 是源库迁移账本，不属于应用业务表，不迁入 Proxy 逻辑库，也不能在观察期结束前删除。

## 初次在线迁移

迁移命令模板位于 `sql/migration-commands.sql.tmpl`。生产按以下阶段执行：

1. 完成上一节所有源表的兼容写入、插入/更新保护触发器和两次全量 `verified`；保存每张表任务参数与 checkpoint 快照。迁移启动后禁止再改源表 DDL 或触发器。
2. 校验源 MySQL 使用 `binlog_format=ROW`、`binlog_row_image=FULL`，迁移账号具备复制和读取权限。迁移源必须使用模板中的 JDBC `URL` 注册，`REGISTER MIGRATION SOURCE STORAGE UNIT` 不支持 `HOST/PORT/DB` 写法；生产 TLS 参数按证书部署调整，禁止为了连通性直接关闭 TLS。
3. 在新目标集群注册存储单元、执行 `shardingctl` 生成的规则，再通过 Proxy 执行版本化空表 DDL；`user` 和 `user_tag` 分别使用 `sql/create-user-target.sql.tmpl`、`sql/create-user-tag-target.sql.tmpl`，非分片表统一落到默认单表节点。执行前分别保存源表和每类目标物理表的 `SHOW CREATE TABLE`，除允许的逻辑/物理表名差异外逐项确认列、索引、默认值、字符集和约束完全一致。ShardingSphere 数据迁移不支持源/目标结构不一致：源表没有公式 `CHECK` 时禁止只给目标新增；只有源表已通过已评审在线 DDL 安全建立同名同义公式约束时，空目标才可同步使用 `sql/enforce-shard-no.sql.tmpl`。MySQL 8.4 新增或启用 CHECK 需要重新校验存量行且不支持 in-place，严禁直接用于在线大表。
4. 保存 `SHOW SHARDING TABLE NODES` 的真实物理节点清单，使用目标 DDL 账号直连每个存储单元，为每个实际分片分别渲染并执行 `sql/target-insert-shard-no-guard.sql.tmpl` 和 `sql/target-update-shard-no-guard.sql.tmpl`。触发器名在同一物理数据库内必须唯一；执行 `sql/verify-target-shard-no-guards.sql.tmpl`，确认每张物理表恰好存在一个 `AFTER INSERT` 和一个 `BEFORE UPDATE`，并完整比对触发器体。任一分片缺失都禁止启动迁移。
5. 对每张表启动全量迁移和 binlog 增量同步。`user_tag_0` 映射到目标逻辑表 `user_tag`；身份目录、Admin 系统表和运行期表必须全部迁入 `ds_0`，不能只搬两张分片表就切主库 DSN。显式排除只作为源库迁移账本的 `shard_backfill_checkpoint`。
6. 持续观察全量完成、增量延迟、错误、源库负载和目标热点；执行 `DATA_MATCH` 检查并保存结果，重新按固定桶公式全量核对目标数据，并验证目标物理触发器拒绝错误主键、UID 和 `shard_no`，Proxy 写审计拒绝缺少分片条件的更新和删除。
7. 切流前让发布网关进入“写请求排队”状态，停止把新变更送入旧库，等待旧连接和事务排空、binlog 追平、最终一致性检查通过。
8. 加载已迁入的单表元数据并核对 `SHOW SINGLE TABLES`；逐个提交全部表的迁移任务，再执行 `REFRESH TABLE METADATA` 并核对分片表节点和单表清单。全部通过后滚动把 Admin/API DSN 切到 Proxy 负载均衡地址，冒烟后释放排队写请求。
9. 观察期内保留源库只读、源/目标保护触发器和迁移账本，不删除源表。

历史 `user_tag_runtime_uid.shard_no` 若曾按裸 UID 取模写入，不能与新固定桶混用。该表是短期工作流状态：先停用户标签调度并确认没有运行中 workflow，再清空 `user_tag_runtime_uid` 和 checkpoint 后由新任务重建；这不暂停用户业务写入。`user_tag_event_outbox` 有未派发事件时不得清空，应按 `id` 分批回填固定桶并在重启派发前校验。当前生产校验已禁止启用尚未实现的用户标签业务计算，但迁移仍要显式记录这一补偿步骤。

### “零停写”的安全边界

ShardingSphere 的全量和 binlog 增量阶段可在源库持续写入时运行；但官方迁移流程明确说明最终流量切换可能需要短暂只读期。开源 Proxy 本身不能对旧库最后一笔写入与新库第一笔写入提供跨集群原子切换。

本方案支持的生产口径是：服务不下线、写请求不丢失、不返回人为维护错误；切换瞬间由网关或消息入口短暂排队写请求。若“零停写”指数据库写入也绝不暂停或排队，则必须额外实现经过业务幂等和冲突验证的双写/outbox 协议，这不是单独引入 ShardingSphere-Proxy 能安全保证的能力，禁止用无保护双写冒充。

切换栅栏设置超时预算。到时仍未追平或检查失败，立即解除排队并继续写旧库，不执行 commit，不切 DSN。

XA 只解决同一逻辑请求在多个目标存储单元的事务原子性，不解决源集群与目标集群双写一致性，也不能替代迁移切流栅栏。压测必须覆盖 Proxy 或 MySQL 节点在 XA prepare 后中断、恢复和启发式事务告警。

## 生产准入硬门禁

以下条件缺一项都不能把本候选方案标记为生产可用：

1. 在 Admin/API 之外的发布网关实现持久化、有界、可观测的写请求排队，并验证超时、重放、幂等和解除栅栏；当前两个仓库不包含该能力。
2. 在运行 Proxy 前部署并验证 MySQL 协议 SQL 防火墙，确认应用账号无法执行 `CREATE/DROP DATABASE`、普通 DDL/DCL、DistSQL 和多语句；当前仓库未选定或交付该生产依赖。
3. 在可信 CI 为两个镜像生成 SBOM 和漏洞报告；基础镜像、Connector/J 及传递依赖存在未批准的 Critical/High 漏洞时禁止发布。
4. 用生产量级脱敏数据完成全量迁移、binlog 追平、逐表 `DATA_MATCH`、回滚和反向补偿演练，并保存迁移账本。
5. 部署至少两个 Proxy、外部 ZooKeeper 仲裁集群和负载均衡，验证 TLS、容量上限、指标告警、滚动升级及 Proxy/ZooKeeper/MySQL 单点故障。
6. 验证跨存储单元 XA 的 prepare、commit、rollback、Proxy 重启、MySQL 断连和未决事务恢复，确认 Atomikos 恢复日志持久化且可告警。
7. 使用真实 SQL 语料执行 `PREVIEW SQL`、索引执行计划和并发压测，确认无广播写、无无界扫描，P95/P99 延迟和后端连接数符合容量预算。
8. 用户标签完整业务写入仍未实现时必须保持生产开关关闭；不得因底层路由已具备就启用半成品流程。

## 后续扩容

物理分片扩容不是直接在线执行 `ALTER ... sharding-count`。直接改规则会让已有数据仍留在旧节点而新查询按新规则路由，必然产生漏读。

每次扩容都按新迁移代际处理：

1. 新建目标存储集群和新的 Proxy 逻辑库 namespace。
2. 使用相同 1024 固定桶生成目标规则；`user`、`user_tag` 和其他 UID 表分别按自身容量决定是否从 `N` 升到 `2N`。
3. 从当前物理节点向新目标规则全量搬迁，并持续同步增量。
4. 重复一致性检查和有界写请求排队切流。
5. 观察期后再归档上一代集群。

允许一次从 1 规划到 4，但生产首轮建议按容量和压测结果选择目标，不以“必须逐级”替代真实迁移验证。任何扩容都不得复用当前目标节点原地搬数据。

## 回滚

- `COMMIT MIGRATION` 前：检查失败就 `ROLLBACK MIGRATION`，解除写请求排队，应用继续使用旧库。
- DSN 切换后但尚无新写：立即切回旧库并解除排队。
- 新库已有成功写入：不得直接切回只读旧库。先停止放行新写，完成目标到源的补偿或由业务事件账本重放，再校验后回切。
- Proxy 单节点故障：负载均衡摘除故障节点，ZooKeeper 中规则不变；不要为恢复单节点而修改分片规则。

完整应用路由约束见[库表路由规范](../../docs/site/角色文档/后端开发/库表路由规范.md)。
