# rdsctl 备份节点注册联动 backup-link-design

> 状态:设计定稿(需求方确认:外部备份平台存在 HTTP API,做注册/注销/心跳/产物通知联动)。
> 锚点用符号名。配套:`docs/dba-features-milestone.md`。

---

## 1. 结论摘要

rdsctl 已具备实例生命周期与样例备份模块(主库 mysqldump),但**数据面节点与外部备份
平台之间没有注册联动**:外部备份平台不知道实例/备份节点存在、不知道备份产物结果,
无法编排/调度备份。本文设计一条**事件驱动的对外联动通道**:

- **形态**:本地 outbox(发件箱)持久化事件 + 后台投递 worker,经 `curl` CLI 调外部平台
  HTTP API(零新 crate,与 store 调 mysql CLI、docker.rs 调 docker CLI 同一离线策略)。
- **事件**:`register_instance / register_node / deregister_node / deregister_instance /
  heartbeat / backup_result`,幂等键保证重复投递不产生副作用;失败重试退避,不阻断任何
  生命周期任务。
- **联动点**:create/scaleout/destroy 生命周期**终态后**、备份任务终态(`watch_backup_task`)、
  实例启停(`set_instance_enabled`)。
- **本地对账**:`backup_outbox`(投递)+ 每节点派生注册状态;随巡检同周期做补偿对账,
  把"应注册而未确认"的事件补投。
- **安全**:默认关闭(`RDSCTL_BACKUP_REG_ENABLED=1` 才生效);token 只经 env,经 stdin
  传给 curl(config),不进 argv/日志/审计;审计只落事件与结果摘要。

估量(粗,团队校准):outbox + worker + curl 适配器 2–3 人日、生命周期钩子与对账 1–2
人日、store 扩展与迁移 0.5–1 人日、状态展示与测试 1–1.5 人日,合计 **4–6 人日**。

---

## 2. 现状与代码锚点

| 事实 | 锚点 | 影响 |
|---|---|---|
| 实例 = 容器拓扑(主 + 读从 + 离线从[备份/统计/大查询])、`Role::is_offline` 标识离线节点,离线节点每实例唯一 | `instance.rs Role/InstNode/RdsInstance`、`Role::is_offline` | 注册负载按节点枚举,offline 节点标记 `backup_capable=true`(备份源优先) |
| 创建/销毁/扩容 = DAG 任务,提交+终态由 watch 系列处理 | `instance.rs create/destroy/scaleout`、`watch_task`、`watch_backup_task` | 注册/注销挂 **任务终态后** 的异步钩子(不进 DAG Step,失败不伤任务/实例状态) |
| 备份 = `run_backup`(提交即锁+审计)+ `backup_nodes`(主容器 mysqldump 落盘)+ `watch_backup_task`(终态放锁、失败仅告警) | `instance.rs run_backup / backup_nodes / watch_backup_task` | `backup_result` 事件在 watch_backup_task 终态分支发;失败也发(带原因),平台可感知 |
| 启停 = `set_instance_enabled`(enable/disable 审计) | `instance.rs set_instance_enabled` | disable → 通知“不可调度/暂停”,enable → 恢复 |
| 巡检周期与扫描口径现成 | `instance.rs start_sweeper / sweep_once` | 对账 worker 复用其周期/扫描语义(独立 ticker,`RDSCTL_BACKUP_TICK_SECS`) |
| 后端方法需 MySQL/Memory 双实现、DDL 幂等 | `store.rs StoreBackend / DDL` | 新表与 5 个方法三处同步 |
| 外部命令 spawn + stdin 管道已有先例 | `docker.rs write_file`(spawn + stdin piped) | curl 投递采用同款(stdin 传 config 与 body) |

---

## 3. 已确认决策

| # | 项 | 结论 |
|---|---|---|
| D1 | 触发时机 | 生命周期任务**终态后**异步投递(不占 DAG Step、不占实例锁);注册失败不回滚实例 |
| D2 | 传输 | `curl` CLI spawn;默认关闭;URL/token 全经 env;token 经 `--config -`(stdin)传递,不进 argv |
| D3 | 可靠性 | outbox 持久化 + 指数退避重试(封顶)+ 幂等键;4xx=永久失败进 dead(人工/审计可见),5xx/超时=重试 |
| D4 | 幂等键 | `{event}-{instance}-{node|master}-{内容摘要短哈希}`;`backup_result` 附 task_id 尾缀 |
| D5 | 对账 | 独立 worker 周期补投"应注册未确认"与心跳 |
| D6 | 联动范围 | 实例全节点注册;offline 节点标记 `backup_capable=true`;节点宿主信息(host_port/region/az)随负载 |
| D7 | 安全 | token/URL 凭据脱敏于日志与审计;默认关闭零影响;平台不可达绝不影响实例状态 |
| D8 | 产物 | `backup_result` 在备份任务终态发:成功带容器内产物路径与字节数;失败带原因摘要 |

---

## 4. 事件契约(与外部平台的稳定面)

统一 JSON 负载(字段名以小驼峰,均为字符串/数值/布尔;实现与真实平台字段映射联调时
仅需改动适配层,见 §6.2):

| 事件 | 触发 | payload 要点 |
|---|---|---|
| `register_instance` | create 任务 success | `{instance, region, az, tenant, status, nodes:[{container,role,host_port,server_id,region,az,backup_capable}], created_at}` |
| `register_node` | scaleout 任务 success(新节点) | `{instance, node:{container,role,host_port,server_id,region,az,backup_capable}}` |
| `deregister_node` | destroy 任务 success(逐节点注销)或离线节点单独移除(暂无单节点销毁,预留) | `{instance, container}` |
| `deregister_instance` | destroy 任务 success(整实例) | `{instance}` |
| `heartbeat` | 对账周期 | `{instance, status, enabled, nodes:[{container,role,backup_capable,host_port}]}` |
| `backup_result` | 备份任务终态 | `{instance, task_id, ok, artifact_path?, bytes?, started_at?, err_summary?}` |

- `role` 序列化:master/read/offline(历史 Stats/Backup 一律按 offline,与
  `instance.rs scaleout`/`Role::label` 口径一致)。
- `backup_capable`:offline 节点 true(外部平台应优先以离线从为备份源);master 亦 true
  (现备份模块在 master 上执行,平台按自身策略选择)。
- 事件大小 ≤ 32 KB(大实例 nodes 多时截断最旧节点并记录,防平台/网络压力)。

---

## 5. 可靠性设计:outbox + 投递 worker

### 5.1 表 `backup_outbox`

| 列 | 说明 |
|---|---|
| id BIGINT AI PK | |
| event VARCHAR(32) | 事件名 |
| instance VARCHAR(96) / node VARCHAR(96) | 归属(便于按实例查询状态) |
| idempotency_key VARCHAR(128) | 唯一键(去重投递) |
| payload_json MEDIUMTEXT | 事件负载 |
| state | pending / delivering / done / dead |
| attempts INT | 已尝试次数 |
| next_at BIGINT | 退避后最早投递时间(UNIX 秒) |
| last_error VARCHAR(240) | 上次错误摘要(脱敏) |
| created_at / updated_at | |

- 投递去重:`UNIQUE(idempotency_key)`;重复触发(任务重跑终态再钩)以 INSERT IGNORE 幂等。
- 完成态清理:done/dead 保留 7 天后清理(与保留任务同 ticker)。

### 5.2 投递 worker

- 独立后台任务,周期 `RDSCTL_BACKUP_TICK_SECS`(默认 60s);仅在
  `RDSCTL_BACKUP_REG_ENABLED=1` 时启动。
- 每轮:取 `state IN (pending,delivering) AND next_at <= now LIMIT 50` → 逐条投递:
  - 构造 curl(见 §6.1);成功(HTTP 2xx)→ done;4xx → dead(记 last_error,审计一次);
    5xx/网络错误/超时(`RDSCTL_BACKUP_TIMEOUT_SECS` 默认 10)→ 失败计数,退避
    `min(60 * 2^attempts, 600)s` 写 next_at,attempts+1;连续失败 10 次 → dead(防呆账)。
  - 每轮每实例不超过 1 条 heartbeat(§7),避免放大。
- 审计:投递尝试 `action=backup_notify,params=<event>:<instance>:<node>,result=ok|err 摘要`
  (params ≤ 240 截断,经脱敏;绝不含 token/URL 凭据)。

### 5.3 注册状态视图(派生)

按 (instance,node) 聚合 outbox:`last_ok / reg_state`(registered=最近 done 事件为
register 系列;deregistered=最后为 deregister;unknown)。查询接口供前端徽标与对账使用,
不单独建状态表(v1 简单化,数据量小)。

---

## 6. 适配层(可替换的"唯一对接面")

### 6.1 curl 投递(默认)

- env:
  - `RDSCTL_BACKUP_REG_URL` —— 基础端点(事件追加为 `?event=<name>` 或路径模板
    `RDSCTL_BACKUP_REG_URL_TMPL` 含 `{event}`;v1 固定为 URL + query event);
  - `RDSCTL_BACKUP_REG_TOKEN` —— Bearer token(仅 env);
  - `RDSCTL_BACKUP_TIMEOUT_SECS`、`RDSCTL_BACKUP_TICK_SECS`。
- 命令:`curl --config -` 从 stdin 读配置:URL、`-X POST`、`header = "Authorization: Bearer
  …"`、`header = "Content-Type: application/json"`、`--data-binary @-` 从 stdin 第二段读
  payload(实现用一次 spawn + 两次 stdin 写:先 config 段再 payload 段,或 config 走临时
  命令行参数集合而仅 header 走 stdin 段——细节实施时定,原则:**token 绝不进 argv**)。
- 幂等键在 payload 与 `X-Idempotency-Key` 头各一份(平台任意一种可用)。

### 6.2 自定义脚本适配(可选)

- env `RDSCTL_BACKUP_REG_SCRIPT=/path/script.sh`:优先级高于 URL;脚本 stdin 收
  JSON payload、argv 收 event/idempotency_key,退出码 0=成功、4x/5x 由 exit code 分级
  (4 或 5 开头四位码表意,简化:0 成功、1 可重试、2 永久失败)。未配置时走 curl。

---

## 7. 生命周期钩子(联动点清单)

| 钩子 | 位置(实现时) | 事件 |
|---|---|---|
| create 任务终态 success | `watch_task` 的 success 分支(新增,仅当实例全节点就绪后) | `register_instance` |
| scaleout 任务终态 success | `watch_task` success 分支按 kind=scaleout 识别新增节点 | `register_node` |
| destroy 任务终态 success | `watch_task` 的 destroy 终态分支(容器已清) | `deregister_instance`(节点级注销由对账 worker 在平台支持时补发,首版以整实例注销为准,open question §10) |
| 备份任务终态 | `watch_backup_task`(success/failed 均已放锁处) | `backup_result` |
| 启停切换 | `set_instance_enabled` 成功后 | `heartbeat`(带 enabled=false/true;不加专用 pause 事件,v1 用 heartbeat 表达) |
| 周期性补偿 | 独立对账 worker(周期同备份 ticker) | 对 running 实例中 `reg_state != registered` 的节点补 `register_node`/`register_instance`;running 实例全量 `heartbeat` 上限每分钟 1 条/实例 |

实现约束:
- 所有钩子**提交到 outbox 即返回**(异步投递),不 await curl、不占实例操作锁;
- 钩子幂等:重复触发靠 idempotency_key INSERT IGNORE;
- 钩子在 `RDSCTL_BACKUP_REG_ENABLED=1` 外一律 no-op(零行为变化)。

---

## 8. Store 扩展(三处同步)

trait 新增方法(签名实施时定稿):
- `backup_outbox_enqueue(event, instance, node, idempotency_key, payload_json) -> bool`
  (INSERT IGNORE,false=重复键);
- `backup_outbox_poll(batch, now) -> Vec<Value>`(取可投递项并置 delivering);
- `backup_outbox_mark(id, state, attempts, next_at, last_error)`;
- `backup_reg_state(instance?, node?) -> Vec<Value>`(派生视图,见 §5.3);
- `backup_outbox_prune(before_ts)`。
MySQL 各方法单语句风格;MemoryBackend 对齐;DDL 进 `store.rs` DDL 常量。

---

## 9. 接口与前端(轻量状态展示)

- `GET /api/rds/backup/reg?instance=` → 各节点 `{container, role, backup_capable,
  reg_state, last_ok, last_error}`(权限 `instances.view`);`POST /api/rds/backup/reg/retry?id=` →
  重置 dead→pending 人工补投(权限 `instances.manage`,审计 `action=backup_reg_retry`)。
- 前端:实例详情「概览」连接信息卡旁加**联动状态徽标**(已注册/未注册/失败可重试,仅
  `instances.manage` 可见重试按钮);审计页可过滤 `backup_notify/backup_reg_*`。
- 默认关闭时接口返回空与 disabled 提示,页面徽标不出现。

---

## 10. 边界、风险与开放问题

- **默认关闭**:无任何 env 配置时行为与现版本一致(P0 回归门槛)。
- **凭据**:token 仅 env、stdin 传递、审计日志脱敏;URL 含凭据视为部署错误(文档告警)。
- 平台不可达/宕机:事件堆积于 outbox(有界规模限制,单实例事件上限 200,超出丢弃最旧并
  审计 `backup_outbox_overflow`),实例状态与备份功能不受影响。
- **开放问题**:
  1. 真实备份平台的鉴权/字段/端点形态(本文按 Bearer + query event + payload 约定,联调时
     只改 §6 适配层);
  2. 节点级注销 vs 整实例注销的首版取舍(上文 v1=整实例注销,deregister_node 事件预留);
  3. 是否要求平台回调确认(webhook 回执)而非"2xx 即确认"——v1 采用后者,回执需平台先支持;
  4. 平台是否需要**产物级元数据**(备份文件在宿主路径/对象存储 URL)——当前产物在容器内
     路径,仅登记字节数,见 `backup_nodes` 先例。

---

## 11. 测试与验收

单元(内存后端):
1. outbox:入队幂等(同 key 去重)、poll 只取到期项、mark 状态迁移、退避计算、dead 阈值。
2. 钩子幂等:重复终态通知只产生一条。
3. reg_state 派生(registered/deregistered/unknown)。

协议级(扩展 acceptance:fake `curl` 脚本垫片——记录 argv/入参、可注入 2xx/4xx/5xx/超时;
或 `RDSCTL_BACKUP_REG_SCRIPT` 垫片更易断言):
4. 启用时:create → register_instance;scaleout → register_node;destroy →
   deregister_instance;备份任务终态 → backup_result(success/failed 各一);
   启停 → heartbeat。
5. 5xx 重试退避、4xx dead 且可经 retry 补投;平台宕机期任务不受阻(终态正常、审计可见)。
6. 未启用(默认):零 outbox 写入、零 curl 调用(垫片断言)、接口空。
7. 审计不含 token;`cargo test` 全绿 + P0 回归。

---

## 12. 与其它设计的同步

- 备份执行仍走既有 `run_backup`(master mysqldump);本设计只加"通知面",不改备份执行路径。
- offline 节点语义与 `Role::is_offline` 单节点约束不变;backup_capable 只是注册负载标记。
- 对账/保留 ticker 与 slow-query、query_audit 保留任务共享调度编排(见
  dba-features-milestone 阶段收尾),互不干扰。

---

## 实施偏差登记(编码落地后,2026-09-03)

| 设计原稿 | 落地现状(锚点: src/backuplink.rs / src/instance.rs / src/store.rs) |
|---|---|
| 周期心跳(每实例 1 条/分钟) | v1 仅在**启停切换**发 heartbeat + 对账周期确保「已注册」;常规周期心跳未发(登记,需平台确认语义后加) |
| scaleout → register_node | 按非主节点(container 幂等键)发送 register_node;创建 → register_instance |
| destroy → 逐节点 deregister_node | v1 仅整实例 `deregister_instance`(register_node 幂等键随实例销毁自然失效);deregister_node 事件预留 |
| 平台回调确认(2xx vs 回执) | 2xx 即 done(与 §10 开放问题 3 一致,回执待平台支持) |
| curl token 经 stdin config | 落地为 **`-H @临时文件`**(权限 600 隐含风险文档化:temp 目录文件短暂存在),token 不进 argv;脚本适配优先生效 |
