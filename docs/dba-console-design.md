# rdsctl DBA Web 查询台(实例数据查询窗口)dba-console-design

> 状态:设计定稿(需求方确认:默认只读 + 独立授权写权限;审计与防泄漏为主要约束)。
> 配套:实施顺序与跨特性依赖见 `docs/dba-features-milestone.md`;与本设计无关的既有
> 行为一律不改。锚点一律用符号名(代码持续演进,不用行号)。

---

## 1. 结论摘要

为 DBA 提供「按实例」的 Web SQL 查询窗口:

- **入口即数据面直达**:在实例详情/列表直接选中实例执行查询,目标节点可选
  `master`(默认)/`read`(在线读从)/`offline`(离线从);结果表格化返回,带行数/耗时/
  截断提示。
- **默认只读,写是独立权限**:语句分类器默认只放行只读语句(`SELECT/WITH/SHOW/
  EXPLAIN/DESCRIBE/DESC`),`instances.query` 权限即可执行;写语句(`INSERT/UPDATE/
  DELETE/REPLACE/…`、DDL)需额外权限 `instances.query.write` 且**强制路由 master**。
- **纵深防泄漏,不靠单一手段**:低权专用账号(非 root)+ 词法分类器 + deny 名单 +
  后端敏感列掩码 + 结果不落库不缓存 + 全量审计。
- **审计完整可溯**:新表 `query_audit` 存完整 SQL(原文)、实例/节点/用户/耗时/行数/结果
  标志;**查询结果行永不入库**;`audit_log` 只落摘要行。

估量(粗,团队校准):后端引擎与护栏 4–5 人日、专用账号与加固 1–2 人日、审计与迁移
1–2 人日、前端 2–3 人日、测试与验收 1–2 人日,合计 **6–9 人日**(不含里程碑总览中列出的
http.rs body 读取前置改造的共享成本)。

---

## 2. 现状与代码锚点

| 事实 | 锚点 | 对本文的影响 |
|---|---|---|
| HTTP 是手写极简实现,POST 参数走 query 串;请求体目前只读首个 4096 字节块 | `http.rs handle()`(`buf = [0u8; 4096]`、`match (method, path)` 分发) | SQL 较长时**必须**增加按 `Content-Length` 分段读 body 的能力(见 §6 前置改造) |
| 每个 API 路由的权限在 `http.rs fn perm_for` 登记;权限目录在 `store.rs::PERMISSIONS`;super 自动展开全量 | `perm_for`、`PERMISSIONS`、`store.rs auth_effective/users_list`、内存后端同款逻辑 | 新增 `instances.query` / `instances.query.write` 须 4 处同步(目录、perm_for、MySQL 展开、内存展开) |
| 容器内执行 SQL 现只有 root 通道与运维查询 | `docker.rs exec_mysql_local`、`instance.rs`(常量 `ROOT_PASS`/`APP_DB`) | 查询台执行不得用 root,需专用账号(§5.4) |
| 审计 `audit_log.params` 为 VARCHAR(255)、写入截 240 | `store.rs DDL`、`MysqlBackend::audit`(`truncate(params,240)`) | 完整 SQL 需独立 TEXT 表(§5.5) |
| 实例视图会把 `root_password` 一并返回 | `instance.rs RdsInstance::to_view`(列表接口 `api.rs instances` 已剔除,详情接口未剔除) | 现状暴露登记为整改项,不随本文档改动;查询路径一律不依赖 root 凭据 |
| 前端单页 + 权限裁剪 + 详情页签 | `rds.html`(导航 `data-view`;实例详情 `data-sub`;权限裁剪块 `hasPerm`) | 新视图与详情页签「查询」需加入裁剪逻辑 |
| 验收用 fake docker/mysql 垫片 | `tests/acceptance.rs Ctx::install_shims` | 垫片需扩展 mysql 分支(返回 TSV 结果/错误/低权账号拒绝) |

---

## 3. 已确认决策

| # | 项 | 结论 |
|---|---|---|
| D1 | 语句范围 | 默认只读;写语句经独立权限 `instances.query.write`,仍全审计 |
| D2 | 执行账号 | 专用低权账号(§5.4),非 root;供给失败**fail-closed 拒绝查询**,不回退 root |
| D3 | 结果泄漏防护 | 结果行不落库/不落审计/不缓存;字节/行数/时长上限;敏感列后端掩码 |
| D4 | 审计 | 完整 SQL 原文入 `query_audit`;`audit_log` 摘要行;保留策略与审计归档对齐(热 30 天) |
| D5 | 目标节点 | master(默认)/read/offline 可选;写语句强制 master;proxy 暂不直接执行(只读经其验证另议,见开放问题) |
| D6 | 依赖 | 零新 crate:复用 `docker exec` 容器内 `mysql` CLI;结果结构化解析见 §5.3 |
| D7 | 前端 | 导航「SQL 查询」页 + 实例详情页签「查询」;掩码在**后端**实施,前端仅渲染 |
| D8 | 治理动作 | 慢查治理 / EXPLAIN 等复用本查询引擎,见 `docs/slow-query-design.md` |

---

## 4. 交互与总体流程

```
DBA(instances.query) ──▶ 详情/导航选实例+节点 ──▶ 输入 SQL ──▶ POST /api/rds/query
后端:
  ① 会话鉴权 + 权限门禁(perm_for)
  ② 分类器:read/write/denied;denied 或(写 && 无 instances.query.write)→ 403/400
  ③ 实例/节点状态与目标校验(running;节点存在)
  ④ 专用账号供给检查(懒供给,幂等)
  ⑤ docker exec <node> mysql(专用账号,批量列名模式)→ 结构化 {columns, rows}
  ⑥ 后端掩码 → 计数/截断 → 响应(no-store)
  ⑦ query_audit 落完整 SQL + 摘要 → audit_log 摘要行(action=query_sql)
```

失败模式:语句被拒 → 400 + 原因;供给失败 → 400 fail-closed;执行失败(mysql 错误)→ 400 + 错误文本(经脱敏,见 §7);超时 → 408 + 「已终止」;超过并发配额 → 429。

---

## 5. 后端设计

### 5.1 权限与门禁

- 新增权限(写入 `store.rs PERMISSIONS`,分组前缀 `instances`):
  - `instances.query` —— 只读语句执行 + 慢查 digest 明细(见 slow-query-design);
  - `instances.query.write` —— 写语句执行(隐含只读可用,由代码显式包含判断)。
- `http.rs perm_for` 登记:`POST /api/rds/query` 先按分类结果决定所需权限——
  perm_for 无法读 body,故路由级登记为 `instances.query`,分类后如属写语句再在
  `api` 层二次校验 `auth::has_perm("instances.query.write")`(与现有"路由所需权限 +
  业务内细粒度校验"风格一致)。
- super 角色展开自动包含新权限(目录随 `PERMISSIONS` 迭代)。
- 前端:导航与页签、按钮按 `hasPerm` 裁剪;无 `instances.query` 用户看不到查询入口。

### 5.2 语句分类与护栏(`src/query.rs`)

新模块 `src/query.rs`(引擎层,不直接持 HTTP):

- `enum SqlClass { Read, Write, Denied }` + `fn classify(sql: &str) -> (SqlClass, String 原因)`:
  - 规范化:去首尾空白/注释头(仅 `--`、`#`、`/*…*/` 剥离)后取首词(词法级,文档如实声明其边界);
  - Read:`SELECT/WITH/SHOW/EXPLAIN/DESCRIBE/DESC`;Write:`INSERT/UPDATE/DELETE/REPLACE/ALTER/CREATE/DROP/TRUNCATE/RENAME/GRANT/REVOKE/LOCK/UNLOCK/SET(除白名单会话项)` 等;其余(含未知)→ Denied;
  - 拒绝:多条语句(未配对引号/括号内的 `;` 剔除后仍含 `;`)、`INTO OUTFILE/DUMPFILE`、`LOAD_FILE`、`SLEEP` 不单独拒绝(靠超时)但列入文档说明;
  - deny 名单(表级,可配置):默认拒绝访问 `mysql.*`、`information_schema` 凭据视图(`USER_PRIVILEGES/SCHEMA_PRIVILEGES` 外的敏感项由账号级 grant 兜底)、`performance_schema` 中账号相关;名单 = 内置默认 + 实例 meta `query_block_tables`(逗号分隔,覆盖补充)+ 全局 env `RDSCTL_QUERY_BLOCK_TABLES`。
- 护栏上限(env,默认值):`RDSCTL_QUERY_TIMEOUT_SECS=30`(tokio timeout 后 kill 子进程,并追加一次 `docker exec mysql KILL QUERY` 尽力而为)、`RDSCTL_QUERY_MAX_BYTES=1048576`(stdout 累计超限即截断并置 `truncated=true`)、`RDSCTL_QUERY_MAX_SQL=65536`(SQL 文本上限)、`RDSCTL_QUERY_CONCURRENCY=4`(进程内信号量,超限 429)。
- 掩码规则(`mask_cols`):列名命中默认正则 `/password|passwd|secret|token|credential|salt|private_key/i` 或实例 meta `query_mask_cols` 追加正则 → 该列全部值替换为 `"***"`,响应携带 `masked_cols` 列表。掩码在**后端**统一实施(§5.3 输出前)。

### 5.3 执行与结构化解析

- `docker.rs` 新增助手(签名以实施为准):
  `pub async fn query_table(container, user, pass, sql) -> Result<String, String>`:
  `docker exec <container> mysql --batch --column-names --default-character-set=utf8mb4
  --connect-timeout=5 -u <user> -p<pass> -e <sql>`。
  - 不加 `--raw`:字符串值中的 `\t \n \\` 由客户端转义,输出可安全按行/列解析;
    解析映射:NULL → `null`,转义还原,数值/时间原样字符串(JSON 统一字符串输出,避免类型推断错误)。
  - 说明:二进制/BLOB 内容不做还原承诺,超长单元格截断并标注(见限制 §7)。
- `src/query.rs` 解析器:首行为列名,后续行为数据行(转义还原);空结果集 → `columns=[]/rows=[]`,不报错。
- 目标节点选择:`node=master|read|offline`,默认 `master`;`offline` 存在时对**只读**语句提供快捷「优先离线从」开关(前端 UI);节点不存在/非 running → 400。写语句强制 `master`(参数不合法即拒)。
- 结构:`QueryResult { columns, rows, rows_returned, truncated, elapsed_ms, node, instance, masked_cols }`;`rows_returned` 计解析行数,显示上限 `RDSCTL_QUERY_MAX_ROWS=1000`(仅影响返回,不影响服务端行数,配合字节上限兜底)。

### 5.4 专用账号与纵深防泄漏(加固核心)

- 账号约定(每实例):只读 `rds_ro_<实例名去噪>`、写 `rds_rw_<实例名去噪>`(写账号按需供给)。口令:随机(复用 `store.rs salt_bytes` 风格),**仅存于实例记录内部字段**(`RdsInstance` serde 字段,`#[serde(default)]`,如 `query_secret`),**不进入 `to_view()` 投影、列表、审计、日志**。
- 供给(懒、幂等,首次查询某节点时):`docker exec <node> mysql(root,容器内 socket) -e "CREATE USER IF NOT EXISTS …; ALTER USER … IDENTIFIED BY '<口令>'; REVOKE ALL, GRANT OPTION FROM …; GRANT SELECT ON <业务库>.* TO …"`。
  - 业务库 = 实例 meta `query_dbs`(默认 `appdb`,即常量 `APP_DB`),可逗号追加;
  - 写账号再授 `INSERT/UPDATE/DELETE`;供给动作本身审计 `action=query_provision,params=<user>+<db 列表>`(不含口令)。
- 供给失败 → 查询请求 400 fail-closed(不静默回退 root)。唯一例外:env `RDSCTL_QUERY_ALLOW_ROOT=1`(lab 专用,默认关闭,文档明示风险)。
- 账号级豁免:`SHOW DATABASES`/连接性语句(如 `SELECT 1`)即使无业务库 grant 也应可用,供给时对每个账号 `GRANT SELECT ON <db>.*` 之外不做过度授权。
- 明文口令与 `ROOT_PASS` 常量同级敏感(都在实例内存/持久化 JSON 中)——现状基线已如此,查询路径不新增 root 使用面即达成改进目标;更彻底的凭据托管列为开放问题(§9)。

### 5.5 查询审计

新表(§8 DDL)`query_audit`:

| 列 | 说明 |
|---|---|
| id | 自增 |
| ts | 秒 |
| user / instance / node | 操作人、实例、目标节点 |
| sql TEXT | **完整 SQL 原文**(审计可溯) |
| sql_hash | sha256(SQL)(查重/检索用,复用 `sha256.rs`) |
| read_only | 1/0(是否只读语句) |
| rows_returned / rows_truncated | 返回行数 / 是否截断 |
| elapsed_ms / ok / err_summary | 耗时、成败、错误摘要(≤240,已脱敏) |

- 写库:同步同一条 `INSERT`(与现有 audit 一致的单语句连接风格);`audit_log` 落摘要行(`action=query_sql`,`params=<sql 前 240>`、`result=ok/<err 摘要>`,截断语义不变),保持既有审计页面可搜到。
- **红线:查询结果行永不写入 query_audit / audit_log / evidence / insights / 日志**;仅存计数与截断标志。
- 保留:热 30 天 + 归档(与 `evidence_snapshots` 同策略口径,见 scaling-design §13);清理任务随 §8 保留任务一并实现。
- MemoryBackend 同语义(见 §8)。

---

## 6. http.rs 前置改造(查询台专用,登记为共享成本)

1. 请求体读取:`handle()` 需对 `POST` 且路由为 `/api/rds/query` 时,按 `Content-Length` 循环 `read` 剩余 body(现有 4096 单次读不足);body 与 query 都支持取参(`qparam` 只解析 query 串,body 解析复用 url_decode 分 `&` 拆解)。
2. 路由分发新增 `("POST", "/api/rds/query") => api::query(&query, &body)`,perm_for 登记 `instances.query`(写语句二次校验见 §5.1)。
3. 响应维持 `Cache-Control: no-store`(response() 已有),查询结果接口不新增任何缓存。

---

## 7. 接口契约

### POST /api/rds/query

参数(query + body,body 优先):`instance`(必填)、`sql`(必填,≤RDSCTL_QUERY_MAX_SQL)、`node`(可选,`master|read|offline`,默认 master)。

成功 200:
```json
{ "ok": true, "instance": "x", "node": "master", "columns": ["id","name","email"], "rows": [["1","a","b"]],
  "rows_returned": 1, "truncated": false, "elapsed_ms": 12, "masked_cols": ["email"], "read_only": true }
```

失败/受限:`400 {ok:false,error:<原因>}`(语句被拒/deny 名单/供给失败/节点不可用/执行错误)、`403`(权限不足,由 http.rs 既有门禁产出)、`408`(超时已终止)、`429`(并发配额)。

错误文本处理:`redact_secret`(复用 `instance.rs capture_degrade_evidence` 同款)对错误串去除疑似口令/连接串后再回显;mysql 报错常回带部分语句文本,仅截断 400 内回显。

### GET /api/rds/query/caps

返回当前护栏默认值 + 本实例目标节点清单 + 掩码默认正则,前端只读展示(权限 `instances.view` 即可调用)。

---

## 8. 存储扩展

### 8.1 DDL(进 `store.rs` DDL 常量,幂等建表)

```sql
CREATE TABLE IF NOT EXISTS query_audit (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  ts BIGINT NOT NULL,
  user VARCHAR(64) NOT NULL,
  instance VARCHAR(96) NOT NULL,
  node VARCHAR(64) NOT NULL DEFAULT 'master',
  sql_text MEDIUMTEXT NOT NULL,
  sql_hash CHAR(64) NOT NULL DEFAULT '',
  read_only INT NOT NULL DEFAULT 1,
  rows_returned INT NOT NULL DEFAULT 0,
  rows_truncated INT NOT NULL DEFAULT 0,
  elapsed_ms INT NOT NULL DEFAULT 0,
  ok INT NOT NULL DEFAULT 1,
  err_summary VARCHAR(240) NOT NULL DEFAULT '',
  KEY idx_qa_ts (ts),
  KEY idx_qa_inst (instance, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 8.2 Store 扩展(三处同步)

- `StoreBackend` trait 新增:
  - `query_audit_insert(&self, user, instance, node, sql, sql_hash, read_only, rows_returned, rows_truncated, elapsed_ms, ok, err_summary)`;
  - `query_audit_list(&self, limit, instance?, user?, since?) -> Vec<Value>`(审计页/报告扩展用);
- `MysqlBackend` / `MemoryBackend` 各自实现(内存版与现有 `MemAudit` 同风格的 Vec 存储与视图)。
- 保留清理:随 slow-query 保留任务同 ticker 执行(`DELETE … WHERE ts < now-30d`),或独立 `RDSCTL_AUDIT_RETENTION_DAYS`。

---

## 9. 前端设计

1. **导航「SQL 查询」**(`data-view="query"`,仅 `instances.query` 显示):实例下拉(仅 running,带 region/az 徽标)+ 节点选择(`master/read/offline`,offline 存在时只读可勾选"优先离线从")+ SQL 编辑区 + 「执行/停止」+ 只读/写权限徽标;结果表格 + 状态行(耗时/行数/截断/已掩码列数)。复用既有卡片/表格样式体系与 `$$` 工具。
2. **实例详情页签「查询」**(`data-sub="query"`,与「监控」并列):自动带当前实例,入口更贴近 DBA 场景。
3. **渲染约束**:掩码列值已由后端替换,前端仅按 `masked_cols` 加列头标注(不二次过滤);结果超出渲染阈值(`RDSCTL_QUERY_MAX_ROWS` 内)直接全量表格 + 纵向虚拟化可选;无语法高亮库依赖(纯 textarea)。
4. 权限裁剪:导航项、页签、写开关按 `hasPerm("instances.query")` / `hasPerm("instances.query.write")` 控制。

---

## 10. 边界、限制与失败模式(文档如实声明)

- 分类器是**词法级**而非解析器级:构造绕过理论存在(文档记录,不承诺攻防完备);真正的数据库级强制由 §5.4 低权账号 + GRANT 兜底,这也是该设计存在的原因。
- 结果还原仅承诺文本安全场景;BLOB/二进制不保证完整(单元格截断)。
- 查询本身是数据面操作:不改变实例状态机、不占实例操作锁;长时间大查询由超时与并发配额约束,但不做慢查隔离(重度只读请选 offline 节点)。
- `root_password` 在详情视图暴露为**既有问题**,本里程碑不改(避免破坏兼容),单独登记整改项(§9 开放问题 + dba-features-milestone 风险表)。
- 查询可能慢且 SQL 由 DBA 提交:本身即慢查治理输入,见 slow-query-design 的采集与治理闭环。

---

## 11. 测试与验收

单元(内存后端 + 假执行):
1. 分类器:read/write/denied/多语句/注释混淆/deny 表/大小写上界。
2. TSV 解析:转义、NULL、空集、超长截断。
3. 掩码默认正则与 meta 追加正则。
4. Store:query_audit 读写、截断标志、audit_log 摘要行共存。

协议级/acceptance(扩展 `tests/acceptance.rs` 垫片 mysql 分支:返回 TSV 行、`SELECT 1` 之外的错误、低权账号拒绝):
5. 只读账号执行 `SELECT` 成功、`UPDATE`(无 write 权限)403 / (有 write 权限但非 master)400。
6. 写账号写 master 成功 + 审计含完整 SQL;结果不入 audit_log(result 只有摘要)。
7. 超时(kill 假执行)与字节截断标志。
8. deny 名单与掩码列生效;`GET /api/rds/query/caps` 可用。
9. 无 `instances.query` 用户:接口 403、前端无入口。
10. 回归:既有 P0 验收三项 + `cargo test` 全绿;`RDSCTL_QUERY_ALLOW_ROOT` 未设时零行为变化。

---

## 12. 依赖与同步点

- 上游:http.rs body 读取(§6)、Store 扩展(§8)、权限目录四同步(§5.1)。
- 下游:slow-query-design 治理入口(EXPLAIN)复用本引擎与 `instances.query` 权限;
- UI 权限树/导航裁剪逻辑与 S-里程碑既有块同源修改。

---

## 13. 开放问题

1. **实例/租户级可见域**:当前 RBAC 权限是全局的,`instances.query` 即全实例可查——租户/实例级授权依赖 M0 tenant 占位后的权限演进,须在慢查文本可见性上同口径(见 slow-query-design)。
2. 专用账号口令以明文存实例 JSON(与 ROOT_PASS 同级):凭据托管(加密/独立 secret 存储)是否独立立项。
3. 写权限的运维门槛:是否要求写语句先走「审批」队列(当前设计=权限即放行,审计兜底)。
4. proxy 直查(经代理读)是否开放——当前设计不支持,避免放大代理故障面。
5. 分类器加固:是否引入 SQL 解析 crate(违反零依赖策略,作为远期选项登记)。

---

## 实施偏差登记(编码落地后,2026-09-03)

| 设计原稿 | 落地现状(锚点: src/query.rs / src/docker.rs / src/store.rs) |
|---|---|
| deny 名单/掩码列/业务库支持「实例 meta」级配置 | v1 仅**全局 env**(`RDSCTL_QUERY_BLOCK_TABLES / RDSCTL_QUERY_MASK_COLS / RDSCTL_QUERY_DBS`);实例 meta 任意键无存储,实例级后置 |
| 写集合含 DDL(经 write 权限放行) | 收紧为**写=仅 DML**(INSERT/UPDATE/DELETE/REPLACE),DDL/管理类首词一律显式拒绝(专用写账号仅授 DML,DB 层也不可能执行 DDL) |
| 分类器 DML 之外保留 SET/事务等 | SET/START/STOP/CALL/FLUSH 等归 DDL/管理拒绝集合 |
| UI:导航页 + 实例详情页签「查询」 | v1 仅导航「SQL 查询」页;详情页签后置 |
| `query_audit` 列名(sql) | 落库列 `sql_text`(视图仍输出 `sql`) |
| 超时 kill docker exec 子进程 | 容器内 `timeout -s KILL` + tokio 外层兜底(docker.rs `query_table`) |
| 实例 meta 级配置 | 全部以 env 默认+可覆盖(见里程碑 env 表) |
