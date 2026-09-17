# 新增 DAG 模块操作手册(How-to)

> 适用:想给 RDS 管控新增一个**功能模块**(真实执行动作的编排单元)时的完整流程。
> 样板:已实现的最小功能模块 `backup`(主库 mysqldump 逻辑备份),本手册全程以其为对照。
> 若只想要"编排示意 / 展示先行"(不执行真实动作),见文末 §6 Phase B Noop 草稿路径,无需写本手册的代码。

---

## 0. 概念分层(先理解再动手)

新增模块要贯穿下面每一层,漏一层就会"后端能跑、页面认不出 / 目录看不到 / 状态不对"。

```
┌─ L0 原子步骤库   dag.rs::Step + docker.rs 执行原语
│                 (最小可序列化动作;示例新增了 Step::DockerExec)
├─ L1 节点组合子   instance.rs 里返回 Vec<TaskNode> 的纯函数
│                 (create_nodes2 / destroy_nodes / scaleout_nodes / backup_nodes)
├─ L2 管理入口     RdsManager 方法(状态门槛→操作锁→审计→submit→终态联动)
├─ L3 提交与持久化 TaskScheduler::submit("kind", ...) → execute/resume
├─ L4 模块分类     dag.rs::classify_node_module(id,name,defs) → 模块标签
├─ L5 前端映射     rds.html: MODULE_META / MODULE_ORDER / nodeModule / CSS .m-*
└─ L6 目录与入口   instance.rs::nodegroups()(编排页目录)/ api + http 路由 + 权限
```

**一个"功能模块" = L0..L6 各同步一点**;只做展示的模块可停在 L4/L5 + Noop 草稿。

---

## 1. 约定与硬性约束(写任何代码前)

| 约束 | 原因 | 违反后果 |
|---|---|---|
| `Step` 必须是 `Serialize + Deserialize`(派生即可,字段只能可序列化类型) | 步骤随任务/节点落库,重启后要能还原 | 反序列化失败,任务无法恢复/展示 |
| 步骤必须**幂等**(重跑结果一致、无重复副作用) | 引擎支持节点重试;崩溃重启后 `resume_pending` 会重跑未终态节点 | 重复创建容器/重复扣库存/重复写数据 |
| 持久化**字段向后兼容**:新 Step 变体只能**追加**,不可改既有变体字段 | 旧库中的老任务需照常解析 | 老任务 JSON 解析失败 |
| 生命周期类模块结束要回到合法状态机状态 | 状态机:creating→running…;`UpdateInstanceStatus` 负责收尾 | 实例悬在 creating/scaling 等中间态 |
| 非生命周期模块(如 backup)**失败不得改实例状态**,只告警+放锁 | 一次失败的备份不应把运行中的实例标记为 failed | 实例被误伤为 Failed |
| 提交侧走 `assert_state`(状态门槛)+ `lock_instance`/`unlock_instance` + 审计 | 并发互斥、留痕 | 并发操作互相破坏;操作无审计 |
| 路由权限最小化,并在 `http.rs::perm_for` 登记 | 安全模型 | 新端点无鉴权(403 无法触发即"裸奔") |

---

## 2. 六步主流程(以 backup 样板为对照)

### 第 1 步 需要新原子动作 → 扩展 L0(dag.rs + docker.rs + exec_step)

多数模块可用既有 Step 拼装(DockerRun/DockerRm/Network*/ExecSql/WaitMysql/WaitHealthy/WriteHostFile/UpdateInstanceStatus/UpdateInstanceTopology/Verify*/Audit/Noop)。
只有既有原子表达不了的动作才新增——backup 需要"在容器内执行任意命令",于是新增:

`src/dag.rs`(enum Step 内追加变体):

```rust
/// 容器内执行任意命令(docker exec,回显 stdout)——通用运维原子步骤
/// (由样例功能模块「backup」引入;见 docs/dag-module-howto.md)
DockerExec {
    container: String,
    args: Vec<String>,
},
```

`src/docker.rs` 增加执行原语(与已有 `exec_mysql_local` 并列):

```rust
/// 容器内执行任意命令(docker exec,回显 stdout)——Step::DockerExec 的执行后端
pub async fn exec_in(container: &str, args: &[String]) -> Result<String, String> {
    let mut cmd: Vec<&str> = vec!["exec", container];
    cmd.extend(args.iter().map(|s| s.as_str()));
    docker(&cmd).await
}
```

`src/instance.rs::exec_step` 增加执行分支:

```rust
Step::DockerExec { container, args } => {
    // 按路由走平台无关执行面(本机默认后端 / 远端 agent;平台由 agent 侧决定)
    let rt = runtime_of_route(&step_route(&ctx.instance, &container))?;
    let out = rt.exec(&container, &args).await?;
    Ok(if out.trim().is_empty() {
        format!("{container}: docker exec 完成")
    } else {
        format!("{container}: {out}")
    })
}
```

> 注:`Step::DockerRun` / `DockerExec` 等步骤现在经平台无关执行面执行,接入非 docker 平台
> 不必改步骤定义(见 [container-platform-abstraction.md](./container-platform-abstraction.md))。

> 提交点:新增变体不需要改 `dag.rs` 其它地方(持久化靠 serde 派生;`resume_pending`
> 从落库 JSON 反序列化)。若新步骤要在模块分类里参与判定,见第 4 步。

### 第 2 步 写节点组合子(L1,instance.rs)

纯函数输入实例名/目标容器等,输出有序 `TaskNode` 列表。对照既有 `slave_steps`/`destroy_nodes`,
backup 组合子(紧邻 `scaleout_nodes` 之后):

```rust
fn backup_nodes(name: &str, master_c: &str) -> Vec<TaskNode> {
    let n = name.to_string();
    let mc = master_c.to_string();
    let dump = format!("/tmp/rds-{n}-backup.sql");
    let cmd = format!(
        // 口令从容器自身 env 取:命令会随步骤 JSON 落库/下发,内联明文等于把口令发给只读用户
        "set -e; mysqldump -u root -p\"$MYSQL_ROOT_PASSWORD\" --single-transaction --quick --databases {APP_DB} > {dump} 2> /tmp/rds-{n}-dump.err; echo 'backup-ok bytes='$(wc -c < {dump})"
    );
    vec![TaskNode {
        id: "backup".into(),
        name: "执行逻辑备份(mysqldump)".into(),   // ← 名称会被 L4 分类,避开更早匹配的关键词
        deps: vec![],
        retries: 2,
        timeout_secs: Some(600),
        steps: vec![
            Step::DockerExec { container: mc.clone(), args: vec!["sh".into(), "-c".into(), cmd] },
            Step::WriteHostFile {
                path: format!("logs/rds/{n}/backup/README.txt"),
                content: format!("rdsctl 逻辑备份\n实例: {n}\n...\n"),
            },
        ],
    }]
}
```

要点:
- `id` 节点内唯一;`deps` 引用其它节点 id 形成 DAG;
- `retries` / `timeout_secs` 语义 = 失败重试次数(不含首次)与单节点超时;
- **步骤顺序执行**,任一步失败该节点失败并按 retries 重试;
- 命名别踩 L4 已有关键词:名称含"从节点"→slave、"网络"→net、含"主库/主节点"→master……
  backup 节点名刻意不含这些;而**离线从节点名「启动从节点(统计/备份)」含"备份"**——所以 L4
  的 backup 判定必须放在 slave 判定之后(见第 4 步的注释,顺序敏感!)。

### 第 3 步 管理入口(L2,instance.rs RdsManager 方法)

对照 `destroy()`/`scaleout()` 的提交侧模板:

```rust
/// 运行中实例 → 主节点容器内 mysqldump 逻辑备份。
/// 提交侧模板:状态门槛 → 停用检查 → 操作锁 → 审计 → 组合子造节点 → submit → 终态联动。
pub fn run_backup(self: &Arc<Self>, name: &str) -> Result<String, String> {
    let inst = self.assert_state(name, InstStatus::Running)?;   // 状态门槛
    if !inst.enabled {
        return Err(format!("实例 {name} 已停用(管理暂停),请先启用再操作"));
    }
    let mc = inst.master().map(|m| m.container.clone()).unwrap_or_default();
    if mc.is_empty() { return Err(format!("实例 {name} 缺少主节点,无法执行备份")); }
    self.lock_instance(name)?;                                   // 实例级操作锁
    self.store.audit(&crate::auth::current_user(), name, "backup", "submitted", "", "");
    let nodes = backup_nodes(name, &mc);
    let tid = self.scheduler.submit("backup", name, &crate::auth::current_user(), nodes);
    watch_backup_task(self, name, tid.clone());                  // 终态联动(见下)
    Ok(tid)
}
```

**终态联动要选对语义**:
- 生命周期任务(create/destroy/scaleout):`watch_task` —— 终态放锁;失败把实例置 `Failed` + 告警;
- 非生命周期任务(backup):`watch_backup_task(mgr, name, tid)` —— 终态放锁;失败**只告警不改状态**;
  否则运行中的实例会被一次失败备份标记成 Failed。

### 第 4 步 模块分类(L4,dag.rs + rds.html 双向同步)

后端 `dag.rs::classify_node_module`(名称关键词 → 标签),**插在 slave 判定之后**:

```rust
if n.contains("从节点") {
    return "slave".into();
}
// 备份(须在从节点之后:离线从节点名如「启动从节点(统计/备份)」先归从库)
if n.contains("备份") || n.contains("backup") {
    return "backup".into();
}
```

defs 存在时还会按步骤兜底判定;如步骤里含 `mysqldump` 也会归 backup(可在步骤 match 中加):

```rust
Step::DockerExec { container, args } => {
    let c = container.to_lowercase();
    if args.iter().any(|a| a.contains("mysqldump")) { return "backup".into(); }
    ...
}
```

前端 `rds.html` 三处必须同步(标签/顺序/名称关键词):

```js
var MODULE_META = {
  net: "网络", master: "主库", slave: "从库", proxy: "代理", repl: "复制",
  verify: "校验", config: "配置", cleanup: "清理", backup: "备份", shard: "分片", other: "其他"
};
var MODULE_ORDER = ["net", "master", "slave", "proxy", "repl", "verify", "config", "cleanup", "backup", "shard", "other"];
function nodeModule(n) {
  ...
  if (nm.indexOf("从节点") >= 0 || ...) return "slave";
  // 备份(须在从节点之后:离线从节点名如「启动从节点(统计/备份)」先归从库)
  if (nm.indexOf("备份") >= 0 || nm.indexOf("backup") >= 0) return "backup";
  ...
}
```

徽章颜色(紧邻既有 `.dg-mod.*` 规则追加,新色与既有色系区分):

```css
.dg-mod.m-backup { color: #9d174d; background: #fdf2f8; border-color: #fbcfe8; }
```

模块图例 `mchip`、拓扑组合卡、dry-run 预览全部经由 `MODULE_META`/`MODULE_ORDER` 自动生效,无需逐个改。

### 第 5 步 API / 权限 / 目录(L3+L6)

`src/api.rs` 加处理函数:

```rust
/// POST /api/rds/backup?name= —— 运行中实例主库逻辑备份(样例功能模块;instances.manage)
pub fn backup(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name");
    if name.is_empty() {
        return (400, "application/json", json!({ "ok": false, "error": "缺少 name 参数" }).to_string());
    }
    match manager().run_backup(&name) {
        Ok(tid) => (200, "application/json", json!({ "ok": true, "task_id": tid }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}
```

`src/http.rs` 两处登记(漏了任一 → 404 或无鉴权):

```rust
// perm_for() 内:
("POST", "/api/rds/backup") => Some("instances.manage"),   // 复用现有权限;新语义才加新权限并同步 store.rs::PERMISSIONS
// 路由分发处:
("POST", "/api/rds/backup") => api::backup(&query),
```

任务种类中文标签 `rds.html`:

```js
var KIND_CN = { create: "创建实例", destroy: "销毁实例", scaleout: "扩容实例", backup: "逻辑备份", user: "自定义草稿" };
```

任务筛选下拉(可选)加 `<option value="backup">备份</option>`。

目录注册 `instance.rs::nodegroups()` —— 编排页"节点组目录"的数据源:

```rust
serde_json::json!({
    "kind": "backup", "label": "主库逻辑备份",
    "desc": "对运行中实例的主节点执行 mysqldump 逻辑备份(容器内落盘 + 宿主登记产物说明)",
    "params": [{"name": "instance", "label": "目标实例", "values": []}],
    "modules": [{"module": "backup", "count": 1}]
}),
```

### 第 6 步 编排向导(前端,rds.html 4 处)

1. **目标卡片** `#dag-kind` 追加 rcard(`data-dagkind="backup"`,标题+说明),让向导可直接选;
2. `planNodesFor(kind,p)` 增加分支(与后端组合子展示一致,dry-run 预览用):

```js
} else if (kind === "backup") {
  add("backup", "执行逻辑备份(mysqldump)", []);
}
```

3. `dagFallbackGroups()` 增加同款兜底(接口失败时目录仍可见);
4. `dagSubmit()` 增加提交分支 + 权限门槛;`renderDagKindPane()` 让 backup 也加载目标实例下拉
   (`if (dagState.kind === "destroy" || dagState.kind === "backup") loadDagInstances();`):

```js
if (k === "backup") {
  if (!hasPerm("instances.manage")) { err.textContent = "无 instances.manage 权限"; return; }
  jpost("/api/rds/backup?name=" + encodeURIComponent(inst))
    .then(function (d) { toast("备份任务已提交:" + d.task_id, "ok"); setRoute("#/tasks"); tick(true); })
    .catch(function (e) { err.textContent = e.message; });
}
```

`dagKindLabel` 同步加 `backup: "逻辑备份"`。

---

## 3. 提交清单(新模块上线前逐项核对)

- [ ] 新增/复用 Step 均在 `dag.rs::Step`,可序列化且幂等;新变体只追加不改写
- [ ] `instance.rs::exec_step` 有对应分支,执行原语在 `docker.rs`(或已有)
- [ ] 组合子函数产出 TaskNode:合理 `deps/retries/timeout_secs`;名称不与更早分类关键词撞车
- [ ] RdsManager 方法:`assert_state` 门槛 → `enabled`/其它业务前置检查 → `lock_instance` → 审计 submitted → `submit` → 选对 watcher
- [ ] `classify_node_module` + 前端 `MODULE_META/MODULE_ORDER/nodeModule` 标签/顺序一致;CSS `.m-<k>` 有色
- [ ] `api.rs` 处理函数 + `http.rs` 路由分发 + `perm_for` 权限登记(新权限还要加 `store.rs::PERMISSIONS`)
- [ ] `nodegroups()` 有目录项;前端 KIND_CN/筛选下拉/向导卡片/dagSubmit/兜底目录已同步
- [ ] 测试:`cargo test`(门槛类 + 提交/终态语义)、`node --check` 页面脚本语法、浏览器探针回归
- [ ] 重启续跑验证:把任务留在 running 态杀进程重启,确认该模块节点能续跑/幂等重入

## 4. 验收场景(以 backup 为例)

1. 对不存在实例 → 400「实例 x 不存在」;
2. 停用实例 → 400「已停用」;非 Running 状态 → 400「当前状态 …,不允许该操作」;
3. 缺主节点 → 400「缺少主节点」;
4. Running 实例 → 200 返回 `task_id: t-backup-N`;任务列表出现 kind=backup,节点徽章为「备份」(玫瑰色 .m-backup);
5. 编排页「节点组目录」出现"主库逻辑备份"卡(模块: 备份 ×1),「新建此编排」→ 选实例 → dry-run 预览单节点链 → 提交成功;
6. 真实环境:主容器内生成 `/tmp/rds-<name>-backup.sql`,宿主 `logs/rds/<name>/backup/README.txt` 登记产物;
7. 备份任务失败(如容器不在):实例仍为 running、告警留痕、操作锁释放后可重试。

## 5. 常见坑

- **分类顺序**:备份关键词放奴隶后面(离线从名字含"备份");master/主节点/主库 在最前,起名避免"主库备份"这类词;
- **失败语义选错 watcher**:非生命周期模块用 `watch_task` 会把运行实例置 Failed;
- **忘记持久化兼容**:改既有 Step 字段或删变体 → 老库解析失败;
- **锁不释放**:忘记终态联动,或新提交路径没 watch → 实例永远"有其他操作进行中";
- **前端少一处**:目录能看到但提交无权限提示 / 提交成功但任务筛选没有该 kind / 徽章显示 other —— 按 §2 六步清单回查。

## 6. 不写代码的替代路径:Phase B「编排示意 / 展示先行」

只想在页面上编排示意(不触发真实容器动作)的模块,走既有"用户草稿"通道,零 Rust 改动:

- POST `/api/rds/task/draft?instance=&nodes=[{"id":"n1","name":"…","note":"…"}]`(tasks.manage)
  创建 **Noop 草稿**;任务可编辑(POST task/edit)、启动(POST task/start)、删除(POST task/delete);
- 草稿节点 steps 全部为 `Noop`,模块归 "other"(前端可按名称关键词细化);
- 适用:业务流程设计评审、模板预演、把"将来接执行器"的编排先固化在任务列表;
- 需要真实执行时,再按本手册 L0→L6 落地为功能模块,把 Noop 换成真实 Step 序列。

---

*对照样板:`git grep -n "backup" src/ | head -50` 可快速查看 backup 模块全部落点。
各落点:dag.rs(Step::DockerExec/分类)、docker.rs(exec_in)、instance.rs(backup_nodes/run_backup/watch_backup_task/nodegroups)、
api.rs(backup)、http.rs(路由+perm)、rds.html(MODULE_META/nodeModule/.m-backup/KIND_CN/向导)。*
