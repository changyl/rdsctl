// rdsctl — DAG 任务调度器(可持久化版)
//
// 通用 DAG 执行引擎。节点由**可序列化步骤**(Step)组成,不再持有闭包——
// 步骤连同任务/节点状态写入 SQLite,进程重启后任务不丢;
// 中断任务启动时标记 failed,配合幂等步骤可安全重提交。
//
// 节点并发执行规则:无依赖节点并发(tokio JoinSet),依赖完成后驱动下游;
// 任一节点失败则其下游全部跳过、任务失败;节点支持重试与单节点超时;
// 任务支持取消(节点间检查 cancel 标志)。

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::store::Store;

/// 节点/任务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending,
    Running,
    Success,
    Failed,
    Skipped,
}

impl Default for TaskStatus {
    fn default() -> Self {
        TaskStatus::Pending
    }
}

impl TaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Running => "running",
            TaskStatus::Success => "success",
            TaskStatus::Failed => "failed",
            TaskStatus::Skipped => "skipped",
        }
    }
}

/// 任务节点执行结果
#[derive(Debug, Clone, Serialize)]
pub struct NodeResult {
    pub status: TaskStatus,
    pub output: String,
    pub attempts: u32,
    pub started_at: u64,
    pub finished_at: u64,
}

/// 可序列化执行步骤(任务/实例重启后可恢复;执行器见 crate::instance::exec_step)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Step {
    DockerRun {
        container: String,
        args: Vec<String>,
    },
    /// 容器内执行任意命令(docker exec,回显 stdout)——通用运维原子步骤
    /// (由样例功能模块「backup」引入;见 docs/dag-module-howto.md)
    DockerExec {
        container: String,
        args: Vec<String>,
    },
    DockerRm {
        container: String,
    },
    NetworkCreate {
        name: String,
    },
    NetworkRm {
        name: String,
    },
    ExecSql {
        container: String,
        user: String,
        pass: String,
        sql: String,
    },
    HostMysql {
        port: u16,
        user: String,
        pass: String,
        sql: String,
    },
    WaitHealthy {
        container: String,
        timeout_secs: u64,
    },
    WaitMysql {
        container: String,
        user: String,
        pass: String,
        timeout_secs: u64,
    },
    WriteHostFile {
        path: String,
        content: String,
    },
    /// 更新实例状态(由执行器写 RdsManager)
    UpdateInstanceStatus {
        instance: String,
        status: String,
    },
    /// 更新实例拓扑(扩容后把新节点加入实例记录)
    UpdateInstanceTopology {
        instance: String,
        container: String,
        role: String,
        host_port: u16,
        server_id: u64,
        /// 扩容目标 region/az(空=沿用实例默认;跨区扩容由 agent 层后续实现)
        #[serde(default)]
        region: String,
        #[serde(default)]
        az: String,
        /// 分片级扩容:目标分片(s1..sN;空=单分片,沿用实例 shard 与全局 master)
        #[serde(default)]
        shard: String,
        /// 分片级扩容:复制源父节点容器(分片 master;空=实例首 master)
        #[serde(default)]
        parent: String,
    },
    /// 创建收尾验证:GTID 追平 + 数据一致 + 代理连通
    VerifyReplication {
        master: String,
        slaves: Vec<String>,
        proxy_port: u16,
    },
    /// 扩容验证:GTID 追平 + 数据一致
    VerifyScaleout {
        master: String,
        new_slave: String,
    },
    /// 启动实例接入转发器(LVS 接入层,进程内实现:VIP → 各 Proxy 宿主端口;镜像无关)
    EnsureLvs {
        instance: String,
    },
    /// 停止实例接入转发器(销毁实例/清理时)
    StopLvs {
        instance: String,
    },
    /// 记录审计事件(由执行器写 Store)
    Audit {
        instance: String,
        action: String,
        params: String,
    },
    /// DTS 占位注册(创建实例可选随建):不实际拉取/运行 DTS 引擎,
    /// 仅登记链路记录(占位容器节点 + 规格) → running;失败不应发生(纯注册,非阻断)。
    DtsRun {
        instance: String,
        node: String,
        dts_container: String,
        target_label: String,
        spec: String,
    },
    /// 节点替换核心(过保/退役机器迁移,见 docs/physical-multi-site-ops.md §3/§7-②):
    /// 在目标宿主机用临时名建新从 → 挂复制链追平 → 校验 → 摘旧容器 → 换名上线。
    /// 任一阶段失败自动清理临时容器,旧节点保持可用(天然回滚;幂等可重跑)。
    /// host = 目标宿主机名(经 Host 注册表 agent 执行);host_port/server_id 为新容器参数。
    ReplaceNodeCore {
        instance: String,
        node: String,
        host: String,
        host_port: u16,
        server_id: u64,
    },
    /// 替换提交(由执行器写 RdsManager):换 node_hosts 绑定 + 节点 host_port/server_id
    ReplaceNodeCommit {
        instance: String,
        node: String,
        host: String,
        host_port: u16,
        server_id: u64,
    },
    /// 实例整体迁移(跨 DC/跨 Host,见 docs/physical-multi-site-ops.md §4/§7-③):
    /// 逐节点迁到目标宿主机组(从节点=同身份替换;主节点=新主就绪后受管切主+身份落位),
    /// 完成 region/az 事实与绑定迁移。失败中断于当前节点,已迁节点保持可用(可续跑)。
    /// region/az = 目标事实;hosts = 逗号分隔目标宿主机(已登记且 agent 接入)。
    MigrateInstance {
        instance: String,
        region: String,
        az: String,
        hosts: String,
    },
    Noop {
        note: String,
    },
}

/// 步骤执行器:Step → 输出日志(实例/容器编排层实现)
pub type StepExecutor = Arc<
    dyn Fn(&Step) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send>> + Send + Sync,
>;

/// 任务节点定义(数据化,可持久化)
#[derive(Debug, Clone)]
pub struct TaskNode {
    pub id: String,
    pub name: String,
    /// 依赖的节点 id
    pub deps: Vec<String>,
    /// 失败重试次数(不含首次)
    pub retries: u32,
    /// 单节点超时(秒)
    pub timeout_secs: Option<u64>,
    /// 节点步骤(顺序执行;任一失败即节点失败)
    pub steps: Vec<Step>,
}

/// 任务元数据(状态 + 时间,经 Mutex 可变共享)
#[derive(Serialize, Default)]
pub struct TaskMeta {
    pub status: TaskStatus,
    pub started_at: Option<u64>,
    pub finished_at: Option<u64>,
}

/// DAG 任务(提交后常驻 scheduler.tasks,前端/持久化可查询)
pub struct DagTask {
    pub id: String,
    pub kind: String,
    pub instance: String,
    /// 提交人(操作审计人员;历史/续跑任务可为空)
    pub creator: String,
    pub created_at: u64,
    pub meta: parking_lot::Mutex<TaskMeta>,
    pub nodes: HashMap<String, String>,
    pub deps: HashMap<String, Vec<String>>,
    pub results: DashMap<String, NodeResult>,
    /// 节点完整定义(含步骤/重试参数;步骤手动重试用;续跑恢复任务为空)
    pub defs: HashMap<String, TaskNode>,
    /// 有序节点定义(用户草稿创建/编辑后原序启动;普通任务沿用 submit 时的 nodes)
    pub defs_order: parking_lot::Mutex<Vec<TaskNode>>,
}

impl DagTask {
    pub fn to_view(&self) -> serde_json::Value {
        let meta = self.meta.lock();
        let nodes: Vec<serde_json::Value> = self
            .nodes
            .iter()
            .map(|(id, name)| {
                let r = self.results.get(id);
                serde_json::json!({
                    "id": id,
                    "name": name,
                    "status": r.as_deref().map(|x| x.status).unwrap_or(TaskStatus::Pending),
                    "output": r.as_deref().map(|x| x.output.clone()).unwrap_or_default(),
                    "attempts": r.as_deref().map(|x| x.attempts).unwrap_or(0),
                    "started_at": r.as_deref().map(|x| x.started_at).unwrap_or(0),
                    "finished_at": r.as_deref().map(|x| x.finished_at).unwrap_or(0),
                    "deps": self.deps.get(id).cloned().unwrap_or_default(),
                    "module": classify_node_module(id, name, &self.defs),
                })
            })
            .collect();
        serde_json::json!({
            "id": self.id,
            "kind": self.kind,
            "instance": self.instance,
            "creator": self.creator,
            "status": meta.status,
            "created_at": self.created_at,
            "started_at": meta.started_at,
            "finished_at": meta.finished_at,
            "nodes": nodes,
        })
    }
}

/// 任务调度器
/// 节点所属功能模块(前端 DAG 模块展示;历史任务 defs 为空时按名称分类)
fn classify_node_module(id: &str, name: &str, defs: &HashMap<String, TaskNode>) -> String {
    let n = String::from(name).to_lowercase();
    if n.contains("主节点") || n.contains("master") {
        return "master".into();
    }
    if n.contains("从节点") {
        return "slave".into();
    }
    // 备份(须在从节点之后:离线从节点名如「启动从节点(统计/备份)」先归从库)
    if n.contains("备份") || n.contains("backup") {
        return "backup".into();
    }
    if n.contains("网络") {
        return "net".into();
    }
    if n.contains("newproxy 配置") || (n.contains("配置") && n.contains("newproxy")) {
        return "config".into();
    }
    if n.contains("代理") || n.contains("proxy") {
        return "proxy".into();
    }
    if n.contains("复制") || n.contains("replica") || n.contains("start replica") {
        return "repl".into();
    }
    if n.contains("校验") || n.contains("验证") || n.contains("连通") {
        return "verify".into();
    }
    if n.contains("清理") || n.contains("记录") {
        return "cleanup".into();
    }
    if let Some(node) = defs.get(id) {
        for step in &node.steps {
            match step {
                Step::NetworkCreate { .. } | Step::NetworkRm { .. } => return "net".into(),
                Step::DockerRun { container, .. } => {
                    let c = container.to_lowercase();
                    if c.contains("master") { return "master".into(); }
                    if c.contains("slave") { return "slave".into(); }
                    if c.contains("proxy") { return "proxy".into(); }
                }
                Step::DockerExec { container, args } => {
                    let c = container.to_lowercase();
                    if args.iter().any(|a| a.contains("mysqldump")) { return "backup".into(); }
                    if c.contains("master") { return "master".into(); }
                    if c.contains("slave") { return "slave".into(); }
                    if c.contains("proxy") { return "proxy".into(); }
                }
                Step::ExecSql { sql, .. } => {
                    if sql.contains("START REPLICA") { return "repl".into(); }
                }
                Step::VerifyReplication { .. } | Step::VerifyScaleout { .. } => return "verify".into(),
                Step::Audit { .. } => return "cleanup".into(),
                _ => {}
            }
        }
    }
    "other".into()
}

pub struct TaskScheduler {
    tasks: DashMap<String, Arc<DagTask>>,
    store: Option<Arc<Store>>,
    executor: StepExecutor,
    next_id: AtomicU64,
    pub running: AtomicU64,
    canceled: DashMap<String, bool>,
}

impl TaskScheduler {
    pub fn new(executor: StepExecutor, store: Option<Arc<Store>>) -> Self {
        Self {
            tasks: DashMap::new(),
            store,
            executor,
            next_id: AtomicU64::new(0),
            running: AtomicU64::new(0),
            canceled: DashMap::new(),
        }
    }

    /// 取消一个任务(节点间检查;已执行节点不中断)
    pub fn cancel(&self, task_id: &str) -> bool {
        if self.tasks.contains_key(task_id) {
            self.canceled.insert(task_id.to_string(), true);
            true
        } else {
            false
        }
    }

    /// 手动重试单个步骤:仅限「整任务已失败/跳过」的节点,且其直接上游须成功。
    /// 语义=重跑该节点一次(不改动下游);若重试后全部节点 ∈ {成功,跳过} 则任务转成功。
    /// 任务不在内存(重启/历史任务)时,先从持久化层水合出完整定义再执行。
    pub async fn rerun_node(self: &Arc<Self>, task_id: &str, node_id: &str) -> Result<String, String> {
        let mut task = self.tasks.get(task_id).map(|t| t.clone());
        if task.is_none() && self.store.is_some() {
            // 历史任务(进程重启后仅剩 DB 记录):水合定义与终态结果,再走同一重试路径
            task = self.hydrate_task(task_id);
        }
        let Some(mut task) = task else {
            return Err(format!("任务 {task_id} 不存在(或该任务无持久化节点定义)"));
        };
        {
            let m = task.meta.lock();
            if m.status != TaskStatus::Failed {
                return Err("仅失败/跳过(终态)的任务支持手动重试步骤".to_string());
            }
        }
        let node = {
            // 内存 defs 缺失(如续跑恢复的任务)时,回退按 DB 定义水合
            let mut nd = task.defs.get(node_id).cloned();
            if nd.is_none() && self.store.is_some() {
                if let Some(t2) = self.hydrate_task(task_id) {
                    task = t2;
                    nd = task.defs.get(node_id).cloned();
                }
            }
            nd
        };
        let Some(node) = node else {
            return Err(format!("节点 {node_id} 定义缺失,无法手动重试(节点步骤未持久化)"));
        };
        let cur = task.results.get(node_id).map(|r| r.status).unwrap_or(TaskStatus::Pending);
        if !matches!(cur, TaskStatus::Failed | TaskStatus::Skipped) {
            return Err(format!("节点 {node_id} 当前状态无需重试"));
        }
        for d in &node.deps {
            let st = task.results.get(d).map(|r| r.status).unwrap_or(TaskStatus::Pending);
            if st != TaskStatus::Success {
                return Err(format!("上游节点 {d} 未成功,请先重试上游"));
            }
        }
        {
            let mut m = task.meta.lock();
            m.status = TaskStatus::Running;
            m.finished_at = None;
        }
        if let Some(st) = &self.store {
            let started = task.meta.lock().started_at;
            st.upsert_task(&task.id, &task.kind, &task.instance, "running", task.created_at, started, None);
        }
        let (id, out, attempts) = run_node(node.clone(), self.executor.clone()).await;
        let ok = out.is_ok();
        let status = if ok { TaskStatus::Success } else { TaskStatus::Failed };
        let now = now_secs();
        let r = NodeResult { status, output: out.unwrap_or_else(|e| e), attempts, started_at: now, finished_at: now };
        task.results.insert(id, r.clone());
        persist_node(&self.store, &task, &node, &r);
        // 汇总终态:全部 ∈ {成功,跳过} → 成功;否则仍失败
        let mut fin = TaskStatus::Success;
        let keys: Vec<String> = task.defs.keys().cloned().collect();
        for k in keys {
            let st = task.results.get(&k).map(|x| x.status).unwrap_or(TaskStatus::Pending);
            if !matches!(st, TaskStatus::Success | TaskStatus::Skipped) {
                fin = TaskStatus::Failed;
                break;
            }
        }
        let finished = if fin == TaskStatus::Success { Some(now_secs()) } else { None };
        {
            let mut m = task.meta.lock();
            m.status = fin;
            m.finished_at = finished;
        }
        if let Some(st) = &self.store {
            st.update_task_status(&task.id, task.meta.lock().status.as_str(), finished);
        }
        if ok {
            Ok(format!("节点 {node_id} 已重试成功"))
        } else {
            Err(format!("节点 {node_id} 重试后仍失败"))
        }
    }

    /// 提交 DAG 任务(数据化节点),返回 task_id。
    /// 接收 &Arc<Self>:spawn 需要 Arc 句柄,同时避免经 crate::manager() 自引用。
    pub fn submit(self: &Arc<Self>, kind: &str, instance: &str, creator: &str, nodes: Vec<TaskNode>) -> String {
        // id 序号:有 DB 时用持久化自增序列(重启安全、不重复);
        // 无 DB(单元测试)退回进程内计数器。
        let seq = match &self.store {
            Some(st) => st.next_task_seq(kind),
            None => self.next_id.fetch_add(1, Ordering::Relaxed),
        };
        let id = format!("t-{}-{}", kind, seq);
        let nodes_map: HashMap<String, String> =
            nodes.iter().map(|n| (n.id.clone(), n.name.clone())).collect();
        let defs_map: HashMap<String, TaskNode> =
            nodes.iter().map(|n| (n.id.clone(), n.clone())).collect();
        let deps_map: HashMap<String, Vec<String>> = nodes
            .iter()
            .map(|n| (n.id.clone(), n.deps.clone()))
            .collect();
        let task = Arc::new(DagTask {
            id: id.clone(),
            kind: kind.to_string(),
            instance: instance.to_string(),
            creator: creator.to_string(),
            created_at: now_secs(),
            meta: parking_lot::Mutex::new(TaskMeta::default()),
            nodes: nodes_map,
            deps: deps_map,
            results: DashMap::new(),
            defs: defs_map,
            defs_order: parking_lot::Mutex::new(nodes.clone()),
        });
        self.tasks.insert(id.clone(), task.clone());

        // 持久化:任务 + 节点骨架
        if let Some(st) = &self.store {
            st.upsert_task(&id, kind, instance, "pending", task.created_at, None, None);
            for n in &nodes {
                let deps_json = serde_json::to_string(&n.deps).unwrap_or_else(|_| "[]".into());
                let steps_json = serde_json::to_string(&n.steps).unwrap_or_else(|_| "[]".into());
                st.upsert_node(
                    &id,
                    &n.id,
                    &n.name,
                    &deps_json,
                    &steps_json,
                    "pending",
                    "",
                    0,
                    n.retries,
                    n.timeout_secs,
                    None,
                    None,
                );
            }
        }
        self.canceled.remove(&id);
        self.running.fetch_add(1, Ordering::Relaxed);
        let sched = Arc::clone(self);
        let executor = self.executor.clone();
        let store = self.store.clone();
        tokio::spawn(async move {
            sched.execute(task, nodes, executor, store).await;
            sched.running.fetch_sub(1, Ordering::Relaxed);
        });
        id
    }

    /// 任务列表(新→旧);内存任务 + DB 历史任务(重启后仍可见)合并去重
    pub fn list(&self) -> Vec<serde_json::Value> {
        let live: Vec<_> = self.tasks.iter().map(|e| e.value().to_view()).collect();
        let hist: Vec<serde_json::Value> = match &self.store {
            Some(st) => st.task_views(300),
            None => Vec::new(),
        };
        let mut v = merge_views(live, hist);
        v.sort_by_key(|t| t["created_at"].as_u64().unwrap_or(0));
        v.reverse();
        v
    }

    /// 用户自定义草稿:仅建 pending 任务不执行(Phase B 可编辑/启动)
    pub fn submit_draft(self: &Arc<Self>, instance: &str, creator: &str, nodes: Vec<TaskNode>) -> Result<String, String> {
        if nodes.is_empty() { return Err("草稿至少包含一个节点".to_string()); }
        let seq = match &self.store {
            Some(st) => st.next_task_seq("user"),
            None => self.next_id.fetch_add(1, Ordering::Relaxed),
        };
        let id = format!("t-user-{seq}");
        let nodes_map: HashMap<String, String> =
            nodes.iter().map(|n| (n.id.clone(), n.name.clone())).collect();
        let deps_map: HashMap<String, Vec<String>> =
            nodes.iter().map(|n| (n.id.clone(), n.deps.clone())).collect();
        let defs_map: HashMap<String, TaskNode> =
            nodes.iter().map(|n| (n.id.clone(), n.clone())).collect();
        let task = Arc::new(DagTask {
            id: id.clone(),
            kind: "user".to_string(),
            instance: instance.to_string(),
            creator: creator.to_string(),
            created_at: now_secs(),
            meta: parking_lot::Mutex::new(TaskMeta::default()),
            nodes: nodes_map,
            deps: deps_map,
            results: DashMap::new(),
            defs: defs_map,
            defs_order: parking_lot::Mutex::new(nodes),
        });
        self.tasks.insert(id.clone(), task.clone());
        self.canceled.remove(&id);
        Ok(id)
    }

    /// 启动用户草稿(执行其有序节点定义)
    pub async fn start_draft(self: &Arc<Self>, task_id: &str) -> Result<String, String> {
        let Some(task) = self.tasks.get(task_id).map(|t| t.clone()) else {
            return Err(format!("任务 {task_id} 不存在"));
        };
        if task.kind != "user" { return Err("仅用户自定义草稿可手动启动".to_string()); }
        {
            let m = task.meta.lock();
            if m.status != TaskStatus::Pending { return Err("仅待启动(pending)的草稿可启动".to_string()); }
        }
        let nodes = task.defs_order.lock().clone();
        if nodes.is_empty() { return Err("草稿无节点,无法启动".to_string()); }
        self.canceled.remove(task_id);
        self.running.fetch_add(1, Ordering::Relaxed);
        let sched = Arc::clone(self);
        let executor = self.executor.clone();
        let store = self.store.clone();
        tokio::spawn(async move {
            sched.execute(task, nodes, executor, store).await;
            sched.running.fetch_sub(1, Ordering::Relaxed);
        });
        Ok(format!("草稿 {task_id} 已启动"))
    }

    /// 编辑用户草稿(pending 未启动;替换全部节点,来源 human)
    pub fn edit_draft(&self, task_id: &str, nodes: Vec<TaskNode>) -> Result<(), String> {
        let Some(orig) = self.tasks.get(task_id).map(|t| t.clone()) else {
            return Err(format!("任务 {task_id} 不存在"));
        };
        if orig.kind != "user" { return Err("仅用户自定义草稿可编辑".to_string()); }
        {
            let m = orig.meta.lock();
            if m.status != TaskStatus::Pending { return Err("仅待启动(pending)的草稿可编辑".to_string()); }
        }
        if nodes.is_empty() { return Err("草稿至少包含一个节点".to_string()); }
        let nodes_map: HashMap<String, String> =
            nodes.iter().map(|n| (n.id.clone(), n.name.clone())).collect();
        let deps_map: HashMap<String, Vec<String>> =
            nodes.iter().map(|n| (n.id.clone(), n.deps.clone())).collect();
        let defs_map: HashMap<String, TaskNode> =
            nodes.iter().map(|n| (n.id.clone(), n.clone())).collect();
        let task = Arc::new(DagTask {
            id: orig.id.clone(),
            kind: orig.kind.clone(),
            instance: orig.instance.clone(),
            creator: orig.creator.clone(),
            created_at: orig.created_at,
            meta: parking_lot::Mutex::new(TaskMeta::default()),
            nodes: nodes_map,
            deps: deps_map,
            results: DashMap::new(),
            defs: defs_map,
            defs_order: parking_lot::Mutex::new(nodes),
        });
        self.tasks.insert(task_id.to_string(), task);
        self.canceled.remove(task_id);
        Ok(())
    }

    /// 删除终态任务(内存 + 历史);运行/待启动任务拒绝
    pub fn delete_task(&self, task_id: &str) -> Result<(), String> {
        let live_term = self.tasks.get(task_id).map(|t| {
            let m = t.meta.lock();
            matches!(m.status, TaskStatus::Success | TaskStatus::Failed | TaskStatus::Skipped)
        });
        match live_term {
            Some(true) => {
                self.tasks.remove(task_id);
                self.canceled.remove(task_id);
            }
            Some(false) => return Err("运行/待启动任务不可删除".to_string()),
            None => {
                // 仅历史(DB)任务
                if let Some(st) = &self.store {
                    if let Some(v) = st.task_view(task_id) {
                        let st2 = v["status"].as_str().unwrap_or("");
                        if !matches!(st2, "success" | "failed" | "skipped") {
                            return Err("运行/待启动任务不可删除".to_string());
                        }
                    } else {
                        return Err(format!("任务 {task_id} 不存在"));
                    }
                } else {
                    return Err(format!("任务 {task_id} 不存在"));
                }
            }
        }
        if let Some(st) = &self.store {
            st.delete_task(task_id)?;
        }
        Ok(())
    }

    pub fn get(&self, id: &str) -> Option<serde_json::Value> {
        self.tasks
            .get(id)
            .map(|t| t.to_view())
            .or_else(|| self.store.as_ref().and_then(|st| st.task_view(id)))
    }

    pub fn instance_tasks(&self, instance: &str) -> Vec<serde_json::Value> {
        let live: Vec<_> = self
            .tasks
            .iter()
            .filter(|e| e.value().instance == instance)
            .map(|e| e.value().to_view())
            .collect();
        let hist: Vec<serde_json::Value> = match &self.store {
            Some(st) => st.task_views_by_instance(instance, 200),
            None => Vec::new(),
        };
        let mut v = merge_views(live, hist);
        v.sort_by_key(|t| t["created_at"].as_u64().unwrap_or(0));
        v.reverse();
        v
    }

    // ─── 执行引擎 ───

    async fn execute(
        &self,
        task: Arc<DagTask>,
        nodes: Vec<TaskNode>,
        executor: StepExecutor,
        store: Option<Arc<Store>>,
    ) {
        {
            let mut m = task.meta.lock();
            m.started_at = Some(now_secs());
            m.status = TaskStatus::Running;
        }
        if let Some(st) = &store {
            st.upsert_task(
                &task.id,
                &task.kind,
                &task.instance,
                "running",
                task.created_at,
                task.meta.lock().started_at,
                None,
            );
        }

        // 依赖计数 + 下游映射 + 环检测
        let mut deps_left: HashMap<String, usize> =
            nodes.iter().map(|n| (n.id.clone(), n.deps.len())).collect();
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
        for n in &nodes {
            for d in &n.deps {
                dependents.entry(d.clone()).or_default().push(n.id.clone());
            }
        }
        if let Some(cycle) = find_cycle(&nodes) {
            for n in &nodes {
                let r = NodeResult {
                    status: if n.id == cycle {
                        TaskStatus::Failed
                    } else {
                        TaskStatus::Skipped
                    },
                    output: if n.id == cycle {
                        format!("DAG 检测到环: {cycle}")
                    } else {
                        "上游节点失败,跳过".into()
                    },
                    attempts: 0,
                    started_at: now_secs(),
                    finished_at: now_secs(),
                };
                task.results.insert(n.id.clone(), r.clone());
                persist_node(&store, &task, n, &r);
            }
            finish_task(&task, TaskStatus::Failed, &store);
            return;
        }

        let node_by_id: HashMap<String, &TaskNode> = nodes.iter().map(|n| (n.id.clone(), n)).collect();
        let mut ready: Vec<&TaskNode> = nodes
            .iter()
            .filter(|n| deps_left.get(&n.id).copied().unwrap_or(0) == 0)
            .collect();
        let mut pending: HashSet<String> = nodes.iter().map(|n| n.id.clone()).collect();
        let mut failed = false;

        while !ready.is_empty() && !self.is_canceled(&task.id) {
            let mut joins = tokio::task::JoinSet::new();
            for n in &ready {
                pending.remove(&n.id);
                let r0 = NodeResult {
                    status: TaskStatus::Running,
                    output: String::new(),
                    attempts: 0,
                    started_at: now_secs(),
                    finished_at: 0,
                };
                task.results.insert(n.id.clone(), r0);
                let node = (*n).clone();
                let executor = executor.clone();
                let cancel_flag = self.is_canceled(&task.id);
                joins.spawn(async move {
                    if cancel_flag {
                        return (node.id.clone(), Err("任务已取消".to_string()), 0u32);
                    }
                    run_node(node, executor).await
                });
            }
            ready.clear();
            while let Some(res) = joins.join_next().await {
                let (id, out, attempts) = match res {
                    Ok(v) => v,
                    Err(e) => {
                        failed = true;
                        // join 出错时无法得知节点 id,置空处理(防御性)
                        let r = NodeResult {
                            status: TaskStatus::Failed,
                            output: format!("节点执行异常: {e}"),
                            attempts: 0,
                            started_at: now_secs(),
                            finished_at: now_secs(),
                        };
                        task.results.insert("__panic__".to_string(), r);
                        continue;
                    }
                };
                let done_ok = out.is_ok();
                let status = if done_ok {
                    TaskStatus::Success
                } else {
                    failed = true;
                    TaskStatus::Failed
                };
                let r = NodeResult {
                    status,
                    output: out.unwrap_or_else(|e| e),
                    attempts,
                    started_at: now_secs(),
                    finished_at: now_secs(),
                };
                task.results.insert(id.clone(), r.clone());
                if let Some(n) = node_by_id.get(&id) {
                    persist_node(&store, &task, n, &r);
                }
                // 驱动下游
                if let Some(ds) = dependents.get(&id) {
                    for d in ds {
                        if let Some(c) = deps_left.get_mut(d) {
                            *c = c.saturating_sub(1);
                            if *c == 0 && pending.contains(d) {
                                if let Some(n) = node_by_id.get(d) {
                                    ready.push(*n);
                                }
                            }
                        }
                    }
                }
            }
            // 失败传播
            if failed {
                for n in &nodes {
                    if pending.contains(&n.id) {
                        pending.remove(&n.id);
                        let r = NodeResult {
                            status: TaskStatus::Skipped,
                            output: "上游节点失败,跳过".into(),
                            attempts: 0,
                            started_at: now_secs(),
                            finished_at: now_secs(),
                        };
                        task.results.insert(n.id.clone(), r.clone());
                        persist_node(&store, &task, n, &r);
                    }
                }
                break;
            }
        }

        let final_status = if failed {
            TaskStatus::Failed
        } else if self.is_canceled(&task.id) {
            TaskStatus::Failed
        } else {
            TaskStatus::Success
        };
        finish_task(&task, final_status, &store);
    }

    fn is_canceled(&self, task_id: &str) -> bool {
        self.canceled.get(task_id).map(|v| *v).unwrap_or(false)
    }

    // ─── M0-5:启动续跑 ───
    // 崩溃/重启后,把持久化层未终态(pending/running)任务重新入执行:
    // 已完成节点保留其原结果与输出(不重跑),仅 pending/running 节点重新执行。
    // 前提:节点步骤幂等(见 docs/scaling-design.md §7)。

    pub fn resume_pending(self: &Arc<Self>) -> usize {
        let Some(st) = &self.store else { return 0 };
        let defs = st.pending_task_definitions();
        if defs.is_empty() {
            return 0;
        }
        let mut n = 0;
        for raw in defs {
            if let Ok(def) = serde_json::from_value::<PersistedTaskDef>(raw) {
                self.resume_one(def);
                n += 1;
            }
        }
        tracing::info!("启动恢复:续跑 {n} 个未完成任务");
        n
    }

    /// 从持久化层水合任意状态的任务(历史/重启后的任务,内存无 defs):
    /// 读取节点完整定义(步骤/重试/超时)与已落库的终态结果,重建内存任务视图。
    /// 供 rerun_node(手动重试历史节点)使用;返回 None = 任务不存在或记录不可解析。
    fn hydrate_task(self: &Arc<Self>, task_id: &str) -> Option<Arc<DagTask>> {
        let st = self.store.as_ref()?;
        let raw = st.task_definition(task_id)?;
        let def = serde_json::from_value::<PersistedTaskDef>(raw).ok()?;
        let now = now_secs();
        let results = DashMap::new();
        for nd in &def.nodes {
            results.insert(
                nd.node_id.clone(),
                NodeResult {
                    status: task_status_from(&nd.status),
                    output: nd.output.clone(),
                    attempts: nd.attempts,
                    started_at: now,
                    finished_at: now,
                },
            );
        }
        let names: HashMap<String, String> =
            def.nodes.iter().map(|n| (n.node_id.clone(), n.name.clone())).collect();
        let deps_full: HashMap<String, Vec<String>> =
            def.nodes.iter().map(|n| (n.node_id.clone(), n.deps.clone())).collect();
        let defs: HashMap<String, TaskNode> = def
            .nodes
            .iter()
            .map(|n| {
                (
                    n.node_id.clone(),
                    TaskNode {
                        id: n.node_id.clone(),
                        name: n.name.clone(),
                        deps: n.deps.clone(),
                        retries: n.retries,
                        timeout_secs: n.timeout_secs,
                        steps: n.steps.clone(),
                    },
                )
            })
            .collect();
        let status = def
            .status
            .as_deref()
            .map(task_status_from)
            .unwrap_or(TaskStatus::Failed);
        let task = Arc::new(DagTask {
            id: def.id.clone(),
            kind: def.kind.clone(),
            instance: def.instance.clone(),
            creator: String::new(),
            created_at: def.created_at,
            meta: parking_lot::Mutex::new(TaskMeta {
                status,
                started_at: def.started_at.or(Some(now)),
                finished_at: def.finished_at.or(Some(now)),
            }),
            nodes: names,
            deps: deps_full,
            results,
            defs,
            defs_order: parking_lot::Mutex::new(Vec::new()),
        });
        self.tasks.insert(task_id.to_string(), task.clone());
        Some(task)
    }

    fn resume_one(self: &Arc<Self>, def: PersistedTaskDef) {
        let executor = self.executor.clone();
        let store = self.store.clone();
        let now = now_secs();
        // 终态节点:原结果带入内存视图,不重跑
        let terminal: HashMap<String, NodeResult> = def
            .nodes
            .iter()
            .filter(|nd| matches!(nd.status.as_str(), "success" | "failed" | "skipped"))
            .map(|nd| {
                (
                    nd.node_id.clone(),
                    NodeResult {
                        status: task_status_from(&nd.status),
                        output: nd.output.clone(),
                        attempts: nd.attempts,
                        started_at: now,
                        finished_at: now,
                    },
                )
            })
            .collect();
        let succeeded: HashSet<String> = def
            .nodes
            .iter()
            .filter(|nd| nd.status == "success")
            .map(|nd| nd.node_id.clone())
            .collect();
        let results: DashMap<String, NodeResult> = DashMap::new();
        for (id, r) in &terminal {
            results.insert(id.clone(), r.clone());
        }
        // 待执行节点:pending/running;指向已成功终态节点的依赖边剪掉(视为已满足)
        let to_run: Vec<TaskNode> = def
            .nodes
            .iter()
            .filter(|nd| nd.status == "pending" || nd.status == "running")
            .map(|nd| TaskNode {
                id: nd.node_id.clone(),
                name: nd.name.clone(),
                deps: nd
                    .deps
                    .iter()
                    .filter(|d| !succeeded.contains(*d))
                    .cloned()
                    .collect(),
                retries: nd.retries,
                timeout_secs: nd.timeout_secs,
                steps: nd.steps.clone(),
            })
            .collect();
        if to_run.is_empty() {
            tracing::warn!("任务 {} 无可续跑节点,跳过", def.id);
            return;
        }
        let names: HashMap<String, String> = def
            .nodes
            .iter()
            .map(|nd| (nd.node_id.clone(), nd.name.clone()))
            .collect();
        let deps_full: HashMap<String, Vec<String>> = def
            .nodes
            .iter()
            .map(|nd| (nd.node_id.clone(), nd.deps.clone()))
            .collect();
        for nd in &to_run {
            results.insert(
                nd.id.clone(),
                NodeResult {
                    status: TaskStatus::Running,
                    output: String::new(),
                    attempts: 0,
                    started_at: now,
                    finished_at: 0,
                },
            );
        }
        let task = Arc::new(DagTask {
            id: def.id.clone(),
            kind: def.kind.clone(),
            instance: def.instance.clone(),
            creator: String::new(),
            created_at: def.created_at,
            meta: parking_lot::Mutex::new(TaskMeta {
                status: TaskStatus::Running,
                started_at: Some(now),
                finished_at: None,
            }),
            nodes: names,
            deps: deps_full,
            results,
            defs: HashMap::new(),
            defs_order: parking_lot::Mutex::new(Vec::new()),
        });
        self.tasks.insert(def.id.clone(), task.clone());
        self.canceled.remove(&def.id);
        self.running.fetch_add(1, Ordering::Relaxed);
        let sched = Arc::clone(self);
        tokio::spawn(async move {
            sched.execute(task, to_run, executor, store).await;
            sched.running.fetch_sub(1, Ordering::Relaxed);
        });
    }
}

/// 持久化层还原的任务定义(续跑与历史任务水合共用;字段对齐 task_definition/pending_task_definitions JSON)
#[derive(Debug, Clone, Deserialize)]
struct PersistedTaskDef {
    id: String,
    kind: String,
    instance: String,
    created_at: u64,
    /// 任务级状态(pending_task_definitions 返回无此项 → 默认值;续跑按运行中处理)
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    started_at: Option<u64>,
    #[serde(default)]
    finished_at: Option<u64>,
    nodes: Vec<PersistedNodeDef>,
}

#[derive(Debug, Clone, Deserialize)]
struct PersistedNodeDef {
    node_id: String,
    name: String,
    deps: Vec<String>,
    steps: Vec<Step>,
    status: String,
    output: String,
    attempts: u32,
    retries: u32,
    timeout_secs: Option<u64>,
}

fn task_status_from(s: &str) -> TaskStatus {
    match s {
        "success" => TaskStatus::Success,
        "failed" => TaskStatus::Failed,
        "skipped" => TaskStatus::Skipped,
        "running" => TaskStatus::Running,
        _ => TaskStatus::Pending,
    }
}

/// 执行单个节点:按重试次数执行步骤序列;单步带超时
async fn run_node(
    node: TaskNode,
    executor: StepExecutor,
) -> (String, Result<String, String>, u32) {
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        let mut logs = Vec::new();
        let mut node_err: Option<String> = None;
        for step in &node.steps {
            let fut = executor(step);
            let out = match node.timeout_secs {
                Some(secs) => match tokio::time::timeout(
                    std::time::Duration::from_secs(secs),
                    fut,
                )
                .await
                {
                    Ok(r) => r,
                    Err(_) => Err(format!("步骤超时(>{secs}s)")),
                },
                None => fut.await,
            };
            match out {
                Ok(o) => logs.push(o),
                Err(e) => {
                    node_err = Some(e);
                    break;
                }
            }
        }
        if let Some(err) = node_err {
            if attempts <= node.retries {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
            let mut out = logs.join("\n");
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&format!("[第 {attempts} 次尝试失败] {err}"));
            return (node.id, Err(out), attempts);
        }
        return (node.id, Ok(logs.join("\n")), attempts);
    }
}

fn persist_node(store: &Option<Arc<Store>>, task: &DagTask, node: &TaskNode, r: &NodeResult) {
    if let Some(st) = store {
        let deps_json = serde_json::to_string(&node.deps).unwrap_or_else(|_| "[]".into());
        let steps_json = serde_json::to_string(&node.steps).unwrap_or_else(|_| "[]".into());
        st.upsert_node(
            &task.id,
            &node.id,
            &node.name,
            &deps_json,
            &steps_json,
            r.status.as_str(),
            &r.output,
            r.attempts,
            node.retries,
            node.timeout_secs,
            Some(r.started_at),
            Some(r.finished_at),
        );
    }
}

fn finish_task(task: &DagTask, status: TaskStatus, store: &Option<Arc<Store>>) {
    {
        let mut m = task.meta.lock();
        m.status = status;
        m.finished_at = Some(now_secs());
    }
    if let Some(st) = store {
        st.update_task_status(&task.id, status.as_str(), Some(now_secs()));
        st.audit(
            &crate::auth::current_user(),
            &task.instance,
            &format!("task:{}", task.kind),
            &task.id,
            if status == TaskStatus::Success {
                "success"
            } else {
                "failed"
            },
            &task.id,
        );
    }
}

/// 合并内存任务视图与 DB 历史视图(按 id 去重,内存优先;重启后历史任务仍可展示)
fn merge_views(
    live: Vec<serde_json::Value>,
    hist: Vec<serde_json::Value>,
) -> Vec<serde_json::Value> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<serde_json::Value> = Vec::with_capacity(live.len() + hist.len());
    for v in live {
        if let Some(id) = v["id"].as_str() {
            seen.insert(id.to_string());
        }
        out.push(v);
    }
    for v in hist {
        let dup = match v["id"].as_str() {
            Some(id) => !seen.insert(id.to_string()),
            None => true,
        };
        if !dup {
            out.push(v);
        }
    }
    out
}

/// 简单环检测:DFS 找第一个环成员
fn find_cycle(nodes: &[TaskNode]) -> Option<String> {
    let mut visiting: HashSet<String> = HashSet::new();
    let mut visited: HashSet<String> = HashSet::new();
    for n in nodes {
        if cycle_dfs(n.id.clone(), nodes, &mut visiting, &mut visited) {
            return Some(n.id.clone());
        }
    }
    None
}

fn cycle_dfs(
    id: String,
    nodes: &[TaskNode],
    visiting: &mut HashSet<String>,
    visited: &mut HashSet<String>,
) -> bool {
    if visiting.contains(&id) {
        return true;
    }
    if visited.contains(&id) {
        return false;
    }
    visiting.insert(id.clone());
    if let Some(n) = nodes.iter().find(|n| n.id == id) {
        for d in &n.deps {
            if cycle_dfs(d.clone(), nodes, visiting, visited) {
                return true;
            }
        }
    }
    visiting.remove(&id);
    visited.insert(id);
    false
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step_ok(note: &str) -> Step {
        Step::Noop {
            note: note.to_string(),
        }
    }

    fn node(id: &str, deps: &[&str], steps: Vec<Step>) -> TaskNode {
        TaskNode {
            id: id.to_string(),
            name: id.to_string(),
            deps: deps.iter().map(|s| s.to_string()).collect(),
            retries: 0,
            timeout_secs: None,
            steps,
        }
    }

    fn make_sched() -> Arc<TaskScheduler> {
        let exec: StepExecutor = Arc::new(|step: &Step| {
            let note = match step {
                Step::Noop { note } => note.clone(),
                _ => "?".into(),
            };
            Box::pin(async move { Ok(note) })
        });
        Arc::new(TaskScheduler::new(exec, None))
    }

    async fn wait_terminal(sched: &TaskScheduler, tid: &str) -> TaskStatus {
        for _ in 0..200 {
            if let Some(v) = sched.get(tid) {
                let st = v["status"].as_str().unwrap_or("");
                if st == "success" || st == "failed" {
                    return if st == "success" {
                        TaskStatus::Success
                    } else {
                        TaskStatus::Failed
                    };
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        TaskStatus::Pending
    }

    #[tokio::test]
    async fn executes_in_dependency_order() {
        let sched = make_sched();
        let nodes = vec![
            node("a", &[], vec![step_ok("a")]),
            node("b", &["a"], vec![step_ok("b")]),
            node("c", &["b"], vec![step_ok("c")]),
        ];
        let tid = sched.submit("test", "i1", "t", nodes);
        let st = wait_terminal(&sched, &tid).await;
        assert_eq!(st, TaskStatus::Success);
        let v = sched.get(&tid).unwrap();
        assert_eq!(v["nodes"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn failure_skips_dependents() {
        let exec: StepExecutor = Arc::new(|step: &Step| {
            let note = match step {
                Step::Noop { note } => note.clone(),
                _ => "?".into(),
            };
            let out = if note.starts_with("FAIL:") {
                Err(note)
            } else {
                Ok(note)
            };
            Box::pin(async move { out })
        });
        let sched = Arc::new(TaskScheduler::new(exec, None));
        let nodes = vec![
            TaskNode {
                id: "a".into(),
                name: "a".into(),
                deps: vec![],
                retries: 0,
                timeout_secs: None,
                steps: vec![Step::Noop {
                    note: "FAIL:boom".into(),
                }],
            },
            node("b", &["a"], vec![step_ok("b")]),
            node("c", &[], vec![step_ok("c")]),
        ];
        let tid = sched.submit("test", "i", "t", nodes);
        let st = wait_terminal(&sched, &tid).await;
        assert_eq!(st, TaskStatus::Failed);
        let v = sched.get(&tid).unwrap();
        let nodes_v = v["nodes"].as_array().unwrap().clone();
        let st_of = |id: &str| {
            nodes_v
                .iter()
                .find(|n| n["id"].as_str() == Some(id))
                .unwrap()["status"]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(st_of("a"), "failed");
        assert_eq!(st_of("b"), "skipped");
        assert_eq!(st_of("c"), "success");
    }

    #[test]
    fn module_classify_backup_and_offline_slave() {
        // 备份节点 → backup;离线从(名字含“备份”)必须先归从库
        let defs = HashMap::new();
        assert_eq!(classify_node_module("backup", "执行逻辑备份(mysqldump)", &defs), "backup");
        assert_eq!(classify_node_module("slave2", "启动从节点(统计/备份)", &defs), "slave");
        assert_eq!(classify_node_module("x", "移除网络", &defs), "net");
        // 步骤级判定:DockerExec 中出现 mysqldump → backup
        let mut d2 = HashMap::new();
        d2.insert(
            "backup".to_string(),
            TaskNode {
                id: "backup".into(),
                name: "自定名".into(),
                deps: vec![],
                retries: 1,
                timeout_secs: None,
                steps: vec![Step::DockerExec {
                    container: "rds-x-master".into(),
                    args: vec!["mysqldump --single-transaction appdb".into()],
                }],
            },
        );
        assert_eq!(classify_node_module("backup", "自定名", &d2), "backup");
    }

    #[tokio::test]
    async fn retry_then_succeed() {
        let tries = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let exec: StepExecutor = {
            let tries = tries.clone();
            Arc::new(move |step: &Step| {
                let t = tries.clone();
                let note = match step {
                    Step::Noop { note } => note.clone(),
                    _ => "?".into(),
                };
                Box::pin(async move {
                    if t.fetch_add(1, Ordering::SeqCst) < 2 {
                        Err("transient".into())
                    } else {
                        Ok(note)
                    }
                })
            })
        };
        let sched = Arc::new(TaskScheduler::new(exec, None));
        let nodes = vec![TaskNode {
            id: "a".into(),
            name: "a".into(),
            deps: vec![],
            retries: 2,
            timeout_secs: None,
            steps: vec![step_ok("ok")],
        }];
        let tid = sched.submit("test", "i", "t", nodes);
        let st = wait_terminal(&sched, &tid).await;
        assert_eq!(st, TaskStatus::Success);
        assert_eq!(tries.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn cancel_stops_task() {
        let sched = make_sched();
        let nodes = vec![node("a", &[], vec![step_ok("a")]), node("b", &["a"], vec![step_ok("b")])];
        let tid = sched.submit("test", "i", "t", nodes);
        sched.cancel(&tid);
        let st = wait_terminal(&sched, &tid).await;
        assert_eq!(st, TaskStatus::Failed);
    }

    /// 历史任务(进程重启后仅剩 DB 记录)手动重试节点:应从持久化水合后重跑,不再报「任务不存在」
    #[tokio::test]
    async fn rerun_node_hydrates_persisted_history_task() {
        use crate::store::{MemoryBackend, Store};
        let store = Arc::new(Store::from_backend(MemoryBackend::new()));
        // 模拟 create 失败现场:net/master 成功、proxy 失败、verify 被跳过;整任务 failed 已落库
        store.upsert_task("t-create-1", "create", "demo", "failed", 100, Some(100), Some(120));
        store.upsert_node("t-create-1", "net", "创建网络", "[]", "[]", "success", "ok", 1, 1, Some(30), Some(100), Some(105));
        store.upsert_node("t-create-1", "master", "启动主节点", "[\"net\"]", "[]", "success", "ok", 1, 1, Some(60), Some(105), Some(115));
        let steps = serde_json::to_string(&vec![Step::Noop { note: "p-ok".into() }]).unwrap();
        store.upsert_node("t-create-1", "proxy", "启动 newproxy 代理", "[\"master\"]", &steps, "failed", "docker 失败", 3, 1, Some(120), Some(116), Some(120));
        store.upsert_node("t-create-1", "verify", "连通性验证", "[\"proxy\"]", "[]", "skipped", "上游失败", 0, 0, Some(30), None, Some(120));
        let exec: StepExecutor = Arc::new(|step: &Step| {
            let note = match step {
                Step::Noop { note } => note.clone(),
                _ => "?".into(),
            };
            Box::pin(async move { Ok(note) })
        });
        let sched = Arc::new(TaskScheduler::new(exec, Some(store.clone())));
        // 调度器内存为空(等价重启):重试 proxy
        let r = sched.rerun_node("t-create-1", "proxy").await.expect("历史任务节点应可重试成功");
        assert!(r.contains("已重试成功"), "{r}");
        let v = sched.get("t-create-1").expect("水合后任务可见");
        let st_of = |id: &str| {
            v["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["id"].as_str() == Some(id))
                .unwrap()["status"]
                .as_str()
                .unwrap()
                .to_string()
        };
        assert_eq!(st_of("proxy"), "success", "proxy 重试后应成功");
        assert_eq!(v["status"], "success", "节点全部成功或跳过时任务应转成功");
        // DB 同步
        let sv = store.task_view("t-create-1").unwrap();
        assert_eq!(sv["status"], "success");
        assert_eq!(
            sv["nodes"].as_array().unwrap().iter().find(|n| n["id"].as_str() == Some("proxy")).unwrap()["status"],
            "success"
        );
        // 真正不存在的任务 → 清晰错误
        let e = sched.rerun_node("t-ghost", "proxy").await.unwrap_err();
        assert!(e.contains("不存在"), "{e}");
    }

    /// M0-5 续跑:崩溃后只重跑 pending/running 节点,已完成节点不重跑、输出不覆盖
    #[tokio::test]
    async fn resume_reruns_only_pending_nodes() {
        use crate::store::{MemoryBackend, Store};
        let store = Arc::new(Store::from_backend(MemoryBackend::new()));
        // 模拟崩溃瞬间的持久化现场:任务 running,节点 a 已完成,节点 b 未完成(依赖 a)
        store.upsert_task("t-create-1", "create", "demo", "running", 100, Some(100), None);
        store.upsert_node(
            "t-create-1", "a", "A", "[]", "[]", "success", "a-output", 1, 1, Some(30), Some(100),
            Some(100),
        );
        store.upsert_node(
            "t-create-1", "b", "B", "[\"a\"]", "[{\"Noop\":{\"note\":\"b-run\"}}]", "pending", "", 0,
            1, Some(30), None, None,
        );
        let exec: StepExecutor = Arc::new(|step: &Step| {
            let note = match step {
                Step::Noop { note } => note.clone(),
                _ => "?".into(),
            };
            Box::pin(async move { Ok(note) })
        });
        let sched = Arc::new(TaskScheduler::new(exec, Some(store.clone())));
        let resumed = sched.resume_pending();
        assert_eq!(resumed, 1, "应恢复 1 个未完成任务");
        let st = wait_terminal(&sched, "t-create-1").await;
        assert_eq!(st, TaskStatus::Success);
        let v = sched.get("t-create-1").expect("任务可见");
        let node = |id: &str| {
            v["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["id"].as_str() == Some(id))
                .unwrap()
                .clone()
        };
        // a 已完成:不被重跑,原输出保留
        assert_eq!(node("a")["status"], "success");
        assert_eq!(node("a")["output"], "a-output");
        // b 被重新执行成功
        assert_eq!(node("b")["status"], "success");
        // 存储层 a 的记录同样未被覆盖
        let sv = store.task_view("t-create-1").unwrap();
        let saved_a = sv["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["id"].as_str() == Some("a"))
            .unwrap();
        assert_eq!(saved_a["output"], "a-output");
        // 终态后不再有可续跑任务
        assert_eq!(sched.resume_pending(), 0);
    }
}
