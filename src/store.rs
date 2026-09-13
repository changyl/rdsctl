// rdsctl — 持久化层(v2:backend trait 化,按 docs/scaling-design.md M0)
//
// 存储语义:任务/任务节点/审计日志/实例记录;启动恢复(interrupted→failed/续跑选项见 M0-5)。
//
// 结构:
//   StoreBackend trait —— 后端抽象(供控制面/调度/实例层调用,调用点零改动)
//   Store             —— 包装 Arc<dyn StoreBackend>,对外提供既有同步方法(委托)
//   MysqlBackend      —— 默认后端:本机 MySQL,经 mysql CLI 执行(与 docker.rs 一致,
//                        零外部 crate、离线可构建)。每条语句独立连接,适合 lab 规模。
//   MemoryBackend     —— 内存后端(测试 / 合成压测 / 无 MySQL 环境),语义与 MySQL 一致。
//
// 表(v1,MySQL):
//   tasks / task_nodes / audit_log / instances / task_seq —— 见 DDL。
// 连接 env:RDSCTL_MYSQL_HOST/PORT/USER/PASS/DB/CLI(默认 127.0.0.1:3306 root/rdsctl)。

use std::collections::HashMap;
use std::process::Command;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

/// RBAC 权限目录(group 前缀 = 前端分组;* = 全部)
pub const PERMISSIONS: &[&str] = &[
    "instances.view",
    "instances.create",
    "instances.destroy",
    "instances.scaleout",
    "instances.manage",      // 启停 / 批量
    "instances.query",       // SQL 查询台:只读语句(低权账号执行,全审计)
    "instances.query.write", // SQL 查询台:写语句(需在只读权限之上显式授予;仍强制 master + 全审计)
    "tasks.view",
    "tasks.cancel",
    "tasks.manage", // 草稿创建/编辑/启动 + 删除归档
    "audit.view",
    "alerts.view",
    "alerts.handle",
    "users.manage",
    "roles.manage",
    "cluster.view",   // 管控集群:成员/角色/term/复制进度/租约台账(只读)
    "cluster.manage", // 管控集群:主动让位 / 重建 sink 投影(控制面运维动作)
];

fn rbac_default_creds() -> (String, String) {
    (
        std::env::var("RDSCTL_USER").unwrap_or_else(|_| "admin".into()),
        std::env::var("RDSCTL_PASS").unwrap_or_else(|_| "admin".into()),
    )
}

// ═════════════════════════ 后端抽象 ═════════════════════════

/// 持久化后端(M0 解耦点;新增方法须三处同步: trait / Mysql / Memory)
pub trait StoreBackend: Send + Sync {
    fn next_task_seq(&self, kind: &str) -> u64;

    fn upsert_task(
        &self,
        id: &str,
        kind: &str,
        instance: &str,
        status: &str,
        created: u64,
        started: Option<u64>,
        finished: Option<u64>,
    );
    fn update_task_status(&self, id: &str, status: &str, finished: Option<u64>);
    fn upsert_node(
        &self,
        task_id: &str,
        node_id: &str,
        name: &str,
        deps_json: &str,
        steps_json: &str,
        status: &str,
        output: &str,
        attempts: u32,
        retries: u32,
        timeout_secs: Option<u64>,
        started: Option<u64>,
        finished: Option<u64>,
    );
    fn mark_interrupted(&self) -> usize;

    fn task_views(&self, limit: usize) -> Vec<Value>;
    fn task_views_by_instance(&self, instance: &str, limit: usize) -> Vec<Value>;
    fn task_view(&self, id: &str) -> Option<Value>;
    /// 删除任务(级联节点);仅允许终态(由调用方保证)
    fn delete_task(&self, id: &str) -> Result<(), String>;
    /// 未终态(pending/running)任务及其全部节点定义(续跑还原,含步骤/重试/超时)
    fn pending_task_definitions(&self) -> Vec<Value>;
    /// 单个任务(任意状态)及其全部节点定义(历史/重启后任务「手动重试节点」水合用)。
    /// 形状与 pending_task_definitions 单条一致,并额外带任务级 status/started_at/finished_at。
    fn task_definition(&self, id: &str) -> Option<Value>;

    fn audit(
        &self,
        user: &str,
        instance: &str,
        action: &str,
        params: &str,
        result: &str,
        task_id: &str,
    );
    /// 审计列表(新→旧);支持 instance/action/关键词 过滤(M0 运维视图)
    fn audit_list(
        &self,
        limit: usize,
        instance: Option<&str>,
        action: Option<&str>,
        q: Option<&str>,
    ) -> Vec<Value>;

    fn instance_upsert(
        &self,
        name: &str,
        region: &str,
        tenant: &str,
        data: &str,
        status: &str,
        updated_at: u64,
    );
    fn instance_load_all(&self) -> Vec<(String, String)>;
    /// 彻底删除实例持久化记录(仅「已销毁」实例调用;任务/审计留痕保留)
    fn instance_delete(&self, name: &str);

    // ─── 实例 lease 分布式锁(M0:跨进程互斥;见 docs/scaling-design.md §6) ───

    /// 尝试获取实例操作锁(lease 秒);成功返回 true。同一 holder 重入视为成功并续期。
    fn lock_instance(&self, name: &str, holder: &str, lease_secs: u64) -> bool;
    /// 续约:仅持有者可续;仍持有时返回 true
    fn renew_instance_lock(&self, name: &str, holder: &str, lease_secs: u64) -> bool;
    /// 释放:仅持有者可释放
    fn unlock_instance(&self, name: &str, holder: &str);
    /// 是否已存在带该标记的审计行(sink 投影幂等判定;设计 §12)
    fn audit_marker_exists(&self, marker: &str) -> bool;
    /// 清空全部实例锁(单控制端 lab 模型:非续跑模式启动恢复时调用,
    /// 清理宕机控制端的孤儿 lease;多活模式由 M1 leader 只清持有者过期行)
    fn clear_all_locks(&self);

    // ─── RBAC(S-安全) ───

    /// 无用户时幂等种入默认 admin(RDSCTL_USER/RDSCTL_PASS,默认 admin/admin)+ super 角色
    fn rbac_seed_default_admin(&self);
    /// 校验口令;命中返回 (enabled, 有效权限)。用户不存在返回 None
    fn auth_effective(&self, user: &str, pass: &str) -> Option<(bool, Vec<String>)>;
    /// 用户是否启用(每请求校验,冻结即时生效)
    fn user_enabled(&self, user: &str) -> bool;
    /// 用户列表(含角色与有效权限,不含口令)
    fn users_list(&self) -> Vec<Value>;
    /// **原始**用户记录(含 salt/pass_hash;仅供 cluster 模式首次把 RBAC 灌入共识状态机)。
    /// 绝不经任何 HTTP 响应泄露:接口层从不调用它。
    fn users_raw(&self) -> Vec<Value>;
    /// 创建/更新用户(更新时 salt/pass 覆盖)
    fn user_upsert(&self, user: &str, salt: &str, pass_hash: &str, enabled: bool);
    fn user_set_enabled(&self, user: &str, enabled: bool);
    /// 覆盖绑定用户-角色
    fn user_roles_set(&self, user: &str, roles: &[&str]);
    /// 角色列表(含权限)
    fn roles_list(&self) -> Vec<Value>;
    fn role_upsert(&self, role: &str, desc: &str);
    /// 覆盖角色权限
    fn role_perms_set(&self, role: &str, perms: &[&str]);

    // ─── 告警(S-告警) ───

    /// 打开发告警:同实例同 kind 已有未决(open/ack)则不重复
    fn alert_open(&self, instance: &str, kind: &str, severity: &str, message: &str);
    /// 实例恢复/销毁:关闭该实例全部未决告警
    fn alert_resolve_instance(&self, instance: &str);
    /// 告警列表(新→旧;severity/status/instance 过滤,status 取 open/ack/resolved)
    fn alert_list(
        &self,
        limit: usize,
        severity: Option<&str>,
        status: Option<&str>,
        instance: Option<&str>,
    ) -> Vec<Value>;
    /// 处置:a=ack 认领(resolved_at 记为 handled),r=resolve 解决;assignee=操作人
    fn alert_action(&self, id: u64, action: &str, assignee: &str) -> bool;
    /// 未决告警按严重度计数(供 Dashboard)
    fn alert_counts(&self) -> (u64, u64, u64); // (critical, warn, info) 仅 open/ack

    // ─── 用户功能模块(页面可新建的“真实步骤模块”,见 docs/func-modules.md) ───

    /// 保存/更新模块(name 唯一;同名覆盖=新版本)
    fn module_upsert(
        &self,
        name: &str,
        category: &str,
        desc: &str,
        steps_json: &str,
        created_by: &str,
        created_at: u64,
    );
    /// 全部模块(可按创建时间倒序)
    fn module_list(&self) -> Vec<Value>;
    /// 删除模块;返回是否命中
    fn module_delete(&self, name: &str) -> bool;

    // ─── DTS 链路注册表(canal;dts-design §3;一从节点一条,instance+node 唯一) ───

    /// 全部/某实例的 DTS 链路(按 updated_at 倒序)
    fn dts_list(&self, instance: Option<&str>) -> Vec<Value>;
    /// 保存/更新 DTS 链路(instance+node 唯一;覆盖 = 状态更新)
    fn dts_upsert(
        &self,
        instance: &str,
        node: &str,
        engine: &str,
        container: &str,
        target_label: &str,
        status: &str,
        last_error: &str,
        spec: &str,
    );
    /// 删除 DTS 链路;返回是否命中
    fn dts_remove(&self, instance: &str, node: &str) -> bool;

    // ─── 物理机(Host)注册表(物理混部/跨机运维,见 docs/physical-multi-site-ops.md §5) ───

    /// 登记/更新一台物理机(name 唯一;更新不改端口高水位)。status: running|maintenance|retiring;
    /// agent_port: 该机远端执行 agent 监听端口(0 = 未接入 agent,绑定节点标记 agent 未接入)
    fn host_upsert(
        &self,
        name: &str,
        ip: &str,
        region: &str,
        az: &str,
        rack: &str,
        cpu_cores: u32,
        mem_gb: u32,
        disk_gb: u32,
        agent_port: u16,
        status: &str,
    );
    /// 机器清单(按 name 排序;含 next_port 等运维字段)
    fn host_list(&self) -> Vec<Value>;
    /// 删除机器;返回是否命中(调用方须先校验无节点绑定)
    fn host_delete(&self, name: &str) -> bool;
    /// 按 Host 端口高水位分配:next_port 自增并返回新值;Host 不存在返回 None
    fn host_alloc_port(&self, name: &str) -> Option<u64>;

    // ─── 审计时间过滤(AI-0 报告/统计用) ───

    /// 审计(新→旧,ts≥since;报告/统计用)
    fn audit_since(&self, since: u64, limit: usize) -> Vec<Value>;

    // ─── 证据快照(AI-0 insights;见 docs/ai0-impl-checklist.md §1) ───

    fn evidence_insert(&self, instance: &str, kind: &str, reason: &str, facts_json: &str);
    /// 某实例最新 n 条(新→旧)
    fn evidence_latest(&self, instance: &str, n: usize) -> Vec<Value>;
    /// ts≥since(可选 kind 过滤)全部(新→旧;聚类/报告用)
    fn evidence_since(&self, kind: Option<&str>, since_ts: u64, limit: usize) -> Vec<Value>;

    // ─── SQL 查询台审计(dba-console-design §5.5/§8)───

    /// 记录一次查询:完整 SQL 原文入专用表(结果行永不入库);ok/err 为摘要
    fn query_audit_insert(
        &self,
        user: &str,
        instance: &str,
        node: &str,
        sql: &str,
        sql_hash: &str,
        read_only: bool,
        rows_returned: u64,
        rows_truncated: bool,
        elapsed_ms: u64,
        ok: bool,
        err_summary: &str,
    );
    /// 查询审计列表(新→旧;instance/user/since 过滤;审计页/报告用)
    fn query_audit_list(
        &self,
        limit: usize,
        instance: Option<&str>,
        user: Option<&str>,
        since: Option<u64>,
    ) -> Vec<Value>;

    // ─── 全局慢查治理(slow-query-design §5;差分快照/基线/治理队列)───

    /// 写一轮差分快照(采样 ts 统一取当前秒;结果行从不含字面量的 digest_text)
    fn slow_snapshots_insert(&self, samples: &[SlowSample]);
    /// 全部基线(instance,node,digest → count/sum);空库返回 []
    fn slow_baselines_load(&self) -> Vec<SlowBase>;
    /// 覆盖某 (instance,node) 的全部基线(先删后插,幂等)
    fn slow_baselines_set(&self, instance: &str, node: &str, rows: &[SlowBase]);
    /// 窗口采样(新→旧;instance/digest 可选过滤;供 Rust 侧全局聚合/趋势)
    fn slow_window(
        &self,
        since_ts: u64,
        instance: Option<&str>,
        digest: Option<&str>,
        limit: usize,
    ) -> Vec<Value>;
    /// 治理候选入队:同 digest 已有 open/ack 则不重复;返回是否新建
    fn slow_gov_ensure(&self, digest: &str, digest_text: &str) -> bool;
    fn slow_gov_list(&self, limit: usize, status: Option<&str>) -> Vec<Value>;
    /// ack/resolve;assignee=操作人;返回是否命中并更新
    fn slow_gov_action(&self, id: u64, action: &str, assignee: &str) -> bool;
    /// 保留清理:快照 before_snap;已解决治理项 before_gov。返回 (快照数, 治理数)
    fn slow_prune(&self, before_snap: u64, before_gov: u64) -> (usize, usize);
    /// 写/刷新治理项建议(dba-ai-design §4;命中 open/ack 同 digest 行即更新,
    /// 刷新 updated_at 防刷写语义与 evidence 一致);返回是否命中
    fn slow_gov_advise(&self, digest: &str, advice_json: &str) -> bool;

    // ─── 备份注册联动(backup-link-design §5;outbox 持久化)───

    /// 入队(幂等:同 idempotency_key 已存在返回 false,不重复投递)
    fn backup_outbox_enqueue(
        &self,
        event: &str,
        instance: &str,
        node: &str,
        idempotency_key: &str,
        payload_json: &str,
    ) -> bool;
    /// 领取可投递项(pending/delivering 且 next_at<=now;置 delivering)LIMIT=limit,新→旧
    fn backup_outbox_poll(&self, limit: usize, now: u64) -> Vec<Value>;
    /// 结果回写(id/state/attempts/next_at/last_error;更新 updated_at)
    fn backup_outbox_mark(
        &self,
        id: u64,
        state: &str,
        attempts: u32,
        next_at: u64,
        last_error: &str,
    );
    /// 注册状态派生视图:按 (instance,node) 最近终态 done 事件(register 系列=registered;
    /// deregister 系列=deregistered;否则 unknown)
    fn backup_outbox_reg_state(&self, instance: Option<&str>) -> Vec<Value>;
    /// 人工重投:dead → pending(attempts/error 清零)
    fn backup_outbox_requeue(&self, id: u64) -> bool;
    /// 完成态清理(done/dead 早于 before_ts);返回删除行数
    fn backup_outbox_prune(&self, before_ts: u64) -> usize;

    // ─── 容量采样(dba-ai-design §6/§9;采样 ticker 写,外推/水位读)───

    /// 写一批采样(ts 由后端统一取当前秒;与 slow_snapshots_insert 同风格)
    fn capacity_insert(&self, samples: &[CapSample]);
    /// 某实例(或全部)自 since 起的采样(按 ts 升序;外推需时序;limit 截断)
    fn capacity_since(&self, instance: Option<&str>, since_ts: u64, limit: usize) -> Vec<Value>;
    /// 保留清理:删除 ts < before_ts 的采样;返回删除行数
    fn capacity_prune(&self, before_ts: u64) -> usize;

    // ─── 报告归档(dba-ai-design §8;定时报告落库 + 历史)───

    /// 归档一份报告(period=today|week;type=daily|weekly;ts=now)
    fn report_insert(&self, period: &str, rtype: &str, text: &str, counts_json: &str);
    /// 报告历史(新→旧;rtype 可选过滤;ts >= since)
    fn reports_list(&self, rtype: Option<&str>, since_ts: u64, limit: usize) -> Vec<Value>;
    /// 保留清理:删除 ts < before_ts;返回删除行数
    fn reports_prune(&self, before_ts: u64) -> usize;
}

/// 单条慢查差分快照(采集器产出;字段与 slow_digest_snapshots 列对应)
#[derive(Debug, Clone)]
pub struct SlowSample {
    pub instance: String,
    pub node: String,
    pub schema_name: String,
    pub digest: String,
    pub digest_text: String,
    pub count_star: u64,
    pub sum_ms: u64,
    pub avg_ms: u64,
    pub max_ms: u64,
    pub first_seen: u64,
    pub last_seen: u64,
}

/// 差分基线(累计值;与 slow_baselines 行对应)
#[derive(Debug, Clone)]
pub struct SlowBase {
    pub instance: String,
    pub node: String,
    pub digest: String,
    pub count_star: u64,
    pub sum_ms: u64,
    pub seen_at: u64,
}

/// 单条容量采样(容量 ticker 产出;对应 capacity_samples 行,ts 由后端写入)
#[derive(Debug, Clone)]
pub struct CapSample {
    pub instance: String,
    pub node: String,
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub data_gib: f64,
}

/// 对调用方暴露的持久化句柄(既有代码保持 `Arc<Store>` 用法不变)
pub struct Store {
    backend: Arc<dyn StoreBackend>,
}

impl Store {
    /// 默认后端:MySQL(连接失败直接 panic,持久化不可用无法启动)。
    /// 设 RDSCTL_STORE_BACKEND=memory 可用内存后端(无 MySQL 的 lab/合成压测)。
    pub fn open() -> Self {
        match std::env::var("RDSCTL_STORE_BACKEND").as_deref() {
            Ok("memory") => {
                tracing::info!("store: memory backend(进程内,不持久)");
                Store::from_backend(MemoryBackend::new())
            }
            _ => Store::from_backend(Arc::new(MysqlBackend::connect())),
        }
    }

    /// 注入自定义后端(测试 / 内存 / 未来连接池实现)
    pub fn from_backend(backend: Arc<dyn StoreBackend>) -> Self {
        Store { backend }
    }

    pub fn backend(&self) -> &dyn StoreBackend {
        &*self.backend
    }

    pub fn next_task_seq(&self, kind: &str) -> u64 {
        self.backend.next_task_seq(kind)
    }
    pub fn upsert_task(
        &self,
        id: &str,
        kind: &str,
        instance: &str,
        status: &str,
        created: u64,
        started: Option<u64>,
        finished: Option<u64>,
    ) {
        self.backend
            .upsert_task(id, kind, instance, status, created, started, finished)
    }
    pub fn update_task_status(&self, id: &str, status: &str, finished: Option<u64>) {
        self.backend.update_task_status(id, status, finished)
    }
    pub fn upsert_node(
        &self,
        task_id: &str,
        node_id: &str,
        name: &str,
        deps_json: &str,
        steps_json: &str,
        status: &str,
        output: &str,
        attempts: u32,
        retries: u32,
        timeout_secs: Option<u64>,
        started: Option<u64>,
        finished: Option<u64>,
    ) {
        self.backend.upsert_node(
            task_id,
            node_id,
            name,
            deps_json,
            steps_json,
            status,
            output,
            attempts,
            retries,
            timeout_secs,
            started,
            finished,
        )
    }
    pub fn mark_interrupted(&self) -> usize {
        self.backend.mark_interrupted()
    }
    pub fn task_views(&self, limit: usize) -> Vec<Value> {
        self.backend.task_views(limit)
    }
    pub fn task_views_by_instance(&self, instance: &str, limit: usize) -> Vec<Value> {
        self.backend.task_views_by_instance(instance, limit)
    }
    pub fn task_view(&self, id: &str) -> Option<Value> {
        self.backend.task_view(id)
    }
    pub fn delete_task(&self, id: &str) -> Result<(), String> {
        self.backend.delete_task(id)
    }
    pub fn pending_task_definitions(&self) -> Vec<Value> {
        self.backend.pending_task_definitions()
    }
    pub fn task_definition(&self, id: &str) -> Option<Value> {
        self.backend.task_definition(id)
    }
    pub fn audit(
        &self,
        user: &str,
        instance: &str,
        action: &str,
        params: &str,
        result: &str,
        task_id: &str,
    ) {
        self.backend
            .audit(user, instance, action, params, result, task_id)
    }
    pub fn audit_list(
        &self,
        limit: usize,
        instance: Option<&str>,
        action: Option<&str>,
        q: Option<&str>,
    ) -> Vec<Value> {
        self.backend.audit_list(limit, instance, action, q)
    }
    pub fn instance_upsert(
        &self,
        name: &str,
        region: &str,
        tenant: &str,
        data: &str,
        status: &str,
        updated_at: u64,
    ) {
        self.backend
            .instance_upsert(name, region, tenant, data, status, updated_at)
    }
    pub fn instance_load_all(&self) -> Vec<(String, String)> {
        self.backend.instance_load_all()
    }
    pub fn instance_delete(&self, name: &str) {
        self.backend.instance_delete(name)
    }
    pub fn lock_instance(&self, name: &str, holder: &str, lease_secs: u64) -> bool {
        self.backend.lock_instance(name, holder, lease_secs)
    }
    pub fn renew_instance_lock(&self, name: &str, holder: &str, lease_secs: u64) -> bool {
        self.backend.renew_instance_lock(name, holder, lease_secs)
    }
    pub fn unlock_instance(&self, name: &str, holder: &str) {
        self.backend.unlock_instance(name, holder)
    }
    pub fn audit_marker_exists(&self, marker: &str) -> bool {
        self.backend.audit_marker_exists(marker)
    }
    pub fn clear_all_locks(&self) {
        self.backend.clear_all_locks()
    }
    pub fn rbac_seed_default_admin(&self) {
        self.backend.rbac_seed_default_admin()
    }
    pub fn auth_effective(&self, user: &str, pass: &str) -> Option<(bool, Vec<String>)> {
        self.backend.auth_effective(user, pass)
    }
    pub fn user_enabled(&self, user: &str) -> bool {
        self.backend.user_enabled(user)
    }
    pub fn users_list(&self) -> Vec<Value> {
        self.backend.users_list()
    }
    /// 原始用户记录(含口令摘要)—— 仅 main.rs 的 cluster 引导灌入使用
    pub fn users_raw(&self) -> Vec<Value> {
        self.backend.users_raw()
    }
    pub fn user_upsert(&self, user: &str, salt: &str, pass_hash: &str, enabled: bool) {
        self.backend.user_upsert(user, salt, pass_hash, enabled)
    }
    pub fn user_set_enabled(&self, user: &str, enabled: bool) {
        self.backend.user_set_enabled(user, enabled)
    }
    pub fn user_roles_set(&self, user: &str, roles: &[&str]) {
        self.backend.user_roles_set(user, roles)
    }
    pub fn roles_list(&self) -> Vec<Value> {
        self.backend.roles_list()
    }
    pub fn role_upsert(&self, role: &str, desc: &str) {
        self.backend.role_upsert(role, desc)
    }
    pub fn role_perms_set(&self, role: &str, perms: &[&str]) {
        self.backend.role_perms_set(role, perms)
    }
    pub fn alert_open(&self, instance: &str, kind: &str, severity: &str, message: &str) {
        self.backend.alert_open(instance, kind, severity, message)
    }
    pub fn alert_resolve_instance(&self, instance: &str) {
        self.backend.alert_resolve_instance(instance)
    }
    pub fn alert_list(
        &self,
        limit: usize,
        severity: Option<&str>,
        status: Option<&str>,
        instance: Option<&str>,
    ) -> Vec<Value> {
        self.backend.alert_list(limit, severity, status, instance)
    }
    pub fn alert_action(&self, id: u64, action: &str, assignee: &str) -> bool {
        self.backend.alert_action(id, action, assignee)
    }
    pub fn alert_counts(&self) -> (u64, u64, u64) {
        self.backend.alert_counts()
    }
    pub fn module_upsert(
        &self,
        name: &str,
        category: &str,
        desc: &str,
        steps_json: &str,
        created_by: &str,
        created_at: u64,
    ) {
        self.backend
            .module_upsert(name, category, desc, steps_json, created_by, created_at)
    }
    pub fn module_list(&self) -> Vec<Value> {
        self.backend.module_list()
    }
    pub fn module_delete(&self, name: &str) -> bool {
        self.backend.module_delete(name)
    }
    pub fn dts_list(&self, instance: Option<&str>) -> Vec<Value> {
        self.backend.dts_list(instance)
    }
    pub fn dts_upsert(
        &self,
        instance: &str,
        node: &str,
        engine: &str,
        container: &str,
        target_label: &str,
        status: &str,
        last_error: &str,
        spec: &str,
    ) {
        self.backend.dts_upsert(
            instance,
            node,
            engine,
            container,
            target_label,
            status,
            last_error,
            spec,
        )
    }
    pub fn dts_remove(&self, instance: &str, node: &str) -> bool {
        self.backend.dts_remove(instance, node)
    }
    pub fn host_upsert(
        &self,
        name: &str,
        ip: &str,
        region: &str,
        az: &str,
        rack: &str,
        cpu_cores: u32,
        mem_gb: u32,
        disk_gb: u32,
        agent_port: u16,
        status: &str,
    ) {
        self.backend.host_upsert(
            name, ip, region, az, rack, cpu_cores, mem_gb, disk_gb, agent_port, status,
        )
    }
    pub fn host_list(&self) -> Vec<Value> {
        self.backend.host_list()
    }
    pub fn host_delete(&self, name: &str) -> bool {
        self.backend.host_delete(name)
    }
    pub fn host_alloc_port(&self, name: &str) -> Option<u64> {
        self.backend.host_alloc_port(name)
    }
    pub fn audit_since(&self, since: u64, limit: usize) -> Vec<Value> {
        self.backend.audit_since(since, limit)
    }
    pub fn evidence_insert(&self, instance: &str, kind: &str, reason: &str, facts_json: &str) {
        self.backend
            .evidence_insert(instance, kind, reason, facts_json)
    }
    pub fn evidence_latest(&self, instance: &str, n: usize) -> Vec<Value> {
        self.backend.evidence_latest(instance, n)
    }
    pub fn evidence_since(&self, kind: Option<&str>, since_ts: u64, limit: usize) -> Vec<Value> {
        self.backend.evidence_since(kind, since_ts, limit)
    }
    pub fn query_audit_insert(
        &self,
        user: &str,
        instance: &str,
        node: &str,
        sql: &str,
        sql_hash: &str,
        read_only: bool,
        rows_returned: u64,
        rows_truncated: bool,
        elapsed_ms: u64,
        ok: bool,
        err_summary: &str,
    ) {
        self.backend.query_audit_insert(
            user,
            instance,
            node,
            sql,
            sql_hash,
            read_only,
            rows_returned,
            rows_truncated,
            elapsed_ms,
            ok,
            err_summary,
        )
    }
    pub fn query_audit_list(
        &self,
        limit: usize,
        instance: Option<&str>,
        user: Option<&str>,
        since: Option<u64>,
    ) -> Vec<Value> {
        self.backend.query_audit_list(limit, instance, user, since)
    }
    pub fn slow_snapshots_insert(&self, samples: &[SlowSample]) {
        self.backend.slow_snapshots_insert(samples)
    }
    pub fn slow_baselines_load(&self) -> Vec<SlowBase> {
        self.backend.slow_baselines_load()
    }
    pub fn slow_baselines_set(&self, instance: &str, node: &str, rows: &[SlowBase]) {
        self.backend.slow_baselines_set(instance, node, rows)
    }
    pub fn slow_window(
        &self,
        since_ts: u64,
        instance: Option<&str>,
        digest: Option<&str>,
        limit: usize,
    ) -> Vec<Value> {
        self.backend.slow_window(since_ts, instance, digest, limit)
    }
    pub fn slow_gov_ensure(&self, digest: &str, digest_text: &str) -> bool {
        self.backend.slow_gov_ensure(digest, digest_text)
    }
    pub fn slow_gov_list(&self, limit: usize, status: Option<&str>) -> Vec<Value> {
        self.backend.slow_gov_list(limit, status)
    }
    pub fn slow_gov_action(&self, id: u64, action: &str, assignee: &str) -> bool {
        self.backend.slow_gov_action(id, action, assignee)
    }
    pub fn slow_prune(&self, before_snap: u64, before_gov: u64) -> (usize, usize) {
        self.backend.slow_prune(before_snap, before_gov)
    }
    pub fn slow_gov_advise(&self, digest: &str, advice_json: &str) -> bool {
        self.backend.slow_gov_advise(digest, advice_json)
    }
    pub fn backup_outbox_enqueue(
        &self,
        event: &str,
        instance: &str,
        node: &str,
        idempotency_key: &str,
        payload_json: &str,
    ) -> bool {
        self.backend
            .backup_outbox_enqueue(event, instance, node, idempotency_key, payload_json)
    }
    pub fn backup_outbox_poll(&self, limit: usize, now: u64) -> Vec<Value> {
        self.backend.backup_outbox_poll(limit, now)
    }
    pub fn backup_outbox_mark(
        &self,
        id: u64,
        state: &str,
        attempts: u32,
        next_at: u64,
        last_error: &str,
    ) {
        self.backend
            .backup_outbox_mark(id, state, attempts, next_at, last_error)
    }
    pub fn backup_outbox_reg_state(&self, instance: Option<&str>) -> Vec<Value> {
        self.backend.backup_outbox_reg_state(instance)
    }
    pub fn backup_outbox_requeue(&self, id: u64) -> bool {
        self.backend.backup_outbox_requeue(id)
    }
    pub fn backup_outbox_prune(&self, before_ts: u64) -> usize {
        self.backend.backup_outbox_prune(before_ts)
    }
    pub fn capacity_insert(&self, samples: &[CapSample]) {
        self.backend.capacity_insert(samples)
    }
    pub fn capacity_since(
        &self,
        instance: Option<&str>,
        since_ts: u64,
        limit: usize,
    ) -> Vec<Value> {
        self.backend.capacity_since(instance, since_ts, limit)
    }
    pub fn capacity_prune(&self, before_ts: u64) -> usize {
        self.backend.capacity_prune(before_ts)
    }
    pub fn report_insert(&self, period: &str, rtype: &str, text: &str, counts_json: &str) {
        self.backend.report_insert(period, rtype, text, counts_json)
    }
    pub fn reports_list(&self, rtype: Option<&str>, since_ts: u64, limit: usize) -> Vec<Value> {
        self.backend.reports_list(rtype, since_ts, limit)
    }
    pub fn reports_prune(&self, before_ts: u64) -> usize {
        self.backend.reports_prune(before_ts)
    }
}

// ═════════════════════════ MySQL 后端 ═════════════════════════

struct MysqlBackend {
    cli: String,
    host: String,
    port: String,
    user: String,
    pass: String,
    db: String,
}

impl MysqlBackend {
    /// 连接 MySQL + 确保库表存在;失败直接 panic
    fn connect() -> Self {
        let cli = env_or("RDSCTL_MYSQL_CLI", "mysql");
        let host = env_or("RDSCTL_MYSQL_HOST", "127.0.0.1");
        let port = env_or("RDSCTL_MYSQL_PORT", "3306");
        let user = env_or("RDSCTL_MYSQL_USER", "root");
        let pass = std::env::var("RDSCTL_MYSQL_PASS").unwrap_or_default();
        let db = env_or("RDSCTL_MYSQL_DB", "rdsctl");
        let s = MysqlBackend {
            cli,
            host,
            port,
            user,
            pass,
            db,
        };

        if let Err(e) = s.q0("SELECT 1") {
            panic!(
                "无法连接 MySQL({}): {e}\n\
                 请确认本机 MySQL 已启动,并可用 RDSCTL_MYSQL_HOST/PORT/USER/PASS/DB 指定连接。",
                s.endpoint()
            );
        }
        let _ = s.q_db(&format!(
            "CREATE DATABASE IF NOT EXISTS `{}` CHARACTER SET utf8mb4",
            s.db_ident()
        ));
        s.q0(DDL).unwrap_or_else(|e| {
            panic!("初始化 MySQL 表失败({}): {e}", s.endpoint());
        });
        // 存量库幂等迁移(M0)
        s.migrate_instances_v2();
        s.migrate_task_nodes_v2();
        // DTS 注册表补列:早期 rds_dts 无 spec(INSERT 会失败导致记录无法落库/不展示)
        s.migrate_rds_dts_spec();
        // Host 注册表补列:早期 rds_hosts 无 agent_port(远端执行 agent 端口,缺列 INSERT 失败)
        s.migrate_rds_hosts_agent_port();
        // 慢查治理建议列补列(dba-ai-design §4;早于本功能建库无此列)
        s.migrate_slow_gov_advice();
        // RBAC:无用户时种入默认 admin
        s.rbac_seed_default_admin();
        s
    }

    /// 幂等迁移:rds_hosts 若缺 agent_port 列则 ALTER 补上(早期建表无此列)
    fn migrate_rds_hosts_agent_port(&self) {
        let db = esc(&self.db);
        let has = self
            .q0(&format!(
                "SELECT COUNT(*) FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='rds_hosts' AND COLUMN_NAME='agent_port'"
            ))
            .map(|s| s.trim() == "1")
            .unwrap_or(false);
        if !has {
            let sql =
                "ALTER TABLE rds_hosts ADD COLUMN agent_port INT NOT NULL DEFAULT 0 AFTER disk_gb";
            if let Err(e) = self.q0(sql) {
                tracing::warn!("迁移 rds_hosts.agent_port 失败(表可能不存在,将随 DDL 新建): {e}");
            } else {
                tracing::info!("迁移:rds_hosts 增加列 agent_port");
            }
        }
    }

    /// 幂等迁移:rds_dts 若缺 spec 列则 ALTER 补上(早期建表无此列,缺列会让 dts_upsert 静默失败)
    fn migrate_rds_dts_spec(&self) {
        let db = esc(&self.db);
        let has = self
            .q0(&format!(
                "SELECT COUNT(*) FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='rds_dts' AND COLUMN_NAME='spec'"
            ))
            .map(|s| s.trim() == "1")
            .unwrap_or(false);
        if !has {
            let sql = "ALTER TABLE rds_dts ADD COLUMN spec VARCHAR(48) NOT NULL DEFAULT '' AFTER last_error";
            if let Err(e) = self.q0(sql) {
                tracing::warn!("迁移 rds_dts.spec 失败(表可能不存在,将随 DDL 新建): {e}");
            } else {
                tracing::info!("迁移:rds_dts 增加列 spec");
            }
        }
    }

    /// 幂等迁移:slow_governance 若缺 advice_json 列则 ALTER 补上(dba-ai-design §4/§9)
    fn migrate_slow_gov_advice(&self) {
        let db = esc(&self.db);
        let has = self
            .q0(&format!(
                "SELECT COUNT(*) FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='slow_governance' AND COLUMN_NAME='advice_json'"
            ))
            .map(|s| s.trim() == "1")
            .unwrap_or(false);
        if !has {
            let sql = "ALTER TABLE slow_governance ADD COLUMN advice_json MEDIUMTEXT NULL AFTER resolved_at";
            if let Err(e) = self.q0(sql) {
                tracing::warn!(
                    "迁移 slow_governance.advice_json 失败(表可能不存在,将随 DDL 新建): {e}"
                );
            } else {
                tracing::info!("迁移:slow_governance 增加列 advice_json");
            }
        }
    }

    /// task_nodes 增加 retries / timeout_secs(续跑还原重试参数,M0-5)
    fn migrate_task_nodes_v2(&self) {
        let db = esc(&self.db);
        let cols = [
            ("retries", "INT NOT NULL DEFAULT 0"),
            ("timeout_secs", "BIGINT NULL"),
        ];
        for (name, ddl) in cols {
            let has = self
                .q0(&format!(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='task_nodes' AND COLUMN_NAME='{name}'"
                ))
                .map(|s| s.trim() == "1")
                .unwrap_or(false);
            if !has {
                let sql = format!("ALTER TABLE task_nodes ADD COLUMN {name} {ddl}");
                if let Err(e) = self.q0(&sql) {
                    tracing::warn!("迁移 task_nodes.{name} 失败: {e}");
                } else {
                    tracing::info!("迁移:task_nodes 增加列 {name}");
                }
            }
        }
    }

    /// 幂等列迁移:仅在缺失时 ALTER(MySQL 无 ADD COLUMN IF NOT EXISTS)
    fn migrate_instances_v2(&self) {
        let db = esc(&self.db);
        let cols = [
            ("region", "VARCHAR(64) NOT NULL DEFAULT ''"),
            ("az", "VARCHAR(64) NOT NULL DEFAULT ''"),
            ("shard", "VARCHAR(96) NOT NULL DEFAULT ''"),
            ("tenant", "VARCHAR(96) NOT NULL DEFAULT ''"),
        ];
        for (name, ddl) in cols {
            let has = self
                .q0(&format!(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS \
                     WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='instances' AND COLUMN_NAME='{name}'"
                ))
                .map(|s| s.trim() == "1")
                .unwrap_or(false);
            if !has {
                let sql = format!("ALTER TABLE instances ADD COLUMN {name} {ddl}");
                if let Err(e) = self.q0(&sql) {
                    tracing::warn!("迁移 instances.{name} 失败: {e}");
                } else {
                    tracing::info!("迁移:instances 增加列 {name}");
                }
            }
        }
        for idx in ["idx_instances_region", "idx_instances_tenant"] {
            let has = self
                .q0(&format!(
                    "SELECT COUNT(*) FROM information_schema.STATISTICS \
                     WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='instances' AND INDEX_NAME='{idx}'"
                ))
                .map(|s| s.trim() != "0")
                .unwrap_or(false);
            if !has {
                let sql = match idx {
                    "idx_instances_region" => {
                        "ALTER TABLE instances ADD KEY idx_instances_region (region, status)"
                            .to_string()
                    }
                    _ => "ALTER TABLE instances ADD KEY idx_instances_tenant (tenant)".to_string(),
                };
                if let Err(e) = self.q0(&sql) {
                    tracing::warn!("迁移实例索引 {idx} 失败: {e}");
                }
            }
        }
    }

    fn endpoint(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    fn db_ident(&self) -> String {
        self.db.replace('`', "``")
    }

    /// 执行 SQL(已选库),返回 stdout
    fn q0(&self, sql: &str) -> Result<String, String> {
        let mut cmd = Command::new(&self.cli);
        cmd.args([
            "-h",
            &self.host,
            "-P",
            &self.port,
            "-u",
            &self.user,
            "--protocol=tcp",
            "--batch",
            "--raw",
            "--skip-column-names",
            "--connect-timeout=5",
            "--default-character-set=utf8mb4",
        ]);
        if !self.pass.is_empty() {
            cmd.env("MYSQL_PWD", &self.pass);
        }
        cmd.arg(&self.db).arg("-e").arg(sql);
        let out = cmd
            .output()
            .map_err(|e| format!("mysql 客户端执行失败: {e}"))?;
        let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
            return Err(if stderr.is_empty() { stdout } else { stderr });
        }
        Ok(stdout)
    }

    /// 不选库执行(建库用)
    fn q_db(&self, sql: &str) -> Result<String, String> {
        let mut cmd = Command::new(&self.cli);
        cmd.args([
            "-h",
            &self.host,
            "-P",
            &self.port,
            "-u",
            &self.user,
            "--protocol=tcp",
            "--connect-timeout=5",
        ]);
        if !self.pass.is_empty() {
            cmd.env("MYSQL_PWD", &self.pass);
        }
        cmd.arg("-e").arg(sql);
        let out = cmd
            .output()
            .map_err(|e| format!("mysql 客户端执行失败: {e}"))?;
        if !out.status.success() {
            return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    fn log_err(op: &str, r: &Result<String, String>) {
        if let Err(e) = r {
            tracing::warn!("store::{op} 失败: {e}");
        }
    }

    /// 任务视图 JSON 选择段(含子查询节点聚合)
    const fn task_json_object() -> &'static str {
        "SELECT JSON_OBJECT(\
         'id', t.id, 'kind', t.kind, 'instance', t.instance, 'status', t.status,\
         'created_at', t.created_at, 'started_at', t.started_at, 'finished_at', t.finished_at,\
         'nodes', (SELECT IFNULL(JSON_ARRAYAGG(JSON_OBJECT(\
             'id', n.node_id, 'name', n.name, 'status', n.status, 'output', n.output,\
             'attempts', n.attempts, 'deps', CAST(n.deps_json AS JSON))), JSON_ARRAY())\
             FROM task_nodes n WHERE n.task_id = t.id))"
    }
}

impl MysqlBackend {
    fn roles_of(&self, user: &str) -> Vec<String> {
        self.q0(&format!(
            "SELECT role FROM user_roles WHERE user='{}' ORDER BY role",
            esc(user)
        ))
        .unwrap_or_default()
        .lines()
        .map(|l| l.trim().to_string())
        .collect()
    }

    fn perms_of(&self, user: &str) -> Vec<String> {
        self.q0(&format!(
            "SELECT DISTINCT rp.perm FROM role_perms rp \
             JOIN user_roles ur ON ur.role = rp.role WHERE ur.user='{}' ORDER BY rp.perm",
            esc(user)
        ))
        .unwrap_or_default()
        .lines()
        .map(|l| l.trim().to_string())
        .collect()
    }
}

impl StoreBackend for MysqlBackend {
    fn next_task_seq(&self, kind: &str) -> u64 {
        // 首次插入即经 LAST_INSERT_ID(1) 返回 1(与内存后端一致,避免出现 t-create-0);
        // 重复键时在同一原子语句内推进 seq = LAST_INSERT_ID(seq+1),并发下各自拿到唯一自增号
        let sql = format!(
            "INSERT INTO task_seq(kind, seq) VALUES('{}', LAST_INSERT_ID(1)) \
             ON DUPLICATE KEY UPDATE seq = LAST_INSERT_ID(seq+1); \
             SELECT LAST_INSERT_ID();",
            esc(kind)
        );
        self.q0(&sql)
            .ok()
            .and_then(|s| {
                s.lines()
                    .next()
                    .map(|l| l.trim().parse::<u64>().unwrap_or(0))
            })
            .unwrap_or(0)
    }

    fn upsert_task(
        &self,
        id: &str,
        kind: &str,
        instance: &str,
        status: &str,
        created: u64,
        started: Option<u64>,
        finished: Option<u64>,
    ) {
        let sql = format!(
            "INSERT INTO tasks(id,kind,instance,status,created_at,started_at,finished_at) \
             VALUES('{}','{}','{}','{}',{},{},{}) \
             ON DUPLICATE KEY UPDATE status=VALUES(status), started_at=VALUES(started_at), \
             finished_at=VALUES(finished_at)",
            esc(id),
            esc(kind),
            esc(instance),
            esc(status),
            created,
            opt(started),
            opt(finished)
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("upsert_task", &r);
    }

    fn update_task_status(&self, id: &str, status: &str, finished: Option<u64>) {
        let sql = format!(
            "UPDATE tasks SET status='{}', finished_at={} WHERE id='{}'",
            esc(status),
            opt(finished),
            esc(id)
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("update_task_status", &r);
    }

    fn upsert_node(
        &self,
        task_id: &str,
        node_id: &str,
        name: &str,
        deps_json: &str,
        steps_json: &str,
        status: &str,
        output: &str,
        attempts: u32,
        retries: u32,
        timeout_secs: Option<u64>,
        started: Option<u64>,
        finished: Option<u64>,
    ) {
        let sql = format!(
            "INSERT INTO task_nodes(task_id,node_id,name,deps_json,steps_json,status,output,attempts,retries,timeout_secs,started_at,finished_at) \
             VALUES('{}','{}','{}','{}','{}','{}','{}',{},{},{},{},{}) \
             ON DUPLICATE KEY UPDATE status=VALUES(status), output=VALUES(output), \
             attempts=VALUES(attempts), retries=VALUES(retries), timeout_secs=VALUES(timeout_secs), \
             started_at=VALUES(started_at), finished_at=VALUES(finished_at)",
            esc(task_id),
            esc(node_id),
            esc(name),
            esc(deps_json),
            esc(steps_json),
            esc(status),
            esc(output),
            attempts,
            retries,
            opt(timeout_secs),
            opt(started),
            opt(finished)
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("upsert_node", &r);
    }

    /// 启动恢复:未终态任务 → failed(interrupted),其未完成节点 → skipped
    fn mark_interrupted(&self) -> usize {
        let now = now_secs();
        let sql = format!(
            "UPDATE tasks SET status='failed', finished_at={now} \
             WHERE status IN ('pending','running'); \
             SELECT ROW_COUNT(); \
             UPDATE task_nodes tn JOIN tasks t ON tn.task_id=t.id \
             SET tn.status='skipped', tn.output='进程中断,任务未完成' \
             WHERE t.status='failed' AND tn.status IN ('pending','running');"
        );
        self.q0(&sql)
            .ok()
            .and_then(|s| {
                s.lines()
                    .next()
                    .map(|l| l.trim().parse::<usize>().unwrap_or(0))
            })
            .unwrap_or(0)
    }

    fn task_views(&self, limit: usize) -> Vec<Value> {
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM ({}\
             AS v FROM tasks t ORDER BY t.created_at DESC, t.id DESC LIMIT {}) x",
            Self::task_json_object(),
            limit
        );
        json_vec(&self.q0(&sql))
    }

    fn task_views_by_instance(&self, instance: &str, limit: usize) -> Vec<Value> {
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM ({}\
             AS v FROM tasks t WHERE t.instance='{}' \
             ORDER BY t.created_at DESC, t.id DESC LIMIT {}) x",
            Self::task_json_object(),
            esc(instance),
            limit
        );
        json_vec(&self.q0(&sql))
    }

    fn task_view(&self, id: &str) -> Option<Value> {
        let sql = format!(
            "{}\nAS v FROM tasks t WHERE t.id='{}'",
            Self::task_json_object(),
            esc(id)
        );
        self.q0(&sql).ok().and_then(|s| {
            if s.is_empty() {
                None
            } else {
                serde_json::from_str(&s).ok()
            }
        })
    }

    fn delete_task(&self, id: &str) -> Result<(), String> {
        let _ = self.q0(&format!(
            "DELETE FROM task_nodes WHERE task_id='{}'",
            esc(id)
        ))?;
        let _ = self.q0(&format!("DELETE FROM tasks WHERE id='{}'", esc(id)))?;
        Ok(())
    }

    fn pending_task_definitions(&self) -> Vec<Value> {
        let sql = "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM ( \
            SELECT JSON_OBJECT('id', t.id, 'kind', t.kind, 'instance', t.instance, \
            'created_at', t.created_at, \
            'nodes', (SELECT IFNULL(JSON_ARRAYAGG(JSON_OBJECT( \
                'node_id', n.node_id, 'name', n.name, \
                'deps', CAST(n.deps_json AS JSON), 'steps', CAST(n.steps_json AS JSON), \
                'status', n.status, 'output', n.output, 'attempts', n.attempts, \
                'retries', n.retries, 'timeout_secs', n.timeout_secs)), JSON_ARRAY()) \
                FROM task_nodes n WHERE n.task_id = t.id)) \
            AS v FROM tasks t WHERE t.status IN ('pending','running')) x";
        json_vec(&self.q0(sql))
    }

    fn task_definition(&self, id: &str) -> Option<Value> {
        let sql = format!(
            "SELECT JSON_OBJECT('id', t.id, 'kind', t.kind, 'instance', t.instance, \
             'created_at', t.created_at, 'status', t.status, \
             'started_at', t.started_at, 'finished_at', t.finished_at, \
             'nodes', (SELECT IFNULL(JSON_ARRAYAGG(JSON_OBJECT( \
                 'node_id', n.node_id, 'name', n.name, \
                 'deps', CAST(n.deps_json AS JSON), 'steps', CAST(n.steps_json AS JSON), \
                 'status', n.status, 'output', n.output, 'attempts', n.attempts, \
                 'retries', n.retries, 'timeout_secs', n.timeout_secs)), JSON_ARRAY()) \
                 FROM task_nodes n WHERE n.task_id = t.id)) \
             AS v FROM tasks t WHERE t.id='{}'",
            esc(id)
        );
        self.q0(&sql).ok().and_then(|s| {
            if s.is_empty() {
                None
            } else {
                serde_json::from_str(&s).ok()
            }
        })
    }

    fn audit(
        &self,
        user: &str,
        instance: &str,
        action: &str,
        params: &str,
        result: &str,
        task_id: &str,
    ) {
        let params = truncate(params, 240);
        let result = truncate(result, 240);
        let sql = format!(
            "INSERT INTO audit_log(ts,user,instance,action,params,result,task_id) \
             VALUES({},'{}','{}','{}','{}','{}','{}')",
            now_secs(),
            esc(user),
            esc(instance),
            esc(action),
            esc(&params),
            esc(&result),
            esc(task_id)
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("audit", &r);
    }

    fn audit_marker_exists(&self, marker: &str) -> bool {
        // params 以 "<marker> " 开头(见 ha::projection::marker)
        let sql = format!(
            "SELECT COUNT(*) FROM audit_log WHERE params LIKE '{} %'",
            esc(marker)
        );
        match self.q0(&sql) {
            Ok(s) => s.trim() != "0",
            Err(_) => false,
        }
    }


    fn audit_list(
        &self,
        limit: usize,
        instance: Option<&str>,
        action: Option<&str>,
        q: Option<&str>,
    ) -> Vec<Value> {
        let mut wh = String::new();
        if let Some(v) = instance {
            if !v.is_empty() {
                wh.push_str(&format!(" AND instance LIKE '%{}%'", esc(v)));
            }
        }
        if let Some(v) = action {
            if !v.is_empty() {
                wh.push_str(&format!(" AND action LIKE '%{}%'", esc(v)));
            }
        }
        if let Some(v) = q {
            if !v.is_empty() {
                let kw = esc(v);
                wh.push_str(&format!(
                    " AND (user LIKE '%{kw}%' OR instance LIKE '%{kw}%' OR action LIKE '%{kw}%' \
                     OR params LIKE '%{kw}%' OR result LIKE '%{kw}%' OR task_id LIKE '%{kw}%')"
                ));
            }
        }
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'ts', ts, 'user', user, 'instance', instance, 'action', action,\
             'params', params, 'result', result, 'task_id', task_id) v \
             FROM audit_log WHERE 1=1{wh} ORDER BY id DESC LIMIT {limit}) x",
            wh = wh,
            limit = limit
        );
        json_vec(&self.q0(&sql))
    }

    fn audit_since(&self, since: u64, limit: usize) -> Vec<Value> {
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'ts', ts, 'user', user, 'instance', instance, 'action', action,\
             'params', params, 'result', result, 'task_id', task_id) v \
             FROM audit_log WHERE ts >= {since} ORDER BY id DESC LIMIT {limit}) x",
            since = since,
            limit = limit
        );
        json_vec(&self.q0(&sql))
    }

    fn evidence_insert(&self, instance: &str, kind: &str, reason: &str, facts_json: &str) {
        let sql = format!(
            "INSERT INTO evidence_snapshots(ts, instance, kind, reason, facts_json) \
             VALUES({},'{}','{}','{}','{}')",
            now_secs(),
            esc(instance),
            esc(kind),
            truncate(reason, 240),
            esc(facts_json)
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("evidence_insert", &r);
    }

    fn evidence_latest(&self, instance: &str, n: usize) -> Vec<Value> {
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'ts', ts, 'instance', instance, 'kind', kind, 'reason', reason,\
             'facts', CAST(facts_json AS JSON)) v \
             FROM evidence_snapshots WHERE instance='{}' ORDER BY id DESC LIMIT {}) x",
            esc(instance),
            n
        );
        json_vec(&self.q0(&sql))
    }

    fn evidence_since(&self, kind: Option<&str>, since_ts: u64, limit: usize) -> Vec<Value> {
        let kind_wh = match kind {
            Some(k) if !k.is_empty() => format!(" AND kind='{}'", esc(k)),
            _ => String::new(),
        };
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'ts', ts, 'instance', instance, 'kind', kind, 'reason', reason,\
             'facts', CAST(facts_json AS JSON)) v \
             FROM evidence_snapshots WHERE ts >= {}{} ORDER BY id DESC LIMIT {}) x",
            since_ts, kind_wh, limit
        );
        json_vec(&self.q0(&sql))
    }

    fn query_audit_insert(
        &self,
        user: &str,
        instance: &str,
        node: &str,
        sql: &str,
        sql_hash: &str,
        read_only: bool,
        rows_returned: u64,
        rows_truncated: bool,
        elapsed_ms: u64,
        ok: bool,
        err_summary: &str,
    ) {
        let err_summary = truncate(err_summary, 240);
        let sql_q = format!(
            "INSERT INTO query_audit(ts,user,instance,node,sql_text,sql_hash,read_only,rows_returned,\
             rows_truncated,elapsed_ms,ok,err_summary) \
             VALUES({},'{}','{}','{}','{}','{}',{},{},{},{},{},'{}')",
            now_secs(),
            esc(user),
            esc(instance),
            esc(node),
            esc(sql),
            esc(sql_hash),
            if read_only { 1 } else { 0 },
            rows_returned,
            if rows_truncated { 1 } else { 0 },
            elapsed_ms,
            if ok { 1 } else { 0 },
            esc(&err_summary)
        );
        let r = self.q0(&sql_q);
        MysqlBackend::log_err("query_audit_insert", &r);
    }

    fn query_audit_list(
        &self,
        limit: usize,
        instance: Option<&str>,
        user: Option<&str>,
        since: Option<u64>,
    ) -> Vec<Value> {
        let mut wh = String::new();
        if let Some(v) = instance {
            if !v.is_empty() {
                wh.push_str(&format!(" AND instance LIKE '%{}%'", esc(v)));
            }
        }
        if let Some(v) = user {
            if !v.is_empty() {
                wh.push_str(&format!(" AND user LIKE '%{}%'", esc(v)));
            }
        }
        if let Some(v) = since {
            wh.push_str(&format!(" AND ts >= {v}"));
        }
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'ts', ts, 'user', user, 'instance', instance, 'node', node,\
             'sql', sql_text, 'sql_hash', sql_hash, 'read_only', read_only,\
             'rows_returned', rows_returned, 'rows_truncated', rows_truncated,\
             'elapsed_ms', elapsed_ms, 'ok', ok, 'err_summary', err_summary) v \
             FROM query_audit WHERE 1=1{wh} ORDER BY id DESC LIMIT {limit}) x",
            wh = wh,
            limit = limit
        );
        json_vec(&self.q0(&sql))
    }

    // ─── 全局慢查(MySQL 实现)───

    fn slow_snapshots_insert(&self, samples: &[SlowSample]) {
        if samples.is_empty() {
            return;
        }
        let now = now_secs();
        let mut vals: Vec<String> = Vec::with_capacity(samples.len());
        for s in samples {
            vals.push(format!(
                "({},{},'{}','{}','{}','{}',{},{},{},{},{},{})",
                now,
                esc(&s.instance),
                esc(&s.node),
                esc(&s.schema_name),
                esc(&s.digest),
                esc(&s.digest_text),
                s.count_star,
                s.sum_ms,
                s.avg_ms,
                s.max_ms,
                s.first_seen,
                s.last_seen
            ));
        }
        let sql = format!(
            "INSERT INTO slow_digest_snapshots(ts,instance,node,schema_name,digest,digest_text,\
             count_star,sum_ms,avg_ms,max_ms,first_seen,last_seen) VALUES {}",
            vals.join(",")
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("slow_snapshots_insert", &r);
    }

    fn slow_baselines_load(&self) -> Vec<SlowBase> {
        let Ok(out) =
            self.q0("SELECT instance,node,digest,count_star,sum_ms,seen_at FROM slow_baselines")
        else {
            return Vec::new();
        };
        let mut v = Vec::new();
        for l in out.lines() {
            let mut it = l.split('\t');
            let row = SlowBase {
                instance: it.next().unwrap_or("").to_string(),
                node: it.next().unwrap_or("").to_string(),
                digest: it.next().unwrap_or("").to_string(),
                count_star: it.next().unwrap_or("0").trim().parse().unwrap_or(0),
                sum_ms: it.next().unwrap_or("0").trim().parse().unwrap_or(0),
                seen_at: it.next().unwrap_or("0").trim().parse().unwrap_or(0),
            };
            v.push(row);
        }
        v
    }

    fn slow_baselines_set(&self, instance: &str, node: &str, rows: &[SlowBase]) {
        let _ = self.q0(&format!(
            "DELETE FROM slow_baselines WHERE instance='{}' AND node='{}'",
            esc(instance),
            esc(node)
        ));
        if rows.is_empty() {
            return;
        }
        let mut vals: Vec<String> = Vec::with_capacity(rows.len());
        for b in rows {
            vals.push(format!(
                "('{}','{}','{}',{},{},{})",
                esc(instance),
                esc(node),
                esc(&b.digest),
                b.count_star,
                b.sum_ms,
                b.seen_at
            ));
        }
        let sql = format!(
            "INSERT INTO slow_baselines(instance,node,digest,count_star,sum_ms,seen_at) VALUES {}",
            vals.join(",")
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("slow_baselines_set", &r);
    }

    fn slow_window(
        &self,
        since_ts: u64,
        instance: Option<&str>,
        digest: Option<&str>,
        limit: usize,
    ) -> Vec<Value> {
        let mut wh = format!(" AND ts >= {since_ts}");
        if let Some(v) = instance {
            if !v.is_empty() {
                wh.push_str(&format!(" AND instance='{}'", esc(v)));
            }
        }
        if let Some(v) = digest {
            if !v.is_empty() {
                wh.push_str(&format!(" AND digest='{}'", esc(v)));
            }
        }
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'ts', ts, 'instance', instance, 'node', node, 'schema_name', schema_name,\
             'digest', digest, 'digest_text', digest_text, 'count_star', count_star,\
             'sum_ms', sum_ms, 'avg_ms', avg_ms, 'max_ms', max_ms,\
             'first_seen', first_seen, 'last_seen', last_seen) v \
             FROM slow_digest_snapshots WHERE 1=1{wh} ORDER BY id DESC LIMIT {limit}) x",
            wh = wh,
            limit = limit
        );
        json_vec(&self.q0(&sql))
    }

    fn slow_gov_ensure(&self, digest: &str, digest_text: &str) -> bool {
        let exists = self
            .q0(&format!(
                "SELECT COUNT(*) FROM slow_governance WHERE digest='{}' AND status IN ('open','ack')",
                esc(digest)
            ))
            .map(|s| s.trim() != "0")
            .unwrap_or(false);
        if exists {
            return false;
        }
        let now = now_secs();
        let sql = format!(
            "INSERT INTO slow_governance(digest,digest_text,status,created_at,updated_at) \
             VALUES('{}','{}','open',{now},{now})",
            esc(digest),
            truncate(digest_text, 1024)
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("slow_gov_ensure", &r);
        true
    }

    fn slow_gov_list(&self, limit: usize, status: Option<&str>) -> Vec<Value> {
        let mut wh = String::new();
        if let Some(v) = status {
            if !v.is_empty() {
                wh.push_str(&format!(" AND status='{}'", esc(v)));
            }
        }
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'id', id, 'digest', digest, 'digest_text', digest_text, 'status', status,\
             'assignee', assignee, 'created_at', created_at, 'updated_at', updated_at,\
             'resolved_at', resolved_at,\
             'advice', CAST(IF(advice_json='' OR advice_json IS NULL, 'null', advice_json) AS JSON)) v \
             FROM slow_governance WHERE 1=1{wh} ORDER BY id DESC LIMIT {limit}) x",
            wh = wh,
            limit = limit
        );
        json_vec(&self.q0(&sql))
    }

    fn slow_gov_action(&self, id: u64, action: &str, assignee: &str) -> bool {
        let now = now_secs();
        let (status_col, ts_col) = if action == "resolve" {
            ("'resolved'", "resolved_at")
        } else {
            ("'ack'", "updated_at")
        };
        let q = format!(
            "UPDATE slow_governance SET status={status_col}, assignee='{}', {ts_col}={} \
             WHERE id={} AND status IN ('open','ack')",
            esc(assignee),
            now,
            id
        );
        self.q0(&q).ok().map(|_| true).unwrap_or(false)
    }

    fn slow_prune(&self, before_snap: u64, before_gov: u64) -> (usize, usize) {
        let n1 = self
            .q0(&format!(
                "DELETE FROM slow_digest_snapshots WHERE ts < {before_snap}; SELECT ROW_COUNT();"
            ))
            .ok()
            .and_then(|s| {
                s.lines()
                    .next()
                    .map(|l| l.trim().parse::<usize>().unwrap_or(0))
            })
            .unwrap_or(0);
        let n2 = self
            .q0(&format!(
                "DELETE FROM slow_governance WHERE status='resolved' AND resolved_at IS NOT NULL AND resolved_at < {before_gov}; SELECT ROW_COUNT();"
            ))
            .ok()
            .and_then(|s| s.lines().next().map(|l| l.trim().parse::<usize>().unwrap_or(0)))
            .unwrap_or(0);
        (n1, n2)
    }

    fn slow_gov_advise(&self, digest: &str, advice_json: &str) -> bool {
        let now = now_secs();
        let q = format!(
            "UPDATE slow_governance SET advice_json='{}', updated_at={} \
             WHERE digest='{}' AND status IN ('open','ack'); SELECT ROW_COUNT();",
            esc(advice_json),
            now,
            esc(digest)
        );
        self.q0(&q)
            .ok()
            .and_then(|s| {
                s.lines()
                    .next()
                    .map(|l| l.trim().parse::<u64>().unwrap_or(0))
            })
            .map(|n| n > 0)
            .unwrap_or(false)
    }

    // ─── 容量采样(MySQL 实现;dba-ai-design §6/§9)───

    fn capacity_insert(&self, samples: &[CapSample]) {
        if samples.is_empty() {
            return;
        }
        let now = now_secs();
        let mut vals: Vec<String> = Vec::with_capacity(samples.len());
        for s in samples {
            vals.push(format!(
                "({},'{}','{}',{},{},{})",
                now,
                esc(&s.instance),
                esc(&s.node),
                s.disk_used_bytes,
                s.disk_total_bytes,
                s.data_gib
            ));
        }
        let sql = format!(
            "INSERT INTO capacity_samples(ts,instance,node,disk_used_bytes,disk_total_bytes,data_gib) \
             VALUES {}",
            vals.join(",")
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("capacity_insert", &r);
    }

    fn capacity_since(&self, instance: Option<&str>, since_ts: u64, limit: usize) -> Vec<Value> {
        let mut wh = format!(" AND ts >= {since_ts}");
        if let Some(v) = instance {
            if !v.is_empty() {
                wh.push_str(&format!(" AND instance='{}'", esc(v)));
            }
        }
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'ts', ts, 'instance', instance, 'node', node,\
             'disk_used_bytes', disk_used_bytes, 'disk_total_bytes', disk_total_bytes,\
             'data_gib', data_gib) v \
             FROM capacity_samples WHERE 1=1{wh} ORDER BY id ASC LIMIT {limit}) x",
            wh = wh,
            limit = limit
        );
        json_vec(&self.q0(&sql))
    }

    fn capacity_prune(&self, before_ts: u64) -> usize {
        self.q0(&format!(
            "DELETE FROM capacity_samples WHERE ts < {before_ts}; SELECT ROW_COUNT();"
        ))
        .ok()
        .and_then(|s| {
            s.lines()
                .next()
                .map(|l| l.trim().parse::<usize>().unwrap_or(0))
        })
        .unwrap_or(0)
    }

    // ─── 报告归档(MySQL 实现;dba-ai-design §8)───

    fn report_insert(&self, period: &str, rtype: &str, text: &str, counts_json: &str) {
        let sql = format!(
            "INSERT INTO reports(ts,period,type,text,counts_json) \
             VALUES({},'{}','{}','{}','{}')",
            now_secs(),
            esc(period),
            esc(rtype),
            esc(text),
            esc(counts_json)
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("report_insert", &r);
    }

    fn reports_list(&self, rtype: Option<&str>, since_ts: u64, limit: usize) -> Vec<Value> {
        let mut wh = format!(" AND ts >= {since_ts}");
        if let Some(v) = rtype {
            if !v.is_empty() {
                wh.push_str(&format!(" AND type='{}'", esc(v)));
            }
        }
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'id', id, 'ts', ts, 'period', period, 'type', type,\
             'text', text, 'counts', CAST(counts_json AS JSON)) v \
             FROM reports WHERE 1=1{wh} ORDER BY id DESC LIMIT {limit}) x",
            wh = wh,
            limit = limit
        );
        json_vec(&self.q0(&sql))
    }

    fn reports_prune(&self, before_ts: u64) -> usize {
        self.q0(&format!(
            "DELETE FROM reports WHERE ts < {before_ts}; SELECT ROW_COUNT();"
        ))
        .ok()
        .and_then(|s| {
            s.lines()
                .next()
                .map(|l| l.trim().parse::<usize>().unwrap_or(0))
        })
        .unwrap_or(0)
    }

    // ─── 备份注册联动(MySQL 实现)───

    fn backup_outbox_enqueue(
        &self,
        event: &str,
        instance: &str,
        node: &str,
        idempotency_key: &str,
        payload_json: &str,
    ) -> bool {
        let now = now_secs();
        let sql = format!(
            "INSERT IGNORE INTO backup_outbox(event,instance,node,idempotency_key,payload_json,\
             state,next_at,created_at,updated_at) VALUES('{}','{}','{}','{}','{}','pending',{},{},{})",
            esc(event),
            esc(instance),
            esc(node),
            esc(idempotency_key),
            esc(payload_json),
            now,
            now,
            now
        );
        self.q0(&sql).ok().map(|_| true).unwrap_or(false)
    }

    fn backup_outbox_poll(&self, limit: usize, now: u64) -> Vec<Value> {
        // 领取即置 delivering(乐观;失败/宕机靠 next_at 到期后重新领取)
        let _ = self.q0(&format!(
            "UPDATE backup_outbox SET state='delivering', updated_at={now} \
             WHERE state IN ('pending','delivering') AND next_at <= {now} \
             ORDER BY id ASC LIMIT {}",
            limit.min(200)
        ));
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'id', id, 'event', event, 'instance', instance, 'node', node,\
             'idempotency_key', idempotency_key, 'payload_json', payload_json,\
             'attempts', attempts, 'state', state) v \
             FROM backup_outbox WHERE state='delivering' AND next_at <= {now} \
             ORDER BY id ASC LIMIT {limit}) x",
            limit = limit.min(200)
        );
        json_vec(&self.q0(&sql))
    }

    fn backup_outbox_mark(
        &self,
        id: u64,
        state: &str,
        attempts: u32,
        next_at: u64,
        last_error: &str,
    ) {
        let sql = format!(
            "UPDATE backup_outbox SET state='{}', attempts={}, next_at={}, last_error='{}', \
             updated_at={} WHERE id={}",
            esc(state),
            attempts,
            next_at,
            esc(last_error),
            now_secs(),
            id
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("backup_outbox_mark", &r);
    }

    fn backup_outbox_reg_state(&self, instance: Option<&str>) -> Vec<Value> {
        let mut wh = String::new();
        if let Some(v) = instance {
            if !v.is_empty() {
                wh.push_str(&format!(" AND instance='{}'", esc(v)));
            }
        }
        // 每 (instance,node) 取最近一条 done 事件,按事件名派生态(列别名 v,勿与表别名混用)
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'instance', t.instance, 'node', t.node, \
             'reg_state', CASE WHEN t.event LIKE 'register%' THEN 'registered' \
                               WHEN t.event LIKE 'deregister%' THEN 'deregistered' \
                               ELSE 'unknown' END, \
             'last_event', t.event, 'last_error', t.last_error, 'updated_at', t.updated_at) v \
             FROM backup_outbox t \
             JOIN (SELECT instance, node, MAX(id) mid FROM backup_outbox \
                   WHERE state='done'{wh} GROUP BY instance, node) x \
               ON t.id = x.mid) q",
            wh = wh
        );
        json_vec(&self.q0(&sql))
    }

    fn backup_outbox_requeue(&self, id: u64) -> bool {
        let sql = format!(
            "UPDATE backup_outbox SET state='pending', attempts=0, next_at={}, last_error='' \
             WHERE id={} AND state='dead'",
            now_secs(),
            id
        );
        self.q0(&sql).ok().map(|_| true).unwrap_or(false)
    }

    fn backup_outbox_prune(&self, before_ts: u64) -> usize {
        self.q0(&format!(
            "DELETE FROM backup_outbox WHERE state IN ('done','dead') AND updated_at < {before_ts}; SELECT ROW_COUNT();"
        ))
        .ok()
        .and_then(|s| s.lines().next().map(|l| l.trim().parse::<usize>().unwrap_or(0)))
        .unwrap_or(0)
    }

    fn instance_upsert(
        &self,
        name: &str,
        region: &str,
        tenant: &str,
        data: &str,
        status: &str,
        updated_at: u64,
    ) {
        let sql = format!(
            "INSERT INTO instances(name, region, tenant, data, status, updated_at) \
             VALUES('{}','{}','{}','{}','{}',{}) \
             ON DUPLICATE KEY UPDATE region=VALUES(region), tenant=VALUES(tenant), \
             data=VALUES(data), status=VALUES(status), updated_at=VALUES(updated_at)",
            esc(name),
            esc(region),
            esc(tenant),
            esc(data),
            esc(status),
            updated_at
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("instance_upsert", &r);
    }

    fn instance_load_all(&self) -> Vec<(String, String)> {
        let sql = "SELECT name, data FROM instances";
        let Ok(out) = self.q0(sql) else {
            return Vec::new();
        };
        out.lines()
            .filter_map(|l| {
                let (name, data) = l.split_once('\t')?;
                Some((name.to_string(), data.to_string()))
            })
            .collect()
    }

    fn instance_delete(&self, name: &str) {
        let sql = format!("DELETE FROM instances WHERE name='{}'", esc(name));
        let r = self.q0(&sql);
        MysqlBackend::log_err("instance_delete", &r);
    }

    fn lock_instance(&self, name: &str, holder: &str, lease_secs: u64) -> bool {
        let now = now_secs();
        let until = now + lease_secs;
        let sql = format!(
            "INSERT INTO instance_locks(name, holder, lease_until, updated_at) \
             VALUES('{}','{}',{},{}) \
             ON DUPLICATE KEY UPDATE \
               holder = IF(holder='{}' OR lease_until <= {}, '{}', holder), \
               lease_until = IF(holder='{}' OR lease_until <= {}, {}, lease_until), \
               updated_at = {}; \
             SELECT IF(holder='{}', 1, 0) FROM instance_locks WHERE name='{}';",
            esc(name),
            esc(holder),
            until,
            now,
            esc(holder),
            now,
            esc(holder),
            esc(holder),
            now,
            until,
            now,
            esc(holder),
            esc(name)
        );
        self.q0(&sql)
            .ok()
            .and_then(|s| s.lines().next().map(|l| l.trim() == "1"))
            .unwrap_or(false)
    }

    fn renew_instance_lock(&self, name: &str, holder: &str, lease_secs: u64) -> bool {
        let now = now_secs();
        let until = now + lease_secs;
        let sql = format!(
            "UPDATE instance_locks SET lease_until={until}, updated_at={now} \
             WHERE name='{}' AND holder='{}'; \
             SELECT ROW_COUNT();",
            esc(name),
            esc(holder)
        );
        self.q0(&sql)
            .ok()
            .and_then(|s| s.lines().next().map(|l| l.trim() == "1"))
            .unwrap_or(false)
    }

    fn unlock_instance(&self, name: &str, holder: &str) {
        let sql = format!(
            "DELETE FROM instance_locks WHERE name='{}' AND holder='{}';",
            esc(name),
            esc(holder)
        );
        let r = self.q0(&sql);
        MysqlBackend::log_err("unlock_instance", &r);
    }

    fn clear_all_locks(&self) {
        let r = self.q0("DELETE FROM instance_locks");
        MysqlBackend::log_err("clear_all_locks", &r);
    }

    // ─── RBAC ───

    fn rbac_seed_default_admin(&self) {
        let (uname, pass) = rbac_default_creds();
        let count = self
            .q0(&format!(
                "SELECT COUNT(*) FROM users WHERE user='{}'",
                esc(&uname)
            ))
            .map(|s| s.trim().parse::<u64>().unwrap_or(0))
            .unwrap_or(0);
        if count > 0 {
            return;
        }
        let salt = crate::sha256::to_hex(&salt_bytes());
        let hash =
            crate::sha256::to_hex(&crate::sha256::digest(format!("{salt}:{pass}").as_bytes()));
        let now = now_secs();
        let q = format!(
            "INSERT INTO users(user,salt,pass_hash,enabled,created_at,updated_at) VALUES('{}','{}','{}',1,{},{})",
            esc(&uname), esc(&salt), hash, now, now
        );
        let _ = self.q0(&q);
        let _ = self.q0(&format!(
            "INSERT INTO roles(name,description,created_at) VALUES('super','超级管理员(全部权限)',{now})"
        ));
        let _ = self.q0(&format!(
            "INSERT INTO user_roles(user,role) VALUES('{}','super')",
            esc(&uname)
        ));
        for perm in crate::store::PERMISSIONS {
            let _ = self.q0(&format!(
                "INSERT IGNORE INTO role_perms(role,perm) VALUES('super','{}')",
                esc(perm)
            ));
        }
        tracing::info!("RBAC 已种入默认管理员: {uname}");
    }

    fn auth_effective(&self, user: &str, pass: &str) -> Option<(bool, Vec<String>)> {
        let row = self
            .q0(&format!(
                "SELECT salt, pass_hash, enabled FROM users WHERE user='{}'",
                esc(user)
            ))
            .ok()?
            .lines()
            .next()
            .map(|l| l.to_string())?;
        let mut it = row.split('\t');
        let salt = it.next()?.to_string();
        let hash = it.next()?.to_string();
        let enabled = it.next()?.trim() != "0";
        let digest =
            crate::sha256::to_hex(&crate::sha256::digest(format!("{salt}:{pass}").as_bytes()));
        if digest != hash {
            return None;
        }
        let roles = self.roles_of(user);
        let perms = if roles.iter().any(|r| r == "super") {
            PERMISSIONS.iter().map(|s| s.to_string()).collect()
        } else {
            self.perms_of(user)
        };
        Some((enabled, perms))
    }

    fn user_enabled(&self, user: &str) -> bool {
        self.q0(&format!(
            "SELECT enabled FROM users WHERE user='{}'",
            esc(user)
        ))
        .ok()
        .and_then(|s| s.lines().next().map(|l| l.trim() != "0"))
        .unwrap_or(false)
    }

    fn users_list(&self) -> Vec<Value> {
        let rows: Vec<(String, bool)> = self
            .q0("SELECT user, enabled FROM users ORDER BY user")
            .unwrap_or_default()
            .lines()
            .filter_map(|l| {
                let (u, e) = l.split_once('\t')?;
                Some((u.to_string(), e.trim() != "0"))
            })
            .collect();
        let mut out = Vec::new();
        for (u, enabled) in rows {
            let roles = self.roles_of(&u);
            let perms = if roles.iter().any(|r| r == "super") {
                PERMISSIONS.iter().map(|s| s.to_string()).collect()
            } else {
                self.perms_of(&u)
            };
            out.push(json!({ "user": u, "enabled": enabled, "roles": roles, "perms": perms }));
        }
        out
    }

    fn users_raw(&self) -> Vec<Value> {
        let rows: Vec<(String, String, String, bool)> = self
            .q0("SELECT user, salt, pass_hash, enabled FROM users ORDER BY user")
            .unwrap_or_default()
            .lines()
            .filter_map(|l| {
                let mut it = l.split('\t');
                let u = it.next()?.to_string();
                let salt = it.next().unwrap_or("").to_string();
                let hash = it.next().unwrap_or("").to_string();
                let en = it.next().unwrap_or("1").trim() != "0";
                Some((u, salt, hash, en))
            })
            .collect();
        rows.into_iter()
            .map(|(u, salt, hash, en)| {
                let roles = self.roles_of(&u);
                json!({ "user": u, "salt": salt, "pass_hash": hash, "enabled": en, "roles": roles })
            })
            .collect()
    }

    fn user_upsert(&self, user: &str, salt: &str, pass_hash: &str, enabled: bool) {
        let now = now_secs();
        let en = if enabled { 1 } else { 0 };
        let q = format!(
            "INSERT INTO users(user,salt,pass_hash,enabled,created_at,updated_at) \
             VALUES('{}','{}','{}',{},{},{}) \
             ON DUPLICATE KEY UPDATE salt=VALUES(salt), pass_hash=VALUES(pass_hash), \
             enabled=VALUES(enabled), updated_at=VALUES(updated_at)",
            esc(user),
            esc(salt),
            pass_hash,
            en,
            now,
            now
        );
        let r = self.q0(&q);
        MysqlBackend::log_err("user_upsert", &r);
    }

    fn user_set_enabled(&self, user: &str, enabled: bool) {
        let en = if enabled { 1 } else { 0 };
        let q = format!(
            "UPDATE users SET enabled={}, updated_at={} WHERE user='{}'",
            en,
            now_secs(),
            esc(user)
        );
        let r = self.q0(&q);
        MysqlBackend::log_err("user_set_enabled", &r);
    }

    fn user_roles_set(&self, user: &str, roles: &[&str]) {
        let _ = self.q0(&format!(
            "DELETE FROM user_roles WHERE user='{}'",
            esc(user)
        ));
        for role in roles {
            let q = format!(
                "INSERT INTO user_roles(user,role) VALUES('{}','{}')",
                esc(user),
                esc(role)
            );
            let r = self.q0(&q);
            MysqlBackend::log_err("user_roles_set", &r);
        }
    }

    fn roles_list(&self) -> Vec<Value> {
        let rows: Vec<(String, String)> = self
            .q0("SELECT name, description FROM roles ORDER BY name")
            .unwrap_or_default()
            .lines()
            .filter_map(|l| {
                let (a, b) = l.split_once('\t')?;
                Some((a.to_string(), b.to_string()))
            })
            .collect();
        let mut out = Vec::new();
        for (name, desc) in rows {
            let perms: Vec<String> = self
                .q0(&format!(
                    "SELECT perm FROM role_perms WHERE role='{}' ORDER BY perm",
                    esc(&name)
                ))
                .unwrap_or_default()
                .lines()
                .map(|l| l.trim().to_string())
                .collect();
            out.push(json!({ "name": name, "description": desc, "perms": perms }));
        }
        out
    }

    fn role_upsert(&self, role: &str, desc: &str) {
        let now = now_secs();
        let q = format!(
            "INSERT INTO roles(name,description,created_at) VALUES('{}','{}',{}) \
             ON DUPLICATE KEY UPDATE description=VALUES(description)",
            esc(role),
            esc(desc),
            now
        );
        let r = self.q0(&q);
        MysqlBackend::log_err("role_upsert", &r);
    }

    fn role_perms_set(&self, role: &str, perms: &[&str]) {
        let _ = self.q0(&format!(
            "DELETE FROM role_perms WHERE role='{}'",
            esc(role)
        ));
        for perm in perms {
            let q = format!(
                "INSERT INTO role_perms(role,perm) VALUES('{}','{}')",
                esc(role),
                esc(perm)
            );
            let r = self.q0(&q);
            MysqlBackend::log_err("role_perms_set", &r);
        }
    }

    // ─── 告警 ───

    fn alert_open(&self, instance: &str, kind: &str, severity: &str, message: &str) {
        let exists = self
            .q0(&format!(
                "SELECT COUNT(*) FROM alerts WHERE instance='{}' AND kind='{}' \
                 AND status IN ('open','ack')",
                esc(instance),
                esc(kind)
            ))
            .map(|s| s.trim() != "0")
            .unwrap_or(false);
        if exists {
            return;
        }
        let q = format!(
            "INSERT INTO alerts(ts,instance,kind,severity,message,status) \
             VALUES({},'{}','{}','{}','{}','open')",
            now_secs(),
            esc(instance),
            esc(kind),
            esc(severity),
            truncate(message, 480)
        );
        let r = self.q0(&q);
        MysqlBackend::log_err("alert_open", &r);
    }

    fn alert_resolve_instance(&self, instance: &str) {
        let q = format!(
            "UPDATE alerts SET status='resolved', resolved_at={} \
             WHERE instance='{}' AND status IN ('open','ack')",
            now_secs(),
            esc(instance)
        );
        let r = self.q0(&q);
        MysqlBackend::log_err("alert_resolve_instance", &r);
    }

    fn alert_list(
        &self,
        limit: usize,
        severity: Option<&str>,
        status: Option<&str>,
        instance: Option<&str>,
    ) -> Vec<Value> {
        let mut wh = String::new();
        if let Some(v) = severity {
            if !v.is_empty() {
                wh.push_str(&format!(" AND severity='{}'", esc(v)));
            }
        }
        if let Some(v) = status {
            if !v.is_empty() {
                wh.push_str(&format!(" AND status='{}'", esc(v)));
            }
        }
        if let Some(v) = instance {
            if !v.is_empty() {
                wh.push_str(&format!(" AND instance LIKE '%{}%'", esc(v)));
            }
        }
        let sql = format!(
            "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'id', id, 'ts', ts, 'instance', instance, 'kind', kind, 'severity', severity,\
             'message', message, 'status', status, 'assignee', assignee,\
             'handled_at', handled_at, 'resolved_at', resolved_at) v \
             FROM alerts WHERE 1=1{wh} ORDER BY id DESC LIMIT {limit}) x",
            wh = wh,
            limit = limit
        );
        json_vec(&self.q0(&sql))
    }

    fn alert_action(&self, id: u64, action: &str, assignee: &str) -> bool {
        let (status_col, ts_col) = if action == "resolve" {
            ("'resolved'", "resolved_at")
        } else {
            ("'ack'", "handled_at")
        };
        let q = format!(
            "UPDATE alerts SET status={status_col}, assignee='{}', {ts_col}={} \
             WHERE id={} AND status IN ('open','ack')",
            esc(assignee),
            now_secs(),
            id
        );
        self.q0(&q).ok().map(|_| true).unwrap_or(false)
    }

    fn alert_counts(&self) -> (u64, u64, u64) {
        let out = self
            .q0("SELECT severity, COUNT(*) FROM alerts WHERE status IN ('open','ack') GROUP BY severity")
            .unwrap_or_default();
        let mut c = (0u64, 0u64, 0u64);
        for l in out.lines() {
            let mut it = l.split('\t');
            let (sev, n) = (it.next().unwrap_or(""), it.next().unwrap_or("0"));
            let n: u64 = n.trim().parse().unwrap_or(0);
            match sev {
                "critical" => c.0 = n,
                "warn" => c.1 = n,
                _ => c.2 = n,
            }
        }
        c
    }

    fn module_upsert(
        &self,
        name: &str,
        category: &str,
        desc: &str,
        steps_json: &str,
        created_by: &str,
        created_at: u64,
    ) {
        let q = format!(
            "INSERT INTO rds_user_modules(name, category, `desc`, steps_json, created_by, created_at, updated_at) \
             VALUES('{}','{}','{}','{}','{}',{},{}) \
             ON DUPLICATE KEY UPDATE category=VALUES(category), `desc`=VALUES(`desc`), \
             steps_json=VALUES(steps_json), created_by=VALUES(created_by), updated_at=VALUES(updated_at)",
            esc(name),
            esc(category),
            esc(desc),
            esc(steps_json),
            esc(created_by),
            created_at,
            created_at
        );
        let r = self.q0(&q);
        MysqlBackend::log_err("module_upsert", &r);
    }

    fn module_list(&self) -> Vec<Value> {
        let sql = "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'name', name, 'category', category, 'desc', `desc`, 'steps_json', steps_json,\
             'created_by', created_by, 'created_at', created_at, 'updated_at', updated_at) v \
             FROM rds_user_modules ORDER BY updated_at DESC) x";
        json_vec(&self.q0(sql))
    }

    fn module_delete(&self, name: &str) -> bool {
        let q = format!("DELETE FROM rds_user_modules WHERE name='{}'", esc(name));
        let r = self.q0(&q);
        MysqlBackend::log_err("module_delete", &r);
        r.is_ok()
    }

    fn dts_list(&self, instance: Option<&str>) -> Vec<Value> {
        let sql = match instance {
            Some(inst) => format!(
                "SELECT JSON_ARRAYAGG(v) FROM (SELECT JSON_OBJECT(\
                 'instance', instance, 'node', node, 'engine', engine, 'container', container,\
                 'target_label', target_label, 'status', status, 'last_error', last_error,\
                 'spec', spec, 'updated_at', updated_at) v FROM rds_dts WHERE instance='{}' ORDER BY updated_at DESC) x",
                esc(inst)
            ),
            None => "SELECT JSON_ARRAYAGG(v) FROM (SELECT JSON_OBJECT(\
                 'instance', instance, 'node', node, 'engine', engine, 'container', container,\
                 'target_label', target_label, 'status', status, 'last_error', last_error,\
                 'spec', spec, 'updated_at', updated_at) v FROM rds_dts ORDER BY updated_at DESC) x"
                .to_string(),
        };
        json_vec(&self.q0(&sql))
    }

    fn dts_upsert(
        &self,
        instance: &str,
        node: &str,
        engine: &str,
        container: &str,
        target_label: &str,
        status: &str,
        last_error: &str,
        spec: &str,
    ) {
        let now = now_secs();
        let q = format!(
            "INSERT INTO rds_dts(instance, node, engine, container, target_label, status, last_error, spec, updated_at) \
             VALUES('{}','{}','{}','{}','{}','{}','{}','{}',{}) \
             ON DUPLICATE KEY UPDATE engine=VALUES(engine), container=VALUES(container), \
             target_label=VALUES(target_label), status=VALUES(status), last_error=VALUES(last_error), \
             spec=VALUES(spec), updated_at=VALUES(updated_at)",
            esc(instance),
            esc(node),
            esc(engine),
            esc(container),
            esc(target_label),
            esc(status),
            esc(last_error),
            esc(spec),
            now
        );
        let r = self.q0(&q);
        MysqlBackend::log_err("dts_upsert", &r);
    }

    fn dts_remove(&self, instance: &str, node: &str) -> bool {
        let q = format!(
            "DELETE FROM rds_dts WHERE instance='{}' AND node='{}'",
            esc(instance),
            esc(node)
        );
        let r = self.q0(&q);
        MysqlBackend::log_err("dts_remove", &r);
        r.is_ok()
    }

    fn host_upsert(
        &self,
        name: &str,
        ip: &str,
        region: &str,
        az: &str,
        rack: &str,
        cpu_cores: u32,
        mem_gb: u32,
        disk_gb: u32,
        agent_port: u16,
        status: &str,
    ) {
        let now = now_secs();
        let q = format!(
            "INSERT INTO rds_hosts(name, ip, region, az, rack, cpu_cores, mem_gb, disk_gb, agent_port, status, next_port, created_at, updated_at) \
             VALUES('{}','{}','{}','{}','{}',{},{},{},{},'{}',{}, {}, {}) \
             ON DUPLICATE KEY UPDATE ip=VALUES(ip), region=VALUES(region), az=VALUES(az), \
             rack=VALUES(rack), cpu_cores=VALUES(cpu_cores), mem_gb=VALUES(mem_gb), \
             disk_gb=VALUES(disk_gb), agent_port=VALUES(agent_port), status=VALUES(status), updated_at=VALUES(updated_at)",
            esc(name),
            esc(ip),
            esc(region),
            esc(az),
            esc(rack),
            cpu_cores,
            mem_gb,
            disk_gb,
            agent_port,
            esc(status),
            35_000, // 新机端口高水位起点(兼容既有宿主端口区间 35000+)
            now,
            now
        );
        let r = self.q0(&q);
        MysqlBackend::log_err("host_upsert", &r);
    }

    fn host_list(&self) -> Vec<Value> {
        let sql = "SELECT IFNULL(JSON_ARRAYAGG(v), JSON_ARRAY()) FROM (SELECT JSON_OBJECT(\
             'name', name, 'ip', ip, 'region', region, 'az', az, 'rack', rack,\
             'cpu_cores', cpu_cores, 'mem_gb', mem_gb, 'disk_gb', disk_gb,\
             'agent_port', agent_port, 'status', status, 'next_port', next_port,\
             'created_at', created_at, 'updated_at', updated_at) v \
             FROM rds_hosts ORDER BY name) x";
        json_vec(&self.q0(sql))
    }

    fn host_delete(&self, name: &str) -> bool {
        let q = format!("DELETE FROM rds_hosts WHERE name='{}'", esc(name));
        let r = self.q0(&q);
        MysqlBackend::log_err("host_delete", &r);
        r.is_ok()
    }

    fn host_alloc_port(&self, name: &str) -> Option<u64> {
        let u = format!(
            "UPDATE rds_hosts SET next_port = next_port + 1 WHERE name='{}'",
            esc(name)
        );
        let r = self.q0(&u);
        MysqlBackend::log_err("host_alloc_port", &r);
        r.ok()?; // Host 不存在时 UPDATE 影响 0 行也成功,继续走 SELECT 判空
        let s = format!("SELECT next_port FROM rds_hosts WHERE name='{}'", esc(name));
        self.q0(&s).ok()?.trim().parse::<u64>().ok()
    }
}

// ═════════════════════════ 内存后端(测试/合成) ═════════════════════════

/// 内存后端:语义对齐 MysqlBackend(无 MySQL 时也可跑协议级测试与合成压测)
pub struct MemoryBackend {
    inner: Mutex<MemState>,
}

struct MemTask {
    kind: String,
    instance: String,
    status: String,
    created: u64,
    started: Option<u64>,
    finished: Option<u64>,
    nodes: HashMap<String, MemNode>,
}

// 节点视图暂不暴露节点级时间(与 MySQL 版一致),字段仅接收
struct MemNode {
    name: String,
    deps: Vec<String>,
    status: String,
    output: String,
    attempts: u32,
    retries: u32,
    timeout_secs: Option<u64>,
    steps: String,
}

#[derive(Default)]
struct MemState {
    tasks: HashMap<String, MemTask>,
    audit: Vec<MemAudit>,
    instances: HashMap<String, (String, String, String, String, u64)>, // name → (region, tenant, data, status, updated_at)
    seq: HashMap<String, u64>,
    locks: HashMap<String, (String, u64)>, // name → (holder, lease_until)
    users: HashMap<String, (String, String, bool, Vec<String>)>, // user → (salt, pass_hash, enabled, roles)
    roles: HashMap<String, (String, Vec<String>)>,               // role → (desc, perms)
    alerts: Vec<MemAlert>,
    next_alert: u64,
    evidence: Vec<MemEvidence>,
    query_audit: Vec<MemQueryAudit>,
    slow_snapshots: Vec<MemSlowSnap>,
    slow_baselines: HashMap<(String, String, String), (u64, u64, u64)>, // (inst,node,digest)→(count,sum,seen_at)
    slow_gov: Vec<MemGov>,
    next_slow_gov: u64,
    capacity: Vec<MemCap>,
    reports: Vec<MemReport>,
    next_report: u64,
    backup_outbox: Vec<MemBOutbox>,
    next_boutbox: u64,
    modules: Vec<MemModule>,
    dts: Vec<MemDts>,
    hosts: Vec<MemHost>,
}

struct MemHost {
    name: String,
    ip: String,
    region: String,
    az: String,
    rack: String,
    cpu_cores: u32,
    mem_gb: u32,
    disk_gb: u32,
    agent_port: u16,
    status: String,
    next_port: u64,
    created_at: u64,
    updated_at: u64,
}

struct MemDts {
    instance: String,
    node: String,
    engine: String,
    container: String,
    target_label: String,
    status: String,
    last_error: String,
    spec: String,
    updated_at: u64,
}

struct MemModule {
    name: String,
    category: String,
    desc: String,
    steps_json: String,
    created_by: String,
    created_at: u64,
    updated_at: u64,
}

struct MemBOutbox {
    id: u64,
    event: String,
    instance: String,
    node: String,
    ikey: String,
    payload: String,
    state: String,
    attempts: u32,
    next_at: u64,
    last_error: String,
    updated_at: u64,
}

struct MemSlowSnap {
    ts: u64,
    instance: String,
    node: String,
    schema_name: String,
    digest: String,
    digest_text: String,
    count_star: u64,
    sum_ms: u64,
    avg_ms: u64,
    max_ms: u64,
    first_seen: u64,
    last_seen: u64,
}

struct MemGov {
    id: u64,
    digest: String,
    digest_text: String,
    status: String,
    assignee: String,
    created_at: u64,
    updated_at: u64,
    resolved_at: Option<u64>,
    advice: String,
}

struct MemCap {
    ts: u64,
    instance: String,
    node: String,
    disk_used_bytes: u64,
    disk_total_bytes: u64,
    data_gib: f64,
}

struct MemReport {
    id: u64,
    ts: u64,
    period: String,
    rtype: String,
    text: String,
    counts: String,
}

struct MemQueryAudit {
    ts: u64,
    user: String,
    instance: String,
    node: String,
    sql: String,
    sql_hash: String,
    read_only: bool,
    rows_returned: u64,
    rows_truncated: bool,
    elapsed_ms: u64,
    ok: bool,
    err_summary: String,
}

struct MemAudit {
    ts: u64,
    user: String,
    instance: String,
    action: String,
    params: String,
    result: String,
    task_id: String,
}

struct MemEvidence {
    ts: u64,
    instance: String,
    kind: String,
    reason: String,
    facts: String,
}

struct MemAlert {
    id: u64,
    ts: u64,
    instance: String,
    kind: String,
    severity: String,
    message: String,
    status: String,
    assignee: String,
    handled_at: Option<u64>,
    resolved_at: Option<u64>,
}

impl MemoryBackend {
    pub fn new() -> Arc<dyn StoreBackend> {
        let b = Arc::new(MemoryBackend {
            inner: Mutex::new(MemState::default()),
        });
        b.rbac_seed_default_admin();
        b
    }
}

/// evidence 行 → 视图(facts 原为 JSON 字符串,解析为对象;解析失败原样字符串兜底)
fn mem_evidence_view(e: &MemEvidence) -> Value {
    let facts =
        serde_json::from_str::<Value>(&e.facts).unwrap_or_else(|_| Value::String(e.facts.clone()));
    json!({
        "ts": e.ts,
        "instance": e.instance,
        "kind": e.kind,
        "reason": e.reason,
        "facts": facts,
    })
}

fn mem_task_view(t: &MemTask, tid: &str) -> Value {
    let mut nodes: Vec<Value> = t
        .nodes
        .iter()
        .map(|(nid, n)| {
            json!({
                "id": nid,
                "name": n.name,
                "status": n.status,
                "output": n.output,
                "attempts": n.attempts,
                "deps": n.deps,
            })
        })
        .collect();
    // 输出顺序稳定(id 排序),与 MySQL 返回不同但前端按 deps 排序渲染
    nodes.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    json!({
        "id": tid,
        "kind": t.kind,
        "instance": t.instance,
        "status": t.status,
        "created_at": t.created,
        "started_at": t.started,
        "finished_at": t.finished,
        "nodes": nodes,
    })
}

impl StoreBackend for MemoryBackend {
    fn next_task_seq(&self, kind: &str) -> u64 {
        let mut g = self.inner.lock().unwrap();
        let v = g.seq.entry(kind.to_string()).or_insert(0);
        *v += 1;
        *v
    }

    fn upsert_task(
        &self,
        id: &str,
        kind: &str,
        instance: &str,
        status: &str,
        created: u64,
        started: Option<u64>,
        finished: Option<u64>,
    ) {
        let mut g = self.inner.lock().unwrap();
        match g.tasks.get_mut(id) {
            Some(t) => {
                t.status = status.to_string();
                if started.is_some() {
                    t.started = started;
                }
                t.finished = finished;
            }
            None => {
                g.tasks.insert(
                    id.to_string(),
                    MemTask {
                        kind: kind.to_string(),
                        instance: instance.to_string(),
                        status: status.to_string(),
                        created,
                        started,
                        finished,
                        nodes: HashMap::new(),
                    },
                );
            }
        }
    }

    fn update_task_status(&self, id: &str, status: &str, finished: Option<u64>) {
        let mut g = self.inner.lock().unwrap();
        if let Some(t) = g.tasks.get_mut(id) {
            t.status = status.to_string();
            t.finished = finished;
        }
    }

    fn upsert_node(
        &self,
        task_id: &str,
        node_id: &str,
        name: &str,
        deps_json: &str,
        steps_json: &str,
        status: &str,
        output: &str,
        attempts: u32,
        retries: u32,
        timeout_secs: Option<u64>,
        _started: Option<u64>,
        _finished: Option<u64>,
    ) {
        let mut g = self.inner.lock().unwrap();
        let Some(task) = g.tasks.get_mut(task_id) else {
            return;
        };
        let deps: Vec<String> = serde_json::from_str(deps_json).unwrap_or_default();
        task.nodes.insert(
            node_id.to_string(),
            MemNode {
                name: name.to_string(),
                deps,
                status: status.to_string(),
                output: output.to_string(),
                attempts,
                retries,
                timeout_secs,
                steps: steps_json.to_string(),
            },
        );
    }

    fn mark_interrupted(&self) -> usize {
        let mut g = self.inner.lock().unwrap();
        let now = now_secs();
        let mut n = 0usize;
        for t in g.tasks.values_mut() {
            if t.status == "pending" || t.status == "running" {
                t.status = "failed".into();
                t.finished = Some(now);
                n += 1;
            }
        }
        // 失败任务中未终态节点 → skipped(与 MySQL 版一致)
        for t in g.tasks.values_mut() {
            if t.status == "failed" {
                for nd in t.nodes.values_mut() {
                    if nd.status == "pending" || nd.status == "running" {
                        nd.status = "skipped".into();
                        nd.output = "进程中断,任务未完成".into();
                    }
                }
            }
        }
        n
    }

    fn task_views(&self, limit: usize) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        let mut v: Vec<Value> = g.tasks.iter().map(|(id, t)| mem_task_view(t, id)).collect();
        v.sort_by_key(|t| {
            (
                std::cmp::Reverse(t["created_at"].as_u64().unwrap_or(0)),
                std::cmp::Reverse(t["id"].as_str().unwrap_or("").to_string()),
            )
        });
        v.truncate(limit);
        v
    }

    fn task_views_by_instance(&self, instance: &str, limit: usize) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        let mut v: Vec<Value> = g
            .tasks
            .iter()
            .filter(|(_, t)| t.instance == instance)
            .map(|(id, t)| mem_task_view(t, id))
            .collect();
        v.sort_by_key(|t| {
            (
                std::cmp::Reverse(t["created_at"].as_u64().unwrap_or(0)),
                std::cmp::Reverse(t["id"].as_str().unwrap_or("").to_string()),
            )
        });
        v.truncate(limit);
        v
    }

    fn task_view(&self, id: &str) -> Option<Value> {
        let g = self.inner.lock().unwrap();
        g.tasks.get(id).map(|t| mem_task_view(t, id))
    }

    fn delete_task(&self, id: &str) -> Result<(), String> {
        let mut g = self.inner.lock().unwrap();
        if g.tasks.remove(id).is_none() {
            return Err(format!("任务 {id} 不存在"));
        }
        Ok(())
    }

    fn pending_task_definitions(&self) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        g.tasks
            .iter()
            .filter(|(_, t)| t.status == "pending" || t.status == "running")
            .map(|(tid, t)| {
                let nodes: Vec<Value> = t
                    .nodes
                    .iter()
                    .map(|(nid, n)| {
                        json!({
                            "node_id": nid,
                            "name": n.name,
                            "deps": n.deps,
                            "steps": serde_json::from_str::<Value>(&n.steps).unwrap_or(json!([])),
                            "status": n.status,
                            "output": n.output,
                            "attempts": n.attempts,
                            "retries": n.retries,
                            "timeout_secs": n.timeout_secs,
                        })
                    })
                    .collect();
                json!({
                    "id": tid,
                    "kind": t.kind,
                    "instance": t.instance,
                    "created_at": t.created,
                    "nodes": nodes,
                })
            })
            .collect()
    }

    fn task_definition(&self, id: &str) -> Option<Value> {
        let g = self.inner.lock().unwrap();
        let t = g.tasks.get(id)?;
        let nodes: Vec<Value> = t
            .nodes
            .iter()
            .map(|(nid, n)| {
                json!({
                    "node_id": nid,
                    "name": n.name,
                    "deps": n.deps,
                    "steps": serde_json::from_str::<Value>(&n.steps).unwrap_or(json!([])),
                    "status": n.status,
                    "output": n.output,
                    "attempts": n.attempts,
                    "retries": n.retries,
                    "timeout_secs": n.timeout_secs,
                })
            })
            .collect();
        Some(json!({
            "id": id,
            "kind": t.kind,
            "instance": t.instance,
            "created_at": t.created,
            "status": t.status,
            "started_at": t.started,
            "finished_at": t.finished,
            "nodes": nodes,
        }))
    }

    fn audit(
        &self,
        user: &str,
        instance: &str,
        action: &str,
        params: &str,
        result: &str,
        task_id: &str,
    ) {
        let mut g = self.inner.lock().unwrap();
        g.audit.push(MemAudit {
            ts: now_secs(),
            user: user.to_string(),
            instance: instance.to_string(),
            action: action.to_string(),
            params: truncate(params, 240),
            result: truncate(result, 240),
            task_id: task_id.to_string(),
        });
    }

    fn audit_marker_exists(&self, marker: &str) -> bool {
        let g = self.inner.lock().unwrap();
        let prefix = format!("{marker} ");
        g.audit
            .iter()
            .any(|a| a.params.starts_with(&prefix))
    }


    fn audit_list(
        &self,
        limit: usize,
        instance: Option<&str>,
        action: Option<&str>,
        q: Option<&str>,
    ) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        g.audit
            .iter()
            .rev()
            .filter(|a| {
                let inst_ok = instance.map_or(true, |f| f.is_empty() || a.instance.contains(f));
                let act_ok = action.map_or(true, |f| f.is_empty() || a.action.contains(f));
                let q_ok = q.map_or(true, |f| {
                    f.is_empty()
                        || a.user.to_lowercase().contains(&f.to_lowercase())
                        || a.instance.to_lowercase().contains(&f.to_lowercase())
                        || a.action.to_lowercase().contains(&f.to_lowercase())
                        || a.params.to_lowercase().contains(&f.to_lowercase())
                        || a.result.to_lowercase().contains(&f.to_lowercase())
                        || a.task_id.to_lowercase().contains(&f.to_lowercase())
                });
                inst_ok && act_ok && q_ok
            })
            .take(limit)
            .map(|a| {
                json!({
                    "ts": a.ts,
                    "user": a.user,
                    "instance": a.instance,
                    "action": a.action,
                    "params": a.params,
                    "result": a.result,
                    "task_id": a.task_id,
                })
            })
            .collect()
    }

    fn audit_since(&self, since: u64, limit: usize) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        g.audit
            .iter()
            .rev()
            .filter(|a| a.ts >= since)
            .take(limit)
            .map(|a| {
                json!({
                    "ts": a.ts,
                    "user": a.user,
                    "instance": a.instance,
                    "action": a.action,
                    "params": a.params,
                    "result": a.result,
                    "task_id": a.task_id,
                })
            })
            .collect()
    }

    fn evidence_insert(&self, instance: &str, kind: &str, reason: &str, facts_json: &str) {
        let mut g = self.inner.lock().unwrap();
        g.evidence.push(MemEvidence {
            ts: now_secs(),
            instance: instance.to_string(),
            kind: kind.to_string(),
            reason: truncate(reason, 240),
            facts: facts_json.to_string(),
        });
    }

    fn evidence_latest(&self, instance: &str, n: usize) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        g.evidence
            .iter()
            .rev()
            .filter(|e| e.instance == instance)
            .take(n)
            .map(mem_evidence_view)
            .collect()
    }

    fn evidence_since(&self, kind: Option<&str>, since_ts: u64, limit: usize) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        g.evidence
            .iter()
            .rev()
            .filter(|e| e.ts >= since_ts && kind.map_or(true, |k| k.is_empty() || e.kind == k))
            .take(limit)
            .map(mem_evidence_view)
            .collect()
    }

    fn query_audit_insert(
        &self,
        user: &str,
        instance: &str,
        node: &str,
        sql: &str,
        sql_hash: &str,
        read_only: bool,
        rows_returned: u64,
        rows_truncated: bool,
        elapsed_ms: u64,
        ok: bool,
        err_summary: &str,
    ) {
        let mut g = self.inner.lock().unwrap();
        g.query_audit.push(MemQueryAudit {
            ts: now_secs(),
            user: user.to_string(),
            instance: instance.to_string(),
            node: node.to_string(),
            sql: sql.to_string(),
            sql_hash: sql_hash.to_string(),
            read_only,
            rows_returned,
            rows_truncated,
            elapsed_ms,
            ok,
            err_summary: truncate(err_summary, 240),
        });
    }

    fn query_audit_list(
        &self,
        limit: usize,
        instance: Option<&str>,
        user: Option<&str>,
        since: Option<u64>,
    ) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        g.query_audit
            .iter()
            .rev()
            .filter(|a| {
                instance.map_or(true, |f| f.is_empty() || a.instance.contains(f))
                    && user.map_or(true, |f| f.is_empty() || a.user.contains(f))
                    && since.map_or(true, |f| a.ts >= f)
            })
            .take(limit)
            .map(|a| {
                json!({
                    "ts": a.ts,
                    "user": a.user,
                    "instance": a.instance,
                    "node": a.node,
                    "sql": a.sql,
                    "sql_hash": a.sql_hash,
                    "read_only": a.read_only,
                    "rows_returned": a.rows_returned,
                    "rows_truncated": a.rows_truncated,
                    "elapsed_ms": a.elapsed_ms,
                    "ok": a.ok,
                    "err_summary": a.err_summary,
                })
            })
            .collect()
    }

    fn slow_snapshots_insert(&self, samples: &[SlowSample]) {
        let mut g = self.inner.lock().unwrap();
        for s in samples {
            g.slow_snapshots.push(MemSlowSnap {
                ts: now_secs(),
                instance: s.instance.clone(),
                node: s.node.clone(),
                schema_name: s.schema_name.clone(),
                digest: s.digest.clone(),
                digest_text: s.digest_text.clone(),
                count_star: s.count_star,
                sum_ms: s.sum_ms,
                avg_ms: s.avg_ms,
                max_ms: s.max_ms,
                first_seen: s.first_seen,
                last_seen: s.last_seen,
            });
        }
    }

    fn slow_baselines_load(&self) -> Vec<SlowBase> {
        let g = self.inner.lock().unwrap();
        g.slow_baselines
            .iter()
            .map(|((inst, node, digest), (count, sum, seen))| SlowBase {
                instance: inst.clone(),
                node: node.clone(),
                digest: digest.clone(),
                count_star: *count,
                sum_ms: *sum,
                seen_at: *seen,
            })
            .collect()
    }

    fn slow_baselines_set(&self, instance: &str, node: &str, rows: &[SlowBase]) {
        let mut g = self.inner.lock().unwrap();
        g.slow_baselines
            .retain(|(i, n, _), _| i != instance || n != node);
        for b in rows {
            g.slow_baselines.insert(
                (b.instance.clone(), b.node.clone(), b.digest.clone()),
                (b.count_star, b.sum_ms, b.seen_at),
            );
        }
    }

    fn slow_window(
        &self,
        since_ts: u64,
        instance: Option<&str>,
        digest: Option<&str>,
        limit: usize,
    ) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        g.slow_snapshots
            .iter()
            .rev()
            .filter(|s| {
                s.ts >= since_ts
                    && instance.map_or(true, |f| f.is_empty() || s.instance == f)
                    && digest.map_or(true, |f| f.is_empty() || s.digest == f)
            })
            .take(limit)
            .map(|s| {
                json!({
                    "ts": s.ts, "instance": s.instance, "node": s.node,
                    "schema_name": s.schema_name, "digest": s.digest,
                    "digest_text": s.digest_text, "count_star": s.count_star,
                    "sum_ms": s.sum_ms, "avg_ms": s.avg_ms, "max_ms": s.max_ms,
                    "first_seen": s.first_seen, "last_seen": s.last_seen,
                })
            })
            .collect()
    }

    fn slow_gov_ensure(&self, digest: &str, digest_text: &str) -> bool {
        let (exists, now) = {
            let g = self.inner.lock().unwrap();
            (
                g.slow_gov
                    .iter()
                    .any(|x| x.digest == digest && (x.status == "open" || x.status == "ack")),
                now_secs(),
            )
        };
        if exists {
            return false;
        }
        let mut g = self.inner.lock().unwrap();
        g.next_slow_gov += 1;
        let id = g.next_slow_gov;
        g.slow_gov.push(MemGov {
            id,
            digest: digest.to_string(),
            digest_text: truncate(digest_text, 1024),
            status: "open".into(),
            assignee: String::new(),
            created_at: now,
            updated_at: now,
            resolved_at: None,
            advice: String::new(),
        });
        true
    }

    fn slow_gov_list(&self, limit: usize, status: Option<&str>) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        g.slow_gov
            .iter()
            .rev()
            .filter(|x| status.map_or(true, |f| f.is_empty() || x.status == f))
            .take(limit)
            .map(|x| {
                let advice = if x.advice.is_empty() {
                    Value::Null
                } else {
                    serde_json::from_str(&x.advice).unwrap_or(Value::String(x.advice.clone()))
                };
                json!({
                    "id": x.id, "digest": x.digest, "digest_text": x.digest_text,
                    "status": x.status, "assignee": x.assignee,
                    "created_at": x.created_at, "updated_at": x.updated_at,
                    "resolved_at": x.resolved_at, "advice": advice,
                })
            })
            .collect()
    }

    fn slow_gov_advise(&self, digest: &str, advice_json: &str) -> bool {
        let mut g = self.inner.lock().unwrap();
        let Some(x) = g
            .slow_gov
            .iter_mut()
            .find(|x| x.digest == digest && (x.status == "open" || x.status == "ack"))
        else {
            return false;
        };
        x.advice = advice_json.to_string();
        x.updated_at = now_secs();
        true
    }

    fn slow_gov_action(&self, id: u64, action: &str, assignee: &str) -> bool {
        let mut g = self.inner.lock().unwrap();
        let Some(x) = g
            .slow_gov
            .iter_mut()
            .find(|x| x.id == id && (x.status == "open" || x.status == "ack"))
        else {
            return false;
        };
        let now = now_secs();
        if action == "resolve" {
            x.status = "resolved".into();
            x.resolved_at = Some(now);
        } else {
            x.status = "ack".into();
        }
        x.assignee = assignee.to_string();
        x.updated_at = now;
        true
    }

    fn slow_prune(&self, before_snap: u64, before_gov: u64) -> (usize, usize) {
        let mut g = self.inner.lock().unwrap();
        let n1 = g.slow_snapshots.len();
        g.slow_snapshots.retain(|s| s.ts >= before_snap);
        let n1 = n1 - g.slow_snapshots.len();
        let n2 = g.slow_gov.len();
        g.slow_gov.retain(|x| {
            !(x.status == "resolved" && x.resolved_at.map_or(false, |t| t < before_gov))
        });
        let n2 = n2 - g.slow_gov.len();
        (n1, n2)
    }

    // ─── 容量采样(内存实现;dba-ai-design §6/§9)───

    fn capacity_insert(&self, samples: &[CapSample]) {
        if samples.is_empty() {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        let now = now_secs();
        for s in samples {
            g.capacity.push(MemCap {
                ts: now,
                instance: s.instance.clone(),
                node: s.node.clone(),
                disk_used_bytes: s.disk_used_bytes,
                disk_total_bytes: s.disk_total_bytes,
                data_gib: s.data_gib,
            });
        }
    }

    fn capacity_since(&self, instance: Option<&str>, since_ts: u64, limit: usize) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        let mut rows: Vec<Value> = g
            .capacity
            .iter()
            .filter(|c| {
                c.ts >= since_ts && instance.map_or(true, |v| v.is_empty() || c.instance == v)
            })
            .map(|c| {
                json!({
                    "ts": c.ts, "instance": c.instance, "node": c.node,
                    "disk_used_bytes": c.disk_used_bytes,
                    "disk_total_bytes": c.disk_total_bytes,
                    "data_gib": c.data_gib,
                })
            })
            .collect();
        rows.sort_by_key(|v| v["ts"].as_u64().unwrap_or(0));
        rows.truncate(limit);
        rows
    }

    fn capacity_prune(&self, before_ts: u64) -> usize {
        let mut g = self.inner.lock().unwrap();
        let n = g.capacity.len();
        g.capacity.retain(|c| c.ts >= before_ts);
        n - g.capacity.len()
    }

    // ─── 报告归档(内存实现;dba-ai-design §8)───

    fn report_insert(&self, period: &str, rtype: &str, text: &str, counts_json: &str) {
        let mut g = self.inner.lock().unwrap();
        g.next_report += 1;
        let id = g.next_report;
        g.reports.push(MemReport {
            id,
            ts: now_secs(),
            period: period.to_string(),
            rtype: rtype.to_string(),
            text: text.to_string(),
            counts: counts_json.to_string(),
        });
    }

    fn reports_list(&self, rtype: Option<&str>, since_ts: u64, limit: usize) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        g.reports
            .iter()
            .rev()
            .filter(|r| r.ts >= since_ts && rtype.map_or(true, |v| v.is_empty() || r.rtype == v))
            .take(limit)
            .map(|r| {
                let counts =
                    serde_json::from_str(&r.counts).unwrap_or(Value::String(r.counts.clone()));
                json!({
                    "id": r.id, "ts": r.ts, "period": r.period, "type": r.rtype,
                    "text": r.text, "counts": counts,
                })
            })
            .collect()
    }

    fn reports_prune(&self, before_ts: u64) -> usize {
        let mut g = self.inner.lock().unwrap();
        let n = g.reports.len();
        g.reports.retain(|r| r.ts >= before_ts);
        n - g.reports.len()
    }

    // ─── 备份注册联动(内存实现)───

    fn backup_outbox_enqueue(
        &self,
        event: &str,
        instance: &str,
        node: &str,
        idempotency_key: &str,
        payload_json: &str,
    ) -> bool {
        let mut g = self.inner.lock().unwrap();
        if g.backup_outbox.iter().any(|b| b.ikey == idempotency_key) {
            return false;
        }
        g.next_boutbox += 1;
        let id = g.next_boutbox;
        let now = now_secs();
        g.backup_outbox.push(MemBOutbox {
            id,
            event: event.to_string(),
            instance: instance.to_string(),
            node: node.to_string(),
            ikey: idempotency_key.to_string(),
            payload: payload_json.to_string(),
            state: "pending".into(),
            attempts: 0,
            next_at: now,
            last_error: String::new(),
            updated_at: now,
        });
        true
    }

    fn backup_outbox_poll(&self, limit: usize, now: u64) -> Vec<Value> {
        let mut g = self.inner.lock().unwrap();
        let ids: Vec<u64> = g
            .backup_outbox
            .iter()
            .filter(|b| (b.state == "pending" || b.state == "delivering") && b.next_at <= now)
            .take(limit.min(200))
            .map(|b| b.id)
            .collect();
        let mut out = Vec::new();
        for id in ids {
            if let Some(b) = g.backup_outbox.iter_mut().find(|b| b.id == id) {
                b.state = "delivering".into();
                b.updated_at = now_secs();
                out.push(json!({
                    "id": b.id, "event": b.event, "instance": b.instance, "node": b.node,
                    "idempotency_key": b.ikey, "payload_json": b.payload,
                    "attempts": b.attempts, "state": b.state,
                }));
            }
        }
        out
    }

    fn backup_outbox_mark(
        &self,
        id: u64,
        state: &str,
        attempts: u32,
        next_at: u64,
        last_error: &str,
    ) {
        let mut g = self.inner.lock().unwrap();
        if let Some(b) = g.backup_outbox.iter_mut().find(|b| b.id == id) {
            b.state = state.to_string();
            b.attempts = attempts;
            b.next_at = next_at;
            b.last_error = truncate(last_error, 240);
            b.updated_at = now_secs();
        }
    }

    fn backup_outbox_reg_state(&self, instance: Option<&str>) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        let mut latest: std::collections::BTreeMap<(String, String), &MemBOutbox> =
            std::collections::BTreeMap::new();
        for b in g.backup_outbox.iter().filter(|b| {
            b.state == "done" && instance.map_or(true, |f| f.is_empty() || b.instance == f)
        }) {
            latest
                .entry((b.instance.clone(), b.node.clone()))
                .and_modify(|cur| {
                    if b.id > cur.id {
                        *cur = b;
                    }
                })
                .or_insert(b);
        }
        latest
            .into_iter()
            .map(|((_, _), b)| {
                let registered = b.event.starts_with("register");
                let deregistered = b.event.starts_with("deregister");
                let state = if deregistered {
                    "deregistered"
                } else if registered {
                    "registered"
                } else {
                    "unknown"
                };
                json!({
                    "instance": b.instance,
                    "node": b.node,
                    "reg_state": state,
                    "last_event": b.event,
                    "last_error": b.last_error,
                    "updated_at": b.updated_at,
                })
            })
            .collect()
    }

    fn backup_outbox_requeue(&self, id: u64) -> bool {
        let mut g = self.inner.lock().unwrap();
        let Some(b) = g
            .backup_outbox
            .iter_mut()
            .find(|b| b.id == id && b.state == "dead")
        else {
            return false;
        };
        b.state = "pending".into();
        b.attempts = 0;
        b.next_at = now_secs();
        b.last_error.clear();
        b.updated_at = now_secs();
        true
    }

    fn backup_outbox_prune(&self, before_ts: u64) -> usize {
        let mut g = self.inner.lock().unwrap();
        let n = g.backup_outbox.len();
        g.backup_outbox
            .retain(|b| !((b.state == "done" || b.state == "dead") && b.updated_at < before_ts));
        n - g.backup_outbox.len()
    }

    fn instance_upsert(
        &self,
        name: &str,
        region: &str,
        tenant: &str,
        data: &str,
        status: &str,
        updated_at: u64,
    ) {
        let mut g = self.inner.lock().unwrap();
        g.instances.insert(
            name.to_string(),
            (
                region.to_string(),
                tenant.to_string(),
                data.to_string(),
                status.to_string(),
                updated_at,
            ),
        );
    }

    fn instance_load_all(&self) -> Vec<(String, String)> {
        let g = self.inner.lock().unwrap();
        g.instances
            .iter()
            .map(|(n, (_, _, d, _, _))| (n.clone(), d.clone()))
            .collect()
    }

    fn instance_delete(&self, name: &str) {
        let mut g = self.inner.lock().unwrap();
        g.instances.remove(name);
    }

    fn lock_instance(&self, name: &str, holder: &str, lease_secs: u64) -> bool {
        let mut g = self.inner.lock().unwrap();
        let now = now_secs();
        let until = now + lease_secs;
        match g.locks.get(name).cloned() {
            None => {
                g.locks
                    .insert(name.to_string(), (holder.to_string(), until));
                true
            }
            Some((h, u)) if h == holder || u <= now => {
                g.locks
                    .insert(name.to_string(), (holder.to_string(), until));
                true
            }
            _ => false,
        }
    }

    fn renew_instance_lock(&self, name: &str, holder: &str, lease_secs: u64) -> bool {
        let mut g = self.inner.lock().unwrap();
        let now = now_secs();
        match g.locks.get_mut(name) {
            Some((h, u)) if h == holder => {
                *u = now + lease_secs;
                true
            }
            _ => false,
        }
    }

    fn unlock_instance(&self, name: &str, holder: &str) {
        let mut g = self.inner.lock().unwrap();
        if let Some((h, _)) = g.locks.get(name) {
            if h == holder {
                g.locks.remove(name);
            }
        }
    }

    fn clear_all_locks(&self) {
        self.inner.lock().unwrap().locks.clear();
    }

    // ─── RBAC(内存实现,语义对齐 MySQL) ───

    fn rbac_seed_default_admin(&self) {
        let mut g = self.inner.lock().unwrap();
        if g.users.is_empty() {
            let (uname, pass) = rbac_default_creds();
            let salt = crate::sha256::to_hex(&salt_bytes());
            g.users.insert(
                uname.clone(),
                (
                    salt.clone(),
                    hash_password(&salt, &pass),
                    true,
                    vec!["super".into()],
                ),
            );
            g.roles.insert(
                "super".into(),
                (
                    "超级管理员(全部权限)".into(),
                    PERMISSIONS.iter().map(|s| s.to_string()).collect(),
                ),
            );
        }
    }

    fn auth_effective(&self, user: &str, pass: &str) -> Option<(bool, Vec<String>)> {
        let g = self.inner.lock().unwrap();
        let (salt, hash, enabled, roles) = g.users.get(user)?.clone();
        if hash_password(&salt, pass) != hash {
            return None;
        }
        let perms = if roles.iter().any(|r| r == "super") {
            PERMISSIONS.iter().map(|s| s.to_string()).collect()
        } else {
            let mut set: Vec<String> = Vec::new();
            for r in &roles {
                if let Some((_, ps)) = g.roles.get(r) {
                    for p in ps {
                        if !set.contains(p) {
                            set.push(p.clone());
                        }
                    }
                }
            }
            set
        };
        Some((enabled, perms))
    }

    fn user_enabled(&self, user: &str) -> bool {
        self.inner
            .lock()
            .unwrap()
            .users
            .get(user)
            .map(|(_, _, e, _)| *e)
            .unwrap_or(false)
    }

    fn users_list(&self) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for (user, (salt, hash, enabled, roles)) in &g.users {
            let _ = (salt, hash);
            let perms = if roles.iter().any(|r| r == "super") {
                PERMISSIONS.iter().map(|s| s.to_string()).collect()
            } else {
                let mut set: Vec<String> = Vec::new();
                for r in roles {
                    if let Some((_, ps)) = g.roles.get(r) {
                        for p in ps {
                            if !set.contains(p) {
                                set.push(p.clone());
                            }
                        }
                    }
                }
                set
            };
            out.push(json!({ "user": user, "enabled": *enabled, "roles": roles.clone(), "perms": perms }));
        }
        out.sort_by(|a, b| a["user"].as_str().unwrap_or("").cmp(b["user"].as_str().unwrap_or("")));
        out
    }

    fn users_raw(&self) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        let mut out: Vec<Value> = g
            .users
            .iter()
            .map(|(user, (salt, hash, enabled, roles))| {
                json!({
                    "user": user,
                    "salt": salt,
                    "pass_hash": hash,
                    "enabled": *enabled,
                    "roles": roles.clone(),
                })
            })
            .collect();
        out.sort_by(|a, b| a["user"].as_str().unwrap_or("").cmp(b["user"].as_str().unwrap_or("")));
        out
    }

    fn user_upsert(&self, user: &str, salt: &str, pass_hash: &str, enabled: bool) {
        let mut g = self.inner.lock().unwrap();
        let e = g
            .users
            .entry(user.to_string())
            .or_insert_with(|| ("".into(), "".into(), true, Vec::new()));
        e.0 = salt.to_string();
        e.1 = pass_hash.to_string();
        e.2 = enabled;
    }

    fn user_set_enabled(&self, user: &str, enabled: bool) {
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.users.get_mut(user) {
            e.2 = enabled;
        }
    }

    fn user_roles_set(&self, user: &str, roles: &[&str]) {
        let mut g = self.inner.lock().unwrap();
        if let Some(e) = g.users.get_mut(user) {
            e.3 = roles.iter().map(|s| s.to_string()).collect();
        }
    }

    fn roles_list(&self) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        g.roles
            .iter()
            .map(|(name, (desc, perms))| json!({ "name": name, "description": desc, "perms": perms }))
            .collect()
    }

    fn role_upsert(&self, role: &str, desc: &str) {
        let mut g = self.inner.lock().unwrap();
        let e = g
            .roles
            .entry(role.to_string())
            .or_insert_with(|| ("".into(), Vec::new()));
        e.0 = desc.to_string();
    }

    fn role_perms_set(&self, role: &str, perms: &[&str]) {
        let mut g = self.inner.lock().unwrap();
        let e = g
            .roles
            .entry(role.to_string())
            .or_insert_with(|| ("".into(), Vec::new()));
        e.1 = perms.iter().map(|s| s.to_string()).collect();
    }

    // ─── 告警(内存) ───

    fn alert_open(&self, instance: &str, kind: &str, severity: &str, message: &str) {
        let (exists, next_id) = {
            let g = self.inner.lock().unwrap();
            let exists = g.alerts.iter().any(|a| {
                a.instance == instance
                    && a.kind == kind
                    && (a.status == "open" || a.status == "ack")
            });
            (exists, g.next_alert)
        };
        if exists {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        g.next_alert = next_id + 1;
        let id = g.next_alert;
        g.alerts.push(MemAlert {
            id,
            ts: now_secs(),
            instance: instance.to_string(),
            kind: kind.to_string(),
            severity: severity.to_string(),
            message: truncate(message, 480),
            status: "open".into(),
            assignee: String::new(),
            handled_at: None,
            resolved_at: None,
        });
    }

    fn alert_resolve_instance(&self, instance: &str) {
        let mut g = self.inner.lock().unwrap();
        let now = now_secs();
        for a in g.alerts.iter_mut() {
            if a.instance == instance && (a.status == "open" || a.status == "ack") {
                a.status = "resolved".into();
                a.resolved_at = Some(now);
            }
        }
    }

    fn alert_list(
        &self,
        limit: usize,
        severity: Option<&str>,
        status: Option<&str>,
        instance: Option<&str>,
    ) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        g.alerts
            .iter()
            .rev()
            .filter(|a| {
                severity.map_or(true, |v| v.is_empty() || a.severity == v)
                    && status.map_or(true, |v| v.is_empty() || a.status == v)
                    && instance.map_or(true, |v| v.is_empty() || a.instance.contains(v))
            })
            .take(limit)
            .map(|a| {
                json!({
                    "id": a.id, "ts": a.ts, "instance": a.instance, "kind": a.kind,
                    "severity": a.severity, "message": a.message, "status": a.status,
                    "assignee": a.assignee, "handled_at": a.handled_at, "resolved_at": a.resolved_at,
                })
            })
            .collect()
    }

    fn alert_action(&self, id: u64, action: &str, assignee: &str) -> bool {
        let mut g = self.inner.lock().unwrap();
        let now = now_secs();
        let Some(a) = g
            .alerts
            .iter_mut()
            .find(|a| a.id == id && (a.status == "open" || a.status == "ack"))
        else {
            return false;
        };
        if action == "resolve" {
            a.status = "resolved".into();
            a.resolved_at = Some(now);
        } else {
            a.status = "ack".into();
            a.handled_at = Some(now);
        }
        a.assignee = assignee.to_string();
        true
    }

    fn module_upsert(
        &self,
        name: &str,
        category: &str,
        desc: &str,
        steps_json: &str,
        created_by: &str,
        created_at: u64,
    ) {
        let mut g = self.inner.lock().unwrap();
        if let Some(m) = g.modules.iter_mut().find(|m| m.name == name) {
            m.category = category.to_string();
            m.desc = desc.to_string();
            m.steps_json = steps_json.to_string();
            m.created_by = created_by.to_string();
            m.updated_at = created_at;
        } else {
            g.modules.push(MemModule {
                name: name.to_string(),
                category: category.to_string(),
                desc: desc.to_string(),
                steps_json: steps_json.to_string(),
                created_by: created_by.to_string(),
                created_at,
                updated_at: created_at,
            });
        }
    }

    fn module_list(&self) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        let mut list: Vec<Value> = g
            .modules
            .iter()
            .map(|m| {
                json!({
                    "name": m.name, "category": m.category, "desc": m.desc,
                    "steps_json": m.steps_json, "created_by": m.created_by,
                    "created_at": m.created_at, "updated_at": m.updated_at,
                })
            })
            .collect();
        list.sort_by(|a, b| {
            b["updated_at"]
                .as_u64()
                .unwrap_or(0)
                .cmp(&a["updated_at"].as_u64().unwrap_or(0))
        });
        list
    }

    fn module_delete(&self, name: &str) -> bool {
        let mut g = self.inner.lock().unwrap();
        let before = g.modules.len();
        g.modules.retain(|m| m.name != name);
        g.modules.len() != before
    }

    fn dts_list(&self, instance: Option<&str>) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        let mut list: Vec<Value> = g
            .dts
            .iter()
            .filter(|d| instance.map_or(true, |i| i.is_empty() || d.instance == i))
            .map(|d| {
                json!({
                    "instance": d.instance, "node": d.node, "engine": d.engine,
                    "container": d.container, "target_label": d.target_label,
                    "status": d.status, "last_error": d.last_error, "spec": d.spec, "updated_at": d.updated_at,
                })
            })
            .collect();
        list.sort_by(|a, b| {
            b["updated_at"]
                .as_u64()
                .unwrap_or(0)
                .cmp(&a["updated_at"].as_u64().unwrap_or(0))
        });
        list
    }

    fn dts_upsert(
        &self,
        instance: &str,
        node: &str,
        engine: &str,
        container: &str,
        target_label: &str,
        status: &str,
        last_error: &str,
        spec: &str,
    ) {
        let mut g = self.inner.lock().unwrap();
        let now = now_secs();
        if let Some(d) = g
            .dts
            .iter_mut()
            .find(|d| d.instance == instance && d.node == node)
        {
            d.engine = engine.to_string();
            d.container = container.to_string();
            d.target_label = target_label.to_string();
            d.status = status.to_string();
            d.last_error = truncate(last_error, 240);
            d.spec = spec.to_string();
            d.updated_at = now;
        } else {
            g.dts.push(MemDts {
                instance: instance.to_string(),
                node: node.to_string(),
                engine: engine.to_string(),
                container: container.to_string(),
                target_label: target_label.to_string(),
                status: status.to_string(),
                last_error: truncate(last_error, 240),
                spec: spec.to_string(),
                updated_at: now,
            });
        }
    }

    fn dts_remove(&self, instance: &str, node: &str) -> bool {
        let mut g = self.inner.lock().unwrap();
        let before = g.dts.len();
        g.dts.retain(|d| d.instance != instance || d.node != node);
        g.dts.len() != before
    }

    fn host_upsert(
        &self,
        name: &str,
        ip: &str,
        region: &str,
        az: &str,
        rack: &str,
        cpu_cores: u32,
        mem_gb: u32,
        disk_gb: u32,
        agent_port: u16,
        status: &str,
    ) {
        let mut g = self.inner.lock().unwrap();
        let now = now_secs();
        if let Some(h) = g.hosts.iter_mut().find(|h| h.name == name) {
            h.ip = ip.to_string();
            h.region = region.to_string();
            h.az = az.to_string();
            h.rack = rack.to_string();
            h.cpu_cores = cpu_cores;
            h.mem_gb = mem_gb;
            h.disk_gb = disk_gb;
            h.agent_port = agent_port;
            h.status = status.to_string();
            h.updated_at = now;
        } else {
            g.hosts.push(MemHost {
                name: name.to_string(),
                ip: ip.to_string(),
                region: region.to_string(),
                az: az.to_string(),
                rack: rack.to_string(),
                cpu_cores,
                mem_gb,
                disk_gb,
                agent_port,
                status: status.to_string(),
                next_port: 35_000,
                created_at: now,
                updated_at: now,
            });
        }
    }

    fn host_list(&self) -> Vec<Value> {
        let g = self.inner.lock().unwrap();
        let mut list: Vec<Value> = g
            .hosts
            .iter()
            .map(|h| {
                json!({
                    "name": h.name, "ip": h.ip, "region": h.region, "az": h.az, "rack": h.rack,
                    "cpu_cores": h.cpu_cores, "mem_gb": h.mem_gb, "disk_gb": h.disk_gb,
                    "agent_port": h.agent_port,
                    "status": h.status, "next_port": h.next_port,
                    "created_at": h.created_at, "updated_at": h.updated_at,
                })
            })
            .collect();
        list.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        list
    }

    fn host_delete(&self, name: &str) -> bool {
        let mut g = self.inner.lock().unwrap();
        let before = g.hosts.len();
        g.hosts.retain(|h| h.name != name);
        g.hosts.len() != before
    }

    fn host_alloc_port(&self, name: &str) -> Option<u64> {
        let mut g = self.inner.lock().unwrap();
        let h = g.hosts.iter_mut().find(|h| h.name == name)?;
        h.next_port += 1;
        Some(h.next_port)
    }

    fn alert_counts(&self) -> (u64, u64, u64) {
        let g = self.inner.lock().unwrap();
        let mut c = (0u64, 0u64, 0u64);
        for a in g.alerts.iter() {
            if a.status == "open" || a.status == "ack" {
                match a.severity.as_str() {
                    "critical" => c.0 += 1,
                    "warn" => c.1 += 1,
                    _ => c.2 += 1,
                }
            }
        }
        c
    }
}

// ═════════════════════════ 工具 ═════════════════════════

fn esc(v: &str) -> String {
    v.replace('\\', "\\\\").replace('\'', "\\'")
}

fn opt(v: Option<u64>) -> String {
    v.map(|x| x.to_string()).unwrap_or_else(|| "NULL".into())
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        s.chars().take(n).collect()
    }
}

fn json_vec(out: &Result<String, String>) -> Vec<Value> {
    match out {
        Ok(s) if !s.trim().is_empty() => serde_json::from_str(s.trim()).unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn env_or(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}

/// 口令盐:时间+pid+进程内计数混拼(离线无 rand;非安全敏感场景足够)
static SALT_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
fn salt_bytes() -> [u8; 16] {
    let mut b = [0u8; 16];
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let c = SALT_COUNTER
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        .wrapping_mul(0x9E3779B97F4A7C15);
    let pid = std::process::id() as u64;
    let x = t ^ c.rotate_left(13) ^ (pid << 32);
    for i in 0..16 {
        b[i] = ((x >> ((i % 8) * 8)) ^ (t >> ((i / 2) % 8 * 8)) ^ (pid >> i)) as u8;
    }
    b
}

/// 计算口令哈希(salt:pass)
pub fn hash_password(salt: &str, pass: &str) -> String {
    crate::sha256::to_hex(&crate::sha256::digest(format!("{salt}:{pass}").as_bytes()))
}

/// 生成 (salt, hash)(供创建用户/改密)
pub fn password_salt_hash(pass: &str) -> (String, String) {
    let salt = crate::sha256::to_hex(&salt_bytes());
    (salt.clone(), hash_password(&salt, pass))
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

const DDL: &str = "CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR(96) PRIMARY KEY,
    kind VARCHAR(32) NOT NULL,
    instance VARCHAR(96) NOT NULL,
    status VARCHAR(16) NOT NULL,
    created_at BIGINT NOT NULL,
    started_at BIGINT NULL,
    finished_at BIGINT NULL,
    KEY idx_tasks_instance (instance)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS task_nodes (
    task_id VARCHAR(96) NOT NULL,
    node_id VARCHAR(96) NOT NULL,
    name VARCHAR(192) NOT NULL,
    deps_json TEXT NOT NULL,
    steps_json MEDIUMTEXT NOT NULL,
    status VARCHAR(16) NOT NULL,
    output MEDIUMTEXT NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    retries INT NOT NULL DEFAULT 0,
    timeout_secs BIGINT NULL,
    started_at BIGINT NULL,
    finished_at BIGINT NULL,
    PRIMARY KEY (task_id, node_id),
    KEY idx_nodes_task (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS audit_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts BIGINT NOT NULL,
    user VARCHAR(64) NOT NULL,
    instance VARCHAR(96) NOT NULL,
    action VARCHAR(96) NOT NULL,
    params VARCHAR(255) NOT NULL,
    result VARCHAR(255) NOT NULL,
    task_id VARCHAR(96) NOT NULL DEFAULT '',
    KEY idx_audit_ts (ts),
    KEY idx_audit_instance (instance)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS instances (
    name VARCHAR(96) PRIMARY KEY,
    region VARCHAR(64) NOT NULL DEFAULT '',
    az VARCHAR(64) NOT NULL DEFAULT '',
    shard VARCHAR(96) NOT NULL DEFAULT '',
    tenant VARCHAR(96) NOT NULL DEFAULT '',
    data MEDIUMTEXT NOT NULL,
    status VARCHAR(16) NOT NULL,
    updated_at BIGINT NOT NULL,
    KEY idx_instances_region (region, status),
    KEY idx_instances_tenant (tenant)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS task_seq (
    kind VARCHAR(32) PRIMARY KEY,
    seq BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS instance_locks (
    name VARCHAR(96) PRIMARY KEY,
    holder VARCHAR(128) NOT NULL,
    lease_until BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS users (
    user VARCHAR(64) PRIMARY KEY,
    salt VARCHAR(64) NOT NULL,
    pass_hash CHAR(64) NOT NULL,
    enabled INT NOT NULL DEFAULT 1,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS roles (
    name VARCHAR(64) PRIMARY KEY,
    description VARCHAR(255) NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS user_roles (
    user VARCHAR(64) NOT NULL,
    role VARCHAR(64) NOT NULL,
    PRIMARY KEY (user, role),
    KEY idx_ur_role (role)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS role_perms (
    role VARCHAR(64) NOT NULL,
    perm VARCHAR(64) NOT NULL,
    PRIMARY KEY (role, perm),
    KEY idx_rp_perm (perm)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS alerts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts BIGINT NOT NULL,
    instance VARCHAR(96) NOT NULL,
    kind VARCHAR(64) NOT NULL,
    severity VARCHAR(16) NOT NULL,
    message VARCHAR(512) NOT NULL DEFAULT '',
    status VARCHAR(16) NOT NULL DEFAULT 'open',
    assignee VARCHAR(64) NOT NULL DEFAULT '',
    handled_at BIGINT NULL,
    resolved_at BIGINT NULL,
    KEY idx_alerts_status (status, severity, instance),
    KEY idx_alerts_instance (instance)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS evidence_snapshots (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts BIGINT NOT NULL,
    instance VARCHAR(96) NOT NULL,
    kind VARCHAR(32) NOT NULL DEFAULT 'degrade',
    reason VARCHAR(240) NOT NULL DEFAULT '',
    facts_json MEDIUMTEXT NOT NULL,
    KEY idx_ev_inst_ts (instance, ts),
    KEY idx_ev_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
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
CREATE TABLE IF NOT EXISTS slow_digest_snapshots (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts BIGINT NOT NULL,
    instance VARCHAR(96) NOT NULL,
    node VARCHAR(64) NOT NULL DEFAULT '',
    schema_name VARCHAR(96) NOT NULL DEFAULT '',
    digest CHAR(64) NOT NULL DEFAULT '',
    digest_text VARCHAR(1024) NOT NULL DEFAULT '',
    count_star BIGINT NOT NULL DEFAULT 0,
    sum_ms BIGINT NOT NULL DEFAULT 0,
    avg_ms BIGINT NOT NULL DEFAULT 0,
    max_ms BIGINT NOT NULL DEFAULT 0,
    first_seen BIGINT NOT NULL DEFAULT 0,
    last_seen BIGINT NOT NULL DEFAULT 0,
    KEY idx_sls_ts (ts),
    KEY idx_sls_digest (digest, ts),
    KEY idx_sls_inst (instance, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS slow_baselines (
    instance VARCHAR(96) NOT NULL,
    node VARCHAR(64) NOT NULL,
    digest CHAR(64) NOT NULL,
    count_star BIGINT NOT NULL DEFAULT 0,
    sum_ms BIGINT NOT NULL DEFAULT 0,
    seen_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (instance, node, digest)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS slow_governance (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    digest CHAR(64) NOT NULL,
    digest_text VARCHAR(1024) NOT NULL DEFAULT '',
    status VARCHAR(16) NOT NULL DEFAULT 'open',
    assignee VARCHAR(64) NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    resolved_at BIGINT NULL,
    advice_json MEDIUMTEXT NULL,
    KEY idx_sgov_status (status, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS backup_outbox (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event VARCHAR(32) NOT NULL,
    instance VARCHAR(96) NOT NULL,
    node VARCHAR(96) NOT NULL DEFAULT '',
    idempotency_key VARCHAR(160) NOT NULL,
    payload_json MEDIUMTEXT NOT NULL,
    state VARCHAR(16) NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    next_at BIGINT NOT NULL DEFAULT 0,
    last_error VARCHAR(240) NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    UNIQUE KEY uk_boutbox_ikey (idempotency_key),
    KEY idx_boutbox_state (state, next_at),
    KEY idx_boutbox_inst (instance, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS rds_user_modules (
    name VARCHAR(96) PRIMARY KEY,
    category VARCHAR(32) NOT NULL DEFAULT 'other',
    `desc` VARCHAR(480) NOT NULL DEFAULT '',
    steps_json MEDIUMTEXT NOT NULL,
    created_by VARCHAR(64) NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    KEY idx_mod_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS rds_dts (
    instance VARCHAR(96) NOT NULL,
    node VARCHAR(96) NOT NULL,
    engine VARCHAR(32) NOT NULL DEFAULT 'canal',
    container VARCHAR(128) NOT NULL DEFAULT '',
    target_label VARCHAR(128) NOT NULL DEFAULT '',
    status VARCHAR(16) NOT NULL DEFAULT 'creating',
    last_error VARCHAR(240) NOT NULL DEFAULT '',
    spec VARCHAR(48) NOT NULL DEFAULT '',
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (instance, node),
    KEY idx_dts_inst (instance)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS rds_hosts (
    name VARCHAR(96) PRIMARY KEY,
    ip VARCHAR(64) NOT NULL DEFAULT '',
    region VARCHAR(48) NOT NULL DEFAULT '',
    az VARCHAR(48) NOT NULL DEFAULT '',
    rack VARCHAR(48) NOT NULL DEFAULT '',
    cpu_cores INT NOT NULL DEFAULT 0,
    mem_gb INT NOT NULL DEFAULT 0,
    disk_gb INT NOT NULL DEFAULT 0,
    agent_port INT NOT NULL DEFAULT 0,
    status VARCHAR(16) NOT NULL DEFAULT 'running',
    next_port BIGINT NOT NULL DEFAULT 35000,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS capacity_samples (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts BIGINT NOT NULL,
    instance VARCHAR(96) NOT NULL,
    node VARCHAR(96) NOT NULL DEFAULT 'master',
    disk_used_bytes BIGINT NOT NULL DEFAULT 0,
    disk_total_bytes BIGINT NOT NULL DEFAULT 0,
    data_gib DOUBLE NOT NULL DEFAULT 0,
    KEY idx_cap_inst_ts (instance, ts),
    KEY idx_cap_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS reports (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts BIGINT NOT NULL,
    period VARCHAR(16) NOT NULL,
    type VARCHAR(16) NOT NULL,
    text MEDIUMTEXT NOT NULL,
    counts_json MEDIUMTEXT NOT NULL,
    KEY idx_reports_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";

#[cfg(test)]
mod tests {
    use super::*;

    fn mem() -> Store {
        Store::from_backend(MemoryBackend::new())
    }

    #[test]
    fn memory_backend_task_lifecycle_and_views() {
        let s = mem();
        let id = format!("t-create-{}", s.next_task_seq("create"));
        s.upsert_task(&id, "create", "demo", "pending", 1, None, None);
        s.upsert_node(
            &id,
            "n1",
            "节点一",
            "[]",
            "[{\"Noop\":{\"note\":\"x\"}}]",
            "pending",
            "",
            0,
            1,
            Some(60),
            None,
            None,
        );
        s.upsert_node(
            &id,
            "n2",
            "节点二",
            "[\"n1\"]",
            "[]",
            "success",
            "done",
            1,
            0,
            None,
            Some(2),
            Some(3),
        );
        let v = s.task_view(&id).expect("view");
        assert_eq!(v["status"], "pending");
        assert_eq!(v["nodes"].as_array().unwrap().len(), 2);
        assert_eq!(v["instance"], "demo");
        // 更新状态
        s.update_task_status(&id, "success", Some(9));
        assert_eq!(s.task_view(&id).unwrap()["status"], "success");
        assert_eq!(s.task_views(10).len(), 1);
        assert_eq!(s.task_views_by_instance("demo", 10).len(), 1);
        assert_eq!(s.task_views_by_instance("other", 10).len(), 0);
    }

    #[test]
    fn memory_backend_mark_interrupted_and_audit() {
        let s = mem();
        s.upsert_task("t1", "create", "a", "running", 1, Some(1), None);
        s.upsert_task("t2", "destroy", "a", "success", 1, Some(1), Some(2));
        s.upsert_node(
            "t1", "n1", "n", "[]", "[]", "running", "", 1, 0, None, None, None,
        );
        let n = s.mark_interrupted();
        assert_eq!(n, 1);
        let v = s.task_view("t1").unwrap();
        assert_eq!(v["status"], "failed");
        assert_eq!(v["nodes"][0]["status"], "skipped");
        assert_eq!(s.task_view("t2").unwrap()["status"], "success");
        s.audit("u", "a", "create", "p", "ok", "t1");
        s.audit("u", "a", "destroy", "p", "ok", "t2");
        let list = s.audit_list(10, None, None, None);
        assert_eq!(list.len(), 2);
        assert_eq!(list[0]["action"], "destroy"); // 最新在前
    }

    #[test]
    fn memory_backend_instance_lease() {
        let s = mem();
        // 首个持有者获取成功
        assert!(s.lock_instance("demo", "c1", 60));
        // 其它持有者在有效期内获取失败
        assert!(!s.lock_instance("demo", "c2", 60));
        // 同 holder 重入续期成功
        assert!(s.lock_instance("demo", "c1", 60));
        // 续约仅限持有者
        assert!(s.renew_instance_lock("demo", "c1", 60));
        assert!(!s.renew_instance_lock("demo", "c2", 60));
        // 释放后其它持有者可获取
        s.unlock_instance("demo", "c2"); // 非持有者释放无效
        assert!(!s.lock_instance("demo", "c2", 60));
        s.unlock_instance("demo", "c1");
        assert!(s.lock_instance("demo", "c2", 60));
        // 非持有者释放无效;持有者释放后才可被第三方获取
        s.unlock_instance("demo", "c1");
        assert!(!s.lock_instance("demo", "c3", 60));
        s.unlock_instance("demo", "c2");
        assert!(s.lock_instance("demo", "c3", 60));
        s.unlock_instance("demo", "c3");
    }

    #[test]
    fn memory_backend_instance_lease_expiry() {
        let s = mem();
        assert!(s.lock_instance("demo", "c1", 1)); // 1s 租约
        std::thread::sleep(std::time::Duration::from_millis(1300));
        assert!(s.lock_instance("demo", "c2", 60)); // 过期后可被抢占
    }

    #[test]
    fn memory_backend_instances() {
        let s = mem();
        s.instance_upsert("demo", "cn-bj", "t1", r#"{"name":"demo"}"#, "running", 1);
        s.instance_upsert("demo2", "cn-sh", "", r#"{"name":"demo2"}"#, "degraded", 2);
        let all = s.instance_load_all();
        assert_eq!(all.len(), 2);
        let map: HashMap<String, String> = all.into_iter().collect();
        assert_eq!(map["demo2"], r#"{"name":"demo2"}"#);
        // upsert 覆盖
        s.instance_upsert(
            "demo",
            "cn-bj",
            "t1",
            r#"{"name":"demo","x":1}"#,
            "failed",
            3,
        );
        assert_eq!(s.instance_load_all().len(), 2);
    }

    #[test]
    fn memory_backend_evidence_and_audit_since() {
        let s = mem();
        // audit_since 过滤
        s.audit("u", "a", "create", "p", "ok", "t1");
        let rows = s.audit_since(0, 100);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["action"], "create");
        let rows0 = s.audit_since(u64::MAX, 100);
        assert_eq!(rows0.len(), 0);
        // evidence 写入/读取
        s.evidence_insert(
            "demo",
            "degrade",
            "从库 rds-demo-slave-1 复制中断",
            r#"{"slaves":[{"container":"rds-demo-slave-1","repl":"STOPPED"}]}"#,
        );
        s.evidence_insert(
            "demo",
            "degrade",
            "代理不可达",
            r#"{"proxy":{"reachable":false}}"#,
        );
        s.evidence_insert("other", "degrade", "容器缺失", r#"{}"#);
        let latest = s.evidence_latest("demo", 10);
        assert_eq!(latest.len(), 2);
        assert_eq!(latest[0]["reason"], "代理不可达"); // 新→旧
        assert_eq!(latest[0]["facts"]["proxy"]["reachable"], false);
        assert_eq!(latest[1]["kind"], "degrade");
        assert_eq!(s.evidence_latest("demo", 1).len(), 1);
        assert_eq!(s.evidence_latest("nope", 10).len(), 0);
        // evidence_since:kind 过滤 + 时间过滤
        assert_eq!(s.evidence_since(Some("degrade"), 0, 100).len(), 3);
        assert_eq!(s.evidence_since(Some("task_fail"), 0, 100).len(), 0);
        assert_eq!(s.evidence_since(None, u64::MAX, 100).len(), 0);
        // kind 大小写/空串等价于不过滤
        assert_eq!(s.evidence_since(Some(""), 0, 100).len(), 3);
    }
}

#[cfg(test)]
mod alert_tests {
    use super::*;

    fn mem() -> Store {
        Store::from_backend(MemoryBackend::new())
    }

    #[test]
    fn memory_alerts_lifecycle() {
        let s = mem();
        s.alert_open("a", "degraded", "warn", "容器缺失");
        s.alert_open("a", "degraded", "warn", "容器缺失-重复应被去重");
        s.alert_open("a", "task_failed", "info", "任务失败");
        s.alert_open("b", "degraded", "critical", "复制中断");
        assert_eq!(s.alert_list(100, None, None, None).len(), 3);
        let (c, w, i) = s.alert_counts();
        assert_eq!((c, w, i), (1, 1, 1));
        // 按严重度过滤
        assert_eq!(s.alert_list(100, Some("warn"), None, None).len(), 1);
        // ack 记录处理人/时间
        let list = s.alert_list(100, None, None, None);
        let id = list[0]["id"].as_u64().unwrap();
        assert!(s.alert_action(id, "ack", "alice"));
        let after = s.alert_list(100, None, None, None);
        let me = after.iter().find(|x| x["id"].as_u64() == Some(id)).unwrap();
        assert_eq!(me["status"], "ack");
        assert_eq!(me["assignee"], "alice");
        assert!(me["handled_at"].as_u64().is_some());
        // resolve 单条
        assert!(s.alert_action(id, "resolve", "bob"));
        assert_eq!(s.alert_list(100, None, Some("resolved"), None).len(), 1);
        // 实例恢复:关闭该实例全部未决告警
        s.alert_resolve_instance("a");
        assert_eq!(s.alert_list(100, None, Some("open"), None).len(), 0);
        assert_eq!(s.alert_list(100, None, Some("ack"), None).len(), 0);
        assert_eq!(s.alert_counts(), (0, 0, 0)); // 全部已处置/关闭
    }
}

#[cfg(test)]
mod query_audit_tests {
    use super::*;

    fn mem() -> Store {
        Store::from_backend(MemoryBackend::new())
    }

    #[test]
    fn memory_query_audit_roundtrip() {
        let s = mem();
        s.query_audit_insert(
            "dba1",
            "demo",
            "master",
            "SELECT * FROM appdb.users WHERE id=1",
            "hash1",
            true,
            2,
            false,
            5,
            true,
            "",
        );
        s.query_audit_insert(
            "dba1",
            "demo",
            "master",
            "UPDATE appdb.users SET a=1",
            "hash2",
            false,
            0,
            false,
            3,
            false,
            "ERROR 1142 (42000): SELECT command denied",
        );
        // 新→旧;完整 SQL 原文可回溯
        let all = s.query_audit_list(10, None, None, None);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0]["sql"], "UPDATE appdb.users SET a=1");
        assert_eq!(all[1]["sql_hash"], "hash1");
        assert_eq!(all[1]["rows_returned"], 2);
        assert_eq!(all[0]["ok"], serde_json::json!(false));
        // instance/user/since 过滤
        assert_eq!(s.query_audit_list(10, Some("demo"), None, None).len(), 2);
        assert_eq!(s.query_audit_list(10, Some("other"), None, None).len(), 0);
        assert_eq!(s.query_audit_list(10, None, Some("dba1"), None).len(), 2);
        assert_eq!(s.query_audit_list(10, None, Some("nobody"), None).len(), 0);
        assert_eq!(s.query_audit_list(10, None, None, Some(u64::MAX)).len(), 0);
    }
}

#[cfg(test)]
mod slow_tests {
    use super::*;

    fn mem() -> Store {
        Store::from_backend(MemoryBackend::new())
    }

    fn sample(inst: &str, digest: &str, count: u64, sum: u64) -> SlowSample {
        SlowSample {
            instance: inst.to_string(),
            node: "master".to_string(),
            schema_name: "appdb".to_string(),
            digest: digest.to_string(),
            digest_text: format!("select from {digest} where a=?"),
            count_star: count,
            sum_ms: sum,
            avg_ms: if count > 0 { sum / count } else { 0 },
            max_ms: sum,
            first_seen: 1000,
            last_seen: 2000,
        }
    }

    #[test]
    fn memory_slow_snapshots_and_window() {
        let s = mem();
        s.slow_snapshots_insert(&[sample("a", "d1", 5, 500), sample("b", "d1", 10, 1000)]);
        s.slow_snapshots_insert(&[sample("a", "d2", 1, 100)]);
        let rows = s.slow_window(0, None, None, 100);
        assert_eq!(rows.len(), 3);
        assert_eq!(s.slow_window(0, Some("a"), None, 100).len(), 2);
        assert_eq!(s.slow_window(0, None, Some("d1"), 100).len(), 2);
        assert_eq!(s.slow_window(u64::MAX, None, None, 100).len(), 0);
    }

    #[test]
    fn memory_slow_baselines_replace_semantics() {
        let s = mem();
        let base = |digest: &str, count: u64| SlowBase {
            instance: "a".into(),
            node: "master".into(),
            digest: digest.into(),
            count_star: count,
            sum_ms: count * 100,
            seen_at: 1,
        };
        s.slow_baselines_set("a", "master", &[base("d1", 7), base("d2", 3)]);
        assert_eq!(s.slow_baselines_load().len(), 2);
        // 覆盖同一 (instance,node):先删后插
        s.slow_baselines_set("a", "master", &[base("d1", 9)]);
        let all = s.slow_baselines_load();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].digest, "d1");
        assert_eq!(all[0].count_star, 9);
    }

    #[test]
    fn memory_slow_gov_lifecycle_and_prune() {
        let s = mem();
        assert!(s.slow_gov_ensure("d1", "select a=?"));
        assert!(
            !s.slow_gov_ensure("d1", "select a=?"),
            "同 digest open 不重复"
        );
        assert!(s.slow_gov_ensure("d2", "update t=?"));
        assert_eq!(s.slow_gov_list(10, None).len(), 2);
        let list = s.slow_gov_list(10, Some("open"));
        assert_eq!(list.len(), 2);
        let id = list[0]["id"].as_u64().unwrap();
        assert!(s.slow_gov_action(id, "ack", "dba1"));
        assert_eq!(s.slow_gov_list(10, Some("ack")).len(), 1);
        assert!(s.slow_gov_action(id, "resolve", "dba1"));
        assert_eq!(s.slow_gov_list(10, Some("resolved")).len(), 1);
        // 快照清理 + 已解决治理项清理
        s.slow_snapshots_insert(&[sample("a", "d1", 1, 10)]);
        let (n1, n2) = s.slow_prune(0, u64::MAX); // 快照 before=0 → 不删;治理全部清理
        assert_eq!(n1, 0);
        assert_eq!(n2, 1);
        assert_eq!(s.slow_gov_list(10, None).len(), 1); // d2 仍 open
        let (n3, _) = s.slow_prune(u64::MAX, 0);
        assert_eq!(n3, 1);
    }
}

#[cfg(test)]
mod backup_outbox_tests {
    use super::*;

    fn mem() -> Store {
        Store::from_backend(MemoryBackend::new())
    }

    #[test]
    fn memory_backend_module_crud() {
        let s = mem();
        assert!(s.module_list().is_empty());
        s.module_upsert(
            "清日志",
            "清理",
            "删除容器内旧日志",
            r#"[{"Noop":{"note":"ok"}}]"#,
            "admin",
            1,
        );
        let list = s.module_list();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["name"], "清日志");
        assert_eq!(list[0]["steps_json"], r#"[{"Noop":{"note":"ok"}}]"#);
        // 同名覆盖=新版本
        s.module_upsert(
            "清日志",
            "维护",
            "v2",
            r#"[{"Noop":{"note":"v2"}}]"#,
            "admin",
            2,
        );
        let list2 = s.module_list();
        assert_eq!(list2.len(), 1);
        assert_eq!(list2[0]["desc"], "v2");
        assert_eq!(list2[0]["updated_at"], 2);
        // 删除
        assert!(s.module_delete("清日志"));
        assert!(s.module_list().is_empty());
        assert!(!s.module_delete("不存在"));
    }

    #[test]
    fn memory_outbox_dedupe_poll_mark() {
        let s = mem();
        assert!(s.backup_outbox_enqueue("register_instance", "demo", "master", "k1", r#"{"a":1}"#));
        // 同 key 幂等拒绝
        assert!(!s.backup_outbox_enqueue(
            "register_instance",
            "demo",
            "master",
            "k1",
            r#"{"a":1}"#
        ));
        assert!(s.backup_outbox_enqueue("heartbeat", "demo", "", "k2", "{}"));
        let polled = s.backup_outbox_poll(10, now_secs() + 5);
        assert_eq!(polled.len(), 2);
        assert_eq!(polled[0]["state"], "delivering");
        let id = polled[0]["id"].as_u64().unwrap();
        s.backup_outbox_mark(id, "done", 1, 0, "");
        let poll2 = s.backup_outbox_poll(10, now_secs() + 5);
        assert_eq!(poll2.len(), 1, "done 不再领取");
    }

    #[test]
    fn memory_outbox_reg_state_and_requeue_prune() {
        let s = mem();
        s.backup_outbox_enqueue("register_node", "demo", "a", "ka", "{}");
        s.backup_outbox_enqueue("register_node", "demo", "b", "kb", "{}");
        let polled = s.backup_outbox_poll(10, now_secs() + 5);
        for p in &polled {
            s.backup_outbox_mark(p["id"].as_u64().unwrap(), "done", 1, 0, "");
        }
        // 派生:registered
        let st = s.backup_outbox_reg_state(Some("demo"));
        assert_eq!(st.len(), 2);
        assert!(st.iter().all(|x| x["reg_state"] == "registered"));
        // dead → requeue
        s.backup_outbox_mark(polled[0]["id"].as_u64().unwrap(), "dead", 10, 0, "超时");
        assert!(s.backup_outbox_requeue(polled[0]["id"].as_u64().unwrap()));
        assert!(!s.backup_outbox_requeue(9999));
        let st2 = s.backup_outbox_reg_state(None);
        assert_eq!(
            st2.iter()
                .filter(|x| x["reg_state"] == "registered")
                .count(),
            1
        );
        // 清理 done/dead 早于 before
        assert_eq!(s.backup_outbox_prune(now_secs() + 1), 1); // dead 该删;done 的 updated_at 为 now,未删
    }
}

#[cfg(test)]
mod capacity_report_tests {
    use super::*;

    fn mem() -> Store {
        Store::from_backend(MemoryBackend::new())
    }

    #[test]
    fn memory_capacity_roundtrip_since_prune() {
        let s = mem();
        s.capacity_insert(&[
            CapSample {
                instance: "demo".into(),
                node: "master".into(),
                disk_used_bytes: 10,
                disk_total_bytes: 100,
                data_gib: 5.0,
            },
            CapSample {
                instance: "demo".into(),
                node: "master".into(),
                disk_used_bytes: 20,
                disk_total_bytes: 100,
                data_gib: 6.0,
            },
        ]);
        s.capacity_insert(&[CapSample {
            instance: "other".into(),
            node: "master".into(),
            disk_used_bytes: 1,
            disk_total_bytes: 10,
            data_gib: 1.0,
        }]);
        let rows = s.capacity_since(Some("demo"), 0, 100);
        assert_eq!(rows.len(), 2);
        // 升序 + 字段完整
        assert_eq!(
            rows[0]["ts"].as_u64().unwrap(),
            rows[1]["ts"].as_u64().unwrap()
        ); // 同批同 ts
        assert_eq!(rows[0]["disk_used_bytes"], 10);
        assert_eq!(rows[1]["data_gib"], 6.0);
        assert_eq!(s.capacity_since(None, 0, 100).len(), 3);
        assert_eq!(s.capacity_since(Some("demo"), u64::MAX, 100).len(), 0);
        assert_eq!(s.capacity_since(None, 0, 2).len(), 2);
        // 保留清理
        assert_eq!(s.capacity_prune(now_secs() + 1), 3);
        assert_eq!(s.capacity_since(None, 0, 100).len(), 0);
    }

    #[test]
    fn memory_reports_roundtrip_list_prune() {
        let s = mem();
        s.report_insert("today", "daily", "日报一", r#"{"created":1}"#);
        s.report_insert("week", "weekly", "周报一", r#"{"created":2}"#);
        let all = s.reports_list(None, 0, 100);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0]["type"], "weekly"); // 新→旧
        assert_eq!(all[0]["counts"]["created"], 2);
        assert_eq!(s.reports_list(Some("daily"), 0, 100).len(), 1);
        assert_eq!(s.reports_list(None, u64::MAX, 100).len(), 0);
        assert_eq!(s.reports_prune(now_secs() + 1), 2);
    }

    #[test]
    fn memory_slow_gov_advise_semantics() {
        let s = mem();
        assert!(s.slow_gov_ensure("d1", "select a=?"));
        assert!(s.slow_gov_ensure("d2", "update t=?"));
        // 命中 open 行写建议;advice 随列表返回
        assert!(s.slow_gov_advise(
            "d1",
            r#"{"rising_pct":null,"reasons":["avg"],"suggestions":[]}"#
        ));
        let list = s.slow_gov_list(10, None);
        let d1 = list.iter().find(|x| x["digest"] == "d1").unwrap();
        assert_eq!(d1["advice"]["reasons"][0], "avg");
        // 未知 digest / 已 closed 不命中
        assert!(!s.slow_gov_advise("nope", "{}"));
        let id = list.iter().find(|x| x["digest"] == "d2").unwrap()["id"]
            .as_u64()
            .unwrap();
        assert!(s.slow_gov_action(id, "resolve", "dba1"));
        assert!(!s.slow_gov_advise("d2", "{}"), "resolved 行不更新建议");
        // 空建议行视图为 null
        assert_eq!(
            s.slow_gov_list(10, None)
                .iter()
                .find(|x| x["digest"] == "d2")
                .unwrap()["advice"],
            Value::Null
        );
    }

    #[test]
    fn memory_host_registry_crud_and_alloc() {
        // Host 注册表:登记/更新/列表/删除 + 按 Host 端口高水位分配递增
        let s = mem();
        assert!(s.host_list().is_empty());
        s.host_upsert(
            "host-cn-north-01",
            "10.0.0.1",
            "cn-bj",
            "az1",
            "rack-a1",
            32,
            64,
            2000,
            9191,
            "running",
        );
        s.host_upsert(
            "host-cn-north-02",
            "10.0.0.2",
            "cn-bj",
            "az1",
            "rack-a2",
            16,
            32,
            1000,
            0,
            "running",
        );
        let all = s.host_list();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0]["name"], "host-cn-north-01"); // 按 name 排序
        assert_eq!(all[0]["cpu_cores"], 32);
        assert_eq!(all[0]["agent_port"], 9191); // agent 端口随清单返回
        assert_eq!(all[0]["next_port"], 35_000);
        // 更新不改端口高水位
        s.host_upsert(
            "host-cn-north-01",
            "10.0.0.1",
            "cn-bj",
            "az1",
            "rack-a1",
            64,
            128,
            4000,
            9191,
            "maintenance",
        );
        assert_eq!(
            s.host_list()
                .iter()
                .find(|h| h["name"] == "host-cn-north-01")
                .unwrap()["cpu_cores"],
            64
        );
        // 按 Host 分配端口:连续自增,互不干扰
        assert_eq!(s.host_alloc_port("host-cn-north-01"), Some(35_001));
        assert_eq!(s.host_alloc_port("host-cn-north-01"), Some(35_002));
        assert_eq!(s.host_alloc_port("host-cn-north-02"), Some(35_001)); // 每 Host 独立高水位
        assert_eq!(s.host_alloc_port("ghost"), None); // 未登记 Host
                                                      // 删除
        assert!(s.host_delete("host-cn-north-02"));
        assert!(!s.host_delete("host-cn-north-02"));
        assert_eq!(s.host_list().len(), 1);
    }
}
