// RDS 管控 —— HTTP 接口(挂入 mgmt 路由)
//
// 路由(全部需登录;POST 用 query 参数,与 /api/kill?cid= 一致):
//   GET  /rds                      管控页面
//   GET  /api/rds/instances        实例列表
//   GET  /api/rds/instance?name=x  单实例详情
//   GET  /api/rds/tasks?instance=x 任务列表(可过滤实例)
//   GET  /api/rds/task?id=x        任务详情(DAG 节点状态)
//   POST /api/rds/create?name=x    创建实例
//   POST /api/rds/destroy?name=x   销毁实例
//   POST /api/rds/scaleout?name=x&role=stats|backup|read  扩容从节点

use crate::{dag::{Step, TaskNode}, instance::Role, manager};
use serde_json::json;

/// GET /api/rds/instances?region=&az=&status=&tenant=&q=&limit=&offset=
/// 返回 { instances: 页数据, total, limit, offset };列表项剔除敏感字段(root_password)
pub fn instances(query: &str) -> String {
    let opt = |k: &str| {
        let v = qparam(query, k);
        if v.is_empty() { None } else { Some(v) }
    };
    let limit = qparam(query, "limit").parse::<usize>().unwrap_or(200).min(1000);
    let offset = qparam(query, "offset").parse::<usize>().unwrap_or(0);
    let region = opt("region");
    let az = opt("az");
    let status = opt("status");
    let tenant = opt("tenant");
    let q = opt("q");
    let (items, total) = manager().list_filtered(
        region.as_deref(),
        az.as_deref(),
        status.as_deref(),
        tenant.as_deref(),
        q.as_deref(),
        offset,
        limit,
    );
    // 列表投影:去掉 root_password(详情接口 /instance 保留)
    let items: Vec<serde_json::Value> = items
        .into_iter()
        .map(|mut v| {
            v.as_object_mut().map(|o| o.remove("root_password"));
            v
        })
        .collect();
    json!({ "instances": items, "total": total, "limit": limit, "offset": offset }).to_string()
}

pub fn instance(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name");
    if name.is_empty() {
        return (400, "application/json", json!({ "error": "缺少 name 参数" }).to_string());
    }
    match manager().get(&name) {
        Some(v) => (200, "application/json", json!({ "instance": v }).to_string()),
        None => (404, "application/json", json!({ "error": format!("实例 {name} 不存在") }).to_string()),
    }
}

pub fn tasks(query: &str) -> String {
    let inst = qparam(query, "instance");
    let list = if inst.is_empty() {
        manager().scheduler.list()
    } else {
        manager().tasks(&inst)
    };
    json!({ "tasks": list }).to_string()
}

pub fn task(query: &str) -> (u16, &'static str, String) {
    let id = qparam(query, "id");
    match manager().scheduler.get(&id) {
        Some(v) => (200, "application/json", json!({ "task": v }).to_string()),
        None => (404, "application/json", json!({ "error": format!("任务 {id} 不存在") }).to_string()),
    }
}

pub fn create(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name");
    let u = |v: &str| v.parse::<u64>().unwrap_or(0);
    let opts = crate::instance::CreateOpts {
        itype: {
            let t = qparam(query, "itype");
            if t.is_empty() { "async".to_string() } else { t }
        },
        proxies: qparam(query, "proxies").parse::<u32>().unwrap_or(2).clamp(1, 4),
        region: qparam(query, "region"),
        az: qparam(query, "az"),
        biz: qparam(query, "biz"),
        contact: qparam(query, "contact"),
        dba: qparam(query, "dba"),
        core: qparam(query, "core").as_str() == "1" || qparam(query, "core").eq_ignore_ascii_case("true"),
        spec: qparam(query, "spec"),
        shard_num: u(&qparam(query, "shard_num")),
        data_size: qparam(query, "data_size"),
        buffer_pool: qparam(query, "buffer_pool"),
        max_qps: u(&qparam(query, "max_qps")),
        max_tps: u(&qparam(query, "max_tps")),
        mysql_version: qparam(query, "mysql_version"),
        proxy_version: qparam(query, "proxy_version"),
        dts: qparam(query, "dts").as_str() == "1" || qparam(query, "dts").eq_ignore_ascii_case("true"),
        dts_spec: qparam(query, "dts_spec"),
    };
    match manager().create(&name, &opts) {
        Ok(tid) => (200, "application/json", json!({ "ok": true, "task_id": tid }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

pub fn destroy(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name");
    match manager().destroy(&name) {
        Ok(tid) => (200, "application/json", json!({ "ok": true, "task_id": tid }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

/// POST /api/rds/instance/delete?name= —— 彻底删除已销毁实例的记录(instances.destroy)
pub fn delete_instance(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name");
    if name.is_empty() {
        return (400, "application/json", json!({ "ok": false, "error": "缺少 name 参数" }).to_string());
    }
    match manager().delete_instance(&name) {
        Ok(()) => (200, "application/json", json!({ "ok": true }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

pub fn scaleout(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name");
    // 目标 region/az:留空=沿用实例默认(同区扩容);非空=按目标区扩容(跨区标签/agent)
    let region = qparam(query, "region");
    let az = qparam(query, "az");
    // 角色:read=在线读从;offline=离线从(备份/统计/大查询,每实例仅一个)
    // 兼容历史参数 stats/backup → 视为 offline
    let role = match qparam(query, "role").as_str() {
        "read" => Role::Read,
        "offline" | "stats" | "backup" | "" => Role::Offline,
        other => {
            return (
                400,
                "application/json",
                json!({ "ok": false, "error": format!("未知角色 {other}(支持 read/offline)") }).to_string(),
            )
        }
    };
    let region_o = if region.is_empty() { None } else { Some(region.as_str()) };
    let az_o = if az.is_empty() { None } else { Some(az.as_str()) };
    // shard:多分片实例分片级扩容目标(s1..sN);不传=实例级(单分片实例)/被多分片守卫拒绝
    let shard = qparam(query, "shard");
    let r = if shard.is_empty() {
        manager().scaleout(&name, role, region_o, az_o)
    } else {
        manager().scaleout_shard(&name, &shard, role, region_o, az_o)
    };
    match r {
        Ok(tid) => (200, "application/json", json!({ "ok": true, "task_id": tid }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

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

// ─── DTS 链路(canal):列表/创建/移除 ───

/// GET /api/rds/dts?instance= —— 全部或某实例的 DTS 链路
pub fn dts_list(query: &str) -> (u16, &'static str, String) {
    let inst = qparam(query, "instance");
    let items = manager().dts_list(if inst.is_empty() { None } else { Some(&inst) });
    (200, "application/json", json!({ "dts": items }).to_string())
}

/// GET /api/rds/orch/facts?instance= —— 复制/半同步事实(vtorc 式事实层,P1)
pub async fn orch_facts(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "instance");
    if name.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 instance 参数"}).to_string());
    }
    let facts = manager().orch_facts(&name).await;
    (200, "application/json", json!({ "facts": facts }).to_string())
}

/// GET /api/rds/orch/ops?instance= —— 受管切换动作视图(进行中 + 历史)
pub fn orch_ops(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "instance");
    if name.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 instance 参数"}).to_string());
    }
    let view = manager().orch_ops_view(&name);
    (200, "application/json", view.to_string())
}

/// POST /api/rds/orch/reparent?instance=&target=&mode= —— 受管主从切换(mode=auto(ERS)|planned(PRS))
pub fn orch_reparent(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    if instance.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 instance 参数"}).to_string());
    }
    let target = qparam(query, "target");
    let mode = qparam(query, "mode");
    let mode = if mode.is_empty() { "auto".to_string() } else { mode };
    let t = if target.is_empty() { None } else { Some(target) };
    match manager().orch_reparent(&instance, t.as_deref(), &mode) {
        Ok(opid) => (200, "application/json", json!({ "ok": true, "op_id": opid }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

/// POST /api/rds/orch/rollback?instance= —— 回滚到最近一次受管切换前的旧主
pub fn orch_rollback(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    if instance.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 instance 参数"}).to_string());
    }
    match manager().orch_rollback(&instance) {
        Ok(opid) => (200, "application/json", json!({ "ok": true, "op_id": opid }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

/// POST /api/rds/dts/create?instance=&node=&target=&spec=
pub fn dts_create(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    let node = qparam(query, "node");
    if instance.is_empty() || node.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 instance/node 参数"}).to_string());
    }
    let target = qparam(query, "target");
    let spec = qparam(query, "spec");
    let t = if target.is_empty() { None } else { Some(target) };
    let s = if spec.is_empty() { None } else { Some(spec) };
    match manager().dts_create(&instance, &node, t.as_deref(), s.as_deref()) {
        Ok(tid) => (200, "application/json", json!({ "ok": true, "task_id": tid }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

/// POST /api/rds/dts/remove?instance=&node=
pub fn dts_remove(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    let node = qparam(query, "node");
    if instance.is_empty() || node.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 instance/node 参数"}).to_string());
    }
    match manager().dts_remove(&instance, &node) {
        Ok(tid) => (200, "application/json", json!({ "ok": true, "task_id": tid }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

// ─── 物理机(Host)注册表与节点绑定(instances.view/manage;见 docs/physical-multi-site-ops.md §5) ───

/// GET /api/rds/hosts —— 机器清单(name/ip/region/az/rack/容量/状态/agent/端口高水位/绑定节点数)
pub fn hosts(_query: &str) -> (u16, &'static str, String) {
    let mgr = manager();
    let mut items = mgr.hosts();
    for it in items.iter_mut() {
        let nm = it["name"].as_str().unwrap_or("").to_string();
        it["bound"] = serde_json::json!(mgr.host_binding_count(&nm));
    }
    (200, "application/json", json!({ "hosts": items }).to_string())
}

/// POST /api/rds/hosts?name=&ip=&region=&az=&rack=&cpu=&mem_gb=&disk_gb=&agent_port= —— 登记/更新机器
pub fn host_create(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name");
    let u = |v: &str| v.trim().parse::<u32>().unwrap_or(0);
    let agent_port = qparam(query, "agent_port").parse::<u16>().unwrap_or(0);
    match manager().host_create(
        &name,
        &qparam(query, "ip"),
        &qparam(query, "region"),
        &qparam(query, "az"),
        &qparam(query, "rack"),
        u(&qparam(query, "cpu")),
        u(&qparam(query, "mem_gb")),
        u(&qparam(query, "disk_gb")),
        agent_port,
    ) {
        Ok(()) => (200, "application/json", json!({ "ok": true }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

/// POST /api/rds/hosts/delete?name= —— 删除机器(仍被节点绑定则拒绝)
pub fn host_delete(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name");
    match manager().host_delete(&name) {
        Ok(()) => (200, "application/json", json!({ "ok": true }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

/// POST /api/rds/hosts/status?name=&status= —— 机器状态(running|maintenance|retiring)
pub fn host_status(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name");
    let status = qparam(query, "status");
    match manager().host_set_status(&name, &status) {
        Ok(()) => (200, "application/json", json!({ "ok": true }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

/// POST /api/rds/host/assign?instance=&node=&host= —— 绑定实例节点到宿主机
pub fn host_assign(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    let node = qparam(query, "node");
    let host = qparam(query, "host");
    match manager().host_assign_node(&instance, &node, &host) {
        Ok(()) => (200, "application/json", json!({ "ok": true }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

/// POST /api/rds/host/clear?instance=&node= —— 解绑(回到本机直连)
pub fn host_clear(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    let node = qparam(query, "node");
    match manager().host_clear_node(&instance, &node) {
        Ok(()) => (200, "application/json", json!({ "ok": true }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

/// POST /api/rds/replace_node?instance=&node=&host= —— 节点替换到目标宿主机(P2-②;
/// 从节点;主节点替换请先 orch/reparent planned 切走主角色)
pub fn replace_node(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    let node = qparam(query, "node");
    let host = qparam(query, "host");
    if instance.is_empty() || node.is_empty() || host.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 instance/node/host 参数"}).to_string());
    }
    match manager().replace_node(&instance, &node, &host) {
        Ok(tid) => (200, "application/json", json!({ "ok": true, "task_id": tid }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

/// POST /api/rds/migrate?instance=&region=&az=&hosts=a,b —— 实例整体迁移(P2-③:
/// 从节点同身份替换 + 主节点受管切主 → 目标机房,成功后更新 region/az 事实)
pub fn migrate(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    let region = qparam(query, "region");
    let az = qparam(query, "az");
    let hosts = qparam(query, "hosts");
    if instance.is_empty() || region.is_empty() || az.is_empty() || hosts.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 instance/region/az/hosts 参数"}).to_string());
    }
    match manager().migrate_instance(&instance, &region, &az, &hosts) {
        Ok(tid) => (200, "application/json", json!({ "ok": true, "task_id": tid }).to_string()),
        Err(e) => (400, "application/json", json!({ "ok": false, "error": e }).to_string()),
    }
}

/// GET /api/rds/audit?instance=&action=&q=&limit=&offset=
/// 分页:服务端按过滤条件全量匹配后切片,返回 {audit, total, offset};不带参数时保持兼容(默认前 200)。
pub fn audit(query: &str) -> String {
    let opt = |k: &str| {
        let v = qparam(query, k);
        if v.is_empty() { None } else { Some(v) }
    };
    let limit = qparam(query, "limit").parse::<usize>().unwrap_or(200).clamp(1, 500);
    let offset = qparam(query, "offset").parse::<usize>().unwrap_or(0);
    let full = manager().audit(200_000, opt("instance").as_deref(), opt("action").as_deref(), opt("q").as_deref());
    let total = full.len();
    let page: Vec<serde_json::Value> = full.into_iter().skip(offset).take(limit).collect();
    serde_json::json!({ "audit": page, "total": total, "offset": offset }).to_string()
}

/// GET /api/rds/summary —— 状态分布 + 进行中任务数(运维首页)
pub fn summary() -> String {
    json!({ "summary": manager().summary() }).to_string()
}

// ─── 用户/角色/权限(S-安全) ───

pub fn permissions() -> String {
    let list: Vec<serde_json::Value> = crate::store::PERMISSIONS
        .iter()
        .map(|p| serde_json::json!({ "perm": p, "group": p.split('.').next().unwrap_or("") }))
        .collect();
    json!({ "permissions": list }).to_string()
}

pub fn users() -> String {
    json!({ "users": manager().store.users_list() }).to_string()
}

pub fn user_action(query: &str) -> (u16, &'static str, String) {
    let action = qparam(query, "action");
    let user = qparam(query, "user");
    let m = manager();
    match action.as_str() {
        "create" | "setpass" => {
            let pass = qparam(query, "pass");
            if user.is_empty() || pass.len() < 4 {
                return (400, "application/json", json!({"ok":false,"error":"用户名必填且密码至少 4 位"}).to_string());
            }
            let enabled = qparam(query, "enabled").as_str() != "0";
            let (salt, hash) = crate::store::password_salt_hash(&pass);
            m.store.user_upsert(&user, &salt, &hash, enabled);
            m.store.audit(&crate::auth::current_user(), "", if action=="create" {"user_create"} else {"user_setpass"}, &user, "ok", "");
            (200, "application/json", json!({"ok":true}).to_string())
        }
        "enabled" => {
            let enabled = qparam(query, "value").as_str() != "0";
            m.store.user_set_enabled(&user, enabled);
            m.store.audit(&crate::auth::current_user(), "", if enabled {"user_enable"} else {"user_freeze"}, &user, "ok", "");
            (200, "application/json", json!({"ok":true}).to_string())
        }
        "roles" => {
            let roles_csv = qparam(query, "roles");
            let roles: Vec<&str> = roles_csv.split(',').filter(|s| !s.is_empty()).collect();
            m.store.user_roles_set(&user, &roles);
            m.store.audit(&crate::auth::current_user(), "", "user_roles", &format!("{user} <- {roles:?}"), "ok", "");
            (200, "application/json", json!({"ok":true}).to_string())
        }
        _ => (400, "application/json", json!({"ok":false,"error":"未知操作"}).to_string()),
    }
}

pub fn roles() -> String {
    json!({ "roles": manager().store.roles_list() }).to_string()
}

pub fn role_action(query: &str) -> (u16, &'static str, String) {
    let action = qparam(query, "action");
    let role = qparam(query, "role");
    let m = manager();
    match action.as_str() {
        "create" => {
            if role.is_empty() { return (400,"application/json",json!({"ok":false,"error":"角色名必填"}).to_string()); }
            m.store.role_upsert(&role, &qparam(query, "desc"));
            m.store.audit(&crate::auth::current_user(), "", "role_create", &role, "ok", "");
            (200,"application/json",json!({"ok":true}).to_string())
        }
        "perms" => {
            let perms_csv = qparam(query, "perms");
            let perms: Vec<&str> = perms_csv.split(',').filter(|s| !s.is_empty()).collect();
            m.store.role_perms_set(&role, &perms);
            m.store.audit(&crate::auth::current_user(), "", "role_perms", &format!("{role} = {perms:?}"), "ok", "");
            (200,"application/json",json!({"ok":true}).to_string())
        }
        _ => (400,"application/json",json!({"ok":false,"error":"未知操作"}).to_string()),
    }
}

// ─── 运营元数据(instances.manage) ───
pub fn meta(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name");
    let k = qparam(query, "k");
    let v = qparam(query, "v");
    match manager().set_meta(&name, &k, &v) {
        Ok(()) => (200, "application/json", json!({"ok":true}).to_string()),
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}

// ─── 代理维度统一管理 ───
pub fn proxies() -> String {
    json!({ "proxies": manager().proxies() }).to_string()
}

pub async fn proxy_action(query: &str) -> (u16, &'static str, String) {
    let container = qparam(query, "container");
    let action = qparam(query, "action");
    match manager().proxy_action(&container, &action).await {
        Ok(msg) => (200, "application/json", json!({"ok":true,"message":msg}).to_string()),
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}

/// GET /api/rds/proxy/metrics?container= —— 代理管理端口 /metrics 代拉(instances.view;规避浏览器跨端口 CORS)
pub async fn proxy_metrics(query: &str) -> (u16, &'static str, String) {
    let container = qparam(query, "container");
    if container.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 container 参数"}).to_string());
    }
    match crate::instance::proxy_metrics_text(&container).await {
        Ok(text) => {
            let parsed = crate::instance::parse_proxy_metrics(&text);
            (200, "application/json", json!({"ok":true,"text":text,"parsed":parsed}).to_string())
        }
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}

/// GET /api/rds/monitor/dbs?instance=&node=&limit= —— DB 节点实时指标(instances.view;node=单节点下钻)
pub async fn monitor_dbs(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "instance");
    if name.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 instance 参数"}).to_string());
    }
    let node = {
        let v = qparam(query, "node");
        if v.is_empty() { None } else { Some(v) }
    };
    let limit = qparam(query, "limit").parse::<usize>().ok().filter(|n| *n > 0);
    match crate::instance::monitor_db_metrics_filtered(&name, node.as_deref(), limit).await {
        Ok(items) => (200, "application/json", json!({"ok":true,"items":items}).to_string()),
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}

/// GET /api/rds/monitor/proxies?instance= —— 全部 Proxy /metrics 并发代拉+聚合(instances.view)
pub async fn monitor_proxies(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "instance");
    if name.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 instance 参数"}).to_string());
    }
    match crate::instance::monitor_proxies_batch(&name).await {
        Ok(v) => (200, "application/json", v.to_string()),
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}

/// GET /api/rds/proxy/conf?container= —— 读取代理配置(instances.manage)
pub fn proxy_conf_get(query: &str) -> (u16, &'static str, String) {
    let container = qparam(query, "container");
    if container.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 container 参数"}).to_string());
    }
    match crate::instance::proxy_conf_get(&container) {
        Ok(content) => (200, "application/json", json!({"ok":true,"content":content}).to_string()),
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}

/// POST /api/rds/proxy/conf?container=&content= —— 更新代理配置并重启(instances.manage)
pub async fn proxy_conf_set(query: &str) -> (u16, &'static str, String) {
    let container = qparam(query, "container");
    let content = qparam(query, "content");
    if container.is_empty() || content.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 container/content 参数"}).to_string());
    }
    match crate::instance::proxy_conf_apply(&container, &content).await {
        Ok(msg) => (200, "application/json", json!({"ok":true,"message":msg}).to_string()),
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}

// ─── 任务单步骤重试 ───
pub async fn retry_task(query: &str) -> (u16, &'static str, String) {
    let task = qparam(query, "task");
    let node = qparam(query, "node");
    match manager().scheduler.rerun_node(&task, &node).await {
        Ok(msg) => (200, "application/json", json!({"ok":true,"message":msg}).to_string()),
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}

/// GET /api/rds/architectures —— 架构组合方案模板(模块组合参考)
pub fn architectures() -> String {
    json!({ "architectures": crate::instance::architectures() }).to_string()
}

/// GET /api/rds/nodegroups —— 内置编排模板目录(模块化任务展示/向导)
pub fn nodegroups() -> String {
    json!({ "nodegroups": crate::instance::nodegroups() }).to_string()
}

// ─── Phase B:用户草稿 + 删除归档(tasks.manage) ───
fn draft_nodes(query: &str) -> Result<Vec<TaskNode>, String> {
    let raw = qparam(query, "nodes");
    let arr: Vec<serde_json::Value> =
        serde_json::from_str(&raw).map_err(|e| format!("nodes 参数解析失败: {e}"))?;
    let mut out = Vec::new();
    for it in arr {
        let id = it.get("id").and_then(|x| x.as_str()).unwrap_or("").to_string();
        if id.is_empty() { continue; }
        let name = it.get("name").and_then(|x| x.as_str()).unwrap_or(&id).to_string();
        let deps: Vec<String> = it
            .get("deps").and_then(|d| d.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default();
        let note = it.get("note").and_then(|x| x.as_str()).unwrap_or("").to_string();
        let note = if note.is_empty() { name.clone() } else { note };
        out.push(TaskNode {
            id, name,
            deps,
            retries: it.get("retries").and_then(|x| x.as_u64()).unwrap_or(0) as u32,
            timeout_secs: it.get("timeout_secs").and_then(|x| x.as_u64()),
            steps: vec![Step::Noop { note }],
        });
    }
    if out.is_empty() { return Err("nodes 不能为空(每个节点需有 id)".to_string()); }
    Ok(out)
}
pub fn draft_task(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    let actor = crate::auth::current_user();
    let nodes = match draft_nodes(query) { Ok(n) => n, Err(e) => return (400, "application/json", json!({"ok":false,"error":e}).to_string()) };
    match manager().scheduler.submit_draft(&instance, &actor, nodes) {
        Ok(tid) => { manager().store.audit(&actor, &instance, "task_draft", &tid, "ok", ""); (200, "application/json", json!({"ok":true,"task_id":tid}).to_string()) }
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}
pub async fn start_task(query: &str) -> (u16, &'static str, String) {
    let task = qparam(query, "task");
    let actor = crate::auth::current_user();
    match manager().scheduler.start_draft(&task).await {
        Ok(msg) => { manager().store.audit(&actor, "", "task_start", &task, "ok", ""); (200, "application/json", json!({"ok":true,"message":msg}).to_string()) }
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}
pub fn edit_task(query: &str) -> (u16, &'static str, String) {
    let task = qparam(query, "task");
    let actor = crate::auth::current_user();
    let nodes = match draft_nodes(query) { Ok(n) => n, Err(e) => return (400, "application/json", json!({"ok":false,"error":e}).to_string()) };
    match manager().scheduler.edit_draft(&task, nodes) {
        Ok(()) => { manager().store.audit(&actor, "", "task_edit", &task, "ok", ""); (200, "application/json", json!({"ok":true}).to_string()) }
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}
pub fn delete_task(query: &str) -> (u16, &'static str, String) {
    let task = qparam(query, "task");
    let actor = crate::auth::current_user();
    match manager().scheduler.delete_task(&task) {
        Ok(()) => { manager().store.audit(&actor, "", "task_delete", &task, "ok", ""); (200, "application/json", json!({"ok":true}).to_string()) }
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}

// ─── 用户功能模块(页面新建“真实步骤模块”;见 docs/func-modules.md) ───

/// GET /api/rds/modules —— 模块库列表(tasks.view)
pub fn modules() -> String {
    json!({ "modules": manager().store.module_list() }).to_string()
}

/// 解析并校验 steps 参数:须为 Step JSON 数组
fn parse_module_steps(query: &str) -> Result<Vec<serde_json::Value>, String> {
    let raw = qparam(query, "steps");
    let arr: Vec<serde_json::Value> =
        serde_json::from_str(&raw).map_err(|e| format!("steps 参数解析失败: {e}"))?;
    if arr.is_empty() {
        return Err("模块至少需要一个步骤".to_string());
    }
    if arr.len() > 60 {
        return Err("模块步骤数过多(上限 60)".to_string());
    }
    for s in &arr {
        serde_json::from_value::<crate::dag::Step>(s.clone())
            .map_err(|e| format!("步骤不是合法 Step: {e}"))?;
    }
    Ok(arr)
}

/// POST /api/rds/module?name=&category=&desc=&steps= —— 新建/更新模块(tasks.manage,同名覆盖)
pub fn module_save(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name").trim().to_string();
    if name.is_empty() || name.len() > 96 {
        return (400, "application/json", json!({"ok":false,"error":"模块名必填且 ≤96 字符"}).to_string());
    }
    let category = {
        let c = qparam(query, "category").trim().to_string();
        if c.is_empty() { "other".to_string() } else { c }
    };
    let desc = qparam(query, "desc").trim().to_string();
    let steps = match parse_module_steps(query) {
        Ok(s) => s,
        Err(e) => return (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    };
    let steps_json = serde_json::to_string(&steps).unwrap_or_else(|_| "[]".to_string());
    let actor = crate::auth::current_user();
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    manager()
        .store
        .module_upsert(&name, &category, &desc, &steps_json, &actor, ts);
    manager()
        .store
        .audit(&actor, "", "module_save", &format!("{name} · {steps_json}"), "ok", "");
    (200, "application/json", json!({"ok":true}).to_string())
}

/// POST /api/rds/module/delete?name= —— 删除模块(tasks.manage)
pub fn module_delete(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name").trim().to_string();
    if name.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少模块名"}).to_string());
    }
    let actor = crate::auth::current_user();
    let hit = manager().store.module_delete(&name);
    manager()
        .store
        .audit(&actor, "", "module_delete", &name, if hit { "ok" } else { "miss" }, "");
    (200, "application/json", json!({"ok":true,"hit":hit}).to_string())
}

/// POST /api/rds/module/run?name=&instance= —— 对目标实例一键运行模块(tasks.manage)
pub fn module_run(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name").trim().to_string();
    let instance = qparam(query, "instance").trim().to_string();
    if name.is_empty() || instance.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"缺少 name/instance 参数"}).to_string());
    }
    match manager().run_module(&name, &instance) {
        Ok(tid) => (200, "application/json", json!({"ok":true,"task_id":tid}).to_string()),
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}

// ─── 实例启停 / 批量(S-批量) ───

pub fn set_enabled(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "name");
    let enabled = qparam(query, "value").as_str() != "0";
    match manager().set_instance_enabled(&name, enabled) {
        Ok(()) => (200, "application/json", json!({"ok":true}).to_string()),
        Err(e) => (400, "application/json", json!({"ok":false,"error":e}).to_string()),
    }
}

pub fn batch(query: &str) -> (u16, &'static str, String) {
    let action = qparam(query, "action");
    let names: Vec<String> = qparam(query, "names")
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if names.is_empty() {
        return (400, "application/json", json!({"ok":false,"error":"names 不能为空"}).to_string());
    }
    let results = manager().batch(&action, &names);
    (200, "application/json", json!({"ok":true,"results":results}).to_string())
}

// ─── 告警(S-告警) ───

pub fn alerts(query: &str) -> String {
    let opt = |k: &str| {
        let v = qparam(query, k);
        if v.is_empty() { None } else { Some(v) }
    };
    let limit = qparam(query, "limit").parse::<usize>().unwrap_or(300).clamp(1, 500);
    let offset = qparam(query, "offset").parse::<usize>().unwrap_or(0);
    // 分页:按过滤条件全量匹配后切片,返回 {alerts, total, offset}
    let full = manager().alerts(200_000, opt("severity").as_deref(), opt("status").as_deref(), opt("instance").as_deref());
    let total = full.len();
    let page: Vec<serde_json::Value> = full.into_iter().skip(offset).take(limit).collect();
    json!({ "alerts": page, "total": total, "offset": offset }).to_string()
}

pub fn alert_action(query: &str) -> (u16, &'static str, String) {
    let id: u64 = qparam(query, "id").parse().unwrap_or(0);
    let action = qparam(query, "action");
    let ok = manager().alert_action(id, action.as_str());
    if ok {
        (200, "application/json", json!({"ok":true}).to_string())
    } else {
        (400, "application/json", json!({"ok":false,"error":"告警不存在或已关闭"}).to_string())
    }
}

/// GET /api/rds/alerts/groups —— open 告警按 (kind, 归一化原因) 归群(alerts.view;
/// 不改 alerts 去重语义,实时计算;dba-ai-design §5.2 2a)
pub fn alert_groups(_query: &str) -> (u16, &'static str, String) {
    let m = manager();
    let open = m.alerts(5000, None, Some("open"), None);
    let groups = crate::insights::group_alerts(&open);
    (
        200,
        "application/json",
        json!({ "groups": groups, "generated_at": now_secs() }).to_string(),
    )
}

/// POST /api/rds/alerts/gov?pattern=<群 key>&action=ack|resolve —— 群处置(alerts.handle):
/// 对群内 open 告警逐条走既有 store.alert_action,一次审计(§5.2)
pub fn alert_group_action(query: &str) -> (u16, &'static str, String) {
    let key = qparam(query, "pattern");
    let action = qparam(query, "action");
    if key.is_empty() || !matches!(action.as_str(), "ack" | "resolve") {
        return (
            400,
            "application/json",
            json!({ "ok": false, "error": "需 pattern 且 action 为 ack/resolve" }).to_string(),
        );
    }
    let m = manager();
    let open = m.alerts(5000, None, Some("open"), None);
    let groups = crate::insights::group_alerts(&open);
    let Some(g) = groups.into_iter().find(|g| g["key"].as_str() == Some(key.as_str())) else {
        return (
            404,
            "application/json",
            json!({ "ok": false, "error": "未找到该告警群(可能已处置)" }).to_string(),
        );
    };
    let user = crate::auth::current_user();
    let mut handled = 0usize;
    if let Some(members) = g["members"].as_array() {
        for mem in members {
            if let Some(id) = mem["id"].as_u64() {
                if m.store.alert_action(id, &action, &user) {
                    handled += 1;
                }
            }
        }
    }
    m.store.audit(
        &user,
        "",
        "alert_group_action",
        &format!("{key} action={action} 告警数={handled}"),
        "ok",
        "",
    );
    (
        200,
        "application/json",
        json!({ "ok": true, "handled": handled }).to_string(),
    )
}

/// GET /api/rds/timeline?instance= —— 单实例事件时间线(alerts ∪ evidence ∪ audit;
/// instances.view;§5.2)
pub fn timeline(query: &str) -> (u16, &'static str, String) {
    let inst = qparam(query, "instance");
    if inst.is_empty() {
        return (
            400,
            "application/json",
            json!({ "ok": false, "error": "缺少 instance 参数" }).to_string(),
        );
    }
    let m = manager();
    let alerts = m.alerts(500, None, None, Some(&inst));
    let evidence = m.store.evidence_latest(&inst, 20);
    let audit = m.store.audit_list(500, Some(&inst), None, None);
    let events = crate::insights::timeline(&alerts, &evidence, &audit);
    (
        200,
        "application/json",
        json!({ "instance": inst, "events": events }).to_string(),
    )
}

/// GET /api/rds/capacity —— 容量预警/水位/回收候选(instances.view;dba-ai-design §6)
pub fn capacity(_query: &str) -> (u16, &'static str, String) {
    (
        200,
        "application/json",
        crate::capacity::overview().to_string(),
    )
}

/// GET /api/rds/ask?q=&instance= —— 规则版值班问答/FAQ(instances.view 只读组合;§7)
pub fn ask(query: &str) -> (u16, &'static str, String) {
    let q = qparam(query, "q");
    let inst = qparam(query, "instance");
    if q.is_empty() {
        return (
            400,
            "application/json",
            json!({ "ok": false, "error": "缺少 q 参数" }).to_string(),
        );
    }
    let m = manager();
    let inst_opt: Option<crate::instance::RdsInstance> = if inst.is_empty() {
        None
    } else {
        m.instances.get(&inst).map(|r| r.value().clone())
    };
    let evidence: Vec<serde_json::Value> = inst_opt
        .as_ref()
        .map(|i| m.store.evidence_latest(&i.name, 5))
        .unwrap_or_default();
    let answer = crate::ask::answer(&q, inst_opt.as_ref(), &evidence);
    (
        200,
        "application/json",
        json!({ "ok": true, "answer": answer }).to_string(),
    )
}

/// GET /api/rds/capacity/forecast?instance= —— 单实例各节点磁盘外推(instances.view;§6)
pub fn capacity_forecast(query: &str) -> (u16, &'static str, String) {
    let inst = qparam(query, "instance");
    if inst.is_empty() {
        return (
            400,
            "application/json",
            json!({ "ok": false, "error": "缺少 instance 参数" }).to_string(),
        );
    }
    let (st, v) = crate::capacity::forecast_view(&inst);
    (st, "application/json", v.to_string())
}

pub fn cancel_task(query: &str) -> (u16, &'static str, String) {
    let id = qparam(query, "task");
    if manager().cancel_task(&id) {
        (200, "application/json", json!({ "ok": true, "task_id": id }).to_string())
    } else {
        (404, "application/json", json!({ "ok": false, "error": format!("任务 {id} 不存在") }).to_string())
    }
}

// ─── AI-0 洞察 / 报告(规则版;见 docs/ai0-impl-checklist.md §4,AI 路标 v2) ───

fn all_instances() -> Vec<crate::instance::RdsInstance> {
    manager()
        .instances
        .iter()
        .map(|e| e.value().clone())
        .collect()
}

/// GET /api/rds/insights?region=&az=&q= —— 异常聚类(degraded/failed 同因实例群)
/// 返回 { clusters:[{pattern,label,count,members[…+latest_snapshot],suggested_playbooks}], generated_at }
pub fn insights(query: &str) -> String {
    let m = manager();
    let all = all_instances();
    let mut clusters = crate::insights::cluster_anomalies(&all);
    let region = qparam(query, "region").to_lowercase();
    let az = qparam(query, "az").to_lowercase();
    let q = qparam(query, "q").to_lowercase();
    for c in &mut clusters {
        let mut n = 0usize;
        if let Some(members) = c.get_mut("members").and_then(|v| v.as_array_mut()) {
            members.retain(|mem| {
                let f = |k: &str| mem[k].as_str().unwrap_or("").to_lowercase();
                (region.is_empty() || f("region").contains(&region))
                    && (az.is_empty() || f("az").contains(&az))
                    && (q.is_empty()
                        || f("name").contains(&q)
                        || f("tenant").contains(&q)
                        || f("last_error").contains(&q))
            });
            n = members.len();
            // 成员注入最近一次异常快照摘要(只带脱敏摘要,不整包透传 facts)
            for mem in members.iter_mut() {
                let name = mem["name"].as_str().unwrap_or("").to_string();
                if let Some(row) = m.store.evidence_latest(&name, 1).into_iter().next() {
                    mem["latest_snapshot"] = crate::insights::snapshot_summary(&row["facts"]);
                }
            }
        }
        c["count"] = json!(n);
    }
    clusters.retain(|c| c["count"].as_u64().unwrap_or(0) > 0);
    json!({ "clusters": clusters, "generated_at": now_secs() }).to_string()
}

/// GET /api/rds/reports?type=daily|weekly&since=&limit= —— 报告归档历史(audit.view;§8)
pub fn reports_history(query: &str) -> (u16, &'static str, String) {
    let rtype = qparam(query, "type");
    let since = qparam(query, "since").parse::<u64>().unwrap_or(0);
    let limit = qparam(query, "limit").parse::<usize>().unwrap_or(20).min(100);
    let list = manager().store.reports_list(
        if rtype.is_empty() { None } else { Some(&rtype) },
        since,
        limit,
    );
    (
        200,
        "application/json",
        json!({ "ok": true, "items": list }).to_string(),
    )
}

/// POST /api/rds/report/run?period=today|week —— 手动生成并归档报告(audit.view;§8;
/// 测试/补跑入口;period 兼容 today|week(前端口径)与 daily|weekly;不受
/// RDSCTL_REPORT_ENABLED 限制,内容并表仍按 include env)
pub fn report_run(query: &str) -> (u16, &'static str, String) {
    let period = qparam(query, "period");
    let rtype = match period.as_str() {
        "week" | "weekly" => "weekly",
        _ => "daily", // 空/today/daily 均产日报
    };
    let (st, v) = crate::report::generate(rtype);
    (st, "application/json", v.to_string())
}

/// GET /api/rds/report?period=today|week —— 规则版运维日报/周报
pub fn report(query: &str) -> (u16, &'static str, String) {
    let period = {
        let p = qparam(query, "period");
        if p.is_empty() { "today".to_string() } else { p }
    };
    let m = manager();
    let now = now_secs();
    match crate::insights::period_since(&period, now) {
        Some(since) => {
            let audit = m.store.audit_since(since, 10_000);
            let all = all_instances();
            match crate::insights::compose_report(&period, now, &audit, &all) {
                Some(v) => (200, "application/json", v.to_string()),
                None => (
                    400,
                    "application/json",
                    json!({ "ok": false, "error": "报告生成失败" }).to_string(),
                ),
            }
        }
        None => (
            400,
            "application/json",
            json!({ "ok": false, "error": "未知 period(支持 today/week)" }).to_string(),
        ),
    }
}

/// POST /api/rds/query?instance=&sql=&node= (body 亦接受参数)——DBA Web 查询台
/// 只读语句需 instances.query;写语句(DML)需 instances.query.write(引擎内二次校验)。
pub async fn query(query: &str, body: &str) -> (u16, &'static str, String) {
    let params = if body.is_empty() {
        query.to_string()
    } else {
        format!("{query}&{body}")
    };
    let instance = qparam(&params, "instance");
    let sql = qparam(&params, "sql");
    let node = qparam(&params, "node");
    let db = qparam(&params, "db"); // 默认库(可选;用于 -D,避免 No database selected)
    if instance.is_empty() || sql.is_empty() {
        return (
            400,
            "application/json",
            serde_json::json!({ "ok": false, "error": "缺少 instance/sql 参数" }).to_string(),
        );
    }
    if !crate::auth::has_perm("instances.query") {
        return (
            403,
            "application/json",
            serde_json::json!({ "ok": false, "error": "权限不足:需要 instances.query" }).to_string(),
        );
    }
    match crate::query::run_query(&instance, &sql, &node, &db).await {
        Ok(v) => (200, "application/json", v.to_string()),
        Err(f) => (
            f.status,
            "application/json",
            serde_json::json!({ "ok": false, "error": f.message }).to_string(),
        ),
    }
}

/// GET /api/rds/query/caps?instance= —— 查询台护栏/节点只读展示(instances.view)
pub fn query_caps(query: &str) -> (u16, &'static str, String) {
    let name = qparam(query, "instance");
    (
        200,
        "application/json",
        serde_json::json!({ "ok": true, "caps": crate::query::caps_view(&name) }).to_string(),
    )
}

// ─── 全局慢查治理(slow-query-design §7)───

/// POST /api/rds/slow/collect —— 立即手动采集一轮慢查样本(instances.manage;排障/补数据)
pub async fn slow_collect(_query: &str) -> (u16, &'static str, String) {
    crate::slow::collect_now().await;
    (200, "application/json", json!({"ok":true}).to_string())
}

/// GET /api/rds/slow?window=24h|7d&min_count=&top=&instance= —— 全局 digest Top
pub fn slow_top(query: &str) -> (u16, &'static str, String) {
    let window = qparam(query, "window");
    let min_count = qparam(query, "min_count").parse::<u64>().unwrap_or(0);
    let top = qparam(query, "top").parse::<usize>().unwrap_or(50);
    let instance = qparam(query, "instance");
    let (st, v) = crate::slow::top_view(
        &window,
        if instance.is_empty() { None } else { Some(&instance) },
        min_count,
        top,
    );
    (st, "application/json", v.to_string())
}

/// GET /api/rds/slow/instance?instance=&window=&limit= —— 单实例 digest 明细
pub fn slow_inst(query: &str) -> (u16, &'static str, String) {
    let inst = qparam(query, "instance");
    if inst.is_empty() {
        return (
            400,
            "application/json",
            json!({ "ok": false, "error": "缺少 instance 参数" }).to_string(),
        );
    }
    let window = qparam(query, "window");
    let limit = qparam(query, "limit").parse::<usize>().unwrap_or(100);
    let (st, v) = crate::slow::instance_view(&inst, &window, limit);
    (st, "application/json", v.to_string())
}

/// GET /api/rds/slow/trend?digest=&window= —— digest 全局趋势(instances.query)
pub fn slow_trend(query: &str) -> (u16, &'static str, String) {
    let digest = qparam(query, "digest");
    if digest.is_empty() {
        return (
            400,
            "application/json",
            json!({ "ok": false, "error": "缺少 digest 参数" }).to_string(),
        );
    }
    let window = qparam(query, "window");
    let (st, v) = crate::slow::trend_view(&digest, &window);
    (st, "application/json", v.to_string())
}

/// GET /api/rds/slow/advice?digest=&window=&top= —— digest 建议(规则版;instances.view,
/// digest_text 按 instances.query 投影;dba-ai-design §4.2/§10.1;LLM 解释版 gate 后另行接入)
pub fn slow_advice(query: &str) -> (u16, &'static str, String) {
    let digest = qparam(query, "digest");
    let window = qparam(query, "window");
    let top = qparam(query, "top").parse::<usize>().unwrap_or(50);
    let (st, v) = crate::slow::advice_view(&digest, &window, top);
    (st, "application/json", v.to_string())
}

/// GET /api/rds/slow/gov?status=open|ack|resolved —— 治理队列
pub fn slow_gov_list(query: &str) -> (u16, &'static str, String) {
    let status = qparam(query, "status");
    let list = manager().store.slow_gov_list(
        200,
        if status.is_empty() { None } else { Some(&status) },
    );
    let masked = !crate::auth::has_perm("instances.query");
    let list: Vec<serde_json::Value> = if masked {
        list.into_iter()
            .map(|mut x| {
                x["digest_text"] = json!("(需 instances.query 权限查看 SQL 摘要)");
                x
            })
            .collect()
    } else {
        list
    };
    (
        200,
        "application/json",
        json!({ "ok": true, "items": list }).to_string(),
    )
}

/// POST /api/rds/slow/gov?id=&action=ack|resolve —— 治理处置(tasks.manage)
pub fn slow_gov_action(query: &str) -> (u16, &'static str, String) {
    let id: u64 = qparam(query, "id").parse().unwrap_or(0);
    let action = qparam(query, "action");
    if id == 0 || !matches!(action.as_str(), "ack" | "resolve") {
        return (
            400,
            "application/json",
            json!({ "ok": false, "error": "需 id 且 action 为 ack/resolve" }).to_string(),
        );
    }
    let ok = manager().store.slow_gov_action(id, &action, &crate::auth::current_user());
    if ok {
        manager().store.audit(
            &crate::auth::current_user(),
            "",
            &format!("slow_gov_{action}"),
            &id.to_string(),
            "ok",
            "",
        );
        (200, "application/json", json!({ "ok": true }).to_string())
    } else {
        (
            400,
            "application/json",
            json!({ "ok": false, "error": "治理项不存在或已关闭" }).to_string(),
        )
    }
}

// ─── 备份节点注册联动(backup-link-design §9)───

/// GET /api/rds/backup/reg?instance= —— 节点注册状态视图(instances.view)
pub fn backup_reg(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    let items = crate::backuplink::reg_view(if instance.is_empty() { None } else { Some(&instance) });
    (
        200,
        "application/json",
        json!({
            "ok": true,
            "enabled": crate::backuplink::enabled(),
            "items": items,
        })
        .to_string(),
    )
}

/// POST /api/rds/backup/reg/retry?id= —— dead → pending 人工补投(instances.manage)
pub fn backup_reg_retry(query: &str) -> (u16, &'static str, String) {
    let id: u64 = qparam(query, "id").parse().unwrap_or(0);
    if id == 0 {
        return (
            400,
            "application/json",
            json!({ "ok": false, "error": "缺少 id 参数" }).to_string(),
        );
    }
    let ok = manager().store.backup_outbox_requeue(id);
    if ok {
        manager().store.audit(
            &crate::auth::current_user(),
            "",
            "backup_reg_retry",
            &id.to_string(),
            "ok",
            "",
        );
        (200, "application/json", json!({ "ok": true }).to_string())
    } else {
        (
            400,
            "application/json",
            json!({ "ok": false, "error": "仅 dead 状态的投递项可重试" }).to_string(),
        )
    }
}

/// GET /api/rds/schema?instance=&db=&table= —— 工作台 schema 元数据(instances.view)
pub async fn schema(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    if instance.is_empty() {
        return (
            400,
            "application/json",
            json!({ "ok": false, "error": "缺少 instance 参数" }).to_string(),
        );
    }
    let db = qparam(query, "db");
    let table = qparam(query, "table");
    match crate::query::schema(&instance, &db, &table).await {
        Ok(v) => (200, "application/json", v.to_string()),
        Err(f) => (
            f.status,
            "application/json",
            json!({ "ok": false, "error": f.message }).to_string(),
        ),
    }
}

/// POST /api/rds/query/save?instance=&name=&sql=（body 亦接受）—— 保存 SQL 到控制端 logs(instances.query)
pub fn query_save(query: &str, body: &str) -> (u16, &'static str, String) {
    let params = if body.is_empty() {
        query.to_string()
    } else {
        format!("{query}&{body}")
    };
    let instance = qparam(&params, "instance");
    let name = qparam(&params, "name");
    let sql = qparam(&params, "sql");
    let name_ok = |s: &str| {
        !s.is_empty()
            && s.len() <= 64
            && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    };
    if instance.is_empty() || !name_ok(&name) {
        return (
            400,
            "application/json",
            json!({ "ok": false, "error": "缺少 instance 或名称(name 仅限字母数字-_,≤64)" }).to_string(),
        );
    }
    if sql.trim().is_empty() {
        return (
            400,
            "application/json",
            json!({ "ok": false, "error": "SQL 为空" }).to_string(),
        );
    }
    let dir = format!("logs/rds/{instance}/queries");
    if let Err(e) = std::fs::create_dir_all(&dir) {
        return (
            500,
            "application/json",
            json!({ "ok": false, "error": format!("创建保存目录失败: {e}") }).to_string(),
        );
    }
    let path = format!("{dir}/{name}.sql");
    if let Err(e) = std::fs::write(&path, &sql) {
        return (
            500,
            "application/json",
            json!({ "ok": false, "error": format!("保存失败: {e}") }).to_string(),
        );
    }
    manager().store.audit(&crate::auth::current_user(), &instance, "query_save", &name, "ok", "");
    (200, "application/json", json!({ "ok": true, "path": path }).to_string())
}

/// GET /api/rds/schema/index?instance=&db=&table= —— 真实索引(SHOW INDEX;instances.view)
pub async fn schema_index(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    let db = qparam(query, "db");
    let table = qparam(query, "table");
    if instance.is_empty() {
        return (
            400,
            "application/json",
            json!({ "ok": false, "error": "缺少 instance 参数" }).to_string(),
        );
    }
    match crate::query::index(&instance, &db, &table).await {
        Ok(v) => (200, "application/json", v.to_string()),
        Err(f) => (f.status, "application/json", json!({ "ok": false, "error": f.message }).to_string()),
    }
}

/// GET /api/rds/schema/routines?instance=&db= —— 例程列表(instances.view)
pub async fn schema_routines(query: &str) -> (u16, &'static str, String) {
    let instance = qparam(query, "instance");
    let db = qparam(query, "db");
    if instance.is_empty() {
        return (400, "application/json", json!({ "ok": false, "error": "缺少 instance 参数" }).to_string());
    }
    match crate::query::routines(&instance, &db).await {
        Ok(v) => (200, "application/json", v.to_string()),
        Err(f) => (f.status, "application/json", json!({ "ok": false, "error": f.message }).to_string()),
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 提取 query 参数(urlencoded 简单处理)
fn qparam(query: &str, key: &str) -> String {
    query
        .split('&')
        .find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            if k == key {
                Some(url_decode(v))
            } else {
                None
            }
        })
        .unwrap_or_default()
}

fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let h = (bytes[i + 1] as char).to_digit(16);
                let l = (bytes[i + 2] as char).to_digit(16);
                if let (Some(h), Some(l)) = (h, l) {
                    out.push((h * 16 + l) as u8);
                    i += 3;
                    continue;
                }
                out.push(bytes[i]);
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
