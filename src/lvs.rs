// LVS 接入层(进程内实现,v0):宿主侧 4 层 TCP 转发器。
//
// 背景:实例「Proxy 集群前有 LVS 接入」在离线 Docker 环境无法依赖第三方镜像
// (haproxy 等需外网拉取;rdsctl 运行宿主即 docker daemon 宿主)。
// 因此 LVS/VIP 落地为 rdsctl 进程内的接入转发器:
//   - 每个实例一个 listener,绑定 127.0.0.1:{实例 lvs_mysql_port};
//   - 后端 = 该实例各 Proxy 容器在宿主上发布的 MySQL 端口(127.0.0.1:{mysql_port},
//     docker 已映射到容器 4051),轮询转发(round-robin),后端故障自动跳过重试;
//   - 语义与拓扑一致:客户端 → LVS 接入 VIP → Proxy 集群 → 分片主从。
// 生命周期由 DAG Step 驱动(创建 EnsureLvs / 销毁 StopLvs),sweeper 兜底恢复
// (进程重启后按持久化实例重建),实例记录字段(lvs/lvs_container/lvs_mysql_port)不变。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use dashmap::DashMap;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use crate::instance::RdsInstance;

/// 单个实例的接入转发器(后台任务句柄)
pub struct Gate {
    task: tokio::task::JoinHandle<()>,
    backend_idx: AtomicUsize,
}

static GATES: OnceLock<DashMap<String, Gate>> = OnceLock::new();

fn registry() -> &'static DashMap<String, Gate> {
    GATES.get_or_init(DashMap::new)
}

/// 当前已启动的接入层实例数(诊断/日志用)
#[allow(dead_code)] // 诊断统计入口(未接入日志/界面,预留)
pub fn active_count() -> usize {
    registry().len()
}

/// 实例后端(Proxy 宿主发布端口)
fn backends_of(inst: &RdsInstance) -> Vec<u16> {
    if inst.proxies.is_empty() {
        if inst.proxy_mysql_port > 0 {
            vec![inst.proxy_mysql_port]
        } else {
            Vec::new()
        }
    } else {
        inst.proxies
            .iter()
            .map(|p| p.mysql_port)
            .filter(|p| *p > 0)
            .collect()
    }
}

/// 确保实例接入转发器在运行(已存在则幂等)。
/// 镜像无关:只在宿主绑定 VIP 端口并把连接转发到 Proxy 宿主端口。
pub fn ensure(inst: &RdsInstance) -> Result<(), String> {
    if registry().contains_key(&inst.name) {
        return Ok(());
    }
    let port = inst.lvs_mysql_port;
    if port == 0 {
        return Err(format!("实例 {} 未分配 LVS 接入端口", inst.name));
    }
    let backends = backends_of(inst);
    if backends.is_empty() {
        return Err(format!(
            "实例 {} 无可用 Proxy 后端(请检查代理集群是否就绪)",
            inst.name
        ));
    }
    // 若端口已被本进程其它实例占用 → 视为已被登记
    if let Some(g) = registry().get(&inst.name) {
        let _ = g;
        return Ok(());
    }
    let rt = match tokio::runtime::Handle::try_current() {
        Ok(h) => h,
        Err(_) => return Err("无 tokio 运行时,无法启动接入转发器".to_string()),
    };
    // 同步完成绑定:失败即时上抛(否则创建任务会误判成功),返回后端口即监听
    let std_listener = std::net::TcpListener::bind(("127.0.0.1", port))
        .map_err(|e| format!("接入层绑定 127.0.0.1:{port} 失败: {e}"))?;
    std_listener
        .set_nonblocking(true)
        .map_err(|e| format!("接入层设置非阻塞失败: {e}"))?;
    let listener =
        TcpListener::from_std(std_listener).map_err(|e| format!("接入层接入运行时失败: {e}"))?;
    let backends_c = backends.clone();
    let name = inst.name.clone();
    let task = rt.spawn(async move {
        let bcs = backends_c.clone();
        tracing::info!(
            "LVS 接入层就绪: {name} → 127.0.0.1:{port} (后端 {} 个 Proxy)",
            bcs.len()
        );
        loop {
            let (mut client, _) = match listener.accept().await {
                Ok(c) => c,
                Err(_) => continue,
            };
            let bcs = bcs.clone();
            let idx = registry()
                .get(&name)
                .map(|g| g.backend_idx.fetch_add(1, Ordering::Relaxed))
                .unwrap_or(0);
            tokio::spawn(async move {
                let n = bcs.len();
                if n == 0 {
                    return;
                }
                let pick = idx % n;
                for attempt in 0..n {
                    let bp = bcs[(pick + attempt) % n];
                    let mut upstream = match TcpStream::connect(("127.0.0.1", bp)).await {
                        Ok(u) => u,
                        Err(_) => continue,
                    };
                    // 关键:必须“双向同时”转发(MySQL 协议:对端先发握手包)。
                    // 单向先收后发会因等待对端首包而永久死锁。
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                    let _ = upstream.shutdown().await;
                    break;
                }
            });
        }
    });
    registry().insert(
        inst.name.clone(),
        Gate {
            task,
            backend_idx: AtomicUsize::new(0),
        },
    );
    Ok(())
}

/// 停止实例接入转发器(销毁实例/清理时)
pub fn stop(name: &str) {
    if let Some((_, g)) = registry().remove(name) {
        g.task.abort();
        tracing::info!("LVS 接入层已停止: {name}");
    }
}

/// 停止全部接入转发器(测试/退出清理)
#[allow(dead_code)] // 进程退出清理入口(当前测试用 stop 单实例;预留)
pub fn shutdown_all() {
    let reg = registry();
    let keys: Vec<String> = reg.iter().map(|e| e.key().clone()).collect();
    for k in keys {
        stop(&k);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::{InstStatus, ProxyNode, RdsInstance};
    use tokio::io::AsyncReadExt;

    fn inst_with(backend_port: u16, vip_port: u16) -> RdsInstance {
        RdsInstance {
            name: "t-lvs".into(),
            status: InstStatus::Running,
            region: String::new(),
            az: String::new(),
            shard: "s0".into(),
            tenant: String::new(),
            enabled: true,
            lvs: vec![format!("127.0.0.1:{vip_port}")],
            lvs_container: "rds-t-lvs".into(),
            lvs_mysql_port: vip_port,
            proxies: vec![ProxyNode {
                container: "rds-t-proxy-1".into(),
                mysql_port: backend_port,
                mng_port: 0,
                spec: String::new(),
                ip: String::new(),
                qps: 0,
                conns: 0,
                cpu: 0.0,
                status: String::new(),
                version: String::new(),
            }],
            shards: Vec::new(),
            biz: String::new(),
            contact: String::new(),
            dba: String::new(),
            core: false,
            itype: "async".into(),
            mysql_version: String::new(),
            proxy_version: String::new(),
            spec: String::new(),
            shard_num: 1,
            data_size: String::new(),
            buffer_pool: String::new(),
            max_qps: 0,
            max_tps: 0,
            network: "rds-t".into(),
            nodes: Vec::new(),
            proxy_container: "rds-t-proxy-1".into(),
            proxy_mysql_port: backend_port,
            proxy_mng_port: 0,
            created_at: 0,
            root_password: String::new(),
            query_secret: String::new(),
            last_error: String::new(),
            node_states: std::collections::HashMap::new(),
            node_hosts: std::collections::HashMap::new(),
            auto_failover: true,
        }
    }

    /// 真实 TCP 回归:后端先发“握手包”,客户端再发数据——必须双向并发转发,
    /// 否则(单向往后等首包)会死锁。本测试验证经 VIP 能收到后端首包并回显。
    #[tokio::test]
    async fn vip_forwards_both_directions() {
        let be = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let bp = be.local_addr().unwrap().port();
        let v0 = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let vp = v0.local_addr().unwrap().port();
        drop(v0);
        let inst = inst_with(bp, vp);
        ensure(&inst).unwrap();

        let srv = tokio::spawn(async move {
            let (mut s, _) = be.accept().await.unwrap();
            // 模拟 MySQL 服务器先发握手
            s.write_all(b"greet-vip").await.unwrap();
            let mut buf = [0u8; 64];
            let n = s.read(&mut buf).await.unwrap();
            s.write_all(&buf[..n]).await.unwrap();
        });

        let mut c = TcpStream::connect(("127.0.0.1", vp)).await.unwrap();
        let mut buf = [0u8; 32];
        // 关键断言:能收到后端经 VIP 发来的首包(此前单向实现会在此处死锁)
        let n = c.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"greet-vip");
        c.write_all(b"ping").await.unwrap();
        let n = c.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"ping");
        srv.await.unwrap();
        stop("t-lvs");
    }
}
