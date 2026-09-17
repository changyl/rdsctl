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
    /// 该转发器占用的 VIP 端口(stop 时据此清归属登记)
    port: u16,
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
    // 同步完成绑定:失败即时上抛(否则创建任务会误判成功),返回后端口即监听。
    //
    // 但**同机多副本**(cluster 模式)下接入层是「宿主机单例」而不是每副本一份:
    // 每个副本的巡检都会 `ensure()` 同一实例的同一 VIP 端口,先绑上的那个赢,
    // 其余必然 EADDRINUSE。实测后果:`t-create-4` 的 `lvs` 节点 3 次重试全失败、
    // 任务判失败,而 VIP 一直可用(端口本来就在服务)—— 失败是假的。
    //
    // 但"端口被占"不能一律放行:它也可能是**别的实例的残留转发器**(销毁任务只在
    // 执行它的那个副本上调 `stop()`,其它副本的转发器会留着)。那时若放行,新实例的
    // 流量会被静默接到旧实例的死代理上 —— 比报错更糟。因此用 `logs/lvs/<port>.owner`
    // 记下占用者实例名,并确认对端**确实在服务**,再决定放行还是上抛。
    let std_listener = match std::net::TcpListener::bind(("127.0.0.1", port)) {
        Ok(l) => l,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            let owner = owner_read(port);
            let serving = vip_serving(port);
            return match owner.as_deref() {
                // 兄弟副本已在为**同一实例**服务 → 幂等成功
                Some(n) if n == inst.name => {
                    tracing::debug!(
                        "实例 {} 接入层已由本机其它副本接管 127.0.0.1:{port}(幂等跳过)",
                        inst.name
                    );
                    Ok(())
                }
                // 端口归属另一个实例 → 真冲突/残留,必须上抛并点名占用者
                Some(other) => Err(format!(
                    "接入层绑定 127.0.0.1:{port} 失败: {e}(该端口由接入层实例 {other} 占用;\
                     若 {other} 已销毁,请重启持有它的那个副本以释放)"
                )),
                // 无归属登记但在服务:多为兄弟副本写登记前的竞态(或旧版本转发器)
                None if serving => {
                    // 无登记但在服务:可能是兄弟副本写登记前的竞态,或两副本工作目录不同
                    // 导致看不到同一份登记文件。VIP 本身可用,故放行且用 debug 避免刷屏;
                    // 真正的"别的实例残留"会命中上面的 Some(other) 分支并报错点名。
                    tracing::debug!(
                        "实例 {} 接入层端口 127.0.0.1:{port} 已在本机服务(无归属登记),按已接管处理",
                        inst.name
                    );
                    Ok(())
                }
                // 被占且不可服务(容器端口映射抢占,或后端全死的残留转发器)→ 真冲突
                None => Err(format!(
                    "接入层绑定 127.0.0.1:{port} 失败: {e}(端口被占用但不可服务,疑似端口冲突或残留转发器)"
                )),
            };
        }
        Err(e) => return Err(format!("接入层绑定 127.0.0.1:{port} 失败: {e}")),
    };
    // 绑定成功即登记归属(必须在 spawn 之前:期间兄弟副本可能正在探测)
    owner_write(port, &inst.name);
    if let Err(e) = std_listener.set_nonblocking(true) {
        owner_clear(port, &inst.name);
        return Err(format!("接入层设置非阻塞失败: {e}"));
    }
    let listener = match TcpListener::from_std(std_listener) {
        Ok(l) => l,
        Err(e) => {
            owner_clear(port, &inst.name);
            return Err(format!("接入层接入运行时失败: {e}"));
        }
    };
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
            port,
        },
    );
    Ok(())
}

/// 该 VIP 端口是否**真的在服务**(能连上,且对端主动发来数据)。
///
/// 必须读一个字节,不能只看 `connect` 成功:残留转发器在**后端全死**时也会 accept,
/// 随后连不上上游便关闭连接(客户端读到 EOF)。只看 connect 会把这种「坏占用」
/// 误判成「兄弟副本已接管」,从而把新实例的流量接到死代理上。
///
/// 与既有 `bind` 一样是同步调用(接入层生命周期是低频路径);探测连接读完即关,无副作用。
fn vip_serving(port: u16) -> bool {
    use std::io::Read as _;
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
    let Ok(mut s) = std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(500))
    else {
        return false;
    };
    let _ = s.set_read_timeout(Some(std::time::Duration::from_millis(1000)));
    let mut b = [0u8; 1];
    matches!(s.read(&mut b), Ok(1))
}

// ─── 接入层归属登记(宿主机本地、跨进程) ───
//
// 端口号本身无法区分「兄弟副本在为同一实例服务」与「别的实例的残留转发器」,
// 因此落一个归属文件:`logs/lvs/<port>.owner` = 占用者实例名。
// 目录可用 RDSCTL_LVS_DIR 覆盖(测试/自定义部署)。

fn owner_dir() -> std::path::PathBuf {
    std::env::var("RDSCTL_LVS_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("logs/lvs"))
}

fn owner_path(port: u16) -> std::path::PathBuf {
    owner_dir().join(format!("{port}.owner"))
}

fn owner_read(port: u16) -> Option<String> {
    std::fs::read_to_string(owner_path(port))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// 登记归属(绑定成功后立即调用)。写失败只降级为 debug 日志:不影响转发本身。
fn owner_write(port: u16, name: &str) {
    let dir = owner_dir();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        tracing::debug!("接入层归属目录创建失败({}):{e}", dir.display());
        return;
    }
    if let Err(e) = std::fs::write(owner_path(port), name) {
        tracing::debug!("接入层归属登记写入失败(port={port}):{e}");
    }
}

/// 清除归属 —— 仅当登记的确实是 `name`,避免误删兄弟副本的登记。
fn owner_clear(port: u16, name: &str) {
    if owner_read(port).as_deref() == Some(name) {
        let _ = std::fs::remove_file(owner_path(port));
    }
}

/// 停止实例接入转发器(销毁实例/清理时)
pub fn stop(name: &str) {
    if let Some((_, g)) = registry().remove(name) {
        // 先清归属再 abort:避免留下指向已停实例的 .owner 文件
        owner_clear(g.port, name);
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

    /// 占住某端口;`speak=true` 时模拟**真在服务**的转发器(accept 后先发一个字节),
    /// 否则模拟「被占但不可服务」(如容器端口映射抢占,或后端全死的残留转发器)。
    fn hold_port(port: u16, speak: bool) -> std::thread::JoinHandle<()> {
        let l = std::net::TcpListener::bind(("127.0.0.1", port)).expect("占住端口");
        std::thread::spawn(move || {
            use std::io::Write as _;
            for st in l.incoming() {
                let Ok(mut st) = st else { break };
                if speak {
                    let _ = st.write_all(b"\x0a");
                    let _ = st.flush();
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
                drop(st);
            }
        })
    }

    fn cleanup_owner(port: u16) {
        let _ = std::fs::remove_file(owner_path(port));
    }

    /// 契约 A:端口归属**同一实例**(兄弟副本已接管)→ `ensure` 幂等成功。
    ///
    /// 回归背景:`t-create-4` 的 `lvs` 节点 3 次重试全失败(兄弟副本的巡检先绑上了
    /// 同一端口),任务被判失败,而 VIP 其实一直可用 —— 假失败。
    #[tokio::test]
    async fn ensure_is_idempotent_when_same_instance_already_owned() {
        let probe = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let vp = probe.local_addr().unwrap().port();
        drop(probe);
        let _h = hold_port(vp, true);
        // 模拟兄弟副本写的归属登记
        owner_write(vp, "t-lvs-peer");
        let mut inst = inst_with(1, vp);
        inst.name = "t-lvs-peer".into(); // 独立实例名,避免被进程内注册表早退短路
        assert!(ensure(&inst).is_ok(), "同实例已接管时必须幂等返回 Ok");
        assert!(!registry().contains_key("t-lvs-peer"), "本进程未接管,不应登记 gate");
        cleanup_owner(vp);
    }

    /// 契约 B:端口归属**另一个实例**(典型:已销毁实例的残留转发器)→ 必须报错,
    /// 且错误里要点名占用者,否则新实例的流量会被静默接到死代理上。
    #[tokio::test]
    async fn ensure_rejects_port_owned_by_another_instance() {
        let probe = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let vp = probe.local_addr().unwrap().port();
        drop(probe);
        let _h = hold_port(vp, true);
        owner_write(vp, "t-lvs-ghost");
        let mut inst = inst_with(1, vp);
        inst.name = "t-lvs-new".into();
        let e = ensure(&inst).expect_err("端口属于别的实例时必须报错");
        assert!(e.contains("t-lvs-ghost"), "错误应点名占用者:{e}");
        cleanup_owner(vp);
    }

    /// 契约 C:端口被占、无归属登记但**确实在服务**(多为写登记前的竞态)→ 放行。
    #[tokio::test]
    async fn ensure_allows_unregistered_but_serving_port() {
        let probe = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let vp = probe.local_addr().unwrap().port();
        drop(probe);
        let _h = hold_port(vp, true);
        cleanup_owner(vp); // 确保无登记
        let mut inst = inst_with(1, vp);
        inst.name = "t-lvs-race".into();
        assert!(ensure(&inst).is_ok(), "在服务但无登记应按已接管放行");
        cleanup_owner(vp);
    }

    /// 契约 D:端口被占但**不可服务**(容器端口映射抢占 / 后端全死的残留转发器)
    /// → 必须报错,不能把「坏占用」当成「兄弟副本已接管」。
    #[tokio::test]
    async fn ensure_rejects_occupied_but_dead_port() {
        let probe = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let vp = probe.local_addr().unwrap().port();
        drop(probe);
        let _h = hold_port(vp, false); // 只 accept,不发数据(模拟坏转发器)
        cleanup_owner(vp);
        let mut inst = inst_with(1, vp);
        inst.name = "t-lvs-dead".into();
        let e = ensure(&inst).expect_err("被占且不可服务时必须报错");
        assert!(e.contains("不可服务"), "错误应说明不可服务:{e}");
        cleanup_owner(vp);
    }

    /// `vip_serving` 的两条基本判据(供上面四条契约复用)
    #[tokio::test]
    async fn vip_serving_detects_real_service() {
        let probe = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let free = probe.local_addr().unwrap().port();
        drop(probe);
        assert!(!vip_serving(free), "无监听时必须为 false");
        let alive = hold_port(free, true);
        // 等待线程进入 accept
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        assert!(vip_serving(free), "在服务时必须为 true");
        drop(alive);
    }
}
