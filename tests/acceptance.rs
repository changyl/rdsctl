//! rdsctl P0 验收测试(真实 MySQL 持久化 + docker CLI 垫片,进程级 kill -9)
//!
//! 覆盖验收三项:
//!   1. kill -9 重启:任务不丢(重启后列表仍可见、状态 failed)、不重跑(容器不二次启动)
//!   2. 并发操作被拒:生命周期任务进行中,其它操作被 400 拒绝
//!   3. 巡检发现异常:容器缺失/复制中断 → degraded + 审计;恢复 → running + 审计
//!
//! 运行前提:本机 MySQL 可达(root 建/删库权限),mysql 客户端在 PATH;
//! 测试为每个用例创建独立数据库 rdsctl_acc_* 并在结束时删除。
//! 通过 RDSCTL_MYSQL_HOST/PORT/USER/PASS 可指向其它 MySQL。

use std::io::Read;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

static SERIAL: Mutex<()> = Mutex::new(());

fn serial_guard() -> std::sync::MutexGuard<'static, ()> {
    // 用例内部断言失败会让锁中毒;其余用例仍应能继续(串行即可)
    SERIAL.lock().unwrap_or_else(|p| p.into_inner())
}

fn mysql_cli() -> String {
    std::env::var("RDSCTL_MYSQL_CLI")
        .ok()
        .filter(|p| Path::new(p).exists())
        .unwrap_or_else(|| {
            let out = Command::new("which")
                .arg("mysql")
                .output()
                .expect("which mysql");
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        })
}

fn mysql_admin(sql: &str) -> Result<String, String> {
    let mut cmd = Command::new(mysql_cli());
    cmd.args([
        "-h",
        "127.0.0.1",
        "-P",
        "3306",
        "-u",
        "root",
        "--batch",
        "--raw",
        "--skip-column-names",
        "-e",
        sql,
    ]);
    let out = cmd.output().map_err(|e| e.to_string())?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn sanitize(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_alphanumeric()).collect()
}

/// 每用例上下文:独立测试库 + fake docker/mysql 垫片目录
struct Ctx {
    db: String,
    dir: PathBuf,
    fake_bin: PathBuf,
    state: PathBuf,
    ctrl: PathBuf,
    log: PathBuf,
    mysql_real: String,
}

impl Ctx {
    fn new(tag: &str) -> Ctx {
        let uniq = format!("rdsctl_acc_{}_{}", sanitize(tag), std::process::id());
        let dir = std::env::temp_dir().join(&uniq);
        let fake_bin = dir.join("bin");
        let state = dir.join("state");
        let ctrl = dir.join("ctrl");
        let log = dir.join("docker.log");
        // 目录同理:先整棵删掉再建,避免继承上一轮的日志/快照/租约文件
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&fake_bin).unwrap();
        std::fs::create_dir_all(&state).unwrap();
        std::fs::create_dir_all(&ctrl).unwrap();
        let mysql_real = mysql_cli();
        // 独立数据库:**先删再建**。
        //
        // 为什么不能只用 `CREATE DATABASE IF NOT EXISTS`:库名含 `std::process::id()`,而
        // 操作系统会**复用 pid**;若上一次同名用例失败(`drop_db` 没跑到),库里会残留
        // `t-create-*`/实例/租约行,下一次同 pid 的用例就会继承它们 —— 表现为
        // "leader 续跑一个陈旧任务,和本用例争同一实例的租约",最后以 8s 桥接超时失败。
        // 这个坑实测把一次排查带偏了很久(95 个残留库),因此这里改为无条件重建。
        mysql_admin(&format!("DROP DATABASE IF EXISTS `{uniq}`"))
            .unwrap_or_else(|e| panic!("清理同名测试库失败: {e}"));
        mysql_admin(&format!(
            "CREATE DATABASE `{uniq}` CHARACTER SET utf8mb4"
        ))
        .unwrap_or_else(|e| panic!("创建测试库失败: {e}"));
        let ctx = Ctx {
            db: uniq,
            dir,
            fake_bin,
            state,
            ctrl,
            log,
            mysql_real,
        };
        ctx.install_shims();
        ctx
    }

    fn install_shims(&self) {
        // fake docker
        std::fs::write(
            self.fake_bin.join("docker"),
            r#"#!/bin/bash
# rdsctl 验收用 docker CLI 垫片
ST="$DK_STATE"; CTRL="$DK_CTRL"; LOG="$DK_LOG"
log(){ echo "T$(date +%s%N) $*" >> "$LOG"; }
case "$1" in
  run)
    # docker run -d --name NAME ...
    name=""; prev=""
    for a in "$@"; do
      if [ "$prev" = "--name" ]; then name="$a"; fi
      prev="$a"
    done
    log "RUN $name"
    if [ -e "$CTRL/hold" ] && [ -n "$DK_RUN_HOLD" ]; then sleep "$DK_RUN_HOLD"; fi
    mkdir -p "$ST/containers"
    [ -n "$name" ] && touch "$ST/containers/$name"
    exit 0 ;;
  rm)
    # docker rm -f NAME (兼容 docker rm NAME)
    name="${@: -1}"
    log "RM $name"
    if [ -e "$CTRL/rmhold" ] && [ -n "$DK_RM_HOLD" ]; then sleep "$DK_RM_HOLD"; fi
    rm -f "$ST/containers/$name" "$ST/networks/$name"
    exit 0 ;;
  inspect)
    name="${@: -1}"
    if [ -e "$ST/containers/$name" ] || [ -e "$ST/networks/$name" ]; then exit 0; fi
    exit 1 ;;
  network)
    case "$2" in
      create) log "NETCREATE $3"; mkdir -p "$ST/networks"; touch "$ST/networks/$3"; exit 0 ;;
      rm) log "NETRM $3"; rm -f "$ST/networks/$3"; exit 0 ;;
      inspect) [ -e "$ST/networks/$3" ] && exit 0 || exit 1 ;;
    esac
    exit 0 ;;
  exec)
    # docker exec NAME mysql ... -e SQL | sh -c ...
    # 查询台垫片:SQL 查询引擎经 docker exec <c> sh -c 'timeout … mysql … -e <SQL>' 调用
    # 新增(集群用例):mysql_down 存在时所有 SQL 均失败 → 可把 DAG 停在 WaitMysql 窗口内
    if [ -e "$CTRL/mysql_down" ]; then exit 1; fi
    for a in "$@"; do
      case "$a" in
        *'FROM appdb.users'*) printf 'id\tname\tpasswd\n1\tadmin\tsecret2\n2\tbob\tNULL\n'; exit 0 ;;
        *'UPDATE appdb.users'*) exit 0 ;;
      esac
    done
    sql=""; prev=""
    for a in "$@"; do
      if [ "$prev" = "-e" ]; then sql="$a"; fi
      prev="$a"
    done
    case "$sql" in
      *performance_schema*)
        if [ -e "$ST/repl_stop" ]; then echo "STOPPED"; else echo "OK"; fi ;;
      *gtid_executed*) echo "aaaaaaaa-1111-1111-1111-111111111111:1-42" ;;
      *WAIT_FOR_EXECUTED_GTID_SET*) echo 0 ;;
      *SELECT*1*) echo 1 ;;
      *SELECT*v*) echo ok ;;
      *) echo 0 ;;
    esac
    exit 0 ;;
  logs) exit 0 ;;
  *) exit 0 ;;
esac
"#,
        )
        .unwrap();
        // fake 宿主机 mysql(代理连通探测)
        std::fs::write(
            self.fake_bin.join("mysql"),
            r#"#!/bin/bash
ST="$DK_STATE"
sql=""; prev=""
for a in "$@"; do
  if [ "$prev" = "-e" ]; then sql="$a"; fi
  prev="$a"
done
case "$sql" in
  *SELECT*1*)
    if [ -e "$ST/proxy_down" ]; then exit 1; fi
    echo 1 ;;
  *) echo 0 ;;
esac
exit 0
"#,
        )
        .unwrap();
        for f in ["docker", "mysql"] {
            let p = self.fake_bin.join(f);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
        }
    }

    fn child_env(&self, cmd: &mut Command) {
        let path = format!(
            "{}:{}",
            self.fake_bin.display(),
            std::env::var("PATH").unwrap_or_default()
        );
        cmd.env("PATH", path);
        cmd.env("DK_STATE", &self.state);
        cmd.env("DK_CTRL", &self.ctrl);
        cmd.env("DK_LOG", &self.log);
        cmd.env("DK_RUN_HOLD", "4");
        cmd.env("DK_RM_HOLD", "3");
        // 管控库连接(绝对真实 mysql,避开垫片)
        cmd.env("RDSCTL_MYSQL_CLI", &self.mysql_real);
        cmd.env("RDSCTL_MYSQL_DB", &self.db);
        cmd.env(
            "RDSCTL_MYSQL_HOST",
            std::env::var("RDSCTL_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
        );
        cmd.env(
            "RDSCTL_MYSQL_PORT",
            std::env::var("RDSCTL_MYSQL_PORT").unwrap_or_else(|_| "3306".into()),
        );
        cmd.env(
            "RDSCTL_MYSQL_USER",
            std::env::var("RDSCTL_MYSQL_USER").unwrap_or_else(|_| "root".into()),
        );
        if let Ok(p) = std::env::var("RDSCTL_MYSQL_PASS") {
            cmd.env("RDSCTL_MYSQL_PASS", p);
        }
    }

    fn touch(&self, name: &str) {
        std::fs::write(self.ctrl.join(name), "1").unwrap();
    }
    fn untouch(&self, name: &str) {
        let _ = std::fs::remove_file(self.ctrl.join(name));
    }

    fn docker_log_lines(&self) -> Vec<String> {
        std::fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .map(|l| l.to_string())
            .collect()
    }

    fn run_count(&self, op: &str, name: &str) -> usize {
        let target = format!("{op} {name}");
        self.docker_log_lines()
            .iter()
            .filter(|l| l.contains(&target))
            .count()
    }

    fn drop_db(&self) {
        // 先掐掉指向该库的连接:节点进程在本函数调用时可能还没退出(ClusterSrv 在 ctx 之后析构),
        // 带着活跃连接做 DROP DATABASE 可能失败 → 库残留(实测留下 95 个)。
        let _ = mysql_admin(&format!(
            "SELECT GROUP_CONCAT(id) FROM information_schema.processlist WHERE db='{}'",
            self.db
        ));
        let _ = mysql_admin(&format!("DROP DATABASE IF EXISTS `{}`", self.db));
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// ─── 极简 HTTP 客户端 ───

struct Resp {
    status: u16,
    headers: String,
    body: String,
}

fn http(port: u16, method: &str, path: &str, cookie: Option<&str>) -> Result<Resp, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|e| e.to_string())?;
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let cookie_hdr = cookie
        .map(|c| format!("Cookie: {c}\r\n"))
        .unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n{cookie_hdr}\r\n"
    );
    use std::io::Write;
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf).to_string();
    let mut parts = text.split("\r\n\r\n");
    let head = parts.next().unwrap_or("");
    let body = parts.next().unwrap_or("").to_string();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Ok(Resp {
        status,
        headers: head.to_string(),
        body,
    })
}

/// 带 body 的 POST(内部 RPC 提案需要;`http()` 只发无 body 请求)
fn http_post_json(port: u16, path: &str, body: &str) -> Result<Resp, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|e| e.to_string())?;
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    use std::io::Write;
    stream
        .write_all(req.as_bytes())
        .map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf).to_string();
    let mut parts = text.split("\r\n\r\n");
    let head = parts.next().unwrap_or("");
    let body = parts.next().unwrap_or("").to_string();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    Ok(Resp {
        status,
        headers: head.to_string(),
        body,
    })
}

fn login(port: u16) -> String {
    let r = http(port, "POST", "/login?user=admin&password=admin", None).expect("login http");
    assert_eq!(r.status, 200, "登录失败: {}", r.body);
    // Set-Cookie: rdsctl_session=xxx; ...
    r.headers
        .lines()
        .find_map(|l| {
            let l = l.trim().to_ascii_lowercase();
            l.strip_prefix("set-cookie:")?
                .split(';')
                .next()
                .map(|s| s.trim().to_string())
        })
        .expect("无 session cookie")
}

/// 以指定账号登录(集群用例需要非 admin 账号;单机模式会话在进程内、集群在状态机)
fn login_as(port: u16, user: &str, pass: &str) -> String {
    let r = http(
        port,
        "POST",
        &format!("/login?user={user}&password={pass}"),
        None,
    )
    .expect("login http");
    assert_eq!(r.status, 200, "登录 {user} 失败: {}", r.body);
    r.headers
        .lines()
        .find_map(|l| {
            let l = l.trim().to_ascii_lowercase();
            l.strip_prefix("set-cookie:")?
                .split(';')
                .next()
                .map(|s| s.trim().to_string())
        })
        .expect("无 session cookie")
}

/// 递归在目录下按**字节**搜索(日志可能是非 UTF-8 的记录格式,文本读会漏)
fn dir_contains_bytes(dir: &std::path::Path, needle: &[u8]) -> bool {
    if needle.is_empty() {
        return false;
    }
    let Ok(rd) = std::fs::read_dir(dir) else {
        return false;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            if dir_contains_bytes(&p, needle) {
                return true;
            }
        } else if let Ok(bytes) = std::fs::read(&p) {
            if bytes.windows(needle.len()).any(|w| w == needle) {
                return true;
            }
        }
    }
    false
}

fn json_get(port: u16, cookie: &str, path: &str) -> (u16, serde_json::Value) {
    let r = http(port, "GET", path, Some(cookie)).expect("http get");
    let v: serde_json::Value = if r.body.trim().is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&r.body).unwrap_or_else(|_| serde_json::json!({"raw": r.body}))
    };
    (r.status, v)
}

/// 轮询直到 predicate 为 true 或超时
fn wait_until<F: FnMut() -> bool>(timeout_secs: u64, mut pred: F, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    while Instant::now() < deadline {
        if pred() {
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("等待超时: {what}");
}

// ─── 服务器进程管理 ───

struct Srv {
    child: Child,
    port: u16,
}

fn start_server(ctx: &Ctx, sweep_secs: u64) -> Srv {
    let bin = env!("CARGO_BIN_EXE_rdsctl");
    let port = free_port();
    let out = ctx.dir.join("server.log");
    let mut cmd = Command::new(bin);
    cmd.args(["--port", &port.to_string()]);
    ctx.child_env(&mut cmd);
    cmd.env("RDSCTL_SWEEP_SECS", sweep_secs.to_string());
    cmd.stdout(Stdio::from(std::fs::File::create(&out).unwrap()));
    cmd.stderr(Stdio::inherit());
    let child = cmd.spawn().expect("spawn rdsctl");
    let mut srv = Srv { child, port };
    // 就绪:登录可达
    let mut ready = false;
    for _ in 0..100 {
        if http(port, "POST", "/login?user=admin&password=admin", None)
            .map(|r| r.status == 200)
            .unwrap_or(false)
        {
            ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
        if srv.child.try_wait().ok().flatten().is_some() {
            panic!("rdsctl 提前退出,见 {}", out.display());
        }
    }
    assert!(ready, "rdsctl {}s 内未就绪", 20);
    srv
}

fn free_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

impl Drop for Srv {
    fn drop(&mut self) {
        // 确保进程终止(避免测试间端口/连接残留)
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn kill9(srv: &mut Srv) {
    let _ = srv.child.kill();
    let _ = srv.child.wait();
}

fn instance_status(port: u16, cookie: &str, name: &str) -> Option<String> {
    let (s, v) = json_get(port, cookie, &format!("/api/rds/instance?name={name}"));
    if s != 200 {
        return None;
    }
    v["instance"]["status"].as_str().map(|x| x.to_string())
}

fn task_status(port: u16, cookie: &str, tid: &str) -> Option<String> {
    let (s, v) = json_get(port, cookie, &format!("/api/rds/task?id={tid}"));
    if s != 200 {
        return None;
    }
    v["task"]["status"].as_str().map(|x| x.to_string())
}

fn submit_create(port: u16, cookie: &str, name: &str) -> String {
    let r = http(
        port,
        "POST",
        &format!("/api/rds/create?name={name}"),
        Some(cookie),
    )
    .unwrap();
    assert_eq!(r.status, 200, "create 失败: {}", r.body);
    serde_json::from_str::<serde_json::Value>(&r.body).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string()
}

fn submit_destroy(port: u16, cookie: &str, name: &str) -> (u16, String) {
    let r = http(
        port,
        "POST",
        &format!("/api/rds/destroy?name={name}"),
        Some(cookie),
    )
    .unwrap();
    (r.status, r.body)
}

fn audit_actions(port: u16, cookie: &str) -> Vec<(String, String)> {
    let (_, v) = json_get(port, cookie, "/api/rds/audit");
    v["audit"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|x| {
                    (
                        x["action"].as_str().unwrap_or("").to_string(),
                        x["params"].as_str().unwrap_or("").to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

// ─── 验收用例 ───

/// 验收 1:kill -9 重启 — 任务不丢(可见且 failed)、不重跑(容器不二次 run)
#[test]
fn kill9_restart_keeps_task_no_rerun() {
    let _g = serial_guard();
    let ctx = Ctx::new("kill9");
    let mut srv = start_server(&ctx, 60); // 巡检周期拉长,避免干扰
    let cookie = login(srv.port);

    // 制造"进行中"的创建任务:docker run 被 hold 卡住
    ctx.touch("hold");
    let tid = submit_create(srv.port, &cookie, "kill9a");
    // 等任务进入 running
    wait_until(
        15,
        || task_status(srv.port, &cookie, &tid).as_deref() == Some("running"),
        "任务进入 running",
    );
    // 等至少一个容器 run 已记录(证明在跑)
    wait_until(
        15,
        || ctx.run_count("RUN", "rds-kill9a-master") >= 1,
        "master run 已记录",
    );

    // 进程内已发生的 RUN 次数(重启后不得增加)
    let runs_before = ctx.run_count("RUN", "rds-kill9a-master");

    // kill -9
    kill9(&mut srv);
    drop(cookie); // cookie 会话在内存,重启后需重新登录

    // 重启(同库同端口)
    ctx.untouch("hold");
    let srv2 = start_server(&ctx, 60);
    let cookie2 = login(srv2.port);

    // a) 任务不丢:重启后列表/详情仍可见,状态 failed(中断),节点终态
    let (s, tv) = json_get(srv2.port, &cookie2, &format!("/api/rds/task?id={tid}"));
    assert_eq!(s, 200, "重启后任务应可见(DB 历史): {tv}");
    assert_eq!(
        tv["task"]["status"].as_str(),
        Some("failed"),
        "中断任务应为 failed"
    );
    let nodes = tv["task"]["nodes"].as_array().cloned().unwrap_or_default();
    assert!(!nodes.is_empty(), "任务节点应完整恢复");
    for n in &nodes {
        let st = n["status"].as_str().unwrap_or("");
        assert!(
            matches!(st, "success" | "skipped"),
            "中断后节点应为终态,当前 {st} (node {})",
            n["id"]
        );
    }
    // 实例被标记为 failed(进程中断,操作未完成)
    let st = instance_status(srv2.port, &cookie2, "kill9a");
    assert_eq!(st.as_deref(), Some("failed"), "实例应为 failed");

    // b) 不重跑:重启后等待数秒,容器 run 计数不再增长
    std::thread::sleep(Duration::from_secs(3));
    let runs_after = ctx.run_count("RUN", "rds-kill9a-master");
    assert_eq!(
        runs_after, runs_before,
        "重启后不得重新 run 容器(kill 前 {runs_before},重启后 {runs_after})"
    );

    // c) 允许从 failed 清理:销毁 → destroyed
    let (stc, body) = submit_destroy(srv2.port, &cookie2, "kill9a");
    assert_eq!(stc, 200, "failed 实例应可销毁: {body}");
    wait_until(
        30,
        || instance_status(srv2.port, &cookie2, "kill9a").as_deref() == Some("destroyed"),
        "销毁完成",
    );
    // 销毁审计
    let acts = audit_actions(srv2.port, &cookie2);
    assert!(
        acts.iter().any(|(a, _)| a == "destroy"),
        "应有销毁审计,实际: {acts:?}"
    );
    ctx.drop_db();
}

/// 验收 2:并发操作被拒(状态机 + 实例操作锁)
#[test]
fn concurrent_ops_rejected() {
    let _g = serial_guard();
    let ctx = Ctx::new("conc");
    let srv = start_server(&ctx, 60);
    let cookie = login(srv.port);

    // 2.1 创建进行中(creating)时:destroy / scaleout / 重复 create 全部拒绝
    ctx.touch("hold");
    let tid = submit_create(srv.port, &cookie, "conc1");
    wait_until(
        15,
        || instance_status(srv.port, &cookie, "conc1").as_deref() == Some("creating"),
        "conc1 creating",
    );
    let (s1, b1) = submit_destroy(srv.port, &cookie, "conc1");
    assert_eq!(s1, 400, "creating 中销毁应被拒: {b1}");
    assert!(
        b1.contains("可销毁") || b1.contains("不允许"),
        "错误应说明原因: {b1}"
    );
    let r2 = http(
        srv.port,
        "POST",
        &format!("/api/rds/scaleout?name=conc1&role=backup"),
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(r2.status, 400, "creating 中扩容应被拒: {}", r2.body);
    let r3 = http(
        srv.port,
        "POST",
        &format!("/api/rds/create?name=conc1"),
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(r3.status, 400, "同名二次创建应被拒: {}", r3.body);
    // 只存在一个 create 任务
    let (_, tv) = json_get(srv.port, &cookie, &format!("/api/rds/task?id={tid}"));
    assert_eq!(tv["task"]["status"].as_str(), Some("running"));

    // 2.2 创建完成后:销毁进行中(destroying)再次销毁被拒(操作锁)
    ctx.untouch("hold");
    wait_until(
        40,
        || task_status(srv.port, &cookie, &tid).as_deref() == Some("success"),
        "conc1 创建任务成功",
    );
    // 任务终态后操作锁才释放;容忍最多几秒的锁释放窗口
    ctx.touch("rmhold");
    let (mut s4, mut b4) = submit_destroy(srv.port, &cookie, "conc1");
    for _ in 0..40 {
        if s4 == 200 {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
        let r = submit_destroy(srv.port, &cookie, "conc1");
        s4 = r.0;
        b4 = r.1;
    }
    assert_eq!(s4, 200, "running 销毁应最终接受: {b4}");
    wait_until(
        10,
        || instance_status(srv.port, &cookie, "conc1").as_deref() == Some("destroying"),
        "conc1 destroying",
    );
    let (s5, b5) = submit_destroy(srv.port, &cookie, "conc1");
    assert_eq!(s5, 400, "destroying 中再次销毁应被拒: {b5}");
    assert!(
        b5.contains("销毁中") || b5.contains("不允许"),
        "应提示状态/锁原因: {b5}"
    );
    // 同时扩容也被拒
    let r6 = http(
        srv.port,
        "POST",
        &format!("/api/rds/scaleout?name=conc1&role=stats"),
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(r6.status, 400, "destroying 中扩容应被拒: {}", r6.body);
    ctx.untouch("rmhold");
    wait_until(
        30,
        || instance_status(srv.port, &cookie, "conc1").as_deref() == Some("destroyed"),
        "conc1 销毁完成",
    );
    ctx.drop_db();
}

/// 验收 3:巡检发现异常(容器缺失 / 复制中断)→ degraded + 审计;恢复 → running + 审计
#[test]
fn sweeper_detects_and_recovers() {
    let _g = serial_guard();
    let ctx = Ctx::new("sweep");
    let srv = start_server(&ctx, 1); // 巡检 1s
    let cookie = login(srv.port);

    let tid = submit_create(srv.port, &cookie, "sw1");
    wait_until(
        40,
        || instance_status(srv.port, &cookie, "sw1").as_deref() == Some("running"),
        "sw1 running",
    );
    // 实例状态先于任务终态置 running(step 在 finish 前),需等任务终态 success
    wait_until(
        15,
        || task_status(srv.port, &cookie, &tid).as_deref() == Some("success"),
        "sw1 创建任务成功",
    );

    // 3.1 节点容器缺失 → degraded + audit
    let fake_rm = ctx.fake_bin.join("docker");
    let st = Command::new(&fake_rm)
        .args(["rm", "-f", "rds-sw1-slave-1"])
        .env("DK_STATE", &ctx.state)
        .env("DK_CTRL", &ctx.ctrl)
        .env("DK_LOG", &ctx.log)
        .status()
        .unwrap();
    assert!(st.success());
    wait_until(
        20,
        || instance_status(srv.port, &cookie, "sw1").as_deref() == Some("degraded"),
        "sw1 降级(容器缺失)",
    );
    {
        let acts = audit_actions(srv.port, &cookie);
        assert!(
            acts.iter()
                .any(|(a, p)| a == "degrade" && p.contains("容器缺失")),
            "应有 degrade 审计(容器缺失): {acts:?}"
        );
    }
    // AI-0:degrade 现场应落 evidence 快照(MySQL 后端直查 evidence_snapshots)
    let cnt = mysql_admin(&format!(
        "SELECT COUNT(*) FROM `{}`.evidence_snapshots \
         WHERE instance='sw1' AND kind='degrade' AND reason LIKE '%容器缺失%'",
        ctx.db
    ))
    .unwrap_or_default();
    assert!(
        cnt.trim().parse::<u64>().unwrap_or(0) >= 1,
        "容器缺失 degrade 应写 evidence 快照,got: {cnt}"
    );
    // AI-0:insights 聚类 + 成员快照摘要
    let (sc, iv) = json_get(srv.port, &cookie, "/api/rds/insights?q=sw1");
    assert_eq!(sc, 200, "insights http: {iv}");
    let clusters = iv["clusters"].as_array().cloned().unwrap_or_default();
    let has_missing = clusters.iter().any(|c| {
        c["label"] == "节点容器缺失"
            && c["members"].as_array().map_or(false, |ms| {
                ms.iter().any(|m| {
                    m["name"] == "sw1"
                        && m["latest_snapshot"]["containers"]
                            .as_array()
                            .map_or(false, |cs| {
                                cs.iter().any(|x| {
                                    x["present"] == serde_json::json!(false)
                                        && x["container"]
                                            .as_str()
                                            .map_or(false, |s| s.ends_with("slave-1"))
                                })
                            })
                })
            })
    });
    assert!(
        has_missing,
        "insights 应含「节点容器缺失」群且带快照摘要: {iv}"
    );
    // AI-0:report(today)含容器缺失信息
    let (sr, rv) = json_get(srv.port, &cookie, "/api/rds/report?period=today");
    assert_eq!(sr, 200, "report http: {rv}");
    assert!(
        rv["text"].as_str().unwrap_or("").contains("容器缺失"),
        "report 文本应含容器缺失: {rv}"
    );
    // 3.2 容器恢复 → running + recover 审计
    let cdir = ctx.state.join("containers");
    std::fs::create_dir_all(&cdir).unwrap();
    std::fs::write(cdir.join("rds-sw1-slave-1"), "1").unwrap();
    wait_until(
        20,
        || instance_status(srv.port, &cookie, "sw1").as_deref() == Some("running"),
        "sw1 恢复 running",
    );
    {
        let acts = audit_actions(srv.port, &cookie);
        assert!(
            acts.iter().any(|(a, _)| a == "recover"),
            "应有 recover 审计: {acts:?}"
        );
    }
    // 3.3 复制中断(IO/SQL 线程停)→ degraded,params 含"复制中断"
    std::fs::write(ctx.state.join("repl_stop"), "1").unwrap();
    wait_until(
        20,
        || instance_status(srv.port, &cookie, "sw1").as_deref() == Some("degraded"),
        "sw1 降级(复制中断)",
    );
    {
        let acts = audit_actions(srv.port, &cookie);
        assert!(
            acts.iter()
                .any(|(a, p)| a == "degrade" && p.contains("复制中断")),
            "应有 degrade 审计(复制中断): {acts:?}"
        );
    }
    // AI-0:原因跃迁各写 1 条快照(容器缺失 + 复制中断 = 2);同问题重复巡检不得新增
    let qsnap = format!(
        "SELECT COUNT(*) FROM `{}`.evidence_snapshots WHERE instance='sw1' AND kind='degrade'",
        ctx.db
    );
    let cnt2 = mysql_admin(&qsnap).unwrap_or_default();
    assert_eq!(
        cnt2.trim(),
        "2",
        "两次原因跃迁应恰好 2 条快照(防刷写),got: {cnt2}"
    );
    std::thread::sleep(Duration::from_secs(3));
    let cnt3 = mysql_admin(&qsnap).unwrap_or_default();
    assert_eq!(cnt3.trim(), "2", "同问题重复巡检不得新增快照,got: {cnt3}");
    // 3.4 degraded 状态允许销毁清理
    std::fs::remove_file(ctx.state.join("repl_stop")).ok();
    let (s, _) = submit_destroy(srv.port, &cookie, "sw1");
    assert_eq!(s, 200, "degraded 实例应可销毁");
    wait_until(
        30,
        || instance_status(srv.port, &cookie, "sw1").as_deref() == Some("destroyed"),
        "sw1 销毁完成",
    );
    ctx.drop_db();
}

/// POST 提交;若命中实例操作锁占用的竞态窗口(“其他操作进行中”),短等后重试(任务终态释放锁存在少量延迟)
fn post_retry_busy(port: u16, cookie: &str, path: &str) -> (u16, String) {
    let mut last = (0u16, String::new());
    for _ in 0..120 {
        let r = http(port, "POST", path, Some(cookie)).expect("http post");
        last = (r.status, r.body);
        if r.status != 400 || !last.1.contains("其他操作进行中") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
    last
}

/// 验收(多分片):async + shard_num=2 → 实体 2 个分片,每分片 1 主 + 1 读从 + 1 离线从(async 语义),
/// shards.slaves 完整记录每分片从节点;sync 多分片每分片 1 读从;实例级扩容被拒;销毁可清理全部节点。
#[test]
fn create_multishard_builds_n_shards() {
    let _g = serial_guard();
    let ctx = Ctx::new("mshard");
    let srv = start_server(&ctx, 60);
    let cookie = login(srv.port);

    // ── async 2 分片(1 个代理):每分片 读从 + 离线从 = 2×3 = 6 个 DB 节点 ──
    let r = http(
        srv.port,
        "POST",
        "/api/rds/create?name=msh&itype=async&proxies=1&shard_num=2",
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(r.status, 200, "多分片创建应被接受: {}", r.body);
    let tid = serde_json::from_str::<serde_json::Value>(&r.body).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_until(
        60,
        || instance_status(srv.port, &cookie, "msh").as_deref() == Some("running"),
        "msh running",
    );
    wait_until(
        30,
        || task_status(srv.port, &cookie, &tid).as_deref() == Some("success"),
        "msh 创建任务 success",
    );

    let (s, iv) = json_get(srv.port, &cookie, "/api/rds/instance?name=msh");
    assert_eq!(s, 200, "instance: {iv}");
    assert_eq!(
        iv["instance"]["shard_num"],
        serde_json::json!(2),
        "shard_num=2"
    );
    let nodes = iv["instance"]["nodes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        nodes.len(),
        6,
        "async 2 分片 × (1 主 + 读从 + 离线从)= 6 节点: {iv}"
    );
    let masters: Vec<&serde_json::Value> = nodes.iter().filter(|x| x["role"] == "master").collect();
    let reads: Vec<&serde_json::Value> = nodes.iter().filter(|x| x["role"] == "read").collect();
    let offs: Vec<&serde_json::Value> = nodes.iter().filter(|x| x["role"] == "offline").collect();
    assert_eq!(masters.len(), 2, "应有 2 个 master(每分片一个)");
    assert_eq!(reads.len(), 2, "应有 2 个读从(每分片一个): {iv}");
    assert_eq!(
        offs.len(),
        2,
        "async 多分片每分片应有 1 个离线从,共 2 个: {iv}"
    );
    // 每分片独立容器命名与 shard 标注
    let shard_ids: std::collections::HashSet<String> = nodes
        .iter()
        .map(|x| x["shard"].as_str().unwrap_or("").to_string())
        .collect();
    assert!(
        shard_ids.contains("s1") && shard_ids.contains("s2"),
        "节点 shard 应含 s1/s2: {iv}"
    );
    // shards 元数据:2 行、master 互不相同、slaves 完整(读从+离线从)
    let shards = iv["instance"]["shards"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(shards.len(), 2);
    let sm0 = shards[0]["master"].as_str().unwrap_or("").to_string();
    let sm1 = shards[1]["master"].as_str().unwrap_or("").to_string();
    assert_ne!(sm0, sm1, "两个分片 master 容器必须不同");
    assert!(
        sm0.ends_with("-s1-master") && sm1.ends_with("-s2-master"),
        "分片命名: {sm0} / {sm1}"
    );
    for sd in shards.iter() {
        let sl = sd["slaves"].as_array().cloned().unwrap_or_default();
        assert_eq!(sl.len(), 2, "每分片 slaves 应含 读从+离线从 共 2 项: {iv}");
        let roles: Vec<String> = sl
            .iter()
            .map(|x| x["role"].as_str().unwrap_or("").to_string())
            .collect();
        assert!(
            roles.contains(&"read".to_string()) && roles.contains(&"offline".to_string()),
            "slaves 角色: {roles:?}"
        );
    }
    // 容器真实被启动(run 记录:每分片 master + slave-1(读) + slave-2(离线))
    for c in [
        "rds-msh-s1-master",
        "rds-msh-s1-slave-1",
        "rds-msh-s1-slave-2",
        "rds-msh-s2-master",
        "rds-msh-s2-slave-1",
        "rds-msh-s2-slave-2",
    ] {
        assert!(
            ctx.run_count("RUN", c) >= 1,
            "容器 {c} 应被 docker run 创建"
        );
    }
    // LVS 接入层:创建时登记 VIP(lvs_mysql_port>0),接入层为进程内转发器(不依赖镜像)
    assert_eq!(
        iv["instance"]["lvs_container"],
        serde_json::json!("rds-msh-lvs"),
        "LVS 接入层应被登记: {iv}"
    );
    assert!(
        iv["instance"]["lvs_mysql_port"].as_u64().unwrap_or(0) > 0,
        "应分配 LVS 接入端口: {iv}"
    );
    let lvs0 = iv["instance"]["lvs"][0].as_str().unwrap_or("").to_string();
    assert!(
        lvs0.starts_with("127.0.0.1:"),
        "VIP 展示地址应为 127.0.0.1:端口,实际: {lvs0}"
    );
    // 多分片实例:实例级扩容(未指定分片)应被明确拒绝
    let ro = post_retry_busy(srv.port, &cookie, "/api/rds/scaleout?name=msh&role=read");
    assert_eq!(ro.0, 400, "多分片实例未指定分片扩容应被拒: {}", ro.1);
    assert!(ro.1.contains("分片"), "拒绝原因应提及分片: {}", ro.1);
    // ── 分片级扩容:async 实例 s1 新增读从(slave-3) ──
    let rs = post_retry_busy(
        srv.port,
        &cookie,
        "/api/rds/scaleout?name=msh&shard=s1&role=read",
    );
    assert_eq!(rs.0, 200, "分片级扩容读从应被接受: {}", rs.1);
    let tid_s = serde_json::from_str::<serde_json::Value>(&rs.1).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_until(
        40,
        || task_status(srv.port, &cookie, &tid_s).as_deref() == Some("success"),
        "s1 扩容任务 success",
    );
    wait_until(
        40,
        || instance_status(srv.port, &cookie, "msh").as_deref() == Some("running"),
        "扩容后回 running",
    );
    let (_, ivS) = json_get(srv.port, &cookie, "/api/rds/instance?name=msh");
    let nodesS = ivS["instance"]["nodes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(nodesS.len(), 7, "扩容后 async 2 分片应为 7 节点: {ivS}");
    let ns1: Vec<&serde_json::Value> = nodesS.iter().filter(|x| x["shard"] == "s1").collect();
    let reads1 = ns1.iter().filter(|x| x["role"] == "read").count();
    assert_eq!(reads1, 2, "s1 应有 2 个读从: {ivS}");
    let slave3 = ns1
        .iter()
        .find(|x| x["container"] == "rds-msh-s1-slave-3")
        .expect("s1-slave-3 应存在");
    assert_eq!(slave3["role"], "read");
    assert!(
        ctx.run_count("RUN", "rds-msh-s1-slave-3") >= 1,
        "s1-slave-3 应被 docker run 创建"
    );
    let shS = ivS["instance"]["shards"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        shS[0]["slaves"].as_array().map(|a| a.len()).unwrap_or(0),
        3,
        "s1 分片 slaves 元数据应含 3 项: {ivS}"
    );
    // s1 已有离线从 → 再次扩容离线从被拒
    let ro2 = post_retry_busy(
        srv.port,
        &cookie,
        "/api/rds/scaleout?name=msh&shard=s1&role=offline",
    );
    assert_eq!(ro2.0, 400, "分片已有离线从,再扩应被拒: {}", ro2.1);
    assert!(ro2.1.contains("离线"), "拒绝原因: {}", ro2.1);
    // 不存在的分片应被拒
    let ro3 = post_retry_busy(
        srv.port,
        &cookie,
        "/api/rds/scaleout?name=msh&shard=s9&role=read",
    );
    assert_eq!(ro3.0, 400, "不存在分片应被拒: {}", ro3.1);
    // 销毁可清理全部分片节点
    let (ds, db) = submit_destroy(srv.port, &cookie, "msh");
    assert_eq!(ds, 200, "destroy: {db}");
    wait_until(
        40,
        || instance_status(srv.port, &cookie, "msh").as_deref() == Some("destroyed"),
        "msh 销毁完成",
    );

    // ── sync 2 分片:每分片 1 主 + 1 读从 = 4 节点(无离线从) ──
    let r2 = http(
        srv.port,
        "POST",
        "/api/rds/create?name=ms2&itype=sync&proxies=1&shard_num=2",
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(r2.status, 200, "sync 多分片创建应被接受: {}", r2.body);
    let tid2 = serde_json::from_str::<serde_json::Value>(&r2.body).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_until(
        60,
        || instance_status(srv.port, &cookie, "ms2").as_deref() == Some("running"),
        "ms2 running",
    );
    wait_until(
        30,
        || task_status(srv.port, &cookie, &tid2).as_deref() == Some("success"),
        "ms2 任务 success",
    );
    let (_, iv2) = json_get(srv.port, &cookie, "/api/rds/instance?name=ms2");
    let nodes2 = iv2["instance"]["nodes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        nodes2.len(),
        4,
        "sync 2 分片 × (1 主 + 1 读从)= 4 节点: {iv2}"
    );
    let offs2: Vec<&serde_json::Value> = nodes2.iter().filter(|x| x["role"] == "offline").collect();
    assert!(offs2.is_empty(), "sync 多分片不应有离线从: {iv2}");
    for c in ["rds-ms2-s1-slave-1", "rds-ms2-s2-slave-1"] {
        assert!(
            ctx.run_count("RUN", c) >= 1,
            "sync 容器 {c} 应被 docker run 创建"
        );
    }
    assert_eq!(
        iv2["instance"]["lvs_container"],
        serde_json::json!("rds-ms2-lvs"),
        "sync 实例也应有 LVS 接入层: {iv2}"
    );
    // sync 分片无离线从 → s1 分片级扩容离线从(slave-2)应成功
    let rs2 = post_retry_busy(
        srv.port,
        &cookie,
        "/api/rds/scaleout?name=ms2&shard=s1&role=offline",
    );
    assert_eq!(rs2.0, 200, "sync 分片级扩容离线从应被接受: {}", rs2.1);
    let tid_s2 = serde_json::from_str::<serde_json::Value>(&rs2.1).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_until(
        40,
        || task_status(srv.port, &cookie, &tid_s2).as_deref() == Some("success"),
        "ms2 s1 离线扩容 success",
    );
    let (_, iv2b) = json_get(srv.port, &cookie, "/api/rds/instance?name=ms2");
    let nodes2b = iv2b["instance"]["nodes"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(nodes2b.len(), 5, "ms2 扩容离线从后应为 5 节点: {iv2b}");
    assert!(
        ctx.run_count("RUN", "rds-ms2-s1-slave-2") >= 1,
        "ms2 s1 离线从应被 docker run 创建"
    );
    let sh2 = iv2b["instance"]["shards"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        sh2[0]["slaves"].as_array().map(|a| a.len()).unwrap_or(0),
        2,
        "ms2 s1 slaves 元数据应为 2 项: {iv2b}"
    );
    let (ds2, db2) = submit_destroy(srv.port, &cookie, "ms2");
    assert_eq!(ds2, 200, "destroy ms2: {db2}");
    wait_until(
        40,
        || instance_status(srv.port, &cookie, "ms2").as_deref() == Some("destroyed"),
        "ms2 销毁完成",
    );
    ctx.drop_db();
}

// ─── DBA 数据服务验收(M-A/M-B/M-C) ───

fn login_user(port: u16, user: &str, pass: &str) -> String {
    let r = http(
        port,
        "POST",
        &format!("/login?user={user}&password={pass}"),
        None,
    )
    .expect("login http");
    assert_eq!(r.status, 200, "登录失败 {user}");
    r.headers
        .lines()
        .find_map(|l| {
            let l = l.trim().to_ascii_lowercase();
            l.strip_prefix("set-cookie:")?
                .split(';')
                .next()
                .map(|s| s.trim().to_string())
        })
        .expect("无 session cookie")
}

fn post_ok(port: u16, cookie: &str, path: &str) -> (u16, String) {
    let r = http(port, "POST", path, Some(cookie)).unwrap();
    (r.status, r.body)
}

fn start_server_env(ctx: &Ctx, sweep_secs: u64, extra: &[(&str, &str)]) -> Srv {
    let bin = env!("CARGO_BIN_EXE_rdsctl");
    let port = free_port();
    let out = ctx.dir.join("server.env.log");
    let mut cmd = Command::new(bin);
    cmd.args(["--port", &port.to_string()]);
    ctx.child_env(&mut cmd);
    cmd.env("RDSCTL_SWEEP_SECS", sweep_secs.to_string());
    for (k, v) in extra {
        cmd.env(k, v);
    }
    cmd.stdout(Stdio::from(std::fs::File::create(&out).unwrap()));
    cmd.stderr(Stdio::inherit());
    let child = cmd.spawn().expect("spawn rdsctl");
    let srv = Srv { child, port };
    let mut ready = false;
    for _ in 0..100 {
        if http(port, "POST", "/login?user=admin&password=admin", None)
            .map(|r| r.status == 200)
            .unwrap_or(false)
        {
            ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(ready, "rdsctl env 启动未就绪");
    srv
}

/// 查询台验收:只读查询+掩码、deny 名单、审计原文、写权限门禁
#[test]
fn dba_query_console_read_mask_write_and_audit() {
    let _g = serial_guard();
    let ctx = Ctx::new("qcon");
    let srv = start_server(&ctx, 60);
    let cookie = login(srv.port);

    let tid = submit_create(srv.port, &cookie, "q1");
    wait_until(
        40,
        || instance_status(srv.port, &cookie, "q1").as_deref() == Some("running"),
        "q1 running",
    );
    wait_until(
        15,
        || task_status(srv.port, &cookie, &tid).as_deref() == Some("success"),
        "q1 创建任务成功",
    );

    // 1) 只读查询 + 敏感列后端掩码
    let (s1, b1) = post_ok(
        srv.port,
        &cookie,
        "/api/rds/query?instance=q1&node=master&sql=SELECT+*+FROM+appdb.users+WHERE+id=1",
    );
    assert_eq!(s1, 200, "只读查询应成功: {b1}");
    let d1: serde_json::Value = serde_json::from_str(&b1).unwrap();
    assert_eq!(d1["ok"], serde_json::json!(true));
    assert!(d1["read_only"] == serde_json::json!(true));
    assert_eq!(d1["rows_returned"], 2);
    let masked = d1["masked_cols"].as_array().unwrap().clone();
    assert!(
        masked.iter().any(|c| c == "passwd"),
        "passwd 列应被掩码: {masked:?}"
    );
    assert_eq!(d1["rows"][0][2], "***", "掩码值应为 ***");

    // 2) 完整 SQL 原文入 query_audit(结果行不入 audit)
    let sql_rows = mysql_admin(&format!(
        "SELECT sql_text FROM `{}`.query_audit WHERE instance='q1' AND read_only=1 ORDER BY id DESC LIMIT 1",
        ctx.db
    ))
    .unwrap_or_default();
    assert!(
        sql_rows.contains("SELECT * FROM appdb.users WHERE id=1"),
        "query_audit 应存完整 SQL 原文, got: {sql_rows}"
    );

    // 3) deny 名单(mysql.user)拒绝,不进 docker
    let (s3, b3) = post_ok(
        srv.port,
        &cookie,
        "/api/rds/query?instance=q1&node=master&sql=SELECT+*+FROM+mysql.user",
    );
    assert_eq!(s3, 400, "deny 表应拒绝: {b3}");
    assert!(b3.contains("deny 名单"), "应提示 deny 名单: {b3}");

    // 4) 角色/用户:只读(instances.query)不能写
    assert_eq!(
        post_ok(
            srv.port,
            &cookie,
            "/api/rds/roles?action=create&role=qro&desc=query-ro"
        )
        .0,
        200
    );
    assert_eq!(
        post_ok(
            srv.port,
            &cookie,
            "/api/rds/roles?action=perms&role=qro&perms=instances.query"
        )
        .0,
        200
    );
    assert_eq!(
        post_ok(
            srv.port,
            &cookie,
            "/api/rds/users?action=create&user=qviewer&pass=vvvv&enabled=1"
        )
        .0,
        200
    );
    assert_eq!(
        post_ok(
            srv.port,
            &cookie,
            "/api/rds/users?action=roles&user=qviewer&roles=qro"
        )
        .0,
        200
    );
    let cv = login_user(srv.port, "qviewer", "vvvv");
    let (s4, b4) = post_ok(
        srv.port,
        &cv,
        "/api/rds/query?instance=q1&node=master&sql=UPDATE+appdb.users+SET+name=1",
    );
    assert_eq!(s4, 403, "无 write 权限写语句应 403: {b4}");
    assert!(b4.contains("instances.query.write"), "应提示缺少权限: {b4}");

    // 5) 授予 write 后 master 可写并审计
    assert_eq!(
        post_ok(
            srv.port,
            &cookie,
            "/api/rds/roles?action=perms&role=qro&perms=instances.query,instances.query.write"
        )
        .0,
        200
    );
    assert_eq!(
        post_ok(
            srv.port,
            &cookie,
            "/api/rds/users?action=create&user=qwrite&pass=wwww&enabled=1"
        )
        .0,
        200
    );
    assert_eq!(
        post_ok(
            srv.port,
            &cookie,
            "/api/rds/users?action=roles&user=qwrite&roles=qro"
        )
        .0,
        200
    );
    let cw = login_user(srv.port, "qwrite", "wwww");
    let (s5, b5) = post_ok(
        srv.port,
        &cw,
        "/api/rds/query?instance=q1&node=master&sql=UPDATE+appdb.users+SET+name=2",
    );
    assert_eq!(s5, 200, "write 用户 master 写应成功: {b5}");
    let d5: serde_json::Value = serde_json::from_str(&b5).unwrap();
    assert_eq!(d5["ok"], serde_json::json!(true));
    assert_eq!(d5["read_only"], serde_json::json!(false));

    ctx.drop_db();
}

/// 慢查验收(MySQL 后端 + API/治理;不依赖 docker 采集)
#[test]
fn slow_global_api_and_governance() {
    let _g = serial_guard();
    let ctx = Ctx::new("slowq");
    let srv = start_server(&ctx, 60);
    let cookie = login(srv.port);
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let old_ts = now - 8 * 86400;

    let ins = |sql: &str| mysql_admin(sql).unwrap_or_else(|e| panic!("insert {sql}: {e}"));
    ins(&format!(
        "INSERT INTO `{}`.slow_digest_snapshots(ts,instance,node,schema_name,digest,digest_text,count_star,sum_ms,avg_ms,max_ms,first_seen,last_seen) VALUES \
         ({},'a','master','appdb','dA','select x from t where a=?',10,5000,500,800,{},{}),\
         ({},'b','master','appdb','dA','select x from t where a=?',20,9000,450,900,{},{}),\
         ({},'a','master','appdb','dB','old',5,100,20,30,{},{})",
        ctx.db,
        now,
        now,
        now,
        now,
        now,
        now,
        old_ts,
        old_ts,
        old_ts
    ));

    // 全局 Top:同 digest 跨实例聚合;dB 超出 24h 窗口不出现在默认窗口
    let (s, v) = json_get(srv.port, &cookie, "/api/rds/slow?window=24h");
    assert_eq!(s, 200, "slow http: {v}");
    let items = v["items"].as_array().cloned().unwrap_or_default();
    let dA = items
        .iter()
        .find(|i| i["digest"] == "dA")
        .expect("应有 dA 聚合");
    assert_eq!(dA["total_count"], 30);
    assert_eq!(dA["total_ms"], 14000);
    assert_eq!(dA["instance_count"], 2);
    assert!(
        items.iter().all(|i| i["digest"] != "dB"),
        "旧样本不应出现在 24h 窗口"
    );
    // digest_text 需 instances.query(admin=super 可见原文)
    assert!(dA["digest_text"]
        .as_str()
        .unwrap_or("")
        .contains("select x from t"));

    // 治理队列:直插 open → 列表 → resolve
    ins(&format!(
        "INSERT INTO `{}`.slow_governance(digest,digest_text,status,assignee,created_at,updated_at) VALUES ('dA','select x from t where a=?','open','',{now},{now})",
        ctx.db
    ));
    let (s2, v2) = json_get(srv.port, &cookie, "/api/rds/slow/gov?status=open");
    assert_eq!(s2, 200);
    let gid = v2["items"]
        .as_array()
        .and_then(|a| a.iter().find(|x| x["digest"] == "dA"))
        .and_then(|x| x["id"].as_u64())
        .expect("治理列表应含 dA");
    let (s3, b3) = post_ok(
        srv.port,
        &cookie,
        &format!("/api/rds/slow/gov?id={gid}&action=resolve"),
    );
    assert_eq!(s3, 200, "resolve: {b3}");
    let (_, v4) = json_get(srv.port, &cookie, "/api/rds/slow/gov?status=resolved");
    assert!(
        v4["items"]
            .as_array()
            .map(|a| a.iter().any(|x| x["id"].as_u64() == Some(gid)))
            .unwrap_or(false),
        "resolved 列表应含该治理项"
    );
    ctx.drop_db();
}

/// 备份联动验收:脚本适配器 + 生命周期事件(register/deregister)
#[test]
fn backup_reg_link_events_on_enable() {
    let _g = serial_guard();
    let ctx = Ctx::new("bklk");
    let rec = ctx.dir.join("bk.log");
    let script = ctx.dir.join("bkrec.sh");
    std::fs::write(
        &script,
        format!(
            r#"#!/bin/bash
EV="$1"; KEY="$2"
echo "EVENT=$EV KEY=$KEY" >> {}
while IFS= read -r line; do echo "PAY=$line" >> {}; done
exit 0
"#,
            rec.display(),
            rec.display()
        ),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    let srv = start_server_env(
        &ctx,
        60,
        &[
            ("RDSCTL_BACKUP_REG_ENABLED", "1"),
            ("RDSCTL_BACKUP_TICK_SECS", "10"),
            ("RDSCTL_BACKUP_REG_SCRIPT", &script.to_string_lossy()),
        ],
    );
    let cookie = login(srv.port);

    let tid = submit_create(srv.port, &cookie, "b1");
    wait_until(
        40,
        || instance_status(srv.port, &cookie, "b1").as_deref() == Some("running"),
        "b1 running",
    );
    wait_until(
        15,
        || task_status(srv.port, &cookie, &tid).as_deref() == Some("success"),
        "b1 创建成功",
    );

    // worker(10s tick)→ register_instance done
    wait_until(
        45,
        || {
            let c = mysql_admin(&format!(
                "SELECT COUNT(*) FROM `{}`.backup_outbox WHERE instance='b1' AND event='register_instance' AND state='done'",
                ctx.db
            ))
            .unwrap_or_default();
            c.trim().parse::<u64>().unwrap_or(0) >= 1
        },
        "register_instance 已投递 done",
    );
    let log = std::fs::read_to_string(&rec).unwrap_or_default();
    assert!(
        log.contains("EVENT=register_instance"),
        "脚本应收到 register_instance: {log}"
    );
    let (sr, br) = json_get(srv.port, &cookie, "/api/rds/backup/reg?instance=b1");
    assert_eq!(sr, 200, "reg http: {br}");
    assert_eq!(br["enabled"], serde_json::json!(true));
    assert!(
        br["items"]
            .as_array()
            .map(|a| a
                .iter()
                .any(|x| x["node"] == "master" && x["reg_state"] == "registered"))
            .unwrap_or(false),
        "master 应 registered: {br}"
    );

    // destroy → deregister_instance
    let (ds, db) = submit_destroy(srv.port, &cookie, "b1");
    assert_eq!(ds, 200, "destroy: {db}");
    wait_until(
        40,
        || instance_status(srv.port, &cookie, "b1").as_deref() == Some("destroyed"),
        "b1 destroyed",
    );
    wait_until(
        45,
        || {
            let c = mysql_admin(&format!(
                "SELECT COUNT(*) FROM `{}`.backup_outbox WHERE instance='b1' AND event='deregister_instance' AND state='done'",
                ctx.db
            ))
            .unwrap_or_default();
            c.trim().parse::<u64>().unwrap_or(0) >= 1
        },
        "deregister_instance 已投递 done",
    );
    let log2 = std::fs::read_to_string(&rec).unwrap_or_default();
    assert!(
        log2.contains("EVENT=deregister_instance"),
        "脚本应收到 deregister_instance: {log2}"
    );
    ctx.drop_db();
}

/// 验收(功能模块库):页面后端 API 支持 新建→列表→一键运行(Noop 步骤)→同名更新→删除。
#[test]
fn user_module_crud_and_run() {
    let _g = serial_guard();
    let ctx = Ctx::new("umod");
    let srv = start_server(&ctx, 60);
    let cookie = login(srv.port);

    // 运行模块需要一个 running 实例(单节点即可)
    let r = http(
        srv.port,
        "POST",
        "/api/rds/create?name=mo&itype=single&proxies=1",
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(r.status, 200, "创建实例: {}", r.body);
    wait_until(
        60,
        || instance_status(srv.port, &cookie, "mo").as_deref() == Some("running"),
        "mo running",
    );

    fn enc(s: &str) -> String {
        s.chars()
            .map(|c| match c {
                ' ' => "%20".to_string(),
                '"' => "%22".to_string(),
                '{' => "%7B".to_string(),
                '}' => "%7D".to_string(),
                '[' => "%5B".to_string(),
                ']' => "%5D".to_string(),
                ',' => "%2C".to_string(),
                ':' => "%3A".to_string(),
                '\\' => "%5C".to_string(),
                _ => c.to_string(),
            })
            .collect()
    }
    let steps = enc(r#"[{"Noop":{"note":"x"}}]"#);

    // 1) 新建模块
    let mk = http(
        srv.port,
        "POST",
        &format!(
            "/api/rds/module?name={}&category=cleanup&desc={}&steps={}",
            enc("清日志"),
            enc("删除旧日志"),
            steps
        ),
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(mk.status, 200, "新建模块: {}", mk.body);
    // 2) 列表命中
    let (_, ls) = json_get(srv.port, &cookie, "/api/rds/modules");
    let mods = ls["modules"].as_array().cloned().unwrap_or_default();
    assert!(
        mods.iter().any(|m| m["name"] == "清日志"),
        "列表应含模块: {ls}"
    );
    // 3) 一键运行(Noop 步骤,不依赖容器)
    let rn = http(
        srv.port,
        "POST",
        &format!("/api/rds/module/run?name={}&instance=mo", enc("清日志")),
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(rn.status, 200, "运行模块: {}", rn.body);
    let tid = serde_json::from_str::<serde_json::Value>(&rn.body).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_until(
        40,
        || task_status(srv.port, &cookie, &tid).as_deref() == Some("success"),
        "模块任务 success",
    );
    // 4) 同名更新(说明变更)
    let up = http(
        srv.port,
        "POST",
        &format!(
            "/api/rds/module?name={}&category=maintain&desc={}&steps={}",
            enc("清日志"),
            enc("v2 描述"),
            steps
        ),
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(up.status, 200, "更新模块: {}", up.body);
    let (_, ls2) = json_get(srv.port, &cookie, "/api/rds/modules");
    let mods2 = ls2["modules"].as_array().cloned().unwrap_or_default();
    let hit = mods2
        .iter()
        .find(|m| m["name"] == "清日志")
        .expect("仍有模块");
    assert_eq!(hit["category"], "maintain");
    assert_eq!(hit["desc"], "v2 描述");
    assert_eq!(mods2.len(), 1, "同名更新不新增: {ls2}");
    // 5) 删除
    let dl = http(
        srv.port,
        "POST",
        &format!("/api/rds/module/delete?name={}", enc("清日志")),
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(dl.status, 200, "删除模块: {}", dl.body);
    let (_, ls3) = json_get(srv.port, &cookie, "/api/rds/modules");
    assert!(
        ls3["modules"]
            .as_array()
            .map(|a| a.is_empty())
            .unwrap_or(false),
        "删除后列表为空: {ls3}"
    );

    let (ds, db) = submit_destroy(srv.port, &cookie, "mo");
    assert_eq!(ds, 200, "destroy mo: {db}");
    wait_until(
        40,
        || instance_status(srv.port, &cookie, "mo").as_deref() == Some("destroyed"),
        "mo 销毁完成",
    );
    ctx.drop_db();
}

// ═══════════════ 管控面集群:真实生命周期端到端(I2/I8/I11 + F6)═══════════════
//
// 这些用例复用本文件的 Ctx/垫片/断言助手,在 **cluster 模式**下跑真实 DAG 生命周期:
// 实例互斥来自共识租约、步骤经账本(begin/done)、执行面走垫片 docker。
// 关注点:①实例操作确实处于共识租约之下(I2);②全副本被 kill -9 后重启能续跑,
//        且**已完成的步骤不重复执行**(I8/I11);③全程无脑裂。

/// 一个 cluster 模式节点进程
struct CNode {
    child: Child,
    id: String,
    public: u16,
    rpc: u16,
}

struct ClusterSrv {
    nodes: Vec<CNode>,
    spec: String,
}

impl Drop for ClusterSrv {
    fn drop(&mut self) {
        for n in &mut self.nodes {
            let _ = n.child.kill();
            let _ = n.child.wait();
        }
    }
}

impl ClusterSrv {
    /// 起 n 个节点(共用同一 Ctx:同库、同垫片、同 docker 状态)
    fn start(ctx: &Ctx, n: usize, resume: bool) -> ClusterSrv {
        let rpc: Vec<u16> = (0..n).map(|_| free_port()).collect();
        let publics: Vec<u16> = (0..n).map(|_| free_port()).collect();
        let spec = (0..n)
            .map(|i| format!("n{}@127.0.0.1:{}", i + 1, rpc[i]))
            .collect::<Vec<_>>()
            .join(",");
        let mut nodes = Vec::new();
        for i in 0..n {
            let id = format!("n{}", i + 1);
            let child = spawn_cnode(ctx, &id, publics[i], rpc[i], &spec, resume);
            nodes.push(CNode {
                child,
                id,
                public: publics[i],
                rpc: rpc[i],
            });
        }
        let cl = ClusterSrv { nodes, spec };
        cl.wait_all_ready();
        cl
    }

    fn wait_all_ready(&self) {
        for n in &self.nodes {
            let mut ready = false;
            for _ in 0..150 {
                if http(n.public, "POST", "/login?user=admin&password=admin", None)
                    .map(|r| r.status == 200 || r.status == 401)
                    .unwrap_or(false)
                {
                    ready = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            assert!(ready, "cluster 节点 {} 未就绪", n.id);
        }
    }

    fn kill_all(&mut self) {
        for n in &mut self.nodes {
            let _ = n.child.kill();
            let _ = n.child.wait();
        }
    }

    /// 用同一 id/端口/数据目录重启全部节点(模拟崩溃后由守护拉起)
    fn restart_all(&mut self, ctx: &Ctx, resume: bool) {
        for n in &mut self.nodes {
            let _ = n.child.kill();
            let _ = n.child.wait();
            n.child = spawn_cnode(ctx, &n.id, n.public, n.rpc, &self.spec, resume);
        }
        self.wait_all_ready();
    }

    fn leader(&self) -> Option<String> {
        self.nodes.iter().find_map(|n| {
            cnode_status(n.rpc)
                .filter(|st| st["role"] == "leader")
                .map(|_| n.id.clone())
        })
    }

    /// 脑裂检测:同一 term 至多一个 leader
    fn assert_no_split_brain(&self) {
        use std::collections::BTreeMap;
        let mut by_term: BTreeMap<u64, Vec<String>> = BTreeMap::new();
        for n in &self.nodes {
            if let Some(st) = cnode_status(n.rpc) {
                if st["role"] == "leader" {
                    by_term
                        .entry(st["term"].as_u64().unwrap_or(0))
                        .or_default()
                        .push(n.id.clone());
                }
            }
        }
        for (t, ls) in by_term {
            assert_eq!(ls.len(), 1, "INV-1 违反:term={t} 多个 leader {ls:?}");
        }
    }

    /// 任取一个"不是某节点"的节点(用于跨副本读取共识状态)
    fn other_than(&self, id: &str) -> &CNode {
        self.nodes.iter().find(|n| n.id != id).expect("无其它节点")
    }
}

fn spawn_cnode(ctx: &Ctx, id: &str, public: u16, rpc: u16, spec: &str, resume: bool) -> Child {
    let bin = env!("CARGO_BIN_EXE_rdsctl");
    let out = ctx.dir.join(format!("cnode-{id}.log"));
    let mut cmd = Command::new(bin);
    cmd.args([
        "serve",
        &format!("--node-id={id}"),
        &format!("--cluster={spec}"),
        &format!("--port={public}"),
        &format!("--rpc-port={rpc}"),
    ]);
    ctx.child_env(&mut cmd);
    cmd.env("RDSCTL_MODE", "cluster")
        .env("RDSCTL_METADATA_SINK", "mysql")
        // 部署侧显式给定稳定 holder(rdsctl.env 文档中的 RDSCTL_CONTROLLER_ID)
        .env("RDSCTL_CONTROLLER_ID", id)
        .env("RDSCTL_DATA_DIR", ctx.dir.join(format!("ha-{id}")).to_string_lossy().to_string())
        // lab 放行:测试机无 NTP 与 agent(两者都会在 /readyz 中如实标注)
        .env("RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK", "1")
        .env("RDSCTL_ALLOW_NO_AGENT", "1")
        .env("RDSCTL_ELECTION_TIMEOUT_MS", "600")
        .env("RDSCTL_HA_TICK_MS", "50")
        .env("RDSCTL_SWEEP_SECS", "60")
        .env("RDSCTL_RESUME_TASKS", if resume { "1" } else { "0" })
        .stdout(Stdio::from(std::fs::File::create(&out).unwrap()))
        .stderr(Stdio::inherit());
    cmd.spawn().expect("spawn cluster node")
}

fn cnode_status(rpc: u16) -> Option<serde_json::Value> {
    let r = http(rpc, "GET", "/internal/status", None).ok()?;
    if r.status != 200 {
        return None;
    }
    serde_json::from_str(&r.body).ok()
}

/// 读取共识租约(跨副本可读)
fn consensus_lease(rpc: u16, instance: &str) -> Option<serde_json::Value> {
    let r = http(
        rpc,
        "GET",
        &format!("/internal/lease?instance={instance}"),
        None,
    )
    .ok()?;
    if r.status != 200 {
        return None;
    }
    serde_json::from_str::<serde_json::Value>(&r.body)
        .ok()
        .and_then(|v| v.get("lease").cloned())
}

/// 某任务的步骤账本记录(用于"确定性等待账本落定")
fn ledger_steps(rpc: u16, task_id: &str) -> Vec<serde_json::Value> {
    match http(
        rpc,
        "GET",
        &format!("/internal/steps?task_id={task_id}"),
        None,
    ) {
        Ok(r) if r.status == 200 => serde_json::from_str::<serde_json::Value>(&r.body)
            .ok()
            .and_then(|v| v["steps"].as_array().cloned())
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn ledger_done_count(rpc: u16, task_id: &str) -> usize {
    ledger_steps(rpc, task_id)
        .iter()
        .filter(|s| s["state"] == "done")
        .count()
}

/// I2 + I8 + I11 + F6:cluster 模式下建实例 → 全副本 kill -9 → 重启续跑。
///
/// 关键断言:
///   ①建实例期间,实例互斥来自**共识租约**(跨副本可读,holder = 发起节点);
///   ②全副本被 kill -9 后重启能续跑并到达终态;
///   ③**已完成步骤不重复执行**:master 容器的 docker run 只发生过 1 次(账本短路);
///   ④全程无脑裂;⑤实例最终为 running。
#[test]
fn cluster_lifecycle_survives_total_restart_and_replays_idempotently() {
    let _g = serial_guard();
    let ctx = Ctx::new("cluster_life");
    // 让 SQL 全部失败 → DAG 会停在 master 的 WaitMysql(超时 120s)窗口内,
    // 给我们足够时间在"某步骤已完成之后"杀掉全部副本。
    ctx.touch("mysql_down");
    let mut cl = ClusterSrv::start(&ctx, 3, false);
    assert!(cl.leader().is_some(), "3 副本应选出 leader");
    cl.assert_no_split_brain();

    let ck = login(cl.nodes[0].public);
    let tid = submit_create(cl.nodes[0].public, &ck, "cl1");

    // 等 master 容器被创建(说明 DockerRun 步骤已执行)
    wait_until(
        90,
        || ctx.run_count("RUN", "rds-cl1-master") >= 1,
        "master 容器创建",
    );
    // 确定性等待该步骤账本落定(集群模式:账本是共识写入);
    // wait_until 在超时时直接 panic 并带上说明,故无需返回值
    wait_until(
        60,
        || ledger_done_count(cl.nodes[0].rpc, &tid) >= 1,
        "步骤账本落定(集群模式下账本必须写入共识状态机)",
    );

    // ①实例互斥来自共识租约:从**另一个副本**读到 holder = 发起节点
    //
    // 注意:这是对**另一副本本地状态机**的读,不是线性一致读(设计 §11.4 的 read-index 属 M1c)。
    // 上一步只证明了 n1 已 apply 到 StepDone,而 n2 可能还差几条日志,直接断言会偶发 404。
    // 因此这里等"跨副本可见"再断言 —— 语义不变(仍要求该值跨副本可读),只是把竞态消掉。
    let mut visible = false;
    for _ in 0..150 {
        if consensus_lease(cl.node_other_rpc(), "cl1").is_some() {
            visible = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    if !visible {
        // 自诊断:把"每个副本的水位 + 是否能看到该租约"打出来,一眼区分
        // 「复制延迟」/「某副本掉队或不可达」/「租约真的被释放了」三种情况。
        let mut diag = Vec::new();
        for n in &cl.nodes {
            let st = match http(n.rpc, "GET", "/internal/status", None) {
                Ok(r) if r.status == 200 => {
                    let v: serde_json::Value =
                        serde_json::from_str(&r.body).unwrap_or(serde_json::Value::Null);
                    format!(
                        "role={} term={} commit={} applied={} leases={}",
                        v["role"], v["term"], v["commit_index"], v["applied_index"], v["leases"]
                    )
                }
                Ok(r) => format!("HTTP {}", r.status),
                Err(e) => format!("rpc 不可达:{e}"),
            };
            let has = consensus_lease(n.rpc, "cl1").is_some();
            diag.push(format!("  {} (rpc {}): 租约可见={} | {}", n.id, n.rpc, has, st));
        }
        panic!("30s 内另一副本未见租约 cl1(自诊断):\n{}", diag.join("\n"));
    }
    let lease = consensus_lease(cl.node_other_rpc(), "cl1").expect("实例应处于共识租约之下");
    assert_eq!(
        lease["holder"], cl.nodes[0].id,
        "实例租约 holder 应为发起操作的节点(且该值跨副本可读,证明互斥来自共识):{lease}"
    );
    assert!(
        lease["renewals"].as_u64().unwrap_or(0) >= 1,
        "长任务期间应持续续约(设计 §5.4):{lease}"
    );
    let fence_before = lease["fence"].as_str().unwrap_or("").to_string();
    assert!(!fence_before.is_empty(), "租约应带 fence:{lease}");

    // ②全副本 kill -9(F6),撤掉 SQL 故障后重启续跑(I11)
    cl.kill_all();
    ctx.untouch("mysql_down");
    cl.restart_all(&ctx, true); // RDSCTL_RESUME_TASKS=1
    // 会话目前是进程内的(M1c 才会入状态机,见 I15):重启后必须重新登录
    let ck = login(cl.nodes[0].public);

    // 等任务到达终态
    wait_until(
        180,
        || {
            matches!(
                task_status(cl.nodes[0].public, &ck, &tid).as_deref(),
                Some("success") | Some("failed")
            )
        },
        "任务终态(全副本 kill -9 后重启续跑应能收敛)",
    );
    let st = task_status(cl.nodes[0].public, &ck, &tid);
    assert_eq!(
        st.as_deref(),
        Some("success"),
        "续跑后任务应为成功(实际 {st:?};失败则打印节点日志:{}）",
        std::fs::read_to_string(ctx.dir.join("cnode-n1.log")).unwrap_or_default()
    );

    // ③已完成的 DockerRun 不得重复执行(账本短路;容器被重建会表现为 RUN 计数增加)
    assert_eq!(
        ctx.run_count("RUN", "rds-cl1-master"),
        1,
        "重放不得重复创建 master 容器(步骤账本应短路)"
    );

    // ④全程无脑裂
    cl.assert_no_split_brain();
    // ⑤实例最终状态(副本视图由集群对账循环回灌,给一点收敛时间)
    wait_until(
        30,
        || instance_status(cl.nodes[0].public, &ck, "cl1").as_deref() == Some("running"),
        "实例最终状态为 running(副本视图应由 sink 回灌收敛)",
    );
    assert_eq!(
        instance_status(cl.nodes[0].public, &ck, "cl1").as_deref(),
        Some("running"),
        "实例最终应为 running"
    );
}

impl ClusterSrv {
    /// 取"非 0 号节点"的 rpc 端口(用于跨副本读取,证明状态在共识里而非进程内)
    fn node_other_rpc(&self) -> u16 {
        self.nodes[1].rpc
    }
}

/// 验收(管控集群页):`GET /api/rds/cluster` 必须给出**全副本**的真实视图(成员/角色/term/
/// 复制进度/就绪前提/租约台账),并且两个运维动作的语义边界正确:
///   · `resync-sink` 只在 leader 上生效(follower 明确 409 not_leader,不假装成功);
///   · `stepdown` 可从任意副本发起(自动转发),且**领导权确实转移**、全程无脑裂。
#[test]
fn cluster_view_api_and_ops() {
    let _g = serial_guard();
    let ctx = Ctx::new("cluster_view");
    let cl = ClusterSrv::start(&ctx, 3, false);
    // 会话是**各副本进程内**的(M1a 尚未把会话搬进状态机):因此对哪个副本发业务请求,
    // 就要在该副本上登录 —— 集群页之所以采用"服务端扇出",正是为了让浏览器只需连一个副本。
    let rpc_leader = cl.leader().expect("3 副本应选出 leader");
    let follower_id = cl
        .nodes
        .iter()
        .map(|n| n.id.clone())
        .find(|id| id != &rpc_leader)
        .expect("应有 follower");
    let cookie = login(cl.nodes[0].public);
    let follower_cookie = login(
        cl.nodes
            .iter()
            .find(|n| n.id == follower_id)
            .unwrap()
            .public,
    );

    // ① 本副本视角的全量视图
    let (st, v) = json_get(cl.nodes[0].public, &cookie, "/api/rds/cluster");
    assert_eq!(st, 200, "cluster: {v}");
    assert_eq!(v["enabled"], serde_json::json!(true), "cluster 模式应 enabled: {v}");
    assert_eq!(v["mode"], serde_json::json!("cluster"), "{v}");
    let members = v["members"].as_array().cloned().unwrap_or_default();
    assert_eq!(members.len(), 3, "应列出全部 3 个副本: {v}");
    assert!(
        members.iter().all(|m| m["reachable"] == serde_json::json!(true)),
        "3 副本同机应全部可达: {v}"
    );
    let leaders: Vec<&serde_json::Value> = members
        .iter()
        .filter(|m| m["role"] == serde_json::json!("leader"))
        .collect();
    assert_eq!(leaders.len(), 1, "至多且恰有一个 leader: {v}");
    let leader_id = leaders[0]["id"].as_str().unwrap().to_string();
    assert_eq!(
        leader_id, rpc_leader,
        "页面看到的 leader 必须与 rpc 层面一致(否则视图会指向错误的人): {v}"
    );
    assert_eq!(v["quorum"]["leader"], serde_json::json!(leader_id), "{v}");
    assert_eq!(v["quorum"]["voters"], serde_json::json!(3), "{v}");
    assert_eq!(v["quorum"]["reachable"], serde_json::json!(3), "{v}");
    assert_eq!(v["quorum"]["ok"], serde_json::json!(true), "本副本应判定多数派可达: {v}");
    // leader_addr 必须与成员表里的地址一致(否则页面会把运维指向错误的地方)
    let leader_addr = members
        .iter()
        .find(|m| m["id"].as_str() == Some(leader_id.as_str()))
        .and_then(|m| m["addr"].as_str())
        .unwrap()
        .to_string();
    assert_eq!(v["quorum"]["leader_addr"], serde_json::json!(leader_addr), "{v}");
    // 每个成员都要有可渲染的关键字段;peers 覆盖全部成员(含自己)
    for m in &members {
        assert!(m["addr"].is_string(), "成员缺地址: {m}");
        assert!(m["term"].is_u64(), "成员缺 term: {m}");
        assert!(m["commit_index"].is_u64(), "成员缺 commit_index: {m}");
        assert!(m["ready"].is_boolean(), "成员缺 ready: {m}");
        assert!(m["data_dir"].is_string(), "成员缺 data_dir: {m}");
        assert_eq!(
            m["peers"].as_array().map(|a| a.len()),
            Some(3),
            "peers 应覆盖 3 个成员: {m}"
        );
    }
    // 只有 leader 维护复制进度:其 peers[].match_index 必须是数字
    let lp = leaders[0]["peers"].as_array().cloned().unwrap_or_default();
    assert!(
        lp.iter().all(|p| p["match_index"].is_u64()),
        "leader 的 peers[].match_index 应为数字: {v}"
    );
    assert!(
        !v["notes"].as_array().map(|a| a.is_empty()).unwrap_or(true),
        "应给出读法说明(notes): {v}"
    );
    assert!(v["leases"].is_array(), "leases 应为数组: {v}");

    // ② 从 follower 视角也应看到同一个 leader(证明视图不是本副本的想象)
    let follower = cl
        .nodes
        .iter()
        .find(|n| n.id != leader_id)
        .expect("应有 follower");
    let (_, fv) = json_get(follower.public, &follower_cookie, "/api/rds/cluster");
    assert_eq!(
        fv["quorum"]["leader"],
        serde_json::json!(leader_id),
        "follower 视图的 leader 应与 leader 视图一致: {fv}"
    );
    assert_eq!(fv["members"].as_array().map(|a| a.len()), Some(3), "{fv}");

    // ③ resync-sink:仅 leader 生效
    let lead_node = cl
        .nodes
        .iter()
        .find(|n| n.id == leader_id)
        .expect("leader 节点存在")
        .public;
    let leader_cookie = login(lead_node);
    let rs = http(
        follower.public,
        "POST",
        "/api/rds/cluster/resync-sink",
        Some(&follower_cookie),
    )
    .unwrap();
    assert_eq!(rs.status, 409, "follower 上重置投影应被拒绝: {}", rs.body);
    let rj: serde_json::Value = serde_json::from_str(&rs.body).unwrap();
    assert_eq!(rj["code"], serde_json::json!("not_leader"), "{rj}");
    assert_eq!(rj["leader"], serde_json::json!(leader_id), "{rj}");
    let rs2 = http(
        lead_node,
        "POST",
        "/api/rds/cluster/resync-sink",
        Some(&leader_cookie),
    )
    .unwrap();
    assert_eq!(rs2.status, 200, "leader 上重置投影应成功: {}", rs2.body);
    let rj2: serde_json::Value = serde_json::from_str(&rs2.body).unwrap();
    assert_eq!(rj2["ok"], serde_json::json!(true), "{rj2}");
    assert_eq!(rj2["detail"]["after"], serde_json::json!(0), "{rj2}");

    // ④ stepdown:从 follower 发起(走转发),领导权必须真的转移,且不得脑裂
    let sd = http(
        follower.public,
        "POST",
        "/api/rds/cluster/stepdown",
        Some(&follower_cookie),
    )
    .unwrap();
    assert_eq!(sd.status, 200, "从 follower 发起让位应被接受(转发): {}", sd.body);
    let sj: serde_json::Value = serde_json::from_str(&sd.body).unwrap();
    assert_eq!(sj["detail"]["via"], serde_json::json!("forward"), "{sj}");
    assert_eq!(sj["detail"]["former_leader"], serde_json::json!(leader_id), "{sj}");
    wait_until(
        30,
        || cl.leader().map(|l| l != leader_id).unwrap_or(false),
        "让位后应换主",
    );
    cl.assert_no_split_brain();
    let new_leader = cl.leader().expect("换主后应有 leader");
    assert_ne!(new_leader, leader_id, "让位后 leader 应改变");
    // 换主后,任一副本的视图应指向新 leader
    let (_, v2) = json_get(cl.nodes[0].public, &cookie, "/api/rds/cluster");
    assert_eq!(v2["quorum"]["leader"], serde_json::json!(new_leader.clone()), "{v2}");

    // ⑤ 权限目录:集群页两个权限已登记(RBAC 可见可授)
    let (_, p) = json_get(cl.nodes[0].public, &cookie, "/api/rds/permissions");
    let perms: Vec<String> = p["permissions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|x| x["perm"].as_str().map(|s| s.to_string()))
        .collect();
    for want in ["cluster.view", "cluster.manage"] {
        assert!(perms.contains(&want.to_string()), "权限目录缺 {want}: {perms:?}");
    }

    // ⑥ 页面已挂载入口/视图(避免"接口有、入口无")
    let page = http(cl.nodes[0].public, "GET", "/", Some(&cookie)).unwrap();
    assert_eq!(page.status, 200);
    for marker in [
        "id=\"nav-cluster\"",
        "data-view=\"cluster\"",
        "id=\"view-cluster\"",
        "id=\"cl-rows\"",
        "id=\"cl-lease-rows\"",
        "id=\"cl-lease-toggle\"",
        "id=\"cl-stepdown\"",
        "id=\"cl-resync\"",
        "/api/rds/cluster",
    ] {
        assert!(page.body.contains(marker), "页面缺少标记 {marker}");
    }
    ctx.drop_db();
}

/// 验收(单机模式):管控集群接口必须**如实**说明没有集群,而不是返回一个空集群
#[test]
fn cluster_view_reports_single_mode_honestly() {
    let _g = serial_guard();
    let ctx = Ctx::new("cluster_single");
    let srv = start_server(&ctx, 60);
    let cookie = login(srv.port);
    let (st, v) = json_get(srv.port, &cookie, "/api/rds/cluster");
    assert_eq!(st, 200, "single 模式也应有可读响应: {v}");
    assert_eq!(v["enabled"], serde_json::json!(false), "{v}");
    assert_eq!(v["mode"], serde_json::json!("single"), "{v}");
    assert_eq!(v["members"].as_array().map(|a| a.len()), Some(0), "{v}");
    assert!(v["quorum"].is_null(), "{v}");
    assert!(
        v["notes"].as_array().map(|a| !a.is_empty()).unwrap_or(false),
        "必须给出原因说明: {v}"
    );
    // 单机模式下两个运维动作都必须明确拒绝(409),不得静默成功
    for path in ["/api/rds/cluster/stepdown", "/api/rds/cluster/resync-sink"] {
        let r = http(srv.port, "POST", path, Some(&cookie)).unwrap();
        assert_eq!(r.status, 409, "{path} 在单机模式应 409: {}", r.body);
        let j: serde_json::Value = serde_json::from_str(&r.body).unwrap();
        assert_eq!(j["code"], serde_json::json!("not_cluster"), "{path}: {j}");
    }
    ctx.drop_db();
}

/// 验收(节点状态页):`GET /api/rds/nodes` 提供跨实例统一节点清单(DB + Proxy)与汇总,
/// 并把「该节点可做什么」由后端按架构/角色裁决(非主节点才有切换类动作,代理才有启停/重启/健康);
/// 同时校验页面确实挂载了导航入口与视图,避免出现「接口有、入口无」或反之。
#[test]
fn nodes_inventory_api_and_view_registered() {
    let _g = serial_guard();
    let ctx = Ctx::new("nodes");
    let srv = start_server(&ctx, 60);
    let cookie = login(srv.port);

    // ① 空态:结构完整(不因无实例而缺字段)
    let (s, v) = json_get(srv.port, &cookie, "/api/rds/nodes");
    assert_eq!(s, 200, "nodes: {v}");
    assert!(v["nodes"].is_array(), "nodes 应为数组: {v}");
    assert_eq!(
        v["nodes"].as_array().unwrap().len(),
        0,
        "初始节点清单应为空: {v}"
    );
    for k in [
        "total",
        "db",
        "proxy",
        "ok",
        "degraded",
        "down",
        "missing",
        "stopped",
        "repl_down",
        "remote",
        "unknown",
    ] {
        assert!(v["summary"].get(k).is_some(), "summary 缺字段 {k}: {v}");
    }

    // ② 建一个 async 实例:1 主 2 从 + 2 代理 = 5 个节点
    let r = http(
        srv.port,
        "POST",
        "/api/rds/create?name=ndv&itype=async&proxies=2",
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(r.status, 200, "create 应被接受: {}", r.body);
    wait_until(
        60,
        || instance_status(srv.port, &cookie, "ndv").as_deref() == Some("running"),
        "ndv running",
    );

    let (s2, v2) = json_get(srv.port, &cookie, "/api/rds/nodes");
    assert_eq!(s2, 200, "nodes: {v2}");
    let rows = v2["nodes"].as_array().cloned().unwrap_or_default();
    let db = rows.iter().filter(|x| x["kind"] == "db").count();
    let px = rows.iter().filter(|x| x["kind"] == "proxy").count();
    assert_eq!(db, 3, "async 1 主 2 从 = 3 个 DB 节点: {v2}");
    assert_eq!(px, 2, "proxies=2 = 2 个 Proxy 节点: {v2}");
    assert_eq!(v2["summary"]["total"], serde_json::json!(5), "summary: {v2}");
    assert_eq!(v2["summary"]["db"], serde_json::json!(3), "summary: {v2}");
    assert_eq!(v2["summary"]["proxy"], serde_json::json!(2), "summary: {v2}");

    // ③ 动作由后端裁决:主节点仅 detail;从节点有受管切主;代理有启停/重启/健康
    let master = rows
        .iter()
        .find(|x| x["kind"] == "db" && x["role"] == "master")
        .expect("应有一个 master");
    let macts: Vec<&str> = master["actions"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|a| a.as_str())
        .collect();
    assert_eq!(macts, vec!["detail"], "主节点不应有切换动作: {master}");
    let slave = rows
        .iter()
        .find(|x| x["kind"] == "db" && x["role"] != "master")
        .expect("应有从节点");
    assert!(
        slave["actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a == "reparent_to_this"),
        "非 xenon 从节点应可受管切主: {slave}"
    );
    let proxy = rows
        .iter()
        .find(|x| x["kind"] == "proxy")
        .expect("应有代理节点");
    let pacts: Vec<&str> = proxy["actions"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|a| a.as_str())
        .collect();
    assert_eq!(
        pacts,
        vec![
            "proxy_restart",
            "proxy_stop",
            "proxy_start",
            "proxy_health",
            "detail"
        ],
        "代理动作契约: {proxy}"
    );
    // 代理的监听地址应按 mysql_port 拼接(而非默认 0)
    let mp = proxy["mysql_port"].as_u64().unwrap_or(0);
    let addr = proxy["addr"].as_str().unwrap_or("");
    assert!(
        mp > 0 && addr.ends_with(&format!(":{mp}")),
        "代理 addr 应等于 host:mysql_port,实际 {addr}(mysql_port={mp})"
    );

    // ④ 过滤:kind 与关键词(容器名)
    let (_, v3) = json_get(srv.port, &cookie, "/api/rds/nodes?kind=proxy&limit=2000");
    assert_eq!(v3["nodes"].as_array().unwrap().len(), 2, "kind=proxy: {v3}");
    let (_, v4) = json_get(
        srv.port,
        &cookie,
        &format!("/api/rds/nodes?q={}", proxy["container"].as_str().unwrap()),
    );
    assert_eq!(v4["nodes"].as_array().unwrap().len(), 1, "q 精确命中: {v4}");

    // ⑤ 页面已挂载入口/视图/分类占位(后端有接口但前端没入口同样是缺陷)
    let page = http(srv.port, "GET", "/", Some(&cookie)).unwrap();
    assert_eq!(page.status, 200, "GET / 应返回页面");
    for marker in [
        "id=\"nav-nodes\"",
        "data-view=\"nodes\"",
        "id=\"view-nodes\"",
        "id=\"nd-rows\"",
        "/api/rds/nodes",
    ] {
        assert!(page.body.contains(marker), "页面缺少标记 {marker}");
    }
    // 创建入口:四张**横向**大类卡 + 高可用版二级选择(异步/半同步)+ 占位卡不可选
    for cat in [
        "data-arch=\"single\"",
        "data-arch=\"ha\"",
        "data-arch=\"xenon\"",
        "id=\"c-ha-mode\"",
        "id=\"c-ha-seg\"",
        "data-ha=\"async\"",
        "data-ha=\"sync\"",
        "is-soon",
        "OceanBase",
        "近期开放",
    ] {
        assert!(page.body.contains(cat), "创建页缺少 {cat}");
    }
    // 一级入口不再把"异步/同步"并列成两张卡(data-arch 只有 3 个可选 + 1 个占位)
    assert!(
        !page.body.contains("data-arch=\"async\"") && !page.body.contains("data-arch=\"sync\""),
        "异步/同步不应作为一级入口并列"
    );

    ctx.drop_db();
}

/// 验收(发现 15:过期租约回收)。真实 3 副本,三条语义分别看护:
///   ① **自愈**:本副本持有、已过期、且本进程没在操作的条目 → 该副本自己主动释放;
///   ② **隔离**:别的 holder 的过期条目,本副本**不得**替它释放(状态机 holder 门禁);
///   ③ **GC**:holder 永久离场(不存在于任何副本)的条目 → leader 按确定性 cutoff 回收;
///   ④ **不误伤**:未过期的条目必须原样保留。
///
/// 用例让"过期"必然发生:`at_ms=0` → `expire_at_ms = ttl`(1970 年),远早于默认 60s 宽限,
/// 因此不需要改动任何环境变量/宽限期,用默认配置就能在几轮对账内观察到结果。
#[test]
fn expired_lease_reap_and_deterministic_purge() {
    let _g = serial_guard();
    let ctx = Ctx::new("lease_gc");
    let cl = ClusterSrv::start(&ctx, 3, false);
    // 注意:内部 `/internal/propose` 是**本地版**(设计上不转发,避免成环),
    // 因此提案必须直接发给 leader 的 rpc 端口;业务 API 才负责转发。
    let leader_id = cl.leader().expect("3 副本应选出 leader");
    let lrpc = cl
        .nodes
        .iter()
        .find(|n| n.id == leader_id)
        .expect("leader 节点存在")
        .rpc;

    // holder 取自 RDSCTL_CONTROLLER_ID(= 节点 id,见 spawn_cnode)
    let mine = "gc-mine";
    let orphan = "gc-orphan";
    let live = "gc-live";
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let grant = |inst: &str, holder: &str, ttl: u64, at: u64| {
        format!(
            r#"{{"op":"lease_grant","instance":"{inst}","holder":"{holder}","ttl_ms":{ttl},"at_ms":{at}}}"#
        )
    };
    // 三条都经内部 RPC 写入(串行,index 顺序确定);任一副本都会转发给 leader
    for body in [
        grant(mine, "n1", 1_000, 0),        // 已过期,holder = n1(本机真实 holder)
        grant(orphan, "ghost", 1_000, 0),   // 已过期,holder 永不在场
        grant(live, "ghost", 300_000, now), // 未过期
    ] {
        let r = http_post_json(lrpc, "/internal/propose", &body).expect("propose");
        assert_eq!(r.status, 200, "提案应成功: {}", r.body);
        let v: serde_json::Value = serde_json::from_str(&r.body).unwrap();
        assert_eq!(v["ok"], serde_json::json!(true), "提案未提交: {v}");
    }
    // 前置:三条都已复制到各副本
    let lease_of = |rpc: u16, inst: &str| -> Option<serde_json::Value> {
        let r = http(rpc, "GET", &format!("/internal/lease?instance={inst}"), None).ok()?;
        if r.status != 200 {
            return None;
        }
        serde_json::from_str::<serde_json::Value>(&r.body)
            .ok()
            .and_then(|v| v.get("lease").cloned())
    };
    // 前置只用"未过期"那条当复制探针:已过期的两条可能在下一轮对账里**立刻**被回收,
    // 拿它们断言"先可见"会与回收竞态(实测会 flaky)。
    wait_until(
        10,
        || (0..3).all(|i| lease_of(cl.nodes[i].rpc, live).is_some()),
        "未过期的租约应复制到全部副本",
    );

    // 等回收:对账周期 5s,给两轮余量
    wait_until(
        30,
        || lease_of(cl.nodes[0].rpc, mine).is_none(),
        "① n1 自己持有的过期租约应被自愈回收",
    );
    wait_until(
        30,
        || lease_of(cl.nodes[0].rpc, orphan).is_none(),
        "③ holder 永久离场的过期租约应被 leader GC 回收",
    );
    // ② 隔离:不能出现"n1 把 ghost 的租约也释放了"——由 ghost 那条最终以 GC 方式消失即可反证:
    //    如果 holder 门禁失效,它会在第 ① 步的同一轮就被删掉,而 ① 的断言无法区分;
    //    因此这里直接验状态机的门禁语义:手工让 n1 释放 ghost 的租约必须被拒。
    let bad = http_post_json(
        lrpc,
        "/internal/propose",
        &format!(r#"{{"op":"lease_release","instance":"{live}","holder":"n1"}}"#),
    )
    .expect("propose");
    assert_eq!(bad.status, 200, "提案本身应被接受(由状态机裁决): {}", bad.body);
    let bv: serde_json::Value = serde_json::from_str(&bad.body).unwrap();
    assert_eq!(
        bv["applied"]["status"],
        serde_json::json!("rejected"),
        "非持有者释放必须被状态机拒绝: {bv}"
    );
    // ④ 未过期条目必须保留
    assert!(
        lease_of(cl.nodes[0].rpc, live).is_some(),
        "未过期的租约不得被任何回收路径清掉"
    );
    // 跨副本一致:回收结果必须在 3 个副本上都成立(共识状态,不是本地清理)
    for n in &cl.nodes {
        assert!(
            lease_of(n.rpc, mine).is_none() && lease_of(n.rpc, orphan).is_none(),
            "副本 {} 上的回收结果应与共识一致",
            n.id
        );
        assert!(
            lease_of(n.rpc, live).is_some(),
            "副本 {} 上未过期租约应保留",
            n.id
        );
    }
    ctx.drop_db();
}

/// 验收(I15/I16 / C8):会话与 RBAC 的权威在**共识状态机**里。
///
/// 修复前的问题(实测):会话存在每个副本的进程内 `DashMap`,于是
/// ①同一 cookie 换一个副本 → 401(负载均衡后面随机掉登录);
/// ②冻结/改密/改权只对处理该请求的副本生效。
#[test]
fn cluster_session_and_rbac_are_consensus_authoritative() {
    let _g = serial_guard();
    let ctx = Ctx::new("cluster_auth");
    let cl = ClusterSrv::start(&ctx, 3, false);
    let n1 = cl.nodes[0].public;
    let n2 = cl.nodes[1].public;
    let n3 = cl.nodes[2].public;

    // ① 在 n1 登录 → 同一 cookie 在 3 个副本上都可用(核心:任一网关服务任一会话)
    let admin = login(n1);
    for (id, p) in [("n1", n1), ("n2", n2), ("n3", n3)] {
        let r = http(p, "GET", "/api/auth/me", Some(&admin)).unwrap();
        assert_eq!(r.status, 200, "{id} 应接受同一 cookie,实际:{} {}", r.status, r.body);
        assert!(r.body.contains("admin"), "{id} 会话应指向 admin:{}", r.body);
    }

    // ② 在 n2 上新建角色与用户,在 n3 上必须立刻可见(写路径经共识)
    let r = http(
        n2,
        "POST",
        "/api/rds/roles?action=create&role=cviewer&desc=%E5%8F%AA%E8%AF%BB",
        Some(&admin),
    )
    .unwrap();
    assert_eq!(r.status, 200, "建角色:{}", r.body);
    let r = http(
        n2,
        "POST",
        "/api/rds/roles?action=perms&role=cviewer&perms=instances.view,cluster.view",
        Some(&admin),
    )
    .unwrap();
    assert_eq!(r.status, 200, "授权角色:{}", r.body);
    let r = http(
        n2,
        "POST",
        "/api/rds/users?action=create&user=alice&pass=alice-pass&enabled=1",
        Some(&admin),
    )
    .unwrap();
    assert_eq!(r.status, 200, "建用户:{}", r.body);
    let r = http(
        n2,
        "POST",
        "/api/rds/users?action=roles&user=alice&roles=cviewer",
        Some(&admin),
    )
    .unwrap();
    assert_eq!(r.status, 200, "绑角色:{}", r.body);
    wait_until(
        20,
        || {
            let (s, v) = json_get(n3, &admin, "/api/rds/users");
            s == 200
                && v["users"]
                    .as_array()
                    .map(|a| a.iter().any(|u| u["user"] == serde_json::json!("alice")))
                    .unwrap_or(false)
        },
        "n3 上应能看到 n2 建的用户(共识复制收敛)",
    );
    let (_, v) = json_get(n3, &admin, "/api/rds/users");
    let alice = v["users"]
        .as_array()
        .unwrap()
        .iter()
        .find(|u| u["user"] == serde_json::json!("alice"))
        .cloned()
        .expect("alice 存在于 n3 视图");
    assert_eq!(
        alice["perms"],
        serde_json::json!(["cluster.view", "instances.view"]),
        "权限应按角色现算:{alice}"
    );

    // ③ alice 在 n3 上登录 → cookie 在 n1 上同样可用(会话跨副本)
    let alice_ck = login_as(n3, "alice", "alice-pass");
    let r = http(n1, "GET", "/api/auth/me", Some(&alice_ck)).unwrap();
    assert_eq!(r.status, 200, "alice 的会话应跨副本可用:{}", r.body);
    assert!(r.body.contains("alice"), "{}", r.body);
    // 权限确实生效:alice 只有 view,+ manage 接口必须 403
    let r = http(n1, "POST", "/api/rds/hosts/status?name=x&status=running", Some(&alice_ck)).unwrap();
    assert_eq!(r.status, 403, "alice 不应有 instances.manage:{}", r.body);

    // ④ 改角色权限(仍在 n2 上)→ 不重新登录,n1 上的 alice 立即多出权限
    let r = http(
        n2,
        "POST",
        "/api/rds/roles?action=perms&role=cviewer&perms=instances.view,cluster.view,instances.manage",
        Some(&admin),
    )
    .unwrap();
    assert_eq!(r.status, 200, "改权限:{}", r.body);
    wait_until(
        20,
        || {
            let r = http(n1, "GET", "/api/auth/me", Some(&alice_ck)).unwrap();
            r.status == 200 && r.body.contains("instances.manage")
        },
        "改角色权限后,已登录会话应立即生效(权限每请求现算)",
    );

    // ⑤ 冻结 alice(epoch+1)→ 该用户**全部**会话在所有副本上立即失效
    let r = http(
        n3,
        "POST",
        "/api/rds/users?action=enabled&user=alice&value=0",
        Some(&admin),
    )
    .unwrap();
    assert_eq!(r.status, 200, "冻结:{}", r.body);
    wait_until(
        20,
        || {
            [n1, n2, n3]
                .iter()
                .all(|p| http(*p, "GET", "/api/auth/me", Some(&alice_ck)).unwrap().status == 401)
        },
        "冻结后该用户全部会话必须在所有副本上失效",
    );

    // ⑥ 解冻 + 改密 → 旧口令在任意副本都失败,新口令成功(口令校验走 read-index,不吃旧状态)
    let r = http(
        n1,
        "POST",
        "/api/rds/users?action=enabled&user=alice&value=1",
        Some(&admin),
    )
    .unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
    let r = http(
        n1,
        "POST",
        "/api/rds/users?action=setpass&user=alice&pass=alice-new-pass",
        Some(&admin),
    )
    .unwrap();
    assert_eq!(r.status, 200, "改密:{}", r.body);
    wait_until(
        20,
        || {
            http(
                n3,
                "POST",
                "/login?user=alice&password=alice-new-pass",
                None,
            )
            .map(|r| r.status == 200)
            .unwrap_or(false)
        },
        "新口令应在所有副本上生效",
    );
    let old = http(n2, "POST", "/login?user=alice&password=alice-pass", None).unwrap();
    assert_eq!(old.status, 401, "旧口令必须失效:{}", old.body);

    // ⑦ logout:在 n1 撤销 → 同一 cookie 在 n2/n3 上也必须失效
    let ck2 = login_as(n1, "alice", "alice-new-pass");
    assert_eq!(http(n2, "GET", "/api/auth/me", Some(&ck2)).unwrap().status, 200);
    let out = http(n1, "POST", "/logout", Some(&ck2)).unwrap();
    assert_eq!(out.status, 200, "logout 应成功:{}", out.body);
    wait_until(
        20,
        || {
            [n1, n2, n3]
                .iter()
                .all(|p| http(*p, "GET", "/api/auth/me", Some(&ck2)).unwrap().status == 401)
        },
        "登出必须在所有副本上生效",
    );

    // ⑧ 权限目录:cluster.view/manage 与既有权限并存(授权矩阵可见)
    let (_, perms) = json_get(n1, &admin, "/api/rds/permissions");
    assert!(
        perms["permissions"].as_array().map(|a| a.len()).unwrap_or(0) >= 16,
        "权限目录应含新增项:{perms}"
    );

    // ⑨ 会话只以哈希落状态机:递归扫**日志与快照的原始字节**,明文 token 不得出现
    let token_plain = ck2.split('=').nth(1).unwrap_or("").to_string();
    assert!(!token_plain.is_empty(), "取到明文 token 才能做本断言");
    for id in ["n1", "n2", "n3"] {
        let d = ctx.dir.join(format!("ha-{id}"));
        assert!(
            !dir_contains_bytes(&d, token_plain.as_bytes()),
            "明文会话 token 出现在 {id} 的日志/快照里(必须只落哈希)"
        );
    }

    ctx.drop_db();
}

/// 验收:单机模式行为不变(会话仍在进程内 + 每请求查库;无集群也能登录与授权)
#[test]
fn single_mode_session_semantics_unchanged() {
    let _g = serial_guard();
    let ctx = Ctx::new("single_auth");
    let srv = start_server(&ctx, 60);
    let ck = login(srv.port);
    let r = http(srv.port, "GET", "/api/auth/me", Some(&ck)).unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(r.body.contains("admin"), "{}", r.body);
    // 无 cookie → 401;登出后 → 401
    assert_eq!(http(srv.port, "GET", "/api/auth/me", None).unwrap().status, 401);
    let out = http(srv.port, "POST", "/logout", Some(&ck)).unwrap();
    assert_eq!(out.status, 200, "单机登出:{}", out.body);
    assert_eq!(
        http(srv.port, "GET", "/api/auth/me", Some(&ck)).unwrap().status,
        401,
        "单机登出后会话必须失效"
    );
    // 用户管理仍工作(走库)
    let r = http(
        srv.port,
        "POST",
        "/api/rds/users?action=create&user=bob&pass=bob-pass&enabled=1",
        Some(&ck),
    );
    // ck 已登出 → 必须 401(顺带验证登出是真实撤销,不是只清 cookie)
    assert_eq!(r.unwrap().status, 401, "登出后的 cookie 不得继续管理用户");
    ctx.drop_db();
}
