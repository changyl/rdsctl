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
            let out = Command::new("which").arg("mysql").output().expect("which mysql");
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        })
}

fn mysql_admin(sql: &str) -> Result<String, String> {
    let mut cmd = Command::new(mysql_cli());
    cmd.args(["-h", "127.0.0.1", "-P", "3306", "-u", "root", "--batch", "--raw", "--skip-column-names", "-e", sql]);
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
        let uniq = format!(
            "rdsctl_acc_{}_{}",
            sanitize(tag),
            std::process::id()
        );
        let dir = std::env::temp_dir().join(&uniq);
        let fake_bin = dir.join("bin");
        let state = dir.join("state");
        let ctrl = dir.join("ctrl");
        let log = dir.join("docker.log");
        std::fs::create_dir_all(&fake_bin).unwrap();
        std::fs::create_dir_all(&state).unwrap();
        std::fs::create_dir_all(&ctrl).unwrap();
        let mysql_real = mysql_cli();
        // 独立数据库
        mysql_admin(&format!("CREATE DATABASE IF NOT EXISTS `{uniq}` CHARACTER SET utf8mb4"))
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
        cmd.env("RDSCTL_MYSQL_HOST", std::env::var("RDSCTL_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into()));
        cmd.env("RDSCTL_MYSQL_PORT", std::env::var("RDSCTL_MYSQL_PORT").unwrap_or_else(|_| "3306".into()));
        cmd.env("RDSCTL_MYSQL_USER", std::env::var("RDSCTL_MYSQL_USER").unwrap_or_else(|_| "root".into()));
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
        self.docker_log_lines().iter().filter(|l| l.contains(&target)).count()
    }

    fn drop_db(&self) {
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
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .ok();
    let cookie_hdr = cookie.map(|c| format!("Cookie: {c}\r\n")).unwrap_or_default();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n{cookie_hdr}\r\n"
    );
    use std::io::Write;
    stream.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
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
            l.strip_prefix("set-cookie:")?.split(';').next().map(|s| s.trim().to_string())
        })
        .expect("无 session cookie")
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
    let r = http(port, "POST", &format!("/api/rds/create?name={name}"), Some(cookie)).unwrap();
    assert_eq!(r.status, 200, "create 失败: {}", r.body);
    serde_json::from_str::<serde_json::Value>(&r.body).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string()
}

fn submit_destroy(port: u16, cookie: &str, name: &str) -> (u16, String) {
    let r = http(port, "POST", &format!("/api/rds/destroy?name={name}"), Some(cookie)).unwrap();
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
    wait_until(15, || task_status(srv.port, &cookie, &tid).as_deref() == Some("running"), "任务进入 running");
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
    assert_eq!(tv["task"]["status"].as_str(), Some("failed"), "中断任务应为 failed");
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
    assert!(b1.contains("可销毁") || b1.contains("不允许"), "错误应说明原因: {b1}");
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
            acts.iter().any(|(a, p)| a == "degrade" && p.contains("容器缺失")),
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
                        && m["latest_snapshot"]["containers"].as_array().map_or(false, |cs| {
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
    assert!(has_missing, "insights 应含「节点容器缺失」群且带快照摘要: {iv}");
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
            acts.iter().any(|(a, p)| a == "degrade" && p.contains("复制中断")),
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
    assert_eq!(
        cnt3.trim(),
        "2",
        "同问题重复巡检不得新增快照,got: {cnt3}"
    );
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
    assert_eq!(iv["instance"]["shard_num"], serde_json::json!(2), "shard_num=2");
    let nodes = iv["instance"]["nodes"].as_array().cloned().unwrap_or_default();
    assert_eq!(nodes.len(), 6, "async 2 分片 × (1 主 + 读从 + 离线从)= 6 节点: {iv}");
    let masters: Vec<&serde_json::Value> = nodes
        .iter()
        .filter(|x| x["role"] == "master")
        .collect();
    let reads: Vec<&serde_json::Value> = nodes.iter().filter(|x| x["role"] == "read").collect();
    let offs: Vec<&serde_json::Value> = nodes.iter().filter(|x| x["role"] == "offline").collect();
    assert_eq!(masters.len(), 2, "应有 2 个 master(每分片一个)");
    assert_eq!(reads.len(), 2, "应有 2 个读从(每分片一个): {iv}");
    assert_eq!(offs.len(), 2, "async 多分片每分片应有 1 个离线从,共 2 个: {iv}");
    // 每分片独立容器命名与 shard 标注
    let shard_ids: std::collections::HashSet<String> = nodes
        .iter()
        .map(|x| x["shard"].as_str().unwrap_or("").to_string())
        .collect();
    assert!(shard_ids.contains("s1") && shard_ids.contains("s2"), "节点 shard 应含 s1/s2: {iv}");
    // shards 元数据:2 行、master 互不相同、slaves 完整(读从+离线从)
    let shards = iv["instance"]["shards"].as_array().cloned().unwrap_or_default();
    assert_eq!(shards.len(), 2);
    let sm0 = shards[0]["master"].as_str().unwrap_or("").to_string();
    let sm1 = shards[1]["master"].as_str().unwrap_or("").to_string();
    assert_ne!(sm0, sm1, "两个分片 master 容器必须不同");
    assert!(sm0.ends_with("-s1-master") && sm1.ends_with("-s2-master"), "分片命名: {sm0} / {sm1}");
    for sd in shards.iter() {
        let sl = sd["slaves"].as_array().cloned().unwrap_or_default();
        assert_eq!(sl.len(), 2, "每分片 slaves 应含 读从+离线从 共 2 项: {iv}");
        let roles: Vec<String> = sl.iter().map(|x| x["role"].as_str().unwrap_or("").to_string()).collect();
        assert!(roles.contains(&"read".to_string()) && roles.contains(&"offline".to_string()), "slaves 角色: {roles:?}");
    }
    // 容器真实被启动(run 记录:每分片 master + slave-1(读) + slave-2(离线))
    for c in [
        "rds-msh-s1-master", "rds-msh-s1-slave-1", "rds-msh-s1-slave-2",
        "rds-msh-s2-master", "rds-msh-s2-slave-1", "rds-msh-s2-slave-2",
    ] {
        assert!(ctx.run_count("RUN", c) >= 1, "容器 {c} 应被 docker run 创建");
    }
    // LVS 接入层:创建时登记 VIP(lvs_mysql_port>0),接入层为进程内转发器(不依赖镜像)
    assert_eq!(iv["instance"]["lvs_container"], serde_json::json!("rds-msh-lvs"), "LVS 接入层应被登记: {iv}");
    assert!(iv["instance"]["lvs_mysql_port"].as_u64().unwrap_or(0) > 0, "应分配 LVS 接入端口: {iv}");
    let lvs0 = iv["instance"]["lvs"][0].as_str().unwrap_or("").to_string();
    assert!(lvs0.starts_with("127.0.0.1:"), "VIP 展示地址应为 127.0.0.1:端口,实际: {lvs0}");
    // 多分片实例:实例级扩容(未指定分片)应被明确拒绝
    let ro = post_retry_busy(srv.port, &cookie, "/api/rds/scaleout?name=msh&role=read");
    assert_eq!(ro.0, 400, "多分片实例未指定分片扩容应被拒: {}", ro.1);
    assert!(
        ro.1.contains("分片"),
        "拒绝原因应提及分片: {}",
        ro.1
    );
    // ── 分片级扩容:async 实例 s1 新增读从(slave-3) ──
    let rs = post_retry_busy(srv.port, &cookie, "/api/rds/scaleout?name=msh&shard=s1&role=read");
    assert_eq!(rs.0, 200, "分片级扩容读从应被接受: {}", rs.1);
    let tid_s = serde_json::from_str::<serde_json::Value>(&rs.1).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_until(40, || task_status(srv.port, &cookie, &tid_s).as_deref() == Some("success"), "s1 扩容任务 success");
    wait_until(40, || instance_status(srv.port, &cookie, "msh").as_deref() == Some("running"), "扩容后回 running");
    let (_, ivS) = json_get(srv.port, &cookie, "/api/rds/instance?name=msh");
    let nodesS = ivS["instance"]["nodes"].as_array().cloned().unwrap_or_default();
    assert_eq!(nodesS.len(), 7, "扩容后 async 2 分片应为 7 节点: {ivS}");
    let ns1: Vec<&serde_json::Value> = nodesS
        .iter()
        .filter(|x| x["shard"] == "s1")
        .collect();
    let reads1 = ns1.iter().filter(|x| x["role"] == "read").count();
    assert_eq!(reads1, 2, "s1 应有 2 个读从: {ivS}");
    let slave3 = ns1.iter().find(|x| x["container"] == "rds-msh-s1-slave-3").expect("s1-slave-3 应存在");
    assert_eq!(slave3["role"], "read");
    assert!(ctx.run_count("RUN", "rds-msh-s1-slave-3") >= 1, "s1-slave-3 应被 docker run 创建");
    let shS = ivS["instance"]["shards"].as_array().cloned().unwrap_or_default();
    assert_eq!(shS[0]["slaves"].as_array().map(|a| a.len()).unwrap_or(0), 3, "s1 分片 slaves 元数据应含 3 项: {ivS}");
    // s1 已有离线从 → 再次扩容离线从被拒
    let ro2 = post_retry_busy(srv.port, &cookie, "/api/rds/scaleout?name=msh&shard=s1&role=offline");
    assert_eq!(ro2.0, 400, "分片已有离线从,再扩应被拒: {}", ro2.1);
    assert!(ro2.1.contains("离线"), "拒绝原因: {}", ro2.1);
    // 不存在的分片应被拒
    let ro3 = post_retry_busy(srv.port, &cookie, "/api/rds/scaleout?name=msh&shard=s9&role=read");
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
    wait_until(30, || task_status(srv.port, &cookie, &tid2).as_deref() == Some("success"), "ms2 任务 success");
    let (_, iv2) = json_get(srv.port, &cookie, "/api/rds/instance?name=ms2");
    let nodes2 = iv2["instance"]["nodes"].as_array().cloned().unwrap_or_default();
    assert_eq!(nodes2.len(), 4, "sync 2 分片 × (1 主 + 1 读从)= 4 节点: {iv2}");
    let offs2: Vec<&serde_json::Value> = nodes2.iter().filter(|x| x["role"] == "offline").collect();
    assert!(offs2.is_empty(), "sync 多分片不应有离线从: {iv2}");
    for c in ["rds-ms2-s1-slave-1", "rds-ms2-s2-slave-1"] {
        assert!(ctx.run_count("RUN", c) >= 1, "sync 容器 {c} 应被 docker run 创建");
    }
    assert_eq!(iv2["instance"]["lvs_container"], serde_json::json!("rds-ms2-lvs"), "sync 实例也应有 LVS 接入层: {iv2}");
    // sync 分片无离线从 → s1 分片级扩容离线从(slave-2)应成功
    let rs2 = post_retry_busy(srv.port, &cookie, "/api/rds/scaleout?name=ms2&shard=s1&role=offline");
    assert_eq!(rs2.0, 200, "sync 分片级扩容离线从应被接受: {}", rs2.1);
    let tid_s2 = serde_json::from_str::<serde_json::Value>(&rs2.1).unwrap()["task_id"]
        .as_str()
        .unwrap()
        .to_string();
    wait_until(40, || task_status(srv.port, &cookie, &tid_s2).as_deref() == Some("success"), "ms2 s1 离线扩容 success");
    let (_, iv2b) = json_get(srv.port, &cookie, "/api/rds/instance?name=ms2");
    let nodes2b = iv2b["instance"]["nodes"].as_array().cloned().unwrap_or_default();
    assert_eq!(nodes2b.len(), 5, "ms2 扩容离线从后应为 5 节点: {iv2b}");
    assert!(ctx.run_count("RUN", "rds-ms2-s1-slave-2") >= 1, "ms2 s1 离线从应被 docker run 创建");
    let sh2 = iv2b["instance"]["shards"].as_array().cloned().unwrap_or_default();
    assert_eq!(sh2[0]["slaves"].as_array().map(|a| a.len()).unwrap_or(0), 2, "ms2 s1 slaves 元数据应为 2 项: {iv2b}");
    let (ds2, db2) = submit_destroy(srv.port, &cookie, "ms2");
    assert_eq!(ds2, 200, "destroy ms2: {db2}");
    wait_until(40, || instance_status(srv.port, &cookie, "ms2").as_deref() == Some("destroyed"), "ms2 销毁完成");
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
            l.strip_prefix("set-cookie:")?.split(';').next().map(|s| s.trim().to_string())
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
    wait_until(40, || instance_status(srv.port, &cookie, "q1").as_deref() == Some("running"), "q1 running");
    wait_until(15, || task_status(srv.port, &cookie, &tid).as_deref() == Some("success"), "q1 创建任务成功");

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
    assert_eq!(post_ok(srv.port, &cookie, "/api/rds/roles?action=create&role=qro&desc=query-ro").0, 200);
    assert_eq!(
        post_ok(srv.port, &cookie, "/api/rds/roles?action=perms&role=qro&perms=instances.query").0,
        200
    );
    assert_eq!(post_ok(srv.port, &cookie, "/api/rds/users?action=create&user=qviewer&pass=vvvv&enabled=1").0, 200);
    assert_eq!(post_ok(srv.port, &cookie, "/api/rds/users?action=roles&user=qviewer&roles=qro").0, 200);
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
    assert_eq!(post_ok(srv.port, &cookie, "/api/rds/users?action=create&user=qwrite&pass=wwww&enabled=1").0, 200);
    assert_eq!(post_ok(srv.port, &cookie, "/api/rds/users?action=roles&user=qwrite&roles=qro").0, 200);
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
    let dA = items.iter().find(|i| i["digest"] == "dA").expect("应有 dA 聚合");
    assert_eq!(dA["total_count"], 30);
    assert_eq!(dA["total_ms"], 14000);
    assert_eq!(dA["instance_count"], 2);
    assert!(items.iter().all(|i| i["digest"] != "dB"), "旧样本不应出现在 24h 窗口");
    // digest_text 需 instances.query(admin=super 可见原文)
    assert!(dA["digest_text"].as_str().unwrap_or("").contains("select x from t"));

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
    std::fs::write(&script, format!(
        r#"#!/bin/bash
EV="$1"; KEY="$2"
echo "EVENT=$EV KEY=$KEY" >> {}
while IFS= read -r line; do echo "PAY=$line" >> {}; done
exit 0
"#,
        rec.display(),
        rec.display()
    ))
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
    wait_until(40, || instance_status(srv.port, &cookie, "b1").as_deref() == Some("running"), "b1 running");
    wait_until(15, || task_status(srv.port, &cookie, &tid).as_deref() == Some("success"), "b1 创建成功");

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
            .map(|a| a.iter().any(|x| x["node"] == "master" && x["reg_state"] == "registered"))
            .unwrap_or(false),
        "master 应 registered: {br}"
    );

    // destroy → deregister_instance
    let (ds, db) = submit_destroy(srv.port, &cookie, "b1");
    assert_eq!(ds, 200, "destroy: {db}");
    wait_until(40, || instance_status(srv.port, &cookie, "b1").as_deref() == Some("destroyed"), "b1 destroyed");
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
        &format!("/api/rds/module?name={}&category=cleanup&desc={}&steps={}", enc("清日志"), enc("删除旧日志"), steps),
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(mk.status, 200, "新建模块: {}", mk.body);
    // 2) 列表命中
    let (_, ls) = json_get(srv.port, &cookie, "/api/rds/modules");
    let mods = ls["modules"].as_array().cloned().unwrap_or_default();
    assert!(mods.iter().any(|m| m["name"] == "清日志"), "列表应含模块: {ls}");
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
        &format!("/api/rds/module?name={}&category=maintain&desc={}&steps={}", enc("清日志"), enc("v2 描述"), steps),
        Some(&cookie),
    )
    .unwrap();
    assert_eq!(up.status, 200, "更新模块: {}", up.body);
    let (_, ls2) = json_get(srv.port, &cookie, "/api/rds/modules");
    let mods2 = ls2["modules"].as_array().cloned().unwrap_or_default();
    let hit = mods2.iter().find(|m| m["name"] == "清日志").expect("仍有模块");
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
    assert!(ls3["modules"].as_array().map(|a| a.is_empty()).unwrap_or(false), "删除后列表为空: {ls3}");

    let (ds, db) = submit_destroy(srv.port, &cookie, "mo");
    assert_eq!(ds, 200, "destroy mo: {db}");
    wait_until(40, || instance_status(srv.port, &cookie, "mo").as_deref() == Some("destroyed"), "mo 销毁完成");
    ctx.drop_db();
}
