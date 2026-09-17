// rdsctl — 平台无关容器规格(ContainerSpec)+ docker CLI 参数解析器
//
// 为什么需要解析器:历史任务在 MySQL `task_nodes` 里存的是 `Step::DockerRun { args: [...] }`
// 的**原始 docker CLI 参数**(自定义功能模块可由用户任意填写)。直接改成结构化字段会破坏
// 向后兼容与重启续跑,因此:
//   - `docker_args_to_spec()` 把原始 args 解析成平台无关规格;
//   - `ContainerSpec::legacy_args` 保留原文,docker driver 逐字回放(零行为变化);
//   - 无法识别的 flag 收进 `spec.extra` —— docker driver 照旧透传,**非 docker 驱动必须
//     经 `check_caps` fail-closed 报错**(不会静默落到本机 docker)。

use serde::{Deserialize, Serialize};

use super::RuntimeError;

/// 平台无关的容器规格。
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ContainerSpec {
    /// 工作负载标识(今天是容器名,k8s=Pod 名)
    pub name: String,
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<PortMapping>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<Mount>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub envs: Vec<EnvVar>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>,
    /// 无法识别的 docker flag 原文(仅 docker driver 可透传;其它驱动 fail-closed)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra: Vec<String>,
    /// 历史 docker CLI 参数原文:`docker run -d --name <name>` 之后的那段 args。
    /// docker driver 优先逐字回放它,保证历史任务行为不变。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub legacy_args: Vec<String>,
}

/// 端口映射(`-p [host_ip:]host_port:container_port[/proto]`)。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PortMapping {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,
    /// None = 由平台动态分配(docker 的 `-p 3306` 形式)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_port: Option<u16>,
    pub container_port: u16,
    #[serde(default = "default_proto")]
    pub proto: String,
}

fn default_proto() -> String {
    "tcp".to_string()
}

impl PortMapping {
    /// 渲染回 docker `-p` 取值(仅 docker driver 使用)。
    pub fn to_cli(&self) -> String {
        let core = match (&self.host_ip, self.host_port) {
            (Some(ip), Some(hp)) => format!("{ip}:{hp}:{}", self.container_port),
            (None, Some(hp)) => format!("{hp}:{}", self.container_port),
            _ => self.container_port.to_string(),
        };
        if self.proto == "tcp" || self.proto.is_empty() {
            core
        } else {
            format!("{core}/{}", self.proto)
        }
    }
}

/// 挂载类型:宿主路径 vs 命名卷。跨平台能力差异(命名卷是 docker 概念)由 caps 表达。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MountKind {
    /// 宿主文件系统路径 bind mount
    HostPath,
    /// 引擎管理的命名卷(docker volume)
    #[default]
    NamedVolume,
}

/// 挂载(`-v source:target[:ro|rw]`)。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Mount {
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub kind: MountKind,
    #[serde(default)]
    pub read_only: bool,
}

impl Mount {
    /// 渲染回 docker `-v` 取值(仅 docker driver 使用)。
    pub fn to_cli(&self) -> String {
        let mut s = format!("{}:{}", self.source, self.target);
        if self.read_only {
            s.push_str(":ro");
        }
        s
    }
}

/// 环境变量(`-e K=V`)。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EnvVar {
    pub key: String,
    #[serde(default)]
    pub value: String,
}

impl EnvVar {
    pub fn to_cli(&self) -> String {
        format!("{}={}", self.key, self.value)
    }
}

/// 解析 `docker run -d --name <name> <args...>` 中 `<name>` 与 `<args>`。
///
/// 语义与 docker 一致:遇到第一个非 `-` 开头的 argv 即视为镜像,其后全部是容器命令。
/// 无法识别的 flag 收进 `spec.extra` 并继续(不在此处失败 —— 只有无法透传 `extra` 的
/// 驱动才 fail-closed,见 `super::check_caps`)。
pub fn docker_args_to_spec(name: &str, args: &[&str]) -> Result<ContainerSpec, RuntimeError> {
    let mut spec = ContainerSpec {
        name: name.to_string(),
        legacy_args: args.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    };
    let mut i = 0usize;
    while i < args.len() {
        let a = args[i];
        if a == "--" {
            i += 1;
            break;
        }
        if !a.starts_with('-') {
            break;
        }
        let (flag, inline) = match a.split_once('=') {
            Some((f, v)) => (f, Some(v)),
            None => (a, None),
        };
        // 已知 flag 的取值:优先 `--flag=value`,否则消费下一个 argv
        let known = matches!(
            flag,
            "--network"
                | "--net"
                | "--hostname"
                | "-h"
                | "-p"
                | "--publish"
                | "-v"
                | "--volume"
                | "-e"
                | "--env"
                | "--restart"
                | "--entrypoint"
                | "--name"
        );
        let value: Option<String> = if known {
            match inline {
                Some(v) => Some(v.to_string()),
                None => {
                    i += 1;
                    Some(
                        args.get(i)
                            .ok_or_else(|| {
                                RuntimeError::Platform(format!("docker 参数 {flag} 缺少取值"))
                            })?
                            .to_string(),
                    )
                }
            }
        } else {
            None
        };
        match flag {
            "--network" | "--net" => spec.network = value,
            "--hostname" | "-h" => spec.hostname = value,
            "-p" | "--publish" => {
                let v = value.unwrap_or_default();
                spec.ports.push(parse_port(&v)?);
            }
            "-v" | "--volume" => {
                let v = value.unwrap_or_default();
                spec.mounts.push(parse_mount(&v)?);
            }
            "-e" | "--env" => {
                let v = value.unwrap_or_default();
                spec.envs.push(parse_env(&v));
            }
            "--restart" => spec.restart_policy = value,
            "--entrypoint" => spec.entrypoint = value,
            "--name" => {
                if let Some(v) = value {
                    spec.name = v;
                }
            }
            // 未知 flag:原文收进 extra,**不吞下一个 argv**(未知 flag 多为无取值开关,
            // 吞掉会把镜像误判为其取值)。带 `=value` 的原样保留。
            // 代价:`--ulimit 65535` 这类「未知且带取值」的写法只能保住 flag 本身;
            // 对 docker 无影响(legacy_args 逐字回放),对其它驱动 extra 非空即 fail-closed。
            other => {
                let mut tok = other.to_string();
                if let Some(v) = inline {
                    tok.push('=');
                    tok.push_str(v);
                }
                spec.extra.push(tok);
            }
        }
        i += 1;
    }
    if i < args.len() {
        spec.image = args[i].to_string();
        i += 1;
    }
    spec.command = args[i..].iter().map(|s| s.to_string()).collect();
    if spec.image.is_empty() {
        return Err(RuntimeError::Platform(format!(
            "docker 参数缺少镜像: {}",
            args.join(" ")
        )));
    }
    Ok(spec)
}

fn parse_u16(s: &str, what: &str) -> Result<u16, RuntimeError> {
    s.trim()
        .parse::<u16>()
        .map_err(|_| RuntimeError::Platform(format!("无法解析{what}端口号: {s}")))
}

/// 解析 `-p` 取值。
fn parse_port(s: &str) -> Result<PortMapping, RuntimeError> {
    let (core, proto) = match s.split_once('/') {
        Some((a, p)) => (a, p.to_string()),
        None => (s, "tcp".to_string()),
    };
    let parts: Vec<&str> = core.split(':').collect();
    let (host_ip, host_port, container_port) = match parts.as_slice() {
        [c] => (None, None, parse_u16(c, "容器")?),
        [h, c] => {
            let hp = if h.trim().is_empty() {
                None
            } else {
                Some(parse_u16(h, "宿主")?)
            };
            (None, hp, parse_u16(c, "容器")?)
        }
        [ip, h, c] => (Some(ip.to_string()), Some(parse_u16(h, "宿主")?), parse_u16(c, "容器")?),
        _ => {
            return Err(RuntimeError::Platform(format!(
                "无法解析端口映射 {s}(期望 [host_ip:]host_port:container_port[/proto])"
            )))
        }
    };
    Ok(PortMapping {
        host_ip,
        host_port,
        container_port,
        proto,
    })
}

/// 解析 `-v` 取值。命名卷与宿主路径靠 source 形态区分(以 `/` 或 `.` 开头 = 宿主路径)。
fn parse_mount(s: &str) -> Result<Mount, RuntimeError> {
    let (rest, read_only) = match s.rsplit_once(':') {
        Some((r, "ro")) => (r, true),
        Some((r, "rw")) => (r, false),
        _ => (s, false),
    };
    let (source, target) = rest.split_once(':').ok_or_else(|| {
        RuntimeError::Platform(format!("无法解析挂载 {s}(期望 source:target[:ro|rw])"))
    })?;
    let kind = if source.starts_with('/') || source.starts_with('.') || source.starts_with('~') {
        MountKind::HostPath
    } else {
        MountKind::NamedVolume
    };
    Ok(Mount {
        source: source.to_string(),
        target: target.to_string(),
        kind,
        read_only,
    })
}

fn parse_env(s: &str) -> EnvVar {
    match s.split_once('=') {
        Some((k, v)) => EnvVar {
            key: k.to_string(),
            value: v.to_string(),
        },
        None => EnvVar {
            key: s.to_string(),
            value: String::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs(args: &[&str]) -> ContainerSpec {
        docker_args_to_spec("c1", args).expect("应可解析")
    }

    #[test]
    fn parses_mysql_node_args() {
        // 对齐 instance.rs::mysql_args 的真实形态
        let s = specs(&[
            "--network",
            "rds-n1",
            "-p",
            "127.0.0.1:35001:3306",
            "-e",
            "MYSQL_ROOT_PASSWORD=rds",
            "-e",
            "MYSQL_ALLOW_EMPTY_PASSWORD=yes",
            "mysql:8.0",
            "--server-id",
            "1",
            "--gtid-mode=ON",
        ]);
        assert_eq!(s.name, "c1");
        assert_eq!(s.network.as_deref(), Some("rds-n1"));
        assert_eq!(s.image, "mysql:8.0");
        assert_eq!(s.ports.len(), 1);
        assert_eq!(s.ports[0].host_ip.as_deref(), Some("127.0.0.1"));
        assert_eq!(s.ports[0].host_port, Some(35001));
        assert_eq!(s.ports[0].container_port, 3306);
        assert_eq!(s.envs[0].key, "MYSQL_ROOT_PASSWORD");
        assert_eq!(s.envs[0].value, "rds");
        // 镜像之后的内容是容器命令,不是 flag
        assert_eq!(s.command, vec!["--server-id", "1", "--gtid-mode=ON"]);
        assert!(s.extra.is_empty());
    }

    #[test]
    fn parses_xenon_node_args_with_named_volumes() {
        // 对齐 instance.rs::xenon_node_args
        let s = specs(&[
            "--network",
            "rds-n1",
            "--hostname",
            "rds-n1-xenon1",
            "-p",
            "127.0.0.1:35101:3306",
            "-p",
            "127.0.0.1:35102:8801",
            "-v",
            "xenon-data-rds-n1-xenon1:/var/lib/mysql",
            "-v",
            "xenon-meta-rds-n1-xenon1:/data/raft.meta",
            "-e",
            "INIT_ROLE=LEADER",
            "xenon-local:latest",
        ]);
        assert_eq!(s.hostname.as_deref(), Some("rds-n1-xenon1"));
        assert_eq!(s.mounts.len(), 2);
        assert_eq!(s.mounts[0].kind, MountKind::NamedVolume);
        assert_eq!(s.mounts[0].source, "xenon-data-rds-n1-xenon1");
        assert_eq!(s.mounts[0].target, "/var/lib/mysql");
        assert!(!s.mounts[0].read_only);
        assert_eq!(s.image, "xenon-local:latest");
        assert!(s.command.is_empty());
    }

    #[test]
    fn parses_proxy_host_bind_mount_with_rw() {
        // 对齐 instance.rs 的 newproxy 配置挂载
        let s = specs(&[
            "--network",
            "rds-n1",
            "-p",
            "127.0.0.1:35201:4051",
            "-v",
            "/opt/rds/logs/rds/n1/newproxy.conf:/app/conf/newproxy.conf:rw",
            "perf-2shard-newproxy:latest",
        ]);
        assert_eq!(s.mounts.len(), 1);
        assert_eq!(s.mounts[0].kind, MountKind::HostPath);
        assert_eq!(s.mounts[0].source, "/opt/rds/logs/rds/n1/newproxy.conf");
        assert_eq!(s.mounts[0].target, "/app/conf/newproxy.conf");
        assert!(!s.mounts[0].read_only);
    }

    #[test]
    fn parses_ro_mount_and_dts_entrypoint() {
        let ro = specs(&["-v", "/etc/my.cnf:/etc/my.cnf:ro", "mysql:8.0"]);
        assert!(ro.mounts[0].read_only);
        // 对齐 instance.rs::DtsRun 的占位容器参数
        let dts = specs(&[
            "--restart",
            "unless-stopped",
            "--entrypoint",
            "/bin/sh",
            "canal/canal-server:v1.1.7",
            "-c",
            "sleep 315360000",
        ]);
        assert_eq!(dts.restart_policy.as_deref(), Some("unless-stopped"));
        assert_eq!(dts.entrypoint.as_deref(), Some("/bin/sh"));
        assert_eq!(dts.command, vec!["-c", "sleep 315360000"]);
    }

    #[test]
    fn unknown_flag_goes_to_extra_not_failure() {
        // 自定义功能模块可能带任意 docker flag:docker driver 必须照旧可用
        let s = specs(&["--privileged", "--cap-add=SYS_ADMIN", "mysql:8.0"]);
        assert_eq!(s.image, "mysql:8.0");
        assert_eq!(s.extra, vec!["--privileged", "--cap-add=SYS_ADMIN"]);
    }

    #[test]
    fn inline_flag_values_and_dynamic_port() {
        let s = specs(&["--network=rds-n1", "-p", "3306", "--env=K=V", "mysql:8.0"]);
        assert_eq!(s.network.as_deref(), Some("rds-n1"));
        assert_eq!(s.ports[0].host_port, None);
        assert_eq!(s.ports[0].container_port, 3306);
        assert_eq!(s.envs[0].key, "K");
    }

    #[test]
    fn legacy_args_are_preserved_verbatim() {
        let raw = vec!["--network", "n1", "mysql:8.0"];
        let s = specs(&raw);
        assert_eq!(s.legacy_args, raw);
    }

    #[test]
    fn missing_image_is_an_error() {
        let e = docker_args_to_spec("c1", &["--network", "n1"]).expect_err("缺镜像应报错");
        assert!(e.to_string().contains("镜像"), "{e}");
    }
}
