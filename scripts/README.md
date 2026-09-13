# rdsctl 编译 / 部署脚本

本机(单机)交付链路:**离线编译 → MySQL 就绪 → 服务启停 → 健康检查 → P0 验收**。

## 快速开始

```bash
./scripts/deploy.sh                # 一键:release 离线编译 + MySQL + 服务 + 健康检查
./scripts/deploy.sh --test         # 部署后追加 P0 验收(单元 + kill-9/并发/巡检 集成)
```

完成后打开 http://127.0.0.1:9113/rds (admin/admin)。

## 各脚本职责

| 脚本 | 说明 |
| --- | --- |
| `lib.sh` | 公共库:仓库根推导、加载 `rdsctl.env`、MySQL 连接默认值、mysql 客户端封装 |
| `build.sh` | 编译。默认 `--offline`(仓库根 `.cargo-home/config.toml` + `vendor/` 源码替换,无需外网);`--debug`/`--online`/`--clean`/`--test` |
| `mysql.sh` | MySQL 生命周期。自动识别「外部实例 vs 捆绑实例(仓库内 `.rdsctl-mysql`)」;`status/info/start/stop/restart/reset` |
| `rdsctl.sh` | HTTP 管控进程生命周期:`start/stop/restart/status/logs/foreground`。pid/日志按**实例键**区分(single=端口,cluster=节点 id);cluster 模式需 `RDSCTL_MODE=cluster` + `RDSCTL_NODE_ID/CLUSTER/RPC_PORT`,参数可经 `RDSCTL_ARGS` 覆盖 |
| `cluster.sh` | **同机多副本集群**:`up [--nodes N] [--with-agent] [--lab] [--release\|--lean] / status / restart / down [--purge]`;状态文件 `logs/ha/cluster.state` |
| `ha-drill.sh` | **集群端到端演练**(起集群→验共识租约/fence/失效转移/失多数派 fail-closed/自愈→停集群);结果 PASS/FAIL,日志 `logs/ha-drill.log` |
| `destroy-all-containers.sh` | 清场工具:按前缀(`rds-`,可用 `--prefix=` 覆盖)强制删除本套管控创建的全部 Docker 容器与专属网络;`--yes` 免确认、`--dry-run` 只预览(不依赖管控端状态) |
| `deploy.sh` | 一键部署编排:编译 → MySQL → 重启服务 → 登录/API 健康检查 → 汇总;`--skip-build`/`--debug`/`--test` |

## 配置

优先级:**环境变量 > 仓库根 `rdsctl.env` > 代码默认值**。

`rdsctl.env.example` 是模板;`deploy.sh` 首次运行自动生成 `rdsctl.env`。常用项:

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `RDSCTL_PORT` | 9113 | HTTP 端口 |
| `RDSCTL_USER` / `RDSCTL_PASS` | admin/admin | 登录凭据 |
| `RDSCTL_MYSQL_HOST/PORT/USER/PASS/DB` | 127.0.0.1 / 3306 / root / '' / rdsctl | 控制面持久化 MySQL |
| `RDSCTL_MYSQL_DATA_DIR` | 仓库内 `.rdsctl-mysql` | 捆绑 MySQL 数据目录 |
| `RDSCTL_SWEEP_SECS` | 30 | 健康巡检周期 |
| `RDSCTL_MYSQL_CLI` | PATH 查找 | mysql 客户端路径(验收垫片环境必须显式指定真实路径) |
| `RDSCTL_REGION/AZ/SHARD/TENANT` | default/default/default/空 | 新实例的控制面归属(M0,多区域扩展) |
| `RDSCTL_CONTROLLER_ID` | hostname:pid | 实例 lease 持有者标识(多进程需唯一) |
| `RDSCTL_LOCK_LEASE` | 30 | 实例操作锁租约(秒),watch_task 自动续约 |
| `RDSCTL_RESUME_TASKS` | 0 | 1=启动续跑(未终态任务重新执行,已完成节点不重跑);0=中断标 failed |
| `RDSCTL_STORE_BACKEND` | mysql | memory=进程内内存后端(lab/合成;不持久) |
| `RDSCTL_DEMO_SEED` | 0 | 1=实例库为空时自动种入一批演示实例(带业务线/DBA/版本/规格标签,不起容器;network=demo-* 巡检豁免) |

## 常见场景

```bash
# 只想重新编译(不重启服务)
./scripts/build.sh

# 改配置后重新部署(只重启服务)
RDSCTL_PORT=9120 ./scripts/deploy.sh --skip-build

# 清理/卸载:停全部本脚本管理的 rdsctl 实例,删 pid 与服务日志(保留 MySQL 数据/配置)
./scripts/deploy.sh clean
# 附加:一并停止捆绑 MySQL 并删除其数据目录(外部 MySQL 不受影响)
CONFIRM_PURGE=1 ./scripts/deploy.sh clean --purge-data
# 附加:同时删除 rdsctl.env(下次 deploy 重新生成默认配置)
./scripts/deploy.sh clean --remove-config

# MySQL 使用外部已有实例(不碰本机数据目录)
#   1) 在 rdsctl.env 配好 RDSCTL_MYSQL_* 指向它
#   2) export RDSCTL_MYSQL_EXTERNAL=1   (脚本只连通/建库,永不 stop/reset 外部实例)

# 查看状态与日志
./scripts/rdsctl.sh status
./scripts/rdsctl.sh logs
```

## 说明与 FAQ

- **为什么离线**:依赖源码已 vendored 在仓库 `vendor/`(50 个包),`build.sh` 默认以 `CARGO_HOME=<仓库>/.cargo-home` 加载 `source replacement` 并加 `--offline`,**无外网可完整构建**(已验证:`cargo build --offline` 通过)。新增/升级依赖时联网 `./scripts/build.sh --online`,再执行 `CARGO_HOME=$PWD/.cargo-home CARGO_NET_OFFLINE=false cargo vendor --versioned-dirs vendor` 刷新 `vendor/` 并提交。`.cargo-home/` 只保留 `config.toml`(registry 缓存不入库)。
- **MySQL 是控制面**(任务/节点/审计/实例记录),与实例内业务 MySQL 无关;不可用时 rdsctl 拒绝启动并给出提示。
- **kill -9 语义**:任务元数据在 MySQL,进程被强杀后重启,未完成任务自动标记 failed 且不会重跑(见 `tests/acceptance.rs` 用例 1)。
- **验收环境**:`tests/acceptance.rs` 用真实 MySQL(每用例独立库)+ 假 docker 垫片;必须在假 PATH 前显式 `RDSCTL_MYSQL_CLI` 指向真实 mysql(垫片会劫持 `mysql` 命令)。
- 端口被占用:先 `./scripts/rdsctl.sh status`/`stop`,或换 `RDSCTL_PORT`。

## 集群(cluster)模式

管控面多副本 + 自带多数派仲裁(设计见 `docs/control-plane-ha-design.md`)。脚本层支持两种用法:

```bash
# ① 同机多副本(开发/演练):一键起 3 副本 + 本机 agent
./scripts/cluster.sh up --with-agent --lab        # --lab 放行未验证前提(时钟/bootstrap),勿用于生产
./scripts/cluster.sh status                       # 每副本 /healthz + /readyz(ready/quorum/premises)+ 当前 leader
./scripts/cluster.sh down [--purge]               # 停集群(可选删数据目录)

# ② 端到端演练:起→验(租约单写者/fence 单调/kill -9 失效转移/失多数派 503/自愈)→停
./scripts/ha-drill.sh                             # 默认 dev-lean;`--release` 用发布产物

# ③ 按节点部署(可配合 systemd 模板;生产多机推荐)
export RDSCTL_MODE=cluster RDSCTL_NODE_ID=node1 \
       RDSCTL_CLUSTER=1@10.0.0.1:9330,2@10.0.0.2:9330,3@10.0.0.3:9330 \
       RDSCTL_RPC_PORT=9330 RDSCTL_PORT=9113 \
       RDSCTL_DATA_DIR=/var/lib/rdsctl/node1
./scripts/deploy.sh --cluster                     # 先跑前提自检,不通过即拒绝部署(非零退出)
```

要点:

- **前提门禁**:`deploy/bin/rdsctl-preflight.sh` 检查 时钟同步(A1)/fsync(A2)/voter 奇数≥3(A3)/
  agent 可达(A4)/多数派可达;不通过 → 退出码 2,部署中止。lab 放行开关:
  `RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK=1`、`RDSCTL_ALLOW_NO_AGENT=1`(放行后 `/readyz` 会标注
  `premises_unverified` / `lab_degraded`)、首次引导用 `RDSCTL_PREFLIGHT_ALLOW_BOOTSTRAP=1`。
- **就绪语义**:cluster 模式 `rdsctl.sh start` 用 `/healthz` 判"进程可用",`status` 展示 `/readyz`
  (ready/quorum_ok/leader/premises)——**前提未验证不会被当成"部署成功"**。
- **数据目录按节点隔离**:默认 `logs/ha/<node-id>`;同机多副本必须各用一套(pid/日志同理由节点 id 区分)。
- 生产(多机)请用 `deploy/systemd/rdsctl@.service` + `/etc/rdsctl/<node>.env`(见 `deploy/README.md`);
  本仓库无法在 macOS 上实测 systemd,故脚本路径是本机能真正验证的那条。
