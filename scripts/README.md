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
| `build.sh` | 编译。默认 `--offline`(使用仓库内 `.cargo-home` vendored registry);`--debug`/`--online`/`--clean`/`--test` |
| `mysql.sh` | MySQL 生命周期。自动识别「外部实例 vs 捆绑实例(仓库内 `.rdsctl-mysql`)」;`status/info/start/stop/restart/reset` |
| `rdsctl.sh` | HTTP 管控进程生命周期:`start/stop/restart/status/logs/foreground`(pid 文件 `logs/rdsctl.pid`,日志 `logs/rdsctl.log`) |
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

- **为什么离线**:依赖已 vendor 在 `.cargo-home`(rsproxy 镜像),`build.sh` 默认 `--offline`,无外网也能完整构建;新引入依赖需联网 `./scripts/build.sh --online` 后把缓存并入 `.cargo-home`。
- **MySQL 是控制面**(任务/节点/审计/实例记录),与实例内业务 MySQL 无关;不可用时 rdsctl 拒绝启动并给出提示。
- **kill -9 语义**:任务元数据在 MySQL,进程被强杀后重启,未完成任务自动标记 failed 且不会重跑(见 `tests/acceptance.rs` 用例 1)。
- **验收环境**:`tests/acceptance.rs` 用真实 MySQL(每用例独立库)+ 假 docker 垫片;必须在假 PATH 前显式 `RDSCTL_MYSQL_CLI` 指向真实 mysql(垫片会劫持 `mysql` 命令)。
- 端口被占用:先 `./scripts/rdsctl.sh status`/`stop`,或换 `RDSCTL_PORT`。
