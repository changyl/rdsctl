# Docker 镜像源配置(离线 / 国外镜像不可达)

rdsctl 的数据面只依赖两类容器镜像,其余(LVS 接入层)已为**进程内实现、不依赖镜像**:

| 用途 | 默认镜像(docker hub) | 可用国内/内网镜像 |
| --- | --- | --- |
| MySQL 节点 | `mysql:8.0` | 如 `docker.m.daocloud.io/library/mysql:8.0` |
| newproxy 代理 | `perf-2shard-newproxy:latest` | 自行推到内网 registry 后按全名覆盖 |
| DTS(canal) | `canal/canal-server:v1.1.7` | 如 `docker.m.daocloud.io/canal/canal-server:v1.1.7` |

## 方式一:环境变量覆盖(rdsctl 侧)

在启动 rdsctl 前设置(部署脚本会透传环境变量;rdsctl.env 亦可用):

```bash
export RDSCTL_MYSQL_IMAGE="docker.m.daocloud.io/library/mysql:8.0"
export RDSCTL_PROXY_IMAGE="registry.internal.example.com/perf-2shard-newproxy:latest"
export RDSCTL_DTS_IMAGE="docker.m.daocloud.io/canal/canal-server:v1.1.7"   # DTS(canal)
./scripts/rdsctl.sh restart        # 重启生效
```

## 方式二:Docker daemon 配置 registry mirror(对 docker pull 全局生效)

`~/.docker/daemon.json`(macOS Docker Desktop 在 Settings → Docker Engine 编辑):

```json
{
  "registry-mirrors": [
    "https://docker.m.daocloud.io",
    "https://docker.1ms.run",
    "https://dockerproxy.net"
  ]
}
```

保存后重启 Docker,再 `docker pull mysql:8.0` 验证(镜像仍叫 mysql:8.0,无需改 rdsctl)。

> 镜像加速仅对 docker hub 官方镜像有效;自定义镜像(如 perf-2shard-newproxy)
> 请先 `docker tag` 后 `docker push` 到可达的内网 registry,再按方式一配置。

## 常见排查

- 创建任务失败于 `docker: ... rdsctl-lvs:latest / registry timeout`:说明服务仍为旧版本。
  新版本接入层(LVS)为进程内转发,不再拉取任何镜像 —— 请用最新 release 重启服务后重新创建实例;
- 进程内 VIP 测试:`nc -vz 127.0.0.1 <实例 lvs_mysql_port>` 应通(实例 running 后;
  若服务刚重启,sweeper 默认 30s 内自动恢复接入层);
- Proxy 直连诊断端口(实例详情拓扑→Proxy 卡)仍可用。
