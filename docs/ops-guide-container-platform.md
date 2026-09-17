# 接入新的容器/承载平台 —— 操作指南

> 面向:要把 rdsctl 管控的数据面搬到 docker 以外的承载平台(物理机直装 / podman / k8s /
> OpenStack / 自研平台)的工程师。
> 契约与设计见 [container-platform-abstraction.md](./container-platform-abstraction.md);
> 物理机/跨机模型见 [physical-multi-site-ops.md](./physical-multi-site-ops.md)。

---

## 0. 30 秒决策:你的平台属于哪一类

| 你的平台 | 接入方式 | 需要改代码? | 需要重编译 rdsctl? | 预计工作量 |
|---|---|---|---|---|
| docker / **podman** / **nerdctl**(CLI 兼容) | A. 改一个环境变量 | 否 | 否 | 分钟级 |
| 物理机上跑 docker(跨机) | C. 部署 agent + 登记 Host | 否 | 否 | 半小时 |
| **k8s** / Nomad / 自研平台(有 API/CLI) | B. 写一个**外部驱动**(shim) | 否 | 否 | 1–3 天 |
| OpenStack 虚机 | C + 人工建机(自动化 ⏳ P3) | 否 | 否 | 1 天 |
| 物理机**直装**(无容器,systemd) | ⏳ P5 规划中 | 是(内置 driver) | 是 | — |
| 希望内置到 rdsctl 长期主用 | `impl WorkloadRuntime` + 注册一处 | 是 | 是 | 2–5 天 |

> 原则:**能用外部驱动就不要改 rdsctl**。外部驱动的凭据/升级都在平台侧,控制面零改动。

---

## 1. 方式 A:CLI 兼容平台(podman / nerdctl / containerd)

rdsctl 的 docker 后端就是「跑一条 CLI 命令」,换 CLI 即接入。

```bash
# agent 所在机器(或单机控制面)上:
export RDSCTL_CONTAINER_CLI=podman     # 或 nerdctl
```

写入 `rdsctl.env` 或 agent 的启动环境后重启:

```bash
./scripts/rdsctl.sh restart            # 单机控制面
# 跨机:在目标物理机上重启 agent
RDSCTL_CONTAINER_CLI=podman ./target/release/rdsctl agent --port 9191
```

**要求**(CLI 需要与 docker 子命令同构):
`run -d --name` / `start` / `stop` / `restart` / `rm -f [-v]` / `rename` /
`inspect -f` / `logs --tail` / `exec` / `exec -i` / `network create|rm|inspect`。

**验证**:

```bash
RDSCTL_CONTAINER_CLI=podman ./target/release/rdsctl agent --port 9191 &
curl -s http://127.0.0.1:9191/agent/ping | python3 -m json.tool
# → {"ok":true,"runtime":"docker","caps":{...},...}
```

`runtime` 仍显示 `docker`(实现类名),平台身份由 `RDSCTL_CONTAINER_CLI` 决定;
`/agent/ping` 的 `caps` 可用于确认能力面。

---

## 2. 方式 B:外部驱动(推荐用于 k8s / 自研平台)

### 2.1 步骤

1. **写驱动**:一个可执行文件,读 stdin 一行 JSON、写 stdout 一行 JSON。
   最小可用子集:`caps` `create` `start` `stop` `remove` `exists` `state` `logs` `exec`。
   其余 action 可先返回 `unsupported`。
   参考:`scripts/runtime/k8s-driver.sh`。
2. **放置**:绝对路径,属主为控制面/agent 运行用户,权限不含 other-write(`0755`)。
   ```bash
   sudo install -o root -g root -m 0755 ./k8s-driver.sh /opt/rdsctl/drivers/k8s-driver.sh
   ```
3. **声明能力**(逗号分隔,列出**不支持**的能力):
   ```bash
   export RDSCTL_RUNTIME_CAPS=host_ports,named_volumes   # 例:k8s 无宿主端口/命名卷
   ```
4. **启用**:
   ```bash
   export RDSCTL_RUNTIME=external
   export RDSCTL_RUNTIME_CMD=/opt/rdsctl/drivers/k8s-driver.sh
   export RDSCTL_RUNTIME_TIMEOUT_SECS=60
   ```
5. **重启并看启动自检**。启动日志会出现:
   ```
   执行后端: external
   外部驱动 /opt/rdsctl/drivers/k8s-driver.sh 能力声明: RuntimeCaps { host_ports: false, … }
   ```
   路径不合法/不可执行 → 进程**直接退出(码 2)**,不会带着错误配置跑起来。

### 2.2 自测驱动(不接控制面)

```bash
echo '{"action":"caps"}' | /opt/rdsctl/drivers/k8s-driver.sh
# → {"ok":true,"caps":{"host_ports":false,...}}

echo '{"action":"create","spec":{"name":"t1","image":"mysql:8.0","envs":[{"key":"K","value":"V"}],"command":[]}}' \
  | /opt/rdsctl/drivers/k8s-driver.sh
# → {"ok":true}
```

### 2.3 驱动必须遵守的三条

1. **`extra` 非空 = 拒绝**。控制面把无法理解的 docker 参数放进 `spec.extra`;
   驱动看不懂就返回 `{"ok":false,"error":"...","code":"unsupported","cap":"docker_flag"}`,
   **不要猜**。仓库里所有真实用例的参数都已结构化,不会落到 extra。
2. **能力缺失要显式报错**,不要静默忽略:
   `{"ok":false,"error":"...","code":"unsupported","cap":"host_ports"}`。
   控制面会把它还原成 `unsupported capability: host_ports(...)` 展示给使用者。
3. **`meta.idem` 要做幂等短路**:同键重复请求直接回放首次结果(重试/续跑依赖它)。

### 2.4 验证清单

```bash
# 1) 能力自检
echo '{"action":"caps"}' | $RDSCTL_RUNTIME_CMD
# 2) 控制面启动自检无错(见日志)
./scripts/rdsctl.sh restart && ./scripts/rdsctl.sh logs | tail -20
# 3) 端到端:创建一个小实例,观察任务中心
#    失败时任务节点会带平台侧原始错误(含 cap 名)
```

---

## 3. 方式 C:物理机(跨机执行)

这是**今天最成熟**的路径(原 P2-①,见 [physical-multi-site-ops.md](./physical-multi-site-ops.md) §7)。

```bash
# 1) 目标物理机上启动 agent(选定该机的执行后端)
RDSCTL_AGENT_TOKEN=<强口令> \
RDSCTL_CONTAINER_CLI=docker \
./target/release/rdsctl agent --port 9191 --runtime docker

# 2) 控制面登记该机器(agent_port=0 表示未接入 → 管理盲区)
curl -s -X POST "$BASE/api/rds/hosts?name=host-cn-north-01&ip=10.0.0.11\
&region=cn-north&az=az1&rack=rack-1&cpu=32&mem_gb=128&disk_gb=2000&agent_port=9191" -b "$CK"

# 3) 把节点绑到该机器(用现有受管工作流,自动走 agent 执行)
curl -s -X POST "$BASE/api/rds/replace_node?instance=<inst>&node=<node>&host=host-cn-north-01" -b "$CK"
```

要点:

- **控制面不碰远端机器**,所有原语经 agent;平台差异(该机用 docker 还是外部驱动)
  由 agent 启动参数决定,控制面不需要知道。
- **agent 端也要选后端**:该物理机若跑 k8s/自研平台,agent 侧配
  `--runtime external --runtime-cmd /opt/.../driver.sh` 即可,控制面完全不变。
- **离线/断连语义**:agent 不可达 → 节点标 `remote`(管理盲区),不触发自动 ERS;
  agent 恢复 → 巡检自动回到 `running`。
- **演练**:`./scripts/agent-drill.sh`(单机双进程验证路由与降级)。

### 3.1 OpenStack

今天:`OpenStack 建虚机 → 虚机内 cloud-init 装 agent → 按 §3 登记 Host`(建机人工/脚本)。
自动化建机 + 引导(节点供给层 `NodeProvider`)为 ⏳ P3。

之所以这样映射:OpenStack 是 IAAS,粒度是虚机;直接在容器层对接会丢故障域与容量语义。

---

## 4. 环境变量一览

| 变量 | 默认 | 作用 | 落地 |
|---|---|---|---|
| `RDSCTL_RUNTIME` | `docker` | 进程默认执行后端:`docker` / `external` | ✅ |
| `RDSCTL_CONTAINER_CLI` | `docker` | docker 驱动的 CLI(填 `podman`/`nerdctl`) | ✅ |
| `RDSCTL_RUNTIME_CMD` | 空 | 外部驱动**绝对路径** | ✅ |
| `RDSCTL_RUNTIME_ARGS` | 空 | 外部驱动附加 argv(空格分隔) | ✅ |
| `RDSCTL_RUNTIME_CAPS` | 空 | 逗号分隔的**不支持**能力名 | ✅ |
| `RDSCTL_RUNTIME_TIMEOUT_SECS` | `60` | 外部驱动单次调用超时 | ✅ |
| `RDSCTL_AGENT_TOKEN` | 空 | agent 鉴权(两端一致;空=不鉴权,仅 lab) | ✅ |
| `RDSCTL_AGENT_FENCE_REQUIRED` | `0` | 1=强制 fence(cluster 模式必须) | ✅ |
| `RDSCTL_AGENT_URL` | 空 | cluster 模式下本机也经该 agent 执行(**取消 Local 直连特权**,A4) | ✅ |
| `RDSCTL_NODE_PROVIDER` | — | 节点供给(P3) | ⏳ |
| `RDSCTL_REQUIRE_AGENT` | — | 本机也强制经 agent(P2) | ⏳ |

命令行覆盖(优先级高于 env):`rdsctl agent --runtime <kind> --runtime-cmd <path>`。

---

## 5. 排障

| 现象 | 原因 | 处理 |
|---|---|---|
| 启动即退出,日志 `执行后端启动自检失败` | `RDSCTL_RUNTIME` 拼错 / 驱动路径非法 | 按日志修正;不合法配置**故意**不启动 |
| `unsupported capability: host_ports(...)` | 平台无宿主端口,但规格带了 `-p` | 用驱动的 `expose` 提供接入点(P4 起控制面自动采用);或让驱动用 hostPort/NodePort 暴露 |
| `unsupported capability: docker_flag(...)` | 步骤里有驱动看不懂的 docker 参数(如自定义功能模块里的 `--privileged`) | 去掉该参数,或在驱动里实现对应语义 |
| `unsupported capability: named_volumes(...)` | 该平台无命名卷概念 | 用宿主路径/PVC 等价物,并在 `RDSCTL_RUNTIME_CAPS` 中如实声明 |
| 节点一直 `remote` | agent 不可达 / `agent_port=0` | 检查 agent 进程与网络;`curl $AGENT/agent/ping` |
| `fence_stale` (HTTP 409) | 该 agent 见过更新的 fence(别的副本已接管) | 正常保护;确认控制面多数派状态 |
| agent 日志出现「版本较旧…退回 /agent/run」 | 控制面已升级、agent 未升级 | 关停窗口内可用(自动回退);建议尽快升级 agent,否则 `purge`(连卷删除)会被忽略 |
| 外部驱动超时 | 平台 API 慢 | 调大 `RDSCTL_RUNTIME_TIMEOUT_SECS`;驱动内加自己的重试 |
| cluster 模式下某个变更报 `fence_required`(409) | 该操作**未持有实例租约**(拿不到 fence) | 属 A4 的设计行为:变更必须由持租约的操作发起;若是巡检类查询报错,说明它误用了变更通道(应走只读原语) |
| cluster 模式下巡检/事实全部报错 | 本机 agent 不可达(`RDSCTL_AGENT_URL` 已配但进程没了) | 恢复 agent;这是刻意设计——配上 agent 后本机也经 agent,不再有直连后门 |

---

## 6. 回滚

任何方式都可**仅靠环境变量**回退,无数据迁移:

```bash
unset RDSCTL_RUNTIME_CMD
export RDSCTL_RUNTIME=docker      # 或直接 unset
./scripts/rdsctl.sh restart
```

已有实例的元数据(容器名/宿主端口/复制链)不受抽象层影响:
工作负载标识仍是容器名,`node_hosts` 绑定不变。
