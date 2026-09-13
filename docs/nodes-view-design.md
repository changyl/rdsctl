# 创建入口分类 与 节点状态页(nodes-view-design)

> 目标(两项):
> ① **实例创建入口按大类分组**——单节点 / 高可用版 / Xenon Raft 高可用 / OceanBase(占位,标注"近期开放");
>    其中「高可用版」的**复制模式(异步 / 半同步)是该卡的二级选择**,不与大类并列;
> ② **侧边栏「节点状态」页**——跨实例汇总每个 DB / Proxy 节点的状态事实,并提供就地控制能力。
> 前端单页 `src/rds.html`;后端新增只读接口 `GET /api/rds/nodes`。
> 状态:已实现并通过验收(`tests/acceptance.rs::nodes_inventory_api_and_view_registered`)。

---

## 1. 结论摘要

| 项 | 做法 | 为什么不那样做 |
|---|---|---|
| 一级入口 | `#c-arch` 里**四张横向卡片**(单节点 / 高可用版 / Xenon Raft 高可用 / OceanBase 占位),沿用既有 `.rcards` 网格,不加分类标题行 | 曾试过"分类标题 + 分组卡片",结果是入口变成**纵向堆叠**且异步/同步并列成两张同级卡 —— 既不横向、也说错了层级 |
| 异步 / 半同步 | 作为**高可用版的二级选择**:选中该卡后才出现 `#c-ha-mode`(复用既有 `.cseg` 分段控件) | 复制模式不是架构大类,它是"高可用版"的参数;并列会让人误以为三者是三套不同的部署形态 |
| 值契约 | 二级选择最终仍落到隐藏的 `#c-itype` 卡片(`data-itype` ∈ `single/async/sync/xenon`)→ `createState.itype` | 提交/校验/选项显隐(DTS、分片锁定、xenon 专属区)都挂在 `data-itype` 上,**保持不变**就不动后端与提交路径 |
| OceanBase 占位 | `.rcard.is-soon`(虚线 + 降透明度 + `aria-disabled`)且 click 绑定里**显式拒绝** | 仅靠样式禁用仍可点;仅靠后端拒绝会变成"点了没反应" |
| 节点状态页 | 新增只读接口 `GET /api/rds/nodes`(DB + Proxy 统一行),前端新增 `view-nodes` + 2.5s 轮询(仅本页可见) | 不给 `/api/rds/instances` 加"节点"维度(实例与节点是两种粒度,混在一起会破坏既有分页/筛选语义) |
| 可执行动作 | **由后端按架构/角色裁决**并下发 `actions[]`,前端只渲染契约内的动作 | 前端按 `itype`/`role` 自行推断会与后端(尤其 xenon 的自治选主语义)漂移 |

不变式:**本页所有写操作走的都是既有接口**(代理启停/重启/健康、xenon raft 竞选、受管 PRS 切换),
不引入新的写路径,因此权限、审计、DAG/步骤账本语义与从别处触发完全一致。

---

## 2. 创建入口(需求 ①)

`src/rds.html` 仍是两组卡片,必须**同改**:

| 位置 | 角色 |
|---|---|
| `#c-arch` | **可见主入口**(第一步);选中的大类经 `mirrorItype()` 同步到隐藏载体 |
| `#c-itype` | **隐藏数据载体**(`display:none`);真正的值落在模块级 `createState.itype` |

一级四张卡与它落到的值:

| 卡片 | `data-arch` | 落的 `itype` | 备注 |
|---|---|---|---|
| 单节点 | `single` | `single` | 无高可用 |
| 高可用版 | `ha` | `async` 或 `sync`(由二级选择决定) | 一主多从 + 受管切换;卡片 `.rc-dia` 里 `<b id="c-ha-badge">` 实时显示当前模式 |
| Xenon Raft 高可用 | `xenon` | `xenon` | raft 自治选主;唯一人工通道 = `trytoleader` |
| OceanBase | (无 `data-arch`) | — | `.is-soon` 占位,**不可创建**;标注「近期开放」 |

二级选择(`#c-ha-mode`,默认隐藏):

```
高可用版复制模式  [ 异步复制(推荐) | 半同步复制 ]
异步:主库提交不等待从库确认,吞吐优先;半同步:主库等待从库确认落盘,一致性更强
```

交互与实现要点:

1. 点 `ha` 卡 → `#c-ha-mode` 显示 → `setHaMode(haMode, silent)` 把当前模式镜像到 `#c-itype`
   (触发其 click 逻辑,统一裁决选项区显隐)→ 进入表单。
2. 点二级按钮 → `setHaMode(b.dataset.ha, false)` → 只更新徽标 + 镜像 + 提示文案,**不重新滚动**。
3. 点其它大类卡 → 隐藏 `#c-ha-mode` 并直接镜像该 `data-itype`。
4. 进入创建页(`navTo("create")`)时回到第一步:收起表单、隐藏 `#c-ha-mode`、清掉 `#c-arch .rcard.on`
   —— 避免"看起来已经选过"。
5. 后端不变:OceanBase/`distributed` 仍被 `src/instance.rs` 的创建校验拒绝(弹窗旧入口文案同步为
   「分布式 OceanBase(占位 · 近期开放)」)。**"占位"是前后端一致的事实,不是前端单方面灰掉。**

---

## 3. 节点状态页(需求 ②)

### 3.1 后端契约

`GET /api/rds/nodes`(权限 `instances.view`;路由 `src/http.rs`,`handler` `src/api.rs::nodes`,
装配 `src/instance.rs::RdsManager::nodes_inventory`)

| 参数 | 默认 | 说明 |
|---|---|---|
| `instance` | 空 | 实例名**精确**匹配 |
| `kind` | 空 | `db` / `proxy` |
| `q` | 空 | 关键词(容器名 / 实例名 / `host_port` / 主机名,子串、忽略大小写) |
| `limit` | 500 | 1–2000(clamp) |
| `offset` | 0 | 分页偏移 |

返回 `{ nodes: [...], summary: {...}, limit, offset }`,`summary` 基于**过滤后的全量**(不受分页影响)。

单行字段(DB / Proxy 同构):`kind/instance/itype/region/az/tenant/instance_status/enabled/
container/state/role/role_label/host_name/host_port`,DB 另带 `server_id/parent/rpc_host_port`,
Proxy 另带 `mysql_port/mng_port`;两者都由 Host 注册表富化 `host_ip/host_region/host_az/host_rack/
agent_port/addr/exec_route`,并由后端裁决 `actions[]`。

`addr`:**只要端口 > 0 就一定给出** —— 未绑定登记机器时为 `127.0.0.1:<host_port>`(`exec_route = local`),
绑定机器时为 `<host_ip>:<host_port>`。此前本机节点不返回 `addr`、代理行还用过 `host_port`(缺省 0)拼成
`127.0.0.1:0`,现已统一到后端一处计算。

### 3.2 节点状态词表(与巡检一致)

`ok` / `degraded` / `repl_down` / `down` / `missing` / `stopped` / `remote` / `unknown`。

**`remote` = 管理盲区**:节点绑定在登记机器上但 agent 不可达,**未做本机检查** —— 不等于节点故障,
前端单独用 violet 色列出而不并入"异常",避免把"看不了"报成"坏了"。

### 3.3 动作裁决(后端 `db_node_actions()`)

| 节点 | `actions[]` | 说明 |
|---|---|---|
| DB,`xenon` 且非 master | `xenon_trytoleader`, `detail` | xenon 自治选主,**唯一**人工通道是竞选;受管 PRS 对 xenon 一律拒绝 |
| DB,非 xenon 且非 master | `reparent_to_this`, `detail` | 受管计划切换(PRS) |
| DB,master | `detail` | 主节点无切换动作 |
| Proxy | `proxy_restart`, `proxy_stop`, `proxy_start`, `proxy_health`, `detail` | 复用 `POST /api/rds/proxy` |

前端动作 → 既有接口:

| 动作 | 请求 |
|---|---|
| `proxy_start/stop/restart/health` | `POST /api/rds/proxy?container=&action=start\|stop\|restart\|health` |
| `xenon_trytoleader` | `POST /api/rds/xenon/raft/trytoleader?instance=&node=` |
| `reparent_to_this` | `POST /api/rds/orch/reparent?instance=&target=&mode=planned` |
| `detail` | 前端路由 `#/i/<instance>` |

危险动作前均有确认:`proxy_stop` 用 `confirmDlg`;`xenon_trytoleader` / `reparent_to_this` 用 `riskDlg`
(需键入实例名),与实例详情页既有交互一致。写操作后 1.2s 触发一次刷新等待事实收敛。

### 3.4 前端

- 侧边栏 `data-view="nodes" id="nav-nodes"`(在「实例」之后);`VIEW_IDS` / `VIEW_TITLE` / `navTo` /
  `routeTo` / 侧栏权限表 / `v.any` 全部登记。
- 视图 `#view-nodes`:筛选栏(关键词 / 实例名 / `kind` / 状态)+ 汇总条 + 表格 + 空态。
- 轮询:沿用 2.5s `tick()`,**仅当本页可见**才请求;失败静默(不重复弹提示),手动「刷新」才提示错误。

---

## 4. 限制与后续(不粉饰)

1. **状态筛选在前端完成。** 接口契约没有 `state` 参数;前端一次取 `limit=2000`,本地过滤与汇总。
   - 本页隐含上限是 **2000 节点/次全量拉取**;超过后 `匹配 N / total_matched M` 会显现,不假装完整。
   - 与 `docs/scaling-design.md` §1 对"前端全量轮询"的判断一致:这是过渡实现,**不是** 10 万级方案。
     正解是把 `state` 变成服务端过滤 + 分页(属 scaling-design 的读路径工作)。
2. **OceanBase 是纯占位。** 没有集群模板、没有引擎镜像、后端 `distributed` 仍不可创建。
3. **节点"控制能力"只覆盖既有后端动作。** 不提供 kill / 直接改配置 / 强制改 role 这类没有共识与审计通道的动作
   —— 集群模式下绕过步骤账本(fence/幂等)的写操作是被设计禁止的。
4. 本页读的是**实例视图里的节点事实**(与实例详情同源),不是实时探测;`state` 的刷新节奏取决于巡检周期。

---

## 5. 验收

`tests/acceptance.rs::nodes_inventory_api_and_view_registered`(需本地 MySQL):

| # | 断言 |
|---|---|
| ① | 空态结构完整:`nodes` 为空数组,`summary` 含全部计数字段 |
| ② | `async` + `proxies=2` → 3 个 DB 节点 + 2 个 Proxy 节点,`summary` 计数一致 |
| ③ | 动作裁决:master 仅 `detail`;非 xenon 从节点含 `reparent_to_this`;代理为 `proxy_restart/stop/start/health/detail` |
| ④ | 代理 `addr` 以 `mysql_port` 结尾(回归"代理 addr 拼成 `:0`"的缺陷) |
| ⑤ | `kind=proxy` 与 `q=<容器名>` 过滤命中数正确 |
| ⑥ | 页面标记:`nav-nodes`/`view-nodes`/`nd-rows`/`/api/rds/nodes`;创建入口 `data-arch` 只有 `single|ha|xenon`(**断言 `data-arch="async"|"sync"` 不存在**)且具备 `c-ha-mode`/`c-ha-seg`/`data-ha=async|sync`/`is-soon`/「近期开放」 |
