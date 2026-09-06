# 多分片实体创建(multishard)

> 日期:2026-09-03 起。修复:创建实例选多分片后实际仍是 1 分片 —— 分片数此前仅作
> 元数据记录,不生成实体。本文记录多分片语义、迭代与边界。

## 1. 语义

`POST /api/rds/create` 参数 `shard_num=N`(N>1)配合 `itype=async|sync` 时,**实体创建 N 个
独立主从分片组**,每组:

- 1 个主节点:`rds-{name}-s{k}-master`(k=1..N,组内自增 server_id);
- `sync`:1 个读从 `rds-{name}-s{k}-slave-1`;
- `async`:**1 个读从 + 1 个离线从(备份/统计)** —— `rds-{name}-s{k}-slave-1 / slave-2`,
  与单分片 async(主 + 读 + 离线)语义对齐;
- 从节点均向同组主节点建复制(parent=组内 master);同一实例网络内、各自独立 host 端口与
  GTID/server_id;
- 每分片独立校验(主写 init → 本分片全部从追平 → 代理连通);
- 实例 `shards` 元数据 = N 行 `ShardInfo{id:s{k}, master, slave, slaves:[读从/离线从…]}`,
  节点 `shard` 字段 = `s{k}`,与前端「分片列表/拓扑/监控」数据契约对齐(可完整展示每个分片
  的全部从节点)。

入口/代理:代理集群仍指向**分片1 读从**(单入口)。N==1 走历史路径(legacy 命名与行为不变)。

## 2. 边界(未接入,明确拒绝/留待后续)

- **分片间数据路由与每分片独立入口代理**:属代理层/引擎模板范围(与 distributed 模板同口径),
  分片2..N 目前经容器网络可达;
- **分片级扩容(已接入)**:`POST /api/rds/scaleout?name=&shard=s{k}&role=read|offline` —— 新从
  节点挂到该分片 master,命名延续 `rds-{name}-s{k}-slave-{n}`(取分片现有最大序号+1),并同步
  `ShardInfo.slaves`;读从不限数量,离线从每分片仅允许一个(async 分片创建已含离线从;sync 分片
  可扩容出第一个离线从)。未指定 shard 的多分片实例级扩容仍被 400 拒绝;
- **分片级维护(单个分片节点启停/重启、缩容)与跨区扩容**:留待后续里程碑;
- 其它类型约束不变:`single` 拒绝 N>1;`distributed`(分片集群引擎模板)仍不可创建。

## 3. 校验

- 单元:`cargo test --bin rdsctl`(历史用例回归);
- 验收:`cargo test --test acceptance create_multishard_builds_n_shards`:
  - async 2 分片 → **6 个 DB 节点**(2 主 + 2 读从 + 2 离线从)、shards=2 且每行 `slaves` 含
    read+offline 2 项、六个容器 RUN 记录齐全;
  - **分片级扩容**:async s1 扩容读从 → 7 节点、s1 出现 `slave-3`(read)且 `slaves` 3 项;
    s1 已有离线从再扩被拒;不存在分片被拒;
  - sync 2 分片 → **4 个 DB 节点**(2 主 + 2 读从,无离线从);s1 扩容离线从 → 5 节点;
  - 未指定 shard 的实例级扩容被拒;销毁清理全部节点。
