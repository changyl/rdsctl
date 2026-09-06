# M0 验收报告(scaling M0 — 架构底座)

> 依据:docs/scaling-design.md M0 范围。日期:2026-09-02。
> 结论:**M0 全部落地并通过**;原有 P0 验收三项保持全绿;协议级单元测试与合成指标
> 见下。

## 1. M0 交付项核对

| 项 | 内容 | 状态 |
|---|---|---|
| M0-0 | docs/scaling-design.md(方案/决策/容量模型/DAG 组合设计归档) | ✅ |
| M0-1 | 持久化 backend trait:StoreBackend(默认 MySQL CLI 实现 + MemoryBackend),既有调用点经 Store 委托零改动 | ✅ |
| M0-2 | region/az/shard/tenant 入数据模型;instances 表迁移列+索引;查询筛选 | ✅ |
| M0-3 | 实例 lease 分布式锁(存储层行级 TTL;进程内快路径 op_locks + watch_task 续约) | ✅ |
| M0-4 | API 实例列表分页(limit/offset+region/az/status/tenant/q)+ 列表投影去敏感字段;前端筛选栏/加载更多/回到第一页/分视图懒加载(不再全量 2s 拉取) | ✅ |
| M0-5 | 调度器续跑:task_nodes 持久化 retries/timeout_secs;pending_task_definitions;publisher→consumer 语义第一步(RDSCTL_RESUME_TASKS=1 时重启续跑,已完成节点不重跑、不覆盖输出) | ✅ |

## 2. 测试结果

### 2.1 单元/协议级测试(全部通过,`cargo test --bin rdsctl`)
- dag:依赖顺序 / 失败跳过下游 / 重试 / 取消 / **续跑只重跑 pending 节点**(12 项内)
- store(MemoryBackend 语义对齐):任务生命周期与视图 / mark_interrupted 与审计顺序 /
  实例记录 / **lease 互斥语义**(重入续期、非持有者释放无效、过期抢占)2 项
- 合成基准 2 项(见 §3)

### 2.2 P0 验收回归(全部通过,`cargo test --test acceptance`)
1. kill -9 重启:任务不丢(重启后列表可见、failed)、不重跑(容器 run 计数不变);
2. 并发操作被拒(creating/destroying 中 destroy/scaleout/重复 create 均 400);
3. 巡检发现异常:容器缺失/复制中断 → degraded+审计;恢复 → running+recover 审计。

> 注:M0-5 之后「续跑」为显式选项(默认仍 mark_interrupted),P0 kill-9 用例继续验证
> 默认语义;续跑语义由协议级单元测试覆盖。

## 3. 合成指标(内存后端,进程内;10k=单 shard 基线)

```
instance_upsert x10_000      : ~308 ms   (≈ 30.8µs/次 内存后端)
RdsManager 载入 10_000 行     : ~108 ms
list_filtered(region,page200): ~55 ms    (10k 内存筛选+投影;M1 换分片缓存后目标 <20ms)
5000×任务/节点/审计写          : ~25 ms   (≈5µs/次)
```

量级说明:内存后端代表「分片内常驻缓存」热路径上限估算;生产 MySQL CLI 后端单写
10–50ms/次(M0 现状,lab 规模可接受);**M1 换连接池后**以线上指标为准。列表接口已
服务端分页(默认 200/页,上限 1000),前端不再全量拉取——10k 级 UI 轮询成本由
「全量 JSON」降为「单页 + 筛选」,数量级与实例总数解耦。

## 4. 遗留与 M1 入口(按设计文档路线)

- MySQL CLI 每语句 spawn → 连接池(async driver,M1,组件决策=仅 MySQL+语言栈);
- Executor/Agent 抽象与双实现(本机 docker + k8s 占位)——M1;
- 分片缓存化 list 查询与 keyset 游标 —— M1;
- 审计分表+保留、事件驱动巡检、队列 worker 多副本 —— M1;
- 重启续跑经 RDSCTL_RESUME_TASKS 已可用(M0-5 语义),大规模自动续跑策略随队列
  claim/lease 一起在 M1 硬化。
