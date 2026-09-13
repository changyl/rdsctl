# 会话与 RBAC 入共识状态机(session-rbac-consensus)

> 目标(设计 §11.4 / 不变量 **C8**,验收 **I15/I16**):
> **任一网关副本可服务任意 Cookie**;冻结/改密/改权**对所有副本立即生效**。
> 现状(M1a 之前):会话是每副本进程内的 `DashMap`、用户/角色在 MySQL ——
> 于是同一 cookie 换一个副本就 401(负载均衡后面随机掉登录),冻结/改权只对处理该请求的副本生效。
> 状态:已实现并通过验收(`tests/acceptance.rs::cluster_session_and_rbac_are_consensus_authoritative`
> + `single_mode_session_semantics_unchanged`,真实 3 副本进程)。**单机模式行为逐字不变。**

---

## 1. 结论摘要

| 项 | 做法 | 为什么不那样做 |
|---|---|---|
| 权威在哪 | 会话/用户/角色全部进**共识状态机**的保留键空间 | 放 Redis/DB 会引入新的共识依赖与新的故障域;DB 只是投影(设计 §12) |
| 物理写入 | 复用带版本的 `Put{key,value,expect_ver}` / `Delete` | 新增 7 个 op 会动日志/快照格式与降级兼容面;CAS 也已由 `Put` 提供 |
| 会话内容 | `{user, epoch, issued_ms, expire_at_ms}`,键是 **token 的 SHA-256** | 明文 token 落盘 = 快照/日志泄露即拿到可用凭据 |
| 冻结/改密 | `u/<user>.epoch` **+1**,校验要求 `s.epoch == u.epoch` | "扫描并删除该用户全部会话"要 N 次提议且非原子,还会随会话数线性增长 |
| 登录 | read-index → 本地口令校验 → 写会话 → **等全部可达副本 apply** | 只读本地会吃旧口令(越权窗口);不等全副本就会出现"刚登录被踢回登录页" |
| 每请求 | 本地状态机读 + **认证读屏障** | 每请求 read-index 要多一个 RTT,把读 QPS 砍半 |
| 登出 | 新增 `POST /logout`(此前**全仓库没有**),撤销后等全部可达副本生效 | 只清浏览器 cookie 是假登出:服务端会话仍然可用 |
| 单机模式 | 完全走原路径(进程内 `DashMap` + 每请求查库) | 保证 `RDSCTL_MODE=single` 的行为逐字不变 |

---

## 2. 键空间与记录

占用的保留前缀(不与业务 KV 混用):

| key | 值 | 说明 |
|---|---|---|
| `r/<role>` | `{description, perms[], ver}` | 角色。`perms` 排序去重 ⇒ 记录确定性 |
| `u/<user>` | `{salt, pass_hash, enabled, roles[], epoch, ver}` | 用户。**口令摘要进日志**(见 §6 安全) |
| `s/<sha256(token)>` | `{user, epoch, issued_ms, expire_at_ms}` | 会话。只存哈希 |
| `a/hydrated` | `{at_ms}` | 首次从 sink 灌入 RBAC 的幂等标记 |

口令算法与既有 `src/store.rs` **逐字一致**:`sha256_hex(salt + ":" + pass)`。
因此从 sink 灌入的既有记录可直接使用,**不需要迁移口令或让所有人改密**。

权限展开也与既有语义一致:`super` 角色 = 权限目录全量;否则各角色并集。

---

## 3. 读写路径

### 3.1 写(经共识;任意副本可发起,自动转发 leader)

| 操作 | 记录变化 | epoch |
|---|---|---|
| 登录 | `Put s/<hash>` | 取当前 `u.epoch` |
| 登出 | `Delete s/<hash>`(幂等) | — |
| 建用户 / 改密 | `Put u/<user>` | **+1** |
| 冻结 / 解冻 | `Put u/<user>` | **+1** |
| 改用户角色 | `Put u/<user>` | 不变 |
| 建角色 / 改角色权限 | `Put r/<role>` | — |

- 用户/角色的读-改-写走 **CAS**(`expect_ver`)+ 有界重试(8 次):两个管理员同时改同一对象时,
  后者必然看到前者的结果,权限不会被静默回退。
- **所有认证写入在返回前等本副本 apply**(`await_local(index)`):
  否则会出现"在 n3 登录成功、紧接着访问 n3 自己却 401"、"n1 登出后 n1 自己仍认这个 cookie"
  —— 都是"leader 已提交 ≠ 本副本已生效"(设计 §19 发现 16/17 的同一类错误)。

### 3.2 登录(唯一需要线性一致读的路径)

```
① read_index   : 向 leader 取 commit_index,等本副本追平(避免用旧口令校验)
② 本地校验     : u/<user> 存在 + 口令匹配 + enabled
③ 写会话       : Put s/<sha256(token)>(明文 token 只在内存里,回给浏览器)
④ 等全副本可见 : wait_visible_on_all(index, 1.5s) —— 逐个成员问 /internal/commit-index,
                 等其 applied_index 覆盖该 index;不可达成员如实列出
```

- 引导未完成(库里还没有任何用户)时返回 **503「认证数据尚未就绪」**,
  **不是** 401「用户名或口令错误」——后者是在对一个还没加载完的库撒谎。
- **没有多数派就没有会话**:无 leader / leader 不可达 → **503 fail-closed**
  (`AuthError::Quorum`)。绝不能退化成"从 sink 读个密码就算登录成功":那会在无多数派时
  签发一个**谁都撤销不掉**的会话。

### 3.3 每请求(热路径,零共识往返)

```
cookie → sha256 → auth_barrier(200ms) → auth_session_lookup(本地状态机)
       → (user, perms) 注入 thread-local actor → 权限门禁 → 路由
```

- **认证读屏障**:`applied_index >= commit_index` 才允许判定;未追平 → **503 `auth_not_caught_up`**。
  宁可 503 也不接受一条可能已被撤销的会话(与 `/readyz` 同一取向)。
- 权限**每请求现算**(不固化进会话):改角色权限后,**已登录会话立即生效,无需重新登录**。

### 3.4 登出

`POST /logout`(任意登录用户可调):删除 `s/<hash>` → 等全部可达副本生效 → 清 cookie。
响应体的 `message` 会如实说明生效范围(例如「已生效 3 个副本」或「未确认:n3,恢复后生效」)。

---

## 4. 引导灌入(升级路径)

- 时机:cluster 模式启动后由 leader **尽快**执行(专用任务,300ms 重试),幂等标记 `a/hydrated`。
- 内容:`users`(含 salt/pass_hash)+ `roles/permissions`;**不含会话**。
- **会话不迁移**:切换到 cluster 模式后所有人需要重新登录一次(设计 §15 的既有口径)。
- 为什么需要:状态机初始为空,而用户/角色早已在 MySQL;不灌入 = 谁都登录不了。

---

## 5. 验收

`tests/acceptance.rs::cluster_session_and_rbac_are_consensus_authoritative`(真实 3 副本):

| # | 断言 |
|---|---|
| ① | 在 n1 登录 → 同一 cookie 在 n1/n2/n3 上**均 200**(修复前:换副本 401) |
| ② | 在 n2 建角色/用户/绑角色 → n3 上立刻可见,且权限按角色现算 |
| ③ | 该用户会话跨副本可用;权限确实生效(仅有 view 时 manage 端点 403) |
| ④ | 在 n2 改角色权限 → **不重新登录**,n1 上的已登录会话立即多出权限 |
| ⑤ | 在 n3 冻结 → 该用户**全部**会话在 3 个副本上全部 401(epoch 生效) |
| ⑥ | 改密后旧口令在所有副本失效、新口令生效 |
| ⑦ | 在 n1 登出 → 同一 cookie 在 3 个副本上全部 401 |
| ⑧ | 明文 token **不出现**在任何副本的日志/快照字节里(递归字节扫描) |

单机回归:`single_mode_session_semantics_unchanged`(会话在进程内、登出是真实撤销、无集群语义)。
单测:`ha::auth`(口令算法与 store 一致、`super` 展开、记录确定性、epoch 失效矩阵、只存哈希、
屏障判据),`i10_cluster_start_does_not_wipe_sink_locks_or_tasks` 断言**少数派下登录 503**。

---

## 6. 限制与安全边界(不粉饰)

1. **口令摘要进入共识日志与快照** ⇒ 所有副本都能读到摘要(以前只有 MySQL 有)。
   副本与 DB 同等可信,因此可接受,但必须在部署文档里写明;会话 token 只落哈希。
2. **屏障的能力边界**:它挡不住"**还没从心跳里获知**新 commit"的副本(那段窗口 ≤1 个心跳,
   默认 300ms)。因此:
   - **登录/登出**在写入侧等"全部可达副本已 apply" ⇒ 对可达副本窗口为零;
   - **其它**认证写入(建用户/改权)**不等**全副本 ⇒ 在最坏情况下另一个副本可能有 ≤1 心跳的
     陈旧权限视图。要彻底消除必须每请求 read-index(代价:每请求 +1 RTT),当前不接受。
3. **cluster 模式没有多数派就没有会话**(登录 503)。这是 C8 的直接推论,不是缺陷;
   单机模式不受影响。
4. **降级为仅探针时管理界面不可用**:sink 不可达时进程只起探针服务,登录页/管理 API 都不在,
   此时只能用 `/readyz` 观察 —— 与既有降级口径一致(M1a)。
5. **回滚跨版本**:旧二进制读新快照会忽略未知字段 ⇒ RBAC 视为空、需重新灌入(灌入标记也会被忽略),
   会话全失效。属 fail-closed,但**必须走停写流程**,不允许混合版本同时服务(设计 §15)。
6. 未做:`SessionRevokeUser`(被 epoch 方案取代)、会话列表的管理界面(后端已有
   `auth_sessions_view`)、按用户/来源 IP 的登录审计维度扩展。
