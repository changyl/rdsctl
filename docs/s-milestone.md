# S-安全 / S-告警 / S-批量 里程碑说明(迁移与验收)

> 依据"管控优先保障安全与可控"的六点要求中已批准的 S-安全、S-告警、S-批量三批。
> 结论:**后端 + 前端均落地并验证;原有 15 单元 + 3 验收全绿;部署冒烟通过。**

## 1. 迁移(数据库 v3)

启动自动幂等(已有库无需手工):

| 表 | 说明 |
|---|---|
| `users` | 用户名/口令盐/哈希(salt:pass SHA-256)/启用位/时间 |
| `roles` / `user_roles` / `role_perms` | 角色、用户-角色、角色-权限 |
| `alerts` | 告警:instance/kind/severity/status/assignee/handled_at/resolved_at |

种子:由 `RDSCTL_USER` / `RDSCTL_PASS` 指定的管理员(具体值见部署配置,不入文档)
+ 内置 `super`(自动展开全部权限)。**首次登录后立即改密**。
`instances` 新增语义字段 `enabled`(JSON 内,serde 默认 true,旧记录兼容)。

## 2. 权限目录(12 项,前端权限树同源)

instances.view / create / destroy / scaleout / manage(启停·批量);tasks.view / cancel;
audit.view;alerts.view / handle;users.manage;roles.manage

## 3. 关键接口

- 会话:`POST /login`(查库)、`GET /api/auth/me`;每请求冻结校验(即时 403)
- RBAC:`GET/POST /api/rds/users|roles`、`GET /api/rds/permissions`
- 实例启停/批量:`POST /api/rds/enable?name&value`;`POST /api/rds/batch?action=destroy|enable|disable|scaleout_read|scaleout_offline&names=…`(逐实例 `{name,ok,message}`)
- 告警:`GET /api/rds/alerts?severity=&status=&instance=`;`POST /api/rds/alert?id&action=ack|resolve`(记录处理人/时间);`/api/rds/summary.alerts.{open,critical,warn,info}`
- 审计:交互操作记录真实登录用户;异步任务记 system、巡检记 sweeper

## 4. 前端(统一状态色与高危交互)

- 新增页签:告警(分级过滤/处理/解决)、用户与权限(用户管理:新建/冻结解冻/角色/重置密码,不可冻结自己;角色与权限:按组勾选权限树)
- 概览:未处理告警**置顶红/橙醒目入口**(点击跳告警),异常实例清单与进行中任务保留
- 实例卡片:多选复选 + 批量条(启用/停用/销毁,按权限点亮);停用标签;权限不足按钮不展示
- 高危操作统一红字弹窗:单实例销毁要求**输入实例名**;批量销毁要求输入短语 `确认销毁N个实例`;执行前不可点确认
- 导航与按钮按 `/api/auth/me` 权限动态裁剪(审计/告警/用户页签等)

## 5. 验收证据

- 自动化:`cargo test --offline` = 15 单元(含 sha256 向量、RBAC/告警内存后端语义、离线扩容唯一、10k 合成)+ 3 P0 验收(kill-9 不丢不重跑/并发拒绝/巡检降级恢复)
- 冒烟(已跑,见过程记录):admin 登录 → me(12 权限);创建 `viewer` 角色(instances.view,audit.view)+ 用户 → 只读列表 200、`destroy` 403"权限不足:需要 instances.destroy";冻结 viewer → 重新登录 403、**旧会话立即 403**;解冻恢复;批量接口对不存在实例逐条返回失败原因;页面已含 alerts/users/batchbar/riskmodal 等视图
- 部署:release 构建 + `./scripts/deploy.sh` 健康检查通过(:9113)

## 6. 说明与边界

- 会话仍为进程内存(单机);冻结/权限变更对已登录会话即时生效(逐请求校验启用位),角色权限变更对已登录会话在登录时快照——如需即时回收,请用户重新登录(M1 会话落库后按需刷新快照)
- 告警事件源:巡检降级(degrade 按消息含"复制中断"升 critical,其余 warn)、生命周期任务失败(info);恢复/销毁成功自动关闭;人工处理=ack(认领人/时间),解决=resolve(解决人/时间)
- 停用(enable=false)语义=管理暂停:仍运行,拒绝扩容等变更;允许启用与销毁清理
- UI 高危弹窗使用原生确认框之上的统一红字组件;批量销毁在输入短语前不可执行
