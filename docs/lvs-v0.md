# LVS 接入层(实例前置接入,进程内实现)

## 背景

实例详情「拓扑结构」把入口画成 `客户端 → LVS · 接入 VIP → Proxy 集群`。
本方案让这条链路真实可用,但不依赖任何第三方镜像:

> 离线/受限网络下无法拉取 haproxy/nginx 等转发镜像,而 rdsctl 运行宿主本身
> 就是 docker daemon 宿主。因此 LVS/VIP 落地为 **rdsctl 进程内 4 层 TCP 转发器**:
> 在宿主 `127.0.0.1:{lvs_mysql_port}` 监听(即实例 VIP),把接入连接轮询转发到
> 该实例**全部 Proxy 容器在宿主发布的端口**(`127.0.0.1:{mysql_port}`,docker 已映射
> 到容器 4051),后端故障自动跳下一个(round-robin)。

## 生命周期

- 创建实例 DAG 在 Proxy 集群启动后执行「启动 LVS 接入(VIP 转发 Proxy 集群)」(
  `Step::EnsureLvs`,幂等):绑定 VIP 端口并注册转发器,随后复制/连通校验经 VIP 验证;
- 实例记录持久化 `lvs = ["127.0.0.1:{lvs_mysql_port}"]`、
  `lvs_container = "rds-{name}-lvs"`(接入层标识)、`lvs_mysql_port`;
- 销毁 DAG 首步 `Step::StopLvs` 关闭转发器(先停接入再停代理/节点/网络);
- **进程重启恢复**:健康巡检(sweeper)每轮对 running/degraded 且登记过接入层的实例
  幂等重建转发器(已存在则跳过),VIP 在服务重启后自动恢复;
- 旧实例(无 `lvs_container`)兼容:连接信息回退直连首代理,无需改造。

## 展示与入口

- 概览「连接信息」入口显示 **LVS 接入 · rds-{name}-lvs**,复制/访问地址用 VIP 端口
  (标注 VIP);拓扑「LVS · VIP」展示真实地址;各 Proxy 端口保留为诊断直连。

## 边界(后续可演进)

- 接入层为进程内单点(与单进程控制端同生命周期,sweeper 自动恢复);
  生产化可演进为 keepalived 双机 VIP + ipvs,或独立接入网关;
- 转发器后端用 Proxy 宿主发布端口(单机 docker lab 语义),跨主机部署时改为
  Proxy 地址列表(字段同构,仅后端地址来源变化)。
