# 外部驱动(把 rdsctl 接到任意容器平台)

这里存放**外部驱动**:一个可执行文件,让 rdsctl 在不重编译的前提下管理
k8s / OpenStack / 自研平台上的 MySQL 工作负载。

- 契约与设计:`docs/container-platform-abstraction.md` §5
- 接入步骤与排障:`docs/ops-guide-container-platform.md` §2

## 目录内容

| 文件 | 说明 |
|---|---|
| `k8s-driver.sh` | Kubernetes **参考实现**(kubectl)。演示契约;未在真实集群验证,也未做生产硬化 |
| `README.md` | 本文件:契约速查 + 自测方法 |

## 契约速查

一次调用 = 一个子进程:stdin 一行 JSON 请求,stdout 一行 JSON 响应,退出码 0。

```bash
echo '{"action":"caps"}' | ./k8s-driver.sh
# {"ok":true,"caps":{"host_ports":false,"raw_flags":false,...}}

echo '{"action":"create","spec":{"name":"t1","image":"mysql:8.0","envs":[],"command":[]}}' | ./k8s-driver.sh
# {"ok":true,"out":"..."}
```

请求字段:`action` + 各 action 的载荷(`spec` / `id` / `argv` / `name` / `path` / `content` / `tail` / `purge`)
+ `meta:{fence,idem}`(变更类原语携带)。

响应字段:

| 字段 | 含义 |
|---|---|
| `ok` | `true` = 成功 |
| `out` | 成功输出(stdout 文本) |
| `err` / `code` | `exec_raw` 用:stderr 与退出码 |
| `error` | 失败原因(直接展示给使用者) |
| `code` / `cap` | `"unsupported"` + 能力名 → 控制面还原为 `unsupported capability: <cap>` |
| `exists` | `exists` action 的结果(bool) |
| `state` | `"Status\|ExitCode\|RestartCount"` 或对象 `{status,exit_code,restarts,health}` |
| `health` | `"healthy" \| "none" \| "starting" \| "unhealthy"` |
| `caps` | `caps` action 的能力声明 |
| `addr` | `expose` action 的接入点 `ip:port` |

## 三条硬要求

1. **总回一行 JSON**:任何内部错误都要转成 `{"ok":false,...}`,不要让脚本 panic /
   打印堆栈(控制面会报「响应非 JSON」,难以定位)。
2. **能力缺失显式报错**:`{"ok":false,"error":"...","code":"unsupported","cap":"host_ports"}`,
   不要静默忽略(静默忽略 = 线上数据面悄悄少一半)。
3. **`spec.extra` 非空即拒绝**:那是控制面无法结构化的 docker 参数,驱动猜不得。

## 安全

- 绝对路径、属主为控制面/agent 运行用户、权限不含 other-write(`0755`)。启动自检会强制。
- 驱动以控制面**同等权限**运行(等价 `docker.sock`):只部署在可信内网。
- 平台凭据放驱动侧(环境变量/凭据文件),不要让控制面持有。

## 自测(写驱动时)

```bash
chmod 755 my-driver.sh
# 1) 能力
echo '{"action":"caps"}' | ./my-driver.sh
# 2) 创建 → 观察平台侧对象真的出现
echo '{"action":"create","spec":{"name":"t1","image":"mysql:8.0","envs":[{"key":"K","value":"V"}],"command":[]}}' | ./my-driver.sh
# 3) 之后接控制面自测见 docs/ops-guide-container-platform.md §2.4
```

`k8s-driver.sh` 的自测(无需真实集群,用假 kubectl 验证协议管道):

```bash
mkdir -p /tmp/fake-bin && printf '#!/bin/sh\nexit 0\n' > /tmp/fake-bin/kubectl && chmod 755 /tmp/fake-bin/kubectl
PATH=/tmp/fake-bin:$PATH echo '{"action":"caps"}' | ./k8s-driver.sh
```

## 为什么优先外部驱动而不是改 rdsctl

| | 外部驱动 | 内置 driver |
|---|---|---|
| 需要重编译控制面 | 否 | 是 |
| 平台凭据位置 | 平台侧 | 控制面进程内 |
| 发布/升级 | 平台方独立 | 跟 rdsctl 版本绑定 |
| 适合 | 接入验证、异构/自研平台、多团队 | 长期主用平台的性能与可观测优化 |

内置 driver 的写法:`impl crate::exec::WorkloadRuntime`,在 `crate::exec::runtime_for_kind`
注册一个 kind;`create` 第一行必须 `crate::exec::check_caps(self, spec)?`。
