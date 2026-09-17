#!/bin/sh
# rdsctl 外部驱动 —— Kubernetes 参考实现(kubectl)
#
# 定位:**契约参考实现**,演示「平台方写一个 shim 即可接入,不必改/重编译 rdsctl」。
#   - 工作负载模型:StatefulSet(稳定名 / 有序 / 可用 replicas 表达 启停)
#   - 网络模型:docker 网络名 → k8s Namespace
#   - 端口模型:k8s 无宿主端口 → 用 Service 暴露,caps.host_ports=false
#   - 卷模型:命名卷 → volumeClaimTemplate;宿主路径 → hostPath
#   - 未做生产硬化:资源 limits、亲和/反亲和、PDB、PVC 容量与 StorageClass 选择、
#     访问方式(Ingress/LoadBalancer)等,均需按实际集群补齐。
#
# 契约见 docs/container-platform-abstraction.md §5;用法见 docs/ops-guide-container-platform.md §2。
#
# 环境变量:
#   RDSCTL_K8S_NAMESPACE_DEFAULT  默认命名空间(无 spec.network 时;默认 rdsctl)
#   RDSCTL_K8S_STORAGE_CLASS      卷模板 StorageClass(空 = 集群默认)
#   RDSCTL_K8S_IMAGE_PULL_SECRET  imagePullSecrets 名称(可选)
#   RDSCTL_K8S_MOUNT_SIZE         命名卷模板容量(默认 10Gi)
#   KUBECTL                       kubectl 可执行(默认 kubectl)
#
# 安全:必须以控制面/agent 相同权限运行,只应部署在可信内网。

set -eu

KUBECTL="${KUBECTL:-kubectl}"
NS_DEFAULT="${RDSCTL_K8S_NAMESPACE_DEFAULT:-rdsctl}"
STORAGE_CLASS="${RDSCTL_K8S_STORAGE_CLASS:-}"
PULL_SECRET="${RDSCTL_K8S_IMAGE_PULL_SECRET:-}"

exec python3 -c "$(cat <<'PY'
import json, os, subprocess, sys

KUBECTL = os.environ.get("KUBECTL", "kubectl")
NS_DEFAULT = os.environ.get("RDSCTL_K8S_NAMESPACE_DEFAULT", "rdsctl")
STORAGE_CLASS = os.environ.get("RDSCTL_K8S_STORAGE_CLASS", "")
PULL_SECRET = os.environ.get("RDSCTL_K8S_IMAGE_PULL_SECRET", "")
MOUNT_STORAGE = os.environ.get("RDSCTL_K8S_MOUNT_SIZE", "10Gi")


def emit(obj):
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
    sys.stdout.flush()
    sys.exit(0)


def ok(out=""):
    emit({"ok": True, "out": out})


def fail(msg, code="platform", cap=""):
    emit({"ok": False, "error": str(msg), "code": code, "cap": cap})


def unsupported(cap, msg):
    fail(msg, code="unsupported", cap=cap)


def run(args, stdin=None, check=True):
    """执行命令;返回 (rc, stdout, stderr)"""
    p = subprocess.run(
        [KUBECTL] + args,
        input=stdin,
        capture_output=True,
        text=True,
    )
    if check and p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout).strip())
    return p.returncode, p.stdout, p.stderr


def ns_of(spec):
    return (spec.get("network") or NS_DEFAULT).strip() or NS_DEFAULT


def labels(spec):
    return {"app.kubernetes.io/managed-by": "rdsctl", "rdsctl.io/workload": spec["name"]}


# ── manifest 渲染 ──────────────────────────────────────────────────────────

def render_sts(spec):
    name = spec["name"]
    ns = ns_of(spec)
    container = {"name": "main", "image": spec["image"]}
    if spec.get("envs"):
        container["env"] = [{"name": e["key"], "value": e.get("value", "")} for e in spec["envs"]]
    if spec.get("command"):
        container["args"] = list(spec["command"])
    if spec.get("entrypoint"):
        unwrap = spec["entrypoint"].split()
        container["command"] = unwrap
    ports = []
    for p in spec.get("ports", []):
        ports.append({"containerPort": p["container_port"], "protocol": (p.get("proto") or "tcp").upper()})
    if ports:
        container["ports"] = ports

    volumes, mounts, claims = [], [], []
    for i, m in enumerate(spec.get("mounts", [])):
        vname = "vol%d" % i
        if m.get("kind") == "host_path":
            volumes.append({"name": vname, "hostPath": {"path": m["source"]}})
        else:
            # 命名卷 → volumeClaimTemplate(持久化;PVC 名为 <模板名>-<工作负载>-0)
            claim = {
                "metadata": {"name": vname},
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": MOUNT_STORAGE}},
                },
            }
            if STORAGE_CLASS:
                claim["spec"]["storageClassName"] = STORAGE_CLASS
            claims.append(claim)
        mounts.append({"name": vname, "mountPath": m["target"], "readOnly": bool(m.get("read_only"))})
    if mounts:
        container["volumeMounts"] = mounts

    pod = {"containers": [container]}
    if spec.get("hostname"):
        pod["hostname"] = spec["hostname"]
    # 只有 hostPath 用显式 volume;命名卷由 volumeClaimTemplates 提供(Pod 不需引用)
    if volumes:
        pod["volumes"] = volumes
    if PULL_SECRET:
        pod["imagePullSecrets"] = [{"name": PULL_SECRET}]

    sts_spec = {
        "serviceName": name,
        "replicas": 1,
        "selector": {"matchLabels": labels(spec)},
        "template": {"metadata": {"labels": labels(spec)}, "spec": pod},
    }
    if claims:
        sts_spec["volumeClaimTemplates"] = claims

    return {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": name, "namespace": ns, "labels": labels(spec)},
        "spec": sts_spec,
    }


def render_svc(spec):
    ports = [
        {"name": "p%d" % p["container_port"], "port": p["container_port"], "targetPort": p["container_port"],
         "protocol": (p.get("proto") or "tcp").upper()}
        for p in spec.get("ports", [])
    ]
    if not ports:
        return None
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": spec["name"], "namespace": ns_of(spec), "labels": labels(spec)},
        "spec": {"selector": labels(spec), "ports": ports},
    }


def apply(manifest):
    rc, out, err = run(["apply", "-f", "-"], stdin=json.dumps(manifest))
    return out


def jload(out, what):
    """解析 kubectl 输出:失败时给出结构化错误而不是抛异常(驱动必须总能回一行 JSON)"""
    try:
        return json.loads(out)
    except Exception as e:
        fail("kubectl %s 输出非 JSON: %s" % (what, e))


# ── action 分发 ────────────────────────────────────────────────────────────

def main():
    line = sys.stdin.readline()
    if not line.strip():
        fail("空请求")
    try:
        req = json.loads(line)
    except Exception as e:
        fail("请求非 JSON: %s" % e)
    action = req.get("action", "")
    spec = req.get("spec") or {}
    wid = req.get("id") or spec.get("name") or ""

    try:
        if action == "caps":
            emit({"ok": True, "caps": {
                "host_ports": False,      # k8s 无宿主端口语义(用 Service/Ingress 暴露)
                "bind_mounts": True,      # hostPath
                "named_volumes": True,    # volumeClaimTemplate
                "networks": True,         # Namespace
                "exec": True,             # kubectl exec
                "logs": True,
                "rename": False,          # StatefulSet 名不可变
                "systemd_unit": False,
                "raw_flags": False,       # 未知 docker 参数一律拒绝
            }})

        if spec.get("extra"):
            unsupported("docker_flag", "本驱动无法透传参数 %s" % spec["extra"])

        if action == "create":
            ns = ns_of(spec)
            run(["create", "namespace", ns], check=False)
            apply(render_sts(spec))
            svc = render_svc(spec)
            if svc:
                apply(svc)
            ok("statefulset %s/%s 已应用" % (ns, spec["name"]))

        if action == "start":
            run(["-n", ns_of(spec), "scale", "statefulset", wid, "--replicas=1"])
            ok()

        if action == "stop":
            run(["-n", ns_of(spec), "scale", "statefulset", wid, "--replicas=0"])
            ok()

        if action == "restart":
            run(["-n", ns_of(spec), "rollout", "restart", "statefulset", wid])
            ok()

        if action == "remove":
            ns = ns_of(spec)
            run(["-n", ns, "delete", "statefulset", wid, "--ignore-not-found"])
            run(["-n", ns, "delete", "service", wid, "--ignore-not-found"])
            if req.get("purge"):
                # 连同数据卷清除(docker rm -f -v 语义)
                for i in range(16):
                    run(["-n", ns, "delete", "pvc", "vol%d-%s-0" % (i, wid), "--ignore-not-found"])
            ok()

        if action == "rename":
            unsupported("rename", "StatefulSet 名称不可变;k8s 请用新名创建后切换")

        if action == "exists":
            rc, _o, _e = run(["-n", ns_of(spec), "get", "statefulset", wid], check=False)
            emit({"ok": True, "exists": rc == 0})

        if action == "state":
            rc, out, _e = run(["-n", ns_of(spec), "get", "statefulset", wid, "-o", "json"], check=False)
            if rc != 0:
                emit({"ok": True, "state": None})
            st = jload(out, "get statefulset")
            want = st.get("spec", {}).get("replicas", 0)
            ready = st.get("status", {}).get("readyReplicas", 0) or 0
            emit({"ok": True, "state": "%s|0|0" % ("running" if ready > 0 else ("exited" if want == 0 else "pending")),
                  "health": ("healthy" if ready > 0 else ("none" if want == 0 else "starting"))})

        if action == "health":
            rc, out, _e = run(["-n", ns_of(spec), "get", "statefulset", wid, "-o", "json"], check=False)
            if rc != 0:
                emit({"ok": True, "health": None})
            st = jload(out, "get statefulset")
            ready = st.get("status", {}).get("readyReplicas", 0) or 0
            emit({"ok": True, "health": "healthy" if ready > 0 else "starting"})

        if action == "logs":
            tail = int(req.get("tail") or 40)
            try:
                _, out, _ = run(["-n", ns_of(spec), "logs", "statefulset/" + wid, "--tail=%d" % tail])
            except RuntimeError:
                out = ""
            ok(out.strip())

        if action == "exec":
            argv = req.get("argv") or []
            _, out, _ = run(["-n", ns_of(spec), "exec", "statefulset/" + wid, "--"] + argv)
            ok(out.strip())

        if action == "exec_raw":
            argv = req.get("argv") or []
            rc, out, err = run(["-n", ns_of(spec), "exec", "statefulset/" + wid, "--"] + argv, check=False)
            emit({"ok": True, "out": out.strip(), "err": err.strip(), "code": rc})

        if action == "write_file":
            path = req.get("path", "")
            cmd = ["-n", ns_of(spec), "exec", "-i", "statefulset/" + wid, "--", "sh", "-c", "cat > %s" % path]
            p = subprocess.run([KUBECTL] + cmd, input=req.get("content", ""), capture_output=True, text=True)
            if p.returncode != 0:
                fail((p.stderr or p.stdout).strip())
            ok()

        if action == "network_ensure":
            run(["create", "namespace", req.get("name", "")], check=False)
            ok()

        if action == "network_remove":
            run(["delete", "namespace", req.get("name", ""), "--ignore-not-found"])
            ok()

        if action == "expose":
            ns = ns_of(spec)
            rc, out, _e = run(["-n", ns, "get", "service", spec.get("name", ""), "-o", "json"], check=False)
            if rc != 0:
                unsupported("host_ports", "Service 不存在,无法给出接入点")
            svc = jload(out, "get service")
            ip = (svc.get("spec", {}).get("clusterIP") or "").strip()
            ports = svc.get("spec", {}).get("ports") or []
            if not ip or not ports:
                unsupported("host_ports", "Service 无 ClusterIP/端口")
            emit({"ok": True, "addr": "%s:%d" % (ip, ports[0]["port"])})

        fail("未支持的 action: %s" % action, code="unsupported", cap="platform")

    except RuntimeError as e:
        fail(str(e))


main()
PY
)"
