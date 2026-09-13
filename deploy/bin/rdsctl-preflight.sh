#!/usr/bin/env bash
# rdsctl 启动前自检(preflight)—— 正确性前提的可执行门禁
#
# 依据:docs/control-plane-ha-design.md §1.3(前提 A1–A6)与 §16(M1a 门禁)。
# 用法:
#   deploy/bin/rdsctl-preflight.sh <node-id> [env-file ...]
#   # systemd 场景由 EnvironmentFile 注入环境,无需手工传 env-file
#
# 退出码约定(与 rdsctl@.service 的 RestartPreventExitStatus=2 对齐):
#   0 = 前提满足,可启动
#   2 = 正确性前提不达标 → 拒绝启动(时钟失同步 / fsync 不可信 / 无法形成多数派 /
#       集群配置非法 / cluster 模式下执行面 agent 不可达)
#   3 = 用法或配置错误(缺参数、文件不可读、变量缺失)
#
# 说明:cluster 模式的**进程内**等价自检由 M1a 在 rdsctl 内实现(见设计文档 §16),
#       语义与退出码与本站脚本保持一致;本脚本负责部署层门禁与集群首启校验。

set -euo pipefail

NODE_ID="${1:-}"
if [ -n "$NODE_ID" ]; then shift; fi

FAILED=0
WARNED=0

say()  { printf '%s\n' "$*"; }
ok()   { printf '  [OK]   %s\n' "$*"; }
warn() { printf '  [WARN] %s\n' "$*"; WARNED=$((WARNED + 1)); }
fail() { printf '  [FAIL] %s\n' "$*"; FAILED=$((FAILED + 1)); }

# ── 加载配置文件(仅手工运行时需要;systemd 已注入环境) ──
for f in "$@"; do
  if [ -r "$f" ]; then
    # shellcheck disable=SC1090
    set -a; . "$f"; set +a
    say "已加载配置:$f"
  else
    say "配置不可读:$f" >&2
    exit 3
  fi
done
if [ "$#" -eq 0 ]; then
  for f in /etc/rdsctl/rdsctl.env "/etc/rdsctl/${NODE_ID}.env"; do
    if [ -n "$NODE_ID" ] && [ -r "$f" ]; then
      # shellcheck disable=SC1090
      set -a; . "$f"; set +a
      say "已加载配置:$f"
    fi
  done
fi

MODE="${RDSCTL_MODE:-single}"
NODE="${RDSCTL_NODE_ID:-${NODE_ID:-node}}"
DATA_DIR="${RDSCTL_DATA_DIR:-$PWD/logs/ha/$NODE}"
CLUSTER="${RDSCTL_CLUSTER:-}"
MAX_SKEW_MS="${RDSCTL_MAX_SKEW_MS:-1000}"
AGENT_URL="${RDSCTL_AGENT_URL:-}"
PUBLIC_PORT="${RDSCTL_PORT:-9113}"

say "rdsctl 启动前自检:node=$NODE mode=$MODE data_dir=$DATA_DIR"
say "----------------------------------------------------------------"

# ── 1. 数据目录:可写 + fsync 可信(A2) + 非网络文件系统 ──
say "1) 本地存储与 fsync(A2)"
if ! mkdir -p "$DATA_DIR" 2>/dev/null; then
  fail "数据目录不可创建:$DATA_DIR(检查属主/权限)"
elif [ ! -w "$DATA_DIR" ]; then
  fail "数据目录不可写:$DATA_DIR"
else
  ok "数据目录可写:$DATA_DIR"
  fstype="$(stat -f -c %T "$DATA_DIR" 2>/dev/null || stat -f %T "$DATA_DIR" 2>/dev/null || echo unknown)"
  case "$fstype" in
    *nfs*|*cifs*|*smbfs*|*smb*|*afp*|*fuse*|*webdav*)
      fail "数据目录位于网络文件系统($fstype):fsync 语义不可信,禁止承载共识日志" ;;
    unknown)
      warn "无法判定文件系统类型(stat 不支持);请人工确认 $DATA_DIR 在本地盘" ;;
    *)
      ok "文件系统类型:$fstype(本地盘)" ;;
  esac
  probe="$DATA_DIR/.fsync-probe.$$"
  if command -v python3 >/dev/null 2>&1; then
    if python3 - "$probe" <<'PY' 2>/dev/null
import os, sys
p = sys.argv[1]
fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o600)
os.write(fd, b"fsync-probe")
os.fsync(fd)
os.close(fd)
os.unlink(p)
PY
    then ok "fsync 探测通过(python3 os.fsync)"; else fail "fsync 探测失败:$DATA_DIR 写入或落盘不可信"; fi
  elif printf 'fsync-probe' > "$probe" 2>/dev/null && sync "$probe" 2>/dev/null; then
    rm -f "$probe"; ok "fsync 探测通过(sync 回退)"
  else
    rm -f "$probe" 2>/dev/null || true
    warn "无 python3/sync 可用,未能验证 fsync;cluster 模式请人工确认落盘可信"
  fi
fi

# ── 2. 时钟同步(A1) ──
say "2) 时钟同步(A1,max_skew=${MAX_SKEW_MS}ms)"
clock_state="unknown"
clock_detail=""
if command -v chronyc >/dev/null 2>&1; then
  ct="$(chronyc tracking 2>/dev/null || true)"
  leap="$(printf '%s\n' "$ct" | awk -F': *' '/Leap status/{print $2}')"
  syst="$(printf '%s\n' "$ct" | awk -F': *' '/System time/{print $2}' | awk '{print $1}')"
  if [ -n "$leap" ]; then
    clock_state="$([ "$leap" = "Normal" ] && echo synced || echo unsynced)"
    clock_detail="chronyc Leap=$leap System time=${syst}s"
  fi
fi
if [ "$clock_state" = "unknown" ] && command -v timedatectl >/dev/null 2>&1; then
  ntp="$(timedatectl show -p NTPSynchronized --value 2>/dev/null || true)"
  if [ -n "$ntp" ]; then
    clock_state="$([ "$ntp" = "yes" ] && echo synced || echo unsynced)"
    clock_detail="timedatectl NTPSynchronized=$ntp"
  fi
fi
if [ "$clock_state" = "unknown" ] && command -v ntpq >/dev/null 2>&1; then
  if ntpq -p >/dev/null 2>&1; then clock_state=synced; clock_detail="ntpq 可达(未取到偏移值)"; fi
fi
case "$clock_state" in
  synced)   ok "时钟已同步($clock_detail;请另配合偏移监控,P99 偏移须 ≤ ${MAX_SKEW_MS}ms)" ;;
  unsynced) if [ "$MODE" = "cluster" ]; then
              if [ "${RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK:-0}" = "1" ]; then
                warn "时钟未同步($clock_detail),但已按 RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK=1 放行:前提 A1 未满足,仅限 lab/演练,不可用于生产"
              else
                fail "时钟未同步($clock_detail):cluster 模式要求 NTP/chrony 同步(A1)"
              fi
            else
              warn "时钟未同步($clock_detail):single 模式可继续,但不可进入 cluster"
            fi ;;
  *)        if [ "$MODE" = "cluster" ]; then
              if [ "${RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK:-0}" = "1" ]; then
                warn "无法判定时钟同步状态(本机无 chronyc/timedatectl/ntpq),已按 RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK=1 放行:前提 A1 **未验证**,仅限 lab/演练"
              else
                fail "无法判定时钟同步状态:cluster 模式必须可验证 A1(安装 chrony/ntp 并上报偏移;lab 可用 RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK=1 显式放行)"
              fi
            else
              warn "无法判定时钟同步状态(single 模式可继续)"
            fi ;;
esac

# ── 3. 集群配置合法性(A3:奇数 voter ≥3) ──
say "3) 集群配置(A3)"
VOTERS=0
if [ "$MODE" = "cluster" ]; then
  if [ -z "$CLUSTER" ]; then
    fail "RDSCTL_CLUSTER 未设置:cluster 模式必填(id@ip:port,...)"
  else
    IFS=',' read -r -a entries <<< "$CLUSTER"
    VOTERS="${#entries[@]}"
    dup="$(printf '%s\n' "${entries[@]}" | awk -F'@' '{print $1}' | sort | uniq -d | tr '\n' ' ')"
    if [ -n "$dup" ]; then fail "集群成员 id 重复:$dup"; fi
    if [ $((VOTERS % 2)) -eq 0 ]; then fail "voter 数为偶数($VOTERS):必须为奇数(3 或 5)"; fi
    if [ "$VOTERS" -lt 3 ]; then fail "voter 数不足($VOTERS):至少 3 个才有多数派容错"; fi
    bad=""
    for e in "${entries[@]}"; do
      case "$e" in *@*:*) ;; *) bad="$bad $e" ;; esac
    done
    if [ -n "$bad" ]; then fail "成员格式非法(应为 id@ip:port):$bad"; else ok "成员格式合法,voter 数=$VOTERS"; fi
  fi
else
  ok "single 模式:跳过集群校验(不参与多数派仲裁)"
fi

# ── 4. 多数派可达(A3/C2:启动时必须能凑齐多数派) ──
tcp_probe() { # host port → 0/1
  local h="$1" p="$2"
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$h" "$p" <<'PY' >/dev/null 2>&1
import socket, sys
s = socket.socket(); s.settimeout(2)
try:
    s.connect((sys.argv[1], int(sys.argv[2])))
except Exception:
    sys.exit(1)
finally:
    s.close()
PY
  elif command -v curl >/dev/null 2>&1; then
    curl -s -m 2 -o /dev/null "telnet://$h:$p" 2>/dev/null
  else
    (exec 3<>"/dev/tcp/$h/$p") 2>/dev/null
  fi
}

if [ "$MODE" = "cluster" ] && [ "${VOTERS:-0}" -ge 3 ]; then
  say "4) 多数派可达(C2)"
  majority=$((VOTERS / 2 + 1))
  need_others=$((majority - 1))
  reach=0
  for e in "${entries[@]}"; do
    id="${e%%@*}"; addr="${e#*@}"; h="${addr%:*}"; p="${addr##*:}"
    if [ "$id" = "$NODE" ]; then continue; fi
    if tcp_probe "$h" "$p"; then
      reach=$((reach + 1)); ok "peer 可达:$id($h:$p)"
    else
      warn "peer 不可达:$id($h:$p)(若为首次引导可忽略,滚动升级期间属预期)"
    fi
  done
  if [ "$reach" -lt "$need_others" ]; then
    if [ "${RDSCTL_PREFLIGHT_ALLOW_BOOTSTRAP:-0}" = "1" ]; then
      warn "可达 peer 数 $reach < 所需的 $need_others:已按 RDSCTL_PREFLIGHT_ALLOW_BOOTSTRAP=1 放行(仅用于**首次引导**,逐个拉起节点时 peer 尚未就绪)"
    else
      fail "可达 peer 数 $reach < 所需的 $need_others(无法与自身构成多数派 $majority/$VOTERS):拒绝启动
     若这是首次引导(逐个拉起节点),可显式设置 RDSCTL_PREFLIGHT_ALLOW_BOOTSTRAP=1"
    fi
  else
    ok "可达 peer 数 $reach ≥ $need_others:可与自身构成多数派"
  fi
else
  if [ "$MODE" != "cluster" ]; then
    say "4) 多数派可达:跳过(single 模式)"
  else
    say "4) 多数派可达:跳过(集群配置非法,已在第 3 项报错)"
  fi
fi

# ── 5. 执行面 agent 可达(A4:fence 强制点) ──
say "5) 执行面 agent(A4)"
if [ -z "$AGENT_URL" ] && [ -n "${RDSCTL_AGENT_PORT:-}" ]; then
  AGENT_URL="http://127.0.0.1:${RDSCTL_AGENT_PORT}"
fi
if [ "$MODE" = "cluster" ]; then
  if [ -z "$AGENT_URL" ]; then
    fail "cluster 模式必须配置 RDSCTL_AGENT_URL(或 RDSCTL_AGENT_PORT):无 agent 则 fence 无法在资源侧强制(A4)"
  elif command -v curl >/dev/null 2>&1; then
    # 带 token 探测:agent 配置 RDSCTL_AGENT_TOKEN 时,无 token 会返回 403(误判为不可达)
    ping_url="${AGENT_URL}/agent/ping"
    if [ -n "${RDSCTL_AGENT_TOKEN:-}" ]; then
      ping_url="${ping_url}?token=${RDSCTL_AGENT_TOKEN}"
    fi
    body="$(curl -s -m 3 "$ping_url" 2>/dev/null || true)"
    case "$body" in
      *'"ok":true'*)
        case "$body" in
          *fence_capable*true*) ok "agent 可达且声明 fence_capable:true($AGENT_URL)" ;;
          *) warn "agent 可达但未声明 fence_capable($AGENT_URL):M1a 落地前属预期,该状态不满足 G1(fence 未强制)" ;;
        esac ;;
      *) fail "agent 不可达或响应异常:$AGENT_URL(A4 要求执行面在线)" ;;
    esac
  else
    warn "无 curl,未能验证 agent 可达性;请人工确认 $AGENT_URL"
  fi
else
  if [ -n "$AGENT_URL" ]; then ok "agent 已配置:$AGENT_URL(单机模式可选)"; else ok "single 模式:本机直连 docker,agent 可选"; fi
fi

# ── 6. 公开端口占用 ──
say "6) 公开端口"
if command -v curl >/dev/null 2>&1 && curl -s -m 2 -o /dev/null "http://127.0.0.1:${PUBLIC_PORT}/login" 2>/dev/null; then
  warn "端口 ${PUBLIC_PORT} 已有服务响应(可能是本机既有 rdsctl 实例;多节点部署请分配不同端口)"
else
  ok "端口 ${PUBLIC_PORT} 未被占用"
fi

say "----------------------------------------------------------------"
if [ "$FAILED" -gt 0 ]; then
  say "自检结果:拒绝启动($FAILED 项前提不达标,$WARNED 项告警) —— 参见 docs/control-plane-ha-design.md §1.3 与 deploy/README.md"
  exit 2
fi
say "自检结果:通过($WARNED 项告警)"
exit 0
