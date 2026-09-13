#!/usr/bin/env bash
# rdsctl 管控面集群端到端演练(起集群 → 验证共识/租约/fence/多数派 → 停集群)
#
# 用途:一条命令证明"集群模式真的能工作",并作为变更后的回归演练。
#       对应验收文档 docs/control-plane-ha-acceptance.md 的 F1/F2/I1/I3/I5 场景
#       (真实多进程版本;更严苛的用例在 tests/ha_cluster.rs 里)。
#
# 用法:
#   ./scripts/ha-drill.sh                 # 3 副本 + 本机 agent + lab 放行(默认)
#   ./scripts/ha-drill.sh --nodes 5       # 5 副本
#   ./scripts/ha-drill.sh --keep          # 演练结束**不**停集群(便于手工观察)
#   ./scripts/ha-drill.sh --release       # 用 release 产物(默认 dev-lean)
#
# 前置:MySQL 可选(sink);lab 放行意味着 A1 时钟未由 NTP 证明(A4 由本机 agent 满足),
#       /readyz 会如实标注 premises_unverified —— 这是演练,不是生产验收。

set -uo pipefail
# shellcheck source=scripts/lib.sh
. "$(dirname "$0")/lib.sh"

NODES=3
KEEP=0
PROFILE="dev-lean"
LEASE_INST="drill-db-$$"
# 租约时间戳用**真实** epoch-ms,而不是 at_ms=0:
# 内部入口(/internal/propose)不做 at_ms 归一化,填 0 会得到"1970 年就过期"的租约 ——
# 而 M1a 收尾加入了过期租约 GC(leader 每 5s 回收),这类条目会被当垃圾清掉,
# 使本演练变成"跑得比 GC 快才通过"的时序竞态。填真实时间后租约是新鲜的,
# 冲突授予依然会被拒(理由从"旧租约未安全过期"变为"旧租约仍在有效期内"),语义不变。
_now_ms() { python3 -c 'import time;print(int(time.time()*1000))'; }
NOW_MS="$(_now_ms)"
while [ $# -gt 0 ]; do
  case "$1" in
    --nodes) NODES="${2:?}"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    --release) PROFILE=release; shift ;;
    --lean) PROFILE=dev-lean; shift ;;
    -h | --help | help)
      sed -n '2,20p' "$0"
      exit 0
      ;;
    *) die "未知参数: $1" ;;
  esac
done

LOG="$ROOT/logs/ha-drill.log"
mkdir -p "$ROOT/logs"
: >"$LOG"
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
fail() {
  say "FAIL: $*"
  exit 1
}
pass() { say "PASS: $*"; }

json() { # 用法: json <json串> <python 表达式,变量名 d>
  python3 -c "import sys,json;d=json.loads(sys.argv[1]);print($2)" "$1"
}

cleanup() {
  if [ "$KEEP" = 0 ]; then
    say "-- 收尾:停止集群"
    "$ROOT/scripts/cluster.sh" down >>"$LOG" 2>&1 || true
  else
    say "-- --keep:集群保持运行(./scripts/cluster.sh down 可停)"
  fi
}
trap cleanup EXIT

say "== 0. 起集群($NODES 副本 + agent,profile=$PROFILE,lab 放行)=="
case "$PROFILE" in
  release) PROFILE_FLAG="--release" ;;
  debug) PROFILE_FLAG="--debug" ;;
  *) PROFILE_FLAG="--lean" ;;
esac
"$ROOT/scripts/cluster.sh" up --nodes "$NODES" --with-agent --lab "$PROFILE_FLAG" >>"$LOG" 2>&1 ||
  fail "cluster.sh up 失败,详见 $LOG"
# shellcheck disable=SC1090
. "$ROOT/logs/ha/cluster.state"
export RDSCTL_CLUSTER="$SPEC"   # 供 lib.sh 的 cluster_leader 使用
STATE_DIR="$ROOT/logs/ha"       # 与 cluster.sh 一致(set -u 下必须先定义再引用)
BASE="$BASE_PORT"
RPC="$RPC_BASE"
say "集群拓扑:nodes=$NODES public=$BASE.. rpc=$RPC.. leader=$(cluster_leader || echo '?')"

# ── 1. 就绪与前提(诚实上报)──
say "== 1. 每个副本 /readyz(ready / quorum_ok / premises_ok)=="
i=0
all_ready=1
while [ $i -lt "$NODES" ]; do
  r="$(curl -s -m 3 "http://127.0.0.1:$((BASE + i))/readyz" 2>/dev/null || true)"
  [ -n "$r" ] || fail "n$((i + 1)) 的 /readyz 不可达"
  ready="$(json "$r" "d['ready']")"
  q="$(json "$r" "d['quorum_ok']")"
  prem="$(json "$r" "d['premises_ok']")"
  say "   n$((i + 1)): ready=$ready quorum_ok=$q premises_ok=$prem role=$(json "$r" "d['role']")"
  [ "$ready" = "True" ] || all_ready=0
  i=$((i + 1))
done
[ "$all_ready" = 1 ] || fail "存在未就绪副本"
[ "$(json "$(curl -s -m 3 "http://127.0.0.1:$BASE/readyz")" "d['premises_ok']")" = "True" ] ||
  say "   注意:premises_ok=False(演练环境无 NTP / 或无 agent)→ lab_degraded=True,勿视为生产就绪"
pass "全部副本就绪且多数派健康"

# ── 2. 单写者与 fence:只在 leader 生效,跨副本可读 ──
say "== 2. 共识租约(单写者 + fence 单调 + 跨副本可读)=="
L1="$(cluster_leader)" || fail "未选出 leader"
L1_IDX=0
for ((k = 0; k < NODES; k++)); do [ "n$((k + 1))" = "$L1" ] && L1_IDX=$k; done
L1_RPC=$((RPC + L1_IDX))
OTHER_IDX=$(((L1_IDX + 1) % NODES))
OTHER_RPC=$((RPC + OTHER_IDX))

op="{\"op\":\"lease_grant\",\"instance\":\"$LEASE_INST\",\"holder\":\"h1\",\"ttl_ms\":30000,\"at_ms\":$NOW_MS}"
resp="$(curl -s -m 5 -X POST "http://127.0.0.1:$L1_RPC/internal/propose" -d "$op")"
[ "$(json "$resp" "d.get('applied',{}).get('status')")" = "applied" ] ||
  fail "向 leader($L1) 提案租约失败:$resp"
lease_of() { # 用法: lease_of <rpc端口> <字段>;未复制到则打印 <无>
  curl -s -m 3 "http://127.0.0.1:$1/internal/lease?instance=$LEASE_INST" 2>/dev/null |
    python3 -c "import sys,json
try: print(json.load(sys.stdin)['lease']['$2'])
except Exception: print('<无>')"
}
fence1="$(lease_of "$L1_RPC" fence)"
# 跨副本读:**轮询等待**(follower 通过日志复制异步追上,不能假设立即可见)
holder_other="<无>"
deadline=$((SECONDS + 10))
while [ $SECONDS -lt $deadline ]; do
  holder_other="$(lease_of "$OTHER_RPC" holder)"
  [ "$holder_other" = "h1" ] && break
  sleep 0.3
done
[ "$holder_other" = "h1" ] || fail "10s 内其它副本未看到该租约(holder=$holder_other)"
pass "租约 holder=h1,fence=$fence1,且**另一副本**读到了同一租约"

say "   冲突授予应被拒(旧租约未过期)"
op2="{\"op\":\"lease_grant\",\"instance\":\"$LEASE_INST\",\"holder\":\"h2\",\"ttl_ms\":30000,\"at_ms\":$NOW_MS}"
resp2="$(curl -s -m 5 -X POST "http://127.0.0.1:$L1_RPC/internal/propose" -d "$op2")"
st2="$(json "$resp2" "d.get('applied',{}).get('status')")"
[ "$st2" = "rejected" ] || fail "冲突授予未被拒:$resp2"
pass "冲突授予被拒($(json "$resp2" "d['applied']['reason']" | head -c 60)…)"

say "   续约后 fence 必须前移(单调)"
curl -s -m 5 -X POST "http://127.0.0.1:$L1_RPC/internal/propose" \
  -d "{\"op\":\"lease_renew\",\"instance\":\"$LEASE_INST\",\"holder\":\"h1\",\"at_ms\":$(_now_ms)}" >>"$LOG" 2>&1
fence2="$(lease_of "$L1_RPC" fence)"
[ "$fence1" != "$fence2" ] || fail "续约后 fence 未变化($fence1)"
pass "fence 单调:$fence1 → $fence2"

# ── 3. F1:kill -9 leader → 失效转移,状态不丢 ──
say "== 3. F1:kill -9 leader($L1) → 其余节点接管 =="
pidf="$LOG_DIR/rdsctl-$L1.pid"
[ -f "$pidf" ] || fail "未找到 $L1 的 pid 文件 $pidf"
kill -9 "$(cat "$pidf")" 2>/dev/null || true
rm -f "$pidf"
L2=""
deadline=$((SECONDS + 30))
while [ $SECONDS -lt $deadline ]; do
  if L2="$(cluster_leader)"; then [ "$L2" != "$L1" ] && break; fi
  sleep 0.5
done
[ -n "$L2" ] && [ "$L2" != "$L1" ] || fail "杀掉 leader 后 30s 内未选出新 leader"
pass "失效转移成功:新 leader=$L2(旧=$L1)"

L2_IDX=0
for ((k = 0; k < NODES; k++)); do [ "n$((k + 1))" = "$L2" ] && L2_IDX=$k; done
prev_holder=""
deadline=$((SECONDS + 10))
while [ $SECONDS -lt $deadline ]; do
  prev_holder="$(lease_of "$((RPC + L2_IDX))" holder)"
  [ "$prev_holder" = "h1" ] && break
  sleep 0.3
done
[ "$prev_holder" = "h1" ] || fail "新 leader 上看不到已提交的租约(holder=$prev_holder)"
pass "已提交状态在新 leader 上仍可见(holder=h1)"

say "   把被杀的 $L1 拉起(等价守护 Restart=always)"
"$ROOT/scripts/cluster.sh" restart >>"$LOG" 2>&1 || fail "cluster.sh restart 失败"
sleep 2
curl -s -m 3 "http://127.0.0.1:$BASE/healthz" | grep -q '"ok":true' || fail "重启后 n1 /healthz 不可达"
pass "被杀的副本已恢复并重新加入"

# ── 4. F2:失多数派 → 停写;恢复多数派 → 自愈 ──
say "== 4. F2:停掉 $((NODES - 1)) 个副本 → 少数派必须停写 =="
# 保留当前 leader,其余全部停掉(用 stop 保留 pid 与数据目录,便于随后拉起)。
# 注意:必须在**此刻**重新解析 leader —— 上一步的 restart 可能已经换主,
# 若沿用旧值就会把真 leader 停掉、留下一个跟随者(它只会回 409 not_leader)。
CUR_L="$(cluster_leader)" || fail "停副本前无法解析当前 leader"
keep_idx=0
found=0
for ((k = 0; k < NODES; k++)); do
  if [ "n$((k + 1))" = "$CUR_L" ]; then
    keep_idx=$k
    found=1
  fi
done
[ "$found" = 1 ] || fail "当前 leader=$CUR_L 不在成员表内"
say "   保留 leader=$CUR_L(n$((keep_idx + 1))),停掉其余 $((NODES - 1)) 个副本"
i=0
while [ $i -lt "$NODES" ]; do
  if [ $i -ne $keep_idx ]; then
    (RDSCTL_MODE=cluster RDSCTL_NODE_ID="n$((i + 1))" RDSCTL_PORT=$((BASE + i)) \
      RDSCTL_RPC_PORT=$((RPC + i)) RDSCTL_CLUSTER="$SPEC" \
      RDSCTL_DATA_DIR="$STATE_DIR/n$((i + 1))" \
      "$ROOT/scripts/rdsctl.sh" stop) >>"$LOG" 2>&1 || true
  fi
  i=$((i + 1))
done
# 等 quorum 判定窗口过期(窗口 = max(4×心跳, 3×选举超时),演练环境约 2.4s)
sleep 4
readyz="$(curl -s -m 3 "http://127.0.0.1:$((BASE + keep_idx))/readyz" 2>/dev/null || true)"
q="$(json "$readyz" "d['quorum_ok']" 2>/dev/null || echo unknown)"
if [ "$q" != "False" ]; then
  say "   诊断:readyz=$readyz"
  say "   诊断:残留进程=$(pgrep -fl 'rdsctl serve' | tr '\n' ' ')"
  fail "失去多数派后 quorum_ok 仍为 $q"
fi
op3="{\"op\":\"lease_grant\",\"instance\":\"solo-$$\",\"holder\":\"x\",\"ttl_ms\":30000,\"at_ms\":$(_now_ms)}"
code="$(curl -s -m 5 -o /dev/null -w '%{http_code}' -X POST \
  "http://127.0.0.1:$((RPC + keep_idx))/internal/propose" -d "$op3")"
[ "$code" = "503" ] || fail "少数派提案应 fail-closed(503),实际 HTTP $code"
pass "失去多数派:quorum_ok=False 且写请求 503(绝不"本地写入成功")"

say "   拉起其余副本 → 应自愈(进程不重启)"
"$ROOT/scripts/cluster.sh" restart >>"$LOG" 2>&1 || fail "cluster.sh restart 失败"
deadline=$((SECONDS + 30))
healed=0
while [ $SECONDS -lt $deadline ]; do
  if L3="$(cluster_leader)"; then
    L3_IDX=0
    for ((k = 0; k < NODES; k++)); do [ "n$((k + 1))" = "$L3" ] && L3_IDX=$k; done
    r="$(curl -s -m 3 "http://127.0.0.1:$((BASE + L3_IDX))/readyz" 2>/dev/null || true)"
    [ "$(json "$r" "d['quorum_ok']" 2>/dev/null || echo False)" = "True" ] && {
      healed=1
      break
    }
  fi
  sleep 1
done
[ "$healed" = 1 ] || fail "恢复多数派后未自愈"
pass "自愈成功:leader=$(cluster_leader),quorum_ok=True"

say "══════════ 演练结果:PASS(全部检查通过)══════════"
say "日志:$LOG;集群日志:$LOG_DIR/rdsctl-n*.log"
