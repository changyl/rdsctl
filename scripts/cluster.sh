#!/usr/bin/env bash
# rdsctl 管控面集群(同机多副本)启停脚本
#
# 用途:在本机一键拉起 N 个 cluster 模式副本(+ 可选执行面 agent),用于开发与演练。
#       生产多机部署请用 deploy/systemd/rdsctl@.service 模板(见 deploy/README.md)。
#
# 用法:
#   ./scripts/cluster.sh up [--nodes 3] [--base-port 9113] [--with-agent] [--lab] [--sink mysql|none]
#   ./scripts/cluster.sh status
#   ./scripts/cluster.sh restart
#   ./scripts/cluster.sh down [--purge]
#   ./scripts/cluster.sh help
#
# 说明:
#   - 每个副本:公开端口 = base-port + i,RPC 端口 = base-port + 1000 + i,节点 id = n{i+1};
#     自增 id 便于 cluster 成员表(RDSCTL_CLUSTER)稳定;
#   - 每个副本独立数据目录 logs/ha/<node>(日志/快照/fence 状态不可共享);
#   - --lab:显式放行未验证前提(时钟/agent)并允许首启无 peer 可达。
#     放行后 /readyz 会标注 premises_unverified / lab_degraded,勿用于生产;
#   - 状态文件 logs/ha/cluster.state 记录本次拉起的节点/端口,status/down 依此操作。
#
# 正确性前提(见 docs/control-plane-ha-design.md §1.3):
#   A1 时钟同步 / A2 fsync 可信 / A3 voter 奇数 ≥3 / A4 执行面 agent / A5 进程守护

set -euo pipefail
# shellcheck source=scripts/lib.sh
. "$(dirname "$0")/lib.sh"

STATE_DIR="$ROOT/logs/ha"
STATE_FILE="$STATE_DIR/cluster.state"
NODES=3
BASE_PORT="${RDSCTL_PORT:-9113}"
WITH_AGENT=0
LAB=0
SINK="${RDSCTL_METADATA_SINK:-mysql}"
PURGE=0
PROFILE="${RDSCTL_PROFILE:-release}"

help() {
  cat <<EOF
rdsctl 集群(同机多副本)

子命令:
  up        拉起集群(默认 3 副本);参数: [--nodes N(奇数≥3)] [--base-port P]
            [--with-agent] [--lab] [--sink mysql|none] [--release|--debug|--lean]
  status    每个副本的 pid + /healthz + /readyz 摘要 + 当前 leader
  restart   全部重启(复用同一 node-id/端口/数据目录)
  down      停止全部副本(与 agent);--purge 连数据目录一起删
  help      显示本帮助

示例:
  ./scripts/cluster.sh up --lab                     # 3 副本(lab 放行,无 agent)
  ./scripts/cluster.sh up --nodes 3 --with-agent --lab   # 3 副本 + 本机 agent(A4 满足)
  ./scripts/cluster.sh status
  ./scripts/cluster.sh down

生产(多机)请用 systemd 模板:deploy/systemd/rdsctl@.service + /etc/rdsctl/<node>.env
一键端到端演练(起集群→验选举/租约/fence→停集群):./scripts/ha-drill.sh
EOF
}

CMD="${1:-status}"
shift || true
while [ $# -gt 0 ]; do
  case "$1" in
    --nodes) NODES="${2:?}"; shift 2 ;;
    --base-port) BASE_PORT="${2:?}"; shift 2 ;;
    --sink) SINK="${2:?}"; shift 2 ;;
    --with-agent) WITH_AGENT=1; shift ;;
    --lab) LAB=1; shift ;;
    --debug) PROFILE=debug; shift ;;
    --lean | --debug-lean | --dev-lean) PROFILE=dev-lean; shift ;;
    --release) PROFILE=release; shift ;;
    --purge) PURGE=1; shift ;;
    -h | --help | help) help; exit 0 ;;
    *) die "未知参数: $1(用 --help 查看用法)" ;;
  esac
done

case "$CMD" in
  -h | --help | help) help; exit 0 ;;
esac

bin_for_profile() {
  case "$PROFILE" in
    debug) echo "$ROOT/target/debug/rdsctl" ;;
    dev-lean) echo "$ROOT/target/dev-lean/rdsctl" ;;
    *) echo "$ROOT/target/release/rdsctl" ;;
  esac
}

AGENT_PORT=$((BASE_PORT + 300))
RPC_BASE=$((BASE_PORT + 1000))

cluster_spec() {
  local i out=""
  for ((i = 0; i < NODES; i++)); do
    out+="n$((i + 1))@127.0.0.1:$((RPC_BASE + i))"
    [ $i -lt $((NODES - 1)) ] && out+=","
  done
  printf '%s' "$out"
}

write_state() {
  mkdir -p "$STATE_DIR"
  {
    echo "# 由 scripts/cluster.sh 生成:本次拉起的集群拓扑"
    echo "NODES=$NODES"
    echo "BASE_PORT=$BASE_PORT"
    echo "RPC_BASE=$RPC_BASE"
    echo "SPEC=$(cluster_spec)"
    echo "AGENT_PORT=$([ "$WITH_AGENT" = 1 ] && echo "$AGENT_PORT" || echo 0)"
    echo "SINK=$SINK"
    echo "LAB=$LAB"
    echo "PROFILE=$PROFILE"
  } >"$STATE_FILE"
}

load_state() {
  [ -f "$STATE_FILE" ] || die "未找到集群状态文件 $STATE_FILE(先执行 ./scripts/cluster.sh up)"
  # shellcheck disable=SC1090
  . "$STATE_FILE"
}

# 为第 i 个节点导出环境(供 rdsctl.sh 使用)
node_env() {
  local i="$1"
  export RDSCTL_MODE=cluster
  export RDSCTL_NODE_ID="n$((i + 1))"
  export RDSCTL_PORT=$((BASE_PORT + i))
  export RDSCTL_RPC_PORT=$((RPC_BASE + i))
  export RDSCTL_CLUSTER="$(cluster_spec)"
  export RDSCTL_METADATA_SINK="$SINK"
  export RDSCTL_DATA_DIR="$STATE_DIR/$RDSCTL_NODE_ID"
  export RDSCTL_BIN="$(bin_for_profile)"
  # 演练用较短节拍(生产用默认值即可)
  export RDSCTL_ELECTION_TIMEOUT_MS="${RDSCTL_ELECTION_TIMEOUT_MS:-800}"
  export RDSCTL_HA_TICK_MS="${RDSCTL_HA_TICK_MS:-50}"
  if [ "$LAB" = 1 ]; then
    export RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK=1
    export RDSCTL_PREFLIGHT_ALLOW_BOOTSTRAP=1
    [ "$WITH_AGENT" = 1 ] || export RDSCTL_ALLOW_NO_AGENT=1
  fi
  if [ "$WITH_AGENT" = 1 ]; then
    export RDSCTL_AGENT_URL="http://127.0.0.1:$AGENT_PORT"
    : "${RDSCTL_AGENT_TOKEN:=lab-token}"
    export RDSCTL_AGENT_TOKEN
  fi
  # cluster 模式下本机也走 agent(A4/设计 §9.1);未起 agent 时由 lab 放行标记
}

start_agent() {
  local bin="$1" pidf="$LOG_DIR/rdsctl-agent-$AGENT_PORT.pid" logf="$LOG_DIR/rdsctl-agent-$AGENT_PORT.log"
  if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
    info "agent 已在运行(pid=$(cat "$pidf"),port=$AGENT_PORT)"
    return 0
  fi
  info "启动执行面 agent → 127.0.0.1:$AGENT_PORT"
  nohup "$bin" agent --port "$AGENT_PORT" --token "$RDSCTL_AGENT_TOKEN" \
    >>"$logf" 2>&1 &
  echo $! >"$pidf"
  # 探测必须带 token(agent 配了 token 时无 token 会返回 403)
  local ping="http://127.0.0.1:$AGENT_PORT/agent/ping"
  [ -n "${RDSCTL_AGENT_TOKEN:-}" ] && ping="$ping?token=$RDSCTL_AGENT_TOKEN"
  local i=0
  while [ $i -lt 40 ]; do
    if curl -s -m 2 "$ping" 2>/dev/null | grep -q '"ok":true'; then
      ok "agent 就绪:$(curl -s -m 2 "$ping")"
      return 0
    fi
    sleep 0.25
    i=$((i + 1))
  done
  die "agent 10s 内未就绪,日志: $logf"
}

stop_agent() {
  local pidf="$LOG_DIR/rdsctl-agent-$AGENT_PORT.pid"
  [ -f "$pidf" ] || return 0
  local pid
  pid="$(cat "$pidf")"
  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    sleep 0.5
    kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
    ok "agent 已停止(port=$AGENT_PORT)"
  fi
  rm -f "$pidf"
}

leader_of() { # 复用 lib.sh 的共享实现(按 RDSCTL_CLUSTER 成员表探测)
  cluster_leader
}

wait_leader() {
  local deadline=$((SECONDS + ${1:-30}))
  local l
  while [ $SECONDS -lt $deadline ]; do
    if l="$(leader_of)"; then
      printf '%s' "$l"
      return 0
    fi
    sleep 0.5
  done
  return 1
}

do_up() {
  # agent token 必须先定下来:`:-` 只做取值不赋值,若此处不设置,
  # 后面 start_agent 的就绪探测会因"变量为空 → 探测不带 token"而被 403 误判失败。
  if [ "$WITH_AGENT" = 1 ]; then
    : "${RDSCTL_AGENT_TOKEN:=lab-token}"
    export RDSCTL_AGENT_TOKEN
  fi
  [ "$NODES" -ge 3 ] || die "--nodes 至少为 3(多数派仲裁)"
  [ $((NODES % 2)) -eq 1 ] || die "--nodes 必须为奇数(当前 $NODES)"
  local bin
  bin="$(bin_for_profile)"
  [ -x "$bin" ] || die "编译产物不存在: $bin
  先执行: ./scripts/build.sh            (release)
        : ./scripts/build.sh --debug-lean  (开发演练,--lean)"
  [ "$SINK" = "none" ] || mysql_alive || warn "MySQL 未就绪:cluster 模式仍可启动,但实例生命周期 API 不可用(降级为仅探针)"
  info "集群拓扑:$NODES 副本,spec=$(cluster_spec),公开端口 $BASE_PORT..$((BASE_PORT + NODES - 1))"
  [ "$LAB" = 1 ] && warn "--lab:已放行未验证前提(时钟/bootstrap),适用开发演练,勿用于生产"
  mkdir -p "$STATE_DIR"
  write_state
  [ "$WITH_AGENT" = 1 ] && start_agent "$bin"
  local i
  for ((i = 0; i < NODES; i++)); do
    node_env "$i"
    # rdsctl.sh 会再读一次环境并调用 cluster_validate
    (cd "$ROOT" && ./scripts/rdsctl.sh start)
  done
  if ! wait_leader 40 >/dev/null; then
    warn "40s 内未选出 leader:请查看 $LOG_DIR/rdsctl-n*.log 与 ./scripts/cluster.sh status"
    return 1
  fi
  ok "集群就绪:leader=$(wait_leader 5 || echo '?'),副本数=$NODES"
  do_status
}

do_status() {
  load_state
  local i pidf st
  for ((i = 0; i < NODES; i++)); do
    local node="n$((i + 1))" pub=$((BASE_PORT + i)) rpc=$((RPC_BASE + i))
    pidf="$LOG_DIR/rdsctl-$node.pid"
    if [ -f "$pidf" ] && kill -0 "$(cat "$pidf")" 2>/dev/null; then
      printf '%s  pid=%s  public=%s  rpc=%s\n' "$node" "$(cat "$pidf")" "$pub" "$rpc"
      printf '   /healthz: %s\n' "$(curl -s -m 2 "http://127.0.0.1:$pub/healthz" 2>/dev/null || echo '<不可达>')"
      printf '   /readyz : %s\n' "$(curl -s -m 3 "http://127.0.0.1:$pub/readyz" 2>/dev/null || echo '<不可达>')"
    else
      printf '%s  未运行(pid 文件 %s)\n' "$node" "$pidf"
    fi
  done
  if st="$(leader_of)"; then
    ok "当前 leader: $st"
  else
    warn "当前无 leader(可能正在选举或已失去多数派)"
  fi
}

do_restart() {
  load_state
  local i
  for ((i = 0; i < NODES; i++)); do
    node_env "$i"
    (cd "$ROOT" && ./scripts/rdsctl.sh restart)
  done
  do_status
}

do_down() {
  if [ ! -f "$STATE_FILE" ]; then
    warn "未找到 $STATE_FILE:尝试停掉所有 rdsctl-*.pid 记录的实例"
    local f
    for f in "$LOG_DIR"/rdsctl-*.pid; do
      [ -e "$f" ] || continue
      kill -TERM "$(cat "$f")" 2>/dev/null || true
      rm -f "$f"
    done
    ok "已停止(通配清理)"
    return 0
  fi
  load_state
  local i
  for ((i = 0; i < NODES; i++)); do
    node_env "$i"
    (cd "$ROOT" && ./scripts/rdsctl.sh stop) || true
  done
  [ "${AGENT_PORT:-0}" != 0 ] && stop_agent
  if [ "$PURGE" = 1 ]; then
    info "--purge:删除集群数据目录 $STATE_DIR/n*"
    rm -rf "$STATE_DIR"/n[0-9]*
    rm -f "$STATE_FILE"
  fi
  ok "集群已停止"
}

case "$CMD" in
  up) do_up ;;
  status) do_status ;;
  restart) do_restart ;;
  down) do_down ;;
  *) die "未知子命令: $CMD(用 --help 查看用法)" ;;
esac
