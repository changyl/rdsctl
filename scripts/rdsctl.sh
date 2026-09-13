#!/usr/bin/env bash
# rdsctl 服务(HTTP 管控进程)启停脚本
#
# 用法:
#   ./scripts/rdsctl.sh start     启动(确保编译产物与 MySQL 可用)
#   ./scripts/rdsctl.sh stop      优雅停止(超时强杀)
#   ./scripts/rdsctl.sh restart
#   ./scripts/rdsctl.sh status    进程 + HTTP 健康检查
#   ./scripts/rdsctl.sh logs      tail 运行日志
#   ./scripts/rdsctl.sh foreground  前台运行(调试用)
#   ./scripts/rdsctl.sh help      显示本帮助
#
# 配置:端口/凭据/MySQL 连接均可由 rdsctl.env 或环境变量覆盖(见 scripts/lib.sh)。

set -euo pipefail
# shellcheck source=scripts/lib.sh
. "$(dirname "$0")/lib.sh"

help() {
  cat <<EOF
rdsctl 服务(HTTP 管控进程)管理

子命令:
  start        启动(后台,nohup;pid=logs/rdsctl.pid,日志=logs/rdsctl.log)
               - 自动检查编译产物(RDSCTL_BIN 可覆盖)与 MySQL 可达性
               - 就绪判定:登录接口返回 200(最多等 30s)
  stop         优雅停止(SIGTERM,15s 超时后 SIGKILL)
  restart      等价 stop + start
  status       进程 + HTTP 健康检查(退出码 0=正常)
  logs         tail -f 运行日志(TAIL_LINES 控制初始行数,默认 200)
  foreground   前台运行(调试;Ctrl-C 即停)
  help         显示本帮助

当前生效:
  模式     : $RDSCTL_MODE  (RDSCTL_MODE=single|cluster)
  端口     : $RDSCTL_PORT  (RDSCTL_PORT)
  页面     : http://127.0.0.1:$RDSCTL_PORT/rds  ($RDSCTL_USER / ***)
  二进制   : \${RDSCTL_BIN:-$ROOT/target/release/rdsctl}
  日志/PID : $LOG_FILE / $PID_FILE
  MySQL    : $RDSCTL_MYSQL_USER@$RDSCTL_MYSQL_HOST:$RDSCTL_MYSQL_PORT/$RDSCTL_MYSQL_DB
  PID/日志键: $(instance_key)   (single=端口;cluster=节点 id)
  rdsctl 参数: $(service_args)

集群(cluster)模式:需先设置 RDSCTL_MODE=cluster、RDSCTL_NODE_ID、
RDSCTL_CLUSTER=id@ip:port,...、RDSCTL_RPC_PORT;就绪判定改用 /healthz,
status 额外展示 /readyz(ready/quorum_ok/leader/premises)。同机多副本:
  ./scripts/cluster.sh up --nodes 3 --lab      # 一键起 3 副本(+ 可选 agent)
  详见 docs/ops-guide-cluster.md 与 deploy/README.md

示例:
  ./scripts/rdsctl.sh start
  ./scripts/rdsctl.sh status && curl -s -X POST 'http://127.0.0.1:$RDSCTL_PORT/login?user=$RDSCTL_USER&password=$RDSCTL_PASS'
  RDSCTL_BIN=./target/debug/rdsctl ./scripts/rdsctl.sh restart
  RDSCTL_SKIP_MYSQL_CHECK=1 ./scripts/rdsctl.sh start   # 跳过 MySQL 预检(如连外部库由应用自检)
EOF
}

CMD="${1:-status}"
RDSCTL_BIN="${RDSCTL_BIN:-$ROOT/target/release/rdsctl}"
cluster_validate
# pid/日志按"实例键"区分:single=端口,cluster=节点 id(同机多副本各一套,互不误杀)
INSTANCE_KEY="$(instance_key)"
PID_FILE="$LOG_DIR/rdsctl-${INSTANCE_KEY}.pid"
LOG_FILE="$LOG_DIR/rdsctl-${INSTANCE_KEY}.log"
URL="http://127.0.0.1:$RDSCTL_PORT"

case "$CMD" in
  -h | --help | help)
    help
    exit 0
    ;;
esac

require_bin() {
  [ -x "$RDSCTL_BIN" ] || die "编译产物不存在: $RDSCTL_BIN
  先执行: ./scripts/build.sh   (或设置 RDSCTL_BIN 指向已编译的 rdsctl)"
}

pid_alive() {
  [ -f "$PID_FILE" ] || return 1
  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || echo '')"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

port_in_use() {
  (exec 3<>"/dev/tcp/127.0.0.1/$RDSCTL_PORT") >/dev/null 2>&1
}

http_ok() {
  if [ "$RDSCTL_MODE" = "cluster" ]; then
    # 集群:公开端口只提供探针/完整 API(取决于 sink),用 /healthz 判"进程可用";
    # /readyz 的语义(ready/quorum/premises)留给 status 展示,不作为启动门槛 ——
    # lab 放行下 premises 未验证会让 ready=false,但那不是"起不来"。
    [ "$(http_code /healthz)" = "200" ]
    return
  fi
  # 单机:登录接口 200 即认为服务就绪(既有行为)
  command -v curl >/dev/null 2>&1 || return 1
  curl -s -m 2 -o /dev/null -w '%{http_code}' \
    -X POST "$URL/login?user=$RDSCTL_USER&password=$RDSCTL_PASS" 2>/dev/null | grep -q '^200$'
}

do_start() {
  require_bin
  if pid_alive; then
    warn "已在运行 (pid=$(cat "$PID_FILE")),跳过启动;如需重启: ./scripts/rdsctl.sh restart"
    return 0
  fi
  if port_in_use; then
    die "端口 $RDSCTL_PORT 已被占用;停止旧实例或修改 RDSCTL_PORT"
  fi
  if [ "${RDSCTL_SKIP_MYSQL_CHECK:-0}" != "1" ]; then
    if mysql_alive; then
      :
    elif [ "$RDSCTL_MODE" = "cluster" ] && [ "$RDSCTL_METADATA_SINK" != "mysql" ]; then
      :   # cluster + sink=none:本就不需要控制库
    elif [ "$RDSCTL_MODE" = "cluster" ]; then
      warn "MySQL 未就绪(${RDSCTL_MYSQL_HOST}:${RDSCTL_MYSQL_PORT}):cluster 模式仍会启动,\n  但会降级为'仅探针与内部 RPC'(共识/租约不受影响,实例生命周期 API 暂不可用);\n  要完整功能: ./scripts/mysql.sh start"
    else
      die "MySQL 未就绪(${RDSCTL_MYSQL_HOST}:${RDSCTL_MYSQL_PORT})
  先执行: ./scripts/mysql.sh start   (或配置 RDSCTL_MYSQL_* 指向可用实例)"
    fi
  fi
  local ARGS=()
  # shellcheck disable=SC2206
  read -r -a ARGS <<<"$(service_args)"
  if [ "$RDSCTL_MODE" = "cluster" ]; then
    info "启动 rdsctl[cluster:$RDSCTL_NODE_ID] → $URL (rpc ${RDSCTL_RPC_PORT:-9330},数据目录 ${RDSCTL_DATA_DIR:-?})"
  else
    info "启动 rdsctl → $URL (pid 文件 $PID_FILE,日志 $LOG_FILE)"
  fi
  export_rdsctl_env
  nohup "$RDSCTL_BIN" "${ARGS[@]}" >>"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"
  # 等待就绪(cluster: /healthz;single: 登录 200)
  local i=0
  while [ $i -lt 60 ]; do
    if ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
      die "进程提前退出,请查看日志: tail -50 $LOG_FILE"
    fi
    if http_ok; then
      if [ "$RDSCTL_MODE" = "cluster" ]; then
        ok "rdsctl[cluster:$RDSCTL_NODE_ID] 进程就绪: $URL (role=$(readyz_field role),leader=$(readyz_field leader),ready=$(readyz_field ready))"
      else
        ok "rdsctl 就绪: $URL (页面 /rds,$RDSCTL_USER/****)"
      fi
      return 0
    fi
    sleep 0.5
    i=$((i + 1))
  done
  die "服务 30s 内未就绪(端口可能被占用或 MySQL 连不上),日志: $LOG_FILE"
}

do_stop() {
  if ! pid_alive; then
    warn "未在运行"
    rm -f "$PID_FILE"
    return 0
  fi
  local pid
  pid="$(cat "$PID_FILE")"
  info "停止 rdsctl (pid=$pid)"
  kill -TERM "$pid" 2>/dev/null || true
  local i=0
  while [ $i -lt 30 ] && kill -0 "$pid" 2>/dev/null; do sleep 0.5; i=$((i + 1)); done
  if kill -0 "$pid" 2>/dev/null; then
    warn "SIGTERM 超时,强杀"
    kill -9 "$pid" 2>/dev/null || true
    sleep 0.5
  fi
  rm -f "$PID_FILE"
  ok "已停止"
}

case "$CMD" in
  start)
    do_start
    ;;
  stop)
    do_stop
    ;;
  restart)
    do_stop || true
    do_start
    ;;
  status)
    if pid_alive; then
      echo "rdsctl 运行中: pid=$(cat "$PID_FILE")"
      echo "  URL   : $URL (页面 /rds)"
      echo "  二进制: $RDSCTL_BIN"
      echo "  日志  : $LOG_FILE"
      if [ "$RDSCTL_MODE" = "cluster" ]; then
        echo "  模式  : cluster (node=$RDSCTL_NODE_ID, rpc=${RDSCTL_RPC_PORT:-9330})"
        echo "  数据  : ${RDSCTL_DATA_DIR:-未设置}"
        if [ "$(http_code /healthz)" = "200" ]; then
          echo "  /healthz: $(curl -s -m 2 "$URL/healthz")"
          echo "  /readyz : $(readyz_json)"
          ok "进程存活;就绪判定见上面 readyz(ready/quorum_ok/premises_unverified)"
          exit 0
        else
          warn "进程在但 /healthz 不可达(可能仍在启动),日志: $LOG_FILE"
          exit 1
        fi
      fi
      if http_ok; then
        ok "HTTP 健康检查通过($RDSCTL_USER 登录 200)"
        exit 0
      else
        warn "进程在但 HTTP 未就绪(可能仍在启动或端口被占),日志: $LOG_FILE"
        exit 1
      fi
    else
      echo "rdsctl 未运行 (pid 文件: $PID_FILE)"
      exit 1
    fi
    ;;
  logs)
    exec tail -n "${TAIL_LINES:-200}" -f "$LOG_FILE"
    ;;
  foreground)
    require_bin
    if [ "$RDSCTL_MODE" != "cluster" ]; then
      mysql_alive || warn "MySQL 未就绪,启动可能会失败"
    fi
    export_rdsctl_env
    read -r -a ARGS <<<"$(service_args)"
    exec "$RDSCTL_BIN" "${ARGS[@]}"
    ;;
  *)
    echo "用法: $0 {start|stop|restart|status|logs|foreground}" >&2
    exit 2
    ;;
esac
