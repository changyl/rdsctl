#!/usr/bin/env bash
# rdsctl MySQL 实例管理
#
# 两种模式(自动识别,无需配置):
#   1. 外部模式:配置的 RDSCTL_MYSQL_HOST:PORT 上已有可用的 MySQL —— 脚本只做连通/建库,
#      不触碰任何本机数据目录。
#   2. 捆绑模式:仓库内数据目录 .rdsctl-mysql(可用 RDSCTL_MYSQL_DATA_DIR 覆盖),
#      首次自动 --initialize-insecure,之后 start/stop/restart。
#   可设置 RDSCTL_MYSQL_EXTERNAL=1 强制只走外部模式。
#
# 用法:
#   ./scripts/mysql.sh status|info         查看状态/配置
#   ./scripts/mysql.sh start               启动(外部可达则跳过)
#   ./scripts/mysql.sh stop                停止捆绑实例(外部实例需 FORCE=1)
#   ./scripts/mysql.sh restart
#   ./scripts/mysql.sh reset               危险:重建捆绑实例数据目录
#   ./scripts/mysql.sh help                显示本帮助

set -euo pipefail
# shellcheck source=scripts/lib.sh
. "$(dirname "$0")/lib.sh"

help() {
  cat <<EOF
rdsctl MySQL 实例管理 — 控制面持久化(rdsctl 的 MySQL)

子命令:
  status               检查 MySQL 是否可达并打印实例归属(捆绑/外部)
  info                 打印当前生效的连接配置与可执行文件路径
  start                启动/确认可用:
                         - 外部模式:仅连通 + 建库(无则自动创建 ${RDSCTL_MYSQL_DB})
                         - 捆绑模式:自动 init + 启动仓库内数据目录
  stop                 停止捆绑实例(优雅→强杀);外部实例需 FORCE=1 才允许停机
  restart              重启(捆绑模式)
  reset                危险:CONFIRM_RESET=1 确认后删除并重建捆绑数据目录
  help                 显示本帮助

模式:
  默认自动:若 RDSCTL_MYSQL_HOST:PORT 已有可用 MySQL → 外部模式,不触碰本机数据;
  否则使用捆绑实例(数据目录 \${RDSCTL_MYSQL_DATA_DIR:-$ROOT/.rdsctl-mysql})。
  设 RDSCTL_MYSQL_EXTERNAL=1 强制外部模式(永不 reset/stop 外部库)。

环境(经 rdsctl.env 或 export):
  RDSCTL_MYSQL_HOST/PORT/USER/PASS/DB   默认 127.0.0.1/3306/root/(空)/rdsctl
  RDSCTL_MYSQL_DATA_DIR                 捆绑实例数据目录(默认仓库内 .rdsctl-mysql)
  RDSCTL_MYSQLD / RDSCTL_MYSQL_CLI      mysqld / mysql 客户端路径(自动查找)

示例:
  ./scripts/mysql.sh info
  ./scripts/mysql.sh start
  FORCE=1 ./scripts/mysql.sh stop      # 允许对外部实例发停机
  CONFIRM_RESET=1 ./scripts/mysql.sh reset
EOF
}

CMD="${1:-status}"
FORCE="${FORCE:-0}"

case "$CMD" in
  -h | --help | help)
    help
    exit 0
    ;;
esac

is_managed() { # 捆绑实例是否在跑(有 pid 文件且进程存活)
  [ -f "$DATA_DIR/mysql.pid" ] || return 1
  local pid
  pid="$(cat "$DATA_DIR/mysql.pid" 2>/dev/null || echo '')"
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

external_mode() { [ "${RDSCTL_MYSQL_EXTERNAL:-0}" = "1" ]; }

require_client() { [ -n "$RDSCTL_MYSQL_CLI" ] || die "未找到 mysql 客户端(可设置 RDSCTL_MYSQL_CLI)"; }

require_mysqld() {
  [ -n "$RDSCTL_MYSQLD" ] || die "未找到 mysqld(可设置 RDSCTL_MYSQLD 指向 mysqld 绝对路径)"
}

create_db_if_missing() {
  if ! mysql_alive; then return 1; fi
  # 仅尝试;无权限时应用启动会给出明确报错
  if ! mysql_exec -N "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='${RDSCTL_MYSQL_DB}'" | grep -q .; then
    if mysql_exec "CREATE DATABASE IF NOT EXISTS \`${RDSCTL_MYSQL_DB}\` CHARACTER SET utf8mb4" >/dev/null 2>&1; then
      ok "数据库 ${RDSCTL_MYSQL_DB} 已创建"
    else
      warn "无法自动创建数据库 ${RDSCTL_MYSQL_DB}(权限不足?),请手工创建或使用有权限的账号"
    fi
  fi
}

launch_mysqld() { # 启动 mysqld(daemon 化),返回 pid
  local basedir
  basedir="$(BASEDIR "$RDSCTL_MYSQLD")"
  # shellcheck disable=SC2086
  nohup "$RDSCTL_MYSQLD" --no-defaults \
    --datadir="$DATA_DIR" \
    --basedir="$basedir" \
    --port="$RDSCTL_MYSQL_PORT" \
    --bind-address=127.0.0.1 \
    --socket="$DATA_DIR/mysql.sock" \
    --pid-file="$DATA_DIR/mysql.pid" \
    --log-error="$DATA_DIR/mysql.err" >>"$DATA_DIR/mysql.out" 2>&1 &
  echo $!
}

wait_alive() { # $1=刚启动的 mysqld pid,$2=轮询次数(0.5s/次)
  local pid="$1" i=0 n="${2:-60}" dead=0
  while [ "$i" -lt "$n" ]; do
    mysql_alive && return 0
    if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
      dead=$((dead + 1))
      # 进程已退出且 4s 内未就绪 → 视为启动失败,不必干等满轮
      [ "$dead" -ge 8 ] && return 1
    fi
    sleep 0.5
    i=$((i + 1))
  done
  return 1
}

# macOS(区分大小写不敏感 APFS)+ Homebrew MySQL 的已知怪癖:停机后重启报
# "Can't create UNDO tablespace ... already exists"。仅在确实出现该错误时,
# 删除 undo 文件让 InnoDB 下次启动重建(数据安全:undo 重启即重建)。
undo_conflict() {
  grep -q "innodb_undo_001.*already exists" "$DATA_DIR/mysql.err" 2>/dev/null
}

repair_undo() {
  warn "检测到 InnoDB undo 表空间冲突,移除 undo 文件让实例重建(仅限停机状态)"
  rm -f "$DATA_DIR"/undo_00* "$DATA_DIR"/undo_*_trunc.log
}

start_bundled() {
  require_client
  require_mysqld
  if mysql_alive; then
    ok "MySQL 已在 ${RDSCTL_MYSQL_HOST}:${RDSCTL_MYSQL_PORT} 运行"
    create_db_if_missing
    return 0
  fi
  if [ ! -f "$DATA_DIR/auto.cnf" ]; then
    info "初始化捆绑实例数据目录: $DATA_DIR"
    mkdir -p "$DATA_DIR"
    "$RDSCTL_MYSQLD" --no-defaults --initialize-insecure \
      --datadir="$DATA_DIR" \
      --basedir="$(BASEDIR "$RDSCTL_MYSQLD")" >/dev/null 2>&1 \
      || die "mysqld --initialize-insecure 失败(详见 $DATA_DIR/mysql.err)"
    ok "初始化完成(root 免密)"
  fi
  info "启动捆绑 MySQL 实例(数据目录 $DATA_DIR)"
  local pid
  pid="$(launch_mysqld)"
  if wait_alive "$pid" 60; then
    ok "MySQL 启动成功(${RDSCTL_MYSQL_HOST}:${RDSCTL_MYSQL_PORT})"
    create_db_if_missing
    return 0
  fi
  # 首次失败:若为 undo 冲突则自愈后重试一次
  if undo_conflict; then
    warn "首次启动失败(undo 冲突),自愈重试…"
    # 确保进程确实退出(pid 文件可能残留)
    if is_managed; then
      kill -9 "$(cat "$DATA_DIR/mysql.pid" 2>/dev/null)" 2>/dev/null || true
      sleep 1
    fi
    rm -f "$DATA_DIR/mysql.pid"
    repair_undo
    pid="$(launch_mysqld)"
    if wait_alive "$pid" 60; then
      ok "MySQL 自愈后启动成功(${RDSCTL_MYSQL_HOST}:${RDSCTL_MYSQL_PORT})"
      create_db_if_missing
      return 0
    fi
  fi
  die "MySQL 启动失败(日志:$DATA_DIR/mysql.err)"
}

stop_bundled() {
  if ! mysql_alive && ! is_managed; then
    warn "MySQL 未在运行"
    return 0
  fi
  if is_managed; then
    local pid
    pid="$(cat "$DATA_DIR/mysql.pid" 2>/dev/null || echo '')"
    info "停止捆绑实例(pid=$pid)"
    [ -n "$pid" ] && kill -TERM "$pid" 2>/dev/null || true
    local i=0
    while [ $i -lt 20 ] && mysql_alive; do sleep 0.5; i=$((i + 1)); done
    if mysql_alive; then
      warn "SIGTERM 未退出,强制 KILL"
      [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null || true
      sleep 1
    fi
    rm -f "$DATA_DIR/mysql.pid"
    ok "捆绑实例已停止"
  else
    if external_mode || [ "$FORCE" = "1" ]; then
      info "使用 mysqladmin 优雅停机外部实例"
      ( export MYSQL_PWD="$RDSCTL_MYSQL_PASS"
        "$(dirname "$RDSCTL_MYSQLD")/mysqladmin" -h "$RDSCTL_MYSQL_HOST" -P "$RDSCTL_MYSQL_PORT" -u "$RDSCTL_MYSQL_USER" shutdown ) \
        || warn "mysqladmin shutdown 失败(可能无权限);未停止任何捆绑进程"
      ok "已发送停机请求"
    else
      die "检测到的是外部/非本脚本管理的 MySQL;如需停机请设置 FORCE=1 或用其自身管理方式"
    fi
  fi
}

BASEDIR() { # 依据 mysqld 位置推算 basedir
  local m="$1" d
  d="$(dirname "$m")"
  case "$d" in
    */bin) dirname "$d" ;;
    *) echo "${d%/bin}" ;;
  esac
}

case "$CMD" in
  status)
    if mysql_alive; then
      ok "MySQL 运行中: $(mysql_info | sed 's/^MySQL: *//')"
      if is_managed; then
        echo "   (捆绑实例: 数据目录 $DATA_DIR, pid $(cat "$DATA_DIR/mysql.pid"))"
      else
        echo "   (外部实例或非本脚本管理的进程)"
      fi
      exit 0
    fi
    echo "MySQL 未运行 (host=${RDSCTL_MYSQL_HOST}:${RDSCTL_MYSQL_PORT})"
    if [ -f "$DATA_DIR/auto.cnf" ]; then echo "   捆绑数据目录已初始化: $DATA_DIR"; fi
    exit 1
    ;;
  info)
    echo "MySQL 客户端 : ${RDSCTL_MYSQL_CLI:-未找到}"
    echo "MySQL 服务端 : ${RDSCTL_MYSQLD:-未找到}"
    mysql_info
    echo "数据目录     : ${RDSCTL_MYSQL_DATA_DIR:-$DATA_DIR} (捆绑模式)"
    echo "外部强制     : ${RDSCTL_MYSQL_EXTERNAL:-0} (1=只用外部实例)"
    ;;
  start)
    if external_mode; then
      require_client
      mysql_alive || die "外部模式但 ${RDSCTL_MYSQL_HOST}:${RDSCTL_MYSQL_PORT} 不可达,请先启动该 MySQL"
      ok "外部 MySQL 可达"
      create_db_if_missing
    else
      start_bundled
    fi
    ;;
  stop)
    stop_bundled
    ;;
  restart)
    stop_bundled || true
    start_bundled
    ;;
  reset)
    echo "!! 将删除数据目录 $DATA_DIR 并重建捆绑实例" >&2
    echo "!! 再次确认请设置 CONFIRM_RESET=1" >&2
    [ "${CONFIRM_RESET:-0}" = "1" ] || exit 1
    stop_bundled || true
    rm -rf "$DATA_DIR"
    start_bundled
    ;;
  *)
    echo "用法: $0 {status|info|start|stop|restart|reset}" >&2
    exit 2
    ;;
esac
