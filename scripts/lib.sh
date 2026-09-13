#!/usr/bin/env bash
# rdsctl 运维脚本公共库(被其它 scripts/*.sh source)
#
# 约定:
#   ROOT         仓库根目录(由本文件位置推导)
#   配置来源:环境变量 > $ROOT/rdsctl.env(若存在)
#   默认配置与 src/store.rs / src/main.rs 保持一致,可用环境变量覆盖。

set -u

# ─── 路径 ───
LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$LIB_DIR/.." && pwd)"
LOG_DIR="$ROOT/logs"
DATA_DIR="${RDSCTL_MYSQL_DATA_DIR:-$ROOT/.rdsctl-mysql}"
mkdir -p "$LOG_DIR"

# ─── 加载配置文件(优先级: 环境变量 > rdsctl.env > 默认值) ───
RDSCTL_ENV_FILE="${RDSCTL_ENV_FILE:-$ROOT/rdsctl.env}"
if [ -f "$RDSCTL_ENV_FILE" ]; then
  # 逐行导出,但已存在于环境中的变量不被文件覆盖
  # shellcheck disable=SC1090
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      '' | \#*) continue ;;
    esac
    key="${line%%=*}"
    if [ -n "$key" ] && [ -z "${!key+x}" ]; then
      export "$line"
    fi
  done <"$RDSCTL_ENV_FILE"
fi

# ─── 默认配置(与程序默认值一致) ───
RDSCTL_PORT="${RDSCTL_PORT:-9113}"
RDSCTL_MYSQL_HOST="${RDSCTL_MYSQL_HOST:-127.0.0.1}"
RDSCTL_MYSQL_PORT="${RDSCTL_MYSQL_PORT:-3306}"
RDSCTL_MYSQL_USER="${RDSCTL_MYSQL_USER:-root}"
RDSCTL_MYSQL_PASS="${RDSCTL_MYSQL_PASS:-}"
RDSCTL_MYSQL_DB="${RDSCTL_MYSQL_DB:-rdsctl}"
RDSCTL_SWEEP_SECS="${RDSCTL_SWEEP_SECS:-30}"
RDSCTL_USER="${RDSCTL_USER:-admin}"
RDSCTL_PASS="${RDSCTL_PASS:-admin}"

# ─── 运行模式与集群(cluster)支持 ───
# single (默认): 单机单进程,实例互斥来自控制库行锁 —— 今天的行为,逐字不变。
# cluster       : 多副本 + 自带多数派仲裁(见 docs/control-plane-ha-design.md)。
RDSCTL_MODE="${RDSCTL_MODE:-single}"
# 仲裁与投影:cluster 模式下 sink 只是投影/读模型(默认 mysql;none=不接)
RDSCTL_METADATA_SINK="${RDSCTL_METADATA_SINK:-mysql}"

# 单实例键:cluster 模式按 node-id 区分(同机多副本必须各用一套 pid/日志),
# single 模式按端口(与既有行为一致)。
instance_key() {
  if [ "$RDSCTL_MODE" = "cluster" ] && [ -n "${RDSCTL_NODE_ID:-}" ]; then
    printf '%s' "$RDSCTL_NODE_ID"
  else
    printf '%s' "$RDSCTL_PORT"
  fi
}

# 传给 rdsctl 的参数:
#   RDSCTL_ARGS 显式优先 > cluster 模式按 NODE_ID/CLUSTER/RPC_PORT 自动拼装 > 单机 --port
service_args() {
  if [ -n "${RDSCTL_ARGS:-}" ]; then
    printf '%s' "$RDSCTL_ARGS"
  elif [ "$RDSCTL_MODE" = "cluster" ]; then
    printf 'serve --node-id=%s --cluster=%s --port=%s --rpc-port=%s' \
      "$RDSCTL_NODE_ID" "$RDSCTL_CLUSTER" "$RDSCTL_PORT" "${RDSCTL_RPC_PORT:-9330}"
  else
    printf -- '--port %s' "$RDSCTL_PORT"
  fi
}

# cluster 模式环境校验:判据与程序内自检、deploy/bin/rdsctl-preflight.sh 保持一致
# (voter 必须奇数 ≥3、自身须在成员表内、数据目录按节点隔离)
cluster_validate() {
  [ "$RDSCTL_MODE" = "cluster" ] || return 0
  [ -n "${RDSCTL_NODE_ID:-}" ] || die "cluster 模式必须设置 RDSCTL_NODE_ID(每个副本唯一)"
  [ -n "${RDSCTL_RPC_PORT:-}" ] || RDSCTL_RPC_PORT=9330
  [ -n "${RDSCTL_CLUSTER:-}" ] || die "cluster 模式必须设置 RDSCTL_CLUSTER=id@ip:port,...(见 deploy/README.md)"
  local n=0 self=0 m
  local old_ifs="$IFS"
  IFS=','
  for m in $RDSCTL_CLUSTER; do
    [ -n "$m" ] || continue
    n=$((n + 1))
    [ "${m%%@*}" = "$RDSCTL_NODE_ID" ] && self=1
  done
  IFS="$old_ifs"
  [ "$n" -ge 3 ] || die "RDSCTL_CLUSTER 至少需要 3 个 voter(当前 $n):多数派仲裁需 3 或 5"
  [ $((n % 2)) -eq 1 ] || die "RDSCTL_CLUSTER 的 voter 数必须为奇数(当前 $n)"
  [ "$self" = "1" ] || die "RDSCTL_NODE_ID=$RDSCTL_NODE_ID 不在 RDSCTL_CLUSTER 成员表内"
  # 数据目录按节点隔离:日志/快照/fence 状态不可共享
  RDSCTL_DATA_DIR="${RDSCTL_DATA_DIR:-$ROOT/logs/ha/$RDSCTL_NODE_ID}"
  export RDSCTL_DATA_DIR RDSCTL_RPC_PORT
  # lab 放行提示:未验证前提时 /readyz 会显式标注 lab_degraded
  if [ "${RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK:-0}" = "1" ] \
    || [ "${RDSCTL_ALLOW_NO_AGENT:-0}" = "1" ]; then
    warn "注意:已启用 lab 放行(时钟/agent 前提未验证)→ /readyz 的 premises_unverified 会标注,勿用于生产"
  fi
}

# 探测当前 leader(遍历 RDSCTL_CLUSTER 成员,命中 role=leader 即返回其 id)
cluster_leader() {
  [ -n "${RDSCTL_CLUSTER:-}" ] || return 1
  local m addr
  for m in $(printf '%s' "$RDSCTL_CLUSTER" | tr ',' ' '); do
    [ -n "$m" ] || continue
    addr="${m#*@}"
    if curl -s -m 2 "http://${addr}/internal/status" 2>/dev/null | grep -q '"role":"leader"'; then
      printf '%s' "${m%%@*}"
      return 0
    fi
  done
  return 1
}

# 探针:cluster 模式就绪判定用 /healthz(免登录);single 模式沿用登录 200
http_code() { # 用法: http_code <path>
  command -v curl >/dev/null 2>&1 || return 1
  curl -s -m 2 -o /dev/null -w '%{http_code}' "$URL$1" 2>/dev/null
}

readyz_json() {
  command -v curl >/dev/null 2>&1 || return 1
  curl -s -m 3 "$URL/readyz" 2>/dev/null
}

# 从 readyz JSON 里取一个字段(grep 提取,避免引入 jq 依赖)
readyz_field() { # 用法: readyz_field <field>
  readyz_json | grep -o "\"$1\":[^,}]*" | head -1 | cut -d: -f2- | tr -d '"'
}

# ─── 可执行文件解析 ───
# mysql 客户端(控制库连接,若外部设置了 RDSCTL_MYSQL_CLI 则优先)
if [ -z "${RDSCTL_MYSQL_CLI:-}" ]; then
  RDSCTL_MYSQL_CLI="$(command -v mysql || true)"
fi
# mysqld 服务端(仅捆绑实例模式使用)
if [ -z "${RDSCTL_MYSQLD:-}" ]; then
  RDSCTL_MYSQLD="$(command -v mysqld || true)"
  if [ -z "$RDSCTL_MYSQLD" ] && [ -x /usr/local/opt/mysql@8.0/bin/mysqld ]; then
    RDSCTL_MYSQLD=/usr/local/opt/mysql@8.0/bin/mysqld
  elif [ -z "$RDSCTL_MYSQLD" ] && [ -x /usr/local/mysql/bin/mysqld ]; then
    RDSCTL_MYSQLD=/usr/local/mysql/bin/mysqld
  fi
fi

info()  { printf '\033[1;36m[rdsctl]\033[0m %s\n' "$*"; }
ok()    { printf '\033[1;32m[rdsctl]\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33m[rdsctl]\033[0m %s\n' "$*" >&2; }
die()   { printf '\033[1;31m[rdsctl]\033[0m %s\n' "$*" >&2; exit 1; }

# 打印最终生效的 MySQL 连接信息
mysql_info() {
  echo "MySQL:  ${RDSCTL_MYSQL_USER}@${RDSCTL_MYSQL_HOST}:${RDSCTL_MYSQL_PORT}/${RDSCTL_MYSQL_DB}$([ -n "$RDSCTL_MYSQL_PASS" ] && echo ' (password set)' || echo ' (no password)')"
}

# 用 mysql 客户端执行一条 SQL(登录凭据经 MYSQL_PWD 环境变量传递,避免命令行泄露)
# 用法: mysql_exec [-N] 'SQL'
mysql_exec() {
  [ -n "$RDSCTL_MYSQL_CLI" ] || die "未找到 mysql 客户端(可设置 RDSCTL_MYSQL_CLI 指向真实 mysql 路径)"
  local raw=""
  if [ "${1:-}" = "-N" ]; then raw="-N"; shift; fi
  local sql="${1:?用法: mysql_exec 'SQL'}"
  # shellcheck disable=SC2086
  (
    export MYSQL_PWD="$RDSCTL_MYSQL_PASS"
    "$RDSCTL_MYSQL_CLI" -h "$RDSCTL_MYSQL_HOST" -P "$RDSCTL_MYSQL_PORT" \
      -u "$RDSCTL_MYSQL_USER" --protocol=tcp --batch --raw --skip-column-names \
      --connect-timeout=5 $raw -e "$sql"
  )
}

# MySQL 是否可达(可被 mysql_exec/服务使用)
mysql_alive() {
  mysql_exec -N 'SELECT 1' >/dev/null 2>&1
}

# 环境变量导出(供服务进程继承)
export_rdsctl_env() {
  export RDSCTL_MYSQL_HOST RDSCTL_MYSQL_PORT RDSCTL_MYSQL_USER RDSCTL_MYSQL_DB RDSCTL_SWEEP_SECS RDSCTL_USER RDSCTL_PASS
  [ -n "$RDSCTL_MYSQL_PASS" ] && export RDSCTL_MYSQL_PASS
  [ -n "${RDSCTL_MYSQL_CLI:-}" ] && export RDSCTL_MYSQL_CLI
}
