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
