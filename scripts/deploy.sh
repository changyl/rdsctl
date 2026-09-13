#!/usr/bin/env bash
# rdsctl 一键部署
#
# 流程: 编译(release, 离线) → 确保 MySQL → 重启服务 → 健康检查 → 汇总
#
# 用法:
#   ./scripts/deploy.sh                # 一键部署到本机(默认端口 9113)
#   RDSCTL_PORT=9120 ./scripts/deploy.sh   # 换端口
#   ./scripts/deploy.sh --skip-build   # 跳过编译(仅重启服务)
#   ./scripts/deploy.sh --test         # 部署后跑 P0 验收(单元+集成,需 MySQL)
#   ./scripts/deploy.sh --debug        # 用 debug 产物
#
# 首次使用会自动生成 rdsctl.env(已存在则保留)以便按需修改配置。

set -euo pipefail
# shellcheck source=scripts/lib.sh
. "$(dirname "$0")/lib.sh"

help() {
  cat <<EOF
rdsctl 一键部署 / 清理

部署(默认命令):
  ./scripts/deploy.sh                   一键部署(编译→MySQL→重启服务→健康检查)
  RDSCTL_PORT=9120 ./scripts/deploy.sh  换端口部署
  ./scripts/deploy.sh --skip-build      跳过编译,仅重启服务(改配置后热生效)
  ./scripts/deploy.sh --debug           用 debug 产物部署
  ./scripts/deploy.sh --lean            用精简 debug(dev-lean)产物部署(占盘小,日常验证用)
  ./scripts/deploy.sh --test            部署后运行 P0 验收(单元 + 集成,需 MySQL)

清理:
  ./scripts/deploy.sh clean             停掉本脚本管理的全部 rdsctl 服务(多端口),
                                       并删除 pid 与服务日志;保留二进制 / 配置 /
                                       MySQL 数据(可随时重新 deploy 恢复)
  ./scripts/deploy.sh clean --purge-data
                                       附加:停止捆绑 MySQL 并删除其数据目录
                                       (\${RDSCTL_MYSQL_DATA_DIR:-$ROOT/.rdsctl-mysql});
                                       外部 MySQL 永不触碰(需 CONFIRM_PURGE=1)
  ./scripts/deploy.sh clean --remove-config
                                       附加:删除 rdsctl.env(下次 deploy 按默认重新生成)
  ./scripts/deploy.sh help              显示本帮助

集群(cluster)模式:
  - 单机多副本一键起停用 ./scripts/cluster.sh up|status|down(开发/演练);
  - 本脚本 --cluster 面向"按节点部署"(可配合 deploy/systemd/rdsctl@.service 使用);
  - cluster 模式的前提检查(时钟/fsync/voter/agent)由 deploy/bin/rdsctl-preflight.sh 承担,
    不通过即拒绝启动(退出码 2),不会"看起来部署成功"。

注意:重复执行 deploy 即"重新部署"(停旧起新);清理属破坏性操作,请确认后再执行。
EOF
}

case "${1:-}" in
  -h | --help | help)
    help
    exit 0
    ;;
  clean | undeploy)
    shift
    # ─── 部署清理 ───
    PURGE_DATA=0
    REMOVE_CONFIG=0
    for a in "$@"; do
      case "$a" in
        --purge-data) PURGE_DATA=1 ;;
        --remove-config) REMOVE_CONFIG=1 ;;
        *)
          echo "clean 未知参数: $a" >&2
          help >&2
          exit 2
          ;;
      esac
    done

    echo "══════════════ rdsctl 清理 ══════════════"
    # 1) 停止本脚本管理的全部 rdsctl 实例(按 logs/rdsctl-*.pid)
    stopped=0
    for pidfile in "$LOG_DIR"/rdsctl-*.pid; do
      [ -e "$pidfile" ] || continue
      pid="$(cat "$pidfile" 2>/dev/null || echo '')"
      if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        info "停止 rdsctl (pid=$pid, ${pidfile##*/})"
        kill -TERM "$pid" 2>/dev/null || true
        for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
        if kill -0 "$pid" 2>/dev/null; then
          warn "SIGTERM 超时,强杀 pid=$pid"
          kill -9 "$pid" 2>/dev/null || true
        fi
        stopped=1
      fi
      rm -f "$pidfile"
    done
    [ "$stopped" = "0" ] && info "没有运行中的 rdsctl 实例"

    # 2) 服务日志
    rm -f "$LOG_DIR"/rdsctl-*.log
    info "已清理 pid/服务日志: $LOG_DIR/rdsctl-*"

    # 3) 可选:捆绑 MySQL 数据目录
    if [ "$PURGE_DATA" = "1" ]; then
      echo "!! --purge-data: 将停止捆绑 MySQL 并删除数据目录 ${DATA_DIR}" >&2
      echo "!! 确认请设 CONFIRM_PURGE=1(外部 MySQL 不受影响)" >&2
      [ "${CONFIRM_PURGE:-0}" = "1" ] || die "未确认,退出(--purge-data 需 CONFIRM_PURGE=1)"
      if mysql_alive && [ ! -f "$DATA_DIR/mysql.pid" ]; then
        warn "当前连接的 MySQL 不是捆绑实例(无 $DATA_DIR/mysql.pid),不执行任何停止"
      else
        "$ROOT/scripts/mysql.sh" stop || true
        rm -rf "$DATA_DIR"
        ok "已删除捆绑 MySQL 数据目录: $DATA_DIR"
      fi
    else
      info "捆绑 MySQL 数据保留(如需清除: CONFIRM_PURGE=1 $0 clean --purge-data)"
    fi

    # 4) 可选:删除配置
    if [ "$REMOVE_CONFIG" = "1" ]; then
      rm -f "$RDSCTL_ENV_FILE"
      info "已删除配置 $RDSCTL_ENV_FILE(下次 deploy 会按默认重新生成)"
    else
      info "配置保留: $RDSCTL_ENV_FILE"
    fi

    ok "清理完成。重新部署: ./scripts/deploy.sh"
    exit 0
    ;;
esac

SKIP_BUILD=0
DO_TEST=0
BUILD_PROFILE="release"
RUN_PREFLIGHT=0
for a in "$@"; do
  case "$a" in
    --skip-build) SKIP_BUILD=1 ;;
    --test) DO_TEST=1 ;;
    --debug) BUILD_PROFILE="debug" ;;
    --lean) BUILD_PROFILE="dev-lean" ;;
    --cluster) RDSCTL_MODE=cluster; export RDSCTL_MODE; RUN_PREFLIGHT=1 ;;
    --no-preflight) RUN_PREFLIGHT=0 ;;
    *) echo "未知参数: $a" >&2; exit 2 ;;
  esac
done

# cluster 模式:先把环境校验清楚(缺 node-id/成员表、voter 非奇数等直接拒绝)
cluster_validate

# 编译档案 → 二进制:显式导出,使后续 status/logs 报告的是**真正在跑的那个**产物
case "$BUILD_PROFILE" in
  debug) RDSCTL_BIN="$ROOT/target/debug/rdsctl" ;;
  dev-lean) RDSCTL_BIN="$ROOT/target/dev-lean/rdsctl" ;;
  *) RDSCTL_BIN="$ROOT/target/release/rdsctl" ;;
esac
export RDSCTL_BIN

# 首次生成默认配置文件(便于用户按需修改后重跑)
if [ ! -f "$RDSCTL_ENV_FILE" ] && [ -f "$ROOT/rdsctl.env.example" ]; then
  cp "$ROOT/rdsctl.env.example" "$RDSCTL_ENV_FILE"
  info "已生成默认配置: $RDSCTL_ENV_FILE (可修改后重新 ./scripts/deploy.sh)"
fi

echo "══════════════ rdsctl 部署 ══════════════"
info "目录   : $ROOT"
info "模式   : $RDSCTL_MODE$([ "$RDSCTL_MODE" = cluster ] && echo " (node=$RDSCTL_NODE_ID, rpc=${RDSCTL_RPC_PORT:-9330}, data=${RDSCTL_DATA_DIR:-?})")"
mysql_info

# 0) cluster 模式:前提门禁(时钟/fsync/voter/agent;不通过即退出,不"假装成功")
if [ "$RDSCTL_MODE" = "cluster" ] && [ "$RUN_PREFLIGHT" = "1" ]; then
  if [ -x "$ROOT/deploy/bin/rdsctl-preflight.sh" ]; then
    info "运行前提自检: deploy/bin/rdsctl-preflight.sh $RDSCTL_NODE_ID"
    if ! "$ROOT/deploy/bin/rdsctl-preflight.sh" "$RDSCTL_NODE_ID"; then
      die "前提自检未通过,拒绝部署(见上;lab 可用 RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK=1 / RDSCTL_ALLOW_NO_AGENT=1 显式放行)"
    fi
  else
    warn "未找到 deploy/bin/rdsctl-preflight.sh,跳过前提自检(不推荐)"
  fi
fi

# 1) 编译
if [ "$SKIP_BUILD" = "0" ]; then
  case "$BUILD_PROFILE" in
    release) "$ROOT/scripts/build.sh" ;;
    debug) "$ROOT/scripts/build.sh" --debug ;;
    dev-lean) "$ROOT/scripts/build.sh" --debug-lean ;;
  esac
else
  info "跳过编译(--skip-build)"
fi

# 2) MySQL 可用(cluster 模式:sink 只是投影,仅 sink=mysql 时需要)
if [ "$RDSCTL_MODE" = "cluster" ] && [ "${RDSCTL_METADATA_SINK:-mysql}" != "mysql" ]; then
  info "cluster + RDSCTL_METADATA_SINK=${RDSCTL_METADATA_SINK}:不依赖控制库,跳过 MySQL"
else
  "$ROOT/scripts/mysql.sh" start
fi

# 3) 服务重启
"$ROOT/scripts/rdsctl.sh" stop || true
case "$BUILD_PROFILE" in
  debug) RDSCTL_BIN="$ROOT/target/debug/rdsctl" "$ROOT/scripts/rdsctl.sh" start ;;
  dev-lean) RDSCTL_BIN="$ROOT/target/dev-lean/rdsctl" "$ROOT/scripts/rdsctl.sh" start ;;
  *) "$ROOT/scripts/rdsctl.sh" start ;;
esac

# 4) 健康检查(登录 + 关键 API)
PORT="$RDSCTL_PORT"
"$ROOT/scripts/rdsctl.sh" status

echo "══════════════ 部署完成 ══════════════"
if [ "$RDSCTL_MODE" = "cluster" ]; then
  echo "  本节点   : $RDSCTL_NODE_ID(rpc ${RDSCTL_RPC_PORT:-9330},数据 ${RDSCTL_DATA_DIR:-?})"
  echo "  探针     : http://127.0.0.1:$PORT/healthz 与 /readyz"
  echo "  集群成员 : $RDSCTL_CLUSTER"
  echo "  注意     : 集群模式公开端口只提供探针与完整 API(取决于 sink),不提供登录页"
  echo "  常用命令 :"
  echo "    ./scripts/rdsctl.sh status|logs|stop|restart     # 单节点"
  echo "    ./scripts/cluster.sh status                      # 同机多副本总览"
  echo "    ./scripts/ha-drill.sh                            # 端到端演练(起→验→停)"
  echo "    deploy/systemd/rdsctl@.service                   # 生产(多机)按节点守护"
else
  echo "  管控页面 : http://127.0.0.1:$PORT/rds"
  echo "  登录凭据 : $RDSCTL_USER / $RDSCTL_PASS"
  echo "  审计 API : /api/rds/audit"
  echo "  常用命令 :"
  echo "    ./scripts/rdsctl.sh status|logs|stop|restart"
  echo "    ./scripts/mysql.sh status|stop|start"
  echo "    ./scripts/build.sh --test   # 单元 + P0 验收"
fi

# 5) 可选:P0 验收
if [ "$DO_TEST" = "1" ]; then
  echo "══ P0 验收测试 ══"
  if [ -d "$ROOT/.cargo-home" ]; then
    CARGO_HOME="$ROOT/.cargo-home" cargo test --offline
  else
    cargo test
  fi
fi
