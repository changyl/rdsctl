#!/usr/bin/env bash
# rdsctl 编译脚本(默认离线,复用仓库内 vendored registry)
#
# 用法:
#   ./scripts/build.sh                # 离线 release 编译
#   ./scripts/build.sh --debug        # 离线 debug 编译(默认 dev,可断点调试;较占盘)
#   ./scripts/build.sh --debug-lean   # 精简 debug(仅行号/关增量,占盘小;日常跑/验证用)
#   ./scripts/build.sh --online       # 联网编译(不使用 vendored CARGO_HOME)
#   ./scripts/build.sh --test         # 编译并跑全部测试(单测 + P0 验收,需 MySQL 可达)
#   ./scripts/build.sh --clean        # 先清理 target 再编译
#   ./scripts/build.sh --drill        # 编译 debug 后跑真实 MySQL 受管切换演练(scripts/orch-drill.sh)
#   ./scripts/build.sh --run-mem      # 编译后以内存后端 + demo 数据前台启动(RDSCTL_PORT,默认 9123)
#   ./scripts/build.sh -h|--help|help # 帮助

set -euo pipefail
# shellcheck source=scripts/lib.sh
. "$(dirname "$0")/lib.sh"

help() {
  cat <<'EOF'
rdsctl 编译脚本 — 默认离线(release),复用仓库内 .cargo-home vendored registry

用法:
  ./scripts/build.sh                编译 release(离线)
  ./scripts/build.sh --debug        编译 debug(离线,默认 dev 档案,可断点调试)
  ./scripts/build.sh --debug-lean   编译精简 debug(dev-lean 档案:仅行号/关增量,占盘小,日常跑/验证用)
  ./scripts/build.sh --online       联网编译(使用系统 CARGO_HOME)
  ./scripts/build.sh --test         编译后运行全部测试(单测 + P0 验收;需 MySQL 可达)
  ./scripts/build.sh --clean        先 cargo clean 再编译
  ./scripts/build.sh --drill        编译 debug 后执行真实 MySQL 受管切换演练(需 docker + 本地镜像)
  ./scripts/build.sh --run-mem      编译后以内存后端+demo 前台启动(端口 RDSCTL_PORT,默认 9123)
  ./scripts/build.sh help           显示本帮助

产物:
  target/release/rdsctl(默认) | target/debug/rdsctl(--debug) | target/dev-lean/rdsctl(--debug-lean)

环境:
  离线模式固定使用仓库内 .cargo-home;其它配置见 scripts/README.md
EOF
}

case "${1:-}" in
  -h | --help | help)
    help
    exit 0
    ;;
esac

PROFILE="release"
MODE="offline"
DO_TEST=0
DO_CLEAN=0
DO_DRILL=0
DO_RUNMEM=0
for a in "$@"; do
  case "$a" in
    --debug) PROFILE="debug" ;;
    --debug-lean) PROFILE="dev-lean" ;;
    --online) MODE="online" ;;
    --test) DO_TEST=1 ;;
    --clean) DO_CLEAN=1 ;;
    --drill) DO_DRILL=1 ;;
    --run-mem) DO_RUNMEM=1 ;;
    *)
      echo "未知参数: $a" >&2
      help >&2
      exit 2
      ;;
  esac
done
[ "$DO_DRILL" = 1 ] && PROFILE="debug" # 演练脚本固定使用 debug 产物

# 离线模式:优先使用仓库内 .cargo-home(预置 rsproxy 镜像缓存)
if [ "$MODE" = "offline" ] && [ -d "$PWD/.cargo-home" ]; then
  export CARGO_HOME="$PWD/.cargo-home"
  CARGO_FLAGS="--offline"
  info "CARGO_HOME=$CARGO_HOME (offline, rsproxy 镜像缓存)"
else
  CARGO_FLAGS=""
  info "联网模式(使用系统 CARGO_HOME)"
fi

command -v cargo >/dev/null || die "未找到 cargo"

if [ "$DO_CLEAN" = "1" ]; then
  info "cargo clean"
  cargo clean
fi

BIN_TARGET="debug"
if [ "$PROFILE" = "release" ]; then
  BIN_TARGET="release"
  CARGO_FLAGS="$CARGO_FLAGS --release"
elif [ "$PROFILE" = "dev-lean" ]; then
  BIN_TARGET="dev-lean"
  CARGO_FLAGS="$CARGO_FLAGS --profile dev-lean"
fi
BIN="$PWD/target/$BIN_TARGET/rdsctl"

info "cargo build $CARGO_FLAGS (profile=$PROFILE)"
cargo build $CARGO_FLAGS
[ -x "$BIN" ] || die "编译产物缺失: $BIN"

ok "编译完成: $BIN"
ls -lh "$BIN" | awk '{print "    size:", $5}'

if [ "$DO_TEST" = "1" ]; then
  info "cargo test $CARGO_FLAGS"
  cargo test $CARGO_FLAGS
  ok "测试通过(单元 + 验收)"
fi

if [ "$DO_DRILL" = "1" ]; then
  info "运行真实 MySQL 受管切换演练(scripts/orch-drill.sh;需 docker + 本地镜像)"
  bash "$(dirname "$0")/orch-drill.sh"
fi

if [ "$DO_RUNMEM" = "1" ]; then
  info "内存后端 + demo 前台启动: http://127.0.0.1:${RDSCTL_PORT:-9123} (Ctrl-C 退出)"
  RDSCTL_STORE_BACKEND=memory RDSCTL_DEMO_SEED=1 "$BIN" --port "${RDSCTL_PORT:-9123}"
fi
