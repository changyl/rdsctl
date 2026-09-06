#!/usr/bin/env bash
# target 瘦身:debug 构建产物(含增量/测试)占用大头,按需清理,释放磁盘
#
# 用法:
#   ./scripts/tidy-target.sh            清理 debug + dev-lean(保留 release;推荐日常)
#   ./scripts/tidy-target.sh --all      再清 release(下次 deploy 全量重编)
#   ./scripts/tidy-target.sh --dry      仅统计占用,不删除
#
# 注意:
#   - 离线缓存 CARGO_HOME=仓库内 .cargo-home(约几十~百 MB)请勿删除,否则离线无法编译;
#   - cargo test/acceptance 使用 dev 档案自动落在 target/debug,清空后下次运行会重建;
#   - 精简 debug 编译见 ./scripts/build.sh --debug-lean(占用显著小于默认 dev)。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

size_of() { du -sh "$1" 2>/dev/null | awk '{print $1}' || echo "-"; }

echo "══ target 占用(清理前) ══"
for d in target/debug target/dev-lean target/release target; do
  [ -e "$d" ] && printf "  %-16s %s\n" "$d" "$(size_of "$d")"
done

MODE="${1:-partial}"
if [ "$MODE" = "--dry" ]; then
  echo "(--dry:仅统计,未删除)"
  exit 0
fi

if [ "$MODE" = "--all" ]; then
  echo "清空整个 target(下次编译全量,release 一并重编)"
  rm -rf target
else
  echo "删除 debug 与 dev-lean 产物(release 保留;下次 debug/test 自动重建)"
  rm -rf target/debug target/dev-lean
fi

echo "══ target 占用(清理后) ══"
[ -e target ] && du -sh target | sed 's/^/  target /' || echo "  target 已移除"
echo "提示:离线缓存 .cargo-home 未动;编译产物缺失时先执行 ./scripts/build.sh"
