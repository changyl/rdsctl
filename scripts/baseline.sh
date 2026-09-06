#!/usr/bin/env bash
# rdsctl AI-0 手工基线度量(见 docs/ai0-impl-checklist.md §6 / S9 验收记录)
#
# 从控制面 audit_log 离线计算(纯只读,无需改代码、不写审计):
#   1. MTTR(小时):每实例 degrade(sweeper)审计 → 首次后续 recover/destroy 审计的时长;
#   2. 处置步骤数:同窗口内 admin(sweeper 之外)发起的审计动作条数;
#   3. 汇总:窗口数 / 平均 MTTR / 最差 MTTR(含实例) / 未闭环(open)降级窗口数。
#
# 用法:
#   ./scripts/baseline.sh            写入 logs/baseline-mttr-<ts>.csv 并打印摘要
#   ./scripts/baseline.sh --csv      只打印 CSV 内容(stdout),便于前后对比
# 配置经 rdsctl.env / 环境变量(lib.sh):RDSCTL_MYSQL_HOST/PORT/USER/PASS/DB/CLI
# 语义:同实例连续 degrade(未闭环再降级)视为同一窗口,窗口自首个 degrade 起算;
#       末个 degrade 后无 resolve 的窗口计为 open,不参与 MTTR 均值。

set -uo pipefail
# shellcheck source=scripts/lib.sh
. "$(dirname "$0")/lib.sh"

CLI="${RDSCTL_MYSQL_CLI:-mysql}"
DB="${RDSCTL_MYSQL_DB:-rdsctl}"

mysql_q() {
  local sql="$1"
  local args=(-h "$RDSCTL_MYSQL_HOST" -P "$RDSCTL_MYSQL_PORT" -u "$RDSCTL_MYSQL_USER"
    --protocol=tcp --batch --raw --skip-column-names --connect-timeout=5
    --default-character-set=utf8mb4)
  if [ -n "${RDSCTL_MYSQL_PASS:-}" ]; then
    MYSQL_PWD="$RDSCTL_MYSQL_PASS" "$CLI" "${args[@]}" "$DB" -e "$sql"
  else
    "$CLI" "${args[@]}" "$DB" -e "$sql"
  fi
}

tmp="$LOG_DIR/baseline-rows.tmp.$$"
trap 'rm -f "$tmp"' EXIT

mysql_q "SELECT id, ts, instance, action FROM audit_log
         WHERE action IN ('degrade','recover','destroy')
         ORDER BY instance, id;" > "$tmp" \
  || { echo "查询审计失败:确认 MySQL 可达且 DB=$DB" >&2; exit 1; }

out="$LOG_DIR/baseline-mttr-$(date +%Y%m%d-%H%M%S).csv"
: > "$out"

cur=""; wid=""; wts=""

while IFS=$'\t' read -r id ts inst act; do
  [ -n "$inst" ] || continue
  if [ "$inst" != "$cur" ]; then
    cur="$inst"; wid=""; wts=""
  fi
  case "$act" in
    degrade)
      if [ -z "$wts" ]; then wid="$id"; wts="$ts"; fi # 连续 degrade 不重置窗口
      ;;
    recover|destroy)
      if [ -n "$wts" ]; then
        # 窗口内 admin 动作数(sweeper 之外;含 resolve 所属操作人之外的 admin 变更)
        steps="$(mysql_q "SELECT COUNT(*) FROM audit_log \
                 WHERE instance='$inst' AND id >= $wid AND id <= $id AND \`user\` <> 'sweeper'" \
                 | tr -d '[:space:]')"
        steps="${steps:-0}"
        hours="$(awk -v a="$wts" -v b="$ts" 'BEGIN{printf "%.2f", (b-a)/3600.0}')"
        printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$inst" "$wts" "$act" "$ts" "$hours" "$steps" >> "$out"
        wid=""; wts=""
      fi
      ;;
  esac
done < "$tmp"

# 未闭环窗口数 = 事件序列以 degrade 结尾的实例数(rows 已按 instance 排序)
open_n="$(awk -F '\t' '{ last[$3]=$4 } END { n=0; for (i in last) if (last[i]=="degrade") n++; print n }' "$tmp")"
open_n="${open_n:-0}"

if [ "${1:-}" = "--csv" ]; then
  cat "$out"
  exit 0
fi

awk -F '\t' '
BEGIN { n=0; sum=0; worst=0; wi="" }
{
  if ($5=="") next
  n++; sum += $5 + 0;
  if ($5 + 0 > worst + 0) { worst = $5 + 0; wi = $1 }
  printf "%s\t%s\t%s\t%s\t%s\t%s\n", $1,$2,$3,$4,$5,$6
}
END {
  avg = n > 0 ? sprintf("%.2f", sum / n) : "0"
  printf "# windows=%d sum_mttr_h=%.2f avg_mttr_h=%s worst_mttr_h=%.2f(%s)\n", n, sum, avg, worst, wi
}
' "$out"

echo "基线 CSV: $out"
echo "open(未闭环)降级窗口实例数: $open_n(需人工排查)"
