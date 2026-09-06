#!/usr/bin/env bash
# =============================================================================
# rdsctl 配套清理工具:销毁由本套管控创建的全部 Docker 容器与专属网络
#
# 命名约定(rdsctl 生命周期编排):容器 rds-<实例>-master / -slave-N / -proxy-N
#   / 备份用临时容器,网络 rds-<实例>。本脚本按前缀整场清空,不依赖管控端状态。
#
# 用法:
#   scripts/destroy-all-containers.sh                 # 交互确认后执行
#   scripts/destroy-all-containers.sh --yes           # 免确认直接执行
#   scripts/destroy-all-containers.sh --prefix=rds-   # 自定义前缀(默认 rds-)
#
# 说明:
#   - 只删容器与 docker 网络,不触碰宿主数据目录;
#   - 管控页(MySQL/内存)里登记的实例记录仍保留——清场后建议在管控页对这些实例
#     执行「销毁实例 / 彻底删除记录」,或重启 rdsctl 使状态一致;
#   - 环境变量 RDSCTL_DOCKER_PREFIX 可代替 --prefix。
# =============================================================================
set -u

PREFIX="${RDSCTL_DOCKER_PREFIX:-rds-}"
YES=0
DRY=0

usage() {
  sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'
}

for arg in "$@"; do
  case "$arg" in
    -y|--yes|-f|--force) YES=1 ;;
    --dry-run) DRY=1 ;;
    --prefix=*) PREFIX="${arg#--prefix=}" ;;
    -h|--help) usage; exit 0 ;;
    *) echo "未知参数: $arg (见 --help)" >&2; exit 2 ;;
  esac
done

# ── 前置检查与防护 ──
if [ -z "$PREFIX" ]; then
  echo "错误:前缀不能为空" >&2; exit 2
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "错误:未找到 docker 命令" >&2; exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "错误:Docker daemon 未运行或当前用户无权限访问" >&2; exit 1
fi

# ── 收集:以指定前缀开头的容器(名称精确匹配,避免误删无关容器) ──
containers=()
while IFS= read -r cid; do
  [ -n "$cid" ] || continue
  name="$(docker inspect -f '{{.Name}}' "$cid" 2>/dev/null | sed 's#^/##')"
  case "$name" in
    "${PREFIX}"*) containers+=("$name") ;;
  esac
done < <(docker ps -aq 2>/dev/null)

# ── 收集:以指定前缀开头的网络(docker 内置网络绝不处理) ──
networks=()
while IFS= read -r net; do
  [ -n "$net" ] || continue
  case "$net" in
    "${PREFIX}"*) networks+=("$net") ;;
  esac
done < <(docker network ls --format '{{.Name}}' 2>/dev/null | grep -vxE 'bridge|host|none')

nc="${#containers[@]}"
nn="${#networks[@]}"

echo "前缀: $PREFIX"
echo "待销毁容器: $nc 个;待移除网络: $nn 个"
if [ "$nc" -gt 0 ]; then
  printf '  容器: %s\n' "${containers[*]}"
fi
if [ "$nn" -gt 0 ]; then
  printf '  网络: %s\n' "${networks[*]}"
fi

if [ "$nc" -eq 0 ] && [ "$nn" -eq 0 ]; then
  echo "没有需要清理的容器/网络。"
  exit 0
fi
if [ "$DRY" -eq 1 ]; then
  echo "[dry-run] 未执行任何删除。"
  exit 0
fi
if [ "$YES" -ne 1 ]; then
  read -r -p "确认强制删除以上全部容器与网络? (输入 yes 继续): " ans
  [ "$ans" = "yes" ] || { echo "已取消。"; exit 0; }
fi

# ── 执行 ──
fail=0
for c in "${containers[@]}"; do
  if docker rm -f "$c" >/dev/null 2>&1; then
    echo "已销毁容器: $c"
  else
    echo "销毁容器失败(可能已不存在): $c" >&2
    fail=1
  fi
done
for n in "${networks[@]}"; do
  if docker network rm "$n" >/dev/null 2>&1; then
    echo "已移除网络: $n"
  else
    echo "移除网络失败(可能已不存在或被占用): $n" >&2
    fail=1
  fi
done

echo "完成:销毁容器 $nc 个、移除网络 $nn 个。"
echo "提示:管控页中这些实例的记录仍在;请在 rdsctl 页面执行「销毁实例」/「彻底删除记录」或重启服务以同步状态。"
exit "$fail"
