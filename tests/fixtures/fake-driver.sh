#!/bin/sh
# rdsctl 外部驱动 —— 最小样本(供接入自测 / 契约对照 / 验收演练使用)
#
# 它不做任何真实编排:全部按 JSON 契约回答固定结果。用途:
#   1) 对照它写自己的驱动(骨架最小、无依赖);
#   2) 验证控制面 ↔ agent ↔ 外部驱动整条链路是通的:
#        RDSCTL_RUNTIME=external RDSCTL_RUNTIME_CMD=$PWD/tests/fixtures/fake-driver.sh \
#          ./target/debug/rdsctl agent --port 9191
#        curl -s http://127.0.0.1:9191/agent/ping   # → runtime=external
#        curl -s -X POST http://127.0.0.1:9191/agent/create -d '{"spec":{"name":"t1","image":"x"}}'
#
# 契约见 docs/container-platform-abstraction.md §5。

read -r line

case "$line" in
  *'"action":"caps"'*)
    echo '{"ok":true,"caps":{"host_ports":true,"bind_mounts":true,"named_volumes":true,"networks":true,"exec":true,"logs":true,"rename":true,"systemd_unit":false,"raw_flags":false}}'
    ;;
  *'"action":"create"'*)   echo '{"ok":true,"out":"fake: created"}' ;;
  *'"action":"start"'*)    echo '{"ok":true}' ;;
  *'"action":"stop"'*)     echo '{"ok":true}' ;;
  *'"action":"restart"'*)  echo '{"ok":true}' ;;
  *'"action":"remove"'*)   echo '{"ok":true}' ;;
  *'"action":"rename"'*)   echo '{"ok":true}' ;;
  *'"action":"exists"'*)   echo '{"ok":true,"exists":true}' ;;
  *'"action":"state"'*)    echo '{"ok":true,"state":"running|0|0","health":"none"}' ;;
  *'"action":"health"'*)   echo '{"ok":true,"health":"none"}' ;;
  *'"action":"logs"'*)     echo '{"ok":true,"out":"fake-driver: no logs"}' ;;
  *'"action":"exec_raw"'*) echo '{"ok":true,"out":"","err":"","code":0}' ;;
  *'"action":"exec"'*)     echo '{"ok":true,"out":"1"}' ;;
  *'"action":"write_file"'*) echo '{"ok":true}' ;;
  *'"action":"network_ensure"'*) echo '{"ok":true}' ;;
  *'"action":"network_remove"'*) echo '{"ok":true}' ;;
  *'"action":"expose"'*)   echo '{"ok":true,"addr":"127.0.0.1:33060"}' ;;
  *)
    # 未实现的 action 必须显式说清楚(不要静默成功)
    echo '{"ok":false,"error":"fake-driver: 未实现的 action","code":"unsupported","cap":"platform"}'
    ;;
esac
