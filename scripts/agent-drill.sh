#!/usr/bin/env bash
# P2-① 远端 agent 真 drill:单机双进程(共享 docker daemon)
#   1) 控制端(rdsctl 9123,memory)创建真实 async 实例;
#   2) 注册 host-agent-a(ip=127.0.0.1, agent_port=9191),把 slave-1 绑定到该 Host;
#   3) agent(9191)进程接管 slave-1 的容器探测:facts 显示 via=agent、agent 日志出现 /agent/sql;
#   4) kill agent → 巡检把 slave-1 标 remote、实例降级,原因含 “agent 未接入”;
#   5) 重启 agent → 巡检恢复 running(经 agent 探测);
#   6) 清理(销毁实例/删 host/杀进程)。
# 前置:docker 可用且本地已有 mysql:8.0 / perf-2shard-newproxy 镜像;已 cargo build。
set -u
PORT=${PORT:-9123}
APORT=${APORT:-9191}
NAME="agdrill01"
HOST="host-agent-a"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG="$ROOT/scripts/logs-agent-drill.log"
AGENT_LOG="$ROOT/scripts/logs-agent-drill-agent.log"
CTRL_LOG="$ROOT/scripts/logs-agent-drill-ctrl.log"
: > "$LOG"; : > "$AGENT_LOG"; : > "$CTRL_LOG"

say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
req(){ local ck=$1; shift; curl -s -H "Cookie: rdsctl_session=$ck" "$@"; }
py(){ python3 -c "$1"; }

say "== 0. 启动 agent($APORT)与 rdsctl($PORT,memory,sweep 2s) =="
pkill -f "rdsctl agent --port $APORT" 2>/dev/null; pkill -f "rdsctl --port $PORT" 2>/dev/null; sleep 1
( cd "$ROOT" && RUST_LOG=debug ./target/debug/rdsctl agent --port $APORT >"$AGENT_LOG" 2>&1 & )
( cd "$ROOT" && RDSCTL_STORE_BACKEND=memory RDSCTL_SWEEP_SECS=2 ./target/debug/rdsctl --port $PORT >"$CTRL_LOG" 2>&1 & )
for i in $(seq 1 20); do curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/login" 2>/dev/null | grep -q 401 && break; sleep 1; done
for i in $(seq 1 20); do curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$APORT/agent/ping" 2>/dev/null | grep -q 200 && break; sleep 1; done
CK=$(curl -s -i -X POST "http://127.0.0.1:$PORT/login" --data "user=admin&password=admin" | grep -o "rdsctl_session=[^;]*" | cut -d= -f2)
say "登录 ok,agent ping ok"

say "== 1. 注册 host-agent-a(agent_port=$APORT)并创建真实 async 实例 $NAME =="
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/hosts?name=$HOST&ip=127.0.0.1&region=cn-north&az=az1&rack=rack-1&cpu=8&mem_gb=16&disk_gb=200&agent_port=$APORT"
say "hosts: $(req "$CK" http://127.0.0.1:$PORT/api/rds/hosts | head -c 200)"
R=$(req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/create?name=$NAME&itype=async&proxies=1&shard_num=1&spec=4C8G&region=cn-north&az=az1&biz=drill&dba=admin&contact=x")
say "create: $R"
TID=$(echo "$R" | py "import sys,json;print(json.load(sys.stdin).get('task_id',''))")
[ -n "$TID" ] || { say "create 未返回 task_id"; exit 1; }
ST=""
for i in $(seq 1 60); do
  ST=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/task?id=$TID" | py "import sys,json;print(json.load(sys.stdin).get('task',{}).get('status',''))")
  [ "$ST" = success ] && break
  [ "$ST" = failed ] && { say "创建失败"; req "$CK" "http://127.0.0.1:$PORT/api/rds/task?id=$TID" | head -c 600 >>"$LOG"; exit 1; }
  sleep 5
done
say "create task=$ST"

say "== 2. 绑定 slave-1 → $HOST(接管其容器探测) =="
SL1="rds-$NAME-slave-1"
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/host/assign?instance=$NAME&node=$SL1&host=$HOST"
req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "
import sys,json; i=json.load(sys.stdin).get('instance',{})
print(' node_hosts=', i.get('node_hosts'))
"

say "== 3. 等待 2-3 轮巡检:slave-1 探测应经 agent(via=agent,facts alive) =="
sleep 14
req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "
import sys,json; i=json.load(sys.stdin).get('instance',{})
print(' status=', i.get('status'))
print(' node_states=', i.get('node_states'))
"
req "$CK" "http://127.0.0.1:$PORT/api/rds/orch/facts?instance=$NAME" | py "
import sys,json
for f in json.load(sys.stdin).get('facts',[]):
    print(' fact', f.get('container'), 'alive', f.get('alive'), 'repl_ok', f.get('repl_ok'), 'via', f.get('via'), 'host', f.get('host'))
"
AG_REQ=$(grep -c "/agent/sql\|/agent/docker\|/agent/state" "$AGENT_LOG" || true)
say "agent 收到请求数 ≈ $AG_REQ(>0 表示路由经 agent)"
[ "$AG_REQ" -gt 0 ] || { say "FAIL: agent 未收到请求"; exit 1; }

say "== 4. kill agent → 巡检把 slave-1 标 remote/降级 =="
pkill -f "rdsctl agent --port $APORT"; sleep 1
sleep 8
req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "
import sys,json; i=json.load(sys.stdin).get('instance',{})
print(' status=', i.get('status'))
print(' node_states=', i.get('node_states'))
print(' last_error=', (i.get('last_error') or '')[:220])
"
ST2=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "import sys,json;print(json.load(sys.stdin).get('instance',{}).get('status',''))")
NS2=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "import sys,json;i=json.load(sys.stdin).get('instance',{});print(i.get('node_states',{}).get('$SL1',''))")
[ "$ST2" = degraded ] && [ "$NS2" = remote ] && say "OK: agent 断开 → degraded + slave-1=remote" || { say "FAIL: 期望 degraded+remote,实际 status=$ST2 node=$NS2"; exit 1; }

say "== 5. 重启 agent → 巡检恢复 =="
( cd "$ROOT" && RUST_LOG=debug ./target/debug/rdsctl agent --port $APORT >>"$AGENT_LOG" 2>&1 & )
for i in $(seq 1 20); do curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$APORT/agent/ping" 2>/dev/null | grep -q 200 && break; sleep 1; done
sleep 10
req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "
import sys,json; i=json.load(sys.stdin).get('instance',{})
print(' status=', i.get('status'))
print(' node_states=', i.get('node_states'))
"
ST3=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "import sys,json;print(json.load(sys.stdin).get('instance',{}).get('status',''))")
NS3=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "import sys,json;i=json.load(sys.stdin).get('instance',{});print(i.get('node_states',{}).get('$SL1',''))")
[ "$ST3" = running ] && [ "$NS3" = ok ] && say "OK: agent 恢复 → running + slave-1=ok" || { say "FAIL: 期望 running+ok,实际 status=$ST3 node=$NS3"; exit 1; }

say "== 6. 清理(等 destroy 终态 → 删实例记录 → 删 host) =="
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/destroy?name=$NAME" >/dev/null
for i in $(seq 1 60); do
  IST=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "import sys,json;print(json.load(sys.stdin).get('instance',{}).get('status',''))")
  [ "$IST" = destroyed ] && break
  sleep 2
done
say "destroy 终态=$IST"
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/instance/delete?name=$NAME" >/dev/null
sleep 1
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/hosts/delete?name=$HOST"
HOSTS=$(req "$CK" http://127.0.0.1:$PORT/api/rds/hosts)
say "hosts 剩余: $HOSTS"
echo "$HOSTS" | grep -q "host-agent-a" && { say "FAIL: host 未删除干净"; exit 1; }
pkill -f "rdsctl agent --port $APORT" 2>/dev/null
pkill -f "rdsctl --port $PORT" 2>/dev/null
say "done: P2-① agent drill 通过"
