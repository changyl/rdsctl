#!/usr/bin/env bash
# P2-② 节点替换(replace_node)真 drill:单机双进程(共享 docker daemon)
#   1) agent(9191)+ 控制端(9123,memory,sweep 2s);注册 host-rp(agent_port=9191);
#   2) 创建真实 async 实例 rpdrill01(主 + slave-1 + slave-2),主库写一条数据;
#   3) POST /api/rds/replace_node 把 slave-1 迁到 host-rp(身份=容器名不变,
#      「临时名容器追平 → 摘旧 → 换名上线」,读流量不中断);
#   4) 校验:任务 success;实例 node_hosts[slave-1]=host-rp;容器换名成功(临时名消失);
#      facts 中 slave-1 via=agent/host=host-rp;替换后复制数据一致(主写→从读);
#   5) 清理。
# 前置:docker + mysql:8.0/perf-2shard-newproxy 本地镜像;已 cargo build。
set -u
PORT=${PORT:-9123}
APORT=${APORT:-9191}
NAME="rpdrill01"
HOST="host-rp"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG="$ROOT/scripts/logs-replace-drill.log"
AGENT_LOG="$ROOT/scripts/logs-replace-drill-agent.log"
CTRL_LOG="$ROOT/scripts/logs-replace-drill-ctrl.log"
: > "$LOG"; : > "$AGENT_LOG"; : > "$CTRL_LOG"

say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
req(){ local ck=$1; shift; curl -s -H "Cookie: rdsctl_session=$ck" "$@"; }
py(){ python3 -c "$1"; }
SL1="rds-$NAME-slave-1"

say "== 0. 启动 agent($APORT)与 rdsctl($PORT,memory,sweep 2s) =="
pkill -f "rdsctl agent --port $APORT" 2>/dev/null; pkill -f "rdsctl --port $PORT" 2>/dev/null
docker ps -a --format '{{.Names}}' | grep "^$NAME\|-rnx$" | xargs -r docker rm -f >/dev/null 2>&1
docker network ls --format '{{.Name}}' | grep "rds-$NAME" | xargs -r docker network rm >/dev/null 2>&1
sleep 1
( cd "$ROOT" && RUST_LOG=debug ./target/debug/rdsctl agent --port $APORT >"$AGENT_LOG" 2>&1 & )
( cd "$ROOT" && RDSCTL_STORE_BACKEND=memory RDSCTL_SWEEP_SECS=2 ./target/debug/rdsctl --port $PORT >"$CTRL_LOG" 2>&1 & )
for i in $(seq 1 20); do curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/login" 2>/dev/null | grep -q 401 && break; sleep 1; done
for i in $(seq 1 20); do curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$APORT/agent/ping" 2>/dev/null | grep -q 200 && break; sleep 1; done
CK=$(curl -s -i -X POST "http://127.0.0.1:$PORT/login" --data "user=admin&password=admin" | grep -o "rdsctl_session=[^;]*" | cut -d= -f2)
say "登录 ok,agent ping ok"

say "== 1. 注册 $HOST(agent_port=$APORT)并创建真实 async 实例 $NAME =="
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/hosts?name=$HOST&ip=127.0.0.1&region=cn-north&az=az1&rack=rack-1&cpu=8&mem_gb=16&disk_gb=200&agent_port=$APORT" >/dev/null
R=$(req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/create?name=$NAME&itype=async&proxies=1&shard_num=1&spec=4C8G&region=cn-north&az=az1&biz=drill&dba=admin&contact=x")
TID=$(echo "$R" | py "import sys,json;print(json.load(sys.stdin).get('task_id',''))")
[ -n "$TID" ] || { say "create 失败: $R"; exit 1; }
for i in $(seq 1 60); do
  ST=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/task?id=$TID" | py "import sys,json;print(json.load(sys.stdin).get('task',{}).get('status',''))")
  [ "$ST" = success ] && break
  [ "$ST" = failed ] && { say "创建失败"; exit 1; }
  sleep 5
done
say "create task=$ST"
# 主库写一条初始数据
docker exec "rds-$NAME-master" sh -c 'exec mysql -N -uroot -p"$MYSQL_ROOT_PASSWORD" -e "$1"' _ \
  "CREATE DATABASE IF NOT EXISTS appdb; CREATE TABLE IF NOT EXISTS appdb.kv (k VARCHAR(64) PRIMARY KEY, v VARCHAR(128)); INSERT INTO appdb.kv VALUES ('replace','ok') ON DUPLICATE KEY UPDATE v='ok';"

say "== 2. replace_node:把 $SL1 迁到 $HOST =="
R=$(req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/replace_node?instance=$NAME&node=$SL1&host=$HOST")
say "replace: $R"
TID=$(echo "$R" | py "import sys,json;print(json.load(sys.stdin).get('task_id',''))")
[ -n "$TID" ] || { say "replace 未返回 task_id: $R"; exit 1; }
for i in $(seq 1 90); do
  ST=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/task?id=$TID" | py "import sys,json;print(json.load(sys.stdin).get('task',{}).get('status',''))")
  [ "$ST" = success ] && break
  [ "$ST" = failed ] && { say "替换失败"; req "$CK" "http://127.0.0.1:$PORT/api/rds/task?id=$TID" | head -c 800 >>"$LOG"; exit 1; }
  sleep 3
done
say "replace task=$ST"

say "== 3. 校验 =="
req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "
import sys,json; i=json.load(sys.stdin).get('instance',{})
print(' status=', i.get('status'))
print(' node_hosts=', i.get('node_hosts'))
for n in i.get('nodes',[]):
    if n['container'].endswith('slave-1') or n['role']=='master': print(' node', n['container'], n['role'], 'host_port', n['host_port'], 'server_id', n['server_id'])
"
NH=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "import sys,json;print(json.load(sys.stdin).get('instance',{}).get('node_hosts',{}).get('$SL1',''))")
[ "$NH" = "$HOST" ] || { say "FAIL: 绑定未更新到 $HOST($NH)"; exit 1; }
docker ps -a --format '{{.Names}}' | grep -qx "$SL1" && say "OK: 正式容器 $SL1 存在" || { say "FAIL: $SL1 容器缺失"; exit 1; }
docker ps -a --format '{{.Names}}' | grep -qx "$SL1-rnx" && { say "FAIL: 残留临时容器 $SL1-rnx"; exit 1; } || say "OK: 临时容器已清理"
# 替换后复制数据一致:主写 → 从读
docker exec "rds-$NAME-master" sh -c 'exec mysql -N -uroot -p"$MYSQL_ROOT_PASSWORD" -e "$1"' _ "INSERT INTO appdb.kv VALUES ('after','replace') ON DUPLICATE KEY UPDATE v='replace';" 2>/dev/null
sleep 6
V=$(docker exec "rds-$NAME-slave-1" sh -c 'exec mysql -N -uroot -p"$MYSQL_ROOT_PASSWORD" -e "$1"' _ "SELECT v FROM appdb.kv WHERE k='after'" 2>/dev/null | tr -d '\r')
say "替换后从库读到 k=after -> '$V'"
[ "$V" = replace ] || { say "FAIL: 替换后复制未跟上($V)"; exit 1; }
sleep 3
req "$CK" "http://127.0.0.1:$PORT/api/rds/orch/facts?instance=$NAME" | py "
import sys,json
for f in json.load(sys.stdin).get('facts',[]):
    print(' fact', f.get('container'), 'alive', f.get('alive'), 'repl_ok', f.get('repl_ok'), 'via', f.get('via'), 'host', f.get('host'))
"
FV=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/orch/facts?instance=$NAME" | py "
import sys,json
for f in json.load(sys.stdin).get('facts',[]):
    if f.get('container')=='$SL1': print(f.get('via','')+'|'+str(f.get('alive',False))+'|'+str(f.get('repl_ok',False)))
")
say "slave-1 facts = $FV"
echo "$FV" | grep -q "^agent|True|True" || { say "FAIL: facts 应为 agent 路径且正常($FV)"; exit 1; }

say "== 4. 清理 =="
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/destroy?name=$NAME" >/dev/null
for i in $(seq 1 60); do
  IST=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "import sys,json;print(json.load(sys.stdin).get('instance',{}).get('status',''))")
  [ "$IST" = destroyed ] && break; sleep 2
done
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/instance/delete?name=$NAME" >/dev/null
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/hosts/delete?name=$HOST" >/dev/null
pkill -f "rdsctl agent --port $APORT" 2>/dev/null; pkill -f "rdsctl --port $PORT" 2>/dev/null
say "done: P2-② replace_node drill 通过"
