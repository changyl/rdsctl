#!/usr/bin/env bash
# P2-③ 实例整体迁移(migrate_instance)真 drill:单机双进程(共享 docker daemon)
#   1) agent(9191)+ 控制端(9123,memory,sweep 2s);注册 host-mg(region=cn-east az=azN);
#   2) 创建真实 async 实例 mgdrill01(cn-north/az1,主 + slave-1 + slave-2);
#   3) POST /api/rds/migrate?region=cn-east&az=azN&hosts=host-mg —— 全节点迁到目标机房:
#      从节点同身份替换(临时名追平→换名),主节点临时新主追平→老主只读→摘→换名→可写;
#   4) 校验:任务 success;实例 region/az=目标;node_hosts 全部=host-mg;
#      主/从容器身份在线、无临时残留;新主可写、主写→从读一致、facts 全经 agent;
#   5) 清理。
# 前置:docker + mysql:8.0/perf-2shard-newproxy 本地镜像;已 cargo build。
set -u
PORT=${PORT:-9123}
APORT=${APORT:-9191}
NAME="mgdrill01"
HOST="host-mg"
REGION="cn-east"
AZ="azN"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG="$ROOT/scripts/logs-migrate-drill.log"
AGENT_LOG="$ROOT/scripts/logs-migrate-drill-agent.log"
CTRL_LOG="$ROOT/scripts/logs-migrate-drill-ctrl.log"
: > "$LOG"; : > "$AGENT_LOG"; : > "$CTRL_LOG"

say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
req(){ local ck=$1; shift; curl -s -H "Cookie: rdsctl_session=$ck" "$@"; }
py(){ python3 -c "$1"; }
MX="rds-$NAME-master"

say "== 0. 启动 agent($APORT)与 rdsctl($PORT,memory,sweep 2s) =="
pkill -f "rdsctl agent --port $APORT" 2>/dev/null; pkill -f "rdsctl --port $PORT" 2>/dev/null
docker ps -a --format '{{.Names}}' | grep "^rds-$NAME\|-rnx\|-mgn" | xargs -r docker rm -f >/dev/null 2>&1
docker network ls --format '{{.Name}}' | grep "rds-$NAME" | xargs -r docker network rm >/dev/null 2>&1
sleep 1
( cd "$ROOT" && RUST_LOG=debug ./target/debug/rdsctl agent --port $APORT >"$AGENT_LOG" 2>&1 & )
( cd "$ROOT" && RDSCTL_STORE_BACKEND=memory RDSCTL_SWEEP_SECS=2 ./target/debug/rdsctl --port $PORT >"$CTRL_LOG" 2>&1 & )
for i in $(seq 1 20); do curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/login" 2>/dev/null | grep -q 401 && break; sleep 1; done
for i in $(seq 1 20); do curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$APORT/agent/ping" 2>/dev/null | grep -q 200 && break; sleep 1; done
CK=$(curl -s -i -X POST "http://127.0.0.1:$PORT/login" --data "user=admin&password=admin" | grep -o "rdsctl_session=[^;]*" | cut -d= -f2)
say "登录 ok,agent ping ok"

say "== 1. 注册 $HOST($REGION/$AZ)并创建真实 async 实例 $NAME(cn-north/az1) =="
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/hosts?name=$HOST&ip=127.0.0.1&region=$REGION&az=$AZ&rack=rack-1&cpu=16&mem_gb=32&disk_gb=400&agent_port=$APORT" >/dev/null
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
docker exec "$MX" mysql -N -uroot -prds_root_2024 -e \
  "CREATE DATABASE IF NOT EXISTS appdb; CREATE TABLE IF NOT EXISTS appdb.kv (k VARCHAR(64) PRIMARY KEY, v VARCHAR(128)); INSERT INTO appdb.kv VALUES ('mig','ok') ON DUPLICATE KEY UPDATE v='ok';"

say "== 2. migrate_instance → $REGION/$AZ @ $HOST =="
R=$(req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/migrate?instance=$NAME&region=$REGION&az=$AZ&hosts=$HOST")
say "migrate: $R"
TID=$(echo "$R" | py "import sys,json;print(json.load(sys.stdin).get('task_id',''))")
[ -n "$TID" ] || { say "migrate 未返回 task_id: $R"; exit 1; }
for i in $(seq 1 200); do
  ST=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/task?id=$TID" | py "import sys,json;print(json.load(sys.stdin).get('task',{}).get('status',''))")
  [ "$ST" = success ] && break
  [ "$ST" = failed ] && { say "迁移失败"; req "$CK" "http://127.0.0.1:$PORT/api/rds/task?id=$TID" | python3 -m json.tool >>"$LOG"; exit 1; }
  sleep 3
done
say "migrate task=$ST"

say "== 3. 校验 =="
req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "
import sys,json; i=json.load(sys.stdin).get('instance',{})
print(' region/az =', i.get('region'), '/', i.get('az'))
print(' node_hosts =', i.get('node_hosts'))
"
REG=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "import sys,json;i=json.load(sys.stdin).get('instance',{});print(i.get('region','')+'/'+i.get('az',''))")
[ "$REG" = "$REGION/$AZ" ] || { say "FAIL: 实例事实未更新($REG)"; exit 1; }
NHS=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "import sys,json;print(json.load(sys.stdin).get('instance',{}).get('node_hosts',{}))")
echo "$NHS" | grep -q "$HOST" || { say "FAIL: node_hosts 缺失 $HOST($NHS)"; exit 1; }
for C in "$MX" "rds-$NAME-slave-1" "rds-$NAME-slave-2"; do
  docker ps -a --format '{{.Names}}' | grep -qx "$C" || { say "FAIL: 容器 $C 缺失"; exit 1; }
done
say "OK: 主/从容器身份全部在线"
docker ps -a --format '{{.Names}}' | grep -E -- "-rnx|-mgn" && { say "FAIL: 存在临时残留容器"; exit 1; } || say "OK: 无临时残留"
# 新主可写 + 主写→从读一致
sleep 4
docker exec "$MX" mysql -N -uroot -prds_root_2024 -e "INSERT INTO appdb.kv VALUES ('after-mig','ok') ON DUPLICATE KEY UPDATE v='ok';" 2>/dev/null \
  || { say "FAIL: 新主不可写"; exit 1; }
sleep 6
V=$(docker exec "rds-$NAME-slave-1" mysql -N -uroot -prds_root_2024 -e "SELECT v FROM appdb.kv WHERE k='after-mig'" 2>/dev/null | tr -d '\r')
say "迁移后主写→从读: '$V'"
[ "$V" = ok ] || { say "FAIL: 迁移后复制未跟上($V)"; exit 1; }
sleep 3
req "$CK" "http://127.0.0.1:$PORT/api/rds/orch/facts?instance=$NAME" | py "
import sys,json
for f in json.load(sys.stdin).get('facts',[]):
    print(' fact', f.get('container'), 'role', f.get('role'), 'alive', f.get('alive'), 'repl_ok', f.get('repl_ok'), 'via', f.get('via'), 'host', f.get('host'))
"
AGN=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/orch/facts?instance=$NAME" | py "import sys,json;print(sum(1 for f in json.load(sys.stdin).get('facts',[]) if f.get('via')=='agent'))")
[ "$AGN" -ge 3 ] && say "OK: facts 全部经 agent($AGN 节点)" || { say "FAIL: facts 未全走 agent($AGN)"; exit 1; }

say "== 4. 清理 =="
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/destroy?name=$NAME" >/dev/null
for i in $(seq 1 60); do
  IST=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "import sys,json;print(json.load(sys.stdin).get('instance',{}).get('status',''))")
  [ "$IST" = destroyed ] && break; sleep 2
done
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/instance/delete?name=$NAME" >/dev/null
req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/hosts/delete?name=$HOST" >/dev/null
pkill -f "rdsctl agent --port $APORT" 2>/dev/null; pkill -f "rdsctl --port $PORT" 2>/dev/null
say "done: P2-③ migrate_instance drill 通过"
