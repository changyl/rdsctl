#!/usr/bin/env bash
# 受管切换(orch_reparent)真实 MySQL 演练:内存后端 + 真实 docker mysql 主从
# 前置:docker 可用且本地已有 mysql:8.0 / perf-2shard-newproxy 镜像
set -u
PORT=${PORT:-9123}
NAME="orchdrill01"
LOG=/tmp/orch-drill.log
: > "$LOG"

say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
req(){ # req <cookie> <curl args...>
  local ck=$1; shift
  curl -s -H "Cookie: rdsctl_session=$ck" "$@"
}
py(){ python3 -c "$1"; }

say "== 0. 启动 rdsctl(memory) =="
pkill -f "rdsctl --port $PORT" 2>/dev/null; sleep 1
( cd "$(dirname "$0")/.." && RDSCTL_STORE_BACKEND=memory ./target/debug/rdsctl --port $PORT >/tmp/orch-rds.log 2>&1 & )
for i in $(seq 1 20); do curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/login" 2>/dev/null | grep -q 401 && break; sleep 1; done
CK=$(curl -s -i -X POST "http://127.0.0.1:$PORT/login" --data "user=admin&password=admin" | grep -o "rdsctl_session=[^;]*" | cut -d= -f2)
say "登录 ok(token ${CK:0:6}…)"

say "== 1. 创建真实 async 实例 $NAME(1 主 1 读从 + 代理) =="
R=$(req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/create?name=$NAME&itype=async&proxies=1&shard_num=1&spec=4C8G&region=cn-north&az=az1&biz=drill&dba=admin&contact=x")
say "create: $R"
TID=$(echo "$R" | py "import sys,json;print(json.load(sys.stdin).get('task_id',''))")
[ -n "$TID" ] || { say "create 未返回 task_id: $R"; exit 1; }

say "== 2. 等待创建任务终态(最长 300s) =="
ST=""
for i in $(seq 1 60); do
  ST=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/task?id=$TID" | py "import sys,json;print(json.load(sys.stdin).get('task',{}).get('status',''))")
  [ "$ST" = success ] && break
  [ "$ST" = failed ] && { say "创建失败"; req "$CK" "http://127.0.0.1:$PORT/api/rds/task?id=$TID" | head -c 500 >>"$LOG"; exit 1; }
  sleep 5
done
say "create task=$ST"

say "== 3. 核对登记主从 + 事实层 =="
sleep 3
req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "
import sys,json; i=json.load(sys.stdin).get('instance',{})
print(' status=',i.get('status'))
for n in i.get('nodes',[]): print(' node',n['container'],n['role'])
"
req "$CK" "http://127.0.0.1:$PORT/api/rds/orch/facts?instance=$NAME" | py "
import sys,json
for f in json.load(sys.stdin).get('facts',[]): print(' fact',f.get('container'),'role',f.get('role'),'alive',f.get('alive'),'repl_ok',f.get('repl_ok'))
"

say "== 4. 受管切换(auto/ERS):候选=slave-1 =="
R=$(req "$CK" -X POST "http://127.0.0.1:$PORT/api/rds/orch/reparent?instance=$NAME&mode=auto")
say "reparent: $R"
for i in $(seq 1 24); do
  sleep 5
  MASTER=$(req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "import sys,json;i=json.load(sys.stdin).get('instance',{});print(next((n['container'] for n in i.get('nodes',[]) if n['role']=='master'),''))")
  [ "$MASTER" = "rds-$NAME-slave-1" ] && break
done
say "切换后 master 容器 = $MASTER"

say "== 5. 直查 MySQL 事实 =="
echo " 新主 read_only(应为 0):" | tee -a "$LOG"
docker exec "rds-$NAME-slave-1" sh -c 'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N -e "$1"' _ "SELECT @@read_only" 2>/dev/null | tee -a "$LOG"
echo " 旧主 read_only(应为 1):" | tee -a "$LOG"
docker exec "rds-$NAME-master" sh -c 'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD" -N -e "$1"' _ "SELECT @@read_only" 2>/dev/null | tee -a "$LOG"

say "== 6. 核对登记角色 =="
req "$CK" "http://127.0.0.1:$PORT/api/rds/instance?name=$NAME" | py "
import sys,json; i=json.load(sys.stdin).get('instance',{})
print(' status=',i.get('status'))
for n in i.get('nodes',[]): print(' node',n['container'],n['role'])
"

say "== 7. 清理(容器/网络/服务) =="
docker rm -f "rds-$NAME-master" "rds-$NAME-slave-1" "rds-$NAME-proxy-1" >/dev/null 2>&1
docker network rm "rds-$NAME" >/dev/null 2>&1
pkill -f "rdsctl --port $PORT" 2>/dev/null
say "done(见 $LOG)"
