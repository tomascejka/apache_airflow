#!/bin/bash
# Setup Zabbix monitoring for Airflow
# Spousti se po startu stacku: bash scripts/setup-zabbix.sh
#
# POZNAMKY K ZABBIX 7.x API:
# - Auth: "Authorization: Bearer <token>" header (ne "auth" pole v JSON body)
# - preprocessing.params: string (ne array)
# - trends: musi byt "0" pro textove items (value_type 1,4)
# - output_format: 1 (JSON) obaluje odpoved do $.body.* — JSONPath musi zacinat $.body.
# - trigger expressions s uvozovkami: pouzit echo | curl -d @-

ZABBIX_URL="http://localhost:8081/api_jsonrpc.php"
ZABBIX_USER="Admin"
ZABBIX_PASS="zabbix"

AIRFLOW_HOST="airflow-apiserver"
AIRFLOW_PORT="8080"
AIRFLOW_URL="http://$AIRFLOW_HOST:$AIRFLOW_PORT"

echo "=== Zabbix Setup for Airflow Monitoring ==="
echo ""

zabbix_call() {
  local METHOD="$1"
  local PARAMS="$2"
  curl -s -X POST "$ZABBIX_URL" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $AUTH_TOKEN" \
    -d "{\"jsonrpc\":\"2.0\",\"method\":\"$METHOD\",\"params\":$PARAMS,\"id\":1}"
}

parse_result_id() {
  local KEY="$1"
  echo "$2" | python -c "import sys,json; r=json.load(sys.stdin); print(r.get('result',{}).get('${KEY}',[''])[0])" 2>/dev/null
}

check_error() {
  echo "$1" | python -c "import sys,json; r=json.load(sys.stdin); e=r.get('error',{}); print(e.get('data',''))" 2>/dev/null
}

# --- 1. Login ---
echo "[1/5] Logging into Zabbix API..."
AUTH_RESULT=$(curl -s -X POST "$ZABBIX_URL" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"user.login","params":{"username":"'"$ZABBIX_USER"'","password":"'"$ZABBIX_PASS"'"},"id":1}')

AUTH_TOKEN=$(echo "$AUTH_RESULT" | python -c "import sys,json; print(json.load(sys.stdin).get('result',''))" 2>/dev/null)

if [ -z "$AUTH_TOKEN" ]; then
  echo "  FAILED: $AUTH_RESULT"
  exit 1
fi
echo "  OK (token: ${AUTH_TOKEN:0:8}...)"

# --- 2. Create Host Group ---
echo "[2/5] Creating host group 'Airflow'..."
HG_RESULT=$(zabbix_call "hostgroup.create" '{"name":"Airflow"}')
GROUP_ID=$(parse_result_id "groupids" "$HG_RESULT")

if [ -z "$GROUP_ID" ]; then
  HG_FIND=$(zabbix_call "hostgroup.get" '{"filter":{"name":["Airflow"]}}')
  GROUP_ID=$(echo "$HG_FIND" | python -c "import sys,json; print(json.load(sys.stdin)['result'][0]['groupid'])" 2>/dev/null)
  echo "  Exists (groupid: $GROUP_ID)"
else
  echo "  Created (groupid: $GROUP_ID)"
fi

# --- 3. Create Host ---
echo "[3/5] Creating host 'Airflow Server'..."
HOST_RESULT=$(zabbix_call "host.create" "{
  \"host\":\"airflow-server\",
  \"name\":\"Airflow Server\",
  \"groups\":[{\"groupid\":\"$GROUP_ID\"}],
  \"interfaces\":[{\"type\":1,\"main\":1,\"useip\":1,\"ip\":\"0.0.0.0\",\"dns\":\"\",\"port\":\"10050\"}]
}")
HOST_ID=$(parse_result_id "hostids" "$HOST_RESULT")

if [ -z "$HOST_ID" ]; then
  HOST_FIND=$(zabbix_call "host.get" '{"filter":{"host":["airflow-server"]}}')
  HOST_ID=$(echo "$HOST_FIND" | python -c "import sys,json; print(json.load(sys.stdin)['result'][0]['hostid'])" 2>/dev/null)
  echo "  Exists (hostid: $HOST_ID)"
else
  echo "  Created (hostid: $HOST_ID)"
fi

# --- 4. Create Items ---
echo "[4/5] Creating monitoring items..."

create_item() {
  local NAME="$1"
  local KEY="$2"
  local URL="$3"
  local VALUE_TYPE="$4"   # 0=float, 3=unsigned, 4=text
  local JSONPATH="$5"     # JSONPath (starts with $.body. for output_format=1)

  local TRENDS="30d"
  if [ "$VALUE_TYPE" = "4" ] || [ "$VALUE_TYPE" = "1" ]; then
    TRENDS="0"
  fi

  local PREPROC_JSON=""
  if [ -n "$JSONPATH" ]; then
    PREPROC_JSON=",\"preprocessing\":[{\"type\":\"12\",\"params\":\"$JSONPATH\",\"error_handler\":\"0\",\"error_handler_params\":\"\"}]"
  fi

  RESULT=$(zabbix_call "item.create" "{
    \"name\":\"$NAME\",
    \"key_\":\"$KEY\",
    \"hostid\":\"$HOST_ID\",
    \"type\":19,
    \"url\":\"$URL\",
    \"value_type\":$VALUE_TYPE,
    \"delay\":\"30s\",
    \"history\":\"7d\",
    \"trends\":\"$TRENDS\",
    \"request_method\":0,
    \"output_format\":1,
    \"status\":0
    $PREPROC_JSON
  }")

  ERROR=$(check_error "$RESULT")
  if [ -n "$ERROR" ] && echo "$ERROR" | grep -qi "already exists"; then
    echo "  - $NAME (exists)"
  elif [ -n "$ERROR" ]; then
    echo "  - $NAME (ERROR: $ERROR)"
  else
    echo "  - $NAME (OK)"
  fi
}

# Health - raw JSON (text, no preprocessing)
create_item "Airflow Health (raw)" "airflow.health.raw" \
  "$AIRFLOW_URL/api/v2/monitor/health" 4 ""

# Scheduler status ($.body. prefix because output_format=1 wraps response)
create_item "Scheduler Status" "airflow.scheduler.status" \
  "$AIRFLOW_URL/api/v2/monitor/health" 4 '$.body.scheduler.status'

# DAG Processor status
create_item "DAG Processor Status" "airflow.dagprocessor.status" \
  "$AIRFLOW_URL/api/v2/monitor/health" 4 '$.body.dag_processor.status'

# Triggerer status
create_item "Triggerer Status" "airflow.triggerer.status" \
  "$AIRFLOW_URL/api/v2/monitor/health" 4 '$.body.triggerer.status'

# Metadatabase status
create_item "Metadatabase Status" "airflow.metadatabase.status" \
  "$AIRFLOW_URL/api/v2/monitor/health" 4 '$.body.metadatabase.status'

# Scheduler heartbeat (text → timestamp)
create_item "Scheduler Last Heartbeat" "airflow.scheduler.heartbeat" \
  "$AIRFLOW_URL/api/v2/monitor/health" 4 '$.body.scheduler.latest_scheduler_heartbeat'

# --- 5. Create Triggers ---
echo "[5/5] Creating triggers..."

create_trigger() {
  local DESC="$1"
  local EXPR="$2"
  local PRIORITY="$3"

  RESULT=$(echo "{\"jsonrpc\":\"2.0\",\"method\":\"trigger.create\",\"params\":{\"description\":\"$DESC\",\"expression\":\"$EXPR\",\"priority\":$PRIORITY},\"id\":1}" \
    | curl -s -X POST "$ZABBIX_URL" \
      -H "Content-Type: application/json" \
      -H "Authorization: Bearer $AUTH_TOKEN" \
      -d @-)

  ERROR=$(check_error "$RESULT")
  if [ -n "$ERROR" ] && echo "$ERROR" | grep -qi "already exists"; then
    echo "  - $DESC (exists)"
  elif [ -n "$ERROR" ]; then
    echo "  - $DESC (ERROR: $ERROR)"
  else
    echo "  - $DESC (OK)"
  fi
}

create_trigger \
  "Airflow Scheduler is unhealthy" \
  'find(/airflow-server/airflow.scheduler.status,#1,\"like\",\"healthy\")=0' \
  4

create_trigger \
  "Airflow DAG Processor is unhealthy" \
  'find(/airflow-server/airflow.dagprocessor.status,#1,\"like\",\"healthy\")=0' \
  3

create_trigger \
  "Airflow Triggerer is unhealthy" \
  'find(/airflow-server/airflow.triggerer.status,#1,\"like\",\"healthy\")=0' \
  3

create_trigger \
  "Airflow Metadatabase is unhealthy" \
  'find(/airflow-server/airflow.metadatabase.status,#1,\"like\",\"healthy\")=0' \
  5

create_trigger \
  "Airflow Health endpoint not responding" \
  "nodata(/airflow-server/airflow.health.raw,120s)=1" \
  5

echo ""
echo "=== Setup complete ==="
echo ""
echo "Zabbix UI:  http://localhost:8081  (Admin / zabbix)"
echo "Airflow UI: http://localhost:8080  (airflow / airflow)"
echo ""
echo "Zabbix -> Monitoring -> Latest data -> Host: Airflow Server"
