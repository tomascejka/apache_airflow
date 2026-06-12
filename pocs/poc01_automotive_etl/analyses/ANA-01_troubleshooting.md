# ANA-01: Debugging a zasekle runy

## Zasekle manualni runy

**Pricina**: `logical_date` nastaveny do budoucnosti. Scheduler ceka az cas nastane - tasky zustanou ve stavu `null` (nenaschedulovane).

**Reseni**: pri manualnim triggeru pouzit `logical_date` v minulosti nebo soucasnosti.

## Jak analyzovat problemy

### 1. GUI (nejjednodussi)
DAG → Grid View → klik na run (barevny ctvercek) → klik na task → **Logs** tab

### 2. Filesystem
```
logs/dag_id=<dag>/run_id=<run>/task_id=<task>/attempt=<N>.log
```

### 3. REST API
```bash
# Stav runu
GET /api/v2/dags/{dag_id}/dagRuns/{run_id}

# Stav tasku
GET /api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances

# Oznacit zasekly run jako failed
PATCH /api/v2/dags/{dag_id}/dagRuns/{run_id}  {"state":"failed"}
```

## REST API - ovladani Airflow

Vsechno co jde v GUI jde i pres API (unpause, trigger, mark failed, cteni logu). Uzitecne pro automatizaci a CI/CD.

```bash
# Auth (Airflow 3 = JWT)
TOKEN=$(curl -s -X POST "http://localhost:8080/auth/token" \
  -H "Content-Type: application/json" \
  -d '{"username":"airflow","password":"airflow"}' \
  | sed 's/.*"access_token":"\([^"]*\)".*/\1/')

# Unpause DAG
curl -X PATCH ".../api/v2/dags/{dag_id}" -d '{"is_paused": false}'

# Trigger run
curl -X POST ".../api/v2/dags/{dag_id}/dagRuns" -d '{"logical_date":"2026-06-12T08:00:00Z"}'

# Mark jako failed
curl -X PATCH ".../api/v2/dags/{dag_id}/dagRuns/{run_id}" -d '{"state":"failed"}'
```

## Operator `>>` mezi dvema listy

**Chyba**: `TypeError: unsupported operand type(s) for >>: 'list' and 'list'`

**Pricina**: Airflow nepodporuje `[list] >> [list]` primo. Funguje `single >> [list]`, `[list] >> single`, ale ne `[list] >> [list]`.

```python
# SPATNE
[t_extract_1, t_extract_2] >> [t_load_csv, t_load_db]

# SPRAVNE - explicitne propojit kazdy task
t_extract_1 >> [t_load_csv, t_load_db]
t_extract_2 >> [t_load_csv, t_load_db]
```

**Detekce**: DAG se nenaparsuje → `has_import_errors: true` v API, chyba v dag-processor logu.

## DAG run bez task instances (Airflow 3.x)

**Priznak**: DAG run se okamzite oznaci jako `success` (duration ~0.05s), ale ma 0 task instances.

**Pricina**: V Airflow 3.x musi mit DAG run asociovanou `dag_version`. Tu vytvari dag-processor. Pokud se DAG triggeruje drive, nez dag-processor zpracuje novou verzi DAGu, scheduler nema tasky k naplanovani a run oznaci jako uspesny.

**Reseni**:
- Pockat na scheduled run (ten vzdy dostane spravnou verzi)
- Nebo po zmene DAGu pockat ~30s na dag-processor, teprve pak triggerovat manualne
- Overit v odpovedi: pokud `dag_versions: []`, run nebude mit tasky

**Jak poznat**: `duration < 1s` + `total_entries: 0` v task instances = run bezel "prazdny".

**Doplneni**: REST API trigger (`POST /dagRuns`) v Airflow 3.x konzistentne nevytvari `dag_versions` u runu, i kdyz DAG verze existuje. Workaround: pouzit CLI trigger zevnitr kontejneru:
```bash
docker compose exec airflow-scheduler airflow dags trigger <dag_id>
```

## statsd-exporter: invalid metric type

**Chyba**: `error loading config: invalid metric type ''`

**Pricina**: V `statsd-mapping.yml` je pravidlo s `match_metric_type: ""` — prazdny string neni validni typ.

**Reseni**: Odstranit `match_metric_type` pole nebo pouzit validni typ (counter, gauge, timer, histogram).

```yaml
# SPATNE
- match: "airflow.*"
  name: "airflow_${1}"
  match_type: "regex"
  match_metric_type: ""     # ← toto zpusobi crash

# SPRAVNE
- match: "airflow.*"
  name: "airflow_other"
  labels:
    metric: "$1"
```

## Grafana provisioned dashboard — datasource not found

**Priznak**: Dashboard se nacte, ale panely ukazuji "No data" nebo "Datasource not found".

**Pricina**: Dashboard JSON odkazuje na datasource pres `uid`, ale provisionovany datasource ma jiny UID nez hardcoded v dashboardu.

**Reseni**: Nastavit explicitni `uid` v datasource provisioningu a pouzit stejne UID v dashboardu:
```yaml
# datasources/prometheus.yml
datasources:
  - name: Prometheus
    uid: prometheus          # ← explicitni UID
    type: prometheus
    url: http://prometheus:9090
```
```json
// dashboard JSON
"datasource": { "type": "prometheus", "uid": "prometheus" }
```

## Zabbix 7.x API: "auth" parameter not recognized (FATAL)

**Chyba**: `Invalid request. Invalid parameter "/": unexpected parameter "auth".`

**Pricina**: Zabbix 7.x zmenil autentizaci API. Starsi verze (<=6.x) pouzivaly `"auth": "<token>"` v JSON body. Zabbix 7.x vyzaduje HTTP header `Authorization: Bearer <token>`.

**Reseni**:
```bash
# SPATNE (Zabbix <=6.x styl)
curl -d '{"jsonrpc":"2.0","method":"host.get","params":{},"auth":"TOKEN","id":1}'

# SPRAVNE (Zabbix 7.x)
curl -H "Authorization: Bearer TOKEN" \
     -d '{"jsonrpc":"2.0","method":"host.get","params":{},"id":1}'
```

## Zabbix HTTP Agent: JSONPath nefunguje — $.body. wrapper (WARNING)

**Chyba**: `cannot extract value from json by path "$.scheduler.status": no data matches`

**Pricina**: Zabbix HTTP Agent s `output_format: 1` (JSON) obaluje HTTP odpoved do `$.body.*` objektu. Airflow vraci `{"scheduler":{"status":"healthy"}}`, ale v Zabbix se stane `{"body":{"scheduler":{"status":"healthy"}}}`.

**Reseni**: JSONPath musi zacinat `$.body.`:
```
# SPATNE
$.scheduler.status

# SPRAVNE
$.body.scheduler.status
```

## Zabbix: trends musi byt "0" pro textove items (WARNING)

**Chyba**: `Invalid parameter "/1/trends": value must be 0.`

**Pricina**: Zabbix nepodporuje trendy pro textove hodnoty (value_type 4=text, 1=character). Trends se pocitaji jen pro numericke typy. Spatny predpoklad — ne kazdy item muze mit trends.

**Reseni**: Nastavit `"trends": "0"` pro textove items.

## Zabbix: preprocessing params je string, ne array (WARNING)

**Chyba**: `Invalid parameter "/1/preprocessing/1/params": a character string is expected.`

**Pricina**: V Zabbix 7.x je `preprocessing.params` string (`"$.body.status"`). Starsi dokumentace a priklady ukazuji array format (`["$.body.status"]`). Spatny predpoklad z dokumentace.

**Reseni**:
```json
// SPATNE
"params": ["$.body.status"]

// SPRAVNE
"params": "$.body.status"
```

## Airflow /api/v2/importErrors vyzaduje autentizaci (WARNING)

**Chyba**: `Response code "401" did not match any of the required status codes "200". {"detail":"Not authenticated"}`

**Pricina**: Na rozdil od `/api/v2/monitor/health` (verejny), endpoint `/api/v2/importErrors` vyzaduje JWT token. Spatny predpoklad — ne vsechny API endpointy jsou verejne.

**Reseni**: Pro monitoring pres Zabbix HTTP Agent pouzit pouze verejne endpointy (health), nebo implementovat JWT auth pres externi skript.
