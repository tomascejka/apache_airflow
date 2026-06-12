# ANA-06: Airflow REST API v2

## Zakladni info

- Airflow 3.x poskytuje REST API na `http://<host>:8080/api/v2/...`
- Swagger UI (OpenAPI docs): `http://<host>:8080/docs`
- Autentizace: **JWT token** (Airflow 2.x pouzival Basic Auth)
- Vsechno co jde v GUI jde i pres API

## Autentizace

```bash
# Ziskat JWT token
TOKEN=$(curl -s -X POST "http://localhost:8080/auth/token" \
  -H "Content-Type: application/json" \
  -d '{"username":"airflow","password":"airflow"}' \
  | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Pouziti tokenu
curl -H "Authorization: Bearer $TOKEN" "http://localhost:8080/api/v2/..."
```

Token ma omezenou platnost. Pro dlouhodobou automatizaci je treba obnovat.

## Prehled endpointu (seskupene)

### 1. DAGs - sprava DAGu

| Metoda | Endpoint | Co dela |
|--------|----------|---------|
| `GET` | `/api/v2/dags` | Seznam vsech DAGu |
| `GET` | `/api/v2/dags/{dag_id}` | Detail DAGu (stav, is_paused, last_parsed, import_errors) |
| `PATCH` | `/api/v2/dags/{dag_id}` | Upravit DAG (pause/unpause: `{"is_paused": false}`) |
| `DELETE` | `/api/v2/dags/{dag_id}` | Smazat DAG |

```bash
# Unpause DAG
curl -X PATCH ".../api/v2/dags/{dag_id}" \
  -H "Content-Type: application/json" \
  -d '{"is_paused": false}'
```

### 2. DAG Runs - sprava behu

| Metoda | Endpoint | Co dela |
|--------|----------|---------|
| `GET` | `/api/v2/dags/{dag_id}/dagRuns` | Seznam vsech runu pro DAG |
| `GET` | `/api/v2/dags/{dag_id}/dagRuns/{run_id}` | Detail runu (state, duration, dag_versions) |
| `POST` | `/api/v2/dags/{dag_id}/dagRuns` | **Spustit novy run** (trigger) |
| `PATCH` | `/api/v2/dags/{dag_id}/dagRuns/{run_id}` | Upravit run (napr. oznacit jako failed) |
| `DELETE` | `/api/v2/dags/{dag_id}/dagRuns/{run_id}` | Smazat run |

```bash
# Trigger DAG run (logical_date v minulosti!)
curl -X POST ".../api/v2/dags/{dag_id}/dagRuns" \
  -H "Content-Type: application/json" \
  -d '{"logical_date": "2026-06-11T10:00:00Z"}'

# Oznacit zasekly run jako failed
curl -X PATCH ".../api/v2/dags/{dag_id}/dagRuns/{run_id}" \
  -H "Content-Type: application/json" \
  -d '{"state": "failed"}'
```

### 3. Task Instances - stav tasku

| Metoda | Endpoint | Co dela |
|--------|----------|---------|
| `GET` | `/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances` | Seznam vsech task instances pro run |
| `GET` | `.../taskInstances/{task_id}` | Detail task instance (hostname, executor, state) |
| `GET` | `.../taskInstances/{task_id}/logs/{attempt}` | **Logy tasku** (JSON format) |

```bash
# Zjistit kde bezel kazdy task (hostname, executor)
curl ".../api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances" \
  -H "Authorization: Bearer $TOKEN"
```

### 4. Import Errors - chyby v DAGech

| Metoda | Endpoint | Co dela |
|--------|----------|---------|
| `GET` | `/api/v2/importErrors` | Seznam vsech import erroru (spatne napsane DAGy) |

```bash
# Zjistit proc se DAG nenaparsoval
curl ".../api/v2/importErrors" -H "Authorization: Bearer $TOKEN"
# Odpoved obsahuje stack_trace s detailni chybou
```

### 5. Monitoring - health checks

| Metoda | Endpoint | Co dela |
|--------|----------|---------|
| `GET` | `/api/v2/monitor/health` | Health check (scheduler, dag-processor, triggerer) |
| `GET` | `/api/v2/version` | Verze Airflow |

```bash
# Health check (nevyzaduje auth)
curl "http://localhost:8080/api/v2/monitor/health"
```

### 6. Ostatni uzitecne endpointy

| Metoda | Endpoint | Co dela |
|--------|----------|---------|
| `GET` | `/api/v2/variables` | Airflow variables (key-value konfigurace) |
| `GET` | `/api/v2/connections` | Connections (DB, API, atd.) |
| `GET` | `/api/v2/pools` | Worker pools (limity paralelismu) |
| `GET` | `/api/v2/dags/{dag_id}/tasks` | Definice tasku v DAGu (ne instance, ale definice) |
| `GET` | `/api/v2/config` | Cela Airflow konfigurace |

## Tipy pro troubleshooting

### DAG se nenaparsoval
```bash
# 1. Zkontroluj import errors
curl ".../api/v2/importErrors"
# 2. Zkontroluj DAG info
curl ".../api/v2/dags/{dag_id}"  # has_import_errors, is_stale
```

### Run se zasekl / nebezel
```bash
# 1. Zkontroluj stav runu
curl ".../api/v2/dags/{dag_id}/dagRuns/{run_id}"
# Sleduj: state, duration, dag_versions

# 2. Zkontroluj task instances
curl ".../api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
# total_entries=0 + duration<1s = run bezel prazdny (chybi dag_version)

# 3. Precti logy tasku
curl ".../api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1"

# 4. Killni zasekly run
curl -X PATCH ".../api/v2/dags/{dag_id}/dagRuns/{run_id}" \
  -d '{"state":"failed"}'
```

### Overeni distribuovaneho zpracovani (edge vs central)
```bash
# Kazdy task instance ma 'hostname' a 'executor' pole
# Ruzne hostnames = ruzne stroje
curl ".../api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances" | \
  python -c "import sys,json; [print(f'{t[\"task_id\"]}: {t[\"hostname\"]} ({t.get(\"executor\",\"default\")})') for t in json.load(sys.stdin)['task_instances']]"
```

## URL encoding pozor

Run ID casto obsahuje `+` (timezone offset, napr. `manual__2026-06-12T10:11:25+00:00`). V curl je treba URL-encodovat: `%2B` misto `+`, `%3A` misto `:`.

## Prakticke pouziti (overeno v PoC)

| Ucel | Metody |
|------|--------|
| CI/CD - automaticky deploy a test | `PATCH dags` (unpause) + `POST dagRuns` (trigger) + polling `GET dagRuns` |
| Monitoring | `GET monitor/health` + `GET importErrors` |
| Debugging | `GET taskInstances` (hostname/state) + `GET logs` |
| Killnuti zaseklych runu | `GET dagRuns` (najit) + `PATCH dagRuns` (state=failed) |
| Validace edge workeru | `GET taskInstances` → porovnat hostnames |
