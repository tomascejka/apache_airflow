# ANA-09: Monitoring Airflow pres Zabbix

## Proc Zabbix

V automotive prostredi je Zabbix casto uz nasazeny pro monitoring infrastruktury (servery, site, databaze). Napojeni Airflow na existujici Zabbix eliminuje potrebu dalsi monitoring platformy (Prometheus + Grafana).

## Zabbix — obecne

### Co to je

Open-source monitoring platforma pro sledovani siti, serveru, aplikaci a sluzeb. Vyviji Zabbix SIA (Lotyssko), prvni release 2001. Soucasna verze 7.2.

### Klicove koncepty

| Koncept | Popis |
|---------|-------|
| **Host** | Monitorovany cil (server, aplikace, zarizeni) |
| **Item** | Jednotliva metrika na hostu (CPU, disk, HTTP response) |
| **Trigger** | Podminka nad item → stav OK/PROBLEM |
| **Action** | Reakce na trigger (email, Slack, skript, ticket) |
| **Template** | Sada itemu + triggeru k pripojeni na host |
| **Host Group** | Logicke seskupeni hostu |
| **Discovery (LLD)** | Automaticke nalezeni a vytvoreni itemu (napr. pro kazdy DAG) |
| **Dashboard** | Vizualizace v Zabbix UI |

### Typy itemu (jak Zabbix sbira data)

| Typ | Popis | Priklad pro Airflow |
|-----|-------|---------------------|
| **Zabbix Agent** | Agent na cili cte lokalni data | CPU/RAM hostu kde bezi Airflow |
| **HTTP Agent** | Zabbix primo vola HTTP endpoint | `GET /api/v2/monitor/health` |
| **External Check** | Zabbix spusti skript na serveru | Skript ziska JWT + vola API |
| **Trapper** | Cil posle data do Zabbix (PUSH) | Airflow callback → zabbix_sender |
| **SNMP** | Pro sitove zarizeni | N/A |
| **JMX** | Pro Java aplikace | N/A |

### Preprocessing

Zabbix umi zpracovat odpoved pred ulozenim:
- **JSONPath** — extrahovat hodnotu z JSON (`$.scheduler.status`)
- **Regex** — pattern matching
- **JavaScript** — vlastni logika
- **Change per second** — prevod counteru na rate
- **Check for error** — detekce chyb v odpovedi

### Alerting (silna stranka Zabbix)

```
Trigger            → Action              → Notifikace
(podminka)           (eskalace)             (kanal)
                   ┌─ 0 min: email ops  ─→ ops@firma.cz
scheduler down ────┤─ 15 min: SMS       ─→ +420...
                   └─ 30 min: ITSM      ─→ ServiceNow ticket
```

Zabbix ma **built-in eskalace** — pokud problem neni vyreseny do X minut, eskaluje na dalsi uroven. Prometheus toto potrebuje AlertManager jako dalsi komponentu.

## 3 zpusoby integrace Airflow + Zabbix

### Zpusob 1: HTTP Agent → REST API (pouzivame v poc5)

```
Zabbix Server ──HTTP GET──→ Airflow API (:8080/api/v2/...)
                  ↑
           kazdych 30s
           JSONPath parsing
```

**Co monitoruje**:
- `/api/v2/monitor/health` — stav scheduler, dag-processor, triggerer (bez auth)
- `/api/v2/dags` — seznam DAGu, is_paused stav (s JWT)
- `/api/v2/dags/{id}/dagRuns?order_by=-start_date&limit=1` — posledni run (s JWT)
- `/api/v2/importErrors` — chyby v DAG souborech (bez auth)

**Vyhody**: primo, zadny middleware, Zabbix parsuje JSON nativne
**Nevyhody**: JWT autentizace je slozitejsi, nedostane infrastrukturni metriky (scheduler loop duration apod.)

**Detail — health endpoint**:

Airflow vraci:
```json
{
  "is_healthy": true,
  "scheduler": {
    "status": "healthy",
    "latest_scheduler_heartbeat": "2026-06-12T12:25:28Z"
  },
  "dag_processor": { "status": "healthy", ... },
  "triggerer": { "status": "healthy", ... }
}
```

Zabbix HTTP Agent item:
- URL: `http://airflow-apiserver:8080/api/v2/monitor/health`
- Preprocessing: JSONPath `$.is_healthy` → boolean (0/1)
- Trigger: `last(/airflow-server/airflow.healthy)=0` → DISASTER

**Detail — DAG run monitoring (s JWT)**:

```bash
# 1. Ziskat JWT token
POST /auth/token
Body: {"username":"airflow","password":"airflow"}
Response: {"access_token":"eyJ..."}

# 2. Seznam DAGu (pro discovery)
GET /api/v2/dags
Header: Authorization: Bearer eyJ...
Response: {"dags": [{"dag_id":"monitoring_demo", ...}], "total_entries": 2}

# 3. Posledni run konkretniho DAGu
GET /api/v2/dags/monitoring_demo/dagRuns?order_by=-start_date&limit=1
Response: {"dag_runs": [{"state":"success", "start_date":"...", ...}]}
```

V Zabbix:
- External script ziska token + vola API
- LLD rule objevi DAGy → item prototype `airflow.dag.status[{#DAG_ID}]`
- Trigger prototype: `last(/airflow-server/airflow.dag.status[{#DAG_ID}])="failed"` → HIGH

### Zpusob 2: StatsD → statsd-zabbix-backend

```
Airflow ──StatsD UDP──→ statsd-zabbix-backend ──→ Zabbix Server (trapper items)
```

**Co monitoruje**: Stejne infrastrukturni metriky jako Prometheus (scheduler heartbeat, executor running/queued, pool slots, task duration...).

**Jak funguje**:
1. Airflow odesila StatsD metriky (stejne nastaveni jako v poc4)
2. `statsd-zabbix-backend` (Node.js) prijima UDP, preformatuje a posle do Zabbix pres `zabbix_sender` protokol
3. Zabbix prijme jako **trapper items**

**Konfigurace**:
```yaml
# Airflow env (stejne jako poc4)
AIRFLOW__METRICS__STATSD_ON: 'true'
AIRFLOW__METRICS__STATSD_HOST: 'statsd-server'
AIRFLOW__METRICS__STATSD_PORT: '8125'

# statsd-zabbix-backend config
{
  "zabbixHost": "zabbix-server",
  "zabbixPort": 10051,
  "zabbixSenderHost": "airflow-server"
}
```

**Vyhody**: vsechny infrastrukturni metriky (stejne jako Prometheus), push model
**Nevyhody**: potrebuje Node.js middleware, slozitejsi setup, statsd-zabbix-backend neni oficialne udrzovany

### Zpusob 3: Health endpoint ping (nejjednodussi)

```
Zabbix Server ──HTTP GET──→ http://airflow:8080/api/v2/monitor/health
                              (kazdych 60s, bez auth)
```

**Co monitoruje**: Jen zda Airflow odpovida a je healthy.

**Jak nastavit v Zabbix UI**:
1. Configuration → Hosts → Create Host
2. Items → Create Item:
   - Type: HTTP Agent
   - URL: `http://airflow-apiserver:8080/api/v2/monitor/health`
   - Value type: Text
3. Preprocessing: JSONPath `$.is_healthy`
4. Trigger: `last(/host/item)=0 or nodata(/host/item,120s)`

**Vyhody**: trivialni, bez auth, 5 minut prace
**Nevyhody**: jen zdravotni stav, zadne DAG detaily

## Srovnani 3 zpusobu

| | HTTP Agent (REST API) | StatsD backend | Health ping |
|---|---|---|---|
| **Slozitost** | Stredni | Vysoka | Trivialni |
| **Health check** | Ano | Ne | Ano |
| **DAG stav** | Ano (per DAG) | Ne | Ne |
| **Infra metriky** | Ne | Ano (~30 metrik) | Ne |
| **Auth** | JWT (slozitejsi) | Bez | Bez |
| **Middleware** | Zadny | statsd-zabbix-backend | Zadny |
| **Doporuceni** | PoC + produkce | Jen pokud chcete metriky v Zabbix | Quick-start |

## Srovnani Prometheus vs Zabbix pro Airflow

| Aspekt | Prometheus + Grafana (poc4) | Zabbix (poc5) |
|--------|---------------------------|---------------|
| **Typ monitoringu** | Infrastrukturni metriky (gauge, counter, histogram) | Business monitoring (DAG uspel/selhal) + health |
| **Transport** | StatsD UDP → exporter → scrape | HTTP Agent → REST API |
| **Granularita** | ~30 metrik, 15s interval, percentily | Per-DAG stav, health check, import errors |
| **Alerting** | Potrebuje AlertManager (dalsi komponenta) | Built-in (eskalace, email, Slack, SMS) |
| **Dashboardy** | Grafana (flexibilni, krasne) | Zabbix UI (funkcni, mene vizualne) |
| **Existujici infra** | Nova platforma | Napojeni na existujici Zabbix |
| **Metriky scheduleru** | Ano (heartbeat, loop duration, critical section) | Ne primo (jen health endpoint) |
| **Setup slozitost** | 3 kontejnery (statsd, prometheus, grafana) | 3 kontejnery (zabbix-server, web, postgres) |
| **Oficialni template** | Community dashboardy na Grafana.com | Zadny (ZBXNEXT-5810 otevreny) |

## Doporuceni

**Idealni kombinace** pro produkci:
- **Zabbix** = alerting na business urovni (DAG failed → ticket v ITSM)
- **Prometheus + Grafana** = detailni performance dashboardy pro DevOps

Oboji muze bezet paralelne — Airflow zvlada StatsD i REST API soucasne.

## Implementace

Viz [poc5_monitoring_zabbix](../poc5_monitoring_zabbix/README.md).
