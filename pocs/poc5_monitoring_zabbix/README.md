# PoC 5: Monitoring (Zabbix)

## Popis

Overit, ze Airflow lze monitorovat pres Zabbix (HTTP Agent → REST API). Cil: sledovat health komponent (scheduler, dag-processor, triggerer), automaticky alertovat pri selhani. Alternativa k poc4 pro prostredi kde uz Zabbix bezi. Detaily viz [ANA-09](../analyses/ANA-09_monitoring_zabbix.md).

Vychazi z [poc4](../poc4_monitoring/README.md) (Prometheus + Grafana).

## Architektura

```
LINKA (Edge Worker)         SERVEROVNA (Centrala)                    MONITORING
┌──────────────────┐        ┌──────────────────────┐                ┌──────────────┐
│ extract_transform│──HTTP─→│ Airflow Server       │←─HTTP Agent───│ Zabbix Server│
│ stroj_1, stroj_2 │        │ (scheduler, worker,  │  /api/v2/...  │ :10051       │
└──────────────────┘        │  apiserver, ...)      │                └──────┬───────┘
                            └──────────────────────┘                       │
                                                                    ┌──────▼───────┐
                                                                    │ Zabbix Web   │
                                                                    │ :8081        │
                                                                    └──────────────┘
```

## Jak funguje monitoring

Na rozdil od poc4 (StatsD → Prometheus), Zabbix **primo vola Airflow REST API**:

1. Zabbix HTTP Agent periodicky (30s) vola `http://airflow-apiserver:8080/api/v2/...`
2. Parsuje JSON odpovedi (JSONPath preprocessing)
3. Vyhodnocuje triggery (scheduler unhealthy → alert)

**Zadny prostredni prekladac** — primo Zabbix → Airflow API.

Detaily viz [ANA-09](../analyses/ANA-09_monitoring_zabbix.md).

## Pristupy

| Sluzba | URL | Prihlaseni |
|--------|-----|------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Zabbix UI | http://localhost:8081 | Admin / zabbix |

## Co Zabbix monitoruje

| Item | Endpoint | Co sleduje |
|------|----------|------------|
| Airflow Healthy | `/api/v2/monitor/health` | Celkovy stav (true/false) |
| Scheduler Status | `/api/v2/monitor/health` | Stav scheduleru (healthy/unhealthy) |
| DAG Processor Status | `/api/v2/monitor/health` | Stav dag-processoru |
| Triggerer Status | `/api/v2/monitor/health` | Stav triggereru |
| DAG Import Errors | `/api/v2/importErrors` | Pocet chyb v DAG souborech |

### Triggery (alerty)

| Trigger | Severity | Podminka |
|---------|----------|----------|
| Airflow is not healthy | DISASTER | `is_healthy = false` |
| Scheduler is unhealthy | HIGH | `scheduler.status != healthy` |
| DAG Processor is unhealthy | AVERAGE | `dag_processor.status != healthy` |
| DAG import errors | WARNING | `import_errors > 0` |

## Spusteni

```bash
docker compose build
docker compose up airflow-init
docker compose up -d

# Pockat ~60s na start Zabbix + Airflow, pak setup:
bash scripts/setup-zabbix.sh
```

## Struktura

```
dags/edge_automotive_etl.py       # Edge ETL (z poc3)
dags/monitoring_demo.py           # Demo workflow (10 tasku)
data/stroj_1/measurements.csv    # vstupni data
data/stroj_2/readings.json       # vstupni data
scripts/setup-zabbix.sh          # Auto-konfigurace Zabbix pres API
Dockerfile                       # Airflow image + edge3 provider
docker-compose.yaml              # 13 kontejneru (Airflow + Zabbix)
```

## Srovnani s poc4 (Prometheus + Grafana)

| | poc4 (Prometheus) | poc5 (Zabbix) |
|---|---|---|
| Transport | StatsD UDP → exporter → scrape | HTTP Agent → REST API |
| Granularita | ~30 infrastrukturnich metrik | Business stav (healthy/DAG ok) |
| Alerting | Potrebuje AlertManager | Built-in (eskalace, email) |
| Dashboardy | Grafana (krasne) | Zabbix UI (funkcni) |
| Pro koho | DevOps (performance tuning) | Ops/ITSM (alert → ticket) |

## Navaznost

- Vychazi z [poc4](../poc4_monitoring/README.md) (Prometheus + Grafana)
- Monitoring analyza viz [ANA-09](../analyses/ANA-09_monitoring_zabbix.md)
- Troubleshooting viz [ANA-01](../poc1_automotive_etl/analyses/ANA-01_troubleshooting.md)
