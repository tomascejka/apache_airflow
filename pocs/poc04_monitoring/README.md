# PoC 4: Monitoring (Prometheus + Grafana)

## Popis

Overit, ze Airflow lze monitorovat pres Prometheus a vizualizovat v Grafane. Cil: videt infrastrukturni metriky (scheduler heartbeat, executor load, pool vyuziti, task duration) v realnem case. Obsahuje demo workflow s 10 tasky pro vizualizaci zateze. Detaily viz [ANA-08](../analyses/ANA-08_monitoring.md).

Vychazi z [poc03](../poc03_edge_etl/README.md) (Edge ETL Varianta B).

## Architektura

```
LINKA (Edge Worker)         SERVEROVNA (Centrala)                    MONITORING
┌──────────────────┐        ┌──────────────────────┐                ┌──────────────┐
│ extract_transform│──HTTP─→│ Airflow Server       │──StatsD UDP──→│ statsd-export│
│ stroj_1, stroj_2 │        │ (scheduler, worker,  │                │ :9102        │
└──────────────────┘        │  apiserver, ...)      │                └──────┬───────┘
                            └──────────────────────┘                       │ scrape
                                                                    ┌──────▼───────┐
                                                                    │ Prometheus   │
                                                                    │ :9090        │
                                                                    └──────┬───────┘
                                                                           │ query
                                                                    ┌──────▼───────┐
                                                                    │ Grafana      │
                                                                    │ :3000        │
                                                                    └──────────────┘
```

## Jak funguje monitoring

Airflow **nema vlastni /metrics endpoint**. Metriky odesila pres **StatsD protokol** (UDP):

1. Airflow komponenty (scheduler, worker, ...) odesiaji metriky na `statsd-exporter:9125` (UDP)
2. `statsd-exporter` prevadi StatsD format na Prometheus format → `http://statsd-exporter:9102/metrics`
3. `Prometheus` scrapuje metriky kazdych 15s
4. `Grafana` vizualizuje data z Promethea

Detaily viz [ANA-08](../analyses/ANA-08_monitoring.md).

## Pristupy

| Sluzba | URL | Prihlaseni |
|--------|-----|------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | (bez auth) |
| statsd-exporter metriky | http://localhost:9102/metrics | (bez auth) |

## DAGy

### edge_automotive_etl (z poc03)
Edge ETL workflow — extract+transform na lince, load na centrale. Viz [poc03](../poc03_edge_etl/README.md).

### monitoring_demo (novy)
Demo workflow s 10 tasky pro vizualizaci v Grafane:

```
[collect_data] → [validate] → [process_batch_1] ─┐
                               [process_batch_2] ─┤
                               [process_batch_3] ─├→ [aggregate] → [export_csv]
                               [process_batch_4] ─┘               → [export_db]
                                                                   → [notify]
```

- Kazdy task simuluje praci pres `time.sleep()` (10-40s)
- Loguje prubeh po 10% krocich
- Celkova doba behu ~100s

## Validace (2026-06-12)

Monitoring stack overen:
- statsd-exporter prijima metriky z Airflow (scheduler heartbeat, executor slots, pool stats)
- Prometheus scrapuje a uklada data
- Grafana dashboard "Airflow Monitoring" auto-provisionovany s 19 panely pokryvajicimi vsechny dostupne Airflow metriky (scheduler, executor, pool, DAG run, task instance, task duration)

Demo DAG `monitoring_demo` dobehl za ~100s, v Grafane viditelny prubeh (running tasks 0→4→0).

## Co Airflow metriky pokryvaji

| Typ | Priklady |
|-----|----------|
| Scheduler | heartbeat, critical section duration, schedule delay |
| Executor | running/queued/open slots (per executor: Celery, Edge) |
| Pool | running/open/queued/deferred slots |
| DAG run | duration (per dag_id, per status) |
| Task | successes, failures count |
| DAG processing | parse time, errors |

**Co metriky nepokryvaji**: procento dokonceni jednoho tasku (Airflow nevystavuje "task progress" metriku — task je bud running nebo success/failed).

## Struktura

```
dags/edge_automotive_etl.py                            # Edge ETL (z poc03)
dags/monitoring_demo.py                                # Demo workflow (10 tasku)
data/stroj_1/measurements.csv                          # vstupni data
data/stroj_2/readings.json                             # vstupni data
config/prometheus.yml                                  # Prometheus scrape konfigurace
config/statsd-mapping.yml                              # StatsD → Prometheus mapping
config/grafana/provisioning/datasources/prometheus.yml # Auto-provisioning datasource
config/grafana/provisioning/dashboards/dashboards.yml  # Dashboard provider
config/grafana/provisioning/dashboards/json/airflow.json # Airflow dashboard (19 panelu)
Dockerfile                                             # Airflow image + edge3 provider
docker-compose.yaml                                    # 11 kontejneru (Airflow + monitoring)
trigger-and-watch.ps1                                  # PS1 skript: trigger DAG + otevre Grafanu + polluje stav
```

## Spusteni

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

### Trigger + sledovani v Grafane

```powershell
# PowerShell skript — triggeruje DAG, otevre Grafanu, polluje stav
.\trigger-and-watch.ps1                          # monitoring_demo (default)
.\trigger-and-watch.ps1 -DagId edge_automotive_etl  # jiny DAG
.\trigger-and-watch.ps1 -SkipBrowser             # bez otevreni browseru
```

Alternativne rucne:
```bash
docker compose exec airflow-scheduler airflow dags unpause monitoring_demo
docker compose exec airflow-scheduler airflow dags trigger monitoring_demo
# Otevre Grafanu: http://localhost:3000/d/airflow-monitoring
```

Pozn: Pouzivat CLI trigger, ne REST API — viz [troubleshooting](../poc01_automotive_etl/analyses/ANA-01_troubleshooting.md).

## Navaznost

- Vychazi z [poc03](../poc03_edge_etl/README.md) (Edge ETL)
- Monitoring analyza viz [ANA-08](../analyses/ANA-08_monitoring.md)
- Troubleshooting viz [ANA-01](../poc01_automotive_etl/analyses/ANA-01_troubleshooting.md)
