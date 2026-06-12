# ANA-08: Monitoring Airflow (Prometheus + Grafana)

## Jak Airflow exportuje metriky

Airflow ma built-in podporu pro **StatsD** metriky. Kazda komponenta (scheduler, worker, dag-processor) odesila metriky ve formatu StatsD. Airflow **nema vlastni /metrics endpoint** (`http://localhost:8080/metrics` vraci 404).

Pro Prometheus je treba **statsd-exporter** — prekladac mezi StatsD a Prometheus formatem:

```
Airflow komponenty ──StatsD UDP──→ statsd-exporter ──HTTP /metrics──→ Prometheus ──→ Grafana
```

## Prometheus — obecne

### Co to je

Open-source time-series databaze s vlastnim dotazovacim jazykem (PromQL). Vyvinul ho SoundCloud (2012), dnes je soucasti CNCF (Cloud Native Computing Foundation) vedle Kubernetes.

### Klicovy princip: PULL model

Vetsina monitorovacich systemu funguje na PUSH (aplikace odesila metriky). Prometheus funguje opacne — PULL (Prometheus si sam stahuje metriky):

```
PUSH (StatsD)                            PULL (Prometheus)
┌─────────┐   UDP     ┌──────────┐       ┌──────────────┐
│ Airflow  ├─────────→│ statsd-  │←─HTTP─│ Prometheus   │
│ scheduler│          │ exporter │ GET   │              │
│ worker   │          │          │/metrics│ kazdych 15s  │
│ ...      │          │ :9125→   │       │ ulozi do TSDB│
└─────────┘           │    :9102 │       └──────────────┘
                      └──────────┘
```

### 4 typy metrik

| Typ | Chovani | Priklad z Airflow |
|-----|---------|-------------------|
| **Counter** | Monotonne roste, nikdy neklesa | `airflow_ti_start` (celkovy pocet startu) |
| **Gauge** | Muze rust i klesat | `airflow_executor_running_tasks` (aktualni pocet) |
| **Histogram** | Distribuce hodnot v bucketech | `airflow_task_duration` (kolik tasku trvalo 0-1s, 1-5s...) |
| **Summary** | Kvantily (p50, p90, p99) na strane klienta | `airflow_scheduler_critical_section_duration{quantile="0.9"}` |

### PromQL — priklady

```promql
# Aktualni hodnota gauge metriky
airflow_executor_running_tasks_CeleryExecutor

# Filtrovani pres label
airflow_pool_open_slots{pool="default_pool"}

# Rate — kolik prirustku za minutu (pro countery)
rate(airflow_ti_start[1m]) * 60

# Percentily ze summary
airflow_scheduler_critical_section_duration{quantile="0.9"}

# Prumer ze summary
airflow_task_duration_sum / airflow_task_duration_count

# Agregace pres label
sum by (job) (airflow_executor_running_tasks_CeleryExecutor)
```

### Prometheus UI

| Sekce | URL | Co ukazuje |
|-------|-----|------------|
| Graph | `/graph` | Interaktivni PromQL editor + graf |
| Targets | `/targets` | Stav scrapovanych targetu (up/down, chyby) |
| Config | `/config` | Aktualni konfigurace |
| Rules | `/rules` | Alerting/recording rules |
| Status > TSDB | `/tsdb-status` | Statistiky databaze |

## Grafana — obecne

### Co to je

Vizualizacni platforma, ktera se pripojuje k datovym zdrojum (datasources). Nepripojuje se k Airflow primo — dotazuje se Promethea.

### Typy panelu

| Typ | Popis | Kdy pouzit |
|-----|-------|------------|
| **stat** | Jedna velka cisla s barevnymi prahy | Heartbeat, aktualni pocty |
| **timeseries** | Casovy graf (cary/sloupce/plochy) | Trendy, prubehy |
| **gauge** | Kruhovy ukazatel | Vyuziti (0-100%) |
| **bar chart** | Sloupcovy graf | Porovnani kategorii |
| **table** | Tabulka | Detailni vypisy |
| **heatmap** | Teplotni mapa | Distribuce latenci |

### Co lze nastavit v panelu

- **Query** — PromQL dotaz (co merit), legendFormat (jak pojmenovat radu)
- **Visualization** — typ grafu, drawStyle (line/bars/points), fillOpacity, stacking
- **Field config** — unit (s, ms, %, bytes...), decimals, min/max, color mode
- **Thresholds** — barevne prahy (zelena < 1s, zluta < 5s, cervena > 5s)
- **Overrides** — prepsani nastaveni pro konkretni radu (jina barva pro "failed")
- **Time range** — panel muze mit vlastni casovy rozsah (nezavisly na dashboardu)

### Auto-provisioning

Grafana podporuje automaticke nastaveni pres soubory:
- `provisioning/datasources/*.yml` — definice zdroju dat
- `provisioning/dashboards/*.yml` — kde hledat JSON dashboardy
- `provisioning/dashboards/json/*.json` — samotne dashboardy

## Pipeline Airflow → Prometheus — detailni konfigurace

### Vrstva 1: Airflow → StatsD UDP

**Kde**: `docker-compose.yaml`, env promenne v `airflow-common-env`

```yaml
AIRFLOW__METRICS__STATSD_ON: 'true'            # zapne odesilani metrik
AIRFLOW__METRICS__STATSD_HOST: 'statsd-exporter'   # kam odesilat
AIRFLOW__METRICS__STATSD_PORT: '9125'           # UDP port
AIRFLOW__METRICS__STATSD_PREFIX: 'airflow'      # prefix vsech metrik
```

**Jak to funguje**: Kazda Airflow komponenta (scheduler, worker, ...) interne vola StatsD klienta. Ten odesila UDP pakety ve formatu:

```
airflow.scheduler.heartbeat:1|c                       ← counter (c)
airflow.executor.running_tasks.CeleryExecutor:2|g      ← gauge (g)
airflow.dag.monitoring_demo.collect_data.duration:15.3|ms  ← timer (ms)
```

**Kdo odesila**: Vsechny komponenty s `STATSD_ON=true` (scheduler, worker, apiserver, dag-processor, triggerer). Edge worker tyto env nema — neposila metriky primo (komunikuje pres API server).

**depends_on**: Airflow ceka na `statsd-exporter: condition: service_started`, aby pri startu neztracela metriky.

### Vrstva 2: statsd-exporter (preklad)

**Kde**: `config/statsd-mapping.yml`

**Co dela**: Prijima StatsD UDP pakety a preklada je do Prometheus formatu. Ma dva porty:
- `:9125` — prijima UDP (StatsD vstup)
- `:9102` — vystavuje HTTP `/metrics` (Prometheus vystup)

**Proc je treba mapping**: StatsD metriky maji "ploche" nazvy (`airflow.pool.open_slots.default_pool`). Prometheus chce strukturovane labels (`airflow_pool_open_slots{pool="default_pool"}`).

```yaml
# Specificke pravidlo: 2 wildcardy → 2 labels
- match: "airflow.dagrun.duration.*.*"
  name: "airflow_dagrun_duration"
  labels:
    dag_id: "$1"       # prvni wildcard = dag_id
    status: "$2"       # druhy wildcard = status (success/failed)

# Specificke pravidlo: 1 wildcard → 1 label
- match: "airflow.pool.open_slots.*"
  name: "airflow_pool_open_slots"
  labels:
    pool: "$1"         # wildcard = pool name

# Catch-all: vse nenamapovane
- match: "airflow.*"
  name: "airflow_other"
  labels:
    metric: "$1"
```

**Co se da tunovat v mappingu**:
- `match_type: "regex"` — regex misto glob patterns
- `timer_type: histogram` — timer metriky jako histogram misto summary
- `buckets: [0.01, 0.1, 1, 5, 10]` — vlastni histogram buckety
- `quantiles` — ktere percentily pocitat (default: 0.5, 0.9, 0.99)

### Vrstva 3: Prometheus (scraping + ulozeni)

**Kde**: `config/prometheus.yml`

```yaml
global:
  scrape_interval: 15s       # jak casto scrapovat
  evaluation_interval: 15s   # jak casto vyhodnocovat alerting rules

scrape_configs:
  - job_name: 'airflow-statsd'
    static_configs:
      - targets: ['statsd-exporter:9102']   # odkud scrapovat
        labels:
          service: 'airflow'                # pridany label
```

**Co se deje kazdych 15s**:
1. Prometheus posle `GET http://statsd-exporter:9102/metrics`
2. Dostane odpoved v Prometheus text formatu
3. Ulozi hodnoty do TSDB (Time Series Database) na disk (`prometheus-data` volume)

**Co se da tunovat**:

| Parametr | Popis | Nas stav |
|----------|-------|----------|
| `scrape_interval` | Frekvence scrapovani | 15s (dobry default) |
| `scrape_timeout` | Timeout jednoho scrapu | 10s (auto) |
| `retention.time` | Jak dlouho drzet data | 15d (default) |
| `retention.size` | Max velikost TSDB | bez limitu |
| `static_configs` | Rucne definovane targety | 1 target |
| `service_discovery` | Auto-discovery (K8s, Consul) | nepouzivame |
| `alerting.rules` | Pravidla pro alerty | nemame |
| `remote_write` | Odesilat do vzdaleneho uloziste (Thanos) | nepouzivame |

### Vrstva 4: Grafana (vizualizace)

**Kde**: `config/grafana/provisioning/`

```
provisioning/
  datasources/prometheus.yml    # pripojeni na Prometheus (uid: prometheus)
  dashboards/dashboards.yml     # kde hledat JSON soubory
  dashboards/json/airflow.json  # samotny dashboard (19 panelu)
```

Klicove: datasource musi mit explicitni `uid: prometheus` a dashboard JSON musi odkazovat na stejne UID:
```yaml
# datasources/prometheus.yml
uid: prometheus                 # explicitni UID
```
```json
// dashboard panel
"datasource": { "type": "prometheus", "uid": "prometheus" }
```

## Kompletni prehled metrik

### Scheduler metriky
| Metrika | Typ | Popis |
|---------|-----|-------|
| `airflow_scheduler_heartbeat` | counter | Heartbeat — zije/nezije |
| `airflow_scheduler_dagruns_running` | gauge | Pocet bezicich DAG runu |
| `airflow_scheduler_tasks_executable` | gauge | Tasky pripravene ke spusteni |
| `airflow_scheduler_tasks_starving` | gauge | Tasky, ktere nemohou ziskat slot |
| `airflow_scheduler_critical_section_duration` | summary | Doba drzeni DB zamku (p50/p90/p99) |
| `airflow_scheduler_critical_section_query_duration` | summary | SQL dotaz v kriticke sekci |
| `airflow_scheduler_scheduler_loop_duration` | summary | Doba jednoho scheduling cyklu |
| `airflow_scheduler_orphaned_tasks_adopted` | counter | Osirely tasky — prebrany jinym workerem |
| `airflow_scheduler_orphaned_tasks_cleared` | counter | Osirely tasky — vycisteny |

### Executor metriky
| Metrika | Typ | Popis |
|---------|-----|-------|
| `airflow_executor_running_tasks_{Executor}` | gauge | Bezici tasky per executor |
| `airflow_executor_queued_tasks_{Executor}` | gauge | Tasky ve fronte per executor |
| `airflow_executor_open_slots_{Executor}` | gauge | Volne sloty per executor |

### Pool metriky
| Metrika | Typ | Popis |
|---------|-----|-------|
| `airflow_pool_running_slots` | gauge | Bezici sloty v poolu |
| `airflow_pool_open_slots` | gauge | Volne sloty v poolu |
| `airflow_pool_queued_slots` | gauge | Tasky cekajici na slot |
| `airflow_pool_deferred_slots` | gauge | Odlozene tasky |
| `airflow_pool_scheduled_slots` | gauge | Naplanovane tasky |
| `airflow_pool_starving_tasks` | gauge | Hladove tasky (zadny slot) |

### DAG run metriky
| Metrika | Typ | Popis |
|---------|-----|-------|
| `airflow_dagrun_duration_success` | summary | Doba behu uspesnych runu |
| `airflow_dagrun_dependency_check` | summary | Doba kontroly zavislosti |

### Task instance metriky
| Metrika | Typ | Popis |
|---------|-----|-------|
| `airflow_ti_start` | counter | Celkovy pocet startu (vsechny tasky) |
| `airflow_ti_start_{dag}_{task}` | counter | Starty per task |
| `airflow_ti_finish` | counter | Celkovy pocet dokonceni |
| `airflow_ti_finish_{dag}_{task}_{status}` | counter | Dokonceni per task + status |
| `airflow_ti_running` | gauge | Aktualne bezici task instances |
| `airflow_ti_running_{pool}_{dag}_{task}` | gauge | Bezici per pool/dag/task |
| `airflow_task_duration` | summary | Doba behu tasku (p50/p90/p99) |
| `airflow_task_scheduled_duration` | summary | Scheduling delay (cekani na slot) |

### Per-DAG task metriky
| Metrika | Typ | Popis |
|---------|-----|-------|
| `airflow_dag_{dag}_{task}_duration` | summary | Doba behu konkretniho tasku |
| `airflow_dag_{dag}_{task}_scheduled_duration` | summary | Scheduling delay per task |

### Ostatni
| Metrika | Typ | Popis |
|---------|-----|-------|
| `airflow_asset_orphaned` | gauge | Osirely assety |
| `airflow_other{metric="operator_successes"}` | counter | Uspesne operatory (catch-all) |
| `airflow_other{metric="job_start"}` | counter | Starty jobu (catch-all) |

## Dashboard panely (19)

| # | Panel | Typ | Co zobrazuje |
|---|-------|-----|-------------|
| 1 | Scheduler Heartbeat | stat | Zije scheduler? Zelena=ano |
| 2 | DAG Runs Running | stat | Kolik runu prave bezi |
| 3 | Tasks Executable | stat | Kolik tasku je pripraveno ke spusteni |
| 4 | Tasks Starving | stat | Tasky bez dostupneho slotu (cervena=problem) |
| 5 | Executor Running & Queued | timeseries | Bezici/cekajici tasky per executor (Celery vs Edge) |
| 6 | Executor Open Slots | timeseries | Volne sloty per executor |
| 7 | Pool All Slot States | timeseries | Kompletni stav default_pool (running/open/queued/deferred/scheduled/starving) |
| 8 | Pool Starving Tasks | timeseries | Detail hladovych tasku |
| 9 | Critical Section Duration | timeseries | DB zamek scheduleru (p50/p90/p99) |
| 10 | Scheduler Loop Duration | timeseries | Scheduling cyklus (p50/p90/p99) |
| 11 | Critical Section Query Duration | timeseries | SQL dotaz v kriticke sekci |
| 12 | Orphaned Tasks | timeseries | Osirely tasky (adopted vs cleared) |
| 13 | DAG Run Duration (success) | timeseries | Doba behu uspesnych runu |
| 14 | DAG Run Dependency Check | timeseries | Doba kontroly zavislosti |
| 15 | Task Instance Starts | timeseries (bars) | Rate startu per minuta |
| 16 | Task Instance Finishes | timeseries (bars) | Rate dokonceni per minuta |
| 17 | Task Duration (global) | timeseries | Doba behu tasku (p50/p90/p99) |
| 18 | Task Scheduled Duration | timeseries | Scheduling delay — cekani na slot |
| 19 | Task Instance Currently Running | timeseries (stacked) | Ktere tasky prave bezi (stack area) |

## Co metriky nepokryvaji

- Procento dokonceni jednoho tasku (Airflow nevystavuje "task progress" — task je bud running nebo success/failed)
- Edge worker metriky primo (edge worker nekomunikuje pres StatsD, ale pres API server)

## Co produkce prida (nemame, ale jde)

1. **Alerting rules** v Prometheus — napr. "pokud scheduler heartbeat chybi > 60s → alert"
2. **AlertManager** — dalsi kontejner, routuje alerty do Slacku/emailu
3. **Recording rules** — predpocitane metriky pro rychlejsi dashboardy
4. **Service discovery** — misto `static_configs` auto-discovery (pri skalovani workeru)
5. **Long-term storage** — Thanos/Cortex pro data starsi nez 15 dni
