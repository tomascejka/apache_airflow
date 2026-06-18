# ANA-18: Skalovani — kolik edge workeru zvladne jeden central server

## Kontext

Zatim testovano jen 1 edge worker + 2 stroje (poc02, poc03). V produkci muze byt desitky stroju na vice linkach. Kolik edge workeru muze bezet proti jednomu centralu?

## Jak Edge Worker komunikuje s centralou

```
Edge Worker ──HTTP poll──> API Server ──> Metadata DB (edge_job, edge_worker, edge_logs)
             <──task──
Edge Worker ──heartbeat──> API Server ──> Metadata DB (stav workeru)
Edge Worker ──log chunks──> API Server ──> Metadata DB / filesystem
```

**Klicove mechanismy:**
1. **Job polling**: edge periodicky polla API server pro nove tasky (HTTP GET, konfigurovatelny interval)
2. **Heartbeat**: pravidelne hlaseni stavu (HTTP POST, interval konfigurovatelny)
3. **Log upload**: chunked upload logu behem behu tasku (HTTP POST, 512KB chunky)

Vsechna komunikace je HTTP → metadata DB. **Zadny message broker** (na rozdil od Celery).

## Zname limity

### Edge Worker specificke

| Parametr | Default | Popis |
|----------|---------|-------|
| `edge.job_poll_interval` | 5s | Jak casto edge polla pro nove tasky |
| `edge.heartbeat_interval` | 30s | Jak casto edge posila heartbeat |
| `edge.worker_concurrency` | 8 | Max paralelnich tasku na jednom edge workeru |

**Zname skalovani**: ~80 edge workeru na jednom centralu je testovane (dle dokumentace). Neni to hard limit, ale zkusenost.

### Bottlenecky pri skalovani

| Bottleneck | Proc | Reseni |
|------------|------|--------|
| **Metadata DB connections** | Kazdy poll/heartbeat = DB query | PGBouncer (connection pooling) |
| **API server zatez** | N workeru × polling interval = N×12 requestu/min (pri 5s) | Vice API serveru za load balancerem |
| **Scheduler parsing** | Vic DAGu = delsi parsing loop | `min_file_process_interval`, mene souboru v dags/ |
| **DB transaction volume** | Kazda zmena stavu tasku = DB write | SSD, dostatecna RAM pro PostgreSQL |

### Vypocet zateze pro nas use case

**Scenar: 20 stroju, 20 edge workeru** (1 edge per stroj)

```
Heartbeaty:   20 workeru × 1 heartbeat/30s = 40 requestu/min
Job polling:  20 workeru × 1 poll/5s = 240 requestu/min
Log upload:   20 workeru × ~2 chunky/task = zavisi na soubehu
─────────────────────────────────────────────────────────
Celkem:       ~300 HTTP requestu/min = trivijalni zatez
```

**Scenar: 50 stroju, 50 edge workeru**

```
Heartbeaty:   50 × 2/min = 100 requestu/min
Job polling:  50 × 12/min = 600 requestu/min
─────────────────────────────────────────────
Celkem:       ~700 HTTP requestu/min = stale zvladnutelne
```

Pro srovnani: bezny webovy server zvladne tisice requestu/min.

## Konfigurace pro skalovani

### Airflow core

```ini
[core]
parallelism = 32              # Max celkovy pocet soubeznich tasku
max_active_tasks_per_dag = 16 # Max tasku na 1 DAG naraz
max_active_runs_per_dag = 4   # Max soubeznich runu 1 DAGu

[database]
sql_alchemy_pool_size = 10    # Connection pool (zvysit pri vic workerech)
sql_alchemy_max_overflow = 20 # Extra connections nad pool_size

[scheduler]
min_file_process_interval = 30 # Jak casto re-parsovat DAG soubory (s)
```

### Edge specificke

```ini
[edge]
job_poll_interval = 5         # Zvysit na 10-15s pri 50+ workerech
heartbeat_interval = 30       # Zvysit na 60s pri 50+ workerech
```

### PGBouncer (doporuceno pri 20+ workerech)

```yaml
# docker-compose.yaml
pgbouncer:
  image: pgbouncer/pgbouncer
  environment:
    DATABASES_HOST: postgres
    DATABASES_PORT: 5432
    DATABASES_USER: airflow
    DATABASES_PASSWORD: airflow
    DATABASES_DBNAME: airflow
    POOL_MODE: transaction
    MAX_CLIENT_CONN: 200
    DEFAULT_POOL_SIZE: 25
```

## Skalovaci stupne

| Stupen | Edge workeru | Opatreni | HW central |
|--------|-------------|----------|------------|
| **Small** | 1–10 | Default konfigurace | 4 CPU, 8GB RAM, SSD |
| **Medium** | 10–30 | PGBouncer, zvysit pool_size | 8 CPU, 16GB RAM, SSD |
| **Large** | 30–80 | Prodlouzit poll/heartbeat intervaly, vice API serveru | 16 CPU, 32GB RAM, SSD RAID |
| **XL** | 80+ | Nezkoumane; zvazit sharding nebo dedicated API servery | ? |

## Doporuceni pro nas use case

### Aktualni stav (PoC)

- 1 edge worker, 2 stroje → **zadne skalovaci opatreni potreba**

### Planovany stav (produkce, odhad)

- 5–20 stroju = 5–20 edge workeru → **Small/Medium stupen**
- PGBouncer jako prevence (jednoduchy setup)
- Default konfigurace staci, zvysit `sql_alchemy_pool_size` na 15–20

### Kdy skalovat dal

- Pokud scheduler latence > 30s → vice CPU, `min_file_process_interval`
- Pokud DB connections vypadavaji → PGBouncer, zvysit pool
- Pokud API server nestihá → druhy API server za Nginx load balancerem

## Otevrene otazky

| # | Otazka | Dopad |
|---|--------|-------|
| 1 | Kolik stroju bude v produkci? (5, 20, 50+?) | Urcuje skalovaci stupen |
| 2 | Bude 1 edge worker per stroj, nebo 1 edge per linka (vice stroju)? | Ovlivnuje pocet workeru |
| 3 | Jak casto bezi ETL? (kazdych 5 min vs kazda hodina) | Ovlivnuje soubehy tasku |

## Souvisejici analyzy

- [ANA-04](ANA-04_edge_worker_architektura.md) — Edge Worker architektura
- [ANA-05](ANA-05_edge_etl_flow_design.md) — Edge ETL design
- [ANA-15](ANA-15_backup_metadata_db.md) — PostgreSQL backup (DB je bottleneck)
