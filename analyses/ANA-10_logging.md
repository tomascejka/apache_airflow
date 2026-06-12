# ANA-10: Logovani v Airflow (vcetne Edge Workeru)

## Jak Airflow loguje — obecne

Airflow ma 2 typy logu:

| Typ | Co obsahuje | Kde se ulozi |
|-----|-------------|-------------|
| **Component logs** | Scheduler, worker, dag-processor, triggerer systemove logy | stdout/stderr kontejneru |
| **Task logs** | Vystup kazdeho tasku (print, logging, chybove hlasky) | Soubor na disku / remote storage |

Task logy jsou klicove — obsahuji co task vypsal, chybove traceback, dobu behu. Jsou dostupne v Airflow UI pod kazdym taskem (tab "Logs").

### Kde se task logy ukladaji

```
logs/
  dag_id=<dag>/
    run_id=<run_id>/
      task_id=<task>/
        attempt=1.log
        attempt=2.log      ← retry
```

Kazdy pokus (attempt) ma vlastni soubor. Airflow UI cte tyto soubory a zobrazuje je.

## Logovaci architektura

```
                          LOKALNI                          REMOTE
┌──────────────┐     ┌──────────────┐              ┌──────────────┐
│ Task         │────→│ FileTask     │─── upload ──→│ S3 / GCS /   │
│ (print,      │     │ Handler      │              │ WASB / HDFS  │
│  logging)    │     │ (lokalni log)│              └──────────────┘
└──────────────┘     └──────────────┘
                           │                       ┌──────────────┐
                           └── stream ────────────→│ Elasticsearch│
                                                   │ CloudWatch   │
                                                   └──────────────┘
```

### Task Handler typy

| Handler | Kam pise | Kdy pouzit |
|---------|----------|------------|
| **FileTaskHandler** | Lokalni soubor | Default, jednoduchy setup |
| **S3TaskHandler** | AWS S3 | Cloud, sdileny pristup |
| **GCSTaskHandler** | Google Cloud Storage | GCP prostredi |
| **WasbTaskHandler** | Azure Blob Storage | Azure prostredi |
| **ElasticsearchTaskHandler** | Elasticsearch | Full-text search, streaming |
| **CloudwatchTaskHandler** | AWS CloudWatch | AWS nativni logging |
| **StackdriverTaskHandler** | GCP Operations | GCP nativni logging |

## Edge Worker — jak loguje

### Standardni Airflow worker (Celery)

Celery worker bezi ve stejne siti jako scheduler/webserver. Sdili volume (`logs/`), takze task logy jsou primo dostupne v UI.

### Edge Worker — jina situace

Edge worker bezi **remote** (jina sit, jiny stroj). Nema sdileny volume se serverem. Jak se logy dostanou na server?

**Edge3 provider resi log transfer automaticky:**

```
EDGE WORKER                                          CENTRALA
┌──────────────────┐                                ┌──────────────────┐
│ Task bezi        │                                │ API Server       │
│                  │   HTTP POST (chunks)           │                  │
│ stdout/stderr ──→│ ──────────────────────────────→│ ulozi do logs/   │
│ lokalni log file │   /edge_worker/v1/logs         │                  │
│                  │   kazdych N bajtu (512KB)       │ → viditelne v UI │
└──────────────────┘                                └──────────────────┘
```

### Mechanismus — log chunk upload

1. Edge worker spusti task → task pise do lokalniho logu
2. Edge worker periodicky (behem behu tasku) posila **chunky** logu na centralu
3. Centrala ulozi chunky do `logs/dag_id=.../` adresare
4. Po dokonceni tasku posle posledni chunk
5. Logy jsou viditelne v Airflow UI

### Konfigurace edge3 logovani

| Parametr | Sekce | Default | Popis |
|----------|-------|---------|-------|
| `push_log_chunks_to_server` | `[edge]` | True (pokud definovano) | Zapne/vypne push logu na centralu |
| `push_log_chunk_size` | `[edge]` | 524288 (512KB) | Velikost jednoho chunku |
| `api_url` | `[edge]` | — | URL centraly kam posilat logy |

### Omezeni

- **Vice API serveru**: Pokud bezi vice instanci api-serveru bez sdileneho log volume, logy budou roztrousene. Reseni: sdileny volume nebo single api-server.
- **Sitovy vypadek**: Pokud edge worker ztrati spojeni behem tasku, logy z doby vypadku se nedostanou na centralu. Zustanou lokalne na edge workeru.
- **Neni real-time streaming**: Logy se posilaji po chunkach, ne jako streaming. Drobne zpozdeni oproti lokalni konzoli.

## Jak se logy daji sbirat — 5 pristupu

### 1. Sdileny volume (default — Celery worker)

```yaml
# docker-compose.yaml
airflow-worker:
  volumes:
    - ./logs:/opt/airflow/logs    # sdileny s api-serverem
```

**Pro**: jednoduche, zadna konfigurace
**Proti**: funguje jen ve stejne siti (NFS/shared storage)

### 2. Edge3 chunk upload (default — Edge Worker)

```yaml
# Edge worker env
AIRFLOW__EDGE__API_URL: 'http://airflow-apiserver:8080/edge_worker/v1/rpcapi'
# push_log_chunks_to_server je default True
```

**Pro**: automaticky s edge3 providerem, zadna extra infra
**Proti**: zavisle na API serveru, neni streaming

### 3. Remote logging — blob storage (S3/GCS/Azure)

```yaml
# Airflow env
AIRFLOW__LOGGING__REMOTE_LOGGING: 'true'
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER: 's3://my-bucket/airflow-logs'
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID: 'aws_default'
```

**Pro**: centralizovane, skalovatelne, perzistentni
**Proti**: cloud zavislost, dalsi infrastruktura, naklady za storage

### 4. Remote logging — streaming (Elasticsearch/CloudWatch)

```yaml
# Airflow env
AIRFLOW__LOGGING__REMOTE_LOGGING: 'true'
AIRFLOW__ELASTICSEARCH__HOST: 'http://elasticsearch:9200'
AIRFLOW__ELASTICSEARCH__LOG_ID_TEMPLATE: '{dag_id}-{task_id}-{run_id}-{try_number}'
```

**Pro**: full-text search, real-time, centralizovane
**Proti**: dalsi infrastruktura (ELK stack), slozitejsi setup

### 5. EFK/ELK stack (externi sber)

```
Airflow kontejnery → Fluentd/Filebeat → Elasticsearch → Kibana
```

Neni Airflow-specificke — sbira stdout/stderr vsech kontejneru. Docker logging driver posle logy do Fluentd.

**Pro**: sbira VSE (ne jen task logy), standardni DevOps pristup
**Proti**: dalsi 3+ kontejnery, konfigurace log driveru

## Srovnani pristupu

| Pristup | Edge Worker? | Real-time? | Centraliz.? | Extra infra? | Slozitost |
|---------|-------------|-----------|-------------|-------------|-----------|
| Sdileny volume | Ne (jen Celery) | Ano | Ano (lokalne) | Ne | Trivialni |
| Edge3 chunk upload | Ano | Ccastecne | Ano | Ne | Jednoduche |
| S3/GCS/Azure | Ano | Ne | Ano | Cloud | Stredni |
| Elasticsearch | Ano | Ano | Ano | ELK/EFK | Vysoka |
| EFK stack | Ano (container) | Ano | Ano | Fluentd+ES+Kibana | Vysoka |

## Doporuceni

### Pro PoC/vyvoj
Edge3 chunk upload staci — logy se automaticky dostanou na centralu.

### Pro produkci
1. **Blob storage** (S3/GCS) — pokud uz mate cloud, nejjednodussi centralizedovane reseni
2. **EFK stack** — pokud chcete sledovat vsechny logy (ne jen task), full-text search
3. **Elasticsearch remote logging** — nejlepsi UX v Airflow UI (streaming + search)

### Specificka situace: Edge Worker
Edge3 chunk upload je jediny zpusob bez dalsi infrastruktury. Pokud nestaci, pouzit remote logging (S3/Elasticsearch) — edge worker pise primo do remote storage, nezavisle na API serveru.

## Aktualni stav v nasem stacku

- Celery worker: sdileny volume `logs/` — logy viditelne v UI
- Edge worker: `edge-logs/` volume (oddeleny), chunk upload na centralu pres API
- Remote logging: vypnute (`remote_logging = False`)
- `push_log_chunk_size`: 512KB (default)
