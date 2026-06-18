# ANA-12a: Object Storage pro ETL data — analyza nasazeni

## Kontext

Navazuje na [ANA-12](ANA-12_nahrada_xcom_produkce.md). XCom Object Storage Backend presmeruje ETL data z metadata DB do externiho S3-compatible storage. Tato analyza resi:

Souvisejici: [ANA-12b](ANA-12b_typy_storage_srovnani.md) — srovnani typu storage (object vs file vs block vs primo do DB)

1. **Jaky S3 storage pouzit** (MinIO vs alternativy)
2. **Jak to provozovat** (infra, deployment)
3. **Jak to zabezpecit** (pristup, opravneni, sifrovani)
4. **Jak resit backup/recovery** (data safety)
5. **Co to znamena v nasi architekture**

## 1. Volba S3-compatible storage

### MinIO — POZOR: Community Edition je archivovany

**Casova osa:**
- 2019-2021: zmena licence z Apache 2.0 na AGPL v3
- cerven 2025: web UI odebrano z Community Edition (jen CLI)
- prosinec 2025: "maintenance mode" — zadne nove funkce, jen security patche
- unor 2026: **repozitar archivovan** — "no longer maintained"

**Co to znamena:**
- Zdrojovy kod zustava dostupny (AGPL v3)
- **Zadne oficialni binaries/kontejnery** — musis buildovat sam
- Zadne nove funkce, jen kriticke opravy (a i ty konci)
- Enterprise verze existuje ($96K/rok) — pro nas use case overkill

**Zaver: MinIO CE nelze doporucit pro novy projekt v 2026.**

### Alternativy

| Storage | Licence | Jazyk | Single-node Docker | S3 kompatibilita | Stav 2026 |
|---------|---------|-------|--------------------|--------------------|-----------|
| **SeaweedFS** | Apache 2.0 | Go | Ano (`server -s3`) | Vysoka | Aktivni, production-proven |
| **Garage** | EUPL (copyleft) | Rust | Ano (single binary) | Vysoka | Aktivni, lightweight |
| RustFS | Apache 2.0 | Rust | Ano | Stredni | **Alpha — NE pro produkci** |
| Ceph RGW | LGPL | C++ | Mozne, ale slozite | Plna | Overkill pro nas use case |

### Doporuceni: SeaweedFS

**Proc:**
- **Apache 2.0** — zadne licencni omezeni, na rozdil od MinIO (AGPL) a Garage (EUPL copyleft)
- **Production-proven** — existuje od 2015, pouzivan ve velkych deploymentech
- **Jednoduchy single-server setup** — 1 Docker kontejner, vsechny komponenty v jednom procesu
- **S3 API na portu 8333** — funguje s Airflow XCom Object Storage Backend (fsspec/boto3)
- **Nizke naroky** — srovnatelne s MinIO (~100-200MB RAM)
- **Web UI** — master dashboard (port 9333) + filer UI (port 8888)

**Docker setup:**
```yaml
services:
  seaweedfs:
    image: chrislusf/seaweedfs:latest
    ports:
      - "8333:8333"   # S3 API
      - "9333:9333"   # Master dashboard
    volumes:
      - seaweedfs_data:/data
    command: server -s3 -dir=/data -ip.bind=0.0.0.0

volumes:
  seaweedfs_data:
```

**Alternativa: Garage** — pokud preferujes Rust a ultra-lightweight (single binary, bezi i na Raspberry Pi). EUPL licence muze byt problem v nekterych corporate prostedich.

### Airflow konfigurace (stejna pro vsechny S3-compatible storage)

```
AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.common.io.xcom.backend.XComObjectStorageBackend
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH=s3://s3_conn@airflow-xcom/xcom
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD=0
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_COMPRESSION=gzip

# Connection (zmeni se jen endpoint_url)
AIRFLOW_CONN_S3_CONN='{"conn_type":"aws","extra":{"aws_access_key_id":"admin","aws_secret_access_key":"secretkey","endpoint_url":"http://seaweedfs:8333"}}'
```

DAG kod se **nemeni** — stejna konfigurace funguje pro SeaweedFS, MinIO, Garage i AWS S3.

## 2. Architektura v nasem setupu

```
┌─────────────────────────────────────────────────────────┐
│ CENTRAL SERVER (Linux)                                  │
│                                                         │
│  ┌─────────────┐  ┌──────────┐  ┌───────────────────┐  │
│  │ Airflow     │  │PostgreSQL│  │ SeaweedFS         │  │
│  │ (scheduler, │  │(metadata)│  │ (ETL data storage)│  │
│  │  webserver, │  │          │  │ S3 API :8333      │  │
│  │  celery     │  │          │  │ Dashboard :9333   │  │
│  │  worker)    │  │          │  │                   │  │
│  └──────┬──────┘  └──────────┘  └────────┬──────────┘  │
│         │                                │              │
│         │  HTTP (Airflow API)            │ HTTP (S3 API)│
└─────────┼────────────────────────────────┼──────────────┘
          │                                │
     ┌────┴────────────────────────────────┴────┐
     │              FACTORY NETWORK             │
     └────┬──────────────────────┬──────────────┘
          │                      │
  ┌───────┴──────┐      ┌───────┴──────┐
  │ Edge Worker 1│      │ Edge Worker N│
  │ (linka 1)    │      │ (linka N)    │
  │              │      │              │
  │ extract +    │      │ extract +    │
  │ transform    │      │ transform    │
  │    ↓         │      │    ↓         │
  │ return data  │      │ return data  │
  │ (→ SeaweedFS)│      │ (→ SeaweedFS)│
  └──────────────┘      └──────────────┘
```

### Tok dat

1. Edge Worker spusti task, extrahuje data ze stroje
2. Task vrati data (`return transformed_data`)
3. XCom Object Storage Backend serializuje data a **uploadne do SeaweedFS** (S3 PUT pres HTTP)
4. V metadata DB se ulozi jen reference (cesta k objektu)
5. Central Worker spusti load task, precte referenci z DB
6. Central Worker **stahne data ze SeaweedFS** (S3 GET)
7. Central Worker zapise data do cilove DB

### Sitove pozadavky

Edge Worker musi videt **dva endpointy** na centrale:
- Airflow API (uz dnes) — pro orchestraci, heartbeaty, logy
- SeaweedFS S3 API (nove) — pro ETL data

Obe jsou HTTP, outbound z edge. **Zadny novy sitovy pozadavek** — stejny pattern (HTTP k centrale), jen dalsi port.

## 3. Provoz (infrastruktura)

### Co bezi na centralnim serveru

| Komponenta | Port | Ucel | RAM |
|-----------|------|------|-----|
| Airflow Webserver | 8080 | UI | ~500MB |
| Airflow Scheduler | - | Planovani | ~500MB |
| Airflow Celery Worker | - | Central tasky (load) | ~500MB |
| PostgreSQL | 5432 | Metadata DB | ~256MB |
| Redis | 6379 | Celery broker | ~100MB |
| SeaweedFS | 8333, 9333 | ETL data storage | ~200MB |
| statsd-exporter | 9125 | Metriky (volitelne) | ~50MB |
| **Celkem** | | | **~2.1GB** |

### Potrebuji load balancer?

**Ne pro nas use case.** Load balancer je nutny pri:
- Vice instanci webserveru (skalovani UI)
- Vice SeaweedFS nodu (distribuovany cluster)

Pro single-server deployment staci:
- **Reverse proxy (nginx/Caddy)** — jeden vstupni bod pro vsechny sluzby, TLS terminace
- Nginx routuje dle portu/cesty na spravnou sluzbu

```
nginx (port 443, TLS)
  ├── /airflow/*     → Airflow Webserver :8080
  ├── /edge_worker/* → Airflow API :8080
  └── /s3/*          → SeaweedFS :8333
```

### Monitoring SeaweedFS

- **Master dashboard** (port 9333): stav clusteru, volume servery, prostor
- **Metriky**: SeaweedFS exportuje Prometheus metriky (integrace s poc04 stackem)
- **Health check**: `GET http://seaweedfs:9333/cluster/status`

## 4. Bezpecnost

### 4a. Pristupova (opravneni/role)

**S3 access keys** — kazdy klient (edge worker, central worker) ma vlastni access key:

```
# SeaweedFS S3 konfigurace (config/s3.json)
{
  "identities": [
    {
      "name": "airflow-central",
      "credentials": [{"accessKey": "central_key", "secretKey": "central_secret"}],
      "actions": ["Read", "Write", "List", "Admin"]
    },
    {
      "name": "airflow-edge",
      "credentials": [{"accessKey": "edge_key", "secretKey": "edge_secret"}],
      "actions": ["Read", "Write", "List"]
    }
  ]
}
```

**Bucket politiky:**
- `airflow-xcom` bucket — Airflow XCom data (pristup: central + edge workers)
- Moznost per-bucket ACL (edge worker vidi jen svuj bucket, pokud chceme izolaci)

**Doporuceni pro produkci:**
- Kazdy edge worker = vlastni access key (revokace bez dopadu na ostatni)
- Rotace klicu v pravidelnych intervalech
- Audit log — kdo kdy co uploadoval/stahnul

### 4b. Sitova (sifrovani)

**TLS/HTTPS** — stejna problematika jako OP-04 (edge↔central):
- SeaweedFS podporuje TLS nativne
- Nebo TLS terminace na reverse proxy (nginx) — jednodussi sprava certifikatu
- Edge worker komunikuje pres HTTPS (sifrovany kanal)

**Sifrovani dat at rest:**
- SeaweedFS podporuje server-side encryption (SSE)
- Alternativa: sifrovany disk (LUKS na Linuxu)

### 4c. Datova (backup/recovery)

**Co zalohovati:**

| Data | Kde | Jak | RPO |
|------|-----|-----|-----|
| Metadata DB (PostgreSQL) | Central server | pg_dump cronjob (viz OP-05) | Hodiny |
| ETL data (SeaweedFS) | Central server | Viz nize | Zavisi na strategii |
| DAGy (kod) | Git repo | Git push (uz reseno) | Real-time |

**Strategie zalohovani SeaweedFS:**

1. **`weed export` / `weed backup`** — nativni nastroj pro export dat
2. **S3-to-S3 replikace** — `rclone sync` nebo `mc mirror` do druheho storage (jiny server, NAS, cloud)
3. **Volume-level backup** — snapshot Docker volume (filesystem backup)
4. **Bucket versioning** — SeaweedFS podporuje S3 versioning (ochrana pred prepisy/smazanim)

**Doporuceni pro produkci:**

```
Faze 1 (zaklad):
  - Bucket versioning = zapnout (ochrana pred accidental delete)
  - Docker volume na separatnim disku (ne na systemovem)
  - Nightly rclone sync na NAS nebo druhy server

Faze 2 (rozsireni):
  - SeaweedFS multi-drive (erasure coding, toleruje pad 1 disku)
  - Offsite backup (cloud S3 bucket jako DR)
```

**Recovery:**
- Ztrata SeaweedFS dat = ztrata ETL dat v prepravni fazi (jeste nenactena do cilove DB)
- Data, ktera uz prosla load taskem, jsou v cilove DB — SeaweedFS je jen "prepravni" storage
- RPO zavisi na frekvenci ETL runu (pokud ETL bezi kazdou hodinu, maximalni ztrata = 1 hodina dat)

### Dulezite: SeaweedFS NENI archiv

SeaweedFS v nasi architekture slouzi jako **prepravni storage** (staging area), ne jako dlouhodoby archiv:

```
Stroj → Edge Worker → SeaweedFS (docasne) → Central Worker → Cilova DB (trvale)
```

ETL data v SeaweedFS jsou docasna — po uspesnem loadu do cilove DB je mozne je smazat (lifecycle policy). Dlouhodobe zalohovani se tyka cilove DB, ne SeaweedFS.

## 5. Otevrene otazky

| # | Otazka | Dopad |
|---|--------|-------|
| 1 | Jak velka jsou ETL data per run? (KB vs MB vs GB) | Urcuje sizing disku a siti |
| 2 | Jak dlouho uchovavat data v SeaweedFS? | Lifecycle policy, disk sizing |
| 3 | Je treba audit trail (kdo uploadoval jake data)? | Access logging konfigurace |
| 4 | Ma zakaznik existujici backup infrastrukturu (NAS, tape)? | Integrace s existujicim backupem |
| 5 | Kolik edge workeru soucasne uploaduje? | Concurrency, throughput |

## 6. Srovnani: pred a po

| Aspekt | Pred (XCom/metadata DB) | Po (SeaweedFS) |
|--------|------------------------|----------------|
| ETL data storage | PostgreSQL (sdileny s Airflow) | SeaweedFS (separatni) |
| Size limit | ~1GB (Postgres blob) | Neomezene (disk) |
| Zatez na metadata DB | Vysoka (ETL + Airflow) | Nizka (jen Airflow) |
| Zmena DAG kodu | - | Zadna |
| Dalsi infra | - | 1 kontejner (~200MB RAM) |
| Backup | pg_dump (vsechno dohromady) | Separatni (ETL vs metadata) |
| Bezpecnost | DB credentials | S3 access keys + TLS |
| Migrace do cloudu | Slozita | Trivialni (zmena connection) |

## 7. Doporuceni

### Pro poc07 (validace)

- SeaweedFS single-node v Docker Compose
- poc03 DAGy beze zmeny
- Overit: edge worker → SeaweedFS upload → central worker download

### Pro produkci

1. **SeaweedFS** na centralnim serveru (1 kontejner)
2. **Nginx reverse proxy** s TLS (spolecny pro Airflow + SeaweedFS)
3. **Per-edge access keys** (izolace, revokace)
4. **Bucket versioning** + nightly backup (rclone na NAS)
5. **Lifecycle policy** — smazat ETL data po uspesnem loadu (napr. 7 dni retence)
