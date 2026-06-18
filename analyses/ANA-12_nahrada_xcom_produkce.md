# ANA-12: Nahrada XCom v produkci — varianty prenosu ETL dat

## Kontext

V poc03 edge worker predava data centrale pres XCom (= metadata DB). To neni vhodne pro produkci:
- XCom uklada data jako JSON blob v PostgreSQL (size limit ~1GB Postgres, ~64KB MySQL)
- Zatezuje metadata DB (SPOF pro cely Airflow)
- Pomale HTTP requesty pri velkych datech

**Cil**: oddelit tok ETL dat od metadata DB. Airflow dal pouziva PostgreSQL pro interni chod, ale vyrobni data jdou jinou cestou.

Discovery zdroje: [DISC-06](DISC-06_xcom_object_storage_backend.md), [DISC-07](DISC-07_minio_on_premise.md), [DISC-08](DISC-08_shared_volumes_antipattern.md)

## KOREKCE: Konfigurace, ne zmena kodu

Puvodni predpoklad byl, ze nahrada XCom vyzaduje zmenu v kodu DAGu. **To neni pravda.**

Airflow ma vestavenou podporu pro **XCom Object Storage Backend** — ciste konfiguracni zmena:
- DAG kod zustava stejny (`return data` z tasku)
- Airflow transparentne uklada XCom data do externiho storage misto metadata DB
- V DB zustava jen reference (cesta k souboru)

## Varianty

### Varianta 1: XCom Object Storage Backend + MinIO (DOPORUCENO)

**Princip**: Airflow automaticky presmeruje XCom data do MinIO (on-premise S3) misto do PostgreSQL.

```
PRED (poc03):
  Edge task → return data → INSERT do xcom tabulky v PostgreSQL → Central task

PO:
  Edge task → return data → UPLOAD do MinIO (S3 API) → ref v PostgreSQL → Central task
```

**Konfigurace (env vars — cely cluster):**
```
AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.common.io.xcom.backend.XComObjectStorageBackend
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH=s3://minio_conn@airflow-xcom/xcom
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD=0
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_COMPRESSION=gzip
```

**MinIO connection:**
```
AIRFLOW_CONN_MINIO_CONN='{"conn_type":"aws","extra":{"aws_access_key_id":"minioadmin","aws_secret_access_key":"minioadmin","endpoint_url":"http://minio:9000"}}'
```

**Potrebne packages (na vsech workerech vcetne edge):**
- `apache-airflow-providers-common-io`
- `apache-airflow-providers-amazon[s3fs]`

**MinIO stack (1 kontejner navic):**
```yaml
minio:
  image: minio/minio:latest
  ports:
    - "9000:9000"
    - "9001:9001"
  environment:
    MINIO_ROOT_USER: minioadmin
    MINIO_ROOT_PASSWORD: minioadmin
  volumes:
    - minio_data:/data
  command: server /data --console-address ":9001"
```

**Vyhody:**
- Zadna zmena v DAG kodu
- Transparentni pro edge i central tasky
- Hybridni rezim (male hodnoty v DB, velke v MinIO)
- Komprese (gzip) snizuje sitovy provoz
- MinIO = lightweight (~100MB RAM)
- S3 API = standard, migrace do AWS/Azure trivialni
- MinIO ma built-in Prometheus metriky

**Nevyhody:**
- Dalsi kontejner (MinIO) v centralnim stacku
- Edge Worker musi videt MinIO endpoint (sitove)
- Dalsi credentials k sprave
- MinIO = dalsi SPOF (ale jednodussi nez PostgreSQL)

### Varianta 2: Sdileny filesystem (NFS/SMB)

**Princip**: Edge zapise soubor na sdileny disk, central ho precte.

**NEDOPORUCENO** — viz [DISC-08](DISC-08_shared_volumes_antipattern.md):
- NFS/SMB pres sit = pomale, nespolehline
- Zadne atomicke updaty (central muze cist neuplny soubor)
- SMB neumi POSIX operace (chmod/chown)
- Edge Worker na jine siti nez centrala = NFS pres WAN = velmi spatny napad
- Jarek Potiuk (Airflow PMC): "object storage je spravny pristup"

### Varianta 3: Prima komunikace (REST API / DB write)

**Princip**: Edge task primo zapisuje do cilove DB nebo posila data pres vlastni REST API.

```python
# v DAG kodu (zmena!)
@task(executor="edge")
def extract_transform():
    data = extract_from_machine()
    transformed = transform(data)
    requests.post("https://central/api/ingest", json=transformed)  # primo
    # nebo: psycopg2.connect(...).execute("INSERT ...")
```

**Vyhody:**
- Nejkratsi cesta dat (zadny prostredik)
- Plna kontrola nad formatem a destinaci

**Nevyhody:**
- **Zmena v DAG kodu** (neni transparentni)
- Edge Worker musi znat connection string cilove DB (security riziko)
- Obchazi Airflow orchestraci (co kdyz central task selze?)
- Zadne retry/tracking na urovni Airflow
- Tight coupling edge → cilova DB

### Varianta 4: ObjectStoragePath v kodu DAGu

**Princip**: Pouziti ObjectStoragePath API primo v DAG kodu pro explicitni zapis do object storage.

```python
from airflow.sdk.io.path import ObjectStoragePath

@task(executor="edge")
def extract_transform():
    data = extract_from_machine()
    path = ObjectStoragePath("s3://minio@data-bucket/") / f"stroj1_{ds}.parquet"
    with path.open("w") as f:
        f.write(data)
    return path  # reference pres XCom (mala hodnota)

@task(executor="celery")
def load(path: ObjectStoragePath):
    with path.open("r") as f:
        data = f.read()
    load_to_db(data)
```

**Vyhody:**
- Explicitni kontrola nad formatem a umistenim
- XCom prenasi jen referenci (mala hodnota)
- Moznost pouzit Parquet, Avro, nebo jiny format

**Nevyhody:**
- Zmena v DAG kodu (neni transparentni)
- Slozitejsi nez Varianta 1
- Rucni sprava bucket struktury

## Srovnani

| Kriterium | V1: XCom Backend + MinIO | V2: NFS/SMB | V3: Prima DB/REST | V4: ObjectStoragePath |
|-----------|-------------------------|-------------|-------------------|----------------------|
| Zmena DAG kodu | **NE** | ANO | ANO | ANO |
| Zmena konfigurace | ANO (env vars) | ANO (mounts) | ANO (credentials) | ANO (connection) |
| Dalsi infrastruktura | MinIO kontejner | NFS server | Zadna | MinIO kontejner |
| Spolehlivost | Vysoka | Nizka | Stredni | Vysoka |
| Edge kompatibilita | Ano (HTTP k MinIO) | Spatna (NFS pres WAN) | Ano | Ano (HTTP k MinIO) |
| Migrace do cloudu | Trivialni (S3 API) | Slozita | Slozita | Trivialni (S3 API) |
| Komplexita | Nizka | Stredni | Stredni | Stredni |

## Doporuceni

### Pro produkci: Varianta 1 (XCom Object Storage Backend + MinIO)

**Proc:**
1. **Zadna zmena v DAG kodu** — poc03 DAGy funguji beze zmeny
2. **Konfiguracni zmena** — env vars na vsech workerech
3. **MinIO = 1 kontejner** — lightweight, jednoduchy setup
4. **S3 API = standard** — migrace do AWS/Azure bez zmeny kodu
5. **Hybridni rezim** — male XCom hodnoty (metadata) zustanou v DB, velke (ETL data) jdou do MinIO
6. **Edge kompatibilita** — edge worker pristupuje k MinIO pres HTTP (stejne jako k Airflow API)

### Rozhodovaci strom

```
Chces transparentni reseni (bez zmeny DAGu)?
├── ANO → Varianta 1 (XCom Backend + MinIO)
│         └── threshold=0 pro edge workery (vsechno do MinIO)
└── NE (chces plnou kontrolu) → Varianta 4 (ObjectStoragePath)
                                  └── explicitni zapis, vlastni formaty
```

### Co je treba overit v PoC

1. XCom Object Storage Backend funguje s edge3 providerem
2. Edge Worker umi pristupovat k MinIO (sitove, credentials)
3. Serializace/deserializace probiha na strane workeru (ne scheduleru)
4. Vykon: latence zapis/cteni pro typicke ETL objemy
5. Chovani pri sitovem vyluce edge ↔ MinIO

## Navrh poc07

```
poc07_xcom_minio/
  docker-compose.yaml    # Airflow stack + MinIO + Edge Worker
  Dockerfile
  dags/                  # poc03 DAGy BEZ ZMENY
  config/
  run.ps1
  analyses/
```

Hypoteza: **XCom Object Storage Backend s MinIO funguje transparentne s edge workerem — DAGy z poc03 funguji beze zmeny kodu, data jdou pres MinIO misto metadata DB.**
