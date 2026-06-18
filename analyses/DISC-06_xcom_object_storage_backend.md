# DISC-06: XCom Object Storage Backend — oficialni docs + Astronomer

## Zdroje

- https://airflow.apache.org/docs/apache-airflow-providers-common-io/stable/xcom_backend.html
- https://www.astronomer.io/docs/learn/custom-xcom-backend-strategies
- https://www.astronomer.io/docs/learn/xcom-backend-tutorial/
- https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/objectstorage.html

## Relevance: VYSOKA (kriticka)

## Souhrn

Airflow ma vestavenou podporu pro presmerovani XCom dat do object storage (S3/MinIO/GCS/Azure). Je to **ciste konfiguracni zmena** — DAG kod zustava stejny (return data z tasku), Airflow transparentne uklada do externiho storage misto metadata DB.

## Klicove poznatky

### XComObjectStorageBackend

Provider: `apache-airflow-providers-common-io`

**Konfigurace (env vars nebo airflow.cfg):**
```
AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.common.io.xcom.backend.XComObjectStorageBackend
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH=s3://conn_id@mybucket/xcom
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD=1048576  # bytes (1MB)
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_COMPRESSION=gzip   # volitelne
```

**Hybridni rezim**: hodnoty mensi nez threshold zustanou v DB, vetsi jdou do object storage. V DB se ulozi jen reference (cesta k souboru).

**Threshold = 0**: vsechno do object storage (doporuceno pro remote/edge workers).

### Podpora backendu

| Backend | Scheme | Provider package |
|---------|--------|-----------------|
| AWS S3 / MinIO | `s3://` | `apache-airflow-providers-amazon[s3fs]` |
| Google Cloud Storage | `gs://` | `apache-airflow-providers-google` |
| Azure Blob | `abfs://` | `apache-airflow-providers-microsoft-azure` |
| Lokalni filesystem | `file://` | zadny (built-in) |

### MinIO konfigurace (on-premise S3)

```
# Env var pro connection
AIRFLOW_CONN_MINIO_LOCAL='{"conn_type":"aws","extra":{"aws_access_key_id":"minioadmin","aws_secret_access_key":"minioadmin","endpoint_url":"http://minio:9000"}}'

# XCom backend
AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.common.io.xcom.backend.XComObjectStorageBackend
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_PATH=s3://minio_local@airflow-xcom/xcom
AIRFLOW__COMMON_IO__XCOM_OBJECTSTORAGE_THRESHOLD=0
```

### Komprese

Podporovane metody: gzip, bz2, zip, snappy. Gzip/bz2/zip built-in, snappy vyzaduje `python-snappy`.

### Custom XCom Backend Class (pokrocile)

Pro specialni potreby: zdedi z `BaseXCom`, override `serialize_value()` a `deserialize_value()`. Pouziti: multi-location storage, specialni serialization format, pristup bez metadata DB.

### ObjectStoragePath API

Od Airflow 2.8.0: unifikovane API pro praci s object storage (S3/GCS/Azure/local) pres `pathlib`-like interface.

```python
from airflow.sdk.io.path import ObjectStoragePath

path = ObjectStoragePath("s3://my-bucket/data/", conn_id="aws_default")
path.mkdir(exist_ok=True)
with (path / "output.csv").open("w") as f:
    f.write(data)
```

ObjectStoragePath objekty lze predat pres XCom mezi tasky.

### Potrebne packages na Edge Workeru

Edge Worker musi mit nainstalovany:
- `apache-airflow-providers-common-io`
- `apache-airflow-providers-amazon[s3fs]` (pro MinIO/S3)
- Edge Worker musi mit sitovy pristup k MinIO/S3 endpointu

### Otevrene otazky

1. Serializace probiha na strane workeru — Edge Worker tedy musi umet pristupovat k MinIO
2. Co se stane pri sitovem vyluce edge ↔ MinIO? (retry mechanismus?)
3. Koexistence s edge3 chunk upload logovanim — konflikty?
