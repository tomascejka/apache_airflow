# DISC-07: MinIO — on-premise S3-compatible object storage

## Zdroje

- https://www.datacamp.com/tutorial/minio-docker
- https://oneuptime.com/blog/post/2026-02-08-how-to-run-minio-in-docker-s3-compatible-object-storage/view
- https://blog.min.io/apache-airflow-minio/
- https://hub.docker.com/r/minio/minio

## Relevance: VYSOKA

## Souhrn

MinIO je lightweight, S3-compatible object storage server. Idealni jako on-premise nahrada AWS S3 pro XCom backend. Jeden Docker kontejner, jednoduchy setup.

## Klicove poznatky

### Co je MinIO

- Open-source (GNU AGPLv3), napsany v Go
- Implementuje kompletni Amazon S3 API
- Jakykoli S3 SDK/nastroj funguje s MinIO bez zmeny kodu
- Single binary, zadne zavislosti

### Docker Compose setup

```yaml
services:
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # Web konzole
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio_data:/data
    command: server /data --console-address ":9001"

volumes:
  minio_data:
```

### Klicove vlastnosti

- **S3 API na portu 9000**: programovy pristup (boto3, aws cli, fsspec)
- **Web konzole na portu 9001**: sprava bucketu, objektu, pristupu
- **Perzistentni data**: volume mount je kriticke (kontejner je ephemeral)
- **Nizke naroky**: ~100MB RAM, minimalni CPU

### Pro nas use case

MinIO bezi na centralni instanci (vedle Airflow):
```
Edge Worker → HTTP → MinIO (port 9000) → XCom data ulozena jako objekty
Central Worker → HTTP → MinIO (port 9000) → Precte XCom data
```

- Edge Worker musi videt MinIO endpoint (sit)
- Stejny endpoint jako Airflow API (HTTP, outbound z edge)
- Zadny dalsi sitovy pozadavek nad ramec toho, co edge uz dnes dela

### Produkcni doporuceni

- Zmenit default credentials (minioadmin/minioadmin)
- TLS pro S3 API (stejne jako pro Airflow API — viz OP-04)
- Backup policy pro MinIO data
- Monitoring: MinIO ma built-in Prometheus metriky (`/minio/v2/metrics/cluster`)
