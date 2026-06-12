# ANA-01: Docker Setup pro Apache Airflow

## Verze
- Airflow: 3.2.2
- Docker image: `apache/airflow:3.2.2`
- Docker Compose: v5.1.4

## Architektura kontejneru

Oficialni `docker-compose.yaml` spousti 7 kontejneru:

| Service | Popis |
|---------|-------|
| `postgres` | Metadata databaze (PostgreSQL 16) |
| `redis` | Message broker pro Celery executor |
| `airflow-apiserver` | Web UI + REST API (port 8080) |
| `airflow-scheduler` | Planovac DAG runu |
| `airflow-dag-processor` | Parsuje DAG soubory |
| `airflow-triggerer` | Async triggery (deferrable operators) |
| `airflow-worker` | Celery worker pro vykonavani tasku |

## Setup kroky

```bash
# 1. Stahnout docker-compose.yaml
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

# 2. Vytvorit adresare
mkdir -p ./dags ./logs ./plugins ./config

# 3. Nastavit AIRFLOW_UID
echo -e "AIRFLOW_UID=$(id -u)" > .env

# 4. Inicializovat DB a admin uzivatele
docker compose up airflow-init

# 5. Spustit vsechny sluzby
docker compose up -d
```

## Pristup

- **UI**: http://localhost:8080
- **Login**: airflow / airflow

## API autentizace (Airflow 3.x)

Airflow 3 pouziva JWT tokeny (ne Basic Auth jako v2):

```bash
# Ziskat token
TOKEN=$(curl -s -X POST "http://localhost:8080/auth/token" \
  -H "Content-Type: application/json" \
  -d '{"username":"airflow","password":"airflow"}' \
  | sed 's/.*"access_token":"\([^"]*\)".*/\1/')

# Pouzit token
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/v2/dags
```

## Dulezite poznamky

- Warning o `FERNET_KEY` je normalni pro dev prostredi (pouziva se default key)
- Example DAGs se nacitaji automaticky (bundle `example_dags`) - pozor na kolize dag_id
- Vlastni DAGs se nacitaji z `./dags/` (bundle `dags-folder`)
- Dag-processor automaticky detekuje zmeny v souborech (interval ~10s)

## Uzitecne prikazy

```bash
# Status kontejneru
docker compose ps

# Logy konkretni sluzby
docker compose logs airflow-scheduler

# Zastavit vse
docker compose down

# Zastavit + smazat data (volumes)
docker compose down -v
```
