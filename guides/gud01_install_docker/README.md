# Instalace Apache Airflow pres Docker Compose

Oficialni zpusob jak rozjet Airflow lokalne pro vyvoj a uceni.

## Prerekvizity

- Docker + Docker Compose (overeni: `docker --version && docker compose version`)

## Instalace

```bash
# 1. Stahnout oficialni docker-compose.yaml
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

# 2. Vytvorit adresare pro mount do kontejneru
mkdir -p ./dags ./logs ./plugins ./config

# 3. Nastavit AIRFLOW_UID (mapovani uzivatele host <-> kontejner)
echo -e "AIRFLOW_UID=$(id -u)" > .env

# 4. Inicializovat databazi + vytvorit admin ucet
docker compose up airflow-init

# 5. Spustit vsechny sluzby na pozadi
docker compose up -d
```

## Pristup

- **Web UI**: http://localhost:8080
- **Login**: `airflow` / `airflow`

## Co bezi

Docker Compose spusti 7 kontejneru (Airflow 3.2.2):

| Kontejner | Role |
|-----------|------|
| `postgres` | Metadata databaze (PostgreSQL 16) |
| `redis` | Message broker pro Celery |
| `airflow-apiserver` | Web UI + REST API (port 8080) |
| `airflow-scheduler` | Planovani a spousteni DAG runu |
| `airflow-dag-processor` | Parsovani DAG souboru |
| `airflow-triggerer` | Async triggery (deferrable operators) |
| `airflow-worker` | Celery worker - vykonava tasky |

## Volume mounty

Adresare na hostu jsou namountovane do kontejneru:

| Host | Kontejner | Ucel |
|------|-----------|------|
| `./dags/` | `/opt/airflow/dags/` | DAG soubory (sem pises kod) |
| `./logs/` | `/opt/airflow/logs/` | Logy z behu tasku |
| `./plugins/` | `/opt/airflow/plugins/` | Custom pluginy |
| `./config/` | `/opt/airflow/config/` | Konfigurace (airflow.cfg) |

Zmeny v `./dags/` se automaticky nacitaji (~10s interval).

## Sprava

```bash
docker compose ps          # stav kontejneru
docker compose logs -f     # logy (vsechny sluzby)
docker compose down        # zastavit
docker compose down -v     # zastavit + smazat data (volumes)
docker compose up -d       # znovu spustit
```

## API (Airflow 3.x)

Airflow 3 pouziva JWT autentizaci (ne Basic Auth):

```bash
# Ziskat token
TOKEN=$(curl -s -X POST "http://localhost:8080/auth/token" \
  -H "Content-Type: application/json" \
  -d '{"username":"airflow","password":"airflow"}' \
  | sed 's/.*"access_token":"\([^"]*\)".*/\1/')

# Priklad: seznam DAGu
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/v2/dags
```

## Poznamky

- Warning o `FERNET_KEY` je normalni pro dev prostredi
- Example DAGs (bundle `example_dags`) se nacitaji automaticky - pozor na kolize `dag_id`
- Vlastni DAGs jsou v bundlu `dags-folder`
- Pro produkci viz managed reseni (AWS MWAA, GCP Cloud Composer)

## Alternativy instalace

| Zpusob | Slozitost | Vhodne pro |
|--------|-----------|------------|
| **Docker Compose** (tento guide) | Nizka | Uceni, vyvoj, testovani |
| `airflow standalone` | Nejnizsi (pip + 1 prikaz) | Rychle hrani, SQLite, bez paralelismu |
| Rucni instalace | Vysoka | Custom produkce |
| Managed cloud | Zadna | Produkce v cloudu |

## Zdroje

1. https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
2. https://airflow.apache.org/docs/apache-airflow/stable/start.html
