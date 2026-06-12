# ANA-02: Standalone architektura

## Co `airflow standalone` dela

Jeden prikaz, ktery:
1. Inicializuje SQLite databazi
2. Vytvori admin ucet (heslo vypise do terminalu)
3. Vygeneruje `airflow.cfg`
4. Spusti 4 komponenty v jednom procesu

## Komponenty

| Komponenta | Role |
|------------|------|
| API server | Web UI + REST API (port 8080) |
| Scheduler | Planovani DAG runu |
| DAG processor | Parsovani DAG souboru |
| Triggerer | Async triggery |

**Chybi oproti Docker Compose:**
- Zadny Redis (broker)
- Zadny Celery worker
- Zadny PostgreSQL (pouziva SQLite)

## Executor

Standalone pouziva **SequentialExecutor** (nebo LocalExecutor) - tasky bezi jeden po druhem, ne paralelne.

## Souborova struktura

Vse v `$AIRFLOW_HOME` (default `~/airflow`):

```
~/airflow/
  airflow.cfg           # konfigurace
  airflow.db            # SQLite databaze
  airflow-api-server.pid # PID webserveru
  dags/                 # DAG soubory
  logs/                 # logy
```

## Instalace

### Varianta pip + venv
```bash
export AIRFLOW_HOME=~/airflow
python3 -m venv airflow_venv
source airflow_venv/bin/activate
pip install --upgrade pip

AIRFLOW_VERSION=3.1.1
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

### Varianta uv (rychlejsi)
```bash
export AIRFLOW_HOME=~/airflow

AIRFLOW_VERSION=3.1.1
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

### Spusteni
```bash
airflow standalone
```

Heslo se zobrazi v terminalu. UI na http://localhost:8080.

## Omezeni

- **SQLite** - nepodporuje paralelni zapisy, jen pro dev
- **SequentialExecutor** - tasky bezi sekvencne
- **Neni pro produkci** - oficalni dokumentace to explicitne rika
- **POSIX only** - nefunguje na Windows nativne (nutny WSL2)

## Srovnani s Docker Compose

| | Standalone | Docker Compose |
|---|---|---|
| DB | SQLite | PostgreSQL |
| Executor | Sequential/Local | Celery |
| Paralelni tasky | Ne | Ano |
| Windows nativne | Ne (WSL2) | Ano (Docker Desktop) |
| Izolace | venv | Kontejnery |
| Setup | pip + 1 prikaz | curl + docker compose |
| Produkce | Ne | Bliz produkci |

## Zdroje

- https://airflow.apache.org/docs/apache-airflow/stable/start.html
- https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html
