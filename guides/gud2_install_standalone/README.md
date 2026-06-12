# Airflow - instalace standalone

Standalone = nejjednodussi zpusob jak rozjet Airflow. Jeden prikaz spusti vse.

## TL;DR

```bash
export AIRFLOW_HOME=~/airflow
pip install "apache-airflow==3.1.1" --constraint <constraint-url>
airflow standalone
```

UI na http://localhost:8080, heslo se vypise do terminalu.

## Dulezite omezeni

- **Windows**: nativne NEFUNGUJE - nutny WSL2 nebo Docker. Detaily viz [ANA-01](analyses/ANA-01_podpora_windows.md)
- **SQLite**: nepodporuje paralelni zapisy
- **SequentialExecutor**: tasky bezi sekvencne, ne paralelne
- **Neni pro produkci** (oficalni doporuceni)

## Architektura

Spousti 4 komponenty v jednom procesu (API server, Scheduler, DAG processor, Triggerer).
Pouziva SQLite misto PostgreSQL, zadny Redis/Celery worker.
Detaily viz [ANA-02](analyses/ANA-02_standalone_architektura.md)

## Srovnani s Docker Compose

| | Standalone | Docker Compose |
|---|---|---|
| DB | SQLite | PostgreSQL |
| Paralelni tasky | Ne | Ano |
| Windows nativne | Ne (WSL2) | Ano |
| Blizko produkci | Ne | Ano |
| Setup slozitost | Nejnizsi | Nizka |

Pro Docker instalaci viz [gud1_install_docker](../gud1_install_docker/README.md).

## Zaver

Pro Windows uzivatele je **Docker Compose praktictejsi**. Standalone se hodi na macOS/Linux pro rychle experimentovani.

## Zdroje

1. https://airflow.apache.org/docs/apache-airflow/stable/start.html
2. https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html
3. https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html
