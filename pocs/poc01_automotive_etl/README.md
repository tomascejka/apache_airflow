# PoC 1: Automotive ETL

## Popis

Overit, ze Airflow zvladne batch ETL ze stroju na vyrobni lince — sber dat z CSV a JSON, transformaci do jednotneho formatu a ulozeni do CSV a SQLite. Vstupni zadani viz [ANA-02](../analyses/ANA-02_automotive_etl_zadani.md).

## Co demonstruje

- Airflow cte **ruzne formaty** z ruznych stroju (CSV z stroj_1, JSON z stroj_2)
- **Transformuje** do spolecne struktury (normalizace statusu, sjednoceni schematu)
- **Zapisuje** do CSV i SQLite databaze
- Scheduling, retry, monitoring funguje out-of-the-box

## Struktura

```
dags/automotive_etl.py      # DAG definice (hlavni logika)
data/stroj_1/measurements.csv   # simulovana vstupni data (CSV)
data/stroj_2/readings.json      # simulovana vstupni data (JSON)
data/output/                     # vystupy (CSV + SQLite DB)
run.ps1                          # startovaci skript (dynamicky port)
docker-compose.yaml              # Airflow stack (upraveny - data mount, dynamicky port)
```

## Spusteni

```powershell
.\run.ps1
```

Skript automaticky zvoli volny port (8080, 8081, ...) a vypise URL.
Instalace viz [gud01_install_docker](../../guides/gud01_install_docker/README.md).

## DAG: automotive_etl

```
[extract_stroj_1] --\
                     +--> [transform] --> [load_to_csv]
[extract_stroj_2] --/                \--> [load_to_db]
```

Detaily viz [dags/automotive_etl.py](dags/automotive_etl.py).

## Poznamky

- **Idempotence**: aktualne neni - kazdy run prida duplicitni radky do SQLite. CSV se prepise. Pro produkci resit UPSERT/truncate+insert.
- **`docker compose down`** zachova data (DB volume). **`down -v`** smaze vse.
- **airflow-init** je jednorazovy kontejner (init DB + admin ucet), po startu se ukonci - to je spravne.

## Analyzy

- [ANA-01: Troubleshooting](analyses/ANA-01_troubleshooting.md) - debugging, zasekle runy, REST API
- [ANA-01: Airflow vs NiFi overview](../analyses/ANA-01_poc_airflow_vs_nifi.md)
- [ANA-02: Vstupni zadani](../analyses/ANA-02_automotive_etl_zadani.md)
