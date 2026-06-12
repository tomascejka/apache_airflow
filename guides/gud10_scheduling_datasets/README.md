# gud10: Scheduling, Datasets a Catchup

## Popis

Casovani DAGu (cron vyrazy, timedelta), data-aware scheduling (assets/datasets) a chovani catchup/backfill. Klicove pro produkci — spravne nastaveni schedule urcuje kdy a proc se DAG spousti.

## Cile

- Jinja date templates (ds, data_interval_start, data_interval_end) se renderuji spravne
- Asset (dataset) producer triggeruje consumer DAG automaticky (data-aware scheduling)
- Consumer DAG se spusti bez manualniho triggeru (run_type = asset_triggered)
- catchup=True vytvori historicke DAG runy pro zmeskane intervaly (~7 runu)
- Kazdy catchup run ma jiny logical_date (odpovida jinemu datovemu intervalu)
- max_active_runs=2 omezuje pocet soucasne bezicich runu

## Koncepty

| Koncept | Popis |
|---------|-------|
| **Cron vyraz** | Standardni UNIX cron format (minuta hodina den mesic den_tydne) |
| **timedelta** | Interval od start_date (napr. kazdych 2h) |
| **Asset (Dataset)** | Logicky identifikator dat — producer aktualizuje, consumer reaguje |
| **Data-aware scheduling** | DAG se triggeruje pri aktualizaci assetu, ne casem |
| **catchup** | True = vytvor historicke runy od start_date, False = jen aktualni |
| **logical_date** | Datum datoveho intervalu (drive execution_date) |
| **data_interval_start/end** | Zacatek a konec datoveho intervalu |

## DAGy

### cron_demo
- Reference cron vyrazu (v komentarich i v bash vystupu)
- Jinja templates: `{{ ds }}`, `{{ data_interval_start }}`, `{{ data_interval_end }}`
- schedule=None (manual trigger, slouzi jako reference)

### dataset_producer + dataset_consumer
- **Data-aware scheduling**: producer aktualizuje asset → consumer se automaticky spusti
- `Asset("machine_data_daily")` — spolecny identifikator
- `outlets=[asset]` v produceru, `schedule=[asset]` v consumeru
- Airflow 3.x: Dataset prejmenovany na **Asset**

### catchup_demo
- `catchup=True` + `start_date` 7 dni v minulosti
- **POZOR:** pri unpause se vytvori 7 historickych runu!
- `max_active_runs=2` — omezeni paralelismu
- Ukazka logical_date vs data_interval_start/end

## Overeni

```bash
# Spustit stack
.\run.ps1

# Cron reference
docker compose exec airflow-scheduler airflow dags trigger cron_demo

# Dataset demo — spustit producer, consumer se triggeruje automaticky
# 1. Nejdrive unpause oba DAGy
docker compose exec airflow-scheduler airflow dags unpause dataset_producer
docker compose exec airflow-scheduler airflow dags unpause dataset_consumer
# 2. Trigger producer → consumer se spusti sam
docker compose exec airflow-scheduler airflow dags trigger dataset_producer

# Catchup demo — POZOR: vytvori 7 historickych runu!
docker compose exec airflow-scheduler airflow dags unpause catchup_demo
```

**V UI (localhost:8080):**
- Datasets tab: videt asset "machine_data_daily" a jeho producer/consumer
- catchup_demo: 7 DAG runu s ruznymi logical_date
- dataset_consumer: automaticky triggerovany po dataset_producer

## logical_date vs execution_date

| Termin | Airflow 2.x | Airflow 3.x | Vyznam |
|--------|-------------|-------------|--------|
| execution_date | primarni | deprecated alias | zacatek intervalu |
| logical_date | - | primarni | zacatek intervalu |
| data_interval_start | - | dostupne | = logical_date |
| data_interval_end | - | dostupne | konec intervalu |
