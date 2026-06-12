# gud07: Sensors

## Popis

Sensors jsou specialni operatory ktere cekaji na splneni externi podminky (soubor, cas, jiny DAG). Klicovy pattern pro integraci s externimi systemy.

## Cile

- FileSensor detekuje soubor na disku a spusti downstream tasky
- TimeDeltaSensor ceka zadany casovy usek (30 sekund)
- ExternalTaskSensor ceka na dokonceni tasku v jinem DAGu
- soft_fail=True zpusobi skip (ne fail) pri timeoutu sensoru
- Sensor poke_interval a timeout parametry funguji spravne

## Koncepty

| Koncept | Popis |
|---------|-------|
| **FileSensor** | Ceka na existenci souboru na disku |
| **TimeDeltaSensor** | Ceka urcity casovy usek od logical_date |
| **ExternalTaskSensor** | Ceka na dokonceni tasku v jinem DAGu |
| **poke_interval** | Jak casto sensor kontroluje podminku (sekundy) |
| **timeout** | Po jake dobe sensor failuje/skipne |
| **mode** | `poke` (drzi worker slot) vs `reschedule` (uvolni mezi pokusy) |
| **soft_fail** | `True` = pri timeoutu skip, `False` = fail |

## DAGy

### file_sensor_demo
- FileSensor ceka na `/opt/airflow/data/trigger_file.csv`
- Cleanup → sensor → zpracovani souboru
- Interaktivni: spustit DAG, pak rucne vytvorit soubor

### time_sensor_demo
- TimeDeltaSensor ceka 30 sekund
- Videt v logs casovy rozdil mezi start a after_wait

### external_task_sensor_demo
- Dva DAGy: `sensor_producer` a `sensor_consumer`
- Consumer ceka na dokonceni producera
- ExternalTaskSensor s poke_interval=10s

## Overeni

```bash
# Spustit stack
.\run.ps1

# --- File Sensor ---
# 1. Trigger DAG (sensor zacne cekat)
docker compose exec airflow-scheduler airflow dags trigger file_sensor_demo
# 2. Vytvorit soubor (sensor detekuje → downstream bezi)
echo "stroj_id,teplota" > data/trigger_file.csv
echo "CNC-001,72.5" >> data/trigger_file.csv

# --- Time Sensor ---
docker compose exec airflow-scheduler airflow dags trigger time_sensor_demo

# --- External Task Sensor ---
# 1. Spustit producer
docker compose exec airflow-scheduler airflow dags trigger sensor_producer
# 2. Spustit consumer (bude cekat na producera)
docker compose exec airflow-scheduler airflow dags trigger sensor_consumer
```

**V UI (localhost:8080):**
- file_sensor_demo: sensor sviti oranzove (running/sensing) dokud nevytvorite soubor
- time_sensor_demo: sensor ceka 30s, pak pokracuje
- sensor_consumer: ceka na sensor_producer

## Poke vs Reschedule mode

| Mode | Chovani | Vhodne pro |
|------|---------|------------|
| `poke` | Drzi worker slot, opakuje kontrolu | Kratke cekani (sekundy/minuty) |
| `reschedule` | Uvolni slot, naplánuje novy pokus | Dlouhe cekani (hodiny) |
