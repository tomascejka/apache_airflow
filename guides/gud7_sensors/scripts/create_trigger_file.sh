#!/bin/bash
# Vytvori soubor ktery FileSensor ceka
# Pouziti: docker compose exec airflow-worker bash /opt/airflow/data/create_trigger_file.sh
# Nebo z hostitele: touch data/trigger_file.csv

echo "stroj_id,teplota,vibrace" > /opt/airflow/data/trigger_file.csv
echo "CNC-001,72.5,0.12" >> /opt/airflow/data/trigger_file.csv
echo "CNC-002,68.3,0.08" >> /opt/airflow/data/trigger_file.csv
echo "Soubor /opt/airflow/data/trigger_file.csv vytvoren."
