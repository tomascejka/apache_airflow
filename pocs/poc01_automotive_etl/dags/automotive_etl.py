import csv
import json
import os
import sqlite3
from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

DATA_DIR = "/opt/airflow/data"
OUTPUT_DIR = "/opt/airflow/data/output"
DB_PATH = "/opt/airflow/data/output/measurements.db"


def extract_stroj_1(**context):
    """Nacte CSV z stroj_1, vrati unified seznam."""
    rows = []
    path = os.path.join(DATA_DIR, "stroj_1", "measurements.csv")
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "timestamp": row["timestamp"],
                "machine": "stroj_1",
                "device_id": row["device_id"],
                "temperature": float(row["temperature"]),
                "status": row["status"].lower(),
            })
    context["ti"].xcom_push(key="stroj_1_data", value=rows)


def extract_stroj_2(**context):
    """Nacte JSON z stroj_2, vrati unified seznam."""
    path = os.path.join(DATA_DIR, "stroj_2", "readings.json")
    with open(path, "r") as f:
        raw = json.load(f)

    rows = []
    for item in raw:
        rows.append({
            "timestamp": item["ts"].replace("T", " "),
            "machine": "stroj_2",
            "device_id": item["sensor"],
            "temperature": item["temp_c"],
            "status": item["state"],
        })
    context["ti"].xcom_push(key="stroj_2_data", value=rows)


def transform(**context):
    """Spoji data z obou stroju, prida metadata."""
    ti = context["ti"]
    data_1 = ti.xcom_pull(key="stroj_1_data", task_ids="extract_stroj_1")
    data_2 = ti.xcom_pull(key="stroj_2_data", task_ids="extract_stroj_2")

    all_data = data_1 + data_2
    for row in all_data:
        row["processed_at"] = datetime.now().isoformat()
        # normalizace statusu
        status_map = {"ok": "ok", "running": "ok", "warn": "warning", "warning": "warning", "error": "critical", "critical": "critical"}
        row["status_normalized"] = status_map.get(row["status"], row["status"])

    context["ti"].xcom_push(key="unified_data", value=all_data)


def load_to_csv(**context):
    """Zapise unified data do CSV."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    data = context["ti"].xcom_pull(key="unified_data", task_ids="transform")

    output_path = os.path.join(OUTPUT_DIR, "unified_measurements.csv")
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "machine", "device_id", "temperature", "status", "status_normalized", "processed_at"])
        writer.writeheader()
        writer.writerows(data)


def load_to_db(**context):
    """Zapise unified data do SQLite."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    data = context["ti"].xcom_pull(key="unified_data", task_ids="transform")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS measurements (
            timestamp TEXT,
            machine TEXT,
            device_id TEXT,
            temperature REAL,
            status_normalized TEXT,
            processed_at TEXT
        )
    """)
    conn.executemany(
        "INSERT INTO measurements VALUES (?, ?, ?, ?, ?, ?)",
        [(r["timestamp"], r["machine"], r["device_id"], r["temperature"], r["status_normalized"], r["processed_at"]) for r in data],
    )
    conn.commit()
    conn.close()


with DAG(
    "automotive_etl",
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    description="PoC: sber dat ze stroju, transformace, ulozeni",
    schedule=timedelta(hours=1),
    start_date=datetime(2026, 6, 12),
    catchup=False,
    tags=["poc", "automotive"],
) as dag:

    t_extract_1 = PythonOperator(task_id="extract_stroj_1", python_callable=extract_stroj_1)
    t_extract_2 = PythonOperator(task_id="extract_stroj_2", python_callable=extract_stroj_2)
    t_transform = PythonOperator(task_id="transform", python_callable=transform)
    t_load_csv = PythonOperator(task_id="load_to_csv", python_callable=load_to_csv)
    t_load_db = PythonOperator(task_id="load_to_db", python_callable=load_to_db)

    [t_extract_1, t_extract_2] >> t_transform >> [t_load_csv, t_load_db]
