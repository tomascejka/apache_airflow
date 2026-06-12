"""
PoC3: Automotive ETL s Edge Workerem - Varianta B (ANA-05).

Architektura:
- EXTRACT + TRANSFORM bezi na Edge Workeru (simuluje PC na vyrobni lince)
  - Edge zna sve syrova data a namapuje je na dohodnuty standardizovany format
  - Novy stroj = zmena JEN na edge, server se nemeni
- LOAD bezi na centrale (univerzalni handler, nezavisly na typu stroje)

Tok:
  [extract_transform_stroj_1] (edge) --\
                                        +--> [load_to_csv] (central)
  [extract_transform_stroj_2] (edge) --/  +-> [load_to_db]  (central)

Standardizovany format (kontrakt edge -> central):
  {
    "timestamp": "YYYY-MM-DD HH:MM:SS",
    "machine": "<id_stroje>",
    "device_id": "<id_zarizeni>",
    "temperature": <float>,
    "status": "ok|warning|critical",
    "extracted_at": "<ISO timestamp>"
  }
"""
import csv
import json
import os
import socket
import sqlite3
from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

EDGE_EXECUTOR = "airflow.providers.edge3.executors.EdgeExecutor"
DATA_DIR = "/opt/airflow/data"
OUTPUT_DIR = "/opt/airflow/data/output"
DB_PATH = "/opt/airflow/data/output/measurements.db"

# Kontrakt: normalizace statusu (kazdy edge si mapuje sve raw hodnoty)
STATUS_MAP = {
    "ok": "ok", "running": "ok",
    "warn": "warning", "warning": "warning",
    "error": "critical", "critical": "critical",
}


def extract_transform_stroj_1(**context):
    """Bezi na EDGE WORKERU - cte CSV z stroj_1 a transformuje na standardizovany format."""
    hostname = socket.gethostname()
    print(f"[extract_transform_stroj_1] hostname: {hostname} (edge worker)")

    rows = []
    path = os.path.join(DATA_DIR, "stroj_1", "measurements.csv")
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_status = row["status"].lower()
            rows.append({
                "timestamp": row["timestamp"],
                "machine": "stroj_1",
                "device_id": row["device_id"],
                "temperature": float(row["temperature"]),
                "status": STATUS_MAP.get(raw_status, raw_status),
                "extracted_at": datetime.now().isoformat(),
            })

    print(f"Extrahovano a transformovano {len(rows)} radku z stroj_1")
    context["ti"].xcom_push(key="data", value=rows)


def extract_transform_stroj_2(**context):
    """Bezi na EDGE WORKERU - cte JSON z stroj_2 a transformuje na standardizovany format."""
    hostname = socket.gethostname()
    print(f"[extract_transform_stroj_2] hostname: {hostname} (edge worker)")

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
            "status": STATUS_MAP.get(item["state"], item["state"]),
            "extracted_at": datetime.now().isoformat(),
        })

    print(f"Extrahovano a transformovano {len(rows)} radku z stroj_2")
    context["ti"].xcom_push(key="data", value=rows)


# ---------------------------------------------------------------------------
# LOAD - univerzalni handlery na centrale (nezavisle na typu stroje)
# Prijimaji standardizovany format, neznaji puvodni raw strukturu.
# ---------------------------------------------------------------------------

STANDARD_FIELDS = ["timestamp", "machine", "device_id", "temperature", "status", "extracted_at"]


def _collect_all_data(ti, source_task_ids):
    """Sebere standardizovana data ze vsech edge tasku."""
    all_data = []
    for task_id in source_task_ids:
        data = ti.xcom_pull(key="data", task_ids=task_id)
        if data:
            all_data.extend(data)
    return all_data


def load_to_csv(**context):
    """Bezi na CENTRALE - univerzalni CSV writer."""
    hostname = socket.gethostname()
    print(f"[load_to_csv] hostname: {hostname} (centrala)")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ti = context["ti"]
    data = _collect_all_data(ti, ["extract_transform_stroj_1", "extract_transform_stroj_2"])

    output_path = os.path.join(OUTPUT_DIR, "unified_measurements.csv")
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=STANDARD_FIELDS)
        writer.writeheader()
        writer.writerows(data)
    print(f"Zapsano {len(data)} radku do {output_path}")


def load_to_db(**context):
    """Bezi na CENTRALE - univerzalni SQLite writer."""
    hostname = socket.gethostname()
    print(f"[load_to_db] hostname: {hostname} (centrala)")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ti = context["ti"]
    data = _collect_all_data(ti, ["extract_transform_stroj_1", "extract_transform_stroj_2"])

    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS measurements (
            timestamp TEXT, machine TEXT, device_id TEXT,
            temperature REAL, status TEXT, extracted_at TEXT
        )
    """)
    conn.executemany(
        "INSERT INTO measurements VALUES (?, ?, ?, ?, ?, ?)",
        [(r["timestamp"], r["machine"], r["device_id"], r["temperature"], r["status"], r["extracted_at"]) for r in data],
    )
    conn.commit()
    conn.close()
    print(f"Zapsano {len(data)} radku do {DB_PATH}")


with DAG(
    "edge_automotive_etl",
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    description="PoC3: Extract+Transform na edge workeru, Load na centrale (Varianta B)",
    schedule=timedelta(hours=1),
    start_date=datetime(2026, 6, 12),
    catchup=False,
    tags=["poc", "automotive", "edge"],
) as dag:

    # EXTRACT + TRANSFORM - bezi na Edge Workeru (linka)
    t_et_1 = PythonOperator(
        task_id="extract_transform_stroj_1",
        python_callable=extract_transform_stroj_1,
        executor=EDGE_EXECUTOR,
        queue="edge_queue",
    )
    t_et_2 = PythonOperator(
        task_id="extract_transform_stroj_2",
        python_callable=extract_transform_stroj_2,
        executor=EDGE_EXECUTOR,
        queue="edge_queue",
    )

    # LOAD - bezi na centrale (CeleryExecutor, default) - univerzalni handlery
    t_load_csv = PythonOperator(task_id="load_to_csv", python_callable=load_to_csv)
    t_load_db = PythonOperator(task_id="load_to_db", python_callable=load_to_db)

    t_et_1 >> [t_load_csv, t_load_db]
    t_et_2 >> [t_load_csv, t_load_db]
