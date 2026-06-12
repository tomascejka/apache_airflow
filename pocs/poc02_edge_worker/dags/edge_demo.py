"""
PoC: Demonstrace Edge Worker architektury.

- central_task bezi na centralnim Airflow (CeleryExecutor, default)
- edge_task bezi na Edge Workeru (simuluje remote stroj na lince)
- report_task sbira vysledky na centrále
"""
import socket
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

EDGE_EXECUTOR = "airflow.providers.edge3.executors.EdgeExecutor"


def run_on_central(**context):
    hostname = socket.gethostname()
    msg = f"Central task bezi na: {hostname}"
    print(msg)
    context["ti"].xcom_push(key="central_result", value=msg)


def run_on_edge(**context):
    hostname = socket.gethostname()
    msg = f"Edge task bezi na: {hostname}"
    print(msg)
    context["ti"].xcom_push(key="edge_result", value=msg)


def report(**context):
    ti = context["ti"]
    central = ti.xcom_pull(key="central_result", task_ids="central_task")
    edge = ti.xcom_pull(key="edge_result", task_ids="edge_task")
    print(f"=== REPORT ===")
    print(f"Central: {central}")
    print(f"Edge:    {edge}")
    print(f"Hostnames by se mely lisit - to dokazuje distribuovane spusteni.")


with DAG(
    "edge_demo",
    description="PoC: Central vs Edge Worker",
    schedule=None,
    start_date=datetime(2026, 6, 12),
    catchup=False,
    tags=["poc", "edge"],
) as dag:

    t_central = PythonOperator(
        task_id="central_task",
        python_callable=run_on_central,
    )

    t_edge = PythonOperator(
        task_id="edge_task",
        python_callable=run_on_edge,
        executor=EDGE_EXECUTOR,
        queue="edge_queue",
    )

    t_report = PythonOperator(
        task_id="report_task",
        python_callable=report,
    )

    [t_central, t_edge] >> t_report
