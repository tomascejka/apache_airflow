"""
Demo DAG pro monitoring - simuluje komplexni workflow s vice tasky.

Ucel: videt v Grafane/Prometheus jak tasky postupne probihaji.
Airflow neposkytuje "procenta dokonceni" tasku, ale poskytuje:
  - pocet tasku ve stavu queued/running/success/failed
  - dobu behu kazdeho tasku (dagrun duration, task duration)
  - pocet tasku ve fronte (executor_queued_tasks, executor_running_tasks)

Workflow:
  [collect_data] --> [validate] --> [process_batch_1] --\
                                   [process_batch_2] ---+--> [aggregate] --> [export_csv]
                                   [process_batch_3] --/                 \-> [export_db]
                                   [process_batch_4] --/                 \-> [notify]

Kazdy task simuluje praci pres sleep (ruzne dlouho).
"""
import socket
import time
from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _work(task_name, duration_seconds, **context):
    """Simuluje praci - loguje prubeh po 10% intervalech."""
    hostname = socket.gethostname()
    print(f"[{task_name}] START na {hostname}, planovana doba: {duration_seconds}s")

    steps = 10
    step_duration = duration_seconds / steps
    for i in range(1, steps + 1):
        time.sleep(step_duration)
        pct = i * 10
        print(f"[{task_name}] {pct}% hotovo ({i * step_duration:.0f}s / {duration_seconds}s)")

    print(f"[{task_name}] DONE")


with DAG(
    "monitoring_demo",
    default_args={"retries": 0},
    description="Demo pro monitoring - 9 tasku, ruzne doby behu",
    schedule=None,
    start_date=datetime(2026, 6, 12),
    catchup=False,
    tags=["poc", "monitoring", "demo"],
) as dag:

    t_collect = PythonOperator(
        task_id="collect_data",
        python_callable=_work,
        op_kwargs={"task_name": "collect_data", "duration_seconds": 15},
    )

    t_validate = PythonOperator(
        task_id="validate",
        python_callable=_work,
        op_kwargs={"task_name": "validate", "duration_seconds": 10},
    )

    batches = []
    for i in range(1, 5):
        t = PythonOperator(
            task_id=f"process_batch_{i}",
            python_callable=_work,
            op_kwargs={"task_name": f"process_batch_{i}", "duration_seconds": 20 + i * 5},
        )
        batches.append(t)

    t_aggregate = PythonOperator(
        task_id="aggregate",
        python_callable=_work,
        op_kwargs={"task_name": "aggregate", "duration_seconds": 15},
    )

    t_export_csv = PythonOperator(
        task_id="export_csv",
        python_callable=_work,
        op_kwargs={"task_name": "export_csv", "duration_seconds": 10},
    )

    t_export_db = PythonOperator(
        task_id="export_db",
        python_callable=_work,
        op_kwargs={"task_name": "export_db", "duration_seconds": 10},
    )

    t_notify = PythonOperator(
        task_id="notify",
        python_callable=_work,
        op_kwargs={"task_name": "notify", "duration_seconds": 5},
    )

    t_collect >> t_validate >> batches
    batches >> t_aggregate >> [t_export_csv, t_export_db, t_notify]
