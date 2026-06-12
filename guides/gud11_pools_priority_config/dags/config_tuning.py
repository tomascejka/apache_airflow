"""
DAG: config_tuning
Demonstrace klicovych konfiguracnich parametru pro concurrency control.

Koncepty:
- AIRFLOW__CORE__PARALLELISM: max tasku across ALL DAGs (globalni limit)
- AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: max soucasnych tasku jednoho DAGu
- AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: max soucasnych runu jednoho DAGu
- max_active_tasks (na DAG urovni): override globalniho max_active_tasks_per_dag
- max_active_runs (na DAG urovni): override globalniho max_active_runs_per_dag

Tento DAG: max_active_tasks=3, takze max 3 tasky bezi soucasne (i kdyz jich je 8).
"""

import time
from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def worker_task(task_num: int, **kwargs):
    """Simulace prace — trva 8 sekund."""
    logger.info("Worker #%d START", task_num)
    time.sleep(8)
    logger.info("Worker #%d HOTOV", task_num)


with DAG(
    "config_tuning",
    default_args={
        "retries": 0,
    },
    description="Concurrency control - max_active_tasks demo",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=3,  # max 3 tasky soucasne pro tento DAG
    max_active_runs=2,  # max 2 runy soucasne
    tags=["gud11", "config"],
) as dag:

    tasks = []
    for i in range(1, 9):  # 8 tasku
        t = PythonOperator(
            task_id=f"worker_{i}",
            python_callable=worker_task,
            op_kwargs={"task_num": i},
        )
        tasks.append(t)

    # Vsechny tasky jsou nezavisle (paralelni)
    # Ale max_active_tasks=3 omezuje na 3 soucasne
