"""
DAG: branching_demo
Demonstrace BranchPythonOperator — podmineny vyber cesty.

Koncepty:
- BranchPythonOperator vraci task_id vetvy, ktera se ma spustit
- Nespustene vetvy dostanou stav "skipped"
- Downstream tasky po branchi musi mit spravny trigger_rule
"""

import random
from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)


def choose_branch(**kwargs):
    """Nahodny vyber vetvy A nebo B."""
    choice = random.choice(["branch_a", "branch_b"])
    logger.info("Zvolena vetev: %s", choice)
    return choice  # vraci task_id vetvy


def process_a():
    logger.info("Zpracovavam vetev A — normalni rezim")


def process_b():
    logger.info("Zpracovavam vetev B — alternativni rezim")


with DAG(
    "branching_demo",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="BranchPythonOperator - podmineny vyber cesty",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud6", "branching"],
) as dag:

    start = EmptyOperator(task_id="start")

    branch = BranchPythonOperator(
        task_id="choose_branch",
        python_callable=choose_branch,
    )

    a = PythonOperator(task_id="branch_a", python_callable=process_a)
    b = PythonOperator(task_id="branch_b", python_callable=process_b)

    # Join task — MUSI mit trigger_rule="none_failed_min_one_success"
    # jinak bude "skipped" protoze jedna vetev je vzdy skipped
    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    end = BashOperator(
        task_id="end",
        bash_command='echo "Workflow dokoncen — obe cesty se spojily"',
        trigger_rule="none_failed_min_one_success",
    )

    start >> branch >> [a, b] >> join >> end
