"""
DAG: trigger_rules_demo
Demonstrace trigger_rule — kdy se task spusti.

Koncepty:
- all_success (default): vsechny upstream uspely
- one_success: alespon jeden uspel
- all_done: vsechny dokonceny (success/failed/skipped)
- none_failed_min_one_success: zadny nefailoval + alespon jeden uspel
- all_failed: vsechny failovaly
- one_failed: alespon jeden failoval

Upozorneni: task "failing_task" ZAMERNE failuje pro demonstraci.
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)


def succeed():
    logger.info("Task USPEL")


def fail():
    logger.info("Task ZAMERNE FAILUJE")
    raise ValueError("Zamerna chyba pro demonstraci trigger rules")


with DAG(
    "trigger_rules_demo",
    default_args={
        "retries": 0,  # zadne retries — chceme videt fail hned
    },
    description="Trigger rules - pravidla spousteni tasku",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud06", "trigger_rules"],
) as dag:

    t_success_1 = PythonOperator(task_id="success_1", python_callable=succeed)
    t_success_2 = PythonOperator(task_id="success_2", python_callable=succeed)
    t_failing = PythonOperator(task_id="failing_task", python_callable=fail)

    # all_success (default) — nespusti se, protoze failing_task failoval
    t_all_success = BashOperator(
        task_id="needs_all_success",
        bash_command='echo "Vsechny upstream uspely"',
        trigger_rule="all_success",
    )

    # one_success — spusti se, protoze alespon success_1 uspel
    t_one_success = BashOperator(
        task_id="needs_one_success",
        bash_command='echo "Alespon jeden upstream uspel"',
        trigger_rule="one_success",
    )

    # all_done — spusti se vzdy (vsechny upstream dokonceny)
    t_all_done = BashOperator(
        task_id="runs_when_all_done",
        bash_command='echo "Vsechny upstream dokonceny (at uz jak)"',
        trigger_rule="all_done",
    )

    # none_failed_min_one_success — nespusti se (failing_task failoval)
    t_none_failed = BashOperator(
        task_id="needs_none_failed",
        bash_command='echo "Zadny nefailoval + alespon jeden uspel"',
        trigger_rule="none_failed_min_one_success",
    )

    # Cleanup — spusti se vzdy
    t_cleanup = BashOperator(
        task_id="cleanup_always",
        bash_command='echo "Cleanup — bezi vzdy po vsech taskach"',
        trigger_rule="all_done",
    )

    [t_success_1, t_success_2, t_failing] >> t_all_success
    [t_success_1, t_success_2, t_failing] >> t_one_success
    [t_success_1, t_success_2, t_failing] >> t_all_done
    [t_success_1, t_success_2, t_failing] >> t_none_failed

    [t_all_success, t_one_success, t_all_done, t_none_failed] >> t_cleanup
