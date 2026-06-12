"""
DAG: callbacks_demo
Demonstrace callbacku — funkce volane pri ruznych udalostech.

Koncepty:
- on_success_callback: po uspesnem dokonceni tasku
- on_failure_callback: po selhani tasku (vcetne vsech retries)
- on_retry_callback: pri kazdem retry pokusu
- sla_miss_callback: pri prekroceni SLA (na DAG urovni)
- Callback dostane context (stejny jako **kwargs v PythonOperator)
"""

import random
from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)


# === CALLBACK FUNKCE ===

def on_success(context):
    """Volana po uspesnem dokonceni tasku."""
    ti = context["ti"]
    logger.info("CALLBACK on_success: Task '%s' USPEL (pokus #%d)",
                ti.task_id, ti.try_number)


def on_failure(context):
    """Volana po selhani tasku (po vyčerpani vsech retries)."""
    ti = context["ti"]
    exception = context.get("exception", "unknown")
    logger.error("CALLBACK on_failure: Task '%s' SELHAL! Chyba: %s",
                 ti.task_id, exception)
    logger.error("Zde by se poslal alert (email, Slack, PagerDuty, ...)")


def on_retry(context):
    """Volana pri kazdem retry pokusu."""
    ti = context["ti"]
    logger.warning("CALLBACK on_retry: Task '%s' retry pokus #%d",
                   ti.task_id, ti.try_number)


# === TASK FUNKCE ===

def sometimes_fails(**kwargs):
    """Task s 50% sanci na fail."""
    if random.random() < 0.5:
        raise Exception("Nahodna chyba!")
    logger.info("Task uspel")


def always_fails():
    """Task ktery vzdy failuje — pro demonstraci on_failure."""
    raise Exception("Tento task vzdy failuje!")


def always_succeeds():
    """Task ktery vzdy uspeje — pro demonstraci on_success."""
    logger.info("Tento task vzdy uspeje")


with DAG(
    "callbacks_demo",
    default_args={
        "retries": 0,
    },
    description="Callbacky - on_success, on_failure, on_retry",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud08", "callbacks"],
) as dag:

    t_success = PythonOperator(
        task_id="always_succeeds",
        python_callable=always_succeeds,
        on_success_callback=on_success,
        on_failure_callback=on_failure,
    )

    t_fail = PythonOperator(
        task_id="always_fails",
        python_callable=always_fails,
        on_success_callback=on_success,
        on_failure_callback=on_failure,
        retries=0,
    )

    t_retry = PythonOperator(
        task_id="sometimes_fails_with_retry",
        python_callable=sometimes_fails,
        on_success_callback=on_success,
        on_failure_callback=on_failure,
        on_retry_callback=on_retry,
        retries=3,
        retry_delay=timedelta(seconds=5),
    )

    # Parallel — nezavisle na sobe
    [t_success, t_fail, t_retry]
