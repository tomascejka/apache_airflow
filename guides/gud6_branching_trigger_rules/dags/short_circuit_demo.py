"""
DAG: short_circuit_demo
Demonstrace ShortCircuitOperator — podminene preskoceni downstream.

Koncepty:
- ShortCircuitOperator: pokud vrati False, vsechny downstream se preskoci
- Pokud vrati True (nebo truthy), downstream se spusti normalne
- Uzitecne pro: maintenance okna, feature flags, podminene zpracovani
"""

import random
from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.standard.operators.bash import BashOperator

logger = logging.getLogger(__name__)


def check_should_run(**kwargs):
    """Simulace rozhodnuti zda pokracovat."""
    should_run = random.choice([True, False])
    logger.info("should_run = %s", should_run)
    if not should_run:
        logger.info("Short circuit! Downstream tasky budou preskoceny.")
    return should_run


def check_is_weekday(**kwargs):
    """Kontrola zda je pracovni den."""
    logical_date = kwargs.get("logical_date")
    if logical_date is None:
        from datetime import datetime
        logical_date = datetime.now()
        logger.info("logical_date=None (manual trigger), pouzivam aktualni cas")
    day = logical_date.weekday()
    is_weekday = day < 5
    logger.info("Den v tydnu: %d (0=po, 6=ne), is_weekday=%s", day, is_weekday)
    return is_weekday


with DAG(
    "short_circuit_demo",
    default_args={
        "retries": 0,
    },
    description="ShortCircuitOperator - podminene preskoceni",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud6", "short_circuit"],
) as dag:

    check = ShortCircuitOperator(
        task_id="check_should_run",
        python_callable=check_should_run,
    )

    step_1 = BashOperator(
        task_id="step_1",
        bash_command='echo "Krok 1 — bezi jen kdyz check vratil True"',
    )

    step_2 = BashOperator(
        task_id="step_2",
        bash_command='echo "Krok 2 — pokracovani zpracovani"',
    )

    check_weekday = ShortCircuitOperator(
        task_id="check_weekday",
        python_callable=check_is_weekday,
    )

    weekday_task = BashOperator(
        task_id="weekday_only_task",
        bash_command='echo "Tento task bezi jen v pracovni dny"',
    )

    check >> step_1 >> step_2
    check_weekday >> weekday_task
