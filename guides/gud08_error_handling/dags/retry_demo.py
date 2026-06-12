"""
DAG: retry_demo
Demonstrace retry mechanismu — task ktery nahodne failuje.

Koncepty:
- retries: pocet opakovani pri selhani
- retry_delay: prodleva mezi pokusy
- retry_exponential_backoff: exponencialni navysovani prodlevy
- max_retry_delay: maximalni prodleva
- Task stavy: running → up_for_retry → running → success/failed
"""

import random
from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def unreliable_task(**kwargs):
    """Task ktery failuje s 60% pravdepodobnosti."""
    ti = kwargs["ti"]
    attempt = ti.try_number

    logger.info("=== Pokus #%d ===", attempt)
    logger.info("Max pokusu: %d (1 original + retries)", ti.max_tries + 1)

    # 60% sance na fail
    if random.random() < 0.6:
        logger.error("CHYBA! Task failoval (nahodny fail, pokus #%d)", attempt)
        raise Exception(f"Nahodna chyba pri pokusu #{attempt}")

    logger.info("USPECH! Task uspel na pokus #%d", attempt)
    return f"OK na pokus #{attempt}"


def always_succeeds():
    """Task ktery vzdy uspeje — pro porovnani."""
    logger.info("Tento task vzdy uspeje")


with DAG(
    "retry_demo",
    default_args={
        "retries": 0,
    },
    description="Retry mechanismus - nahodny fail s retries",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud08", "retry"],
) as dag:

    t_unreliable = PythonOperator(
        task_id="unreliable_task",
        python_callable=unreliable_task,
        retries=3,
        retry_delay=timedelta(seconds=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=1),
    )

    t_after = PythonOperator(
        task_id="after_retry",
        python_callable=always_succeeds,
    )

    t_unreliable >> t_after
