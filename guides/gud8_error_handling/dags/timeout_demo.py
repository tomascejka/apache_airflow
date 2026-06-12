"""
DAG: timeout_demo
Demonstrace timeoutu — execution_timeout a dagrun_timeout.

Koncepty:
- execution_timeout: max doba behu jednoho tasku
- dagrun_timeout: max doba behu celeho DAG runu
- Pri timeoutu task dostane AirflowTaskTimeout exception
"""

from datetime import datetime, timedelta
import logging
import time

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def fast_task():
    """Task ktery dobehne rychle."""
    logger.info("Fast task — hotovo za 2 sekundy")
    time.sleep(2)
    logger.info("Fast task HOTOV")


def slow_task():
    """Task ktery bezi prilis dlouho — bude prerusen timeoutem."""
    logger.info("Slow task — spim 120 sekund (ale timeout je 15s)...")
    for i in range(120):
        time.sleep(1)
        if i % 5 == 0:
            logger.info("Slow task: %d sekund...", i)
    logger.info("Slow task HOTOV (tohle by se nemelo zobrazit)")


def normal_task():
    """Normalni task po timeoutu."""
    logger.info("Normal task — bezi po timeout tasku")


with DAG(
    "timeout_demo",
    default_args={
        "retries": 0,
    },
    description="Timeouty - execution_timeout a dagrun_timeout",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
    tags=["gud8", "timeout"],
) as dag:

    t_fast = PythonOperator(
        task_id="fast_task",
        python_callable=fast_task,
        execution_timeout=timedelta(seconds=30),
    )

    t_slow = PythonOperator(
        task_id="slow_task_with_timeout",
        python_callable=slow_task,
        execution_timeout=timedelta(seconds=15),  # timeout po 15s
    )

    t_after = PythonOperator(
        task_id="after_timeout",
        python_callable=normal_task,
    )

    t_fast >> t_slow >> t_after
