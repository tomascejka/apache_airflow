"""
DAG: python_basics
Demonstrace PythonOperator - klasicky pristup (python_callable).

Koncepty:
- PythonOperator s python_callable
- Pristup ke kontextu (** kwargs)
- logging modul vs print
- op_args a op_kwargs
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def greet(name, greeting="Ahoj"):
    """Jednoducha funkce s argumenty."""
    message = f"{greeting}, {name}! Cas: {datetime.now().isoformat()}"
    logger.info(message)
    print(f"[print] {message}")  # print jde do stdout, logger do Airflow logs
    return message


def show_context(**kwargs):
    """Ukazka pristupu ke kontextu TaskInstance."""
    ti = kwargs["ti"]
    dag_run = kwargs["dag_run"]

    logger.info("=== Task Context ===")
    logger.info("task_id: %s", ti.task_id)
    logger.info("dag_id: %s", ti.dag_id)
    logger.info("run_id: %s", dag_run.run_id)
    logger.info("logical_date: %s", kwargs.get("logical_date"))
    logger.info("ds: %s", kwargs.get("ds"))

    # Vypis vsech klicu v kontextu
    logger.info("Dostupne klice v kontextu: %s", sorted(kwargs.keys()))


def compute(x, y, operation="add"):
    """Funkce s op_args a op_kwargs."""
    if operation == "add":
        result = x + y
    elif operation == "multiply":
        result = x * y
    else:
        result = x - y

    logger.info("%s(%d, %d) = %d", operation, x, y, result)
    return result


with DAG(
    "python_basics",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="PythonOperator - klasicky pristup",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud4", "python"],
) as dag:

    t1 = PythonOperator(
        task_id="greet_user",
        python_callable=greet,
        op_args=["Airflow"],  # pozicni argumenty
        op_kwargs={"greeting": "Nazdar"},  # pojmenovane argumenty
    )

    t2 = PythonOperator(
        task_id="show_context",
        python_callable=show_context,
    )

    t3 = PythonOperator(
        task_id="compute_result",
        python_callable=compute,
        op_args=[10, 5],
        op_kwargs={"operation": "multiply"},
    )

    t1 >> t2 >> t3
