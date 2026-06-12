"""
DAG: xcom_demo
Demonstrace XCom - explicitni push/pull, Jinja templates, limity.

Koncepty:
- xcom_push / xcom_pull (explicitni)
- Jinja template {{ ti.xcom_pull(task_ids='...') }}
- XCom s custom key
- Limity: XCom by default max 48KB (SQLite) / neomezeno (Postgres)
"""

from datetime import datetime, timedelta
import json
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

logger = logging.getLogger(__name__)


def push_values(**kwargs):
    """Explicitni XCom push s ruznymi klici."""
    ti = kwargs["ti"]

    # Default key "return_value" — pres return
    # Custom keys — pres xcom_push
    ti.xcom_push(key="stroj_id", value="CNC-001")
    ti.xcom_push(key="metriky", value={"teplota": 72.5, "vibrace": 0.12})

    logger.info("XCom pushed: stroj_id=CNC-001, metriky={teplota: 72.5, vibrace: 0.12}")

    # Return value = automaticky push s key="return_value"
    return "Hodnota z return"


def pull_values(**kwargs):
    """Explicitni XCom pull."""
    ti = kwargs["ti"]

    # Pull default key (return_value)
    return_val = ti.xcom_pull(task_ids="push_values")
    logger.info("return_value: %s", return_val)

    # Pull custom keys
    stroj = ti.xcom_pull(task_ids="push_values", key="stroj_id")
    metriky = ti.xcom_pull(task_ids="push_values", key="metriky")
    logger.info("stroj_id: %s", stroj)
    logger.info("metriky: %s", metriky)
    logger.info("teplota: %s", metriky["teplota"])


def show_xcom_size(**kwargs):
    """Ukazka limitu XCom — co se stane s velkymi daty."""
    ti = kwargs["ti"]

    # Male data — OK
    small = {"status": "ok"}
    ti.xcom_push(key="small_data", value=small)
    logger.info("Small data size: ~%d bytes", len(json.dumps(small)))

    # Stredne data — OK (ale pozor na mnozstvi)
    medium = {f"key_{i}": f"value_{i}" for i in range(100)}
    ti.xcom_push(key="medium_data", value=medium)
    logger.info("Medium data size: ~%d bytes", len(json.dumps(medium)))

    logger.info("POZOR: XCom neni urcen pro velka data (GB)!")
    logger.info("Pro velka data pouzijte: S3, GCS, lokalni soubory, databazi.")


with DAG(
    "xcom_demo",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="XCom - explicitni push/pull a Jinja templates",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud5", "xcom"],
) as dag:

    t_push = PythonOperator(
        task_id="push_values",
        python_callable=push_values,
    )

    t_pull = PythonOperator(
        task_id="pull_values",
        python_callable=pull_values,
    )

    # Jinja template pristup k XCom — bez Python kodu
    t_jinja = BashOperator(
        task_id="jinja_xcom",
        bash_command=(
            'echo "Return value: {{ ti.xcom_pull(task_ids=\'push_values\') }}" && '
            'echo "Stroj: {{ ti.xcom_pull(task_ids=\'push_values\', key=\'stroj_id\') }}"'
        ),
    )

    t_size = PythonOperator(
        task_id="show_xcom_size",
        python_callable=show_xcom_size,
    )

    t_push >> [t_pull, t_jinja, t_size]
