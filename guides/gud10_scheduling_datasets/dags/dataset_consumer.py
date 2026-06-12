"""
DAG: dataset_consumer
Consumer DAG — automaticky se spusti kdyz producer aktualizuje asset.

Koncepty:
- schedule=[asset] — data-aware scheduling
- DAG se spusti automaticky kdyz VSECHNY uvedene assety jsou aktualizovane
- Neni treba cron ani manual trigger
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG, Asset
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# Stejny asset jako v produceru (stejne URI)
machine_data = Asset("machine_data_daily")


def consume_data(**kwargs):
    """Zpracovani dat po aktualizaci assetu."""
    logger.info("=== Consumer spusten ===")
    logger.info("Asset 'machine_data_daily' byl aktualizovan")
    logger.info("Zpracovavam nova data pro: %s", kwargs.get("ds"))


def generate_report(**kwargs):
    """Generovani reportu z novych dat."""
    logger.info("Report vygenerovan na zaklade novych dat")


with DAG(
    "dataset_consumer",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Consumer DAG - triggerovany assetem",
    schedule=[machine_data],  # data-aware scheduling!
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud10", "dataset", "consumer"],
) as dag:

    consume = PythonOperator(
        task_id="consume_data",
        python_callable=consume_data,
    )

    report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    consume >> report
