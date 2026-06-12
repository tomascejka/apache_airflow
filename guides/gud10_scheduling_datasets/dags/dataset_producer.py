"""
DAG: dataset_producer
Producer DAG — generuje data a aktualizuje dataset (asset).

Koncepty:
- Asset (drive Dataset) = logicky identifikator dat
- Outlet: task oznaci asset jako aktualizovany
- Consumer DAG se automaticky triggeruje pri aktualizaci assetu

Airflow 3.x: Dataset prejmenovany na Asset
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG, Asset
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# Definice assetu (logicky identifikator — URI)
machine_data = Asset("machine_data_daily")


def produce_data(**kwargs):
    """Simulace produkce dat."""
    logger.info("Generuji data stroju pro %s", kwargs.get("ds"))
    logger.info("Data vygenerovana a zapsana (simulace)")
    return {"records": 150, "date": kwargs.get("ds")}


with DAG(
    "dataset_producer",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Producer DAG - aktualizuje asset (dataset)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud10", "dataset", "producer"],
) as dag:

    produce = PythonOperator(
        task_id="produce_data",
        python_callable=produce_data,
        outlets=[machine_data],  # tento task aktualizuje asset
    )
