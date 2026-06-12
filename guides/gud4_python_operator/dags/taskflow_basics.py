"""
DAG: taskflow_basics
Demonstrace TaskFlow API (@task dekorator).

Koncepty:
- @task dekorator (nahrazuje PythonOperator)
- Return value = automaticky XCom push
- Parametry funkce = automaticky XCom pull z upstream tasku
- @dag dekorator
- Porovnani s klasickym PythonOperator pristupem
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import dag as dag_decorator, task

logger = logging.getLogger(__name__)


@dag_decorator(
    dag_id="taskflow_basics",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="TaskFlow API - @task dekorator",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud4", "taskflow"],
)
def taskflow_basics():

    @task
    def extract():
        """Simulace extrakce dat."""
        data = {
            "stroj_id": "CNC-001",
            "teplota": 72.5,
            "vibrace": 0.12,
            "cas": datetime.now().isoformat(),
        }
        logger.info("Extrahovana data: %s", data)
        return data  # automaticky se ulozi do XCom

    @task
    def transform(raw_data: dict):
        """Transformace dat - vstup automaticky z XCom."""
        transformed = {
            "stroj_id": raw_data["stroj_id"],
            "teplota_f": raw_data["teplota"] * 9 / 5 + 32,
            "vibrace_status": "OK" if raw_data["vibrace"] < 0.5 else "ALARM",
            "zpracovano": datetime.now().isoformat(),
        }
        logger.info("Transformovana data: %s", transformed)
        return transformed

    @task
    def load(processed_data: dict):
        """Simulace ulozeni dat."""
        logger.info("=== LOAD ===")
        logger.info("Stroj: %s", processed_data["stroj_id"])
        logger.info("Teplota (F): %.1f", processed_data["teplota_f"])
        logger.info("Vibrace: %s", processed_data["vibrace_status"])
        logger.info("Data uspesne ulozena (simulace).")

    # TaskFlow automaticky resi dependencies pres function calls
    raw = extract()
    processed = transform(raw)
    load(processed)


taskflow_basics()
