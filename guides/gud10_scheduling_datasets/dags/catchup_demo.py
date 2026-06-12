"""
DAG: catchup_demo
Demonstrace catchup a backfill.

Koncepty:
- catchup=True: pri prvnim spusteni Airflow vytvori DAG runy pro vsechny
  "zmeskane" intervaly od start_date
- catchup=False: spusti jen posledni/aktualni interval
- logical_date (drive execution_date): datum intervalu, NE cas spusteni
- data_interval_start / data_interval_end: zacatek a konec datoveho intervalu

POZOR: Tento DAG ma catchup=True a start_date 7 dni v minulosti.
Pri unpause se vytvori 7 historickych DAG runu!
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def show_dates(**kwargs):
    """Ukazka ruznych datumovych kontextovych promennych."""
    logger.info("=== Datove promenne ===")
    logger.info("ds (logical_date): %s", kwargs.get("ds"))
    logger.info("logical_date: %s", kwargs.get("logical_date"))
    logger.info("data_interval_start: %s", kwargs.get("data_interval_start"))
    logger.info("data_interval_end: %s", kwargs.get("data_interval_end"))
    logger.info("")
    logger.info("VYSVETLENI:")
    logger.info("  logical_date = zacatek intervalu (drive 'execution_date')")
    logger.info("  data_interval_start = totez co logical_date")
    logger.info("  data_interval_end = konec intervalu")
    logger.info("  Priklad: denni DAG pro 2024-01-15:")
    logger.info("    logical_date = 2024-01-15 00:00")
    logger.info("    data_interval_end = 2024-01-16 00:00")
    logger.info("    Spusten az 2024-01-16 (po konci intervalu)")


with DAG(
    "catchup_demo",
    default_args={
        "retries": 0,
    },
    description="Catchup - historicke DAG runy",
    schedule="@daily",
    start_date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=7),
    catchup=True,  # DULEZITE: vytvori historicke runy!
    max_active_runs=2,  # omezeni aby se nespustilo 7 runu najednou
    tags=["gud10", "catchup"],
) as dag:

    show = PythonOperator(
        task_id="show_dates",
        python_callable=show_dates,
    )

    process = BashOperator(
        task_id="process_interval",
        bash_command='echo "Zpracovavam data pro interval {{ ds }} az {{ macros.ds_add(ds, 1) }}"',
    )

    show >> process
