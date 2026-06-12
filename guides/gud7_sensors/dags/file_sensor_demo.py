"""
DAG: file_sensor_demo
Demonstrace FileSensor — cekani na soubor.

Koncepty:
- FileSensor ceka na existenci souboru
- poke_interval: jak casto kontrolovat (sekundy)
- timeout: po jake dobe vzdát (sekundy)
- mode: "poke" (drzi worker slot) vs "reschedule" (uvolni slot mezi pokusy)
- soft_fail: True = skip misto fail pri timeoutu

Pouziti:
1. Trigger DAG
2. Sensor ceka na soubor /opt/airflow/data/trigger_file.csv
3. Rucne vytvorit soubor: echo "test" > data/trigger_file.csv (z hostitele)
4. Sensor detekuje → downstream bezi
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def process_file(**kwargs):
    """Zpracovani souboru po detekci sensorem."""
    filepath = "/opt/airflow/data/trigger_file.csv"
    logger.info("Soubor detekovan! Zpracovavam: %s", filepath)
    with open(filepath) as f:
        content = f.read()
    logger.info("Obsah souboru:\n%s", content)
    logger.info("Pocet radku: %d", len(content.strip().split("\n")))


with DAG(
    "file_sensor_demo",
    default_args={
        "retries": 0,
    },
    description="FileSensor - cekani na soubor",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud7", "sensor"],
) as dag:

    # Cleanup — smazat soubor z predchoziho runu
    cleanup = BashOperator(
        task_id="cleanup_old_file",
        bash_command="rm -f /opt/airflow/data/trigger_file.csv && echo 'Stary soubor smazan (pokud existoval)'",
    )

    # FileSensor — ceka na soubor
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/data/trigger_file.csv",
        poke_interval=5,  # kontrola kazdych 5 sekund
        timeout=120,  # max 2 minuty cekani
        mode="poke",  # drzi worker slot (pro kratke cekani OK)
        soft_fail=True,  # pri timeoutu skip misto fail
    )

    process = PythonOperator(
        task_id="process_file",
        python_callable=process_file,
    )

    done = BashOperator(
        task_id="done",
        bash_command='echo "Pipeline dokoncena — soubor zpracovan"',
    )

    cleanup >> wait_for_file >> process >> done
