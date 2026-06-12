"""
DAG: time_sensor_demo
Demonstrace TimeSensor a TimeDeltaSensor.

Koncepty:
- TimeSensor: ceka do konkretniho casu (napr. 14:00)
- TimeDeltaSensor: ceka urcity casovy usek od spusteni
- Uzitecne pro: maintenance okna, koordinace s externymi systemy
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.providers.standard.operators.bash import BashOperator

logger = logging.getLogger(__name__)


with DAG(
    "time_sensor_demo",
    default_args={
        "retries": 0,
    },
    description="TimeDeltaSensor - cekani na casovy usek",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud07", "sensor", "time"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command='echo "Start: $(date +%H:%M:%S)"',
    )

    # TimeDeltaSensor — ceka 30 sekund od logical_date
    wait_30s = TimeDeltaSensor(
        task_id="wait_30_seconds",
        delta=timedelta(seconds=30),
        poke_interval=5,
        mode="poke",
    )

    after_wait = BashOperator(
        task_id="after_wait",
        bash_command='echo "Po 30s cekani: $(date +%H:%M:%S)"',
    )

    start >> wait_30s >> after_wait
