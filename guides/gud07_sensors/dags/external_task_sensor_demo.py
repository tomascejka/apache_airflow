"""
DAG: external_task_sensor_demo
Demonstrace ExternalTaskSensor — cekani na jiny DAG.

Obsahuje 2 DAGy:
- producer_dag: jednoduchý DAG ktery se spusti prvni
- consumer_dag: ceka na dokonceni producer_dag pomoci ExternalTaskSensor

Koncepty:
- ExternalTaskSensor ceka na dokonceni tasku/DAGu v jinem DAGu
- execution_delta nebo execution_date_fn pro mapovani casu
- Alternativa: TriggerDagRunOperator (aktivni trigger misto pasivniho cekani)
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

logger = logging.getLogger(__name__)


# === PRODUCER DAG ===
with DAG(
    "sensor_producer",
    default_args={"retries": 0},
    description="Producer DAG — spustit jako prvni",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud07", "sensor", "producer"],
) as producer_dag:

    produce = BashOperator(
        task_id="produce_data",
        bash_command='echo "Producer: data vygenerovana v $(date +%H:%M:%S)" && sleep 5',
    )

    done = BashOperator(
        task_id="producer_done",
        bash_command='echo "Producer HOTOV"',
    )

    produce >> done


# === CONSUMER DAG ===
with DAG(
    "sensor_consumer",
    default_args={"retries": 0},
    description="Consumer DAG — ceka na sensor_producer",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud07", "sensor", "consumer"],
) as consumer_dag:

    wait_for_producer = ExternalTaskSensor(
        task_id="wait_for_producer",
        external_dag_id="sensor_producer",
        external_task_id="producer_done",
        mode="poke",
        poke_interval=10,
        timeout=120,
        soft_fail=True,
        allowed_states=["success"],
    )

    consume = BashOperator(
        task_id="consume_data",
        bash_command='echo "Consumer: zpracovavam data od producera"',
    )

    wait_for_producer >> consume
