"""
DAG: priority_demo
Demonstrace priority_weight — poradi spousteni tasku.

Koncepty:
- priority_weight: vyssi cislo = vyssi priorita
- weight_rule: jak se pocita efektivni priorita
  - "downstream" (default): suma priorit vsech downstream tasku
  - "upstream": suma priorit vsech upstream tasku
  - "absolute": jen vlastni priority_weight
- Viditelne v UI: tasky s vyssi prioritou se spusti driv
"""

import time
from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def process_with_priority(task_name: str, priority: int, **kwargs):
    """Task ktery loguje svou prioritu."""
    logger.info("Task '%s' s prioritou %d — SPUSTEN", task_name, priority)
    time.sleep(3)
    logger.info("Task '%s' HOTOV", task_name)


with DAG(
    "priority_demo",
    default_args={
        "retries": 0,
    },
    description="Priority weight - poradi spousteni",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud11", "priority"],
) as dag:

    # Vsechny tasky ve stejnem poolu (aby soutezily o sloty)
    # Vyssi priority_weight = spusti se driv

    t_critical = PythonOperator(
        task_id="critical_task",
        python_callable=process_with_priority,
        op_kwargs={"task_name": "CRITICAL", "priority": 10},
        priority_weight=10,
        pool="machine_pool",
    )

    t_high = PythonOperator(
        task_id="high_priority",
        python_callable=process_with_priority,
        op_kwargs={"task_name": "HIGH", "priority": 5},
        priority_weight=5,
        pool="machine_pool",
    )

    t_normal = PythonOperator(
        task_id="normal_priority",
        python_callable=process_with_priority,
        op_kwargs={"task_name": "NORMAL", "priority": 1},
        priority_weight=1,
        pool="machine_pool",
    )

    t_low = PythonOperator(
        task_id="low_priority",
        python_callable=process_with_priority,
        op_kwargs={"task_name": "LOW", "priority": 0},
        priority_weight=0,
        pool="machine_pool",
    )

    # Vsechny nezavisle — scheduler ridi poradi dle priority
