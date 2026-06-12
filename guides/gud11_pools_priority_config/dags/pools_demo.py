"""
DAG: pools_demo
Demonstrace Pools — omezeni soubehu tasku.

Koncepty:
- Pool = sdileny limit na pocet soucasne bezicich tasku
- Nastaveni: pool="nazev_poolu" na tasku
- Pool "machine_pool" ma 2 sloty (vytvoreny v airflow-init)
- 6 tasku v poolu → max 2 bezi soucasne, ostatni cekaji

Bez poolu by vsech 6 tasku bezelo paralelne.
S poolem (2 sloty) bezi max 2 najednou.
"""

import time
from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def process_machine(machine_id: str, **kwargs):
    """Simulace zpracovani stroje — trva 10 sekund."""
    logger.info("START zpracovani stroje %s", machine_id)
    logger.info("Pool slot obsazen — max 2 stroje soucasne")
    time.sleep(10)
    logger.info("KONEC zpracovani stroje %s", machine_id)
    return f"{machine_id} OK"


with DAG(
    "pools_demo",
    default_args={
        "retries": 0,
    },
    description="Pools - omezeni soubehu (2 sloty, 6 tasku)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud11", "pools"],
) as dag:

    machines = ["CNC-001", "CNC-002", "CNC-003", "LASER-001", "PRESS-001", "PRESS-002"]

    tasks = []
    for machine in machines:
        t = PythonOperator(
            task_id=f"process_{machine.lower().replace('-', '_')}",
            python_callable=process_machine,
            op_kwargs={"machine_id": machine},
            pool="machine_pool",  # vsechny tasky sdili pool s 2 sloty
        )
        tasks.append(t)

    # Vsechny tasky jsou nezavisle (paralelni), ale pool omezuje na 2 soucasne
