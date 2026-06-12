"""
DAG: dynamic_mapping_demo
Demonstrace dynamic task mapping — .expand() pro dynamicky pocet tasku.

Koncepty:
- .expand(): vytvori N instanci tasku dynamicky za behu
- Pocet tasku neni znam pri parsovani DAGu
- Kazda instance zpracuje jeden prvek ze seznamu
- V UI videt "mapped task" s indexy [0], [1], [2], ...
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG, task

logger = logging.getLogger(__name__)


@task
def get_machines():
    """Dynamicky vrati seznam stroju k zpracovani."""
    machines = ["CNC-001", "CNC-002", "CNC-003", "LASER-001", "PRESS-001"]
    logger.info("Nalezeno %d stroju: %s", len(machines), machines)
    return machines


@task
def process_machine(machine_id: str):
    """Zpracuje data jednoho stroje. Jedna instance na stroj."""
    import time
    import random

    teplota = round(random.uniform(60, 90), 1)
    logger.info("Zpracovavam stroj %s: teplota=%.1f°C", machine_id, teplota)
    time.sleep(1)  # simulace zpracovani
    return {"machine": machine_id, "teplota": teplota}


@task
def report(results):
    """Souhrn vysledku."""
    logger.info("=== REPORT ===")
    logger.info("Zpracovano %d stroju", len(results))
    for r in results:
        logger.info("  %s: %.1f°C", r["machine"], r["teplota"])


with DAG(
    "dynamic_mapping_demo",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Dynamic task mapping - .expand()",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud09", "dynamic"],
) as dag:

    machines = get_machines()

    # .expand() vytvori 5 instanci process_machine (jednu pro kazdy stroj)
    processed = process_machine.expand(machine_id=machines)

    report(results=processed)
