"""
DAG: cron_demo
Demonstrace ruznych cron vyrazu a timedelta schedulingu.

Koncepty:
- schedule s cron vyrazem ("0 * * * *", "0 6 * * 1-5", ...)
- schedule s timedelta (timedelta(hours=1))
- start_date: od kdy se pocitaji intervaly
- Tento DAG pouziva schedule=None pro manual trigger, ale ukazuje priklady v komentarich

POZNAMKA: DAG je schedule=None aby negeneroval automaticke runy.
Slouzi jako reference pro cron vyrazy.
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

logger = logging.getLogger(__name__)


# === Priklady cron vyrazu (pro referenci) ===
# "0 * * * *"       - kazdou hodinu (minuta 0)
# "0 6 * * *"       - denne v 6:00
# "0 6 * * 1-5"     - pondeli-patek v 6:00
# "*/15 * * * *"    - kazdych 15 minut
# "0 0 1 * *"       - prvniho v mesici o pulnoci
# "0 8,12,18 * * *" - v 8:00, 12:00 a 18:00
# "@daily"          - preset: "0 0 * * *"
# "@hourly"         - preset: "0 * * * *"
# "@weekly"         - preset: "0 0 * * 0"
# timedelta(hours=2) - kazdé 2 hodiny od start_date

with DAG(
    "cron_demo",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Cron vyrazy a scheduling - reference",
    schedule=None,  # manual trigger pro demonstraci
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud10", "scheduling"],
) as dag:

    show_schedule = BashOperator(
        task_id="show_schedule_info",
        bash_command="""
echo "=== Scheduling priklady ==="
echo ""
echo "Cron format: minuta hodina den_mesice mesic den_tydne"
echo ""
echo "Priklady:"
echo "  '0 * * * *'        - kazdou hodinu"
echo "  '0 6 * * *'        - denne v 6:00"
echo "  '0 6 * * 1-5'      - Po-Pa v 6:00"
echo "  '*/15 * * * *'     - kazdych 15 minut"
echo "  '0 0 1 * *'        - prvniho v mesici"
echo "  '@daily'           - preset: '0 0 * * *'"
echo "  '@hourly'          - preset: '0 * * * *'"
echo "  'timedelta(hours=2)' - kazdych 2h od start_date"
echo ""
echo "logical_date: {{ ds | default('N/A (manual trigger)') }}"
echo "data_interval_start: {{ data_interval_start | default('N/A') }}"
echo "data_interval_end: {{ data_interval_end | default('N/A') }}"
""",
    )

    process = BashOperator(
        task_id="process_data",
        bash_command='echo "Zpracovavam data pro interval {{ ds | default(\"manual\") }}"',
    )

    show_schedule >> process
