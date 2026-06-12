"""
DAG: params_demo
Demonstrace DAG Params — konfigurace pri triggeru.

Koncepty:
- Params s default hodnotami
- Pristup pres {{ params.nazev }} v Jinja
- Pristup pres kwargs["params"] v Python
- Validace (type, enum, min/max)
- Trigger s custom hodnotami (UI nebo CLI --conf)
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

logger = logging.getLogger(__name__)


def process_with_params(**kwargs):
    """Zpracovani s parametry z triggeru."""
    params = kwargs["params"]

    logger.info("=== Parametry DAG runu ===")
    logger.info("stroj_id: %s", params["stroj_id"])
    logger.info("format: %s", params["format"])
    logger.info("batch_size: %d", params["batch_size"])
    logger.info("debug: %s", params["debug"])

    if params["debug"]:
        logger.info("DEBUG MOD AKTIVNI — detailni logovani zapnuto")

    logger.info("Zpracovavam data pro stroj %s ve formatu %s (batch=%d)",
                params["stroj_id"], params["format"], params["batch_size"])


with DAG(
    "params_demo",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="DAG Params - konfigurace pri triggeru",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud05", "params"],
    params={
        "stroj_id": Param(
            default="CNC-001",
            type="string",
            description="ID stroje k zpracovani",
        ),
        "format": Param(
            default="csv",
            type="string",
            enum=["csv", "json", "parquet"],
            description="Vystupni format dat",
        ),
        "batch_size": Param(
            default=100,
            type="integer",
            minimum=1,
            maximum=10000,
            description="Pocet zaznamu v davce",
        ),
        "debug": Param(
            default=False,
            type="boolean",
            description="Zapnout debug logovani",
        ),
    },
) as dag:

    t_process = PythonOperator(
        task_id="process_with_params",
        python_callable=process_with_params,
    )

    # Jinja pristup k params
    t_bash = BashOperator(
        task_id="bash_with_params",
        bash_command=(
            'echo "Stroj: {{ params.stroj_id }}" && '
            'echo "Format: {{ params.format }}" && '
            'echo "Batch: {{ params.batch_size }}"'
        ),
    )

    t_process >> t_bash
