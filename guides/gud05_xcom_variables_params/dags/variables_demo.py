"""
DAG: variables_demo
Demonstrace Airflow Variables — konfigurace za behu.

Koncepty:
- Variable.get() v Python kodu
- Jinja template {{ var.value.nazev }} a {{ var.json.nazev }}
- Env vars: AIRFLOW_VAR_* (nastaveno v docker-compose)
- Priorita: env var > DB (UI/CLI)
- Defaultni hodnoty
"""

from datetime import datetime, timedelta
import logging

from airflow.models import Variable
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

logger = logging.getLogger(__name__)


def read_variables(**kwargs):
    """Cteni Variables z ruznych zdroju."""
    # Z env var AIRFLOW_VAR_ENVIRONMENT (nastaveno v docker-compose)
    env = Variable.get("environment", default_var="unknown")
    logger.info("environment = %s (z env var AIRFLOW_VAR_ENVIRONMENT)", env)

    max_retries = Variable.get("max_retries", default_var="5")
    logger.info("max_retries = %s", max_retries)

    email = Variable.get("notification_email", default_var="none@example.com")
    logger.info("notification_email = %s", email)

    # Variable ktera neexistuje — s default hodnotou
    missing = Variable.get("neexistujici_variable", default_var="DEFAULT_HODNOTA")
    logger.info("neexistujici_variable = %s (default)", missing)


def set_and_read_variable(**kwargs):
    """Nastaveni a cteni Variable programaticky."""
    # Set
    Variable.set("my_config", '{"batch_size": 100, "format": "csv"}')
    logger.info("Variable 'my_config' nastavena")

    # Read jako JSON
    config = Variable.get("my_config", deserialize_json=True)
    logger.info("my_config = %s", config)
    logger.info("batch_size = %d", config["batch_size"])
    logger.info("format = %s", config["format"])


with DAG(
    "variables_demo",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Airflow Variables - konfigurace za behu",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud05", "variables"],
) as dag:

    t_read = PythonOperator(
        task_id="read_variables",
        python_callable=read_variables,
    )

    t_set = PythonOperator(
        task_id="set_and_read",
        python_callable=set_and_read_variable,
    )

    # Jinja pristup k Variables
    t_jinja = BashOperator(
        task_id="jinja_variables",
        bash_command=(
            'echo "Environment: {{ var.value.environment }}" && '
            'echo "Max retries: {{ var.value.max_retries }}"'
        ),
    )

    t_read >> t_set >> t_jinja
