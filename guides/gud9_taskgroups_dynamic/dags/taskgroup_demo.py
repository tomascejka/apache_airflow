"""
DAG: taskgroup_demo
Demonstrace TaskGroup — vizualni seskupeni tasku.

Koncepty:
- TaskGroup pro organizaci v UI Graph view
- Vnorene TaskGroups (nested)
- prefix_group_id pro nazvy tasku
- Dependency mezi skupinami
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG, task
from airflow.sdk.definitions.taskgroup import TaskGroup
from airflow.providers.standard.operators.bash import BashOperator

logger = logging.getLogger(__name__)


with DAG(
    "taskgroup_demo",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="TaskGroup - vizualni seskupeni tasku",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud9", "taskgroup"],
) as dag:

    start = BashOperator(task_id="start", bash_command='echo "Start pipeline"')

    # TaskGroup: Extract
    with TaskGroup("extract", tooltip="Extrakce dat z ruznych zdroju") as extract_group:
        extract_csv = BashOperator(
            task_id="extract_csv",
            bash_command='echo "Extracting CSV data..." && sleep 2',
        )
        extract_json = BashOperator(
            task_id="extract_json",
            bash_command='echo "Extracting JSON data..." && sleep 1',
        )
        extract_api = BashOperator(
            task_id="extract_api",
            bash_command='echo "Extracting API data..." && sleep 1',
        )

    # TaskGroup: Transform (s vnorenou skupinou)
    with TaskGroup("transform", tooltip="Transformace dat") as transform_group:
        with TaskGroup("validate", tooltip="Validace dat") as validate_group:
            validate_schema = BashOperator(
                task_id="validate_schema",
                bash_command='echo "Validating schema..."',
            )
            validate_values = BashOperator(
                task_id="validate_values",
                bash_command='echo "Validating values..."',
            )

        clean = BashOperator(
            task_id="clean_data",
            bash_command='echo "Cleaning data..."',
        )
        enrich = BashOperator(
            task_id="enrich_data",
            bash_command='echo "Enriching data..."',
        )

        validate_group >> clean >> enrich

    # TaskGroup: Load
    with TaskGroup("load", tooltip="Ulozeni dat") as load_group:
        load_db = BashOperator(
            task_id="load_to_db",
            bash_command='echo "Loading to database..."',
        )
        load_file = BashOperator(
            task_id="load_to_file",
            bash_command='echo "Loading to file..."',
        )

    end = BashOperator(task_id="end", bash_command='echo "Pipeline dokoncena"')

    # Dependencies mezi skupinami
    start >> extract_group >> transform_group >> load_group >> end
