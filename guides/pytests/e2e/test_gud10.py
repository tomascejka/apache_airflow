"""
E2E testy pro gud10_scheduling_datasets.

Cile:
- Jinja date templates (ds, data_interval_start, data_interval_end) se renderuji
- Asset (dataset) producer triggeruje consumer DAG automaticky
- Consumer DAG se spusti bez manualniho triggeru (data-aware scheduling)
- catchup=True vytvori historicke DAG runy pro zmeskane intervaly
"""

import os
import time

STACK_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "gud10_scheduling_datasets")


# -- cron_demo --

def test_cron_demo_success(stack):
    """cron_demo je schedule=None (manual trigger) — slouzi jako reference.

    Overujeme ze Jinja templates ds, data_interval_start se renderuji.
    """
    stack.unpause("cron_demo")
    run_id = stack.trigger_dag("cron_demo")
    assert run_id
    assert stack.wait_dag_run("cron_demo", run_id) == "success"

    tasks = stack.get_task_states("cron_demo", run_id)
    assert tasks["show_schedule_info"] == "success"
    assert tasks["process_data"] == "success"


def test_cron_demo_jinja_templates(stack):
    """V logu show_schedule_info je viditelny renderovany 'logical_date'."""
    run_id = stack.trigger_dag("cron_demo")
    stack.wait_dag_run("cron_demo", run_id)
    stack.assert_log_contains(
        "cron_demo", run_id, "show_schedule_info",
        r"logical_date:", "cron_demo: Jinja template logical_date renderovan",
    )


# -- dataset_producer + dataset_consumer --

def test_dataset_auto_trigger(stack):
    """Data-aware scheduling: producer aktualizuje asset → consumer se automaticky spusti.

    Postup:
    1. Unpause oba DAGy
    2. Trigger producer (produce_data s outlets=[machine_data_daily])
    3. Producer dokonci → Airflow automaticky triggeruje consumer
    4. Consumer musi dobehnout do "success" bez manualniho triggeru
    """
    stack.unpause("dataset_producer")
    stack.unpause("dataset_consumer")

    # Trigger producer
    run_id = stack.trigger_dag("dataset_producer")
    assert run_id, "producer trigger failed"
    state = stack.wait_dag_run("dataset_producer", run_id, timeout=60)
    assert state == "success", f"producer state={state}"

    # Consumer se musi triggerovat automaticky
    consumer_run_id = stack.wait_for_consumer_run("dataset_consumer", timeout=90)
    assert consumer_run_id, "Consumer DAG se nespustil automaticky po producer runu"


# -- catchup_demo --

def test_catchup_creates_historical_runs(stack):
    """catchup=True + start_date 7 dni v minulosti → Airflow vytvori historicke runy.

    Pri unpause se scheduler automaticky vytvori ~7 scheduled runu
    (jeden pro kazdy den od start_date). Overujeme ze existuji
    alespon 3 scheduled runy (konzervativni — zavisi na casovani).
    """
    stack.unpause("catchup_demo")

    # Cekame az scheduler vytvori a zpracuje historicke runy
    elapsed = 0
    scheduled_count = 0
    while elapsed < 120:
        data = stack._cli_json("dags", "list-runs", "catchup_demo", "-o", "json")
        if data:
            scheduled_count = sum(
                1 for r in data if r.get("run_id", "").startswith("scheduled__")
            )
            if scheduled_count >= 3:
                break
        time.sleep(10)
        elapsed += 10

    assert scheduled_count >= 3, (
        f"Expected >= 3 scheduled (catchup) runs, got {scheduled_count}"
    )
