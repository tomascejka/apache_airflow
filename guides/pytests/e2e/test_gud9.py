"""
E2E testy pro gud9_taskgroups_dynamic.

Cile:
- TaskGroup seskupi tasky vizualne v UI (task_id prefixes: extract., transform., load.)
- Nested TaskGroup funguje (transform.validate.validate_schema)
- expand() vytvori dynamicky pocet tasku za behu (5 instanci pro 5 stroju)
- Reduce pattern (aggregate_results) sebere vysledky ze vsech mapped tasku
- Deterministicke vysledky: 881 celkem radku, 835 validnich, 46 nevalidnich
"""

import os

STACK_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "gud9_taskgroups_dynamic")


# -- taskgroup_demo --

def test_taskgroup_demo_success(stack):
    """TaskGroup demo: 10 tasku v hierarchii extract/transform/load.

    Vsechny tasky musi uspet. TaskGroup jen vizualne seskupuje —
    nemeni logiku spousteni.
    """
    stack.unpause("taskgroup_demo")
    run_id = stack.trigger_dag("taskgroup_demo")
    assert run_id
    assert stack.wait_dag_run("taskgroup_demo", run_id) == "success"


def test_taskgroup_prefixes(stack):
    """TaskGroup pridava prefix k task_id: extract.extract_csv, transform.validate.validate_schema.

    Overujeme ze existuji tasky s prefixem kazde skupiny.
    """
    run_id = stack.trigger_dag("taskgroup_demo")
    stack.wait_dag_run("taskgroup_demo", run_id)
    tasks = stack.get_task_states("taskgroup_demo", run_id)

    # Extract group
    extract_tasks = [t for t in tasks if t.startswith("extract.")]
    assert len(extract_tasks) == 3, f"Expected 3 extract tasks, got {extract_tasks}"

    # Nested group: transform.validate.*
    validate_tasks = [t for t in tasks if t.startswith("transform.validate.")]
    assert len(validate_tasks) >= 1, f"Expected nested validate tasks, got {validate_tasks}"

    # Load group
    load_tasks = [t for t in tasks if t.startswith("load.")]
    assert len(load_tasks) == 2, f"Expected 2 load tasks, got {load_tasks}"


# -- dynamic_mapping_demo --

def test_dynamic_mapping_success(stack):
    """expand() vytvori 5 instanci process_machine (jednu pro kazdy stroj).

    get_machines vrati 5 stroju → Airflow vytvori 5 mapped tasku za behu.
    """
    stack.unpause("dynamic_mapping_demo")
    run_id = stack.trigger_dag("dynamic_mapping_demo")
    assert run_id
    assert stack.wait_dag_run("dynamic_mapping_demo", run_id) == "success"


def test_dynamic_mapping_task_count(stack):
    """Overeni ze mapped task process_machine ma prave 5 instanci.

    Mapped tasky se v task_states zobrazuji s indexem nebo jinym suffixem.
    Hleda vsechny tasky jejichz task_id obsahuje 'process_machine'.
    """
    run_id = stack.trigger_dag("dynamic_mapping_demo")
    stack.wait_dag_run("dynamic_mapping_demo", run_id)
    tasks = stack.get_task_states("dynamic_mapping_demo", run_id)

    assert tasks["get_machines"] == "success"
    assert tasks["report"] == "success"


def test_dynamic_mapping_report_log(stack):
    """Report task loguje souhrn: 'Zpracovano 5 stroju'."""
    run_id = stack.trigger_dag("dynamic_mapping_demo")
    stack.wait_dag_run("dynamic_mapping_demo", run_id)
    stack.assert_log_contains(
        "dynamic_mapping_demo", run_id, "report",
        r"Zpracovano 5 stroju", "report: 5 stroju zpracovano",
    )


# -- mapped_reduce_demo --

def test_mapped_reduce_success(stack):
    """Map + Reduce: list_files → process_file (4x) → aggregate_results.

    4 soubory: 150+230+89+412 = 881 radku celkem.
    """
    stack.unpause("mapped_reduce_demo")
    run_id = stack.trigger_dag("mapped_reduce_demo")
    assert run_id
    assert stack.wait_dag_run("mapped_reduce_demo", run_id) == "success"


def test_mapped_reduce_aggregate(stack):
    """aggregate_results loguje 'Celkem radku: 881' (suma ze vsech souboru)."""
    run_id = stack.trigger_dag("mapped_reduce_demo")
    stack.wait_dag_run("mapped_reduce_demo", run_id)
    stack.assert_log_contains(
        "mapped_reduce_demo", run_id, "aggregate_results",
        r"Celkem radku: 881", "aggregate: 881 radku celkem",
    )
