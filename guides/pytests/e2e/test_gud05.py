"""
E2E testy pro gud05_xcom_variables_params.

Cile:
- XCom push/pull funguje (explicitni i Jinja)
- Variables cteni z env var AIRFLOW_VAR_* funguje
- Variable.set() programaticky zapise hodnotu
- Params s validaci se predaji do tasku
- Trigger s custom --conf zmeni chovani DAGu
"""

import os

STACK_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "gud05_xcom_variables_params")


# -- xcom_demo --

def test_xcom_demo_success(stack):
    """XCom demo DAG: push_values → pull_values → jinja_xcom → show_xcom_size."""
    stack.unpause("xcom_demo")
    run_id = stack.trigger_dag("xcom_demo")
    assert run_id
    assert stack.wait_dag_run("xcom_demo", run_id) == "success"


def test_xcom_demo_tasks(stack):
    """Vsechny 4 tasky xcom pipeline musi uspet."""
    run_id = stack.trigger_dag("xcom_demo")
    stack.wait_dag_run("xcom_demo", run_id)
    tasks = stack.get_task_states("xcom_demo", run_id)
    assert tasks["push_values"] == "success"
    assert tasks["pull_values"] == "success"
    assert tasks["jinja_xcom"] == "success"
    assert tasks["show_xcom_size"] == "success"


def test_xcom_demo_logs(stack):
    """Overeni ze xcom_pull vraci spravne hodnoty z xcom_push.

    push_values uklada stroj_id='CNC-001' a teplota=72.5.
    pull_values je vytahne pres ti.xcom_pull() a loguje.
    """
    run_id = stack.trigger_dag("xcom_demo")
    stack.wait_dag_run("xcom_demo", run_id)
    stack.assert_log_contains("xcom_demo", run_id, "pull_values", r"stroj_id: CNC-001")
    stack.assert_log_contains("xcom_demo", run_id, "pull_values", r"teplota.*72\.5")


# -- variables_demo --

def test_variables_demo_success(stack):
    """Variables demo: cteni z AIRFLOW_VAR_*, Variable.set(), Jinja {{ var.value.X }}."""
    stack.unpause("variables_demo")
    run_id = stack.trigger_dag("variables_demo")
    assert run_id
    assert stack.wait_dag_run("variables_demo", run_id) == "success"


def test_variables_demo_tasks(stack):
    """Vsechny 3 tasky variables pipeline musi uspet."""
    run_id = stack.trigger_dag("variables_demo")
    stack.wait_dag_run("variables_demo", run_id)
    tasks = stack.get_task_states("variables_demo", run_id)
    assert tasks["read_variables"] == "success"
    assert tasks["set_and_read"] == "success"
    assert tasks["jinja_variables"] == "success"


def test_variables_env_var(stack):
    """AIRFLOW_VAR_ENVIRONMENT=development se precte jako Variable.

    Nastaveno v docker-compose.yaml jako env var, Airflow ho automaticky
    zpristupni jako Variable.get('environment').
    """
    run_id = stack.trigger_dag("variables_demo")
    stack.wait_dag_run("variables_demo", run_id)
    stack.assert_log_contains(
        "variables_demo", run_id, "read_variables",
        r"environment = development",
    )


# -- params_demo (default) --

def test_params_default_success(stack):
    """Params demo s default hodnotami (bez --conf): stroj_id=CNC-001, format=csv."""
    stack.unpause("params_demo")
    run_id = stack.trigger_dag("params_demo")
    assert run_id
    assert stack.wait_dag_run("params_demo", run_id) == "success"


def test_params_default_tasks(stack):
    """Oba tasky params_demo (process_with_params, bash_with_params) musi uspet."""
    run_id = stack.trigger_dag("params_demo")
    stack.wait_dag_run("params_demo", run_id)
    tasks = stack.get_task_states("params_demo", run_id)
    assert tasks["process_with_params"] == "success"
    assert tasks["bash_with_params"] == "success"


def test_params_default_values(stack):
    """Overeni default hodnot parametru v logu.

    DAG definuje Param('stroj_id', default='CNC-001') a Param('format', default='csv').
    Bez --conf se pouziji tyto default hodnoty.
    """
    run_id = stack.trigger_dag("params_demo")
    stack.wait_dag_run("params_demo", run_id)
    stack.assert_log_contains("params_demo", run_id, "process_with_params", r"stroj_id: CNC-001")
    stack.assert_log_contains("params_demo", run_id, "process_with_params", r"format: csv")


# -- params_demo (custom conf) --

def test_params_custom_conf(stack):
    """Trigger s --conf meni chovani: stroj_id=CNC-999, format=json.

    subprocess.run predava JSON conf jako samostatny argument — zadne
    shell quoting problemy (na rozdil od PowerShell).
    """
    conf = '{"stroj_id":"CNC-999","format":"json","batch_size":500,"debug":true}'
    run_id = stack.trigger_dag("params_demo", conf=conf)
    assert run_id
    assert stack.wait_dag_run("params_demo", run_id) == "success"

    tasks = stack.get_task_states("params_demo", run_id)
    assert tasks["process_with_params"] == "success"
    assert tasks["bash_with_params"] == "success"

    stack.assert_log_contains("params_demo", run_id, "process_with_params", r"stroj_id: CNC-999")
    stack.assert_log_contains("params_demo", run_id, "process_with_params", r"format: json")
