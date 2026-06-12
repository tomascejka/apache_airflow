"""
E2E testy pro gud04_python_operator.

Cile:
- PythonOperator spusti python_callable s op_args a op_kwargs
- @task dekorator funguje jako nahrada PythonOperator
- Return value z @task se automaticky ulozi do XCom
- Kontextove promenne (ti, dag_run, ds) jsou dostupne
- logging modul funguje v Airflow logu
"""

import os

STACK_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "gud04_python_operator")


# -- python_basics --

def test_python_basics_success(stack):
    """PythonOperator DAG dokonci vsechny 3 tasky v poradi greet → context → compute."""
    stack.unpause("python_basics")
    run_id = stack.trigger_dag("python_basics")
    assert run_id, "trigger failed"
    state = stack.wait_dag_run("python_basics", run_id)
    assert state == "success"


def test_python_basics_tasks(stack):
    """Vsechny tri tasky (greet_user, show_context, compute_result) musi byt success."""
    run_id = stack.trigger_dag("python_basics")
    stack.wait_dag_run("python_basics", run_id)
    tasks = stack.get_task_states("python_basics", run_id)
    assert tasks["greet_user"] == "success"
    assert tasks["show_context"] == "success"
    assert tasks["compute_result"] == "success"


def test_python_basics_logs(stack):
    """Overeni ze op_args/op_kwargs se predaly spravne a logging funguje.

    - compute_result: multiply(10, 5) = 50 (op_args=[10,5], op_kwargs=operation=multiply)
    - greet_user: 'Nazdar, Airflow' (op_args=['Airflow'], op_kwargs=greeting='Nazdar')
    - show_context: 'task_id: show_context' (pristup ke kontextu **kwargs)
    """
    run_id = stack.trigger_dag("python_basics")
    stack.wait_dag_run("python_basics", run_id)
    stack.assert_log_contains(
        "python_basics", run_id, "compute_result",
        r"multiply\(10, 5\) = 50", "compute_result: multiply(10,5)=50",
    )
    stack.assert_log_contains(
        "python_basics", run_id, "greet_user",
        r"Nazdar, Airflow", "greet_user: op_args/op_kwargs",
    )
    stack.assert_log_contains(
        "python_basics", run_id, "show_context",
        r"task_id: show_context", "show_context: kontext dostupny",
    )


# -- taskflow_basics --

def test_taskflow_basics_success(stack):
    """TaskFlow API: @task dekorator vytvori ETL pipeline extract → transform → load."""
    stack.unpause("taskflow_basics")
    run_id = stack.trigger_dag("taskflow_basics")
    assert run_id, "trigger failed"
    state = stack.wait_dag_run("taskflow_basics", run_id)
    assert state == "success"


def test_taskflow_basics_tasks(stack):
    """Vsechny tri @task funkce (extract, transform, load) musi byt success."""
    run_id = stack.trigger_dag("taskflow_basics")
    stack.wait_dag_run("taskflow_basics", run_id)
    tasks = stack.get_task_states("taskflow_basics", run_id)
    assert tasks["extract"] == "success"
    assert tasks["transform"] == "success"
    assert tasks["load"] == "success"


def test_taskflow_basics_logs(stack):
    """Overeni ze data protecou celou ETL pipeline pres automaticky XCom.

    - load: '=== LOAD ===' + 'Vibrace: OK' (data z extract → transform → load)
    - transform: '162.5' (72.5°C * 9/5 + 32 = 162.5°F)
    """
    run_id = stack.trigger_dag("taskflow_basics")
    stack.wait_dag_run("taskflow_basics", run_id)
    stack.assert_log_contains(
        "taskflow_basics", run_id, "load",
        r"=== LOAD ===", "load: data dosla do load",
    )
    stack.assert_log_contains(
        "taskflow_basics", run_id, "load",
        r"Vibrace: OK", "load: vibrace_status",
    )
    stack.assert_log_contains(
        "taskflow_basics", run_id, "transform",
        r"162\.5", "transform: teplota_f=162.5",
    )
