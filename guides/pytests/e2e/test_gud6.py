"""
E2E testy pro gud6_branching_trigger_rules.

Cile:
- BranchPythonOperator vybere jednu vetev, druha je skipped
- Trigger rules urcuji kdy se task spusti
- ShortCircuitOperator preskoci downstream pri False
"""

import os

STACK_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "gud6_branching_trigger_rules")


# -- branching_demo (non-deterministic) --

def test_branching_demo_success(stack):
    """BranchPythonOperator: DAG uspesne dokonci bez ohledu na zvolenou vetev.

    choose_branch nahodne vraci 'branch_a' nebo 'branch_b'.
    join a end maji trigger_rule=none_failed_min_one_success, takze projdou vzdy.
    """
    stack.unpause("branching_demo")
    run_id = stack.trigger_dag("branching_demo")
    assert run_id
    assert stack.wait_dag_run("branching_demo", run_id) == "success"


def test_branching_demo_invariant(stack):
    """Invariant: prave jedna vetev je success, druha je skipped.

    BranchPythonOperator vraci task_id jedne vetvy. Ta se spusti (success),
    druha se automaticky preskoci (skipped). Nemuze nastat situace
    kde jsou obe success nebo obe skipped.
    """
    run_id = stack.trigger_dag("branching_demo")
    stack.wait_dag_run("branching_demo", run_id)
    tasks = stack.get_task_states("branching_demo", run_id)

    assert tasks["start"] == "success"
    assert tasks["choose_branch"] == "success"

    states = {tasks["branch_a"], tasks["branch_b"]}
    assert states == {"success", "skipped"}, (
        f"Expected one success + one skipped, got: a={tasks['branch_a']}, b={tasks['branch_b']}"
    )

    assert tasks["join"] == "success"
    assert tasks["end"] == "success"


# -- trigger_rules_demo (deterministic) --

def test_trigger_rules_demo(stack):
    """trigger_rules_demo: DAG state je 'success' protoze leaf task cleanup_always uspeje.

    I kdyz failing_task zamerne failuje, cleanup_always (trigger_rule=all_done)
    bezi vzdy a jako jediny leaf task urcuje DAG run stav.
    """
    stack.unpause("trigger_rules_demo")
    run_id = stack.trigger_dag("trigger_rules_demo")
    assert run_id
    state = stack.wait_dag_run("trigger_rules_demo", run_id)
    assert state == "success", f"DAG run state={state}"


def test_trigger_rules_task_states(stack):
    """Deterministicke stavy tasku dle trigger_rule:

    - success_1, success_2: success (vzdy uspeji)
    - failing_task: failed (zamerne vyhazuje exception, retries=0)
    - needs_all_success (all_success): upstream_failed (failing_task failoval)
    - needs_one_success (one_success): success (alespon success_1 uspel)
    - runs_when_all_done (all_done): success (vsechny upstream dokonceny)
    - needs_none_failed (none_failed_min_one_success): upstream_failed (failing_task)
    - cleanup_always (all_done): success (bezi vzdy po vsech upstream)
    """
    run_id = stack.trigger_dag("trigger_rules_demo")
    stack.wait_dag_run("trigger_rules_demo", run_id)
    tasks = stack.get_task_states("trigger_rules_demo", run_id)

    assert tasks["success_1"] == "success"
    assert tasks["success_2"] == "success"
    assert tasks["failing_task"] == "failed"
    assert tasks["needs_all_success"] == "upstream_failed"
    assert tasks["needs_one_success"] == "success"
    assert tasks["runs_when_all_done"] == "success"
    assert tasks["needs_none_failed"] == "upstream_failed"
    assert tasks["cleanup_always"] == "success"


# -- short_circuit_demo (non-deterministic) --

def test_short_circuit_demo_success(stack):
    """ShortCircuitOperator: DAG vzdy uspeje bez ohledu na vysledek check_should_run.

    Pokud check_should_run vrati False, step_1 a step_2 jsou skipped (ne failed).
    check_weekday je nezavisly chain — kontroluje pracovni den.
    """
    stack.unpause("short_circuit_demo")
    run_id = stack.trigger_dag("short_circuit_demo")
    assert run_id
    assert stack.wait_dag_run("short_circuit_demo", run_id) == "success"


def test_short_circuit_invariant(stack):
    """Invariant: step_1 a step_2 maji vzdy stejny stav.

    Oba jsou downstream od check_should_run v sekvenci (check → step_1 → step_2).
    Pokud check vrati True: oba success. Pokud False: oba skipped.
    Nemohou mit ruzny stav.
    """
    run_id = stack.trigger_dag("short_circuit_demo")
    stack.wait_dag_run("short_circuit_demo", run_id)
    tasks = stack.get_task_states("short_circuit_demo", run_id)

    assert tasks["check_should_run"] == "success"
    assert tasks["check_weekday"] == "success"

    assert tasks["step_1"] == tasks["step_2"], (
        f"step_1={tasks['step_1']} vs step_2={tasks['step_2']} (should be same)"
    )
