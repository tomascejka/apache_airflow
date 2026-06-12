"""
E2E testy pro gud8_error_handling.

Cile:
- Task s retries se opakuje pri selhani
- on_success_callback se zavola po uspechu
- on_failure_callback se zavola po selhani (po vycerpani vsech retries)
- execution_timeout prerusi task po zadane dobe
"""

import os

STACK_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "gud8_error_handling")


# -- callbacks_demo --

def test_callbacks_demo_states(stack):
    """Overeni stavu tasku v callbacks_demo.

    - always_succeeds: musi uspet (retries=0)
    - always_fails: musi failnout (zamerne vyhazuje exception)
    - DAG run = failed (protoze always_fails selze)
    """
    stack.unpause("callbacks_demo")
    run_id = stack.trigger_dag("callbacks_demo")
    assert run_id
    state = stack.wait_dag_run("callbacks_demo", run_id, timeout=90)
    assert state == "failed", f"Expected failed (always_fails), got {state}"

    tasks = stack.get_task_states("callbacks_demo", run_id)
    assert tasks["always_succeeds"] == "success"
    assert tasks["always_fails"] == "failed"


def test_callbacks_demo_on_success(stack):
    """on_success_callback zapise do logu 'CALLBACK on_success' po uspechu tasku."""
    run_id = stack.trigger_dag("callbacks_demo")
    stack.wait_dag_run("callbacks_demo", run_id, timeout=90)
    stack.assert_log_contains(
        "callbacks_demo", run_id, "always_succeeds",
        r"CALLBACK on_success",
    )


def test_callbacks_demo_on_failure(stack):
    """on_failure_callback zapise do logu 'CALLBACK on_failure' po selhani tasku."""
    run_id = stack.trigger_dag("callbacks_demo")
    stack.wait_dag_run("callbacks_demo", run_id, timeout=90)
    stack.assert_log_contains(
        "callbacks_demo", run_id, "always_fails",
        r"CALLBACK on_failure",
    )


# -- timeout_demo --

def test_timeout_demo_states(stack):
    """execution_timeout prerusi slow_task po 15s.

    - fast_task (2s, timeout 30s): success
    - slow_task_with_timeout (120s, timeout 15s): failed (AirflowTaskTimeout)
    - after_timeout: upstream_failed (protoze slow_task failoval)
    """
    stack.unpause("timeout_demo")
    run_id = stack.trigger_dag("timeout_demo")
    assert run_id
    state = stack.wait_dag_run("timeout_demo", run_id, timeout=90)
    assert state == "failed"

    tasks = stack.get_task_states("timeout_demo", run_id)
    assert tasks["fast_task"] == "success"
    assert tasks["slow_task_with_timeout"] == "failed"
    assert tasks["after_timeout"] == "upstream_failed"


# -- retry_demo (non-deterministic) --

def test_retry_demo_runs(stack):
    """retry_demo ma 60% fail rate s retries=3.

    Nedeterministicky — muze uspet i failnout.
    Overujeme jen ze DAG dobehne do terminalniho stavu
    a v logu je viditelny pokus "#".
    """
    stack.unpause("retry_demo")
    run_id = stack.trigger_dag("retry_demo")
    assert run_id
    state = stack.wait_dag_run("retry_demo", run_id, timeout=120)
    assert state in ("success", "failed"), f"Unexpected state: {state}"

    # V logu musi byt alespon 1 pokus
    stack.assert_log_contains(
        "retry_demo", run_id, "unreliable_task",
        r"Pokus #", "retry_demo: log obsahuje cislo pokusu",
    )
