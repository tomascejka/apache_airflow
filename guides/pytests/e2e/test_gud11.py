"""
E2E testy pro gud11_pools_priority_config.

Cile:
- Pool s 2 sloty omezuje soubehu na max 2 tasky soucasne
- priority_weight urcuje poradi spousteni
- max_active_tasks omezuje pocet soucasne bezicich tasku jednoho DAGu
"""

import os

STACK_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "gud11_pools_priority_config")


# -- pool existence --

def test_machine_pool_exists(stack):
    """Pool 'machine_pool' musi existovat s 2 sloty.

    Pool je vytvoren v airflow-init (docker-compose.yaml entrypoint).
    Bez nej by pools_demo a priority_demo pouzivaly default_pool.
    """
    data = stack._cli_json("pools", "list", "-o", "json")
    assert data, "pools list returned no data"
    pool = next((p for p in data if p["pool"] == "machine_pool"), None)
    assert pool, "machine_pool not found"
    assert int(pool["slots"]) == 2, f"Expected 2 slots, got {pool['slots']}"


# -- pools_demo --

def test_pools_demo_success(stack):
    """6 tasku v machine_pool (2 sloty) — vsechny musi uspet.

    Pool omezuje na max 2 soucasne bezici tasky.
    Vsech 6 tasku postupne dobehne (frontovani).
    """
    stack.unpause("pools_demo")
    run_id = stack.trigger_dag("pools_demo")
    assert run_id
    # 6 tasku * 10s kazdy / 2 sloty = ~30s minimum, dáme 120s
    state = stack.wait_dag_run("pools_demo", run_id, timeout=120)
    assert state == "success", f"pools_demo state={state}"


def test_pools_demo_all_tasks(stack):
    """Vsech 6 process_* tasku musi byt success."""
    run_id = stack.trigger_dag("pools_demo")
    stack.wait_dag_run("pools_demo", run_id, timeout=120)
    tasks = stack.get_task_states("pools_demo", run_id)

    process_tasks = {k: v for k, v in tasks.items() if k.startswith("process_")}
    assert len(process_tasks) == 6, f"Expected 6 process tasks, got {len(process_tasks)}"
    for task_id, state in process_tasks.items():
        assert state == "success", f"{task_id} = {state}"


# -- priority_demo --

def test_priority_demo_success(stack):
    """4 tasky s ruznymi priority_weight (10, 5, 1, 0) — vsechny musi uspet.

    Poradi spusteni je rideno priority_weight (vyssi = driv),
    ale vsechny nakonec dobehnou.
    """
    stack.unpause("priority_demo")
    run_id = stack.trigger_dag("priority_demo")
    assert run_id
    state = stack.wait_dag_run("priority_demo", run_id, timeout=120)
    assert state == "success", f"priority_demo state={state}"

    tasks = stack.get_task_states("priority_demo", run_id)
    assert tasks["critical_task"] == "success"
    assert tasks["high_priority"] == "success"
    assert tasks["normal_priority"] == "success"
    assert tasks["low_priority"] == "success"


# -- config_tuning --

def test_config_tuning_success(stack):
    """8 worker tasku s max_active_tasks=3 — vsechny musi uspet.

    DAG ma max_active_tasks=3, takze max 3 tasky bezi soucasne.
    Vsech 8 postupne dobehne.
    """
    stack.unpause("config_tuning")
    run_id = stack.trigger_dag("config_tuning")
    assert run_id
    # 8 tasku * 8s kazdy / 3 soucasne = ~24s minimum, dáme 120s
    state = stack.wait_dag_run("config_tuning", run_id, timeout=120)
    assert state == "success", f"config_tuning state={state}"


def test_config_tuning_all_workers(stack):
    """Vsech 8 worker_* tasku musi byt success."""
    run_id = stack.trigger_dag("config_tuning")
    stack.wait_dag_run("config_tuning", run_id, timeout=120)
    tasks = stack.get_task_states("config_tuning", run_id)

    worker_tasks = {k: v for k, v in tasks.items() if k.startswith("worker_")}
    assert len(worker_tasks) == 8, f"Expected 8 worker tasks, got {len(worker_tasks)}"
    for task_id, state in worker_tasks.items():
        assert state == "success", f"{task_id} = {state}"
