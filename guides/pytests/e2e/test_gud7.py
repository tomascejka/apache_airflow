"""
E2E testy pro gud7_sensors.

Cile:
- FileSensor detekuje soubor a spusti downstream
- TimeDeltaSensor ceka zadany casovy usek
- poke_interval a soft_fail funguji spravne
"""

import os

STACK_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "gud7_sensors")


def stack_setup(stack):
    """Vytvoreni fs_default connection pro FileSensor.

    Airflow 3.x nevytvari default connections automaticky.
    FileSensor vyzaduje conn_id='fs_default' typu 'fs'.
    """
    stack._run(
        "docker", "compose", "exec", "-T", "airflow-scheduler",
        "airflow", "connections", "add", "fs_default",
        "--conn-type", "fs", "--conn-extra", '{"path": "/"}',
        timeout=120,
    )


def test_file_sensor_detect(stack):
    """FileSensor detekuje soubor a cely pipeline dobehne.

    Postup:
    1. Trigger DAG (cleanup smazne stary soubor, sensor zacne ceket)
    2. Po 10s vytvorime trigger_file.csv v kontejneru
    3. Sensor detekuje soubor → process_file a done se spusti
    """
    stack.unpause("file_sensor_demo")
    run_id = stack.trigger_dag("file_sensor_demo")
    assert run_id, "trigger failed"

    # Pockat az sensor bezi, pak vytvorit soubor
    import time
    time.sleep(10)

    stack.exec_in_container(
        "airflow-worker",
        "bash", "-c",
        "mkdir -p /opt/airflow/data && echo 'stroj,teplota\nCNC-001,72.5' > /opt/airflow/data/trigger_file.csv",
    )

    state = stack.wait_dag_run("file_sensor_demo", run_id, timeout=90)
    assert state == "success", f"DAG state={state}"

    tasks = stack.get_task_states("file_sensor_demo", run_id)
    assert tasks["cleanup_old_file"] == "success"
    assert tasks["wait_for_file"] == "success"
    assert tasks["process_file"] == "success"
    assert tasks["done"] == "success"


def test_file_sensor_log_contains_detection(stack):
    """Po detekci souboru se v logu process_file objevi potvrzeni."""
    run_id = stack.trigger_dag("file_sensor_demo")

    import time
    time.sleep(10)
    stack.exec_in_container(
        "airflow-worker",
        "bash", "-c",
        "mkdir -p /opt/airflow/data && echo 'test' > /opt/airflow/data/trigger_file.csv",
    )

    stack.wait_dag_run("file_sensor_demo", run_id, timeout=90)
    stack.assert_log_contains(
        "file_sensor_demo", run_id, "process_file",
        r"Soubor detekovan", "process_file: soubor detekovan",
    )


def test_time_sensor_waits(stack):
    """TimeDeltaSensor ceka 30 sekund a pak spusti downstream task.

    DAG: start → wait_30_seconds → after_wait
    Timeout 90s protoze sensor ceka 30s + overhead.
    """
    stack.unpause("time_sensor_demo")
    run_id = stack.trigger_dag("time_sensor_demo")
    assert run_id, "trigger failed"

    state = stack.wait_dag_run("time_sensor_demo", run_id, timeout=90)
    assert state == "success", f"DAG state={state}"

    tasks = stack.get_task_states("time_sensor_demo", run_id)
    assert tasks["start"] == "success"
    assert tasks["wait_30_seconds"] == "success"
    assert tasks["after_wait"] == "success"
