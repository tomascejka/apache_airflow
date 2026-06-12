"""
Shared fixtures for Airflow e2e/integration tests.

Each test module defines STACK_DIR pointing to its gudXX directory.
The `stack` fixture starts docker compose, waits for scheduler,
runs tests, then tears down.

Usage:
    cd guides
    pytest pytests/e2e/test_gud04.py -v
    pytest pytests/e2e/ -v --tb=short   # all gudXX serially
"""

import json
import os
import re
import subprocess
import time

import pytest


class AirflowStack:
    """Helper for interacting with Airflow via docker compose CLI."""

    def __init__(self, workdir: str):
        self.workdir = workdir

    def _run(self, *args, timeout=60):
        return subprocess.run(
            list(args),
            capture_output=True,
            text=True,
            cwd=self.workdir,
            timeout=timeout,
        )

    def _cli_json(self, *airflow_args, timeout=120):
        """Run airflow CLI command, return parsed JSON or None."""
        result = self._run(
            "docker", "compose", "exec", "-T", "airflow-scheduler",
            "airflow", *airflow_args,
            timeout=timeout,
        )
        for line in result.stdout.splitlines():
            stripped = line.strip()
            if stripped.startswith("[") or stripped.startswith("{"):
                try:
                    return json.loads(stripped)
                except json.JSONDecodeError:
                    continue
        return None

    # -- DAG operations --

    def unpause(self, dag_id: str):
        self._run(
            "docker", "compose", "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "unpause", dag_id,
        )

    def trigger_dag(self, dag_id: str, conf: str | None = None) -> str | None:
        """Trigger a DAG, return run_id."""
        args = [
            "docker", "compose", "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "trigger", "-o", "json",
        ]
        if conf:
            args += ["-c", conf]
        args.append(dag_id)

        result = self._run(*args, timeout=120)
        for line in result.stdout.splitlines():
            stripped = line.strip()
            if stripped.startswith("[") or stripped.startswith("{"):
                try:
                    data = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                if isinstance(data, list):
                    return data[0]["dag_run_id"]
                return data["dag_run_id"]
        return None

    def wait_dag_run(self, dag_id: str, run_id: str, timeout: int = 60) -> str:
        """Poll until DAG run reaches terminal state. Returns state string."""
        elapsed = 0
        while elapsed < timeout:
            data = self._cli_json("dags", "list-runs", dag_id, "-o", "json")
            if data:
                for run in data:
                    if run["run_id"] == run_id:
                        state = run["state"]
                        if state in ("success", "failed"):
                            return state
            time.sleep(3)
            elapsed += 3
        return "timeout"

    def get_task_states(self, dag_id: str, run_id: str) -> dict[str, str]:
        """Return {task_id: state} dict for a DAG run."""
        data = self._cli_json(
            "tasks", "states-for-dag-run", dag_id, run_id, "-o", "json",
        )
        if data:
            return {t["task_id"]: t["state"] for t in data}
        return {}

    def get_log(self, dag_id: str, run_id: str, task_id: str, attempt: int = 1) -> str:
        """Read task log from container filesystem."""
        log_path = (
            f"/opt/airflow/logs/dag_id={dag_id}"
            f"/run_id={run_id}"
            f"/task_id={task_id}"
            f"/attempt={attempt}.log"
        )
        for container in ("airflow-worker", "airflow-scheduler"):
            result = self._run(
                "docker", "compose", "exec", "-T", container,
                "cat", log_path,
            )
            if result.returncode == 0 and result.stdout:
                return result.stdout
        return ""

    def assert_log_contains(self, dag_id, run_id, task_id, pattern, msg=None):
        """Assert regex pattern is found in task log."""
        log = self.get_log(dag_id, run_id, task_id)
        assert log, f"Log not found: {dag_id}/{task_id}/{run_id}"
        assert re.search(pattern, log), (
            f"{msg or pattern} not found in log of {task_id}"
        )

    def wait_for_consumer_run(self, dag_id: str, timeout: int = 90) -> str | None:
        """Wait for a DAG to get a run (e.g. triggered by dataset/asset)."""
        elapsed = 0
        while elapsed < timeout:
            data = self._cli_json("dags", "list-runs", dag_id, "-o", "json")
            if data:
                for run in data:
                    if run["state"] == "success":
                        return run["run_id"]
            time.sleep(5)
            elapsed += 5
        return None

    def exec_in_container(self, container: str, *cmd):
        """Run arbitrary command in a container."""
        return self._run(
            "docker", "compose", "exec", "-T", container, *cmd,
        )


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def stack(request):
    """Start docker compose stack, wait for scheduler, yield helper, tear down."""
    workdir = request.module.STACK_DIR
    assert os.path.isdir(workdir), f"STACK_DIR not found: {workdir}"

    s = AirflowStack(workdir)

    # Start
    s._run("docker", "compose", "up", "airflow-init", timeout=180)
    s._run("docker", "compose", "up", "-d", timeout=180)

    # Wait for scheduler (max 120s)
    ready = False
    for _ in range(24):
        result = s._run(
            "docker", "compose", "exec", "-T", "airflow-scheduler",
            "airflow", "jobs", "check", "--job-type", "SchedulerJob",
        )
        if result.returncode == 0:
            ready = True
            break
        time.sleep(5)

    if not ready:
        s._run("docker", "compose", "down", "-v", timeout=60)
        pytest.fail("Scheduler did not start within 120s")

    # Module-specific post-setup (e.g. create connections, pools)
    setup_fn = getattr(request.module, "stack_setup", None)
    if setup_fn:
        setup_fn(s)

    # Wait for DAG parsing
    time.sleep(30)

    yield s

    # Teardown
    s._run("docker", "compose", "down", "-v", timeout=120)
