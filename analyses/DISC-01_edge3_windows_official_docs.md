# DISC-01: Oficialni dokumentace edge3 — Windows install

## Zdroje

- https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/install_on_windows.html
- https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/deployment.html
- https://github.com/apache/airflow/blob/main/providers/edge3/docs/install_on_windows.rst

## Relevance: VYSOKA

## Souhrn

Oficialni dokumentace popisuje instalaci Edge Workeru na Windows. Klicove: dokumentace byla napsana pro **Airflow 2.10.5** a na Airflow 3.x nefunguje.

## Klicove poznatky

### Testovana konfigurace

- Windows 10, Python 3.12.8 (64-bit)
- Backend: Airflow 2.10.5
- Minimum: Python 3.10+

### Instalacni kroky

1. Vytvorit `C:\Airflow`, Python venv
2. `pip install apache-airflow-providers-edge3 --constraint ...`
3. Vytvorit `dags/` slozku, nakopirovat DAG soubory
4. Vytvorit `start_worker.bat` s env vars:
   - `AIRFLOW__API_AUTH__JWT_SECRET` (3.0.0+) nebo `AIRFLOW__CORE__INTERNAL_API_SECRET_KEY` (pre-3.0)
   - `AIRFLOW__CORE__DAGS_FOLDER=dags`
   - `AIRFLOW__LOGGING__BASE_LOG_FOLDER=edge_logs`
   - `AIRFLOW__EDGE__API_URL=https://hostname:port/edge_worker/v1/rpcapi`
   - `AIRFLOW__CORE__EXECUTOR=airflow.providers.edge3.executors.edge_executor.EdgeExecutor`
5. Spusteni: `airflow edge worker --concurrency 4 --queues windows`

### Varovani v dokumentaci

- "Edge Worker is **only manually tested on Windows** and the setup is **not validated in CI**"
- "It is recommended to **use Linux** for Edge Worker"
- "Windows-based setup is only for **testing at your own risk**"
- "Technically limited due to **Python OS restrictions** and is a **Proof-of-Concept quality**"

### Znamy problem: dvojtecka v ceste

Windows zakazuje `:` v nazvech souboru. Airflow Run ID obsahuje timestamp s dvojteckami (napr. `manual__2024-01-15T10:30:00+00:00`), coz rozbije vytvareni log souboru.

**Workaround**: zmena `AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE` s Jinja `| replace(":", "-")` — ale toto je **server-side** konfigurace, ovlivnuje cely cluster.

### Proxy podpora

`set http_proxy=...` / `set https_proxy=...` pred spustenim workeru.

### Deployment docs — obecne pozadavky

- Airflow CLI v PATH s Task SDK a edge3 providerem
- Homogenni konfigurace across cluster
- Operator dependencies na edge stroji
- Pristup k `DAGS_FOLDER` (Git sync nebo shared mount)
- `[edge] api_enabled = true` na serveru
- `airflow db migrate` na centralni instanci (vytvori edge3 tabulky)
