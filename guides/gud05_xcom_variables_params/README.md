# gud05: XCom, Variables a Params

## Popis

Predavani dat mezi tasky (XCom), globalni konfigurace (Variables) a parametrizace DAG runu (Params). Tri ruzne mechanismy pro ruzne ucely.

## Cile

- XCom push/pull funguje mezi tasky (explicitni i pres Jinja templates)
- Custom XCom keys (ne jen default return_value)
- Airflow Variables se ctou z env var AIRFLOW_VAR_* (nastaveno v docker-compose)
- Variable.set() programaticky zapise hodnotu do DB
- DAG Params s validaci (type, enum, min/max) se predaji do tasku
- Trigger s custom --conf meni chovani DAGu za behu

## Koncepty

| Koncept | Ucel | Scope |
|---------|------|-------|
| **XCom** | Predavani dat mezi tasky v ramci jednoho DAG runu | Task → Task |
| **Variables** | Globalni konfigurace (napr. environment, credentials) | Globalni (vsechny DAGy) |
| **Params** | Parametry konkretniho DAG runu (trigger-time konfigurace) | Jeden DAG run |

## DAGy

### xcom_demo
- **xcom_push / xcom_pull** — explicitni pristup
- **Custom keys** — ne jen default `return_value`
- **Jinja template** — `{{ ti.xcom_pull(task_ids='...') }}`
- Ukazka limitu XCom (neni pro velka data)
- 4 tasky: `push_values` → `pull_values`, `jinja_xcom`, `show_xcom_size`

### variables_demo
- **Variable.get()** v Python kodu
- **Jinja** — `{{ var.value.nazev }}`
- **Env vars** — `AIRFLOW_VAR_*` (nastaveno v docker-compose)
- **Default hodnoty** — fallback kdyz Variable neexistuje
- **Variable.set()** — programaticke nastaveni
- 3 tasky: `read_variables` → `set_and_read` → `jinja_variables`

### params_demo
- **Param()** s typem, enum, min/max
- **Trigger s custom hodnotami** — UI formuler nebo CLI `--conf`
- **{{ params.nazev }}** v Jinja a `kwargs["params"]` v Python
- 2 tasky: `process_with_params` → `bash_with_params`

## Overeni

```bash
# Spustit stack
.\run.ps1

# XCom demo
docker compose exec airflow-scheduler airflow dags trigger xcom_demo

# Variables demo
docker compose exec airflow-scheduler airflow dags trigger variables_demo

# Params demo — s default hodnotami
docker compose exec airflow-scheduler airflow dags trigger params_demo

# Params demo — s custom hodnotami
docker compose exec airflow-scheduler airflow dags trigger params_demo --conf '{"stroj_id": "CNC-999", "format": "json", "batch_size": 500, "debug": true}'
```

**V UI (localhost:8080):**
- Admin → XComs: videt ulozene XCom hodnoty
- Admin → Variables: videt AIRFLOW_VAR_* a programaticky vytvorene
- params_demo: pri triggeru z UI se zobrazi formular s validaci

## Srovnani: XCom vs Variables vs Params

| Aspekt | XCom | Variables | Params |
|--------|------|-----------|--------|
| Scope | Task → Task | Globalni | Jeden DAG run |
| Zapis | `xcom_push` / `return` | UI, CLI, env var, API | Trigger config |
| Cteni | `xcom_pull` / Jinja | `Variable.get()` / Jinja | `params["key"]` / Jinja |
| Velikost | Male data (metadata) | Male konfigurace | Male konfigurace |
| Pouziti | Mezivysledky workflow | Environment, cesty, URL | Parametrizace spusteni |
