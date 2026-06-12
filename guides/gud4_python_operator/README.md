# gud4: PythonOperator a TaskFlow API

## Popis

Demonstrace dvou zpusobu psani Python tasku v Airflow: klasicky PythonOperator a moderni TaskFlow API (@task dekorator). Navazuje na gud3 (BashOperator).

## Cile

- PythonOperator spusti python_callable s op_args a op_kwargs
- @task dekorator funguje jako nahrada PythonOperator (TaskFlow API)
- Return value z @task se automaticky ulozi do XCom
- Kontextove promenne (ti, dag_run, ds) jsou dostupne pres **kwargs
- logging modul funguje v Airflow logu (na rozdil od print ktery jde do stdout)

## Koncepty

| Koncept | Popis |
|---------|-------|
| **PythonOperator** | Klasicky operator — `python_callable`, `op_args`, `op_kwargs` |
| **@task dekorator** | TaskFlow API — funkce = task, return = XCom, parametr = dependency |
| **@dag dekorator** | Alternativa k `with DAG(...)` context manageru |
| **Context (kwargs)** | Pristup k `ti`, `dag_run`, `logical_date`, `ds` a dalsim |
| **logging vs print** | `logging.getLogger()` jde do Airflow logu, `print` do stdout |

## DAGy

### python_basics
- **PythonOperator** s `python_callable`
- `op_args` (pozicni) a `op_kwargs` (pojmenovane argumenty)
- `**kwargs` pro pristup ke kontextu TaskInstance
- `logging` modul vs `print`
- 3 tasky: `greet_user` → `show_context` → `compute_result`

### taskflow_basics
- **@task** dekorator (nahrazuje PythonOperator)
- Return value se automaticky uklada do XCom
- Parametry funkce se automaticky napoji na XCom z upstream tasku
- ETL vzor: `extract` → `transform` → `load`
- 3 tasky propojene pres function calls (ne >> operator)

## Overeni

```bash
# Spustit stack
.\run.ps1

# Trigger DAGu
docker compose exec airflow-scheduler airflow dags trigger python_basics
docker compose exec airflow-scheduler airflow dags trigger taskflow_basics

# Kontrola logu
docker compose exec airflow-scheduler airflow tasks logs python_basics greet_user -1
docker compose exec airflow-scheduler airflow tasks logs taskflow_basics extract -1
```

**V UI (localhost:8080):**
- python_basics: v logs videt rozdil mezi `logger.info` a `print` vystupem
- taskflow_basics: v Admin → XComs videt automaticky ulozene hodnoty z kazdeho tasku

## Klic: PythonOperator vs TaskFlow

| Aspekt | PythonOperator | @task |
|--------|---------------|-------|
| Definice | `PythonOperator(python_callable=fn)` | `@task` nad funkci |
| Argumenty | `op_args`, `op_kwargs` | parametry funkce |
| XCom push | `ti.xcom_push()` nebo `return` | `return` (automaticky) |
| XCom pull | `ti.xcom_pull()` nebo Jinja | parametr funkce (automaticky) |
| Dependencies | `>>` operator | function call |
| Vhodne pro | jednoduche tasky, externi funkce | ETL pipeliny, ciste Python workflows |
