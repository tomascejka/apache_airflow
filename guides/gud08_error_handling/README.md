# gud08: Error Handling

## Popis

Mechanismy pro zpracovani chyb v Airflow: retries, callbacky a timeouty. Klicove pro produkci — kazdy task muze selhat.

## Cile

- Task s retries se opakuje pri selhani (stav up_for_retry v UI)
- retry_exponential_backoff navysuje prodlevu mezi pokusy
- on_success_callback se zavola po uspechu tasku
- on_failure_callback se zavola po selhani tasku (po vycerpani vsech retries)
- on_retry_callback se zavola pri kazdem retry pokusu
- execution_timeout prerusi task po zadane dobe (AirflowTaskTimeout)

## Koncepty

| Koncept | Popis |
|---------|-------|
| **retries** | Pocet opakovani pri selhani tasku |
| **retry_delay** | Prodleva mezi pokusy |
| **retry_exponential_backoff** | Exponencialni navysovani prodlevy (10s, 20s, 40s, ...) |
| **on_success_callback** | Funkce volana po uspechu tasku |
| **on_failure_callback** | Funkce volana po selhani (vsechny retries vycerpany) |
| **on_retry_callback** | Funkce volana pri kazdem retry |
| **execution_timeout** | Max doba behu jednoho tasku |
| **dagrun_timeout** | Max doba behu celeho DAG runu |

## DAGy

### retry_demo
- Task s 60% sanci na fail, `retries=3`, exponential backoff
- V logs videt cislo pokusu a prodlevu mezi nimi
- Task stav `up_for_retry` v UI

### callbacks_demo
- 3 tasky: vzdy uspeje, vzdy failuje, nahodny s retry
- Callbacky `on_success`, `on_failure`, `on_retry`
- V logs videt "CALLBACK" zpravy
- **POZOR:** `always_fails` zamerne failuje

### timeout_demo
- `fast_task` (2s, timeout 30s) — OK
- `slow_task_with_timeout` (120s, timeout 15s) — prerusen po 15s
- V logs videt AirflowTaskTimeout
- **POZOR:** `slow_task_with_timeout` zamerne failuje (timeout)

## Overeni

```bash
# Spustit stack
.\run.ps1

# Retry demo — spustit vicekrat, sledovat retry pokusy
docker compose exec airflow-scheduler airflow dags trigger retry_demo

# Callbacks demo
docker compose exec airflow-scheduler airflow dags trigger callbacks_demo

# Timeout demo
docker compose exec airflow-scheduler airflow dags trigger timeout_demo
```

**V UI (localhost:8080):**
- retry_demo: task stav `up_for_retry` (oranzova), pak `success` nebo `failed`
- callbacks_demo: v logs hledat "CALLBACK on_success/on_failure/on_retry"
- timeout_demo: `slow_task_with_timeout` failuje s AirflowTaskTimeout

## Task stavy pri chybach

```
running → success                    (vse OK)
running → up_for_retry → running → success   (retry uspel)
running → up_for_retry → ... → failed        (vsechny retries vycerpany)
running → failed                     (timeout nebo 0 retries)
```
