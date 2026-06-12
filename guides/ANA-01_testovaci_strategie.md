# ANA-01: Testovaci strategie pro gudX laboratorie

## Kontext

Kazda gudX laborator demonstruje Airflow koncepty. Cil: automatizovane overeni, ze DAGy funguji spravne — `test.ps1` ktery spusti po `run.ps1`.

## Spolecny testovaci framework

### Predpoklady
- Stack bezi (`run.ps1` uz probehl)
- Airflow scheduler je healthy
- DAGy jsou naparsovane (dag-processor je zpracoval)

### Dostupne nastroje (Airflow CLI v kontejneru)
```bash
docker compose exec airflow-scheduler airflow dags trigger <dag_id>
docker compose exec airflow-scheduler airflow dags list-runs -d <dag_id> -o json
docker compose exec airflow-scheduler airflow tasks states-for-dag-run <dag_id> <run_id>
docker compose exec airflow-scheduler airflow tasks logs <dag_id> <task_id> <run_id>
docker compose exec airflow-scheduler airflow dags unpause <dag_id>
docker compose exec airflow-scheduler airflow pools list -o json
```

### Spolecny pattern test.ps1
1. Unpause DAGy (jsou paused at creation)
2. Trigger DAG
3. Pockej na dokonceni (poll `dags list-runs` dokud state != "running"/"queued")
4. Over task stavy (`tasks states-for-dag-run`)
5. Over log obsah (`tasks logs | grep "pattern"`)
6. Report PASS/FAIL

### Casove limity
- Jednoduche DAGy: 30s timeout
- DAGy s retries/sensors: 120s timeout
- DAGy se sleep/timeout: 180s timeout

---

## Analyza po gudX

### gud4_python_operator

**Cile:**
1. PythonOperator spusti python_callable s op_args/op_kwargs
2. @task dekorator funguje jako nahrada PythonOperator
3. Return value se automaticky ulozi do XCom (TaskFlow)
4. Kontextove promenne (ti, dag_run, ds) jsou dostupne

**Testovatelnost: VYSOKA** — plne deterministicke, zadne externi zavislosti

| DAG | Ocekavane stavy | Log pattern k overeni | Cas |
|-----|----------------|----------------------|-----|
| python_basics | vsechny 3 success | `multiply(10, 5) = 50` | <10s |
| taskflow_basics | vsechny 3 success | `teplota_f.*162.5`, `Vibrace: OK` | <10s |

**test.ps1 strategie:**
- Trigger oba DAGy
- Pockej na success
- Grep logy: `multiply(10, 5) = 50`, `=== LOAD ===`, `Vibrace: OK`

---

### gud5_xcom_variables_params

**Cile:**
1. XCom push/pull funguje (explicitni i Jinja)
2. Variables cteni z env var AIRFLOW_VAR_* funguje
3. Variable.set() programaticky zapise hodnotu
4. Params s validaci (type, enum, min/max) se predaji do tasku

**Testovatelnost: VYSOKA** — deterministicke, env vars nastavene v docker-compose

| DAG | Ocekavane stavy | Log pattern | Cas |
|-----|----------------|-------------|-----|
| xcom_demo | vsechny 4 success | `stroj_id: CNC-001`, `teplota.*72.5` | <10s |
| variables_demo | vsechny 3 success | `environment = development` | <10s |
| params_demo | vsechny 2 success | `stroj_id: CNC-001`, `format: csv` | <10s |
| params_demo (custom) | vsechny 2 success | `stroj_id: CNC-999`, `format: json` | <10s |

**test.ps1 strategie:**
- Trigger xcom_demo, variables_demo, params_demo (default)
- Trigger params_demo s `--conf '{"stroj_id":"CNC-999","format":"json","batch_size":500,"debug":true}'`
- Grep logy: env var hodnoty, custom params

---

### gud6_branching_trigger_rules

**Cile:**
1. BranchPythonOperator vybere jednu vetev, druha je skipped
2. Trigger rules urcuji kdy se task spusti (all_success, one_success, all_done, ...)
3. ShortCircuitOperator preskoci downstream pri False

**Testovatelnost: STREDNI** — branching a short_circuit jsou nedeterministicke (random)

| DAG | Deterministicky? | Klicova aserce | Cas |
|-----|-----------------|----------------|-----|
| branching_demo | NE (random) | prave 1 z branch_a/branch_b je success, druhy skipped; join+end vzdy success | <10s |
| trigger_rules_demo | ANO | failing_task=failed, needs_one_success=success, needs_all_success=upstream_failed, runs_when_all_done=success, cleanup_always=success | <10s |
| short_circuit_demo | CASTECNE (chain1 random, chain2 dle dne) | check_should_run vzdy success; downstream dle vysledku | <10s |

**test.ps1 strategie:**
- trigger_rules_demo: hard-assert presne stavy vsech 8 tasku (plne deterministicke)
- branching_demo: assert invariant (prave 1 vetev success + 1 skipped, join+end success)
- short_circuit_demo: precti log check_should_run, pak assert konzistentni downstream stavy

---

### gud7_sensors

**Cile:**
1. FileSensor detekuje soubor a spusti downstream
2. TimeDeltaSensor ceka zadany casovy usek
3. ExternalTaskSensor ceka na dokonceni jineho DAGu

**Testovatelnost: STREDNI-NIZKA** — sensory vyzaduji externi akce a cekani

| DAG | Externi akce | Timeout/cekani | Komplikace |
|-----|-------------|----------------|------------|
| file_sensor_demo | Vytvorit soubor `data/trigger_file.csv` | sensor timeout 120s, poke 5s | Test musi vytvorit soubor BEHEM cekani sensoru |
| time_sensor_demo | Zadne | TimeDeltaSensor ceka 30s od logical_date | Pro manual trigger: logical_date ~ now, takze sensor projde hned nebo brzy |
| sensor_producer + sensor_consumer | Trigger producer PRED consumerem | sensor timeout 120s, poke 10s | Consumer musi cekat na producera se STEJNYM logical_date — problematicke |

**test.ps1 strategie:**
- file_sensor_demo: trigger DAG → pockat 5s → vytvorit soubor → pockat na success (~15s)
- time_sensor_demo: trigger → pockat na success (~45s)
- external_task_sensor: **SLOZITE** — ExternalTaskSensor matchuje logical_date, coz u manual triggeru nemusi sedet. Mozna skip nebo zjednodusit.

**RIZIKO:** ExternalTaskSensor matching je slozity — pro test.ps1 doporucuji testovat jen file_sensor a time_sensor.

---

### gud8_error_handling

**Cile:**
1. Task s retries se opakuje pri selhani (videt up_for_retry stav)
2. Callbacky (on_success, on_failure, on_retry) se volaji
3. execution_timeout prerusi task po zadane dobe

**Testovatelnost: STREDNI** — nektery DAGy zamerne failuji, retry_demo je nedeterministicky

| DAG | Deterministicky? | Ocekavany DAG run stav | Klicova aserce | Cas |
|-----|-----------------|----------------------|----------------|-----|
| retry_demo | NE (60% fail) | success NEBO failed | Pokud success: `USPECH! Task uspel na pokus #N`; try_number > 0 | az 120s (retries + backoff) |
| callbacks_demo | ANO | failed (kvuli always_fails) | `CALLBACK on_success.*always_succeeds`, `CALLBACK on_failure.*always_fails` | <15s |
| timeout_demo | ANO | failed (kvuli timeout) | fast_task=success, slow_task_with_timeout=failed | ~20s (15s timeout + overhead) |

**test.ps1 strategie:**
- callbacks_demo: assert always_succeeds=success, always_fails=failed; grep `CALLBACK on_success`, `CALLBACK on_failure`
- timeout_demo: assert fast_task=success, slow_task_with_timeout=failed; grep `AirflowTaskTimeout` nebo timeout error
- retry_demo: assert DAG run stav je `success` NEBO `failed` (oba validni); grep `Pokus #` v logu

---

### gud9_taskgroups_dynamic

**Cile:**
1. TaskGroup seskupi tasky vizualne (task_id prefix: "extract.extract_csv")
2. expand() vytvori dynamicky pocet tasku (5 instanci pro 5 stroju)
3. Reduce pattern agreguje vysledky z mapped tasku

**Testovatelnost: VYSOKA** — plne deterministicke

| DAG | Ocekavane stavy | Klicova aserce | Cas |
|-----|----------------|----------------|-----|
| taskgroup_demo | vsech 10 success | task_id obsahuje prefixes "extract.", "transform.validate.", "load." | <15s |
| dynamic_mapping_demo | get_machines=success, process_machine (5 mapped)=success, report=success | 5 mapped instanci; log `Zpracovavam stroj CNC-001` | <15s |
| mapped_reduce_demo | list_files=success, process_file (4 mapped)=success, aggregate_results=success | log `Souboru: 4`, `Celkem radku: 881` | <15s |

**test.ps1 strategie:**
- taskgroup_demo: assert vsechny tasky success
- dynamic_mapping_demo: assert 5 mapped instanci process_machine; grep `Zpracovavam stroj`
- mapped_reduce_demo: assert 4 mapped instanci; grep `Souboru: 4` v aggregate_results logu

---

### gud10_scheduling_datasets

**Cile:**
1. Cron vyrazy a Jinja date templates funguji (ds, data_interval_start)
2. Asset (dataset) producer triggeruje consumer automaticky
3. Catchup=True vytvori historicke DAG runy

**Testovatelnost: STREDNI** — dataset chain a catchup maji specificke chovani

| DAG | Klicova aserce | Komplikace | Cas |
|-----|---------------|------------|-----|
| cron_demo | vsechny success; log obsahuje `logical_date` | Zadne | <10s |
| dataset_producer + consumer | Producer success → consumer se automaticky triggeruje | Nutne unpause oba DAGy PRED triggerem producera; cekani na auto-trigger consumera | ~30s |
| catchup_demo | Vytvori 7 historickych runu | Nutne unpause; max_active_runs=2 zpomaluje; vsechny runy musi dokoncit | ~60-120s |

**test.ps1 strategie:**
- cron_demo: trigger, assert success, grep `ds` a `data_interval` v logu
- dataset: unpause oba → trigger producer → pockat → overit ze consumer ma alespon 1 run
- catchup_demo: unpause → pockat → overit ze existuje vice nez 1 DAG run (idealne 7)

---

### gud11_pools_priority_config

**Cile:**
1. Pool omezuje pocet soucasne bezicich tasku (2 sloty, 6 tasku)
2. priority_weight urcuje poradi spousteni
3. max_active_tasks omezuje soubehu v ramci DAGu

**Testovatelnost: NIZKA-STREDNI** — overeni soubehu vyzaduje casovou analyzu

| DAG | Co overit | Jak | Cas |
|-----|----------|-----|-----|
| pools_demo | Vsech 6 tasku success; pool existuje s 2 sloty | `airflow pools list` → machine_pool slots=2; vsechny tasky success | ~35s (6 tasku * 10s / 2 sloty) |
| priority_demo | Vsechny 4 tasky success; vyssi priorita = driv | Log start_date porovnani — ale slozite automatizovat | ~25s |
| config_tuning | Vsech 8 tasku success | assert vsechny success | ~30s (8 tasku * 8s / 3 soucasne) |

**test.ps1 strategie:**
- pools_demo: assert pool existuje (`airflow pools list`), assert vsech 6 tasku success
- priority_demo: assert vsechny 4 success (poradi overit slozite — skip)
- config_tuning: assert vsech 8 tasku success

---

## Souhrnna matice testovatelnosti

| gudX | Deterministicke? | Externi zavislosti? | Cas | Obtiznost test.ps1 |
|------|-----------------|--------------------|----|-------------------|
| **gud4** | ANO | NE | <10s | SNADNE |
| **gud5** | ANO | NE (env vars v compose) | <15s | SNADNE |
| **gud6** | CASTECNE | NE | <10s | STREDNI (invarianty misto presnych hodnot) |
| **gud7** | ANO (ale s externi akci) | ANO (soubor, timing) | 15-120s | SLOZITE (sensor orchestrace) |
| **gud8** | CASTECNE | NE | 15-120s | STREDNI (zamerne faily, nedeterministicky retry) |
| **gud9** | ANO | NE | <15s | SNADNE |
| **gud10** | ANO (ale slozite chovani) | NE | 30-120s | STREDNI (dataset chain, catchup timing) |
| **gud11** | ANO (ale concurrency tezko overit) | NE | 25-35s | STREDNI (pool existence OK, poradi slozite) |

## Doporuceni: co testovat v test.ps1

### Tier 1 — VZDY testovat (jednoduche, deterministicke)
- Vsechny DAGy: trigger → pockej → assert task stavy (success/failed/skipped)
- Log grep na klicove patterny

### Tier 2 — Testovat s invarianty (nedeterministicke)
- branching_demo: "prave 1 vetev success"
- retry_demo: "DAG run je success NEBO failed" (oba OK)
- short_circuit_demo: "downstream konzistentni s check vysledkem"

### Tier 3 — Testovat s orchestraci (externi akce)
- file_sensor_demo: vytvorit soubor behem cekani
- dataset_producer/consumer: unpause + trigger + verify chain

### Tier 4 — Vynechat / manualne (prilis slozite pro automatizaci)
- ExternalTaskSensor (logical_date matching)
- priority_weight poradi (casova analyza)
- Presny pocet soucasnych tasku v poolu (race condition)

## Struktura test.ps1

```
test.ps1
  1. Predpoklady: stack bezi, scheduler healthy
  2. Helper funkce:
     - Trigger-And-Wait($dagId, $timeoutSec)
     - Assert-TaskState($dagId, $runId, $taskId, $expectedState)
     - Assert-LogContains($dagId, $taskId, $runId, $pattern)
  3. Testy per DAG (volaji helpery)
  4. Summary: X passed, Y failed, Z skipped
```
