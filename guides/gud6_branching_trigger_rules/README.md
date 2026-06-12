# gud6: Branching a Trigger Rules

## Popis

Podminene vetveni workflow (BranchPythonOperator, ShortCircuitOperator) a pravidla spousteni tasku (trigger_rule). Klicove pro realne workflow kde ne vsechny cesty bezi vzdy.

## Cile

- BranchPythonOperator vybere jednu vetev, ostatni jsou skipped
- Join task po branchi funguje s trigger_rule="none_failed_min_one_success"
- Trigger rules urcuji kdy se task spusti (all_success, one_success, all_done, none_failed_min_one_success)
- Zamerne failujici task (failing_task) ovlivni downstream dle jejich trigger_rule
- ShortCircuitOperator preskoci vsechny downstream tasky pri False

## Koncepty

| Koncept | Popis |
|---------|-------|
| **BranchPythonOperator** | Vraci task_id vetvy ktera se spusti, ostatni jsou "skipped" |
| **trigger_rule** | Pravidlo kdy se task spusti (all_success, one_success, all_done, ...) |
| **ShortCircuitOperator** | Pokud vrati False, vsechny downstream se preskoci |
| **EmptyOperator** | Prazdny task — uzitecny pro join/start/end body |
| **skip stav** | Task ktery nebyl spusten kvuli branchi nebo short circuitu |

## DAGy

### branching_demo
- **BranchPythonOperator** — nahodny vyber vetev A nebo B
- Join task s `trigger_rule="none_failed_min_one_success"`
- Opakovanym spoustenim videt ruzne cesty v Graph view
- 5 tasku: `start` → `choose_branch` → `branch_a`/`branch_b` → `join` → `end`

### trigger_rules_demo
- 3 upstream tasky: 2x success + 1x ZAMERNE fail
- 4 downstream s ruznymi `trigger_rule` — videt ktere se spusti
- `cleanup_always` s `all_done` — bezi vzdy
- **POZOR:** `failing_task` zamerne failuje!

### short_circuit_demo
- **ShortCircuitOperator** s nahodnym True/False
- Prakticky priklad: kontrola pracovniho dne
- Downstream tasky se preskoci kdyz funkce vrati False

## Overeni

```bash
# Spustit stack
.\run.ps1

# Branching — spustit 3-4x a sledovat ruzne cesty v UI Graph view
docker compose exec airflow-scheduler airflow dags trigger branching_demo

# Trigger rules — failing_task zamerne failuje
docker compose exec airflow-scheduler airflow dags trigger trigger_rules_demo

# Short circuit — spustit vicekrat, nekdy se downstream preskoci
docker compose exec airflow-scheduler airflow dags trigger short_circuit_demo
```

**V UI (localhost:8080):**
- Graph view: barevne vidite ktere tasky prosly (zelene), ktere failovaly (cervene), ktere byly preskoceny (ruzove)
- trigger_rules_demo: `needs_all_success` bude upstream_failed, `needs_one_success` bude success, `runs_when_all_done` bude success

## Trigger Rules — prehled

| Rule | Spusti se kdyz... |
|------|-------------------|
| `all_success` | vsechny upstream uspely (default) |
| `one_success` | alespon jeden upstream uspel |
| `all_done` | vsechny upstream dokonceny (jakkoliv) |
| `none_failed_min_one_success` | zadny nefailoval + alespon 1 uspel |
| `all_failed` | vsechny upstream failovaly |
| `one_failed` | alespon jeden upstream failoval |
