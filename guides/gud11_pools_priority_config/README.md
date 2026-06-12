# gud11: Pools, Priority a Config Tuning

## Popis

Rizeni soubehu tasku (pools, priority) a ladeni konfigurace Airflow (parallelism, max_active_tasks). Klicove pro produkci — spravne nastaveni brani pretizeni systemu.

## Cile

- Pool machine_pool (2 sloty) omezuje pocet soucasne bezicich tasku
- Vsech 6 tasku v poolu dobehne uspesne (i kdyz max 2 soucasne)
- priority_weight urcuje poradi spousteni (vyssi cislo = vyssi priorita)
- max_active_tasks=3 na DAG urovni omezuje soubehu (8 tasku, max 3 najednou)
- Pool existuje v systemu (`airflow pools list` vraci machine_pool s 2 sloty)

## Koncepty

| Koncept | Popis |
|---------|-------|
| **Pool** | Sdileny limit na pocet soucasne bezicich tasku |
| **priority_weight** | Vyssi cislo = vyssi priorita (spusti se driv) |
| **weight_rule** | Jak se pocita efektivni priorita (downstream/upstream/absolute) |
| **parallelism** | Globalni limit tasku across ALL DAGs |
| **max_active_tasks** | Max soucasnych tasku jednoho DAGu |
| **max_active_runs** | Max soucasnych runu jednoho DAGu |

## DAGy

### pools_demo
- Pool `machine_pool` s **2 sloty** (vytvoren v docker-compose airflow-init)
- 6 tasku ve stejnem poolu → max 2 bezi soucasne
- V UI videt frontu: 2 running, 4 queued/scheduled

### priority_demo
- 4 tasky s ruznou prioritou (10, 5, 1, 0) ve stejnem poolu
- Scheduler spusti CRITICAL (10) a HIGH (5) prvni
- Pool omezuje na 2 soucasne → videt poradi dle priority

### config_tuning
- 8 nezavislych tasku, `max_active_tasks=3`
- Max 3 bezi soucasne, ostatnich 5 ceka
- Demonstrace DAG-level concurrency control

## Overeni

```bash
# Spustit stack
.\run.ps1

# Pools demo — sledovat frontu v UI
docker compose exec airflow-scheduler airflow dags trigger pools_demo

# Priority demo — sledovat poradi spousteni
docker compose exec airflow-scheduler airflow dags trigger priority_demo

# Config tuning — sledovat max 3 soucasne
docker compose exec airflow-scheduler airflow dags trigger config_tuning

# Kontrola poolu
docker compose exec airflow-scheduler airflow pools list
```

**V UI (localhost:8080):**
- pools_demo: v Grid view videt 2 zelene (running) + 4 sede (queued)
- priority_demo: CRITICAL a HIGH se spusti prvni
- config_tuning: max 3 zelene soucasne, ostatni cekaji
- Admin → Pools: videt machine_pool s 2 sloty a obsazenost

## Hierarchie concurrency limitu

```
AIRFLOW__CORE__PARALLELISM (globalni, napr. 32)
  └── Pool slots (napr. machine_pool: 2)
       └── DAG max_active_tasks (napr. 3)
            └── Task instance
```

Plati nejstriknejsi limit. Priklad:
- parallelism=32, pool=2 sloty, max_active_tasks=8
- Efektivni limit = 2 (pool je nejstriknejsi)

## Konfiguracni parametry — prehled

| Parametr | Scope | Default | Popis |
|----------|-------|---------|-------|
| `parallelism` | Globalni | 32 | Max tasku across ALL DAGs |
| `max_active_tasks_per_dag` | Globalni | 16 | Max tasku jednoho DAGu (global default) |
| `max_active_runs_per_dag` | Globalni | 16 | Max runu jednoho DAGu (global default) |
| `max_active_tasks` | DAG | - | Override max_active_tasks_per_dag pro konkretni DAG |
| `max_active_runs` | DAG | - | Override max_active_runs_per_dag pro konkretni DAG |
| `pool` | Task | "default_pool" | Prirazeni do poolu |
| `priority_weight` | Task | 1 | Priorita (vyssi = driv) |
