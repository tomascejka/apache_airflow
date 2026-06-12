# gud09: TaskGroups a Dynamic Task Mapping

## Popis

Organizace slozitych workflow pomoci TaskGroup (vizualni seskupeni) a dynamicke generovani tasku za behu pomoci .expand() (dynamic task mapping).

## Cile

- TaskGroup seskupi tasky vizualne v UI (task_id prefixes: extract., transform., load.)
- Nested TaskGroup funguje (transform.validate.validate_schema)
- expand() vytvori dynamicky pocet tasku za behu (5 instanci pro 5 stroju)
- Mapped tasky maji indexy [0]-[N] a kazdy zpracuje jeden vstup
- Reduce pattern (aggregate_results) sebere vysledky ze vsech mapped tasku
- Deterministicke vysledky: 881 celkem radku, 835 validnich, 46 nevalidnich

## Koncepty

| Koncept | Popis |
|---------|-------|
| **TaskGroup** | Vizualni seskupeni tasku v UI Graph view |
| **Nested TaskGroup** | Vnorene skupiny (skupina ve skupine) |
| **expand()** | Dynamicky vytvori N instanci tasku za behu |
| **Mapped Task** | Task s indexy [0], [1], [2]... — kazdy zpracuje jeden vstup |
| **Map + Reduce** | expand() pro paralelni zpracovani → agregace vysledku |

## DAGy

### taskgroup_demo
- ETL pipeline: Extract (3 zdroje) → Transform (validate+clean+enrich) → Load (DB+file)
- **Nested TaskGroup**: validate je skupina uvnitr transform
- V UI Graph view: rozkliknutelne skupiny
- 10 tasku v prehledne hierarchii

### dynamic_mapping_demo
- **expand()** — dynamicky pocet tasku (5 stroju)
- `get_machines` vrati seznam → `process_machine` se vytvori 5x
- V UI videt "mapped task" s indexy [0]...[4]
- Pocet tasku neni hardcoded — meni se dle dat

### mapped_reduce_demo
- **Map + Reduce** pattern
- `list_files` → `process_file` (4 instance) → `aggregate_results`
- Reduce sbirá vysledky ze vsech mapped tasku
- Realny priklad: zpracovani davky souboru

## Overeni

```bash
# Spustit stack
.\run.ps1

# TaskGroup
docker compose exec airflow-scheduler airflow dags trigger taskgroup_demo

# Dynamic mapping
docker compose exec airflow-scheduler airflow dags trigger dynamic_mapping_demo

# Map + Reduce
docker compose exec airflow-scheduler airflow dags trigger mapped_reduce_demo
```

**V UI (localhost:8080):**
- taskgroup_demo: Graph view — rozkliknout skupiny extract/transform/load
- dynamic_mapping_demo: videt mapped task s indexy [0]-[4], kazdy s vlastnim logem
- mapped_reduce_demo: aggregate_results obsahuje souhrn ze vsech souboru
