# ANA-02: Tutorial DAG - Analyza

## Soubor
`dags/tutorial.py`

## Struktura DAGu `my_tutorial`

### DAG konfigurace
- `dag_id`: `my_tutorial` (prejmenovano z `tutorial` kvuli kolizi s example DAGs)
- `schedule`: kazdy den (`timedelta(days=1)`)
- `start_date`: 2021-01-01
- `catchup`: False (nespousti zpetne DAG runy pro minule datumy)
- `tags`: `["example"]`

### Default args (sdilene pro vsechny tasky)
- `depends_on_past`: False
- `retries`: 1
- `retry_delay`: 5 minut

### Tasky

| Task ID | Typ | Popis | Specificke args |
|---------|-----|-------|-----------------|
| `print_date` (t1) | BashOperator | Spusti `date` | - |
| `sleep` (t2) | BashOperator | Spusti `sleep 5` | `retries=3` (override default) |
| `templated` (t3) | BashOperator | Jinja sablona s `ds` makry | `depends_on_past=False` |

### Dependencies (tok dat)
```
t1 >> [t2, t3]

t1 (print_date)
    |
    +---> t2 (sleep)
    |
    +---> t3 (templated)
```

t1 bezi prvni, po jeho uspesnem dokonceni se paralelne spusti t2 a t3.

## Jinja Templating

Task `templated` demonstruje pouziti Jinja sablon v `bash_command`:
- `{{ ds }}` - execution date ve formatu YYYY-MM-DD
- `{{ macros.ds_add(ds, 7) }}` - execution date + 7 dni
- `{% for i in range(5) %}` - Jinja for loop

## Dokumentace v DAGu

- `dag.doc_md` - markdown dokumentace celeho DAGu (viditelna v UI)
- `t1.doc_md` - markdown dokumentace konkretniho tasku (viditelna v Task Instance Details)

## Vysledky spusteni

Oba runy (scheduled i manual) dokonceny se stavem `success`.
