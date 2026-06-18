# DISC-09: Idempotence ETL — patterny a best practices

## Zdroje

- https://dataskew.io/blog/data-pipeline-design-patterns/ — 8 design patternu (2026)
- https://medium.com/towards-data-engineering/building-idempotent-data-pipelines-a-practical-guide-to-reliability-at-scale-2afc1dcb7251 — Practical guide
- https://medium.com/@chanon.krittapholchai/apache-airflow-useful-practices-idempotent-dag-6d52b1594704 — Airflow idempotent DAG
- https://towardsdatascience.com/how-to-design-better-dags-in-apache-airflow-494f5cb0c9ab/ — Better DAGs design
- https://airbyte.com/data-engineering-resources/idempotency-in-data-pipelines — Idempotency overview

## Relevance: VYSOKA (kriticka pro produkci)

## Souhrn

Idempotentni pipeline = spusteni se stejnym vstupem produkuje vzdy stejny vysledek, bez ohledu na pocet spusteni. Airflow sam o sobe duplicity neresi — idempotence musi byt v kodu tasku.

## Klicove poznatky

### Proc je idempotence kriticka

Airflow spousti tasky opakovane pri:
- **Retry** — task selhal, Airflow ho spusti znovu (napr. 3x)
- **Backfill** — doplneni historickych dat (catchup=True)
- **Rerun** — manualni opakovanove spusteni z UI
- **Scheduler restart** — scheduler padne a znovu naplanuje running tasky

Bez idempotence: kazdy retry/rerun = duplicitni data v DB.

### 5 strategii pro idempotentni load

1. **UPSERT (INSERT ON CONFLICT)**
   - Pokud zaznam existuje → UPDATE, pokud ne → INSERT
   - Potrebuje unikatni klic (natural key)
   - Nejcastejsi pristup pro inkrementalni load

2. **DELETE + INSERT (partition swap)**
   - Smaz vsechna data pro dany partition (napr. datum) → vloz cerstve
   - Jednoduche, ale vyzaduje jasny partition key
   - Vhodne kdyz reprocessujes cely den/hodinu

3. **TRUNCATE + INSERT (full refresh)**
   - Smaz vsechno → vloz vse znovu
   - Nejjednodussi, ale drahe pro velke tabulky
   - Vhodne pro male lookup tabulky

4. **MERGE (SQL MERGE/UPSERT)**
   - Kombinace INSERT + UPDATE + DELETE v jednom prikazu
   - PostgreSQL 15+ podporuje MERGE
   - Vhodne pro komplexni CDC scenary

5. **Append + dedup at read**
   - Vzdy append (nikdy neupravuj), deduplikace az pri cteni
   - Window funkce: ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts DESC)
   - Vhodne pro data lake / analytiku

### Klicove principy Airflow

- **Pouzivej logical_date** (ne CURRENT_TIMESTAMP) — rerun zpracuje stejna data
- **Pis do specifickych partici** — ne "vsechno do jedne tabulky"
- **Kazdy zaznam musi mit natural key** — (machine_id, device_id, timestamp)
- **UPSERT misto INSERT** — zamez duplicitam na urovni DB
