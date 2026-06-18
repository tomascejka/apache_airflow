# ANA-13: Idempotence ETL — jak zabranit duplicitam pri opakovani

## Kontext

V poc01 kazdy rerun DAGu prida duplicitni data do SQLite — load task pouziva `INSERT` bez kontroly. V produkci je toto neprijatelne: Airflow spousti tasky opakovane (retry, backfill, rerun) a kazde opakovani musi produkovat **stejny vysledek**.

Discovery zdroje: [DISC-09](DISC-09_idempotence_etl_patterns.md), [DISC-10](DISC-10_upsert_sql_syntaxe.md)

## Co je idempotence

```
Idempotentni operace:
  Spust 1x → vysledek A
  Spust 2x → vysledek A  (stejny!)
  Spust 5x → vysledek A  (porad stejny!)

NE-idempotentni operace (poc01):
  Spust 1x → 9 radku v DB
  Spust 2x → 18 radku v DB  (duplicity!)
  Spust 5x → 45 radku v DB  (katastrofa)
```

## Kdy Airflow spousti tasky opakovane

| Situace | Popis | Jak casto |
|---------|-------|-----------|
| **Retry** | Task selhal (sit, DB timeout) → Airflow spusti znovu | Bezne (konfigurovatelne: 1-5x) |
| **Rerun** | Operator manualne klikne "Clear" v UI | Obcas (debugging, oprava dat) |
| **Backfill** | Doplneni historickych dat (catchup=True) | Pri prvnim nasazeni |
| **Scheduler restart** | Scheduler padl, znovu naplanuje running tasky | Vzacne |

**Kazda z techto situaci muze zpusobit duplicity, pokud load neni idempotentni.**

## Strategie pro idempotentni load

### Strategie 1: UPSERT (DOPORUCENO pro nas use case)

**Princip**: pokud zaznam existuje (dle klice) → aktualizuj, pokud ne → vloz.

```python
# V load tasku (central worker)
@task(executor="celery")
def load_to_db(data):
    for row in data:
        db.execute("""
            INSERT INTO measurements (machine_id, device_id, timestamp, temperature, status)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (machine_id, device_id, timestamp)
            DO UPDATE SET temperature = EXCLUDED.temperature, status = EXCLUDED.status
        """, row)
```

**Natural key**: `(machine_id, device_id, timestamp)` — unikatne identifikuje kazde mereni.

**Vyhody:**
- Jednoduche — jedna zmena v load tasku
- Inkrementalni — zpracovava jen nova/zmenena data
- Bezpecne — retry nikdy nevytvori duplicity
- Zachovava historii — stare zaznamy zustavaji

**Nevyhody:**
- Potrebuje unikatni klic/index na cilove tabulce
- Pomalejsi nez cisty INSERT (kontrola konfliktu)

### Strategie 2: DELETE + INSERT (partition swap)

**Princip**: smaz vsechna data pro dany casovy interval → vloz cerstve.

```python
@task(executor="celery")
def load_to_db(data, logical_date):
    # Smaz data pro tento run
    db.execute("DELETE FROM measurements WHERE batch_date = ?", logical_date.date())
    # Vloz cerstve
    db.executemany("INSERT INTO measurements (...) VALUES (...)", data)
```

**Vyhody:**
- Jednoduche — zadny unikatni klic nutny
- Cisty stav — vzdy cerstve data pro dany partition

**Nevyhody:**
- Kratke okno bez dat (mezi DELETE a INSERT)
- Potrebuje partition key (batch_date)
- Pokud INSERT selze po DELETE → ztrata dat (reseni: transakce)

### Strategie 3: TRUNCATE + INSERT (full refresh)

**Princip**: smaz vse → vloz vse znovu.

```python
@task(executor="celery")
def load_to_db(all_data):
    db.execute("TRUNCATE TABLE measurements")
    db.executemany("INSERT INTO measurements (...) VALUES (...)", all_data)
```

**Vyhody:**
- Nejjednodussi — zadna logika
- Vzdy konzistentni stav

**Nevyhody:**
- Drahe pro velke tabulky (vse smazat a znovu vlozit)
- Kratke okno bez dat
- Task musi mit VSECHNA data (ne jen novy batch)

### Strategie 4: Append + dedup at read

**Princip**: vzdy jen pridavej, duplicity resit az pri cteni.

```sql
-- Pri cteni (view nebo CTE)
SELECT DISTINCT ON (machine_id, device_id, timestamp) *
FROM measurements
ORDER BY machine_id, device_id, timestamp, loaded_at DESC;
```

**Vyhody:**
- Nejrychlejsi zapis (cisty INSERT)
- Zadna ztrata dat (vsechno je v DB)
- Audit trail (vidis vsechny verze)

**Nevyhody:**
- Rostouci tabulka (duplicity zustavaji)
- Kazdy dotaz musi deduplicovat (pomalejsi cteni)
- Slozitejsi pro konzumenty dat

## Srovnani

| Kriterium | UPSERT | DELETE+INSERT | TRUNCATE | Append+dedup |
|-----------|--------|---------------|----------|--------------|
| Slozitost implementace | Nizka | Nizka | Nejnizsi | Stredni |
| Potrebuje unikatni klic | **Ano** | Ne (partition key) | Ne | Ne |
| Rychlost zapisu | Stredni | Rychla | Rychla | Nejrychlejsi |
| Rychlost cteni | Rychla | Rychla | Rychla | Pomalejsi |
| Bezpecnost (retry) | Vysoka | Stredni* | Stredni* | Vysoka |
| Rust tabulky | Minimalni | Minimalni | Minimalni | Vysoka |

*DELETE+INSERT a TRUNCATE musi byt v transakci, jinak hrozba ztrata dat.

## Doporuceni pro nas use case

### UPSERT jako default

**Proc:**
1. Kazde mereni ma prirozeny klic: `(machine_id, device_id, timestamp)`
2. Retry/rerun je bezpecny — stejny zaznam se prepise, ne zduplikuje
3. Inkrementalni — zpracovavame jen novy batch, ne vsechna data
4. Nezavisly na partition scheme

### Co je treba zmenit oproti poc01

```
poc01 (soucasny):
  INSERT INTO measurements VALUES (...)  ← duplicity pri rerun

Produkce:
  INSERT INTO measurements VALUES (...)
  ON CONFLICT (machine_id, device_id, timestamp)
  DO UPDATE SET temperature = EXCLUDED.temperature, status = EXCLUDED.status
```

**Zmena = 2 radky SQL v load tasku + UNIQUE INDEX na cilove tabulce.**

### Dalsi principy

1. **Pouzivej `logical_date`** z Airflow contextu — ne `datetime.now()`
   ```python
   @task
   def load(data, **context):
       batch_date = context["logical_date"]  # stejne pri retry/rerun
   ```

2. **Pridej `loaded_at` sloupec** — pro audit trail (kdy byl zaznam nacten)

3. **UNIQUE INDEX** na cilove tabulce:
   ```sql
   CREATE UNIQUE INDEX idx_measurements_key
   ON measurements (machine_id, device_id, timestamp);
   ```

## Otevrene otazky

| # | Otazka | Dopad |
|---|--------|-------|
| 1 | Ma kazde mereni prirozeny unikatni klic (machine_id + timestamp)? | Pokud ne, musime generovat (hash of content) |
| 2 | Mohou se data zpetne menit (late-arriving data)? | Pokud ano, UPSERT musi aktualizovat i stare zaznamy |
| 3 | Jak dlouho uchovavat data v cilove DB? | Lifecycle policy, archivace starych dat |
| 4 | Muze se zmenit schema mereni (nove sloupce)? | Schema migration strategie |

## Souvisejici analyzy

- [ANA-05](ANA-05_edge_etl_flow_design.md) — ETL flow design (Varianta B)
- [ANA-05a](ANA-05a_tasky_operace_airflow.md) — tasky vs operace, retry hranice
- [ANA-12](ANA-12_nahrada_xcom_produkce.md) — prestupni uloziste (kde data prochazi pred loadem)
