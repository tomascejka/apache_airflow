# ANA-19: Kdo udrzuje DAGy — role a odpovednosti

## Kontext

Code-first pristup (KAD-01) znamena, ze DAGy jsou Python kod. Kdo je pise, kdo je udrzuje, kdo resi problemy? Jak rozdelit odpovednost v tymu?

## Dva typy prace s Airflow

| Oblast | Co to obnasi | Kdo typicky |
|--------|-------------|-------------|
| **Platforma** (infra) | Instalace, upgrade, monitoring, backup, skalovani, security | DevOps / SysAdmin |
| **DAGy** (business logika) | Psani ETL kodu, schema kontrakty, debugging tasku, nove stroje | Data engineer / Vyvojar |

### Proc je to dulezite rozlisovat

- **Platforma** se meni zridka (upgrade Airflow, nova verze Dockeru, zmena konfigurace)
- **DAGy** se meni casto (novy stroj, zmena formatu dat, novy report)
- Ruzne skillsety: infra = Linux/Docker/networking, DAGy = Python/SQL/data

## Modely vlastnictvi

### Model A: Jeden clovek dela vse (MALY TYM)

```
1 clovek = infra + DAGy + monitoring + troubleshooting
```

**Kdy**: 1–3 lidi v tymu, startup/PoC faze, desitky DAGu.

**Vyhody**: rychle rozhodovani, zadna koordinace
**Nevyhody**: bus factor = 1, pretizeni

**Pro nas use case**: pravdepodobne toto na zacatku.

### Model B: Rozdeleni infra vs DAGy (STREDNI TYM)

```
DevOps (1-2 lidi):     Airflow cluster, Docker, monitoring, security, upgrade
Data engineer (1-3):   DAGy, schema kontrakty, data quality, nove stroje
```

**Kdy**: 4–10 lidi, desitky DAGu, stabilni produkce.

**Vyhody**: specializace, jasne odpovednosti
**Nevyhody**: koordinace pri zmene (novy provider = infra + DAG zmena)

### Model C: Platform team + domena (VELKY TYM)

```
Platform team:          Airflow jako sluzba, self-service sablony, CI/CD
Domenove tymy:          Kazdy tym vlastni sve DAGy, pouziva sablony
```

**Kdy**: 10+ lidi, stovky DAGu, vice domenovych tymu.

**Neni relevantni pro nas** — uvadime pro uplnost.

## Pozadavky na znalosti

### Minimalni Python pro DAGy

DAGy v TaskFlow API nevyzaduji pokrocily Python. Staci:

| Znalost | Priklad v DAGu |
|---------|----------------|
| Funkce + dekorator | `@task def extract():` |
| Slovniky, seznamy | `{"machine_id": "stroj_1", "temperature": 42.5}` |
| Zakladni I/O | `open("file.csv")`, `csv.reader()` |
| SQL zaklady | `INSERT INTO ... ON CONFLICT DO UPDATE` |

**Netrivialni casti** (resi se jednou, pak se kopiruje):
- DAG scheduling (`schedule="@hourly"`)
- Executor routing (`queue="edge_worker"`)
- XCom / data passing mezi tasky

### Sablony pro novy stroj

V nasem use case pridani noveho stroje znamena:
1. Zkopirovat existujici `extract_transform_stroj_N` funkci
2. Upravit parsovani (jiny format CSV/JSON)
3. Otestovat lokalne
4. Commitnout a deploynout

```python
# Sablona — novy stroj je zmena jedne funkce
@task(queue="edge_worker")
def extract_transform_stroj_N(**context):
    # 1. Nacti data z lokalniho stroje
    raw = read_machine_data("/data/stroj_N/")
    # 2. Normalizuj do standardniho formatu
    return [normalize(row) for row in raw]
```

Toto zvladne i junior vyvojar s Python zaklady.

## Dokumentace a onboarding

### Co dokumentovat

| Dokument | Kde | Pro koho |
|----------|-----|----------|
| Architektura (jak to funguje) | README.md + ANA-* | Vsichni |
| Jak pridat novy stroj | CONTRIBUTING.md | DAG autori |
| Jak spustit lokalne | run.ps1 + README | DAG autori |
| Jak upgradovat Airflow | ops/UPGRADE.md | DevOps |
| Troubleshooting | ANA-01_troubleshooting.md | Vsichni |

### Onboarding noveho cloveka

1. Precist README.md (co to je, proc, architektura)
2. Spustit `run.ps1` lokalne (videt jak to funguje)
3. Upravit existujici DAG (zmenit transform funkci)
4. Pridat novy stroj (kopie + uprava)
5. Projit CI pipeline (lint + test)

## Doporuceni

### Hned (maly tym, start)

1. **Model A** — jeden/dva lidi delaji vse
2. **Dokumentace**: README + CONTRIBUTING.md + troubleshooting
3. **Sablony**: template pro novy stroj, aby pridani bylo jednoduche
4. **Code review**: i v malem tymu — druhe oci na DAG zmenach

### Pozdeji (rust tymu)

1. **Model B** — oddelit infra od DAGu
2. **CI/CD** (ANA-17): automaticke testy pred merge
3. **Airflow RBAC**: role (Admin, Viewer, DAG author) pro pristup k UI
4. **Tagy na DAGech**: `tags=["linka_1", "stroj_1"]` pro filtraci v UI

### Klicove pravidlo

> **Novy stroj = zmena jen na edge strane.** Central load task se nemeni (ANA-05).
> To znamena, ze pridani stroje nevyzaduje znalost cele architektury — staci Python zaklady a znalost formatu dat.

## Souvisejici analyzy

- [KAD-01](KAD-01_code_first_orchestrace.md) — Code-first pristup (Python znalost nutna)
- [ANA-05](ANA-05_edge_etl_flow_design.md) — Novy stroj = novy edge kod, central se nemeni
- [ANA-17](ANA-17_cicd_dag_deployment.md) — CI/CD pipeline pro DAGy
