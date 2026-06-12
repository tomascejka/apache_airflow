# ANA-01: Srovnani orchestratoru — Airflow vs Prefect vs Dagster

Analyza zpracovana z DISC-01 az DISC-04.

## Prehled

Tri hlavni Python-based orchestratory pro batch workflow v 2026:

| | Airflow | Prefect | Dagster |
|---|---------|---------|---------|
| **Paradigma** | Task-centric (DAG = graf tasku) | Flow-centric (Python funkce s dekoratory) | Asset-centric (Software-Defined Assets) |
| **Verze (2026)** | 3.2 (Apr 2026) | 3.7 (May 2026) | Components GA + pay-as-you-go |
| **Licence** | Apache 2.0 | Apache 2.0 (Prefect) / komerci (Cloud) | Apache 2.0 (Dagster) / komerci (Dagster+) |
| **Zalozeni** | 2014 (Airbnb) | 2018 (Prefect Technologies) | 2019 (Elementl) |

## Architekturni rozdily

### Airflow — task-centric
- DAG = orientovany acyklicky graf **tasku**
- Kazdy task = operator (BashOperator, PythonOperator, ...)
- Scheduler cyklicky vyhodnocuje DAGy a prirazuje tasky workerum
- Data se predavaji pres XCom (male) nebo externi storage (velke)
- Silny v orchestraci: "spust A, pak B, pak C"

### Prefect — flow-centric
- `@flow` a `@task` dekoratory na Python funkcich
- Nulovy boilerplate — existujici Python skript se stane workflow
- Nativni Python: tasky vraci hodnoty, flow je predava dal
- Transakcni semantika (Prefect 3.0): atomicke skupiny tasku
- Silny v jednoduchosti: "zmen Python skript na scheduled workflow"

### Dagster — asset-centric
- Software-Defined Assets = definujes **co** chces vyrobit (tabulka, soubor, model)
- IO Managers resi jak se data ctou/zapisuji
- Asset graph = vizualizace datovych zavislosti
- Silny v data platformach: "mam tyto data assets a jejich zavislosti"

## Srovnani pro nas use case (automotive batch ETL)

### Pozadavky
1. Batch ETL ze stroju na vyrobni lince (CSV, JSON)
2. Edge execution — tasky bezi na remote stroji (linka)
3. Centralni orchestrace — scheduler v serverovne
4. Monitoring a alerting
5. Self-hosted (on-premise, zadny cloud)
6. Tym s Python znalostmi

### Hodnoceni

| Kriterium | Airflow | Prefect | Dagster |
|-----------|---------|---------|---------|
| Edge Worker | **Nativni (edge3 provider)** | Neni | Neni |
| Self-hosted | Plne podporovano | Mozne, ale cloud-first | Mozne, vetsi narocnost |
| Batch ETL | Idealni (puvodni ucel) | Mozne | Mozne (asset overhead) |
| Ekosystem | **Nejvetsi** (1000+ provideru) | Stredni | Mensi |
| Komunita | **Nejvetsi** (40k+ GitHub stars) | Stredni (18k+) | Stredni (12k+) |
| Hiring pool | **Nejvetsi** | Maly | Maly |
| Operacni slozitost | Vysoka (hodne komponent) | Nizka | Stredni |
| Krivka uceni | Stredni | Nizka | Vysoka (IO managers, assets) |
| Monitoring | StatsD/Prometheus + REST API | Cloud dashboardy | Dagster UI (asset health) |
| Alerting | Externi (Prometheus/Zabbix) | Cloud built-in | Dagster+ alerts |
| Dokumentace | **Vynikajici** (rozsahla, mature) | Dobra | Dobra |

### Klicovy diferenciator: Edge Worker

**Airflow je jediny orchestrator s nativnim edge worker konceptem.**

- `apache-airflow-providers-edge3`: agent bezi na remote stroji, pripoji se k centrale pres HTTP
- Scheduler prirazuje tasky edge workerovi, ten je spusti lokalne
- Overeno v poc02 a poc03 — funguje pro automotive ETL
- Prefect a Dagster predpokladaji cloud/K8s prostredi — nemaji koncept "edge agenta"

Pro nas use case (tovarna, vyrobni linka, on-premise) je to **rozhodujici vyhoda**.

## Deployment model

| | Airflow | Prefect | Dagster |
|---|---------|---------|---------|
| **Full self-hosted** | Ano (Docker Compose, K8s, standalone) | Ano (Prefect server OSS) | Ano (K8s, Docker) |
| **Cloud managed** | Astronomer, MWAA, Cloud Composer | Prefect Cloud | Dagster+ |
| **Hybrid** | Centrala + Edge Workers | Cloud control + agenti | Cloud control + code locations |
| **Air-gapped** | Plne podporovano | Mozne, ale omezene | Mozne |

## Doporuceni

### Pro nas use case: **Airflow**

Duvody:
1. **Edge Worker** — jediny orchestrator s nativni podporou remote execution na lince
2. **Self-hosted** — plne podporovano, zadna zavislost na cloudu
3. **Battle-tested** — 10+ let v produkci, nejvetsi komunita
4. **Batch ETL** — puvodni a hlavni ucel Airflow
5. **Ekosystem** — 1000+ provideru, snadna integrace
6. **Hiring** — nejvic lidi zna Airflow

### Kdy zvazit alternativy

- **Prefect**: pokud je on-premise pozadavek zrusen, tym je maly (1-3 lide), a nepotrebujeme edge execution
- **Dagster**: pokud jdeme greenfield s modern data stack (dbt, Snowflake), neni edge pozadavek, a mame data engineering tym

### Kdy NEmigovat

Pokud uz mate 50+ produkcnich DAGu na Airflow a funguji — **zustante, nemigrovejte**. Migrace je draha a riskantni. Toto potvrzuje vice nezavislych zdroju.

## Zdroje

- [DISC-01: Srovnani orchestratoru 2026](DISC-01_srovnani_orchestratoru_2026.md)
- [DISC-02: Prefect 3 detail](DISC-02_prefect_3_detail.md)
- [DISC-03: Dagster detail](DISC-03_dagster_detail.md)
- [DISC-04: Edge/distributed srovnani](DISC-04_edge_distributed_srovnani.md)
