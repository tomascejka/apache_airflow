# DISC-02: Prefect 3 — detailni pohled

## Zdroje

- [What's new in Prefect 3.0 (docs)](https://docs.prefect.io/v3/get-started/whats-new-prefect-3)
- [Introducing Prefect 3.0 (blog)](https://www.prefect.io/blog/introducing-prefect-3-0)
- [Dagster vs Prefect: Self-Serve Plans Compared](https://www.prefect.io/blog/dagster-vs-prefect-self-serve-plans-compared)
- [Prefect vs Dagster (Prefect)](https://www.prefect.io/compare/dagster)
- [Airflow vs Prefect (Alps Agility)](https://www.alpsagility.com/orchestrating-with-airflow-vs-prefect)

## Relevance: STREDNI

## Klicove poznatky

### Architektura
- **Flow-centric**: Python funkce s `@flow` a `@task` dekoratory
- Nulovy boilerplate — jakakoli Python funkce se stane workflow
- Hybridni model: Prefect Cloud = control plane, compute bezi ve vasi infrastrukture
- Plne open-source self-hosted server (Prefect 2 OSS), ale nejlepsi experience je na Prefect Cloud

### Prefect 3.0 novinky
- Events a automation system v open-source
- Transakcni semantika (atomicke skupiny tasku, rollback pri selhani)
- Workers — silnejsi governance model pro infrastrukturu
- Podpora behu kdekoli: laptop, lambda, legacy orchestrator
- Multi-modalni: batch, event-driven, interactive, human-in-the-loop, background tasks

### Distributed execution
- Workers + task runners (Dask, Ray) pro distribuovane zpracovani
- Task muze bezet kdekoli — lokalne, Kubernetes, cloud
- Hybridni model = agent ve vasi infrastrukture, Prefect ridi control plane
- **Zadny ekvivalent Airflow Edge Worker** — Prefect nema koncept "edge agenta" ktery by bezel na remote stroji a sbiral data

### Self-hosted
- Prefect server (OSS) je plne funkcni, ale chybi nektere enterprise features (RBAC, audit logs)
- Pro air-gapped deployment je treba pecilve evaluovat
- Operacne jednodussi nez Airflow (mene komponent)

### Slabiny pro nas use case
- Mensi ekosystem nez Airflow
- Mensi hiring pool
- Zadny edge worker koncept pro IoT/manufacturing
- Cloud-first mindset — self-hosted je "druhorada" moznost
