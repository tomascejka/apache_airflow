# DISC-03: Dagster — detailni pohled

## Zdroje

- [Dagster.io](https://dagster.io/)
- [Dagster vs Prefect (Dagster)](https://dagster.io/vs/dagster-vs-prefect)
- [Dagster vs Prefect: Orchestration Compared 2026 (Modern DataTools)](https://www.modern-datatools.com/compare/dagster-vs-prefect)
- [Airflow vs Dagster vs Prefect 2026 (DataVidhya)](https://datavidhya.com/blog/airflow-vs-dagster-vs-prefect/)
- [Dagster GitHub](https://github.com/dagster-io/dagster)

## Relevance: STREDNI

## Klicove poznatky

### Architektura
- **Asset-centric**: Software-Defined Assets — definujes co chces vyrobit (data asset), ne jak to spustit
- IO Managers resi cteni/zapis — oddeleni logiky od storage
- Code locations — modularni deployment, kazdy tym ma svuj "location"
- Asset graph = vizualizace datovych zavislosti (ne task zavislosti)

### Silne stranky
- Nejlepsi developer experience pro greenfield projekty
- Hluboké integrace s dbt, Airbyte, Snowflake (modern data stack)
- Declarativni programovaci model — testovatelnost, type checking
- Components GA (Oct 2025) — znovupouzitelne stavebni bloky
- Pay-as-you-go pricing (May 2026)

### Dagster+ (cloud)
- Serverless compute: $0.010/min + kredity
- Hybrid: vlastni infrastruktura (ECS, K8s, Docker), zadny compute charge
- Control plane v cloudu, compute u zakaznika
- RBAC, audit logs, SSO

### Self-hosted
- Plne open-source, self-hosted na Kubernetes funguje dobre
- Vyzaduje vetsi operacni investici nez Prefect
- Dokumentace pro self-hosted je kvalitni

### Distributed execution
- Code locations mohou bezet na ruznych strojich/kontejnerech
- Dagster nema koncept "edge worker" — distribuovane behy jsou pres K8s/ECS
- Pro remote execution: pouziva se Kubernetes operator nebo ECS launcher
- **Zadny ekvivalent Airflow Edge Worker** pro IoT/edge scenare

### Slabiny pro nas use case
- Mensi ekosystem a komunita nez Airflow
- Asset-centric model je overhead pro jednoduchy ETL (task-centric staci)
- Strmejsi krivka uceni (IO managers, resources, asset graph)
- Zadny edge/IoT koncept
- Modern data stack focus — automotive ETL neni typicky use case
