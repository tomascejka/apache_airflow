# DISC-01: Srovnani orchestratoru 2026

## Zdroje

- [Orchestration Showdown: Dagster vs Prefect vs Airflow (ZenML)](https://www.zenml.io/blog/orchestration-showdown-dagster-vs-prefect-vs-airflow)
- [Airflow vs Dagster vs Prefect 2026 (DataVidhya)](https://datavidhya.com/blog/airflow-vs-dagster-vs-prefect/)
- [Airflow vs Prefect vs Dagster vs Flyte: 2026 (UK Data Services)](https://ukdataservices.co.uk/blog/articles/python-data-pipeline-tools-2025)
- [Complete Data Orchestration Comparison 2026 (Orchestra)](https://www.getorchestra.io/blog/dagster-vs-prefect-vs-airflow-complete-data-orchestration-comparison-2026)
- [Which Orchestrator Wins in 2026 (Andrei Nita)](https://andreinita.co/blog/airflow-vs-prefect-vs-dagster/)
- [Decoding Data Orchestration Tools (FreeAgent)](https://engineering.freeagent.com/2025/05/29/decoding-data-orchestration-tools-comparing-prefect-dagster-airflow-and-mage/)

## Relevance: VYSOKA

## Klicove poznatky

### Stav v 2026
- Airflow 3.2 (Apr 2026): asset partitioning, multi-team deployments, multi-language Task SDK
- Dagster: Components GA (Oct 2025), pay-as-you-go pricing (May 2026)
- Prefect 3.7 (May 2026): enterprise audit trails, Marvin 3.0 agent framework

### Doporuceni z vice zdroju
- **Airflow**: doporuceny jako default pro vetsinu data engineering tymu — nejvetsi ekosystem, nejvice battle-tested, nejvetsi hiring pool
- **Dagster**: doporuceny pro greenfield projekty, dbt-heavy tymy, asset-centric pristup
- **Prefect**: doporuceny pro jednoduchost, male tymy, ML/data science workflows
- **Migrace**: pokud mas 50+ produkcnich DAGu na Airflow a funguji — zustanit, nemigovat

### Architekturni rozdily
- Airflow: task-centric (DAG = graf tasku)
- Dagster: asset-centric (Software-Defined Assets = co chci vyrobit)
- Prefect: flow-centric (Python funkce s dekoratory, dynamicke)
