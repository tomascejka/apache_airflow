# DISC-04: Edge/distributed execution — srovnani orchestratoru

## Zdroje

- [Comparing Workflow Architectures: Prefect vs Dagster vs Airflow (ThinhDA)](https://thinhdanggroup.github.io/airflow-prefect-dagster/)
- [Airflow, Dagster, or Prefect: Which Scheduler Fits Your Team (DZone)](https://dzone.com/articles/airflow-vs-dagster-vs-prefect-which-scheduler-fits)
- [Orchestration Showdown: Dagster vs Prefect vs Airflow (ZenML)](https://www.zenml.io/blog/orchestration-showdown-dagster-vs-prefect-vs-airflow)
- [Airflow vs Prefect vs Dagster 2026 (DataStackX)](https://datastackx.com/insights/airflow-vs-prefect-vs-dagster/)
- [Data Pipeline Orchestration 2026 (Reintech)](https://reintech.io/blog/data-pipeline-orchestration-airflow-dagster-prefect-2026)

## Relevance: VYSOKA

## Klicove poznatky

### Distribuovana architektura — srovnani

| Aspekt | Airflow | Prefect | Dagster |
|--------|---------|---------|---------|
| Edge/remote worker | **Edge Worker (edge3 provider)** — dedikovan agent na remote stroji | Zadny ekvivalent | Zadny ekvivalent |
| Distribuovane behy | CeleryExecutor, KubernetesExecutor, Edge Worker | Workers + Dask/Ray task runners | K8s operator, ECS launcher |
| Hybridni model | Centrala + Edge Workers (nativni) | Cloud control plane + agenti | Cloud control plane + code locations |
| Self-hosted distributed | Plne podporovano (Celery + Edge) | Mozne, ale cloud-first design | Mozne pres K8s, vetsi operacni narocnost |

### Edge Worker — unikatni vyhoda Airflow
- Airflow jako jediny ma **nativni edge worker koncept** (apache-airflow-providers-edge3)
- Edge Worker bezi na remote stroji (linka, tovarna), pripoji se k centrale pres HTTP
- Scheduler prirazuje tasky edge workerovi, ten je spusti lokalne
- **Zadny jiny orchestrator to nenabizi** — Prefect a Dagster predpokladaji cloud/K8s prostredi

### Operacni narocnost

| | Airflow | Prefect | Dagster |
|---|---------|---------|---------|
| Komponenty | scheduler, worker, webserver, DB, (optional: Celery, Redis) | server/cloud, agent, DB | dagster-daemon, dagster-webserver, DB |
| Slozitost | Vysoka (nejvic komponent) | Nizka (zejm. s Cloud) | Stredni |
| Platform team? | Doporuceno | Neni treba | Doporuceno |

### Kdo je pro koho
- **Airflow**: platform team, 100+ pipeline, battle-tested, edge/distributed
- **Prefect**: nejrychlejsi cesta od Python skriptu k produkci, male tymy
- **Dagster**: moderni data platforma od nuly, dbt-heavy tymy
