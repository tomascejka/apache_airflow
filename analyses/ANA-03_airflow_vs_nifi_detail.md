# ANA-03: Airflow vs NiFi - detailni srovnani

Overview viz [ANA-01](ANA-01_poc_airflow_vs_nifi.md).

## Airflow - pokryti vsech vstupne-vystupnich formatu

| Vstup/Vystup | Airflow operator/hook | Provider |
|---|---|---|
| Database (SQL) | `SqlSensor`, `SqlExecuteQueryOperator` | `apache-airflow-providers-common-sql` |
| CSV/JSON/XML soubory | `PythonOperator` + pandas/lxml | built-in |
| REST API | `HttpOperator`, `HttpSensor` | `apache-airflow-providers-http` |
| REST API s auth | `HttpHook` (bearer, basic, custom) | `apache-airflow-providers-http` |
| Filesystem | `BashOperator`, `PythonOperator` | built-in |
| S3/GCS/Azure | Dedicovane operatory | cloud providery |

## Co by NiFi prinesl navic

| Feature | Prinos pro batch use case |
|---------|--------------------------|
| Visual drag-and-drop UI | Maly - batch ETL se lepe definuje kodem |
| Real-time streaming | Zadny - use case je batch/cyklicky |
| Data provenance (sledovani kazdeho zaznamu) | Stredni - uzitecne pro audit, ale ne nutne |
| Back-pressure / flow control | Zadny - neni streaming |
| 300+ built-in processoru | Stredni - ale Airflow ma tez siroku sadu operatoru |
| Cluster/HA | Stredni - ale Docker Compose Airflow ma tez Celery worker |

## Co NiFi NEPRINESE (a co prinese negativniho)

### Komplexita
- Strma krivka uceni - jiny koncept nez Airflow, vizualni flow vs kod
- Dalsi infrastruktura - JVM proces, ZooKeeper (cluster), databaze
- 40-60% casu na spravu infrastruktury (reportovano produkcionimi tymy)

### Operacni rizika
- Spaghetti flows - vizualni toky se rychle stavaji neprehlednymi
- Zadne undo v UI
- UI refresh kazdych 30s - frustrujici pri vyvoji
- Custom deploy - kontejnerizace NiFi vyzaduje vlastni reseni
- Horizontalni skalovani - NiFi nema built-in podporu

### Duplicita
- Pro batch ETL dva nastroje delajici totez
- Integrace Airflow<->NiFi pridava dalsi bod selhani
- Dve UI, dva zpusoby monitoringu, dva systemy na udrzbu

## Typicky Airflow-only DAG pro batch ETL

```
[extract_from_api] --> [transform_to_csv] --> [load_to_database]
[extract_from_db]  --> [transform_to_json] --> [push_to_api]
```

## Zdroje

- https://www.astronomer.io/blog/apache-nifi-vs-airflow/
- https://www.datacamp.com/blog/apache-nifi-vs-airflow
- https://tasrieit.com/blog/apache-nifi-vs-airflow-2026
- https://medium.com/@harsh.b26/apache-nifi-drawbacks-a4a14abadf4c
- https://labs.theagilemonkeys.com/posts/is-apache-nifi-the-right-tool-for-your-ai-data-pipelines/
