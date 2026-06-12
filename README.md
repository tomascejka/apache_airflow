# Airflow

## Co to je?

Apache Airflow je open-source platforma pro **vytvareni, planovani a monitoring batch workflow** (davkovych uloh).

- Workflow se definuji jako **Python kod** (ne XML/YAML) - lze je verzovat, testovat, sdilet
- Zakladni jednotkou je **DAG** (Directed Acyclic Graph) - orientovany acyklicky graf, ktery popisuje co se ma spustit, v jakem poradi a kdy
- DAG obsahuje **tasky** (ulohy), kazdy task pouziva **operator** (BashOperator, PythonOperator, atd.)
- Airflow resi **scheduling** (casove planovani), **retry** (opakovani pri selhani), **dependency management** (zavislosti mezi tasky)
- Ma **webove UI** pro vizualizaci, monitoring a ladeni workflow

### Typicke pouziti

- ETL/ELT pipeliny (extract, transform, load)
- Planovane davkove zpracovani dat
- Orchestrace ML pipeline
- Automatizace pravidelnych procesu

### Co Airflow NENI

Airflow je **batch** nastroj - spousti ulohy v naplanovanych intervalech (hodina, den, tyden). Kazdy DAG run ma zacatek a konec.

**Neni to streaming/real-time nastroj.** Pro to existuji Kafka, Flink, NiFi.

| Scenar | Airflow? | Duvod |
|--------|----------|-------|
| "Kazdy den zpracuj vcerejsi objednavky" | Ano | Batch, planovany interval |
| "Zpracuj kazdou objednavku do 1 sekundy" | Ne | Real-time, potrebujes streaming |
| "Kazdou hodinu agreguj metriky" | Ano | Hodina je OK latence |
| "Reaguj na kazdy IoT event okamzite" | Ne | Continuous streaming |

**Pravidlo**: je akceptovatelna latence v minutach/hodinach? Ano = Airflow. Sekundy = streaming nastroj.

## Struktura projektu

### Guides

| Adresar | Popis |
|---------|-------|
| [gud01_install_docker](guides/gud01_install_docker/README.md) | Instalace Airflow pres Docker Compose |
| [gud02_install_standalone](guides/gud02_install_standalone/README.md) | Instalace Airflow standalone (pip) |
| [gud03_building_your_first_workflow](guides/gud03_building_your_first_workflow/README.md) | Prvni DAG - BashOperator, Jinja, dependencies |
| [gud04_python_operator](guides/gud04_python_operator/README.md) | PythonOperator a TaskFlow API (@task dekorator) |
| [gud05_xcom_variables_params](guides/gud05_xcom_variables_params/README.md) | XCom, Variables, Params - predavani dat a konfigurace |
| [gud06_branching_trigger_rules](guides/gud06_branching_trigger_rules/README.md) | Podminene vetveni a pravidla spousteni |
| [gud07_sensors](guides/gud07_sensors/README.md) | Sensors - cekani na externi podminky |
| [gud08_error_handling](guides/gud08_error_handling/README.md) | Retries, callbacky, timeouty |
| [gud09_taskgroups_dynamic](guides/gud09_taskgroups_dynamic/README.md) | TaskGroups a dynamic task mapping |
| [gud10_scheduling_datasets](guides/gud10_scheduling_datasets/README.md) | Scheduling, datasets (assets), catchup |
| [gud11_pools_priority_config](guides/gud11_pools_priority_config/README.md) | Pools, priority, concurrency control |

### PoCs - proof of concepts

| Adresar | Popis |
|---------|-------|
| [poc01_automotive_etl](pocs/poc01_automotive_etl/README.md) | Batch ETL ze stroju (CSV+JSON → transform → CSV+SQLite) |
| [poc02_edge_worker](pocs/poc02_edge_worker/README.md) | Distribuovana architektura - centrala + remote Edge Worker |
| [poc03_edge_etl](pocs/poc03_edge_etl/README.md) | Spojeni poc01+poc02: extract na lince (edge), transform+load na centrale |
| [poc04_monitoring](pocs/poc04_monitoring/README.md) | Monitoring: Prometheus + Grafana + demo workflow |
| [poc05_monitoring_zabbix](pocs/poc05_monitoring_zabbix/README.md) | Monitoring: Zabbix (HTTP Agent → REST API) |
| [poc06_alternatives](pocs/poc06_alternatives/README.md) | Alternativy: Airflow vs Prefect vs Dagster (analyticky) |

### Ostatni

| Adresar | Popis |
|---------|-------|
| [analyses/](analyses/) | Globalni analyzy (Airflow vs NiFi, zadani, monitoring, ...) |
| [guides/ANA-01](guides/ANA-01_testovaci_strategie.md) | Testovaci strategie pro gudXX laboratorie |

## Klicova zjisteni

**Windows** - nativne nefunguje (POSIX only). Reseni: Docker Desktop (doporuceno) nebo WSL2.

**Integrace s Apache NiFi** - neexistuje oficialni provider. Airflow = batch orchestrace, NiFi = real-time data flow. Lze kombinovat pres REST API (`HttpOperator`, `nipyapi`) nebo sensor pattern (NiFi zapise -> Airflow senzor detekuje).

**Airflow vs NiFi pro batch ETL** - pro cyklicky/scheduled ETL (DB, CSV, JSON, REST API) **staci samotny Airflow**. NiFi pridava hodnotu az pri real-time/streaming pozadavcich. Pro batch by prinesl zbytecnou komplexitu (dalsi infrastruktura, 2 nastroje na udrzbu, strmejsi krivka uceni). Detaily viz [ANA-01](analyses/ANA-01_poc_airflow_vs_nifi.md).

**Airflow vs Prefect vs Dagster** - Airflow je nejzralejsi volba pro enterprise batch orchestraci. Prefect a Dagster jsou moderni alternativy, ale nemaji edge/distributed worker support. Detaily viz [poc06/ANA-01](pocs/poc06_alternatives/analyses/ANA-01_srovnani_orchestratoru.md).

## Next steps

- Dalsi iterace poc03: idempotence (UPSERT misto INSERT), vice stroju, realne formaty dat
- Prezentace vysledku architektovi (ANA-01 + ANA-02 + poc03 validace)

## Analyzy

### Globalni

- [ANA-01: Airflow vs NiFi pro batch ETL](analyses/ANA-01_poc_airflow_vs_nifi.md)
- [ANA-02: Automotive ETL - vstupni zadani](analyses/ANA-02_automotive_etl_zadani.md)
- [ANA-03: Airflow vs NiFi - detail](analyses/ANA-03_airflow_vs_nifi_detail.md)
- [ANA-04: Edge Worker architektura](analyses/ANA-04_edge_worker_architektura.md)
- [ANA-05: Edge ETL - navrh flow](analyses/ANA-05_edge_etl_flow_design.md)
- [ANA-06: REST API v2](analyses/ANA-06_rest_api.md)
- [ANA-07: Prezentace pro architekta](analyses/ANA-07_prezentace_architekt.md)
- [ANA-08: Monitoring (Prometheus + Grafana)](analyses/ANA-08_monitoring.md)
- [ANA-09: Monitoring (Zabbix)](analyses/ANA-09_monitoring_zabbix.md)
- [ANA-10: Logovani (vcetne Edge Workeru)](analyses/ANA-10_logging.md)
- [KAD-01: Code-first orchestrace](analyses/KAD-01_code_first_orchestrace.md)

### Guides

- [guides/ANA-01: Testovaci strategie](guides/ANA-01_testovaci_strategie.md)
- [gud02/ANA-01: Podpora Windows](guides/gud02_install_standalone/analyses/ANA-01_podpora_windows.md)
- [gud02/ANA-02: Standalone architektura](guides/gud02_install_standalone/analyses/ANA-02_standalone_architektura.md)

### PoCs

- [poc06/ANA-01: Airflow vs Prefect vs Dagster](pocs/poc06_alternatives/analyses/ANA-01_srovnani_orchestratoru.md)
