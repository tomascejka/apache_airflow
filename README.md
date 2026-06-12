# Airflow

## Co to je?

Apache Airflow je open-source platforma pro **vytvárení, plánování a monitoring batch workflow** (davkovych uloh).

- Workflow se definuji jako **Python kod** (ne XML/YAML) - lze je verzovat, testovat, sdílet
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

| Adresar | Typ | Popis |
|---------|-----|-------|
| [guides/gud1_install_docker/](guides/gud1_install_docker/README.md) | Guide | Instalace Airflow pres Docker Compose |
| [guides/gud2_install_standalone/](guides/gud2_install_standalone/README.md) | Guide | Instalace Airflow standalone (pip) |
| [guides/gud3_building_your_first_workflow/](guides/gud3_building_your_first_workflow/README.md) | Guide | Prvni DAG - BashOperator, Jinja, dependencies |
| [guides/gud4_python_operator/](guides/gud4_python_operator/README.md) | Guide | PythonOperator a TaskFlow API (@task dekorator) |
| [guides/gud5_xcom_variables_params/](guides/gud5_xcom_variables_params/README.md) | Guide | XCom, Variables, Params - predavani dat a konfigurace |
| [guides/gud6_branching_trigger_rules/](guides/gud6_branching_trigger_rules/README.md) | Guide | Podminene vetveni a pravidla spousteni |
| [guides/gud7_sensors/](guides/gud7_sensors/README.md) | Guide | Sensors - cekani na externi podminky |
| [guides/gud8_error_handling/](guides/gud8_error_handling/README.md) | Guide | Retries, callbacky, timeouty |
| [guides/gud9_taskgroups_dynamic/](guides/gud9_taskgroups_dynamic/README.md) | Guide | TaskGroups a dynamic task mapping |
| [guides/gud10_scheduling_datasets/](guides/gud10_scheduling_datasets/README.md) | Guide | Scheduling, datasets (assets), catchup |
| [guides/gud11_pools_priority_config/](guides/gud11_pools_priority_config/README.md) | Guide | Pools, priority, concurrency control |
| [poc1_automotive_etl/](poc1_automotive_etl/README.md) | PoC | Batch ETL ze stroju (CSV+JSON → transform → CSV+SQLite) |
| [poc2_edge_worker/](poc2_edge_worker/README.md) | PoC | Distribuovana architektura - centrala + remote Edge Worker |
| [poc3_edge_etl/](poc3_edge_etl/README.md) | PoC | Spojeni poc1+poc2: extract na lince (edge), transform+load na centrale |
| [poc4_monitoring/](poc4_monitoring/README.md) | PoC | Monitoring: Prometheus + Grafana + demo workflow |
| [poc5_monitoring_zabbix/](poc5_monitoring_zabbix/README.md) | PoC | Monitoring: Zabbix (HTTP Agent → REST API) |
| [poc6_alternatives/](poc6_alternatives/README.md) | PoC | Alternativy: Airflow vs Prefect vs Dagster (analyticky) |
| [analyses/](analyses/) | Analyzy | Globalni analyzy (Airflow vs NiFi, zadani) |

## Klicova zjisteni

**Windows** - nativne nefunguje (POSIX only). Reseni: Docker Desktop (doporuceno) nebo WSL2.

**Integrace s Apache NiFi** - neexistuje oficialni provider. Airflow = batch orchestrace, NiFi = real-time data flow. Lze kombinovat pres REST API (`HttpOperator`, `nipyapi`) nebo sensor pattern (NiFi zapise -> Airflow senzor detekuje).

**Airflow vs NiFi pro batch ETL** - pro cyklicky/scheduled ETL (DB, CSV, JSON, REST API) **staci samotny Airflow**. NiFi pridava hodnotu az pri real-time/streaming pozadavcich. Pro batch by prinesl zbytecnou komplexitu (dalsi infrastruktura, 2 nastroje na udrzbu, strmejsi krivka uceni). Detaily viz [ANA-01](analyses/ANA-01_poc_airflow_vs_nifi.md).

## Next steps

- Dalsi iterace poc3: idempotence (UPSERT misto INSERT), vice stroju, realne formaty dat
- Prezentace vysledku architektovi (ANA-01 + ANA-02 + poc3 validace)

## Analyzy

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
- [gud2/ANA-01: Podpora Windows](guides/gud2_install_standalone/analyses/ANA-01_podpora_windows.md)
- [gud2/ANA-02: Standalone architektura](guides/gud2_install_standalone/analyses/ANA-02_standalone_architektura.md)
- [KAD-01: Code-first orchestrace](analyses/KAD-01_code_first_orchestrace.md)
- [poc6/ANA-01: Airflow vs Prefect vs Dagster](poc6_alternatives/analyses/ANA-01_srovnani_orchestratoru.md)