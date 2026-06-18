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

### Guides — co jsme se naucili

| # | Adresar | Koncept | Poznatek |
|---|---------|---------|----------|
| 01 | [gud01_install_docker](guides/gud01_install_docker/README.md) | Docker Compose setup | 7 kontejneru, Airflow 3.2.2, JWT auth (ne Basic Auth) |
| 02 | [gud02_install_standalone](guides/gud02_install_standalone/README.md) | Standalone (pip) | Na Windows nefunguje — nutny Docker nebo WSL2 |
| 03 | [gud03_building_your_first_workflow](guides/gud03_building_your_first_workflow/README.md) | DAG, tasky, operatory | BashOperator, Jinja templates (`{{ ds }}`), `>>` dependencies |
| 04 | [gud04_python_operator](guides/gud04_python_operator/README.md) | PythonOperator, TaskFlow | @task dekorator = moderni pristup; return = XCom automaticky |
| 05 | [gud05_xcom_variables_params](guides/gud05_xcom_variables_params/README.md) | XCom, Variables, Params | 3 mechanismy: task↔task (XCom), globalni (Variables), per-run (Params) |
| 06 | [gud06_branching_trigger_rules](guides/gud06_branching_trigger_rules/README.md) | Branching, trigger rules | BranchPythonOperator, ShortCircuitOperator, 6 trigger rules |
| 07 | [gud07_sensors](guides/gud07_sensors/README.md) | Sensors | FileSensor, TimeDeltaSensor, ExternalTaskSensor; poke vs reschedule |
| 08 | [gud08_error_handling](guides/gud08_error_handling/README.md) | Retries, callbacky, timeouty | Exponential backoff, on_success/failure/retry callbacky, execution_timeout |
| 09 | [gud09_taskgroups_dynamic](guides/gud09_taskgroups_dynamic/README.md) | TaskGroups, expand() | Vizualni skupiny + dynamicky pocet tasku za behu (map+reduce) |
| 10 | [gud10_scheduling_datasets](guides/gud10_scheduling_datasets/README.md) | Scheduling, assets, catchup | Cron, Asset-driven scheduling, catchup vytvori historicke runy |
| 11 | [gud11_pools_priority_config](guides/gud11_pools_priority_config/README.md) | Pools, priority, concurrency | Pool sloty omezuji soubehu; hierarchie: parallelism > pool > max_active_tasks |

Guides gud04-gud11 maji automatizovane testy (`test.ps1` + `pytests/e2e/`).

### PoCs — co jsme overili

| # | Adresar | Hypoteza | Vysledek |
|---|---------|----------|----------|
| 01 | [poc01_automotive_etl](pocs/poc01_automotive_etl/README.md) | Airflow zvladne batch ETL ze stroju (CSV+JSON) | **OVERENO** — extract, transform, load do CSV+SQLite funguje |
| 02 | [poc02_edge_worker](pocs/poc02_edge_worker/README.md) | Tasky lze spoustet na remote Edge Workeru | **OVERENO** — edge task bezi na jinem hostname nez centrala |
| 03 | [poc03_edge_etl](pocs/poc03_edge_etl/README.md) | Extract+transform na lince, load na centrale | **OVERENO** — Varianta B funguje, data prenesena pres XCom |
| 04 | [poc04_monitoring](pocs/poc04_monitoring/README.md) | Prometheus + Grafana pro infrastrukturni metriky | **OVERENO** — StatsD → statsd-exporter → Prometheus → Grafana (19 panelu) |
| 05 | [poc05_monitoring_zabbix](pocs/poc05_monitoring_zabbix/README.md) | Zabbix pro business monitoring (health, DAG stavy) | **OVERENO** — HTTP Agent → REST API, built-in alerting s eskalaci |
| 06 | [poc06_alternatives](pocs/poc06_alternatives/README.md) | Existuje lepsi alternativa nez Airflow? | **NE** — Airflow jediny s nativnim Edge Workerem; Prefect/Dagster nemaji edge koncept |

Zavislosti PoCs: `poc01 → poc02 → poc03 → poc04/poc05`, `poc06` referencuje vsechny.

### Analyzy — co jsme se dozvedeli

| Analyza | Tema | Hlavni zaver |
|---------|------|-------------|
| [ANA-01](analyses/ANA-01_poc_airflow_vs_nifi.md) | Airflow vs NiFi | Pro batch ETL staci Airflow; NiFi az pri streaming pozadavku |
| [ANA-02](analyses/ANA-02_automotive_etl_zadani.md) | Vstupni zadani | Stroje → Windows PC → batch processing; formaty/volume/latence nezname |
| [ANA-03](analyses/ANA-03_airflow_vs_nifi_detail.md) | NiFi detail | NiFi pridava zbytecnou komplexitu pro batch; Airflow pokryva vsechny I/O formaty |
| [ANA-04](analyses/ANA-04_edge_worker_architektura.md) | Edge Worker arch. | edge3 provider, HTTP-only komunikace; Windows = experimental (reseni: WSL2/Docker) |
| [ANA-05](analyses/ANA-05_edge_etl_flow_design.md) | Edge ETL design | Varianta B (edge=extract+transform, central=load) — skalovatelne, schema contract |
| [ANA-06](analyses/ANA-06_rest_api.md) | REST API v2 | JWT auth, kompletni CRUD, URL encoding run_id; health check bez autentizace |
| [ANA-07](analyses/ANA-07_prezentace_architekt.md) | Prezentace | 3-urovnova prezentace (business, tech, deep-detail) |
| [ANA-08](analyses/ANA-08_monitoring.md) | Prometheus+Grafana | StatsD → exporter → Prometheus PULL; 4 typy metrik, 19 panelu |
| [ANA-09](analyses/ANA-09_monitoring_zabbix.md) | Zabbix | HTTP Agent → REST API; built-in eskalace; doplnuje Prometheus |
| [ANA-10](analyses/ANA-10_logging.md) | Logovani | Edge Worker: chunk upload (HTTP POST na centralu); 5 pristupu od simple po ELK |
| [ANA-11](analyses/ANA-11_edge_worker_windows_deployment.md) | Edge na Windows | Nativni Windows broken; Docker na Win (PoC) nebo mini-Linux PC (produkce) |
| [ANA-05a](analyses/ANA-05a_tasky_operace_airflow.md) | Tasky vs operace | Jak Airflow premysli; 5 operaci = 2 tasky; kdy rozdelit; role komponent |
| [ANA-13](analyses/ANA-13_idempotence_etl.md) | Idempotence ETL | UPSERT pattern; 4 strategie; zmena = 2 radky SQL + UNIQUE INDEX |
| [ANA-12](analyses/ANA-12_nahrada_xcom_produkce.md) | Nahrada XCom | XCom Object Storage Backend = konfiguracni zmena, DAGy beze zmeny |
| [ANA-12a](analyses/ANA-12a_object_storage_analyza.md) | Object Storage detail | MinIO CE archivovany → SeaweedFS; infra, bezpecnost, backup/recovery |
| [ANA-12b](analyses/ANA-12b_typy_storage_srovnani.md) | Typy storage | Object vs file vs block vs primo do DB; proc object storage pro edge |
| [ANA-14](analyses/ANA-14_ssl_tls_edge_central.md) | SSL/TLS | Nginx reverse proxy + TLS terminace; self-signed (PoC), interni CA (produkce) |
| [ANA-15](analyses/ANA-15_backup_metadata_db.md) | Backup metadata DB | pg_dump cronjob (zaklad); PITR pro pokrocile; streaming replication pro HA |
| [ANA-16](analyses/ANA-16_schema_versioning.md) | Schema versioning | Additive-only schema (doporuceno); tolerantni parser; schema kontrakt v Gitu |
| [ANA-17](analyses/ANA-17_cicd_dag_deployment.md) | CI/CD pro DAGy | Lint (ruff) + DAG integrity test + git-sync deployment |
| [ANA-18](analyses/ANA-18_skalovani_edge_workeru.md) | Skalovani | ~80 edge workeru testovano; 20 workeru = trivijalni zatez; PGBouncer pri 20+ |
| [ANA-19](analyses/ANA-19_udrzba_dagu_tym.md) | Udrzba DAGu | 3 modely (1 clovek / infra vs DAGy / platform team); sablony pro novy stroj |
| [ANA-20](analyses/ANA-20_alerting_strategie.md) | Alerting | Zabbix (business) + AlertManager (infra); eskalacni schema; zavisi na zakaznikovi |
| [KAD-01](analyses/KAD-01_code_first_orchestrace.md) | Code-first | Python DAGy v Gitu; GUI jen pro monitoring — ACCEPTED |

Discovery zdroje: [DISC-01](analyses/DISC-01_edge3_windows_official_docs.md)–[DISC-05](analyses/DISC-05_industrial_linux_pcs.md) (Edge na Windows), [DISC-06](analyses/DISC-06_xcom_object_storage_backend.md)–[DISC-08](analyses/DISC-08_shared_volumes_antipattern.md) (XCom nahrada), [DISC-09](analyses/DISC-09_idempotence_etl_patterns.md)–[DISC-10](analyses/DISC-10_upsert_sql_syntaxe.md) (Idempotence), [DISC-11](analyses/DISC-11_tls_airflow_edge.md)–[DISC-12](analyses/DISC-12_tls_on_premise_certifikaty.md) (SSL/TLS)

Lokalni analyzy: [gud02/ANA-01](guides/gud02_install_standalone/analyses/ANA-01_podpora_windows.md), [gud02/ANA-02](guides/gud02_install_standalone/analyses/ANA-02_standalone_architektura.md), [guides/ANA-01](guides/ANA-01_testovaci_strategie.md), [poc06/ANA-01](pocs/poc06_alternatives/analyses/ANA-01_srovnani_orchestratoru.md)

## Klicova architektonicka rozhodnuti

1. **Orchestrator**: Airflow (jediny s nativnim Edge Workerem pro on-premise)
2. **Pristup**: Code-first (Python DAGy v Gitu, ne GUI nastroje)
3. **Distribuce**: Central Linux server + Edge Worker na lince — Docker na Windows nebo mini-Linux PC ([ANA-11](analyses/ANA-11_edge_worker_windows_deployment.md))
4. **ETL flow**: Edge = extract + transform (zna syrova data), Central = load (univerzalni handlery)
5. **Data transfer**: XCom (PoC) → sdileny storage nebo REST API (produkce)
6. **Monitoring**: Prometheus+Grafana (infra metriky) + Zabbix (business alerting, pokud uz bezi)
7. **Logging**: Edge3 chunk upload (automaticky s edge3 providerem)

## Co zbyva zjistit / Open Questions

### Produkce — kriticke

| # | Otazka | Proc je dulezita | Mozny smer |
|---|--------|-----------------|------------|
| OP-01 | **Idempotence ETL** — jak resit duplicity pri opakovanem spusteni? | Kazdy retry/rerun prida duplicitni data do DB | **ANALYZOVANO** viz [ANA-13](analyses/ANA-13_idempotence_etl.md): UPSERT na natural key (machine_id, device_id, timestamp) |
| OP-02 | **Edge Worker na Windows** — jak nasadit na vyrobni linku? | Nativni Windows broken (issue #55297); Windows neni podporovany target | **ANALYZOVANO** viz [ANA-11](analyses/ANA-11_edge_worker_windows_deployment.md): Docker na Win (PoC) nebo mini-Linux PC (produkce) |
| OP-03 | **Data transfer v produkci** — cim nahradit XCom? | XCom data zatezuji metadata DB (SPOF) | **ANALYZOVANO** viz [ANA-12](analyses/ANA-12_nahrada_xcom_produkce.md): XCom Object Storage Backend + MinIO (konfiguracni zmena, DAGy beze zmeny) |
| OP-04 | **SSL/TLS** pro edge-to-central komunikaci | HTTP bez sifrovani = bezpecnostni riziko v produkci | **ANALYZOVANO** viz [ANA-14](analyses/ANA-14_ssl_tls_edge_central.md): Nginx reverse proxy + self-signed (PoC) / interni CA (produkce) |
| OP-05 | **Disaster recovery** — zaloha metadata DB | PostgreSQL je SPOF, ztrata = ztrata historie vsech runu | **ANALYZOVANO** viz [ANA-15](analyses/ANA-15_backup_metadata_db.md): pg_dump cronjob (zaklad), PITR (pokrocile), streaming replication (HA) |

### Skalovani — stredni priorita

| # | Otazka | Poznamka |
|---|--------|----------|
| OP-06 | ~~Skalovani edge workeru~~ | **ANALYZOVANO** viz [ANA-18](analyses/ANA-18_skalovani_edge_workeru.md): ~80 workeru testovano, 20 = trivijalni zatez |
| OP-07 | Jake jsou realne datove objemy a formaty? | ANA-02 definuje zadani, ale konkretni formaty nezname |
| OP-08 | ~~Schema versioning (edge ↔ central kontrakt)~~ | **ANALYZOVANO** viz [ANA-16](analyses/ANA-16_schema_versioning.md): additive-only schema, tolerantni parser |

### Provoz — nizka priorita (az pri nasazeni)

| # | Otazka | Poznamka |
|---|--------|----------|
| OP-09 | ~~Kdo udrzuje DAGy~~ | **ANALYZOVANO** viz [ANA-19](analyses/ANA-19_udrzba_dagu_tym.md): Model A (maly tym), sablony pro novy stroj |
| OP-10 | ~~CI/CD pro DAGy~~ | **ANALYZOVANO** viz [ANA-17](analyses/ANA-17_cicd_dag_deployment.md): lint + DAG integrity test + git-sync deploy |
| OP-11 | ~~Alerting strategie~~ | **ANALYZOVANO** viz [ANA-20](analyses/ANA-20_alerting_strategie.md): Zabbix (business) + AlertManager (infra) |

## Next steps

### HOTOVO — Discovery (10/11 open questions analyzovano)

- [x] **OP-01**: Idempotence → [ANA-13](analyses/ANA-13_idempotence_etl.md)
- [x] **OP-02**: Edge na Windows → [ANA-11](analyses/ANA-11_edge_worker_windows_deployment.md)
- [x] **OP-03**: Nahrada XCom → [ANA-12](analyses/ANA-12_nahrada_xcom_produkce.md)
- [x] **OP-04**: SSL/TLS → [ANA-14](analyses/ANA-14_ssl_tls_edge_central.md)
- [x] **OP-05**: Backup metadata DB → [ANA-15](analyses/ANA-15_backup_metadata_db.md)
- [x] **OP-06**: Skalovani → [ANA-18](analyses/ANA-18_skalovani_edge_workeru.md)
- [x] **OP-08**: Schema versioning → [ANA-16](analyses/ANA-16_schema_versioning.md)
- [x] **OP-09**: Udrzba DAGu → [ANA-19](analyses/ANA-19_udrzba_dagu_tym.md)
- [x] **OP-10**: CI/CD → [ANA-17](analyses/ANA-17_cicd_dag_deployment.md)
- [x] **OP-11**: Alerting → [ANA-20](analyses/ANA-20_alerting_strategie.md)

### OTEVRENE

- [ ] **OP-07**: Zjistit realne datove formaty a objemy od zakaznika
- [ ] **poc07**: Validace SeaweedFS + XCom Object Storage Backend s edge workerem

### Dalsi krok — Prezentace pro architekta

- [ ] Pripravit prezentaci (zaklad viz [ANA-07](analyses/ANA-07_prezentace_architekt.md))
- [ ] Zahrnout: 6 PoCs (vsechny PASS), 20 analyz, 7 KAD, otevrene otazky (OP-07)
