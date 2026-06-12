# ANA-04: Edge Worker - distribuovana architektura (server Linux + agent Windows)

## Otazka

Je mozne mit Airflow server na Linuxu a agenta (workera) na Windows stroji na vyrobni lince?

## Odpoved

**ANO.** Existuje oficialni reseni: **Edge Worker** (`apache-airflow-providers-edge3`).

## Architektura

```
CENTRALNI SERVER (Linux)                    LINKA (Windows PC)
+---------------------------+               +------------------------+
| Scheduler (EdgeExecutor)  |               | Edge Worker            |
| API Server       <--------|-- HTTP(S) ----|  (lehky Python proces) |
| Database (PostgreSQL)     |               |  DAG soubory (kopie)   |
| DAG soubory               |               |  Lokalni logy          |
| Celery Workers (lokalni)  |               +------------------------+
+---------------------------+
```

- Edge Worker se pripojuje k API serveru pres **HTTP(S)**
- Posila heartbeaty (jsem zivy, jsem volny)
- Stahuje tasky z fronty, vykonava je lokalne
- Vysledky reportuje zpet pres API

## Klicove vlastnosti

- **Lehky** - jen Python + pip balicek, zadny Docker/DB na remote site
- **HTTP(S) only** - staci odchozi pripojeni z linky na server
- **Paralelni executory** - EdgeExecutor lze pouzit vedle CeleryExecutor
- **Centralni monitoring** - vsechno vidis v jednom Airflow UI

## Windows podpora

**EXPERIMENTALNI** - oficalni stav:
- Manualne testovano, neni production-ready
- Task SDK na Windows nefunguje (Airflow 3.x)
- Problem s ":" v nazvech souboru (log paths) - nutny workaround
- Doporuceni: **pouzit Linux** pro Edge Worker

### Mozna reseni pro Windows na lince

| Varianta | Popis |
|----------|-------|
| **WSL2 na Windows PC** | Edge Worker bezi v Linux subsystemu |
| **Docker na Windows PC** | Edge Worker v kontejneru |
| **Linux mini-PC na lince** | Misto Windows pouzit maly Linux stroj |
| **Nativne Windows** | Experimentalni, rizikove |

## Alternativa: Celery Worker na remote site

Misto Edge Workeru lze pouzit klasicky **CeleryExecutor** s remote workerem:
- Worker na lince se pripoji na sdileny Redis/RabbitMQ broker
- Vyzaduje sitovou viditelnost na broker (ne jen HTTP)
- Worker musi mit plnou instalaci Airflow + pristup k DAG souborum

Edge Worker je jednodussi (jen HTTP, lehci instalace).

## Zaver

Architektura **server (Linux) + agent (Windows linka)** je technicky mozna. Pro produkci doporucuji Edge Worker na Linuxu (WSL2/Docker/mini-PC), ne nativne Windows.

## Zdroje

- https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/architecture.html
- https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/install_on_windows.html
- https://airflow.apache.org/docs/apache-airflow-providers-edge3/stable/edge_executor.html
- https://medium.com/apache-airflow/airflow-edgeexecutor-quickstart-fe2d6996c6b3
- https://airflowsummit.org/sessions/2025/edgeexecutor-edge-worker-the-new-option-to-run-anywhere/
