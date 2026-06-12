# PoC 2: Edge Worker - distribuovana architektura

## Popis

Overit, ze Airflow podporuje distribuovanou architekturu — centralni server ridi workflow, remote Edge Worker (simulujici PC na vyrobni lince) vykonava tasky. Cil: task bezi na edge workeru, vysledek se prenese na centralu. Analyza viz [ANA-04](../analyses/ANA-04_edge_worker_architektura.md).

## Co demonstruje

- Central task bezi na Celery workeru (centrala)
- Edge task bezi na **Edge Workeru** (remote agent) - jiny hostname
- Komunikace pouze pres HTTP (Edge Worker nema pristup k DB ani Redis)

## Validace (2026-06-12)

**Edge Worker funguje.** Overeno: task s `executor=EdgeExecutor` bezi na edge workeru (jiny hostname), ne na centrale.

| Task | Hostname | Kde bezel | Executor |
|------|----------|-----------|----------|
| `central_task` | `163d8d6e7cbc` | Celery worker (centrala) | CeleryExecutor (default) |
| `edge_task` | **`89c68784702b`** | **Edge worker (remote)** | **EdgeExecutor** |
| `report_task` | `163d8d6e7cbc` | Celery worker (centrala) | CeleryExecutor (default) |

- Ruzne hostnames = ruzne kontejnery = distribuovane spusteni
- Edge worker **nema pristup k DB ani Redis** - komunikuje s centralou pouze pres HTTP
- V produkci: `163d8d6e7cbc` = server v serverovne, `89c68784702b` = PC na vyrobni lince

## Struktura

```
dags/edge_demo.py       # DAG s central + edge tasky
Dockerfile              # Airflow image + edge3 provider
docker-compose.yaml     # Centralni Airflow + Edge Worker kontejner
www/                    # Referencni material (Medium clanek)
```

## Spusteni

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

UI na http://localhost:8080 (airflow/airflow). DAG `edge_demo` - unpause a trigger.

## Jak to funguje

1. **docker-compose.yaml** - hybridni executor: `CeleryExecutor,EdgeExecutor`
2. **Centralni sluzby** maji `AIRFLOW__EDGE__API_ENABLED=true`
3. **edge-worker** kontejner se pripojuje pres HTTP na `http://airflow-apiserver:8080/edge_worker/v1/rpcapi`
4. **DAG task** s `executor=EdgeExecutor` + `queue="edge_queue"` bezi na edge workeru
5. Sdileny `JWT_SECRET` pro autentizaci

## Klicova konfigurace

| Parametr | Centrala | Edge Worker |
|----------|----------|-------------|
| `CORE__EXECUTOR` | `CeleryExecutor,EdgeExecutor` | `EdgeExecutor` |
| `EDGE__API_ENABLED` | `true` | - |
| `EDGE__API_URL` | - | `http://apiserver:8080/edge_worker/v1/rpcapi` |
| `API_AUTH__JWT_SECRET` | sdileny | sdileny (musi se shodovat) |
| `API_AUTH__JWT_ISSUER` | sdileny | sdileny (musi se shodovat) |

## Troubleshooting

- **403 Forbidden** - `JWT_SECRET` nebo `JWT_ISSUER` se neshoduji mezi centralou a workerem
- Referencni quickstart viz [www/edge_executor_quickstart.md](www/edge_executor_quickstart.md)

## Analyzy

- [ANA-04: Edge Worker architektura](../analyses/ANA-04_edge_worker_architektura.md)
- [ANA-02: Vstupni zadani](../analyses/ANA-02_automotive_etl_zadani.md)
