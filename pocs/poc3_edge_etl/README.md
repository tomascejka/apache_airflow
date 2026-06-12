# PoC 3: Edge ETL - Varianta B (Extract+Transform na edge, Load na centrale)

## Popis

Spojit ETL logiku (poc1) s distribuovanou architekturou (poc2) do realistickeho scenare. Extract a transform bezi na edge workeru (linka), load bezi na centralni (serverovna). Overit prenos dat pres XCom a spravne prirazeni tasku na edge vs centralu. Implementuje **Variantu B** z [ANA-05](../analyses/ANA-05_edge_etl_flow_design.md).

## Architektura

```
LINKA (Edge Worker)                    SERVEROVNA (Centrala)
+---------------------------+          +---------------------------+
| extract_transform_stroj_1 |          |                           |
|   (CSV -> std. format)    |---HTTP-->| load_to_csv  (univerzalni)|
| extract_transform_stroj_2 |          | load_to_db   (univerzalni)|
|   (JSON -> std. format)   |          |                           |
+---------------------------+          +---------------------------+
```

- **Extract + Transform** bezi na Edge Workeru = zna sve raw data, transformuje na standardizovany format (kontrakt)
- **Load** bezi na centrale = univerzalni handler, nezavisly na typu stroje
- Novy stroj = zmena JEN na edge, server se nemeni
- Komunikace pres HTTP (XCom pres metadata DB)

## Jak funguje distribuce

Cele flow je v **jednom souboru** (`dags/edge_automotive_etl.py`). Distribuce praci na vice stroju se neresi rozdelenim kodu do souboru, ale **parametrem `executor=` na urovni tasku**:

```python
# Tento task pobezi na EDGE WORKERU (linka)
PythonOperator(
    task_id="extract_transform_stroj_1",
    python_callable=extract_transform_stroj_1,
    executor=EDGE_EXECUTOR,   # ← "posli na edge"
    queue="edge_queue",       # ← do teto fronty
)

# Tento task pobezi na CENTRALE (default CeleryExecutor)
PythonOperator(
    task_id="load_to_csv",
    python_callable=load_to_csv,  # zadny executor = default = centrala
)
```

**Mechanismus:**
1. Jeden DAG soubor je namountovany na **vsech strojich** (edge i centrala) — vsichni vidi stejny kod
2. **Scheduler** (centrala) rozhodne, ktery task kam posle podle `executor=` a `queue=`
3. Edge Worker si vyzvedne jen tasky s `queue="edge_queue"`, Celery worker vyzvedne zbytek
4. Data mezi nimi tecou pres **XCom** (ulozeno v centralni metadata DB)

Airflow je **orchestrator** — jeden soubor definuje cele flow, ale jednotlive tasky fyzicky bezi na ruznych strojich.

## Co se deje krok za krokem

```
Cas   Kdo              Co dela                                    Kde bezi
─────────────────────────────────────────────────────────────────────────────
 0s   Scheduler        DAG run spusten (manualne/dle schedule)    centrala
      Scheduler        Vidi 2 tasky bez zavislosti → spusti oba   centrala
                       paralelne, posle je do edge_queue

 1s   Edge Worker      Vyzvedne extract_transform_stroj_1         LINKA
                       → precte CSV soubor (data/stroj_1/)
                       → namapuje raw sloupce na std. format
                       → normalizuje status (running→ok, warn→warning)
                       → ulozi vysledek do XCom (→ metadata DB)

      Edge Worker      Vyzvedne extract_transform_stroj_2         LINKA
                       → precte JSON soubor (data/stroj_2/)
                       → namapuje raw pole na std. format
                       → normalizuje status
                       → ulozi vysledek do XCom (→ metadata DB)

 3s   Scheduler        Oba edge tasky hotove → zavislost splnena  centrala
                       → spusti load_to_csv a load_to_db
                       → posle je do default queue (Celery)

 4s   Celery Worker    Vyzvedne load_to_csv                       SERVEROVNA
                       → xcom_pull data z obou edge tasku
                       → zapise 9 radku do CSV souboru

      Celery Worker    Vyzvedne load_to_db                        SERVEROVNA
                       → xcom_pull data z obou edge tasku
                       → zapise 9 radku do SQLite DB

 5s   Scheduler        Vsechny tasky success → DAG run success    centrala
```

### Proc toto poradi?

Definuje ho **dependency graf** na konci DAG souboru:

```python
t_et_1 >> [t_load_csv, t_load_db]    # stroj_1 musi dobehnout pred load
t_et_2 >> [t_load_csv, t_load_db]    # stroj_2 musi dobehnout pred load
```

- `extract_transform_*` nemaji zadnou vstupni zavislost → **bezi paralelne**
- `load_*` zavisi na obou extract → **cekaji az oba skonci**, pak bezi paralelne
- Scheduler tohle ridi automaticky, neni treba rucne synchronizovat

### Kudy tecou data?

```
Edge Worker                          Centralni metadata DB              Celery Worker
┌──────────┐    xcom_push("data")    ┌──────────────┐   xcom_pull()   ┌──────────┐
│ stroj_1  │ ──────────────────────→ │  XCom tabulka │ ─────────────→ │ load_csv │
│ stroj_2  │ ──────────────────────→ │  (PostgreSQL) │ ─────────────→ │ load_db  │
└──────────┘                         └──────────────┘                  └──────────┘
```

Edge worker nema pristup k DB primo — XCom data odesle pres HTTP na API server, ktery je ulozi do PostgreSQL. Load tasky si je pak prectou z DB.

## Standardizovany format (kontrakt)

```json
{
  "timestamp": "YYYY-MM-DD HH:MM:SS",
  "machine": "<id_stroje>",
  "device_id": "<id_zarizeni>",
  "temperature": 23.5,
  "status": "ok|warning|critical",
  "extracted_at": "<ISO timestamp>"
}
```

## Zmeny oproti puvodni verzi

| Puvodni (Varianta A) | Nova (Varianta B) |
|---|---|
| Edge: jen Extract | Edge: Extract + Transform |
| Central: Transform + Load | Central: jen Load |
| Central musi znat raw formaty | Central nezavisly na raw formatech |
| Novy stroj = zmena na edge + centrale | Novy stroj = zmena JEN na edge |

## Validace Varianta B (2026-06-12)

| Task | Hostname | Kde bezel | Executor | Vysledek |
|------|----------|-----------|----------|----------|
| `extract_transform_stroj_1` | `97edf4dfdc3c` | **Edge Worker (linka)** | **EdgeExecutor** | 5 radku |
| `extract_transform_stroj_2` | `97edf4dfdc3c` | **Edge Worker (linka)** | **EdgeExecutor** | 4 radky |
| `load_to_csv` | `da503ab36416` | Celery worker (centrala) | CeleryExecutor | 9 radku |
| `load_to_db` | `da503ab36416` | Celery worker (centrala) | CeleryExecutor | 9 radku |

Ruzne hostnames = extract+transform na edge workeru, load na jinem stroji (centrala).

## Struktura

```
dags/edge_automotive_etl.py     # DAG (extract+transform=edge, load=central)
data/stroj_1/measurements.csv   # vstupni data (CSV)
data/stroj_2/readings.json      # vstupni data (JSON)
data/output/                    # vystupy (CSV + SQLite DB)
Dockerfile                      # Airflow image + edge3 provider
docker-compose.yaml             # Centrala (7 kontejneru) + Edge Worker
```

## Spusteni

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

UI na http://localhost:8080 (airflow/airflow). DAG `edge_automotive_etl`.

## Navaznost

- Vychazi z [poc1](../poc1_automotive_etl/README.md) (ETL logika) a [poc2](../poc2_edge_worker/README.md) (Edge Worker)
- Navrh flow viz [ANA-05](../analyses/ANA-05_edge_etl_flow_design.md)
- Vstupni zadani viz [ANA-02](../analyses/ANA-02_automotive_etl_zadani.md)
- Edge Worker architektura viz [ANA-04](../analyses/ANA-04_edge_worker_architektura.md)
