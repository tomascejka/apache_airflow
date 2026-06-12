# ANA-07: Edge ETL — prezentace pro architekta

Tri urovne pohledu: business → technicky → detail.

---

## 1. Business pohled

**Co to dela:** Automaticky sbira data ze stroju na vyrobni lince, prenasi je na server a uklada do cilovych systemu.

```
LINKA                                  SERVER
┌─────────────────────┐                ┌─────────────────────┐
│                     │                │                     │
│  Stroj 1 ──┐       │                │       ┌──→  CSV     │
│             ├───────│───────────────→│───────┤             │
│  Stroj 2 ──┘       │                │       └──→  DB      │
│  ...                │                │       └──→  (cokoliv dalsiho)
│                     │                │                     │
│  Sbira + pripravuje │                │  Uklada kam potreba │
└─────────────────────┘                └─────────────────────┘
```

**Klicove vlastnosti:**

| Vlastnost | Vyznam |
|-----------|--------|
| Novy stroj na lince | Zmena **jen na lince**, server se nemeni |
| Nova linka | Nasadime agenta, server se nemeni |
| Novy cilovy system (jina DB, API, ...) | Zmena **jen na serveru**, linky se nemeni |
| Komunikace linka → server | Pres HTTP/S, zadna sdilena DB, zadny VPN |
| Automatizace | Bezi dle casoveho planu (napr. kazdou hodinu), bez zasahu cloveka |

**Shrnutí:** Linka zna sve stroje. Server nezna detaily stroju — prijima data v dohodnutem formatu a uklada je. Kazda strana se meni nezavisle.

---

## 2. Technicky pohled

**Technologie:** Apache Airflow 3.2 (open-source, Python, batch orchestrator)

```
LINKA (Windows/Linux PC)               SERVEROVNA (Linux server)
┌─────────────────────┐                ┌──────────────────────────────┐
│  Airflow Edge Worker │◄──HTTP/S──►  │  Airflow Server              │
│  (lehky agent)       │               │  ┌────────────────────────┐  │
│                      │               │  │ Scheduler (ridici)     │  │
│  - cte data ze stroju│               │  │ API Server (REST API)  │  │
│  - transformuje      │               │  │ Celery Worker (vykonny)│  │
│  - odesle na server  │               │  │ PostgreSQL (metadata)  │  │
└─────────────────────┘                │  │ Redis (fronta uloh)    │  │
                                       │  └────────────────────────┘  │
                                       └──────────────────────────────┘
```

**Jak to spolu mluvi:**

| Krok | Co se deje | Komunikace |
|------|-----------|-------------|
| 1 | Scheduler rozhodne, ze je cas spustit ETL | interni |
| 2 | Posle extract+transform ulohy na edge worker | HTTP/S (API) |
| 3 | Edge worker cte data ze stroju (CSV, JSON, ...) | lokalni soubory |
| 4 | Edge worker transformuje na standardni format | lokalne |
| 5 | Edge worker odesle vysledek zpet na server | HTTP/S (XCom → API → DB) |
| 6 | Scheduler spusti load ulohy na centralnim workeru | interni (Redis fronta) |
| 7 | Celery worker precte data z DB a zapise do cilu | lokalne |

**Infrastruktura:**
- Server: Docker Compose (7 kontejneru) nebo nativni instalace na Linux
- Linka: jeden lehky agent (Edge Worker), instalace pres pip nebo Docker
- Mezi nimi: staci HTTP/S spojeni (zadna sdilena DB, zadny sdileny filesystem)

**Overeno v PoC:** Extract+transform bezi na jinem stroji nez load (ruzne hostnames v logach).

---

## 3. Deep-detail pohled

**Vse je v jednom Python souboru:** `dags/edge_automotive_etl.py`

Distribuce se neresi rozdelenim kodu, ale parametrem `executor=` na kazdem tasku. Vsechny stroje vidi stejny kod (namountovany pres volume).

### Workflow (DAG) definice

```python
with DAG("edge_automotive_etl", schedule=timedelta(hours=1), ...) as dag:

    # EDGE: bezi na lince
    t_et_1 = PythonOperator(
        task_id="extract_transform_stroj_1",
        python_callable=extract_transform_stroj_1,  # Python funkce (viz nize)
        executor=EDGE_EXECUTOR,                      # posli na edge worker
        queue="edge_queue",
    )
    t_et_2 = PythonOperator(
        task_id="extract_transform_stroj_2",
        python_callable=extract_transform_stroj_2,
        executor=EDGE_EXECUTOR,
        queue="edge_queue",
    )

    # CENTRALA: bezi na serveru (default executor = Celery)
    t_load_csv = PythonOperator(task_id="load_to_csv", python_callable=load_to_csv)
    t_load_db  = PythonOperator(task_id="load_to_db",  python_callable=load_to_db)

    # Zavislosti: oba extract musi dobehnout, pak oba load paralelne
    t_et_1 >> [t_load_csv, t_load_db]
    t_et_2 >> [t_load_csv, t_load_db]
```

### Python callable — co dela kazdy task

**`extract_transform_stroj_1`** (bezi na LINCE):
```python
def extract_transform_stroj_1(**context):
    # 1. EXTRACT: precte raw CSV soubor ze stroje
    with open("/opt/airflow/data/stroj_1/measurements.csv") as f:
        reader = csv.DictReader(f)   # sloupce: timestamp, device_id, temperature, status

    # 2. TRANSFORM: namapuje na standardni format + normalizuje status
    rows.append({
        "timestamp":    row["timestamp"],
        "machine":      "stroj_1",
        "device_id":    row["device_id"],
        "temperature":  float(row["temperature"]),
        "status":       STATUS_MAP.get(raw_status, raw_status),  # running→ok, warn→warning
        "extracted_at": datetime.now().isoformat(),
    })

    # 3. ODESLE data na server (pres XCom → HTTP → metadata DB)
    context["ti"].xcom_push(key="data", value=rows)
```

**`extract_transform_stroj_2`** — stejny princip, ale cte JSON (jiny raw format, jina jmena poli: `ts`, `sensor`, `temp_c`, `state`). Vystup je identicky standardni format.

**`load_to_csv`** (bezi na SERVERU):
```python
def load_to_csv(**context):
    # 1. SEBERE data ze vsech edge tasku (standardni format, nevi odkud prisla)
    data = _collect_all_data(ti, ["extract_transform_stroj_1", "extract_transform_stroj_2"])

    # 2. ZAPISE do CSV
    writer = csv.DictWriter(f, fieldnames=STANDARD_FIELDS)
    writer.writerows(data)
```

**`load_to_db`** — stejny princip, ale zapisuje do SQLite databaze misto CSV.

### Tok dat

```
Edge Worker                         Centralni DB                    Celery Worker
┌──────────────┐  xcom_push(data)   ┌──────────┐   xcom_pull()    ┌──────────┐
│ stroj_1: CSV │ ─────────────────→ │ PostgreSQL│ ───────────────→ │ load_csv │
│ stroj_2: JSON│ ─────────────────→ │ (XCom)   │ ───────────────→ │ load_db  │
└──────────────┘                    └──────────┘                   └──────────┘
     HTTP/S POST                     ulozeno v DB                   Celery/Redis
```

### Standardni format (kontrakt mezi linkou a serverem)

```json
{
  "timestamp":    "2026-06-12 08:30:00",
  "machine":      "stroj_1",
  "device_id":    "sensor_A1",
  "temperature":  23.5,
  "status":       "ok",
  "extracted_at": "2026-06-12T10:22:23.511"
}
```

Kazdy edge worker mapuje sve raw data na tento format. Server ho prijima a uklada — nezna puvodni strukturu.
