# ANA-05a: Tasky vs operace — jak Airflow premysli

## Kontext

Navazuje na [ANA-05](ANA-05_edge_etl_flow_design.md) (rozdeleni prace edge vs central). Tato analyza vysvetluje, jak Airflow premysli o praci — a proc se logicke operace (cti, transformuj, posli, uloz) nemapuji 1:1 na Airflow tasky.

## Zakladni jednotka: Task

**Task** je nejmensi jednotka, kterou Airflow:
- **Naplanuje** (scheduler rozhodne kdy a kde)
- **Spusti** (na spravnem workeru — edge nebo central)
- **Monitoruje** (stav: running → success/failed)
- **Retryuje** (pri selhani, s konfigurovatelnym poctem a intervalem)
- **Loguje** (kazdy task ma vlastni log soubor)

Task neni "operace se souborem". Task je **hranice odpovednosti a retry**.

## Logicke operace vs Airflow tasky

### Prirozeny mental model (operace)

Kdyz premyslime o ETL, myslime v operacich:

```
1. Precti surova data ze stroje          (souborova operace)
2. Transformuj do standardniho formatu   (datova operace)
3. Posli data na prestupni uloziste      (sitova operace)
4. Vycti data z prestupniho uloziste     (sitova operace)
5. Zapis do cilove databaze              (databazova operace)
```

### Jak to mapuje Airflow (tasky)

```
Operace 1: Precti surova data        ┐
Operace 2: Transformuj               ├→ TASK 1: extract_transform
Operace 3: Posli na prestupni        ┘  (bezi na EDGE WORKERU, na lince)
           uloziste                      operace 3 = automaticky "return data"
                                         (XCom backend uploadne do SeaweedFS)

           --- sitova hranice ---         (HTTP, automaticky)

Operace 4: Vycti z prestupniho       ┐
           uloziste                   ├→ TASK 2: load
Operace 5: Zapis do cilove DB        ┘  (bezi na CELERY WORKERU, na serveru)
                                         operace 4 = automaticky (XCom backend)
```

**5 logickych operaci = 2 Airflow tasky.** Operace 3 a 4 (prenos dat) jsou neviditelne — framework je resi sam.

### Proc prave tady je hranice

```
TASK 1 (edge)              TASK 2 (server)
┌──────────────┐           ┌──────────────┐
│ 1. Cti data  │           │ 4. Vycti data│
│ 2. Transformuj│  ──────> │ 5. Uloz do DB│
│ 3. return    │  auto     │              │
└──────────────┘  (HTTP)   └──────────────┘
  Jiny stroj                 Jiny stroj
  Jina sit                   Jina sit
  Jina zodpovednost          Jina zodpovednost
```

Hranice tasku = hranice **kde se meni stroj, sit a zodpovednost**:
- **Task 1** bezi na lince — zodpovida za cteni a upravu dat ze stroje
- **Task 2** bezi na serveru — zodpovida za ulozeni do cilove DB
- **Mezi nimi** — automaticky prenos pres prestupni uloziste

## Proc NE 1 task, proc NE 5 tasku

### 1 velky task (vsechno najednou)

```python
@task(executor="edge")
def extract_transform_load():
    data = read_from_machine()
    transformed = transform(data)
    db.insert(transformed)  # edge worker zapisuje primo do DB
```

**Problemy:**
- Edge worker musi mit pristup k produkcni DB (security riziko na factory floor)
- Pokud DB zapis selze, opakuje se i cteni ze stroje (zbytecne)
- Zmena cilove DB = zmena kodu na vsech N edge workerech
- Airflow nevidí load jako separatni krok (horsi debugging)

### 5 malych tasku (kazda operace zvlast)

```
[1.cti] → [2.transformuj] → [3.posli] → [4.vycti] → [5.uloz]
 edge       edge               edge        server      server
```

**Problemy:**
- Kazdy task = start noveho Python procesu (overhead)
- Operace 1 a 2 trvaji milisekundy — nemaji smysl jako separatni tasky
- Operace 3 a 4 nejsou "prace" — jsou to automaticke prenosy dat
- Vice tasku = vic complexity v DAG grafu, vic veci co mohou selhat

### 2 tasky (spravny kompromis)

```
[extract_transform] → [load]
 edge                   server
```

**Proc to funguje:**
- Hranice = zmena stroje (edge → server) a zodpovednosti (cteni vs zapis)
- Na edge: extract+transform je rychle (ms-sec), retry je levny
- Prenos dat: automaticky (neni task, neni overhead)
- Na serveru: load je separatni (retry bez opetovneho cteni ze stroje)

## Kdy rozdelit edge task na vic tasku

2 tasky je **default**. Rozdeleni edge tasku dava smysl pouze kdyz:

### 1. Casove nakladna transformace

```
[extract]  →  [transform]  →  (auto)  →  [load]
 edge 2s       edge 5min                   server

Pokud transform selze po 4 minutach, extract se NEOPAKUJE.
```

### 2. Vice nezavislych zdroju dat

```
[extract_stroj_1] ──┐
[extract_stroj_2] ──┼──→ [transform_all] → (auto) → [load]
[extract_stroj_3] ──┘
 paralelne na edge      edge                          server

Pokud stroj_2 selze, stroj_1 a stroj_3 se NEOPAKUJI.
```

### 3. Ruzne retry strategie

```
[extract]  →  [transform]
 retries=5     retries=2
 (sit/HW)      (logika)

Cteni ze stroje muze selhat castecji (sit) — vic retries.
Transformace selhava vzacne — mene retries.
```

### Pravidlo pro rozdeleni

```
Ma smysl rozdelit task kdyz:
  ✓ Kroky trvaji ruzne dlouho (sec vs min)
  ✓ Kroky maji ruznou pravdepodobnost selhani
  ✓ Kroky jsou nezavisle (paralelizace)
  ✓ Chci jemnejsi viditelnost v Airflow UI

NEMA smysl rozdelit kdyz:
  ✗ Oba kroky trvaji milisekundy
  ✗ Krok B vyzaduje kompletni vystup kroku A
  ✗ Pridavas task jen "protoze je to logicky jiny krok"
```

## Role komponent v nasi architekture

```
SCHEDULER (mozek)
  - Bezi na centralnim serveru
  - Planuje: ktery task, kdy, na jakem workeru
  - Monitoruje: stav tasku, retry, timeout
  - SAM NEPROVADI tasky — jen je rozdeluje

EDGE WORKER (ruce na lince)
  - Bezi na stroji na vyrobni lince
  - Provadi: extract + transform tasky
  - Komunikuje s centralou pres HTTP (Airflow API + S3 API)
  - Zna svuj stroj (syrova data), nezna cilovou DB

CELERY WORKER (ruce na serveru)
  - Bezi na centralnim serveru
  - Provadi: load tasky (zapis do cilove DB)
  - Cte data z prestupniho uloziste (SeaweedFS)
  - Zna cilovou DB, nezna jednotlive stroje

PRESTUPNI ULOZISTE (schranka)
  - SeaweedFS na centralnim serveru
  - Pasivne drzi data (S3 API: PUT/GET)
  - Edge worker → PUT data → Central worker GET data
  - Docasne (data se smazou po uspesnem loadu)

CILOVA DB (archiv)
  - MongoDB, PostgreSQL, ClickHouse... (business rozhodnuti)
  - Trvale uloziste pro analyzu a reporting
  - Pristup ma jen central worker (ne edge)
```

## Otevrene otazky

| # | Otazka | Proc je dulezita | Dopad na architekturu |
|---|--------|-----------------|----------------------|
| 1 | **Velke vstupy nebo mnoho malych?** Generuji stroje par velkych souboru (100MB+) nebo tisice malych zaznamu (KB)? | Urcuje zda rozdelit edge task na extract + transform (velke) nebo nechat jako jeden (male) | Velke: 2 edge tasky (retry hranice), Male: 1 edge task (jednodussi) |
| 2 | **Frekvence?** Jak casto stroj generuje data — kazdou sekundu, minutu, hodinu? | Urcuje scheduling DAGu a zatez na prestupni uloziste | Vysoka frekvence: batching (sbirat N minut, pak poslat), Nizka: primo per-event |
| 3 | **Pocet stroju per linka?** Kolik stroju cte jeden edge worker? | Urcuje zda paralelizovat extract tasky (expand/map) | 1 stroj = jednoduche, N stroju = dynamicke tasky |
| 4 | **Tolerance na ztratu?** Je OK ztratit 1 davku dat pri selhani? | Urcuje retry strategii a idempotenci (viz OP-01) | Kriticke: vyssi retry count + idempotence, Nekriticke: jednodussi |

Tyto otazky souvisi s OP-07 (realne datove formaty a objemy od zakaznika). Odpovedi ovlivni finalni navrh DAGu — viz [pravidlo pro rozdeleni tasku](#pravidlo-pro-rozdeleni).

## Souvisejici analyzy

- [ANA-05](ANA-05_edge_etl_flow_design.md) — rozdeleni prace edge vs central (Varianta B)
- [ANA-12](ANA-12_nahrada_xcom_produkce.md) — proc nahrazujeme XCom, prestupni uloziste
- [ANA-12a](ANA-12a_object_storage_analyza.md) — volba storage (SeaweedFS), infra, bezpecnost
- [ANA-12b](ANA-12b_typy_storage_srovnani.md) — typy storage (object vs file vs block vs primo DB)
