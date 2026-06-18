# ANA-12b: Typy storage — srovnani a volba pro ETL data

## Kontext

Navazuje na [ANA-12](ANA-12_nahrada_xcom_produkce.md) a [ANA-12a](ANA-12a_object_storage_analyza.md). Pri nahrade XCom vyvstava otazka: jaky typ storage pouzit pro ETL data? A je vubec nutny mezistupen, nebo lze zapisovat primo do cilove DB?

## 3 zakladni typy storage

```
1. FILE STORAGE (NFS, SMB, CIFS)
   - Hierarchie: /slozka/podslozka/soubor.csv
   - POSIX operace: open, read, write, seek, lock
   - Pristup: mount jako lokalni disk (sitovy)
   - Priklad: NAS, sitovy disk na Windows

2. BLOCK STORAGE (iSCSI, SAN, EBS)
   - Surove bloky dat na disku (zadna struktura)
   - Pouziti: disk pro databazi, VM volume
   - Pristup: OS ho vidi jako fyzicky disk
   - Priklad: PostgreSQL data lezi na block storage

3. OBJECT STORAGE (S3 API)
   - Plochy namespace: bucket/klic → objekt (data + metadata)
   - Pristup: HTTP API (PUT, GET, DELETE)
   - Priklad: AWS S3, SeaweedFS, Garage
```

## Srovnani vlastnosti

| Vlastnost | File storage (NFS/SMB) | Block storage | Object storage (S3) |
|-----------|----------------------|---------------|---------------------|
| Pristup | Mount (sitovy disk) | Raw disk (OS) | HTTP API (PUT/GET) |
| Atomicita zapisu | **Ne** — lze cist neuplny soubor | Ano (na urovni bloku) | **Ano** — objekt je bud cely, nebo neexistuje |
| Pres sit/WAN | Krehke (NFS pres VPN = pomale) | Nelze (lokalni) | **Nativne** (HTTP = navrzeno pro sit) |
| Metadata k datum | Nazev + timestamps | Zadne | Libovolne key-value pairy |
| Verzovani | Ne (nebo slozite) | Ne | Built-in (S3 versioning) |
| Skalovani | Slozite (vice IOPS) | Pridej disk | Jednoduche (pridej node) |
| Vhodne pro | Sdilene soubory, legacy apps | Databaze, VM disky | Velke soubory, ETL data, archivy |

## Proc object storage pro nase ETL data

1. **HTTP pristup** — edge worker uz dnes komunikuje s centralou pres HTTP (Airflow API). S3 API je stejny pattern, jen dalsi port.
2. **Atomicky PUT** — central worker nikdy neprecte neuplna data. U NFS toto neni zaruceno.
3. **Metadata** — ke kazdemu objektu lze pridat `machine_id`, `batch_timestamp`, `edge_worker_id`.
4. **Neni treba mount** — edge worker nepotrebuje sitovy disk, jen HTTP pristup.
5. **XCom backend integrace** — Airflow ma vestavenou podporu pro S3-compatible storage (konfiguracni zmena, zadna zmena DAGu).

## Proc NE file storage pro edge

Viz [ANA-12 sekce "Rizika NFS/SMB"](ANA-12_nahrada_xcom_produkce.md) — 7 konkretnich rizik. Hlavni:
- Iluze lokalniho FS (kazda operace = sitovy RPC)
- Zadna atomicita (neuplne soubory)
- NFS pres WAN = pomale, nespolehline

## Proc NE block storage

Block storage je lokalni — nelze sdilet mezi edge a centralou pres sit. Je urcen pro databaze a VM disky, ne pro prenos dat mezi stroji.

## 4. moznost: Primo do cilove DB (bez mezistupne)

Krome 3 typu storage existuje jeste varianta: edge worker zapisuje **primo do cilove databaze** (napr. MongoDB, PostgreSQL, ClickHouse). Zadny mezistupen.

### Dva ruzne architektonicke problemy

```
Problem 1: KAM ukladat ETL data (cilovy storage)
  → MongoDB, PostgreSQL, ClickHouse, cokoliv
  → Business rozhodnuti (jaka DB pro analyticka data)

Problem 2: JAK prenest data z edge na centralu (prepravni kanal)
  → XCom, Object Storage, primo do cilove DB
  → Technicke rozhodnuti (spolehlivost, bezpecnost, flexibilita)
```

Tyto dva problemy lze resit **spolecne** (primo do DB) nebo **oddelene** (mezistupen + separatni load).

### Srovnani: mezistupen vs primo

```
VARIANTA A: S mezistupnem (object storage)
  Edge → SeaweedFS (docasne) → Central Worker → Cilova DB (trvale)
  3 kroky, DAGy beze zmeny, separace odpovednosti

VARIANTA B: Primo do cilove DB
  Edge → Cilova DB (primo)
  1 krok, zmena DAG kodu, tight coupling
```

| Kriterium | Object storage (mezistupen) | Primo do DB |
|-----------|---------------------------|-------------|
| Zmena DAG kodu | **Ne** (XCom backend) | **Ano** (DB klient v tasku) |
| Pocet kroku | 3 (edge → storage → central → DB) | 1 (edge → DB) |
| Edge worker potrebuje | S3 credentials | DB connection string |
| Edge zna cilovou DB | **Ne** (separace) | **Ano** (tight coupling) |
| Airflow vidi load krok | Ano (separatni task) | Ne (soucas edge tasku) |
| Retry pri selhani DB | Airflow retry na load tasku | V kodu edge tasku |
| Zmena cilove DB | Zmena jen na centrale | Zmena na vsech N edge workerech |
| Security na factory floor | Edge ma jen S3 key | Edge ma pristup k produkcni DB |
| Debugging | 2 tasky = lepsi viditelnost | 1 task = horsi viditelnost |

### Priklad: MongoDB primo

```python
# ZMENA v DAG kodu (edge task)
@task(executor="edge")
def extract_transform_load():
    data = extract_from_machine()
    transformed = transform(data)
    # primo zapis do MongoDB — edge worker musi znat connection string
    client = pymongo.MongoClient("mongodb://central:27017")
    client.etl_db.measurements.insert_many(transformed)
```

### Priklad: Object storage (bez zmeny DAGu)

```python
# BEZ ZMENY — stejne jako v poc03
@task(executor="edge")
def extract_transform():
    data = extract_from_machine()
    return transform(data)  # XCom backend → automaticky do SeaweedFS

@task(executor="celery")
def load(data):
    # central worker zapise do cilove DB
    client = pymongo.MongoClient("mongodb://localhost:27017")
    client.etl_db.measurements.insert_many(data)
```

## Klicova architektonicka otazka

```
Edge worker VI, kam data konci?

├── NE (object storage jako mezistupen)
│   ├── Separace odpovednosti (edge = extract+transform, central = load)
│   ├── Zmena cilove DB = zmena jen na centrale, ne na N linkach
│   ├── Edge nema pristup k produkcni DB (security)
│   └── = Varianta B z ANA-05, validovana v poc03
│
└── ANO (primo do cilove DB)
    ├── Jednodussi (mene komponent)
    ├── ALE: edge worker ma pristup k produkcni DB (security riziko)
    ├── ALE: zmena cilove DB = zmena na vsech edge workerech
    └── ALE: ztrata Airflow orchestrace load kroku
```

## Doporuceni

**Object storage (SeaweedFS) jako mezistupen** — zachovava separaci odpovednosti z ANA-05, Varianta B:
- Edge zna jen "predej data centrale" (S3 PUT)
- Central rozhoduje kam ulozit (MongoDB, Postgres, ClickHouse — cokoliv)
- Zmena cilove DB = zmena jen na centrale, ne na N linkach
- Edge nema pristup k produkcni DB

Primo zapis do DB je legitimni volba pokud:
- Cilova DB je znama a nemenila se
- Security na factory floor neni concern
- Maly pocet edge workeru (zmena neni nakladna)

## Souvisejici analyzy

- [ANA-12](ANA-12_nahrada_xcom_produkce.md) — proc nahrazujeme XCom, varianty prenosu
- [ANA-12a](ANA-12a_object_storage_analyza.md) — volba storage (SeaweedFS), infra, bezpecnost, backup
- [ANA-05](ANA-05_edge_etl_flow_design.md) — Varianta B (edge=extract+transform, central=load)
