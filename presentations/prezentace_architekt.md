---
marp: true
theme: default
paginate: true
style: |
  :root {
    --color-background: #ffffff;
    --color-foreground: #1a1a2e;
    --color-accent: #0f3460;
  }
  section {
    background-color: var(--color-background);
    color: var(--color-foreground);
    font-family: Arial, sans-serif;
    font-size: 0.95em;
  }
  h1 {
    color: var(--color-accent);
    font-size: 2.2em;
  }
  h2 {
    color: var(--color-accent);
    font-size: 1.6em;
    border-bottom: 2px solid var(--color-accent);
    padding-bottom: 8px;
  }
  h3 {
    color: var(--color-accent);
    font-size: 1.2em;
  }
  p, ul, ol {
    font-size: 1.05em;
  }
  table {
    font-size: 0.95em;
  }
  .accent {
    display: block;
    font-size: 1.15em;
    font-weight: 600;
    color: var(--color-accent);
    margin: 0.2em 0 0.6em 0;
  }
  .title-slide {
    text-align: center;
  }
  .title-slide h1 {
    font-size: 2.8em;
    margin-top: 120px;
  }
  .title-slide p {
    font-size: 1.3em;
  }
  code {
    font-size: 0.9em;
  }
  .green { color: #27ae60; font-weight: bold; }
  .red { color: #e74c3c; font-weight: bold; }
  .orange { color: #f39c12; font-weight: bold; }
---

<!-- _class: title-slide -->

# Apache Airflow
# Edge ETL Architektura

Batch ETL ze stroju na vyrobni lince — discovery vysledky

<!--
Prezentace shrnuje discovery fazi pro architekta.
Cil: predstavit navrzenou architekturu, co jsme overili (6 PoCs), a co je jeste otevrene.
-->

---

## Agenda

1. **Problem** — co resime
2. **Architektura** — navrzene reseni
3. **PoC vysledky** — co jsme overili (6/6 PASS)
4. **Klicova rozhodnuti** — proc Airflow, proc takhle
5. **Produkcionalizace** — co je treba pro provoz
6. **Otevrene otazky** — co zbyva zjistit
7. **Dalsi kroky**

<!--
Prezentace ma 3 urovne: business pohled, technicky pohled, a deep-detail.
Zaciname shora — business kontext, pak technologie.
-->

---

<!-- _class: title-slide -->

# 1. Problem

Co resime a proc

---

## Vstupni zadani

<span class="accent">Stroje na vyrobni lince produkuji data. Chceme je automaticky sbirat, zpracovavat a ukladat.</span>

- Stroje na linkach → ruzne formaty (CSV, JSON, proprietary)
- Data se zpracovavaji davkove (batch) — ne real-time
- Akceptovatelna latence: minuty az hodiny
- Kazdy stroj ma jina data, jiny format, jine pojmenovani

**Pozadavky:**
- On-premise (data nesmi opustit interni sit)
- Automatizace (bez manualnich zasahu)
- Skalovatelnost (novy stroj = minimalni zmena)

<!--
ANA-02: Vstupni zadani. Zname: Windows na linkach, batch, heterogenni data.
Nezname (OP-07): konkretni formaty, objemy, frekvence — ceka na zakaznika.
-->

---

## Business pohled

```
LINKA                                  SERVER
┌─────────────────────┐                ┌─────────────────────┐
│                     │                │                     │
│  Stroj 1 ──┐       │                │       ┌──→  CSV     │
│             ├───────│───── HTTP/S ──→│───────┤             │
│  Stroj 2 ──┘       │                │       └──→  DB      │
│  ...                │                │                     │
│                     │                │                     │
│  Sbira + pripravuje │                │  Uklada kam potreba │
└─────────────────────┘                └─────────────────────┘
```

| Co se zmeni | Kde se meni | Druha strana |
|-------------|-------------|-------------|
| Novy stroj na lince | **Jen na lince** | Server se nemeni |
| Nova linka | Nasadime agenta | Server se nemeni |
| Novy cilovy system | **Jen na serveru** | Linky se nemeni |

<!--
Klicovy argument: separace odpovednosti. Linka zna sve stroje, server nezna detaily stroju.
Kazda strana se meni nezavisle — to je skalovatelne.
-->

---

<!-- _class: title-slide -->

# 2. Architektura

Navrzene reseni

---

## Technicky pohled

```
LINKA (Windows PC)                     SERVEROVNA (Linux server)
┌──────────────────────┐               ┌──────────────────────────────┐
│  Airflow Edge Worker │◄── HTTP/S ──►│  Airflow Server              │
│  (lehky agent)       │              │  ┌────────────────────────┐  │
│                      │              │  │ Scheduler (ridici)     │  │
│  - cte data ze stroju│              │  │ API Server (REST)      │  │
│  - transformuje      │              │  │ Celery Worker (vykonny)│  │
│  - odesle na server  │              │  │ PostgreSQL (metadata)  │  │
│                      │              │  │ Redis (fronta uloh)    │  │
│  Docker na Windows   │              │  │ SeaweedFS (ETL data)   │  │
│  nebo mini-Linux PC  │              │  └────────────────────────┘  │
└──────────────────────┘               └──────────────────────────────┘
```

<span class="accent">Edge Worker = lehky agent. Zadna DB, zadny Redis, jen Python + HTTP.</span>

<!--
ANA-04: Edge Worker architektura. HTTP-only komunikace, heartbeaty, centralni monitoring.
ANA-11: Na lince je Windows — nativni Airflow nefunguje.
Reseni: Docker na Windows (PoC) nebo mini-Linux PC (produkce).
-->

---

## ETL flow — kdo co dela

<span class="accent">Varianta B: Edge = extract + transform, Central = load</span>

| Krok | Kde bezi | Co dela |
|------|----------|---------|
| 1. Extract | **Edge** (linka) | Cte raw data ze stroje (CSV, JSON, ...) |
| 2. Transform | **Edge** (linka) | Normalizuje do standardniho formatu |
| 3. Transfer | Edge → Server | Data pres HTTP/S (XCom → staging storage) |
| 4. Load | **Central** (server) | Zapise do ciloveho systemu (DB, CSV, API) |

**Proc na edge?**
- Edge zna syrova data (format, cesty, encoding)
- Central dostava jednotny format — nezna puvod dat
- Novy stroj = zmena **jen edge kodu**, central se nemeni

<!--
ANA-05: Edge ETL design. 3 varianty (A/B/C), Varianta B doporucena.
ANA-05a: Airflow premysli v taskach — 5 logickych operaci = 2 Airflow tasky.
Hranice tasku = kde se meni stroj/sit/odpovednost.
-->

---

## Schema kontrakt

<span class="accent">Dohodnuty format mezi linkou a serverem</span>

```json
{
  "timestamp":    "2026-06-12 08:30:00",
  "machine_id":   "stroj_1",
  "device_id":    "sensor_A1",
  "temperature":  23.5,
  "status":       "ok",
  "extracted_at": "2026-06-12T10:22:23"
}
```

- Kazdy edge mapuje sve raw data na tento format
- Server prijima a uklada — nezna puvodni strukturu
- **Additive-only**: nove sloupce se pridavaji, existujici se nemeni

<!--
ANA-16: Schema versioning. Additive-only = backward compatible by default.
Central parser = tolerantni (ignoruje extra sloupce, doplni chybejici jako None).
-->

---

<!-- _class: title-slide -->

# 3. PoC vysledky

6 proof-of-concepts — vsechny PASS

---

## Overene hypotezy

| # | Co jsme testovali | Vysledek |
|---|-------------------|----------|
| PoC 01 | Batch ETL ze stroju (CSV+JSON → transform → SQLite) | <span class="green">PASS</span> |
| PoC 02 | Edge Worker na remote stroji (jiny hostname) | <span class="green">PASS</span> |
| PoC 03 | Edge extract+transform, central load | <span class="green">PASS</span> |
| PoC 04 | Prometheus + Grafana monitoring (19 panelu) | <span class="green">PASS</span> |
| PoC 05 | Zabbix business monitoring + alerting | <span class="green">PASS</span> |
| PoC 06 | Srovnani alternativ (Prefect, Dagster) | <span class="green">Airflow = spravna volba</span> |

<span class="accent">Vsech 6 PoCs uspesne — technicka proveditelnost overena.</span>

<!--
Zavislosti: poc01 → poc02 → poc03 → poc04/poc05. poc06 referencuje vsechny.
Kazdy PoC ma vlastni docker-compose, run.ps1, README s popisem.
-->

---

## Proc Airflow (a ne alternativy)

| Kriterium | Airflow | Prefect | Dagster |
|-----------|---------|---------|---------|
| **Edge Worker** | Nativni (edge3 provider) | Zadny edge agent | Zadny edge koncept |
| Ekosystem | 1000+ provideru | Desitky | Stovky |
| Komunita | 40k+ GitHub stars | 20k+ | 12k+ |
| On-premise | Plna podpora | Cloud-first | Plna podpora |
| Licencni riziko | Apache 2.0 | Hybridni | Apache 2.0 |

<span class="accent">Klicovy diferenciator: Airflow jediny s nativnim Edge Workerem pro on-premise.</span>

<!--
ANA-01/03: Airflow vs NiFi. poc06/ANA-01: Airflow vs Prefect vs Dagster.
Prefect: cloud-first, zadny edge. Dagster: asset-centric, vysoka slozitost.
Pokud uz bezi 50+ DAGu na Airflow: NEMIGROVAT.
-->

---

<!-- _class: title-slide -->

# 4. Klicova rozhodnuti

9 architektonickych rozhodnuti (KAD)

---

## Architektonicka rozhodnuti

| # | Rozhodnuti | Zduvodneni |
|---|-----------|------------|
| 1 | **Airflow** jako orchestrator | Jediny s nativnim Edge Workerem |
| 2 | **Code-first** (Python DAGy v Gitu) | Verzovani, testy, CI/CD, reprodukovatelnost |
| 3 | **Central Linux + Edge na lince** | Docker na Win nebo mini-Linux PC |
| 4 | **Edge = extract+transform** | Edge zna raw data, central je format-agnostic |
| 5 | **SeaweedFS** jako staging storage | ETL data mimo metadata DB (SPOF) |
| 6 | **Prometheus+Grafana + Zabbix** | Infra metriky + business alerting |
| 7 | **Edge3 chunk upload** pro logy | Automaticky s edge3 providerem |
| 8 | **UPSERT** pro idempotenci | Retry/rerun neprida duplicity |
| 9 | **Nginx + TLS** pro bezpecnost | Interni CA pro produkci |

<!--
Kazde rozhodnuti ma analyzu (ANA-xx). Celkem 20 analyz + 1 formalni KAD.
-->

---

## Edge Worker na Windows — jak?

<span class="accent">Airflow nativne na Windows nefunguje (issue #55297, oficalne nepodporovano)</span>

| Varianta | Pro | Proti | Kdy |
|----------|-----|-------|-----|
| **Docker na Windows** | Bez noveho HW, rychle | Docker Desktop licence, overhead | PoC, testovani |
| **Mini-Linux PC** | Stabilni, nativni podpora | Novy HW (~$500/linka) | Produkce |

**Rozhoduje zakaznik**: novy HW ano/ne?

<!--
ANA-11: 3 varianty. WSL2 nedoporuceno (prilis hacku).
Rancher Desktop = free alternativa k Docker Desktop.
OnLogic, Advantech, Siemens = prumyslove mini-Linux PC.
-->

---

## Data transfer — staging storage

<span class="accent">XCom (PoC) → SeaweedFS (produkce)</span>

**Problem:** XCom uklada data do metadata DB (PostgreSQL) = SPOF, neefektivni pro vetsi data.

**Reseni:** XCom Object Storage Backend — **konfiguracni zmena, DAGy beze zmeny**.

```
Edge Worker → HTTP/S → SeaweedFS (prestupni uloziste) → Central Worker
```

- SeaweedFS: open-source (Apache 2.0), S3 API, 1 kontejner, ~100MB RAM
- MinIO CE archivovan (02/2026) → SeaweedFS jako nahrada
- Hybridni rezim: male data v DB, velke v SeaweedFS

<!--
ANA-12/12a/12b: XCom nahrada, object storage analyza, typy storage srovnani.
NFS/SMB = antipattern pro edge (Jarek Potiuk, Airflow PMC).
poc07 (validace) zatim odlozen.
-->

---

<!-- _class: title-slide -->

# 5. Produkcionalizace

Co je treba pro provozni nasazeni

---

## Bezpecnost

| Oblast | Reseni | Slozitost |
|--------|--------|-----------|
| **Edge ↔ Central komunikace** | Nginx reverse proxy + TLS terminace | Nizka (jednorazovy setup) |
| **Certifikaty** | Interni CA (step-ca) pro produkci | Stredni |
| **Autentizace** | JWT token (sdileny secret) | Uz soucasti edge3 |
| **SeaweedFS pristup** | S3 access/secret keys | Nizka |

<!--
ANA-14: SSL/TLS. Self-signed pro PoC, interni CA pro produkci.
Let's Encrypt neni pouzitelne pro interni sit.
-->

---

## Spolehlivost

| Oblast | Reseni | Analyza |
|--------|--------|---------|
| **Idempotence** | UPSERT na natural key (machine, device, timestamp) | ANA-13 |
| **Backup metadata DB** | pg_dump cronjob (denne, 7-denni retence) | ANA-15 |
| **Schema kompatibilita** | Additive-only schema, tolerantni parser | ANA-16 |
| **Disaster recovery** | Zaklad: pg_dump, pokrocile: PITR, HA: streaming repl. | ANA-15 |

<span class="accent">Kazdy retry/rerun je bezpecny — zadne duplicity, zadna ztrata dat.</span>

<!--
OP-01: poc01 opraven — UPSERT + UNIQUE constraint + logical_date.
OP-05: PostgreSQL je SPOF, pg_dump jako zaklad. PITR pro RPO sekundy.
-->

---

## Monitoring a alerting

<span class="accent">Dva systemy — kazdy dela to, v cem je nejlepsi</span>

| Typ | Nastroj | Priklad |
|-----|---------|---------|
| **Infra metriky** | Prometheus + Grafana | CPU, RAM, disk, scheduler heartbeat, task duration |
| **Business alerting** | Zabbix | DAG selhal → email → SMS → ticket (eskalace) |

```
Airflow → StatsD → statsd-exporter → Prometheus → Grafana (dashboardy)
Airflow → REST API → Zabbix HTTP Agent → triggery → eskalace
```

**Zabbix eskalace:** L1 email (0 min) → L1 SMS (15 min) → L2 email (30 min) → management (60 min)

<!--
ANA-08: Prometheus setup (poc04, 19 panelu).
ANA-09: Zabbix setup (poc05, HTTP Agent → REST API).
ANA-20: Alerting strategie — zavisi na existujici infra zakaznika.
-->

---

## CI/CD a provoz

| Oblast | Reseni |
|--------|--------|
| **Lint** | `ruff check dags/` (syntaxe, styl, komplexita) |
| **Testy** | DagBag import errors + unit testy (pytest) |
| **Deployment** | Git-sync sidecar (periodicky pull z Gitu) |
| **Udrzba DAGu** | Maly tym: 1 clovek; vetsi: infra vs DAG autori |

<span class="accent">Novy stroj = kopie existujici extract funkce + uprava parsovani. Junior zvladne.</span>

<!--
ANA-17: CI/CD pipeline. ANA-19: Udrzba DAGu.
Minimalni CI: GitHub Actions (ruff + pytest na kazdem PR).
-->

---

## Skalovani

<span class="accent">~80 edge workeru testovano na jednom centralu</span>

| Scenar | Edge workeru | Zatez | Opatreni |
|--------|-------------|-------|----------|
| **Start** | 1–10 | Trivijalni | Default konfigurace |
| **Stredni** | 10–30 | ~300 req/min | PGBouncer |
| **Velky** | 30–80 | ~700 req/min | Delsi polling interval, vic API serveru |

Pro nas odhad (5–20 stroju) = **Small/Medium stupen**, bez specialnich opatreni.

<!--
ANA-18: Skalovani edge workeru. Bottlenecky: DB connections, scheduler parsing.
Edge komunikuje HTTP polling (job poll 5s, heartbeat 30s) — zadny message broker.
-->

---

<!-- _class: title-slide -->

# 6. Otevrene otazky

Co zbyva zjistit

---

## Stav open questions — 10/11 hotovo

| Status | Otazky |
|--------|--------|
| <span class="green">ANALYZOVANO</span> | OP-01 Idempotence, OP-02 Edge na Win, OP-03 XCom, OP-04 TLS |
| <span class="green">ANALYZOVANO</span> | OP-05 Backup, OP-06 Skalovani, OP-08 Schema, OP-09 Udrzba |
| <span class="green">ANALYZOVANO</span> | OP-10 CI/CD, OP-11 Alerting |
| <span class="orange">OTEVRENE</span> | **OP-07: Realne datove formaty a objemy** |

<span class="accent">OP-07 je jedina blokovana otazka — ceka na vstup od zakaznika.</span>

**Co potrebujeme vedet:**
- Jake formaty produkuji stroje? (CSV, JSON, XML, binarne?)
- Jak casto? (kazdych 5 min, hodinu, den?)
- Kolik dat? (KB, MB, GB za run?)
- Kolik stroju na lince? Kolik linek?

<!--
OP-07 ovlivnuje: sizing, schema kontrakt, frekvenci ETL, volbu storage.
Bez toho muzeme navrhnout architekturu, ale ne presne dimenzovat.
-->

---

## Rozhodnuti pro zakaznika

| Otazka | Varianty | Kdo rozhoduje |
|--------|----------|--------------|
| HW na lince | Docker na Win vs mini-Linux PC | Zakaznik (novy HW ~$500?) |
| Monitoring | Zabbix (pokud uz bezi) vs AlertManager | Zakaznik (existujici infra?) |
| Komunikacni kanaly | Email, SMS, Slack, Teams? | Zakaznik |
| Backup RPO | Hodiny (pg_dump) vs sekundy (PITR) | Zakaznik (kriticnost?) |

<!--
Tato rozhodnuti nemuzeme ucinit bez zakaznika.
Vsechna maji analyzy s doporucenim (ANA-11, ANA-15, ANA-20).
-->

---

<!-- _class: title-slide -->

# 7. Dalsi kroky

---

## Roadmapa

| Faze | Co | Predpoklad |
|------|----|------------|
| **Nyni** | Schvaleni architektury architektem | Tato prezentace |
| **Po schvaleni** | OP-07: Zjistit datove formaty od zakaznika | Pristup k zakaznikovi |
| | poc07: Validace SeaweedFS + edge worker | 1–2 dny |
| | Rozhodnuti: Docker na Win vs mini-Linux | Zakaznik |
| **Implementace** | Produkcni DAGy pro realne stroje | Po OP-07 |
| | TLS setup (Nginx + interni CA) | Jednorazove |
| | Monitoring setup (Prometheus/Zabbix) | Jednorazove |
| | CI/CD pipeline (GitHub Actions + git-sync) | Jednorazove |

---

## Shrnuti

<span class="accent">Discovery faze dokoncena. Technicka proveditelnost overena.</span>

- **6 PoCs** — vsechny PASS
- **20 analyz** — pokryvaji vsechny kriticke otazky
- **9 architektonickych rozhodnuti** — zduvodnena a zdokumentovana
- **10/11 open questions** analyzovano
- **1 bloker**: OP-07 (datove formaty od zakaznika)

**Minimalni architektura**: Airflow + PostgreSQL + Edge Worker (3 komponenty).
Zbytek (SeaweedFS, Prometheus, Zabbix, Nginx) se pridava inkrementalne.

<!--
Vsechno je v Git repu: guides, PoCs, analyzy, discovery zdroje.
Architekt ma pristup k detailum v ANA-xx souborech.
-->

---

<!-- _class: title-slide -->

# Dotazy?

<br><br><br>

Detaily: `analyses/ANA-*.md` v Git repu

Kontakt: Tomas Cejka
