# ANA-11: Edge Worker na Windows — varianty nasazeni

## Kontext

Edge Worker (edge3 provider) musi bezet na stroji na vyrobni lince. Stroje typicky bezi na Windows. Otazka: jak nasadit Edge Worker, kdyz Airflow oficalne nepodporuje Windows?

Discovery zdroje: [DISC-01](DISC-01_edge3_windows_official_docs.md), [DISC-02](DISC-02_github_edge_windows_broken.md), [DISC-03](DISC-03_wsl2_production.md), [DISC-04](DISC-04_docker_na_windows.md), [DISC-05](DISC-05_industrial_linux_pcs.md)

## Stav Windows podpory (cerven 2026)

### Fakta

1. **Airflow oficalne nepodporuje Windows** — security model explicitne rika, ze Windows bugy nejsou eligible pro CVE (PR #66931, merged 2026-05)
2. **Edge Worker na Windows je broken pro Airflow 3.x** — Task SDK zmeny rozbily kompatibilitu (issue #55297, OPEN od 2025-09)
3. **Oficialni docs = Airflow 2.10.5 only** — dokumentace napsana a testovana na stare verzi
4. **Castecne opravy** — multiprocessing pickle error opraven (PR #55284), ale issue zustava otevreny
5. **Znamy problem**: dvojtecka v log paths vyzaduje server-side workaround (ovlivni cely cluster)

### Zaver

Nativni beh Edge Workeru na Windows **neni viabilni pro produkci s Airflow 3.x**. I kdyby se issue #55297 uzavrel, Windows zustane "community-supported" bez security garanci.

## Varianty nasazeni

### Varianta A: Docker kontejner na Windows PC

**Koncept**: Linux kontejner s Edge Workerem bezi na existujicim Windows PC na lince.

**Jak to funguje**:
```
Windows PC na lince
  └── Docker Engine (Rancher Desktop / Podman / Docker Desktop)
       └── Linux kontejner
            ├── Python 3.12 + edge3 provider
            ├── DAGy (git sync nebo volume)
            └── airflow edge worker --concurrency 4
```

**Pristup k datum**: volume mount z Windows FS (`-v C:\data:/data`)

**Container runtime**:
- **Rancher Desktop** (doporuceno): free (Apache 2.0), zadne licencni omezeni, GUI, compose podpora
- **Podman Desktop**: free, daemonless, ale mene stabilni na Windows
- **Docker Desktop**: stabilni, ale placeny pro firmy >250 zamestnancu

**Vyhody**:
- Zadne Windows path issues (Linux FS uvnitr kontejneru)
- Task SDK funguje normalne
- Vyuziti existujiciho HW (zadny novy nakup)
- Jednoducha aktualizace (novy image)
- Restart policy = auto-restart kontejneru

**Nevyhody**:
- Hyper-V/WSL2 backend = ~2GB RAM overhead
- Container runtime musi bezet jako sluzba (auto-start po rebootu)
- Zavislost na Windows stability (BSOD = edge worker padne)
- Docker Desktop na Windows Server nefunguje (jen Pro/Enterprise/Education)

**Slozitost nasazeni**: STREDNI

### Varianta B: WSL2 na Windows PC

**Koncept**: Edge Worker bezi primo v WSL2 Linux prostredi na existujicim Windows PC.

**Jak to funguje**:
```
Windows PC na lince
  └── WSL2 (Ubuntu)
       ├── Python 3.12 + edge3 provider
       ├── DAGy
       └── airflow edge worker
```

**Auto-start**: NSSM (Non-Sucking Service Manager) jako Windows Service wrapper.

**Vyhody**:
- Nativni Linux vykon
- Zadny container overhead
- Mensi RAM footprint nez Docker

**Nevyhody**:
- WSL2 = developer tool, ne oficialne production-ready
- Auto-start bez loginu vyzaduje NSSM hack
- WSL2 networking = NAT (ne bridged) — komplikuje sitovou konfiguraci
- Pri neplanovanem rebootu muze WSL2 nestartovat spravne
- Slozitejsi monitoring (Windows musi monitorovat WSL2 health)
- Microsoft muze zmenit WSL2 behavior mezi updaty

**Slozitost nasazeni**: STREDNI-VYSOKA

### Varianta C: Dedickovany mini-Linux PC

**Koncept**: Maly Linux PC vedle existujiciho Windows stroje na lince. Edge Worker bezi nativne na Linuxu.

**Jak to funguje**:
```
Vyrobni linka
  ├── Windows PC (ridici stroj)
  │    └── generuje data → sit/USB → mini PC
  └── Linux mini PC
       ├── Python 3.12 + edge3 provider
       ├── DAGy
       └── airflow edge worker
```

**HW moznosti**:

| Kategorie | Priklad | Cena | Factory-grade |
|-----------|---------|------|---------------|
| Prumyslovy | OnLogic CL100/CL200 | $400-800 | Ano (fanless, dustproof, DIN rail) |
| Consumer+ | Intel NUC / ASUS NUC | $150-400 | Ne (ale tichy, kompaktni) |
| Budget | Radxa X4 (Intel N100) | $60-100 | Ne |
| SBC | Raspberry Pi 5 | $80 | Ne, ARM = mozne issues |

**Vyhody**:
- **Plne podporovana platforma** (Linux = first-class citizen pro Airflow)
- Zadne hacky (WSL, Docker na Windows)
- Security updates = standardni Linux patching
- Nezavisly na Windows PC (Windows BSOD ≠ edge worker outage)
- Jednoduchy monitoring (standardni Linux tooling)
- Moznost DIN rail mount v rozvodne

**Nevyhody**:
- Dalsi HW = dalsi naklady ($100-800 per linka)
- Dalsi zarizeni k udrzbe (OS updates, HW failures)
- Sit mezi Windows PC a mini PC (pristup k datum)
- Fyzicky prostor na lince

**Slozitost nasazeni**: NIZKA (standardni Linux setup)

### Varianta D: Nativni Windows (NE pro produkci)

Pouze pro testovani/demo s Airflow 2.x. Pro Airflow 3.x nefunguje. Neni dalsi uvahy.

## Srovnani variant

| Kriterium | A: Docker na Win | B: WSL2 | C: Mini-Linux PC |
|-----------|-----------------|---------|-------------------|
| Podpora Airflow | Plna (Linux kontejner) | Plna (Linux) | Plna (nativni Linux) |
| Novy HW | Ne | Ne | Ano ($100-800) |
| RAM overhead | ~2GB (Hyper-V) | ~1GB (WSL2) | 0 (nativni) |
| Auto-start | Docker restart policy | NSSM hack | systemd (nativni) |
| Stabilita | Vysoka | Stredni | Nejvyssi |
| Nezavislost na Win PC | Ne | Ne | Ano |
| Slozitost ops | Stredni | Vysoka | Nizka |
| Licencni riziko | Mozne (Docker Desktop) | Ne | Ne |
| Security patching | Kontejner image + Windows | WSL2 + Windows | Jen Linux |

## Doporuceni

### Pro PoC / testovani

**Varianta A (Docker na Windows)** s Rancher Desktop:
- Rychle nasazeni bez noveho HW
- Free licence
- Dostatecne stabilni pro testovani

### Pro produkci

**Varianta C (dedickovany mini-Linux PC)** — doporuceno:
- Jedina plne podporovana varianta bez kompromisu
- Nezavisla na Windows stroji (izolace selhani)
- Standardni Linux ops (systemd, apt, cron)
- OnLogic CL100 (~$500) pro factory-grade, Intel NUC (~$200) pro kancelarske prostredi

**Varianta A (Docker na Windows)** — akceptovatelna alternativa:
- Pokud zakaznik nechce dalsi HW
- Pouzit Rancher Desktop (free) s restart policy
- Vyzaduje monitoring Docker health z centraly

### Nedoporuceno pro produkci

- **Varianta B (WSL2)**: prilis mnoho hacku, Microsoft to nepovazuje za production tool
- **Varianta D (nativni Windows)**: broken, nepodporovane

## Rozhodovaci strom

```
Muze zakaznik pridat HW na linku?
├── ANO → Varianta C (mini-Linux PC)
│         ├── Factory prostredi? → OnLogic CL100 ($500)
│         └── Kancelar/lab? → Intel NUC ($200)
└── NE → Varianta A (Docker na existujicim Windows PC)
          └── Rancher Desktop (free) + Linux kontejner
```

## Otevrene otazky z teto analyzy

1. **Pristup k datum**: jak edge worker cte data ze stroje? Sit (SMB/NFS), USB, serial port?
   - Docker: volume mount z Windows FS, nebo sitovy pristup
   - Mini PC: sitovy pristup k Windows share, nebo primo pripojeny k masine
2. **Sitova topologie**: je na lince Ethernet pro mini PC? DHCP nebo staticka IP?
3. **Sprava vice edge zarizeni**: jak hromadne deployovat a updatovat?
   - Ansible, Fleet management, nebo manualne (pokud <10 linek)
