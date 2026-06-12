# Instrukce pro Claude CLI

## Projekt

Apache Airflow — discovery faze pro architekta. Automotive use case (batch ETL ze stroju na vyrobni lince).

## Struktura

- `gudXX_*/` — guides (instalace, konfigurace)
- `tutX_*/` — tutorials (uceni)
- `pocXX_*/` — proof of concept (overeni hypotezy)
- `analyses/ANA-XX_*.md` — globalni analyzy
- `pocXX/analyses/ANA-XX_*.md` — lokalni analyzy specificke pro dany PoC

## Pravidla pro praci

### Dokumentace
- **README.md** = high-level, strucny. Na detaily odkazovat do `analyses/*.md` nebo zdrojovych souboru.
- Kazdy `pocXX/README.md` musi zacinat kapitolou `## Popis` (1-3 vety: co chceme overit, ceho chceme dosahnout).
- **Analyzy**: `analyses/ANA-XX_<nazev>.md` format. Obsahuji detailni technicke informace.
- Jazyk dokumentace: **cestina** (bez diakritiky v souborech).

### Skripty
- Kazdy `pocXX/` musi mit `run.ps1` (PowerShell skript pro spusteni stacku).
- Kazdy pocXX ma vlastni `docker-compose.yaml` a `Dockerfile`.

### Chyby a troubleshooting
- Vsechny chyby zapisovat do `poc01_automotive_etl/analyses/ANA-01_troubleshooting.md`.
- Rozlisovat: **FATAL** (chyba, ktera nedava smysl, musi se hledat reseni) vs **WARNING** (spatny predpoklad, jina verze).
- U kazde chyby: Chyba → Pricina → Reseni (s prikladem kodu).

### Novy PoC
- Novy adresar `pocXX_<nazev>/` se svym docker-compose.
- Pokud vychazi z predchoziho PoC, zkopirovat DAGy a data, ne celou strukturu.
- Pridat do root `README.md` tabulky i do seznamu analyz.

### Technicke detaily
- Airflow 3.2.2 + edge3 provider
- Docker Compose na Windows (bash shell v CLI)
- REST API trigger nefunguje spolehlive (dag_versions issue) → pouzivat CLI trigger: `docker compose exec airflow-scheduler airflow dags trigger <dag_id>`

### Styl
- Strucne odpovedi, ne overengineering.
- Nejdrive overview/fundamentals, pak detaily.
- Pri praci s novymi nastroji (Zabbix, Prometheus, Grafana): nejdrive obecny popis nastroje, pak konkretni integrace s Airflow.

## Workflow: zpracovani noveho tematu

Kdyz uzivatel zada "prover tema X" nebo "priprav pocXX pro tema X", postupuj takto:

### Faze 1: Zalozeni struktury

1. Vytvorit adresar (`pocXX_<nazev>/` nebo relevantni umisteni)
2. Vytvorit `README.md` s `## Popis` (1-3 vety: co overujeme, ceho chceme dosahnout)
3. Vytvorit podadresare (`dags/`, `config/`, `data/`, `scripts/`, `analyses/`)

### Faze 2: Discovery (DISC)

1. Vyhledat zdroje (dokumentace, clanky, GitHub, Stack Overflow)
2. Pro kazdy hodnotny zdroj vytvorit `analyses/DISC-XX_<nazev>.md`:
   - URL zdroje
   - Strucny souhrn co zdroj rika
   - Relevance pro nas use case (vysoka/stredni/nizka)
   - Klicove poznatky k pouziti v analyze/implementaci
3. DISC soubory jsou "surovy material" — nemusí byt uhladene, dulezita je informacni hodnota

### Faze 3: Analyza (ANA)

1. Z DISC zdroju vytvorit analyzy `analyses/ANA-XX_<nazev>.md`:
   - Obecny popis (co to je, jak to funguje — overview/fundamentals)
   - Detailni technicka cast (konfigurace, integrace, priklady)
   - Srovnani alternativ (pokud existuji)
   - Doporuceni
2. Analyzy jsou "zpracovany material" — strukturovane, srozumitelne

### Faze 4: Open Questions (OP)

1. Pro nezodpovezene otazky vytvorit `analyses/OP-XX_<nazev>.md`:
   - Formulace otazky
   - Proc je dulezita
   - Mozne smery reseni
   - Co je treba zjistit (dalsi discovery/experiment)
2. OP soubory slouzi jako TODO pro dalsi iterace

### Faze 5: Implementace (pokud je PoC)

1. `docker-compose.yaml`, `Dockerfile`, DAGy, konfigurace
2. `run.ps1` pro spusteni
3. Validace — spustit, overit, zdokumentovat vysledky do README
4. Chyby → `ANA-01_troubleshooting.md`

### Faze 6: Sumarizace

1. Prolinkovat vsechny DISC/ANA/OP do `README.md`
2. Pridat do root `README.md` (tabulka + analyzy)
3. Shrnuti: co jsme zjistili, co funguje, co ne, dalsi kroky

### Priklad vysledne struktury

```
pocXX_tema/
  README.md                          # High-level: Popis, architektura, pristupy, validace
  run.ps1                            # Spusteni stacku
  docker-compose.yaml
  Dockerfile
  dags/
  config/
  data/
  scripts/
  analyses/
    DISC-01_oficialni_dokumentace.md  # Zdroj: Airflow docs
    DISC-02_blog_integrace.md        # Zdroj: blog post
    DISC-03_github_example.md        # Zdroj: GitHub repo
    ANA-01_architektura.md           # Analyza: jak to funguje
    ANA-02_konfigurace.md            # Analyza: jak nakonfigurovat
    OP-01_skalovani.md               # Otazka: jak skalovat?
```

### Pojmenovani

- `DISC-XX` — Discovery (zdroj informaci)
- `ANA-XX` — Analysis (zpracovana analyza)
- `OP-XX` — Open Question (nezodpovezena otazka)
- `KAD-XX` — Key Architecture Decision (klicove architektonicke rozhodnuti)
- Cislovani: v ramci adresare kde lezi (pocXX/analyses/ nebo globalni analyses/)
- Globalni analyzy (across PoCs): `analyses/ANA-XX_*.md` v root projektu
- Globalni KAD: `analyses/KAD-XX_*.md` v root projektu

### KAD (Key Architecture Decision)

KAD dokumentuje klicova architektonicka rozhodnuti, ktera ovlivnuji volbu nastroju, pristupu nebo technologie. Format:

```
# KAD-XX: <nazev>

## Rozhodnuti
<co jsme rozhodli, 1-2 vety>

## Kontext
<proc jsme to rozhodli, jake faktory hraly roli>

## Dusledky
<co z toho vyplyva, co to znamena pro projekt>

## Status
ACCEPTED / PROPOSED / SUPERSEDED
```
