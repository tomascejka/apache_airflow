# DISC-02: GitHub Issue #55297 — Edge Worker broken na Windows

## Zdroj

- https://github.com/apache/airflow/issues/55297
- https://github.com/apache/airflow/pull/55284 (fix multiprocessing)
- https://github.com/apache/airflow/pull/66931 (security model — Windows out of scope)

## Relevance: VYSOKA (kriticka)

## Souhrn

Umbrella issue trackujici vsechny opravy nutne pro obnoveni Windows podpory Edge Workeru. Issue je **OPEN** (vytvoreno 2025-09-05, stale neuzavreno k cernu 2026).

## Klicove poznatky

### Problem

"Launching and running tasks on edge worker seems to be broken. There seems to be several unix/linux specific layers that would need cleanup to restore this functionality."

- Oficialni install docs **nefunguji s Airflow 3.x** na Windows
- Task SDK zmeny v Airflow 3 rozbily Windows kompatibilitu
- Reporter: dheerajturaga (Airflow MEMBER)
- Verze: Airflow 3.0.6 na Windows 11
- Labels: kind:bug, area:core, area:providers, area:task-sdk, provider:edge

### Castecne opravy

**PR #55284** (merged 2025-09-05) — "Fix EdgeWorker multiprocessing pickle error on Windows":
- Root cause: vnorena funkce `_run_job_via_supervisor` uvnitr `_launch_job_af3` nesla pickle-ovat na Windows (Windows pouziva `spawn` misto `fork`)
- Chyba: `AttributeError: Can't pickle local object`
- Fix: extrakce vnorene funkce na `@staticmethod` tridy `EdgeWorker`

**PR #42426** (merged 2024-09-25) — "Bugfix task execution from runner in Windows":
- Windows nema `SIGKILL` v `signal` modulu; kozmeticka oprava logu

### Issue zustava OPEN

Koment maintainera: "I added this issue to track all PRs that would need to be merged to bring back windows support for edge worker" — potvrzuje, ze je potreba vice oprav.

### KRITICKE: Airflow nepodporuje Windows oficalne

**PR #66931** (merged 2026-05-16) — "docs(security): document supported deployment platforms":
- Airflow security model explicitne rika: **non-Linux platformy jsou out of scope pro CVE alokaci**
- "Bugs that only manifest on Windows / macOS / other non-Linux platforms are not eligible for CVE allocation because Airflow does not officially support those platforms as deployment targets."
- **Windows NENI podporovany deployment target pro Airflow.**
