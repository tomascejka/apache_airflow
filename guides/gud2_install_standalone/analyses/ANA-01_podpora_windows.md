# ANA-01: Podpora Windows

## Odpoved

**Airflow na Windows nativne NEFUNGUJE.**

Oficalni dokumentace rika:
> "Airflow currently can be run on POSIX-compliant Operating Systems."

## Moznosti na Windows

| Zpusob | Popis |
|--------|-------|
| **WSL2** | Windows Subsystem for Linux 2 - Airflow bezi v Linux prostredi uvnitr Windows |
| **Docker** | Linux kontejnery - Airflow bezi v kontejneru (viz [gud1_install_docker](../../gud1_install_docker/README.md)) |

## Co to znamena pro `airflow standalone`

- `pip install apache-airflow` na Windows (cmd/PowerShell) **nefunguje**
- Musis pouzit WSL2 (Ubuntu terminal) nebo Docker
- Docker je jednodussi - nemusis resit Python verze, zavislosti, ani WSL2 konfiguraci

## Systemove pozadavky

- **OS**: POSIX-compliant (Linux, macOS). Produkce = pouze Linux (Debian Bookworm v CI)
- **Python**: 3.10, 3.11, 3.12, 3.13, 3.14
- **RAM**: min 4 GB
- **DB**: PostgreSQL 13-17, MySQL 8.0, SQLite 3.15+ (jen dev)

## Zaver

Pro Windows uzivatele je **Docker Compose nejpraktictejsi cesta** - funguje out-of-the-box bez WSL2 konfigurace.
Standalone rezim vyzaduje WSL2 nebo macOS/Linux.

## Zdroj

- https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html
