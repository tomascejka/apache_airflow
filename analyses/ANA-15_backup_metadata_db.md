# ANA-15: Backup a disaster recovery metadata DB

## Kontext

PostgreSQL metadata DB je SPOF pro cely Airflow — obsahuje historii runu, stav tasku, connections, variables, users. Ztrata = ztrata vsech provoznich dat. Tato analyza resi jak zalohovati a obnovit.

## Co zalohovati

| Data | Kde | Kriticnost | Frekvence zmeny |
|------|-----|-----------|-----------------|
| **Metadata DB** (PostgreSQL) | Central server | VYSOKA — SPOF | Kazdy task run |
| **DAGy** (Python soubory) | Git repo | STREDNI — verzovane | Pri deployi |
| **Connections + Variables** | V metadata DB | VYSOKA — credentials | Zridka |
| **Airflow konfigurace** | airflow.cfg / env vars | STREDNI | Zridka |
| **ETL data** (SeaweedFS) | Central server | NIZKA — docasne | Kazdy ETL run |

**Priorita**: metadata DB > connections/variables > konfigurace > ETL data.

DAGy jsou v Gitu — uz zalohovane.

## Strategie zalohovani PostgreSQL

### Strategie 1: pg_dump cronjob (DOPORUCENO pro zacatek)

**Princip**: pravidelny logicky dump cele databaze.

```bash
# Cron: kazdy den v 2:00
0 2 * * * pg_dump -h localhost -U airflow airflow_db | gzip > /backups/airflow_$(date +\%F).sql.gz
```

**Retence**: ukladat poslednich 7 denich + 4 tydenni + 3 mesicni.

```bash
# Rotace: smazat backupy starsi nez 30 dni
find /backups -name "airflow_*.sql.gz" -mtime +30 -delete
```

**Obnoveni**:
```bash
psql -h localhost -U airflow airflow_db < backup.sql
```

**Vyhody:**
- Nejjednodussi — jeden cron prikaz
- Funguje na jakemkoli PostgreSQL
- Backup je citelny SQL (moznost castecne obnovy)

**Nevyhody:**
- RPO = az 24 hodin (ztratice data od posledniho dumpu)
- Dump velkych DB muze trvat minuty (zamky)
- Obnova = import celeho dumpu (pomale pro velke DB)

**Vhodne pro**: nase pouziti (mala DB, desitky DAGu, stovky runu denne).

### Strategie 2: pg_basebackup + WAL archivace (PITR)

**Princip**: fyzicky backup + prubezna archivace WAL logu = Point-in-Time Recovery.

```
PostgreSQL → WAL logy → archiv (kazda zmena)
                    ↓
            pg_basebackup (tydne)
```

**Vyhody:**
- RPO = sekundy (posledni WAL zaznam)
- PITR — obnova k libovolnemu casu
- Inkrementalni (WAL logy jsou male)

**Nevyhody:**
- Slozitejsi konfigurace (archive_mode, archive_command)
- Vyzaduje uloziste pro WAL logy
- Overkill pro malou DB

**Vhodne pro**: vetsi instalace (100+ DAGu, tisice runu denne).

### Strategie 3: Streaming replication (HA)

**Princip**: hot standby PostgreSQL replika — real-time kopie.

```
Primary (zapis) ──WAL stream──> Standby (cteni)
```

**Vyhody:**
- RPO = 0 (synchronni replikace)
- Failover: standby prevezme za sekundy
- Read replika pro UI (snizeni zateze primary)

**Nevyhody:**
- Druhy PostgreSQL server (HW naklady)
- Slozitejsi ops (failover, promotion)
- Overkill pro jednoduche nasazeni

**Vhodne pro**: mission-critical instalace kde Airflow nesmí byt nedostupny.

### Strategie 4: pgBackRest (enterprise)

- Podporuje full, differential, inkrementalni backupy
- Paralelni backup/restore, komprese, sifrovani
- Dedikovaný backup repo server
- **Overkill pro nas use case**, ale zminka pro uplnost

## Srovnani

| Kriterium | pg_dump | PITR (WAL) | Streaming repl. |
|-----------|---------|------------|-----------------|
| Slozitost | Nizka | Stredni | Vysoka |
| RPO | Hodiny (cron interval) | Sekundy | 0 (sync) |
| RTO | Minuty (import SQL) | Minuty (replay WAL) | Sekundy (failover) |
| Dalsi HW | Ne (lokalni disk) | Ne (lokalni disk) | Ano (standby server) |
| Pro nas use case | **DOPORUCENO** | Mozne | Overkill |

## Doporuceni

### Faze 1 (zaklad — hned)

1. **pg_dump cronjob** — denne v 2:00, gzip komprese
2. **Retence**: 7 denich + 4 tydenni
3. **Backupy na jiny disk** nez DB data (ochrana pred diskem failure)
4. **Mesicni test obnovy** — obnovit backup do staging prostredi

### Faze 2 (rozsireni — pokud roste)

1. WAL archivace pro PITR (RPO sekundy misto hodin)
2. Offsite backup (NAS, druhy server, cloud S3)
3. PGBouncer pro connection pooling (Airflow otvira hodne connections)

### Faze 3 (HA — pokud je kriticke)

1. Streaming replication s hot standby
2. Automaticky failover (Patroni, pg_auto_failover)

### Docker Compose integrace

```yaml
# Backup kontejner v docker-compose.yaml
backup:
  image: postgres:16
  volumes:
    - pgdata:/var/lib/postgresql/data:ro
    - ./backups:/backups
  entrypoint: >
    bash -c "while true; do
      pg_dump -h postgres -U airflow airflow_db | gzip > /backups/airflow_$$(date +%F_%H%M).sql.gz;
      find /backups -name 'airflow_*.sql.gz' -mtime +30 -delete;
      sleep 86400;
    done"
```

### Co zalohovati mimo DB

| Co | Jak | Kam |
|----|-----|-----|
| Connections + Variables | `airflow connections export` / `airflow variables export` | Git (sifrovane) nebo backup dir |
| airflow.cfg / env vars | Kopie .env souboru | Git (bez credentials) |
| SeaweedFS data | `rclone sync` nebo volume snapshot | NAS / druhy server |

## Otevrene otazky

| # | Otazka | Dopad |
|---|--------|-------|
| 1 | Ma zakaznik existujici backup infrastrukturu (NAS, tape, Veeam)? | Integrace s existujicim |
| 2 | Jaky je akceptovatelny RPO? (hodiny vs minuty) | Urcuje strategii (pg_dump vs PITR) |
| 3 | Je Airflow mission-critical (24/7 dostupnost)? | Urcuje zda HA (replication) |
