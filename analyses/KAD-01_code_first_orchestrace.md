# KAD-01: Code-first orchestrace (Python)

## Rozhodnuti

Pro orchestraci batch ETL workflow pouzivame **code-first pristup** — workflow se definuji jako Python kod, ne pres GUI/drag-and-drop.

## Kontext

- Airflow, Prefect i Dagster jsou Python-based — workflow = Python kod
- Zadne klikani v GUI pro definici pipeline (na rozdil od NiFi, Azure Data Factory, SSIS)
- Vyhody: verzovani v Gitu, code review, testovatelnost, CI/CD, reprodukovatelnost
- Nevyhody: vyzaduje programatorske znalosti (Python), neni vizualni pro ne-vyvojare
- Pro automotive ETL use case: vyvoj a udrzba pipeline je v rukou vyvojovskeho tymu, ne operatoru

## Dusledky

- Tym musi umet Python (alespon zakladne)
- Workflow se verzuji v Gitu jako jakykoli jiny kod
- Zmeny v pipeline prochazi standardnim code review procesem
- Testovani pipeline je mozne (unit testy, integracni testy)
- GUI (Airflow UI, Grafana) slouzi pouze pro **monitoring a debugging**, ne pro tvorbu workflow

## Status

ACCEPTED
