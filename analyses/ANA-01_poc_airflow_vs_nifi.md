# ANA-01: Airflow vs NiFi - overview pro rozhodnuti

## Use case

Cyklicky batch ETL: sber dat ze stroju na vyrobni lince, transformace, ulozeni.
Detaily viz [ANA-02](ANA-02_automotive_etl_zadani.md).

## Co je NiFi

Vizualni nastroj pro presun a transformaci dat **v realnem case**. Data tečou nepretrzite z bodu A do bodu B, cestou se transformuji, filtruji, routuji. Drag-and-drop UI, 300+ processoru, data provenance (audit trail kazdeho zaznamu).

- **NiFi** = proud vody v rece (tece porad)
- **Airflow** = kbelik (naplnis, vylijes, opakujes dle schedule)

## Doporuceni

**Samotny Airflow staci.** NiFi pridava hodnotu az pri real-time/streaming pozadavcich.

## Proc

| | Airflow | NiFi |
|---|---|---|
| Batch/scheduled ETL | Ano - presne na to je | Ano, ale je to overkill |
| Real-time streaming | Ne | Ano - hlavni sila |
| Komplexita | 1 nastroj | +1 nastroj, +infrastruktura |

## Kdy prehodnotit

- Pokud se objevi pozadavek na real-time (latence pod 1 min)
- Pokud se objevi pozadavek na data provenance (audit trail kazdeho zaznamu)

## Detail

Detailni technicke srovnani (operatory, rizika NiFi, zdroje) viz [ANA-03](ANA-03_airflow_vs_nifi_detail.md).
