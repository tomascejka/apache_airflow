# ANA-05: Edge ETL - navrh flow (Extract/Transform/Load rozdeleni)

## Kontext

Jak rozdelit praci mezi Edge Worker (linka) a Central Server?

## Varianty

### Varianta A: Edge=Extract, Central=Transform+Load (soucasny poc03)

```
Edge:     cte syrova data --> posle RAW na central
Central:  transformuje + ulozi
```

- Problem: central musi znat vsechny syorve formaty vsech stroju
- Nescaluje se - novy stroj = zmena na centralne
- Jednoduche na edge, slozite na centralne

### Varianta B: Edge=Extract+Transform, Central=Load (doporucena)

```
Edge:     cte syrova data --> transformuje na dohodnuty format --> posle na central
Central:  prijme standardizovana data --> ulozi do ext. systemu
```

- Edge zna sve syrova data a namapuje je na dohodnuty format (JSON/YAML schema)
- Central ma **jeden univerzalni handler** pro vsechny linky/stroje
- Novy stroj = zmena JEN na edge, server se nemeni
- Schema definuje server predem (kontrakt)

### Varianta C: Edge=vse, Central=jen forward

```
Edge:     cte --> transformuje --> ulozi lokalne do souboru
Central:  sebere soubory od edge workeru --> preposlena do ext. systemu
```

- Edge dela vsechno, central je jen "postman"
- Vhodne pokud je ext. system pristupny jen ze serverovny

## Doporuceni: Varianta B

Realisticky scener (dle konzultace s architektem): kazdy edge worker bude muset udelat Extract + Transform, protoze zna strukturu svych dat. Load (ulozeni/odeslani) dela server.

## Jak edge preda data serveru? (VYRESENO)

| Zpusob | Vyhoda | Nevyhoda | Status |
|--------|--------|----------|--------|
| **XCom** (poc03) | Jednoduche, v ramci Airflow | Limit velikosti, zatezuje metadata DB | PoC only |
| **XCom Object Storage Backend** | Transparentni, zadna zmena DAGu | Dalsi kontejner (SeaweedFS) | **DOPORUCENO** |
| **Sdileny filesystem (NFS/SMB)** | Zadny limit velikosti | Antipattern pro edge (viz ANA-12) | NE |
| **Primo do cilove DB** | Nejkratsi cesta | Tight coupling, security riziko | Podminene |

**Reseni**: XCom Object Storage Backend + SeaweedFS. Detaily viz [ANA-12](ANA-12_nahrada_xcom_produkce.md) serie.

## Detailni rozbor tasku a operaci

Viz [ANA-05a](ANA-05a_tasky_operace_airflow.md) — jak Airflow premysli o taskach vs operacich, proc 5 logickych operaci = 2 tasky, kdy rozdelit na vic, role komponent.

## Implementace

**Varianta B implementovana v poc03** (`poc03_edge_etl/dags/edge_automotive_etl.py`):
- Edge: `extract_transform_stroj_1` (CSV -> std. format), `extract_transform_stroj_2` (JSON -> std. format)
- Central: `load_to_csv`, `load_to_db` - univerzalni handlery, nezavisle na raw formatech
- Prenos dat: XCom (pro PoC dostacujici)
