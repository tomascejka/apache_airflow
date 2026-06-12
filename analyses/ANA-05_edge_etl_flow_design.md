# ANA-05: Edge ETL - navrh flow (Extract/Transform/Load rozdeleni)

## Kontext

Jak rozdelit praci mezi Edge Worker (linka) a Central Server?

## Varianty

### Varianta A: Edge=Extract, Central=Transform+Load (soucasny poc3)

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

## Otevrena otazka: jak edge preda data serveru?

| Zpusob | Vyhoda | Nevyhoda |
|--------|--------|----------|
| **XCom** (soucasny poc3) | Jednoduche, v ramci Airflow | Limit na velikost, jde pres metadata DB |
| **Soubor na sdilenem storage** | Zadny limit velikosti | Vyzaduje sdileny filesystem/S3 |
| **REST API (POST)** | Cisty, decoupleny | Nutne implementovat endpoint |
| **Soubor + sensor** | Edge zapise soubor, central senzor detekuje | Jednoduche, robustni |

Pro PoC staci XCom. Pro produkci pravdepodobne soubor na sdilenem storage nebo REST API.

## Implementace

**Varianta B implementovana v poc3** (`poc3_edge_etl/dags/edge_automotive_etl.py`):
- Edge: `extract_transform_stroj_1` (CSV -> std. format), `extract_transform_stroj_2` (JSON -> std. format)
- Central: `load_to_csv`, `load_to_db` - univerzalni handlery, nezavisle na raw formatech
- Prenos dat: XCom (pro PoC dostacujici)
