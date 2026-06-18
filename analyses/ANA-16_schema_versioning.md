# ANA-16: Schema versioning edge ↔ central

## Kontext

Edge worker produkuje data ve standardizovanem formatu (schema contract z ANA-05). Co se stane kdyz:
- Pridame novy stroj s jinym formatem?
- Zmenime schema (novy sloupec, zmena typu)?
- Edge ma starsi verzi kodu nez centrala?

## Problem

```
Edge Worker v1:  { machine_id, device_id, timestamp, temperature, status }
Edge Worker v2:  { machine_id, device_id, timestamp, temperature, status, humidity }
                                                                          ^^^^^^^^ novy sloupec

Central Worker:  ocekava v1 schema → co s "humidity"?
```

### Scenare zmeny

| Zmena | Priklad | Riziko |
|-------|---------|--------|
| Pridani sloupce | `humidity` | Central nezna sloupec → ignoruje nebo selze |
| Odebrani sloupce | `-status` | Central ocekava sloupec → selze |
| Zmena typu | `temperature: str→float` | Parsovani selze |
| Prejmenovani | `temp_c→temperature` | Central nenajde stary nazev |

## Strategie

### Strategie 1: Additive-only schema (DOPORUCENO)

**Princip**: schema se smi pouze **rozsirovat** (pridat sloupce), nikdy neodebirat ani menit typ. Backward compatible by default.

**Pravidla:**
1. **Nove sloupce = volitelne** (nullable / default hodnota)
2. **Existujici sloupce se nemeji a neodebiraji**
3. **Central ignoruje nezname sloupce** (tolerantni parser)

```python
# Central load task — tolerantni
EXPECTED_COLUMNS = ["machine_id", "device_id", "timestamp", "temperature", "status"]

@task
def load(data):
    for row in data:
        # Vezmi jen zname sloupce, ignoruj extra
        clean = {k: row[k] for k in EXPECTED_COLUMNS if k in row}
        # Chybejici sloupce = None
        for col in EXPECTED_COLUMNS:
            clean.setdefault(col, None)
        db.upsert(clean)
```

**Vyhody:**
- Jednoduche — zadna infrastruktura navic
- Backward i forward compatible
- Edge muze byt napred (novy sloupec) i pozadu (stary kod)

**Nevyhody:**
- Nikdy nemuzes odebrat sloupec (schema roste)
- "Breaking changes" vyzaduji novou verzi schema (v2)

### Strategie 2: Schema verze v datech

**Princip**: kazdy zaznam nese verzi schema.

```python
# Edge worker
return {
    "_schema_version": 2,
    "machine_id": "stroj_1",
    "temperature": 45.2,
    "humidity": 60.1,  # nove ve v2
}
```

```python
# Central worker
@task
def load(data):
    for row in data:
        version = row.get("_schema_version", 1)
        if version == 1:
            process_v1(row)
        elif version == 2:
            process_v2(row)
```

**Vyhody:**
- Explicitni — jasne ktera verze
- Umoznuje breaking changes (nova verze = novy handler)

**Nevyhody:**
- Slozitejsi central kod (handler per verze)
- Roste s poctem verzi

### Strategie 3: Schema registry (enterprise)

- Centralni registr schemat (napr. Confluent Schema Registry, AWS Glue)
- Producent registruje schema, konzument validuje
- Automaticka kontrola kompatibility (backward/forward/full)
- **Overkill pro nas use case** (desitky, ne tisice schemat)

## Srovnani

| Kriterium | Additive-only | Verze v datech | Schema registry |
|-----------|--------------|----------------|-----------------|
| Slozitost | Nizka | Stredni | Vysoka |
| Breaking changes | Ne (jen rozsireni) | Ano (nova verze) | Ano (validovane) |
| Infrastruktura navic | Ne | Ne | Ano (registr) |
| Pro nas use case | **DOPORUCENO** | Alternativa | Overkill |

## Doporuceni

### Additive-only schema jako default

1. **Definovat schema kontrakt** (JSON schema nebo Python dataclass):
   ```python
   # schema_contract.py (sdileny mezi edge a central)
   SCHEMA_V1 = {
       "required": ["machine_id", "device_id", "timestamp", "temperature", "status"],
       "optional": ["humidity", "vibration", "pressure"],
   }
   ```

2. **Edge worker validuje** pred odeslanim (vsechny required sloupce)
3. **Central worker toleruje** nezname sloupce (ignoruje extra, doplni chybejici jako None)
4. **Novy stroj** = novy edge worker kod, stejna schema (jen jina extract logika)

### Kdy prejit na verze v datech

- Kdyz je treba **odebrat sloupec** (breaking change)
- Kdyz se **meni typ** existujiciho sloupce
- Kdyz je **vic nez 5 stroju** s ruznou strukturou dat

### Prakticke doporuceni

- Schema kontrakt v **sdilenem Git repu** (edge i central ho importuji)
- **CI/CD test**: pri zmene schema otestovat ze central parser zvladne stare i nove
- **Dokumentace**: changelog schemat (co se zmenilo, kdy, proc)

## Souvisejici analyzy

- [ANA-05](ANA-05_edge_etl_flow_design.md) — schema contract mezi edge a central
- [ANA-13](ANA-13_idempotence_etl.md) — UPSERT na natural key (soucasti schema)
