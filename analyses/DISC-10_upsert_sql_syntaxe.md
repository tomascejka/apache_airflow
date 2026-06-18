# DISC-10: UPSERT — SQL syntaxe pro ruzne databaze

## Zdroje

- https://www.baeldung.com/sql/postgresql-upsert-merge-insert — PostgreSQL UPSERT
- https://wiki.postgresql.org/wiki/UPSERT — PostgreSQL wiki
- https://www.bytebase.com/blog/sql-upsert/ — Cross-DB srovnani

## Relevance: STREDNI (implementacni detail)

## Souhrn

Kazda databaze ma jinou syntaxi pro UPSERT. Pro nas use case je dulezite vedet jak to funguje v PostgreSQL (soucasna cilova DB v poc01) a MongoDB (potencialni cilova DB).

## Syntaxe

### PostgreSQL (od 9.5)

```sql
INSERT INTO measurements (machine_id, device_id, timestamp, temperature, status)
VALUES ('stroj_1', 'dev_01', '2024-01-15 10:30:00', 45.2, 'running')
ON CONFLICT (machine_id, device_id, timestamp)
DO UPDATE SET
  temperature = EXCLUDED.temperature,
  status = EXCLUDED.status;
```

- `ON CONFLICT (...)` — unikatni klic/index
- `DO UPDATE SET` — co aktualizovat pri konfliktu
- `DO NOTHING` — preskoc duplicitu (bez update)
- `EXCLUDED` — odkazuje na novy (vkladany) radek

**Batch UPSERT:**
```sql
INSERT INTO measurements (machine_id, device_id, timestamp, temperature, status)
VALUES
  ('stroj_1', 'dev_01', '2024-01-15 10:30:00', 45.2, 'running'),
  ('stroj_1', 'dev_01', '2024-01-15 10:31:00', 45.5, 'running')
ON CONFLICT (machine_id, device_id, timestamp)
DO UPDATE SET temperature = EXCLUDED.temperature, status = EXCLUDED.status;
```

**POZOR**: duplicity v ramci jednoho INSERT statementu jsou ERROR (ne conflict).

### PostgreSQL MERGE (od 15+)

```sql
MERGE INTO measurements AS target
USING staging AS source
ON target.machine_id = source.machine_id
   AND target.device_id = source.device_id
   AND target.timestamp = source.timestamp
WHEN MATCHED THEN UPDATE SET temperature = source.temperature
WHEN NOT MATCHED THEN INSERT VALUES (source.*);
```

### MongoDB

```javascript
db.measurements.updateOne(
  { machine_id: "stroj_1", device_id: "dev_01", timestamp: ISODate("2024-01-15T10:30:00") },
  { $set: { temperature: 45.2, status: "running" } },
  { upsert: true }
);
```

**Batch:**
```javascript
db.measurements.bulkWrite([
  { updateOne: {
      filter: { machine_id: "stroj_1", device_id: "dev_01", timestamp: ... },
      update: { $set: { temperature: 45.2 } },
      upsert: true
  }}
]);
```

### SQLite (poc01)

```sql
INSERT OR REPLACE INTO measurements (machine_id, device_id, timestamp, temperature, status)
VALUES ('stroj_1', 'dev_01', '2024-01-15 10:30:00', 45.2, 'running');
```

Vyzaduje PRIMARY KEY nebo UNIQUE constraint na (machine_id, device_id, timestamp).
