# PoC 6: Alternativy — Airflow vs Prefect vs Dagster

## Popis

Srovnat Airflow s alternativnimi orchestratory (Prefect, Dagster) pro automotive batch ETL use case. Cil: zdokumentovat proc jsme zvolili Airflow a za jakych podminek by davaly smysl alternativy. Analyticky PoC — bez implementace.

## Vysledek

**Airflow je spravna volba** pro nas use case (automotive batch ETL, edge execution, on-premise). Hlavni duvody:

1. **Edge Worker** — jediny orchestrator s nativni podporou remote execution na vyrobni lince
2. **Self-hosted** — plne podporovano bez zavislosti na cloudu
3. **Battle-tested** — 10+ let, nejvetsi ekosystem a komunita
4. **Batch ETL** — puvodni a hlavni ucel Airflow

Prefect a Dagster jsou kvalitni nastroje, ale pro jine use case (cloud-first, modern data stack, male tymy bez edge pozadavku).

## Srovnani (zkracene)

| Kriterium | Airflow | Prefect | Dagster |
|-----------|---------|---------|---------|
| Paradigma | Task-centric | Flow-centric | Asset-centric |
| Edge Worker | Nativni | Neni | Neni |
| Self-hosted | Plne | Mozne (cloud-first) | Mozne |
| Ekosystem | Nejvetsi | Stredni | Mensi |
| Krivka uceni | Stredni | Nizka | Vysoka |

Detaily viz [ANA-01](analyses/ANA-01_srovnani_orchestratoru.md).

## Analyzy

- [DISC-01: Srovnani orchestratoru 2026](analyses/DISC-01_srovnani_orchestratoru_2026.md)
- [DISC-02: Prefect 3 detail](analyses/DISC-02_prefect_3_detail.md)
- [DISC-03: Dagster detail](analyses/DISC-03_dagster_detail.md)
- [DISC-04: Edge/distributed srovnani](analyses/DISC-04_edge_distributed_srovnani.md)
- [ANA-01: Srovnani orchestratoru](analyses/ANA-01_srovnani_orchestratoru.md)
- [OP-01: Alternativni edge reseni](analyses/OP-01_alternativni_edge_reseni.md)
