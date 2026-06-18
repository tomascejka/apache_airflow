# ANA-17: CI/CD pro DAGy — deployment pipeline

## Kontext

DAGy jsou Python kod v Gitu (KAD-01). Jak dostat zmeny z Gitu do Airflow bezpecne? Jak zabranit deployi vadneho DAGu?

## Pipeline

```
Developer → Git push → CI (lint + test) → Merge → CD (deploy do dags/) → Airflow
```

### Faze 1: CI — validace pred merge

| Krok | Nastroj | Co kontroluje |
|------|---------|---------------|
| 1. Lint | `ruff` nebo `flake8` | Syntaxe, style, complexity |
| 2. Import test | `python -c "import dag_file"` | DAG se nacte bez chyb |
| 3. DAG validation | `pytest` + DagBag | Zadne import errors, vsechny tasky maji dependencies |
| 4. Unit testy | `pytest` | Business logika (transform funkce, schema validace) |
| 5. Type check | `mypy` (volitelne) | Typove chyby |

**Minimalni CI (staci pro zacatek):**

```yaml
# .github/workflows/dag-ci.yml
name: DAG CI
on: [pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - run: pip install apache-airflow pytest ruff
      - run: ruff check dags/
      - run: pytest tests/
```

### Faze 2: Testy DAGu

**Tier 1 — DAG integrity (sekundy, kazdy PR):**
```python
# tests/test_dag_integrity.py
from airflow.models import DagBag

def test_no_import_errors():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"Import errors: {dag_bag.import_errors}"

def test_all_dags_have_tags():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"DAG {dag_id} has no tags"
```

**Tier 2 — Unit testy (sekundy, kazdy PR):**
```python
# tests/test_transform.py
def test_normalize_status():
    assert normalize_status("OK") == "ok"
    assert normalize_status("WARN") == "warning"
    assert normalize_status("unknown") == "unknown"

def test_schema_contract():
    data = extract_transform_stroj_1()
    assert "machine_id" in data[0]
    assert "timestamp" in data[0]
```

**Tier 3 — Integration testy (minuty, pred release):**
- Spusti DAG v Docker prostredi
- Overi ze tasky projdou
- Overi vystupni data

### Faze 3: CD — deployment

**Varianta A: Git-sync (DOPORUCENO)**

```
Git repo → git-sync sidecar → /opt/airflow/dags/ → Airflow scheduler cte
```

- `git-sync` kontejner periodicky pulluje z Gitu (napr. kazdych 60s)
- Airflow automaticky nacte nove/zmenene DAGy
- Zadny manualni deploy

**Varianta B: CI/CD kopie (jednoduche)**

```bash
# V CD pipeline (po merge do main)
scp -r dags/ airflow-server:/opt/airflow/dags/
# nebo
rsync -av --delete dags/ airflow-server:/opt/airflow/dags/
```

**Varianta C: Docker image (immutable)**

- DAGy zabalene v Docker image
- Novy deploy = novy image (rolling update)
- Nejbezpecnejsi, ale nejslozitejsi

### Srovnani deployment variant

| Kriterium | Git-sync | SCP/Rsync | Docker image |
|-----------|----------|-----------|--------------|
| Slozitost | Nizka | Nejnizsi | Vysoka |
| Automatizace | Plna (sidecar) | Skript v CI/CD | Plna (build+deploy) |
| Rollback | `git revert` | Kopie stare verze | Stary image tag |
| Immutable | Ne (meni se za behu) | Ne | **Ano** |
| Pro nas use case | **DOPORUCENO** | Start | Produkce at-scale |

## Doporuceni

### Hned (minimalni setup)

1. **DAGy v Git repu** (uz mame — KAD-01)
2. **Ruff lint** v pre-commit hooku nebo CI
3. **test_dag_integrity.py** — import errors test
4. **Manualni deploy** (scp/rsync) nebo git-sync

### Pozdeji (s rostem tymu)

1. **GitHub Actions** CI pipeline (lint + test na kazdem PR)
2. **Git-sync sidecar** v Docker Compose
3. **Branch protection** — main vyzaduje CI pass + code review
4. **Integration testy** v staging prostredi pred deploy do produkce

### Edge worker DAGy

Edge worker taky potrebuje DAGy (`DAGS_FOLDER`). Distribuce:
- **Git-sync** na edge workeru (pull z Gitu)
- **Rsync** z centraly na edge
- **Soucasti Docker image** (pokud edge bezi v kontejneru — ANA-11 Varianta A)

## Souvisejici analyzy

- [KAD-01](KAD-01_code_first_orchestrace.md) — DAGy v Gitu
- [ANA-16](ANA-16_schema_versioning.md) — schema contract testy v CI
- [ANA-05a](ANA-05a_tasky_operace_airflow.md) — struktura tasku
