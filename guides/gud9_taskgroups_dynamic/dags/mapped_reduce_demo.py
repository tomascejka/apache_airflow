"""
DAG: mapped_reduce_demo
Demonstrace expand + reduce pattern — map faze → agregace.

Koncepty:
- expand(): map faze — dynamicky pocet tasku
- reduce: sber vysledku z mapped tasku do jednoho
- Kombinace s TaskFlow API
- Realny priklad: zpracovani souboru z adresare
"""

from datetime import datetime, timedelta
import logging

from airflow.sdk import DAG, task

logger = logging.getLogger(__name__)


@task
def list_files():
    """Simulace — vrati seznam souboru ke zpracovani."""
    files = [
        {"name": "batch_001.csv", "rows": 150},
        {"name": "batch_002.csv", "rows": 230},
        {"name": "batch_003.csv", "rows": 89},
        {"name": "batch_004.csv", "rows": 412},
    ]
    logger.info("Nalezeno %d souboru", len(files))
    return files


@task
def process_file(file_info: dict):
    """Zpracuje jeden soubor — map faze."""
    name = file_info["name"]
    rows = file_info["rows"]

    # Simulace zpracovani
    valid_rows = int(rows * 0.95)  # 5% radku je nevalidnich
    logger.info("Soubor %s: %d radku, %d validnich", name, rows, valid_rows)

    return {
        "file": name,
        "total_rows": rows,
        "valid_rows": valid_rows,
        "invalid_rows": rows - valid_rows,
    }


@task
def aggregate_results(results):
    """Agregace vysledku — reduce faze."""
    total_files = len(results)
    total_rows = sum(r["total_rows"] for r in results)
    total_valid = sum(r["valid_rows"] for r in results)
    total_invalid = sum(r["invalid_rows"] for r in results)

    logger.info("=== AGREGACE ===")
    logger.info("Souboru: %d", total_files)
    logger.info("Celkem radku: %d", total_rows)
    logger.info("Validnich: %d (%.1f%%)", total_valid, total_valid / total_rows * 100)
    logger.info("Nevalidnich: %d (%.1f%%)", total_invalid, total_invalid / total_rows * 100)

    return {
        "total_files": total_files,
        "total_rows": total_rows,
        "valid_rows": total_valid,
        "invalid_rows": total_invalid,
    }


with DAG(
    "mapped_reduce_demo",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="Expand + Reduce pattern (map → aggregate)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gud9", "dynamic", "reduce"],
) as dag:

    files = list_files()

    # Map faze — dynamicky pocet tasku
    processed = process_file.expand(file_info=files)

    # Reduce faze — agregace vsech vysledku
    aggregate_results(results=processed)
