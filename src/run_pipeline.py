"""
run_pipeline.py
----------------
Orchestrates the full ETL pipeline end to end:

    1. Transform  - PySpark cleaning (src/transform.py)
    2. Load       - load cleaned data into DuckDB warehouse (src/load.py)

This mirrors what a real orchestrator (Airflow, Prefect, Dagster) would
trigger as a DAG of tasks; each stage is a standalone script so it can
be lifted into an Airflow DAG later with minimal changes.

Usage:
    python src/run_pipeline.py
"""

import logging

from load import load_to_warehouse as load_stage
from transform import run_pipeline as transform_stage

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("=== STARTING ORDERS ETL PIPELINE ===")

    logger.info("--- Stage 1/2: Transform ---")
    transform_stage()

    logger.info("--- Stage 2/2: Load ---")
    load_stage()

    logger.info("=== PIPELINE COMPLETE ===")


if __name__ == "__main__":
    main()
