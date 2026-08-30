"""
load.py
-------
Data Engineering pipeline - Load stage.

Loads the cleaned orders parquet into a DuckDB database so it can be
queried with SQL, the way it would sit in a warehouse (Snowflake /
BigQuery / Redshift) in a production setting. DuckDB is used here so
the project runs anywhere with zero external infra - swap the
connection string for a real warehouse in production.

Usage:
    python src/load.py
"""

import logging
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

CLEAN_PATH = Path("data/processed/orders_data_clean.parquet")
DB_PATH = Path("data/processed/orders.duckdb")
TABLE_NAME = "orders"


def load_to_warehouse(clean_path: Path = CLEAN_PATH, db_path: Path = DB_PATH) -> None:
    logger.info(f"Connecting to warehouse at {db_path}")
    con = duckdb.connect(str(db_path))

    logger.info(f"Loading {clean_path} into table '{TABLE_NAME}'")
    con.execute(f"""
        CREATE OR REPLACE TABLE {TABLE_NAME} AS
        SELECT * FROM read_parquet('{clean_path}/*.parquet')
    """)

    row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    logger.info(f"Loaded {row_count} rows into '{TABLE_NAME}'")

    con.close()
    logger.info("Load stage complete.")


if __name__ == "__main__":
    load_to_warehouse()
