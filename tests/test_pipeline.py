"""
test_pipeline.py
-----------------
Lightweight smoke tests for the ETL pipeline. These aren't exhaustive
unit tests - they check that each stage runs and produces data that
satisfies the core business requirements, which is enough to catch
obvious regressions in CI.

Usage:
    pytest tests/
"""

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from load import load_to_warehouse
from transform import run_pipeline as run_transform

RAW_PATH = Path(__file__).resolve().parent.parent / "data" / "raw" / "orders_data.parquet"
CLEAN_PATH = Path(__file__).resolve().parent.parent / "data" / "processed" / "orders_data_clean.parquet"
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "processed" / "orders.duckdb"


def test_raw_data_exists():
    assert RAW_PATH.exists(), "Raw input file is missing"


def test_transform_stage_runs_and_produces_output():
    run_transform(raw_path=RAW_PATH, clean_path=CLEAN_PATH)
    assert CLEAN_PATH.exists(), "Transform stage did not produce output"


def test_cleaned_data_meets_requirements():
    import pandas as pd

    df = pd.read_parquet(CLEAN_PATH)

    # No night-time orders should remain
    assert "night" not in df["time_of_day"].unique()

    # No TV products should remain
    assert not df["product"].str.contains("tv").any()

    # product/category must be lowercase
    assert df["product"].str.islower().all()
    assert df["category"].str.islower().all()

    # purchase_state should be populated (no nulls, plausible 2-letter codes)
    assert df["purchase_state"].notnull().all()
    assert df["purchase_state"].str.len().eq(2).all()

    # Sanity check on row count - cleaning should remove some rows, not all
    assert 0 < len(df) < 185_950


def test_load_stage_populates_warehouse():
    load_to_warehouse(clean_path=CLEAN_PATH, db_path=DB_PATH)

    con = duckdb.connect(str(DB_PATH), read_only=True)
    row_count = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    con.close()

    assert row_count > 0, "Warehouse table 'orders' is empty after load"
