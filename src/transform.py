"""
transform.py
------------
Data Engineering pipeline - Transform stage.

Cleans the raw Voltmart orders dataset for downstream use in a
demand-forecasting model, per the business requirements:

    - Remove orders placed between 12am-5am (inclusive)
    - Convert order_date from timestamp -> date
    - Add time_of_day column (morning / afternoon / evening)
    - Lowercase product and category values
    - Remove discontinued "TV" products
    - Extract purchase_state from purchase_address
    - Compute number of unique states served (n_states)

Usage:
    python src/transform.py
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

RAW_PATH = Path("data/raw/orders_data.parquet")
CLEAN_PATH = Path("data/processed/orders_data_clean.parquet")


def get_spark_session(app_name: str = "orders_pipeline_transform") -> SparkSession:
    return (
        SparkSession
        .builder
        .appName(app_name)
        .getOrCreate()
    )


def add_time_of_day_and_filter_night(df):
    """Add time_of_day column, drop night orders (12am-5am), cast order_date to date."""
    return (
        df
        .withColumn(
            "time_of_day",
            F.when((F.hour("order_date") >= 0) & (F.hour("order_date") <= 5), "night")
             .when((F.hour("order_date") >= 6) & (F.hour("order_date") <= 11), "morning")
             .when((F.hour("order_date") >= 12) & (F.hour("order_date") <= 17), "afternoon")
             .when((F.hour("order_date") >= 18) & (F.hour("order_date") <= 23), "evening")
             .otherwise(None)
        )
        .filter(F.col("time_of_day") != "night")
        .withColumn("order_date", F.col("order_date").cast(types.DateType()))
    )


def clean_product_and_category(df):
    """Lowercase product/category text, drop discontinued TV products."""
    return (
        df
        .withColumn("product", F.lower("product"))
        .withColumn("category", F.lower("category"))
        .filter(~F.col("product").contains("tv"))
    )


def extract_purchase_state(df):
    """Extract the US state abbreviation from purchase_address."""
    return (
        df
        .withColumn("address_split", F.split("purchase_address", " "))
        .withColumn(
            "purchase_state",
            F.col("address_split").getItem(F.size("address_split") - 2)
        )
        .drop("address_split")
    )


def run_pipeline(raw_path: Path = RAW_PATH, clean_path: Path = CLEAN_PATH) -> None:
    spark = get_spark_session()

    logger.info(f"Reading raw data from {raw_path}")
    orders_data = spark.read.parquet(str(raw_path))
    logger.info(f"Raw row count: {orders_data.count()}")

    orders_data = add_time_of_day_and_filter_night(orders_data)
    orders_data = clean_product_and_category(orders_data)
    orders_data = extract_purchase_state(orders_data)

    n_states = orders_data.select("purchase_state").distinct().count()
    logger.info(f"Clean row count: {orders_data.count()} | unique states: {n_states}")

    clean_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Writing cleaned data to {clean_path}")
    (
        orders_data
        .write
        .parquet(str(clean_path), mode="overwrite")
    )

    logger.info("Transform stage complete.")
    spark.stop()


if __name__ == "__main__":
    run_pipeline()
