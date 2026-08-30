"""
app.py
------
Streamlit dashboard for exploring the cleaned Voltmart orders data.

Reads directly from the DuckDB warehouse produced by the pipeline
(src/load.py), so it always reflects the latest pipeline run.

Usage:
    streamlit run dashboard/app.py
"""

from pathlib import Path

import duckdb
import streamlit as st

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "processed" / "orders.duckdb"

st.set_page_config(page_title="Voltmart Orders Dashboard", layout="wide")


@st.cache_data
def load_data():
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute("SELECT * FROM orders").df()
    con.close()
    return df


st.title("Voltmart Orders Dashboard")
st.caption(
    "Explore the cleaned orders dataset produced by the ETL pipeline "
    "(src/transform.py -> src/load.py)."
)

if not DB_PATH.exists():
    st.error(
        f"No database found at `{DB_PATH}`.\n\n"
        "Run the pipeline first:\n\n"
        "```bash\ncd src\npython run_pipeline.py\n```"
    )
    st.stop()

df = load_data()

# ---- Top-level metrics ----
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total orders", f"{len(df):,}")
col2.metric("Total revenue", f"${df['turnover'].sum():,.0f}")
col3.metric("Total margin", f"${df['margin'].sum():,.0f}")
col4.metric("States covered", df["purchase_state"].nunique())

st.divider()

# ---- Filters ----
with st.sidebar:
    st.header("Filters")
    states = st.multiselect(
        "Purchase state",
        options=sorted(df["purchase_state"].unique()),
        default=None,
    )
    categories = st.multiselect(
        "Category",
        options=sorted(df["category"].unique()),
        default=None,
    )
    time_options = st.multiselect(
        "Time of day",
        options=sorted(df["time_of_day"].unique()),
        default=None,
    )

filtered = df.copy()
if states:
    filtered = filtered[filtered["purchase_state"].isin(states)]
if categories:
    filtered = filtered[filtered["category"].isin(categories)]
if time_options:
    filtered = filtered[filtered["time_of_day"].isin(time_options)]

st.caption(f"Showing {len(filtered):,} of {len(df):,} orders based on current filters.")

# ---- Charts ----
left, right = st.columns(2)

with left:
    st.subheader("Revenue by category")
    rev_by_cat = (
        filtered.groupby("category")["turnover"]
        .sum()
        .sort_values(ascending=False)
    )
    st.bar_chart(rev_by_cat)

with right:
    st.subheader("Revenue by state")
    rev_by_state = (
        filtered.groupby("purchase_state")["turnover"]
        .sum()
        .sort_values(ascending=False)
    )
    st.bar_chart(rev_by_state)

left2, right2 = st.columns(2)

with left2:
    st.subheader("Orders by time of day")
    orders_by_time = filtered["time_of_day"].value_counts()
    st.bar_chart(orders_by_time)

with right2:
    st.subheader("Daily order volume")
    daily = (
        filtered.groupby("order_date")
        .size()
        .rename("orders")
    )
    st.line_chart(daily)

st.divider()

# ---- Top products table ----
st.subheader("Top 10 products by revenue")
top_products = (
    filtered.groupby("product")
    .agg(total_revenue=("turnover", "sum"), units_sold=("quantity_ordered", "sum"))
    .sort_values("total_revenue", ascending=False)
    .head(10)
    .reset_index()
)
top_products["total_revenue"] = top_products["total_revenue"].map(lambda x: f"${x:,.2f}")
st.dataframe(top_products, use_container_width=True, hide_index=True)

# ---- Raw data (optional) ----
with st.expander("View raw filtered data"):
    st.dataframe(filtered, use_container_width=True)
