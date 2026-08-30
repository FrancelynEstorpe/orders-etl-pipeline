# Project Documentation: Voltmart Orders ETL Pipeline

A complete, step-by-step explanation of what this project is, why it exists,
and how every part of it works — written so you can explain it confidently
in a portfolio, on a resume, or in an interview.

---

## 1. What is this project?

This is a **data engineering pipeline**: a system that takes raw, messy
data and turns it into clean, structured, queryable data that a business
can actually use.

Specifically, it takes a raw e-commerce orders dataset (185,950 orders
from a fictional electronics retailer, "Voltmart") and:

1. **Cleans** it (fixes formatting, removes bad rows, adds useful new columns)
2. **Loads** it into a SQL database
3. Makes it ready for **analysis** (business questions) and **forecasting**
   (predicting future demand)

Think of it like a factory assembly line for data: raw material goes in
one end, a usable product comes out the other.

---

## 2. Why does this project exist? (The business problem)

**Scenario:** Voltmart is an electronics e-commerce company. A Machine
Learning team wants to build a demand-forecasting model — something that
predicts how much of each product will sell in the future, so the company
can plan inventory, staffing, and pricing.

**The problem:** The raw order data isn't usable as-is. It has issues like:

- Timestamps that need to be simplified to dates
- Orders placed at strange hours (12am–5am) that are considered
  unreliable/noise and should be excluded
- Inconsistent text casing (e.g. "TV" vs "tv" vs "Tv")
- A discontinued product ("TV") still appearing in the data
- No easy way to tell which US state an order came from — that
  information is buried inside a full street address

**The role being simulated:** You are acting as a **Data Engineer**. Your
job isn't to build the forecasting model itself — that's the ML team's
job. Your job is to hand them clean, trustworthy, well-structured data
they can build on. This is one of the most common real-world data
engineering tasks.

---

## 3. Why this project matters for a portfolio/job search

Based on analysis of thousands of live Data Engineer job postings in 2026,
the three most commonly required skills are:

- **Data Pipelines / ETL** (appears in ~74% of postings)
- **SQL** (appears in ~71% of postings)
- **Python** (appears in ~71% of postings)

Python and SQL appear together in 58% of all postings — the closest thing
to a single "canonical" Data Engineer skill combination. On top of that,
**PySpark** (the tool used in this project's cleaning stage) is
increasingly preferred over older Spark+Scala combinations and commands a
salary premium.

This project directly demonstrates all three: Python, SQL, and a
PySpark-based ETL pipeline — which is why it's a strong, relevant
portfolio piece rather than a generic tutorial project.

---

## 4. The pipeline, step by step

The project follows the classic **ETL** pattern: **E**xtract, **T**ransform, **L**oad.

```
 EXTRACT              TRANSFORM                LOAD              ANALYZE
┌─────────┐        ┌───────────────┐      ┌─────────────┐    ┌──────────────┐
│ raw     │  --->  │ clean & shape │ ---> │ load into   │--->│ run SQL      │
│ parquet │        │ with PySpark  │      │ SQL database│    │ queries      │
└─────────┘        └───────────────┘      └─────────────┘    └──────────────┘
```

### Step 1: Extract

**What happens:** The pipeline reads the raw dataset,
`orders_data.parquet`, into memory using PySpark.

**Why parquet?** Parquet is a columnar file format widely used in data
engineering because it's compressed, fast to read, and preserves data
types (unlike CSV). It's the industry-standard format for large datasets.

**Where in the code:** `src/transform.py`, function `run_pipeline()`,
the line `spark.read.parquet(...)`.

---

### Step 2: Transform (the core cleaning logic)

This is the heart of the project. Each transformation solves a specific
business requirement.

#### 2a. Handling `order_date` and adding `time_of_day`

- **Problem:** Orders placed between 12am and 5am are considered
  unreliable (possibly bot activity, testing, or data entry errors) and
  need to be excluded from the forecasting model.
- **Solution:** The code inspects the *hour* of each `order_date`
  timestamp and classifies it into `morning`, `afternoon`, `evening`, or
  `night`. Rows classified as `night` are then filtered out entirely.
- **Also:** `order_date` originally includes a precise time
  (e.g. `2023-01-22 21:25:00`). Since the forecasting model only needs
  the day, not the exact time, the column is converted from a
  `timestamp` to a simpler `date` type.
- **Code concept used:** `F.when(...).otherwise(...)` — PySpark's
  equivalent of an if/elif/else statement, applied to every row at once.

#### 2b. Cleaning `product` and `category`

- **Problem:** Text values are inconsistently capitalized (e.g. "TV",
  "tv", "Headphones", "headphones"), which would cause the same product
  to be treated as multiple different products in later analysis.
- **Solution:** Every value in `product` and `category` is converted to
  lowercase.
- **Problem 2:** Voltmart has stopped selling TVs, but old TV orders are
  still in the historical data.
- **Solution:** Any row where `product` contains "tv" is removed.

#### 2c. Extracting `purchase_state`

- **Problem:** The dataset only has a full address string like
  `"944 Walnut St, Boston, MA 02215"`. There's no dedicated "state"
  column, but the business wants to analyze sales by state.
- **Solution:** The address is split into words by spaces. The state
  abbreviation is always the second-to-last "word" in that split
  (right before the zip code), so the code extracts it directly.
- **Bonus:** The code also counts how many *unique* states appear in
  the cleaned dataset (`n_states`), which is a useful summary metric.

**Where in the code:** All of this lives in `src/transform.py`, split
into three separate functions (`add_time_of_day_and_filter_night`,
`clean_product_and_category`, `extract_purchase_state`) so each rule is
isolated, readable, and easy to test independently.

---

### Step 3: Load

**What happens:** The now-clean data (170,921 rows, down from 185,950
after removing night orders and TV products) is written into a SQL
database — specifically **DuckDB**, a lightweight, file-based SQL engine.

**Why DuckDB and not a "real" cloud warehouse?** DuckDB behaves like a
production data warehouse (Snowflake, BigQuery, Redshift) but requires no
account, no cloud setup, and no cost — perfect for a portfolio project
that needs to run anywhere. The code is written so that swapping DuckDB
for a real warehouse later is a small, isolated change (only
`src/load.py` would need updating), not a rewrite of the whole pipeline.

**Where in the code:** `src/load.py` — reads the cleaned parquet file
and writes it into a table called `orders` inside `orders.duckdb`.

---

### Step 4: Analyze

**What happens:** Once the data is loaded into SQL, it can answer real
business questions directly.

**Included queries** (`sql/analysis_queries.sql`):

| # | Question it answers |
|---|---|
| 1 | Which product categories generate the most revenue and margin? |
| 2 | Which US states generate the most revenue? |
| 3 | What time of day do most orders happen, and are they bigger or smaller orders? |
| 4 | What are the top 10 best-selling products by revenue? |
| 5 | What does daily order volume/revenue look like over time? (This is the exact shape of data the ML team would need to build their forecasting model.) |
| 6 | Which categories have the lowest profit margins, and might need a pricing review? |

This step demonstrates that the cleaned data isn't just "tidy" — it's
genuinely *useful* for answering real business questions.

---

### Step 5: Orchestration

**What happens:** `src/run_pipeline.py` runs Transform and then Load in
sequence, with logging at each stage, so the entire pipeline can be
triggered with a single command.

**Why this matters:** In a real company, this kind of pipeline wouldn't
be run manually — it would be scheduled and monitored by an orchestration
tool like **Apache Airflow**. This project is deliberately structured so
that each stage is its own function/script, which makes it
straightforward to convert into an Airflow DAG (a sequence of scheduled
tasks) later without restructuring the code — a natural "next step" to
mention if asked how you'd productionize this.

---

## 5. Results summary

| Metric | Value |
|---|---|
| Raw orders | 185,950 |
| Orders after cleaning | 170,921 |
| Orders removed (night-time) | 15,029 |
| Unique states represented | 8 (CA, GA, MA, ME, NY, OR, TX, WA) |
| Top revenue category | Alimentation (~$7.82M) |
| Top revenue state | California (~$12.42M) |

---

## 6. How to run it yourself

```bash
# 1. Set up environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 2. Run the full pipeline (transform + load)
cd src
python run_pipeline.py

# 3. Query the results
cd ..
duckdb data/processed/orders.duckdb < sql/analysis_queries.sql
```

Requires **Java 8/11/17** installed separately (PySpark runs on the JVM
under the hood).

---

## 7. How to talk about this project in an interview

A concise way to describe it out loud:

> "I built an end-to-end ETL pipeline that cleans a raw e-commerce orders
> dataset using PySpark, applies several business-defined cleaning rules
> — like filtering unreliable orders, standardizing text fields, and
> deriving a new geographic field from a raw address — and loads the
> result into a SQL warehouse. I then wrote SQL queries to answer real
> business questions like revenue by state and category. It's structured
> so each stage is independent, which would make it easy to plug into an
> orchestrator like Airflow in production."

This shows: PySpark competency, understanding of *why* each cleaning
step matters (not just copying code), SQL skills, and awareness of how
this fits into a bigger, real-world system.

---

## 8. Key terms glossary (for reference)

| Term | Meaning |
|---|---|
| **ETL** | Extract, Transform, Load — the standard pattern for moving and cleaning data |
| **PySpark** | Python interface to Apache Spark, used for processing large datasets across multiple machines |
| **Parquet** | A compressed, columnar file format optimized for large datasets |
| **DuckDB** | A lightweight, file-based SQL database, often used for analytics |
| **Data warehouse** | A database optimized for analysis/reporting (e.g. Snowflake, BigQuery, Redshift) |
| **Orchestration** | Scheduling and managing when/how pipeline stages run (e.g. Airflow) |
| **DAG** | "Directed Acyclic Graph" — the structure Airflow uses to represent a pipeline's steps and their order |
