-- analysis_queries.sql
-- Business-facing queries run against the `orders` table in orders.duckdb
-- Run with: duckdb data/processed/orders.duckdb < sql/analysis_queries.sql

-- 1. Revenue and margin by category
SELECT
    category,
    ROUND(SUM(turnover), 2) AS total_revenue,
    ROUND(SUM(margin), 2)   AS total_margin,
    ROUND(SUM(margin) / SUM(turnover) * 100, 2) AS margin_pct,
    COUNT(*) AS order_count
FROM orders
GROUP BY category
ORDER BY total_revenue DESC;

-- 2. Revenue by state (which markets matter most)
SELECT
    purchase_state,
    ROUND(SUM(turnover), 2) AS total_revenue,
    COUNT(*) AS order_count
FROM orders
GROUP BY purchase_state
ORDER BY total_revenue DESC;

-- 3. Order volume by time of day (staffing / demand patterns)
SELECT
    time_of_day,
    COUNT(*) AS order_count,
    ROUND(AVG(turnover), 2) AS avg_order_value
FROM orders
GROUP BY time_of_day
ORDER BY order_count DESC;

-- 4. Top 10 products by revenue
SELECT
    product,
    ROUND(SUM(turnover), 2) AS total_revenue,
    SUM(quantity_ordered) AS units_sold
FROM orders
GROUP BY product
ORDER BY total_revenue DESC
LIMIT 10;

-- 5. Daily order trend (for forecasting model input)
SELECT
    order_date,
    COUNT(*) AS orders,
    ROUND(SUM(turnover), 2) AS daily_revenue
FROM orders
GROUP BY order_date
ORDER BY order_date;

-- 6. Lowest-margin categories (candidates for pricing review)
SELECT
    category,
    ROUND(AVG(margin / NULLIF(turnover, 0)) * 100, 2) AS avg_margin_pct
FROM orders
GROUP BY category
ORDER BY avg_margin_pct ASC
LIMIT 5;
