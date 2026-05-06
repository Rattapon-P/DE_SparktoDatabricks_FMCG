-- Gold layer: 4 consumption-ready marts (one mart = one stakeholder need)
USE CATALOG workspace;
USE SCHEMA default;

-- Mart 1: department-level KPI for category managers
CREATE OR REPLACE TABLE gold_dept_kpi
USING DELTA AS
SELECT
    department,
    COUNT(*)                                                AS total_items_sold,
    COUNT(DISTINCT order_id)                                AS unique_orders,
    COUNT(DISTINCT user_id)                                 AS unique_customers,
    SUM(reordered)                                          AS reorder_count,
    ROUND(AVG(CAST(reordered AS DOUBLE)), 4)                AS reorder_rate,
    ROUND(COUNT(*) / COUNT(DISTINCT order_id), 2)           AS avg_items_per_order,
    CURRENT_TIMESTAMP()                                     AS gold_loaded_at
FROM silver_fact_order_items
WHERE department IS NOT NULL
GROUP BY department
ORDER BY reorder_rate DESC;

-- Mart 2: SKU performance for merchandising
CREATE OR REPLACE TABLE gold_sku_performance
USING DELTA AS
SELECT
    f.product_id,
    p.product_name,
    f.department,
    p.aisle,
    COUNT(*)                                       AS total_sold,
    COUNT(DISTINCT f.user_id)                      AS unique_buyers,
    ROUND(AVG(CAST(f.reordered AS DOUBLE)), 4)     AS reorder_rate,
    ROUND(AVG(f.add_to_cart_order), 2)             AS avg_cart_position,
    CURRENT_TIMESTAMP()                            AS gold_loaded_at
FROM silver_fact_order_items f
JOIN silver_dim_products      p ON f.product_id = p.product_id
WHERE f.department IS NOT NULL
GROUP BY f.product_id, p.product_name, f.department, p.aisle
ORDER BY total_sold DESC;

-- Mart 3: hourly demand by department for workforce / fulfillment planning
CREATE OR REPLACE TABLE gold_hourly_demand
USING DELTA
PARTITIONED BY (order_dow)
AS
SELECT
    department,
    order_hour_of_day,
    COUNT(DISTINCT order_id)  AS orders,
    COUNT(*)                  AS items,
    CURRENT_TIMESTAMP()       AS gold_loaded_at,
    order_dow
FROM silver_fact_order_items
WHERE department IS NOT NULL
GROUP BY department, order_hour_of_day, order_dow;

-- Mart 4: customer segmentation for CRM / marketing
CREATE OR REPLACE TABLE gold_customer_segment
USING DELTA
PARTITIONED BY (segment)
AS
SELECT
    user_id,
    total_orders,
    avg_days_between_orders,
    CASE
        WHEN avg_days_between_orders <= 7  THEN 'Weekly'
        WHEN avg_days_between_orders <= 14 THEN 'Bi-weekly'
        WHEN avg_days_between_orders <= 30 THEN 'Monthly'
        ELSE 'Occasional'
    END AS frequency_band,
    CURRENT_TIMESTAMP() AS gold_loaded_at,
    CASE
        WHEN total_orders >= 50 THEN 'VIP'
        WHEN total_orders >= 20 THEN 'Loyal'
        WHEN total_orders >= 5  THEN 'Regular'
        ELSE 'New'
    END AS segment
FROM silver_dim_users;

-- Sanity / preview
SELECT * FROM gold_dept_kpi          ORDER BY reorder_rate DESC LIMIT 10;
SELECT * FROM gold_sku_performance   ORDER BY total_sold   DESC LIMIT 10;
SELECT segment, COUNT(*) FROM gold_customer_segment GROUP BY segment;
