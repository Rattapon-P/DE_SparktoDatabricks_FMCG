-- Silver layer: cleaned dimensions + fact partitioned by department
USE CATALOG workspace;
USE SCHEMA default;

CREATE OR REPLACE TABLE silver_dim_products
USING DELTA AS
SELECT
    p.product_id,
    TRIM(p.product_name) AS product_name,
    p.aisle_id,
    a.aisle,
    p.department_id,
    d.department,
    CURRENT_TIMESTAMP() AS silver_loaded_at
FROM bronze_products p
LEFT JOIN bronze_aisles      a ON p.aisle_id      = a.aisle_id
LEFT JOIN bronze_departments d ON p.department_id = d.department_id
WHERE p.product_name IS NOT NULL;

CREATE OR REPLACE TABLE silver_dim_users
USING DELTA AS
SELECT
    user_id,
    COUNT(DISTINCT order_id)                       AS total_orders,
    MIN(order_number)                              AS first_order_number,
    MAX(order_number)                              AS last_order_number,
    ROUND(AVG(days_since_prior_order), 2)          AS avg_days_between_orders,
    CURRENT_TIMESTAMP()                            AS silver_loaded_at
FROM bronze_orders
GROUP BY user_id;

-- prior and train have no overlap by design -> UNION ALL skips the sort/dedup step
CREATE OR REPLACE TABLE silver_fact_order_items
USING DELTA
PARTITIONED BY (department)
AS
WITH order_products AS (
    SELECT order_id, product_id, add_to_cart_order, reordered FROM bronze_order_products_prior
    UNION ALL
    SELECT order_id, product_id, add_to_cart_order, reordered FROM bronze_order_products_train
),
deduped AS (
    SELECT DISTINCT order_id, product_id, add_to_cart_order, reordered
    FROM order_products
    WHERE order_id IS NOT NULL AND product_id IS NOT NULL
)
SELECT
    op.order_id,
    o.user_id,
    op.product_id,
    dp.aisle,
    op.add_to_cart_order,
    op.reordered,
    o.order_dow,
    o.order_hour_of_day,
    o.days_since_prior_order,
    CURRENT_TIMESTAMP() AS silver_loaded_at,
    dp.department
FROM deduped op
JOIN bronze_orders     o  ON op.order_id   = o.order_id
LEFT JOIN silver_dim_products dp ON op.product_id = dp.product_id;

SELECT 'silver_dim_products'    AS t, COUNT(*) AS rows FROM silver_dim_products
UNION ALL SELECT 'silver_dim_users',        COUNT(*) FROM silver_dim_users
UNION ALL SELECT 'silver_fact_order_items', COUNT(*) FROM silver_fact_order_items;
