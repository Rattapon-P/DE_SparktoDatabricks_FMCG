-- Bronze layer verification on Databricks (CSVs auto-converted to managed Delta on upload)
USE CATALOG workspace;
USE SCHEMA default;

SHOW TABLES LIKE 'bronze_*';

SELECT 'bronze_orders' AS table_name, COUNT(*) AS row_count FROM bronze_orders
UNION ALL SELECT 'bronze_products', COUNT(*) FROM bronze_products
UNION ALL SELECT 'bronze_aisles', COUNT(*) FROM bronze_aisles
UNION ALL SELECT 'bronze_departments', COUNT(*) FROM bronze_departments
UNION ALL SELECT 'bronze_order_products_prior', COUNT(*) FROM bronze_order_products_prior
UNION ALL SELECT 'bronze_order_products_train', COUNT(*) FROM bronze_order_products_train;

DESCRIBE DETAIL bronze_order_products_prior;

SELECT spark_partition_id() AS partition_id, COUNT(*) AS rows_in_partition
FROM bronze_order_products_prior
GROUP BY spark_partition_id()
ORDER BY partition_id;
