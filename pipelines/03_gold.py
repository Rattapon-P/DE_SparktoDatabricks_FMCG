"""Gold layer: 4 consumption-ready marts (dept KPI, SKU perf, hourly demand, customer segments)."""
from pyspark.sql import SparkSession, functions as F

SILVER = "/home/jovyan/work/data/silver"
GOLD = "/home/jovyan/work/data/gold"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("Gold-Layer")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()

    fact = spark.read.format("delta").load(f"{SILVER}/fact_order_items")
    dim_products = spark.read.format("delta").load(f"{SILVER}/dim_products")
    dim_users = spark.read.format("delta").load(f"{SILVER}/dim_users")

    # Drop orphan partitions (department IS NULL) before building business marts
    fact_clean = fact.filter(F.col("department").isNotNull())

    dept_kpi = (
        fact_clean.groupBy("department")
        .agg(
            F.count("*").alias("total_items_sold"),
            F.countDistinct("order_id").alias("unique_orders"),
            F.countDistinct("user_id").alias("unique_customers"),
            F.sum("reordered").alias("reorder_count"),
            F.round(F.avg("reordered"), 4).alias("reorder_rate"),
            F.round(F.count("*") / F.countDistinct("order_id"), 2).alias("avg_items_per_order"),
            F.current_timestamp().alias("gold_loaded_at"),
        )
        .orderBy(F.desc("reorder_rate"))
    )
    dept_kpi.write.format("delta").mode("overwrite").save(f"{GOLD}/dept_kpi")
    print(f"[gold] dept_kpi: {dept_kpi.count():,} rows")

    sku_performance = (
        fact_clean.groupBy("product_id")
        .agg(
            F.count("*").alias("total_sold"),
            F.countDistinct("user_id").alias("unique_buyers"),
            F.round(F.avg("reordered"), 4).alias("reorder_rate"),
            F.round(F.avg("add_to_cart_order"), 2).alias("avg_cart_position"),
        )
        .join(
            F.broadcast(
                dim_products.select("product_id", "product_name", "department", "aisle")
            ),
            on="product_id",
            how="inner",
        )
        .select(
            "product_id",
            "product_name",
            "department",
            "aisle",
            "total_sold",
            "unique_buyers",
            "reorder_rate",
            "avg_cart_position",
            F.current_timestamp().alias("gold_loaded_at"),
        )
        .orderBy(F.desc("total_sold"))
    )
    sku_performance.write.format("delta").mode("overwrite").save(f"{GOLD}/sku_performance")
    print(f"[gold] sku_performance: {sku_performance.count():,} rows")

    hourly_demand = fact_clean.groupBy(
        "order_dow", "order_hour_of_day", "department"
    ).agg(
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("items"),
        F.current_timestamp().alias("gold_loaded_at"),
    )
    (
        hourly_demand.write.format("delta")
        .mode("overwrite")
        .partitionBy("order_dow")
        .save(f"{GOLD}/hourly_demand")
    )
    print(f"[gold] hourly_demand: {hourly_demand.count():,} rows")

    customer_segment = (
        dim_users.withColumn(
            "segment",
            F.when(F.col("total_orders") >= 50, "VIP")
            .when(F.col("total_orders") >= 20, "Loyal")
            .when(F.col("total_orders") >= 5, "Regular")
            .otherwise("New"),
        )
        .withColumn(
            "frequency_band",
            F.when(F.col("avg_days_between_orders") <= 7, "Weekly")
            .when(F.col("avg_days_between_orders") <= 14, "Bi-weekly")
            .when(F.col("avg_days_between_orders") <= 30, "Monthly")
            .otherwise("Occasional"),
        )
        .select(
            "user_id",
            "total_orders",
            "avg_days_between_orders",
            "segment",
            "frequency_band",
            F.current_timestamp().alias("gold_loaded_at"),
        )
    )
    (
        customer_segment.write.format("delta")
        .mode("overwrite")
        .partitionBy("segment")
        .save(f"{GOLD}/customer_segment")
    )
    print(f"[gold] customer_segment: {customer_segment.count():,} rows")

    spark.stop()


if __name__ == "__main__":
    main()
