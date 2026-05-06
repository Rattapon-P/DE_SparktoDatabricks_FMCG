"""Silver layer: cleaned dimensions + fact (UNION ALL prior+train), partitioned by department."""
from pyspark.sql import SparkSession, functions as F

BRONZE = "/home/jovyan/work/data/bronze"
SILVER = "/home/jovyan/work/data/silver"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("Silver-Layer")
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

    orders = spark.read.parquet(f"{BRONZE}/orders")
    products = spark.read.parquet(f"{BRONZE}/products")
    aisles = spark.read.parquet(f"{BRONZE}/aisles")
    departments = spark.read.parquet(f"{BRONZE}/departments")
    op_prior = spark.read.parquet(f"{BRONZE}/order_products_prior")
    op_train = spark.read.parquet(f"{BRONZE}/order_products_train")

    # prior and train have no overlap by Kaggle ML split design -> UNION ALL skips dedup sort
    order_products = op_prior.unionByName(op_train)

    dim_products = (
        products.join(F.broadcast(aisles), "aisle_id", "left")
        .join(F.broadcast(departments), "department_id", "left")
        .select(
            F.col("product_id"),
            F.trim(F.col("product_name")).alias("product_name"),
            F.col("aisle_id"),
            F.col("aisle"),
            F.col("department_id"),
            F.col("department"),
            F.current_timestamp().alias("silver_loaded_at"),
        )
        .filter(F.col("product_name").isNotNull())
        .dropDuplicates(["product_id"])
    )
    dim_products.write.format("delta").mode("overwrite").save(f"{SILVER}/dim_products")
    print(f"[silver] dim_products: {dim_products.count():,} rows")

    dim_users = orders.groupBy("user_id").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.min("order_number").alias("first_order_number"),
        F.max("order_number").alias("last_order_number"),
        F.round(F.avg("days_since_prior_order"), 2).alias("avg_days_between_orders"),
        F.current_timestamp().alias("silver_loaded_at"),
    )
    dim_users.write.format("delta").mode("overwrite").save(f"{SILVER}/dim_users")
    print(f"[silver] dim_users: {dim_users.count():,} rows")

    fact_order_items = (
        order_products.filter(
            F.col("order_id").isNotNull() & F.col("product_id").isNotNull()
        )
        .dropDuplicates(["order_id", "product_id"])
        .join(
            orders.select(
                "order_id",
                "user_id",
                "order_dow",
                "order_hour_of_day",
                "days_since_prior_order",
            ),
            on="order_id",
            how="inner",
        )
        .join(
            F.broadcast(dim_products.select("product_id", "department", "aisle")),
            on="product_id",
            how="left",
        )
        .withColumn("silver_loaded_at", F.current_timestamp())
        .select(
            "order_id",
            "user_id",
            "product_id",
            "department",
            "aisle",
            "add_to_cart_order",
            "reordered",
            "order_dow",
            "order_hour_of_day",
            "days_since_prior_order",
            "silver_loaded_at",
        )
    )

    (
        fact_order_items.write.format("delta")
        .mode("overwrite")
        .partitionBy("department")
        .save(f"{SILVER}/fact_order_items")
    )
    print(f"[silver] fact_order_items: {fact_order_items.count():,} rows (partitioned by department)")

    spark.stop()


if __name__ == "__main__":
    main()
