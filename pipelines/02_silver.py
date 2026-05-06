"""Silver layer: cleaned dimensions + fact (UNION ALL prior+train), partitioned by department."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession, functions as F

from config.logging_config import setup_logging

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
    logger = setup_logging("silver_transform")
    logger.info("=" * 60)
    logger.info("Starting Silver layer transformation")
    logger.info("=" * 60)

    start = time.time()
    spark = None
    try:
        spark = build_spark()
        logger.info("Spark version: %s | UI: %s", spark.version, spark.sparkContext.uiWebUrl)

        logger.info("Reading Bronze tables...")
        orders = spark.read.parquet(f"{BRONZE}/orders")
        products = spark.read.parquet(f"{BRONZE}/products")
        aisles = spark.read.parquet(f"{BRONZE}/aisles")
        departments = spark.read.parquet(f"{BRONZE}/departments")
        op_prior = spark.read.parquet(f"{BRONZE}/order_products_prior")
        op_train = spark.read.parquet(f"{BRONZE}/order_products_train")

        # prior and train have no overlap by Kaggle ML split design -> UNION ALL skips dedup sort
        order_products = op_prior.unionByName(op_train)

        logger.info("Building dim_products...")
        t0 = time.time()
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
        logger.info("dim_products: %s rows in %.1fs", f"{dim_products.count():,}", time.time() - t0)

        logger.info("Building dim_users...")
        t0 = time.time()
        dim_users = orders.groupBy("user_id").agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.min("order_number").alias("first_order_number"),
            F.max("order_number").alias("last_order_number"),
            F.round(F.avg("days_since_prior_order"), 2).alias("avg_days_between_orders"),
            F.current_timestamp().alias("silver_loaded_at"),
        )
        dim_users.write.format("delta").mode("overwrite").save(f"{SILVER}/dim_users")
        logger.info("dim_users: %s rows in %.1fs", f"{dim_users.count():,}", time.time() - t0)

        logger.info("Building fact_order_items (largest table)...")
        t0 = time.time()
        fact = (
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
            fact.write.format("delta")
            .mode("overwrite")
            .partitionBy("department")
            .save(f"{SILVER}/fact_order_items")
        )
        fact_count = fact.count()
        logger.info("fact_order_items: %s rows in %.1fs (partitioned by department)", f"{fact_count:,}", time.time() - t0)

        orphan_count = fact.filter(F.col("department").isNull()).count()
        if orphan_count > 0:
            logger.warning(
                "Data quality: %s rows have NULL department (%.4f%% of fact)",
                f"{orphan_count:,}",
                orphan_count / fact_count * 100,
            )

        elapsed = time.time() - start
        logger.info("=" * 60)
        logger.info("Silver layer complete in %.1fs", elapsed)
        logger.info("=" * 60)

    except Exception:
        logger.error("Silver transform failed", exc_info=True)
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
