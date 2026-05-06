"""Serving layer: push Gold marts into PostgreSQL warehouse for BI/serving consumption."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession, functions as F

from config.logging_config import setup_logging

GOLD = "/home/jovyan/work/data/gold"

PG_OPTS = {
    "url": "jdbc:postgresql://pg-warehouse:5432/warehouse",
    "user": "dataeng",
    "password": "dataeng",
    "driver": "org.postgresql.Driver",
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("Serve-Postgres")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def main() -> None:
    logger = setup_logging("serve_postgres")
    logger.info("=" * 60)
    logger.info("Starting Serving layer (Gold -> Postgres)")
    logger.info("=" * 60)

    start = time.time()
    spark = None
    try:
        spark = build_spark()

        dept_kpi = spark.read.format("delta").load(f"{GOLD}/dept_kpi")
        sku_performance = spark.read.format("delta").load(f"{GOLD}/sku_performance")
        hourly_demand = spark.read.format("delta").load(f"{GOLD}/hourly_demand")
        customer_segment = spark.read.format("delta").load(f"{GOLD}/customer_segment")

        top_sku = sku_performance.orderBy(F.desc("total_sold")).limit(1000)

        segment_summary = customer_segment.groupBy("segment", "frequency_band").agg(
            F.count("*").alias("user_count"),
            F.round(F.avg("total_orders"), 1).alias("avg_orders"),
            F.round(F.avg("avg_days_between_orders"), 1).alias("avg_days"),
        )

        targets = [
            ("kpi_dept", dept_kpi),
            ("kpi_sku_top1000", top_sku),
            ("kpi_hourly_demand", hourly_demand),
            ("kpi_customer_segment", segment_summary),
        ]

        failures = []
        for table_name, df in targets:
            t0 = time.time()
            try:
                (
                    df.write.format("jdbc")
                    .options(**PG_OPTS, dbtable=table_name)
                    .mode("overwrite")
                    .save()
                )
                logger.info(
                    "%s: %s rows pushed in %.1fs",
                    table_name,
                    f"{df.count():,}",
                    time.time() - t0,
                )
            except Exception:
                logger.exception("Failed to push %s", table_name)
                failures.append(table_name)

        if failures:
            raise RuntimeError(f"Serving layer failed for tables: {failures}")

        elapsed = time.time() - start
        logger.info("=" * 60)
        logger.info("Serving layer complete in %.1fs", elapsed)
        logger.info("=" * 60)

    except Exception:
        logger.error("Serving layer failed", exc_info=True)
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
