"""Serving layer: push Gold marts into PostgreSQL warehouse for BI/serving consumption."""
from pyspark.sql import SparkSession, functions as F

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

    for table_name, df in targets:
        (
            df.write.format("jdbc")
            .options(**PG_OPTS, dbtable=table_name)
            .mode("overwrite")
            .save()
        )
        print(f"[serve] {table_name}: {df.count():,} rows -> postgres")

    spark.stop()


if __name__ == "__main__":
    main()
