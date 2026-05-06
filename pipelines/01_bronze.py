"""Bronze layer: raw CSV -> Parquet, preserved as-is for forensic lineage."""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from config.logging_config import setup_logging

RAW = "/home/jovyan/work/data/raw"
BRONZE = "/home/jovyan/work/data/bronze"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("Bronze-Ingestion")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


def main() -> None:
    logger = setup_logging("bronze_ingestion")
    logger.info("=" * 60)
    logger.info("Starting Bronze layer ingestion")
    logger.info("=" * 60)

    start = time.time()
    spark = None
    try:
        spark = build_spark()
        logger.info("Spark version: %s | UI: %s", spark.version, spark.sparkContext.uiWebUrl)
        Path(BRONZE).mkdir(parents=True, exist_ok=True)

        orders_schema = StructType(
            [
                StructField("order_id", IntegerType(), False),
                StructField("user_id", IntegerType(), False),
                StructField("eval_set", StringType(), True),
                StructField("order_number", IntegerType(), True),
                StructField("order_dow", IntegerType(), True),
                StructField("order_hour_of_day", IntegerType(), True),
                StructField("days_since_prior_order", DoubleType(), True),
            ]
        )

        sources = [
            ("aisles", spark.read.csv(f"{RAW}/aisles.csv", header=True, inferSchema=True)),
            ("departments", spark.read.csv(f"{RAW}/departments.csv", header=True, inferSchema=True)),
            ("products", spark.read.csv(f"{RAW}/products.csv", header=True, inferSchema=True)),
            ("orders", spark.read.csv(f"{RAW}/orders.csv", header=True, schema=orders_schema)),
            ("order_products_prior", spark.read.csv(f"{RAW}/order_products__prior.csv", header=True, inferSchema=True)),
            ("order_products_train", spark.read.csv(f"{RAW}/order_products__train.csv", header=True, inferSchema=True)),
        ]

        for name, df in sources:
            t0 = time.time()
            df.write.mode("overwrite").parquet(f"{BRONZE}/{name}")
            logger.info("bronze_%s: %s rows written in %.1fs", name, f"{df.count():,}", time.time() - t0)

        elapsed = time.time() - start
        logger.info("=" * 60)
        logger.info("Bronze layer complete in %.1fs", elapsed)
        logger.info("=" * 60)

    except Exception:
        logger.error("Bronze ingestion failed", exc_info=True)
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
