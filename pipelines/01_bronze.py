"""Bronze layer: raw CSV -> Parquet, preserved as-is for forensic lineage."""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

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
    spark = build_spark()
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

    aisles = spark.read.csv(f"{RAW}/aisles.csv", header=True, inferSchema=True)
    departments = spark.read.csv(f"{RAW}/departments.csv", header=True, inferSchema=True)
    products = spark.read.csv(f"{RAW}/products.csv", header=True, inferSchema=True)
    orders = spark.read.csv(f"{RAW}/orders.csv", header=True, schema=orders_schema)
    op_prior = spark.read.csv(f"{RAW}/order_products__prior.csv", header=True, inferSchema=True)
    op_train = spark.read.csv(f"{RAW}/order_products__train.csv", header=True, inferSchema=True)

    targets = [
        ("aisles", aisles),
        ("departments", departments),
        ("products", products),
        ("orders", orders),
        ("order_products_prior", op_prior),
        ("order_products_train", op_train),
    ]

    for name, df in targets:
        df.write.mode("overwrite").parquet(f"{BRONZE}/{name}")
        print(f"[bronze] {name}: {df.count():,} rows")

    spark.stop()


if __name__ == "__main__":
    main()
