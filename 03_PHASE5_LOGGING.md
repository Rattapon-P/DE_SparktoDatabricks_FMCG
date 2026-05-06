# 📝 Phase 5 — Logging & Production Polish

> **Status:** ⏳ Not started
> **Time estimate:** 2 hours
> **Prerequisite:** Phase 3 done (you have working notebooks)

---

## 🎯 Goal

Convert your **notebooks** into **production-style Python scripts** with proper logging — not `print()`. This is what hiring managers look for to distinguish "notebook user" from "data engineer".

---

## 🧠 Why Logging Matters (Concept First)

### Why NOT print()?

| Aspect | `print()` | Logging |
|---|---|---|
| Severity levels | ❌ all same | ✅ DEBUG / INFO / WARNING / ERROR / CRITICAL |
| Toggle in production | ❌ have to delete code | ✅ change config only |
| Timestamp | ❌ no | ✅ automatic |
| Source location | ❌ no | ✅ shows file + line |
| Log to file | ❌ have to redirect manually | ✅ built-in handlers |
| Filter by module | ❌ no | ✅ logger hierarchy |

### Spark logging architecture

Spark has **2 logging layers** — important to understand:

```
┌─────────────────────────────────────────┐
│  Your Python code (driver)              │
│  ├─ Python logging (your business logs) │  ← We control this
│  └─ Spark internal (Java/Scala log4j)   │  ← Configure via log4j.properties
└─────────────────────────────────────────┘
            ↓ submits jobs to
┌─────────────────────────────────────────┐
│  Spark executors (distributed)          │
│  └─ Java log4j (from Spark itself)      │  ← Logs separately on each worker
└─────────────────────────────────────────┘
```

**Key insight:** Python `logging` you set up in your script **only logs from the driver**, NOT from inside transformations that run on executors. That's normal — logs from executors go through Spark's own log4j and appear in Spark UI → Executors → stderr.

For 95% of data engineering work, **driver-side Python logging is enough** because that's where your pipeline orchestration runs. Don't try to log inside `.map()` lambdas — that's an antipattern.

---

## 🛠️ Step-by-Step

### Step 1: Create Project Structure (10 min)

```bash
cd ~/gosoft-de-project
mkdir -p jobs config logs
touch jobs/__init__.py
touch config/__init__.py
touch jobs/silver_transform.py
touch jobs/gold_aggregate.py
touch jobs/run_pipeline.py
touch config/logging_config.py
```

Final structure:
```
jobs/
├── __init__.py
├── silver_transform.py     # Silver layer logic
├── gold_aggregate.py       # Gold layer logic
└── run_pipeline.py         # Main orchestrator

config/
├── __init__.py
└── logging_config.py       # Logging setup

logs/                       # Auto-created log files
```

### Step 2: Logging Configuration (`config/logging_config.py`)

```python
"""Centralized logging configuration for all pipeline jobs."""
import logging
import logging.config
from pathlib import Path
from datetime import datetime


def setup_logging(job_name: str, log_level: str = "INFO") -> logging.Logger:
    """
    Configure logging for a Spark job.
    
    Args:
        job_name: Name used for logger and log file (e.g., "silver_transform")
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    
    Returns:
        Configured logger ready to use
    """
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{job_name}_{timestamp}.log"
    
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "simple": {
                "format": "%(asctime)s | %(levelname)-8s | %(message)s",
                "datefmt": "%H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": log_level,
                "formatter": "simple",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "class": "logging.FileHandler",
                "level": "DEBUG",
                "formatter": "detailed",
                "filename": str(log_file),
                "mode": "w",
                "encoding": "utf-8",
            },
        },
        "loggers": {
            # Quiet down noisy Spark Java loggers
            "py4j": {"level": "WARN"},
            "pyspark": {"level": "WARN"},
        },
        "root": {
            "level": "DEBUG",
            "handlers": ["console", "file"],
        },
    }
    
    logging.config.dictConfig(config)
    logger = logging.getLogger(job_name)
    logger.info(f"Logger initialized — log file: {log_file}")
    return logger
```

### Step 3: Silver Transform Job (`jobs/silver_transform.py`)

```python
"""Silver layer transformation — clean Bronze data into deduped, conformed Delta tables."""
import sys
import time
from pathlib import Path

# Add parent dir to path so config/ can be imported
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, functions as F, DataFrame
from config.logging_config import setup_logging


# ============ Configuration ============
BRONZE = "/home/jovyan/work/data/bronze"
SILVER = "/home/jovyan/work/data/silver"


def get_spark() -> SparkSession:
    """Initialize Spark session with Delta Lake support."""
    import os
    jar_path = f"{os.getcwd()}/postgresql-42.7.0.jar"
    
    return (SparkSession.builder
        .appName("Silver-Transform")
        .config("spark.jars", jar_path)
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate())


def build_dim_products(products_b: DataFrame, aisles_b: DataFrame, departments_b: DataFrame) -> DataFrame:
    """Build dim_products by denormalizing products + aisles + departments."""
    return (products_b
        .join(F.broadcast(aisles_b), "aisle_id", "left")
        .join(F.broadcast(departments_b), "department_id", "left")
        .select(
            F.col("product_id"),
            F.trim(F.col("product_name")).alias("product_name"),
            F.col("aisle_id"), F.col("aisle"),
            F.col("department_id"), F.col("department"),
            F.current_timestamp().alias("silver_loaded_at")
        )
        .filter(F.col("product_name").isNotNull())
        .dropDuplicates(["product_id"]))


def build_dim_users(orders_b: DataFrame) -> DataFrame:
    """Build dim_users from orders aggregation."""
    return (orders_b
        .groupBy("user_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.min("order_number").alias("first_order_number"),
            F.max("order_number").alias("last_order_number"),
            F.round(F.avg("days_since_prior_order"), 2).alias("avg_days_between_orders"),
            F.current_timestamp().alias("silver_loaded_at")
        ))


def build_fact_order_items(order_products_b: DataFrame, orders_b: DataFrame, dim_products: DataFrame) -> DataFrame:
    """Build fact_order_items with cleaning, dedup, and dimension enrichment."""
    return (order_products_b
        .filter(F.col("order_id").isNotNull() & F.col("product_id").isNotNull())
        .dropDuplicates(["order_id", "product_id"])
        .join(orders_b.select("order_id", "user_id", "order_dow",
                              "order_hour_of_day", "days_since_prior_order"),
              on="order_id", how="inner")
        .join(F.broadcast(dim_products.select("product_id", "department", "aisle")),
              on="product_id", how="left")
        .withColumn("silver_loaded_at", F.current_timestamp())
        .select("order_id", "user_id", "product_id",
                "department", "aisle",
                "add_to_cart_order", "reordered",
                "order_dow", "order_hour_of_day", "days_since_prior_order",
                "silver_loaded_at"))


def main():
    logger = setup_logging("silver_transform")
    logger.info("="*60)
    logger.info("Starting Silver layer transformation")
    logger.info("="*60)
    
    start_time = time.time()
    
    try:
        # Initialize Spark
        logger.info("Initializing Spark session...")
        spark = get_spark()
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Spark UI: {spark.sparkContext.uiWebUrl}")
        
        # Read Bronze
        logger.info("Reading Bronze tables...")
        orders_b = spark.read.parquet(f"{BRONZE}/orders")
        order_products_b = spark.read.parquet(f"{BRONZE}/order_products_prior")
        products_b = spark.read.parquet(f"{BRONZE}/products")
        aisles_b = spark.read.parquet(f"{BRONZE}/aisles")
        departments_b = spark.read.parquet(f"{BRONZE}/departments")
        logger.info("All Bronze tables loaded")
        
        # Build & write dim_products
        logger.info("Building dim_products...")
        t0 = time.time()
        dim_products = build_dim_products(products_b, aisles_b, departments_b)
        (dim_products.write.format("delta").mode("overwrite").save(f"{SILVER}/dim_products"))
        count = dim_products.count()
        logger.info(f"✅ dim_products: {count:,} rows in {time.time()-t0:.1f}s")
        
        # Build & write dim_users
        logger.info("Building dim_users...")
        t0 = time.time()
        dim_users = build_dim_users(orders_b)
        (dim_users.write.format("delta").mode("overwrite").save(f"{SILVER}/dim_users"))
        count = dim_users.count()
        logger.info(f"✅ dim_users: {count:,} rows in {time.time()-t0:.1f}s")
        
        # Build & write fact_order_items
        logger.info("Building fact_order_items (largest table)...")
        t0 = time.time()
        fact = build_fact_order_items(order_products_b, orders_b, dim_products)
        (fact.write.format("delta").mode("overwrite")
            .partitionBy("department").save(f"{SILVER}/fact_order_items"))
        count = fact.count()
        logger.info(f"✅ fact_order_items: {count:,} rows in {time.time()-t0:.1f}s")
        
        # Data quality check
        orphan_count = fact.filter(F.col("department").isNull()).count()
        if orphan_count > 0:
            logger.warning(f"Data quality: {orphan_count:,} rows have NULL department ({orphan_count/count*100:.4f}%)")
        
        elapsed = time.time() - start_time
        logger.info("="*60)
        logger.info(f"Silver layer complete in {elapsed:.1f}s")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Silver transform failed: {e}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
```

### Step 4: Gold Aggregate Job (`jobs/gold_aggregate.py`)

Same pattern. Skeleton:

```python
"""Gold layer — pre-aggregated business marts."""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, functions as F
from config.logging_config import setup_logging


SILVER = "/home/jovyan/work/data/silver"
GOLD = "/home/jovyan/work/data/gold"

PG_OPTS = {
    "url": "jdbc:postgresql://pg-warehouse:5432/warehouse",
    "user": "dataeng",
    "password": "dataeng",
    "driver": "org.postgresql.Driver"
}


def get_spark():
    # ... same as silver_transform.py
    pass


def build_dept_kpi(fact_clean):
    return (fact_clean.groupBy("department").agg(
        F.count("*").alias("total_items_sold"),
        F.countDistinct("order_id").alias("unique_orders"),
        F.countDistinct("user_id").alias("unique_customers"),
        F.sum("reordered").alias("reorder_count"),
        F.round(F.avg("reordered"), 4).alias("reorder_rate"),
        F.round(F.count("*") / F.countDistinct("order_id"), 2).alias("avg_items_per_order"),
        F.current_timestamp().alias("gold_loaded_at")
    ).orderBy(F.desc("reorder_rate")))


# ... build_sku_performance, build_hourly_demand, build_customer_segment
# (copy logic from your notebook, wrap in functions)


def main():
    logger = setup_logging("gold_aggregate")
    logger.info("Starting Gold layer aggregation")
    
    try:
        spark = get_spark()
        
        fact = spark.read.format("delta").load(f"{SILVER}/fact_order_items")
        fact_clean = fact.filter(F.col("department").isNotNull())
        logger.info(f"Loaded fact_clean: {fact_clean.count():,} rows")
        
        # Build each mart
        for mart_name, builder, partition_col in [
            ("dept_kpi", lambda: build_dept_kpi(fact_clean), None),
            # ... add others
        ]:
            t0 = time.time()
            df = builder()
            writer = df.write.format("delta").mode("overwrite")
            if partition_col:
                writer = writer.partitionBy(partition_col)
            writer.save(f"{GOLD}/{mart_name}")
            logger.info(f"✅ {mart_name}: {df.count():,} rows in {time.time()-t0:.1f}s")
        
        # Push to Postgres
        logger.info("Pushing to Postgres serving layer...")
        # ... Postgres push code with try/except per table
        
        logger.info("Gold layer complete")
        
    except Exception as e:
        logger.error(f"Gold aggregate failed: {e}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()
```

### Step 5: Pipeline Orchestrator (`jobs/run_pipeline.py`)

```python
"""Orchestrate full Bronze → Silver → Gold pipeline."""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.logging_config import setup_logging
from jobs import silver_transform, gold_aggregate


def main():
    logger = setup_logging("pipeline_run")
    logger.info("🚀 PIPELINE START")
    start = time.time()
    
    try:
        logger.info("Stage 1/2: Silver transform")
        silver_transform.main()
        
        logger.info("Stage 2/2: Gold aggregate")
        gold_aggregate.main()
        
        elapsed = time.time() - start
        logger.info(f"🎉 PIPELINE SUCCESS in {elapsed:.1f}s ({elapsed/60:.1f} min)")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
```

### Step 6: Run It (10 min)

```bash
# From WSL host
docker exec spark-jupyter bash -c "cd /home/jovyan/work && python jobs/silver_transform.py"

# Or for full pipeline
docker exec spark-jupyter bash -c "cd /home/jovyan/work && python jobs/run_pipeline.py"

# Tail logs in another terminal
tail -f ~/gosoft-de-project/logs/silver_transform_*.log
```

---

## 🎓 Concept: Where Logs Show Up

| Log location | What's there |
|---|---|
| `logs/silver_transform_*.log` | Your Python logger output (driver-side) |
| `console stdout` | Same as above (handler routed there) |
| Spark UI → http://localhost:4040 → **Executors** tab → stderr | Spark internal logs (Java log4j) |
| `docker logs spark-jupyter` | Container-level logs |

**Interview question incoming:** *"Where would you look for logs if a Spark job fails?"*

Your answer: *"Three places — Python application logs for business logic and orchestration errors, Spark UI Executors stderr for distributed runtime errors like OOM or task failures, and Spark history server for completed job analysis. In production on Databricks I'd ship logs to a central system like Datadog or CloudWatch."*

---

## ✅ End-of-Phase-5 Checklist

- [ ] `config/logging_config.py` works
- [ ] `jobs/silver_transform.py` runs end-to-end and produces log file
- [ ] `jobs/gold_aggregate.py` runs end-to-end
- [ ] `jobs/run_pipeline.py` orchestrates both
- [ ] Log file shows: timestamps, severity levels, function names, line numbers
- [ ] Tested error path (rename a Bronze folder temporarily, see ERROR log + exit code 1)
- [ ] Git commit: `git commit -m "Phase 5: production-style logging + scripts"`

---

## 🎤 Interview Talking Points

> *"I refactored my notebooks into modular Python scripts with proper logging. Each job uses `logging.dictConfig` with separate handlers for console (INFO+) and file (DEBUG+). I quiet down `py4j` and `pyspark` Java loggers to WARN level so my logs aren't drowned out. The orchestrator script wraps each stage with try/except, exits with code 1 on failure so an Airflow scheduler would mark the task failed. For executor-side logs I rely on Spark UI since Python logging only works on the driver."*
