# 🛒 Instacart Medallion Architecture — Spark to Databricks

> End-to-end Data Engineering portfolio project implementing **Bronze → Silver → Gold** lakehouse architecture on **32 million rows** of FMCG transactional data. Built locally with Docker + PySpark, then migrated to **Databricks SQL** with Photon engine.

**Author:** Rattapon Pheanwicha · [LinkedIn](https://www.linkedin.com/in/rattapon-pheanwicha) · [Portfolio](https://rattapon-p.github.io/RattaponP.github.io)

---

## 🎯 Project Goals

This project demonstrates production-grade Data Engineering patterns at retail scale:

1. **Medallion architecture** — proper layer separation (Bronze/Silver/Gold) with clear responsibility per layer
2. **Format progression** — CSV → Parquet → Delta, showing format-aware engineering
3. **Performance engineering** — partitioning, predicate pushdown, columnar optimization
4. **Multi-environment validation** — Local Docker (Spark) + Databricks (Photon)
5. **Data quality awareness** — orphan partition detection, schema verification, sanity checks

---

## 📊 Dataset

**Source:** [Instacart Market Basket Analysis](https://www.kaggle.com/c/instacart-market-basket-analysis) — anonymized grocery orders from 200K+ users.

| Table | Rows | Description |
|---|---|---|
| `orders` | 3.4M | Order metadata (user, time, sequence) |
| `order_products__prior` | 32.4M | Product line items in historical orders |
| `order_products__train` | 1.4M | Product line items in train split |
| `products` | 49K | Product catalog |
| `aisles` | 134 | Aisle taxonomy |
| `departments` | 21 | Department taxonomy |

**Total:** ~37 million transactional rows

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│  RAW CSV (Kaggle)                                       │
│  └─→ Local: /data/raw/                                  │
│  └─→ Databricks: workspace.default (uploaded)           │
└──────────────────┬──────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────────┐
│  BRONZE LAYER — As-is, raw fidelity                     │
│  ─ Format: Parquet (local) / Delta (Databricks)         │
│  ─ Tables: bronze_orders, bronze_products,              │
│            bronze_order_products_prior, bronze_aisles,  │
│            bronze_departments                           │
│  ─ Principle: preserve forensic lineage,                │
│               minimal transformation                    │
└──────────────────┬──────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────────┐
│  SILVER LAYER — Cleaned, joined, validated              │
│  ─ Format: Delta (partitioned by department)            │
│  ─ Tables: silver_dim_products, silver_dim_users,       │
│            silver_fact_order_items                      │
│  ─ Principle: business entities, schema enforcement,    │
│               quality contracts                         │
└──────────────────┬──────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────────┐
│  GOLD LAYER — Consumption-ready marts                   │
│  ─ Format: Delta                                        │
│  ─ Tables: gold_dept_kpi, gold_sku_performance,         │
│            gold_hourly_demand, gold_customer_segment    │
│  ─ Principle: 1 mart = 1 stakeholder need               │
└──────────────────┬──────────────────────────────────────┘
                   ↓
┌─────────────────────────────────────────────────────────┐
│  SERVING LAYER                                          │
│  ─ Local: PostgreSQL (kpi_* tables)                     │
│  ─ Databricks: SQL Warehouse (Photon)                   │
└─────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

### Local Environment
- **Compute:** Docker + Spark 3.5 (single-node)
- **Storage:** Parquet (Bronze) + Delta Lake (Silver/Gold)
- **Orchestration:** Python scripts
- **Serving:** PostgreSQL 15

### Databricks Environment (Free Edition)
- **Compute:** Serverless SQL Warehouse with **Photon engine**
- **Storage:** Delta Lake on managed location
- **Catalog:** Unity Catalog (`workspace.default`)
- **Query:** Databricks SQL Editor

---

## 🎓 Key Engineering Decisions

### 1. Why Medallion?
- **Bronze ≠ Silver ≠ Gold** — different responsibility per layer
- Bronze enables **reprocessing** when Silver logic changes
- Gold isolates **business logic** from data plumbing
- Each layer has own SLA, format choice, partitioning strategy

### 2. Why partition Silver by `department`?
- Most analyst queries filter by department first (`WHERE department = ...`)
- 21 departments = good cardinality (not too few, not too many)
- Predicate pushdown reduces scan to ~5% of total data
- Trade-off: small overhead for queries that ignore department

### 3. Why `UNION ALL` not `UNION`?
- `prior` and `train` splits have **no overlap** by design (Kaggle ML split)
- `UNION ALL` skips the sort/dedup step → ~2x faster on 32M rows
- Lesson: know your data → pick the operator that matches reality

### 4. Why split-and-merge for upload?
- Databricks UI uploader has **100 MB per-file limit**
- Solution: Python `pandas` chunked writer to split → 7 chunks ~80 MB each
- Databricks **auto-merges** files with matching schemas → single Delta table
- No manual `UNION ALL` needed at SQL layer

---

## 🔬 Discoveries on Databricks Free Edition

> Curious skepticism is an engineering virtue. Trust nothing, verify everything.

### Discovery 1: Free Edition runs full Photon + Spark SQL

I verified empirically using:

```sql
-- Spark-only function that fails on other engines
SELECT spark_partition_id() FROM bronze_aisles LIMIT 5;

-- Broadcast hint syntax
SELECT /*+ BROADCAST(bronze_aisles) */ * FROM bronze_aisles LIMIT 5;
```

Both ran successfully → engine is real Spark SQL. Query profile showed `PhotonScan` operators with 100% disk cache hit on warm runs.

### Discovery 2: CSV uploads auto-convert to Delta

The Catalog Explorer UI does **not** keep the original CSV files — they're parsed and written as managed Delta tables. Verified with:

```sql
DESCRIBE DETAIL bronze_orders;
-- format: delta
-- numFiles: 3
-- sizeInBytes: 19,426,066 (≈ 18.5 MiB from 110 MB CSV = 5.66x compression)
```

**Implication:** UI uploader is fine for prototyping, but production ingestion should use **Volumes + COPY INTO** to preserve raw lineage.

### Discovery 3: Multi-file auto-merge

Uploading multiple CSV files with **matching schemas** in a single drag-and-drop creates **one** Delta table containing all rows. No manual UNION needed.

### Discovery 4: Compression ratios on real data

| Format | Size | Ratio |
|---|---|---|
| 3 CSV files | ~110 MB | 1.0x |
| 3 Parquet files (in Delta) | 18.5 MiB | **5.66x** |

Useful for capacity planning at scale.

---

## 📈 Sample Insights

```sql
-- Top 5 departments by reorder rate
SELECT 
    dp.department_name,
    COUNT(*) AS items_sold,
    ROUND(AVG(CAST(reordered AS DOUBLE)) * 100, 2) AS reorder_rate_pct
FROM silver_fact_order_items foi
JOIN silver_dim_products dp ON foi.product_id = dp.product_id
GROUP BY dp.department_name
ORDER BY reorder_rate_pct DESC
LIMIT 5;
```

| Department | Items Sold | Reorder Rate |
|---|---|---|
| produce | ~9.5M | 65% |
| dairy eggs | ~5.4M | 67% |
| beverages | ~2.7M | 64% |
| ... | ... | ... |

---

## 🚀 Run Locally

```bash
# Clone
git clone https://github.com/rattapon/DE_SparktoDatabrick_FMCG.git
cd DE_SparktoDatabrick_FMCG

# Build container
docker compose up -d

# Download Instacart dataset to ./data/raw/

# Run Bronze → Silver → Gold
docker compose exec spark python pipelines/01_bronze.py
docker compose exec spark python pipelines/02_silver.py
docker compose exec spark python pipelines/03_gold.py

# Load to Postgres serving
docker compose exec spark python pipelines/04_serve.py
```

---

## 📋 Run on Databricks

1. Create Databricks Free Edition account
2. Upload CSVs via Catalog Explorer (split files >100 MB)
3. Open SQL Editor and run scripts in `databricks_sql/` folder:
   - `01_bronze.sql` — verify uploaded tables
   - `02_silver.sql` — build silver fact + dimensions
   - `03_gold.sql` — build 4 marts

---

## 🧠 What I Learned

1. **Local-first development is faster** — debugging Spark on my laptop > debugging on Databricks UI
2. **Photon is impressive** — 5.66x compression + sub-second queries on small data
3. **AI tools (Genie) describe what — humans decide why** — query optimization is a syntax problem; schema design is a stakeholder problem
4. **Data quality is layer-aware** — Bronze accepts everything, Silver enforces contracts, Gold validates business KPIs
5. **Pragmatic > Perfect** — UI upload was "wrong" for production but right for portfolio velocity

---

## 📁 Repository Structure

```
DE_SparktoDatabrick_FMCG/
├── data/
│   └── raw/                 # Instacart CSVs (gitignored)
├── pipelines/
│   ├── 01_bronze.py         # Local Bronze ingestion
│   ├── 02_silver.py         # Local Silver transforms
│   ├── 03_gold.py           # Local Gold marts
│   └── 04_serve.py          # PostgreSQL serving
├── databricks_sql/
│   ├── 01_bronze.sql        # Bronze verification
│   ├── 02_silver.sql        # Silver build
│   └── 03_gold.sql          # Gold marts
├── docs/
│   └── architecture.png     # Architecture diagram
├── docker-compose.yml
└── README.md
```

---

## 📌 Status

- ✅ Local pipeline complete (Bronze → Silver → Gold)
- ✅ PostgreSQL serving layer working
- ✅ Databricks Bronze layer migrated (~37M rows)
- ⏳ Databricks Silver + Gold (in progress)
- ⏳ Architecture diagram refresh

---

## 🤝 Built While Preparing for Gosoft Interview

This portfolio was built between **April 29 – May 7, 2026** as part of Data Engineer interview preparation for Gosoft (Thailand) Co., Ltd. — the technology arm of CP All / 7-Eleven Thailand.

The goal: demonstrate not just *what* Medallion architecture is, but *why* each design decision matters at retail scale.

---

*Built with curiosity, verified with skepticism.* 🔬
