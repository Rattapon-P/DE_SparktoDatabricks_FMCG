"""
Generate small, referentially-consistent samples from the full Instacart dataset.

The full CSVs in data/raw/ are gitignored (the largest is 577 MB). This script
produces ~100-order samples in data/raw/sample/ so anyone cloning the repo can
run the pipeline end-to-end without downloading the full Kaggle dump.

Run once from the project root:
    python scripts/generate_samples.py
"""
from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
OUT = RAW / "sample"
N_ORDERS = 100


def stream_csv(path: Path):
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            yield header, row


def write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    # 1. Take the first N_ORDERS orders, remember their ids.
    order_ids: set[str] = set()
    orders_rows: list[list[str]] = []
    orders_header: list[str] = []
    for header, row in stream_csv(RAW / "orders.csv"):
        orders_header = header
        if len(orders_rows) >= N_ORDERS:
            break
        orders_rows.append(row)
        order_ids.add(row[0])  # order_id is column 0
    write_csv(OUT / "orders.csv", orders_header, orders_rows)
    print(f"orders.csv         -> {len(orders_rows)} rows")

    # 2. Filter order_products by those order_ids (stream — file is 577 MB).
    product_ids: set[str] = set()
    for name in ("order_products__train.csv", "order_products__prior.csv"):
        kept: list[list[str]] = []
        kept_header: list[str] = []
        for header, row in stream_csv(RAW / name):
            kept_header = header
            if row[0] in order_ids:  # order_id
                kept.append(row)
                product_ids.add(row[1])  # product_id
        write_csv(OUT / name, kept_header, kept)
        print(f"{name:30s} -> {len(kept)} rows")

    # 3. Filter products by referenced product_ids; remember aisle/department ids.
    aisle_ids: set[str] = set()
    dept_ids: set[str] = set()
    products_kept: list[list[str]] = []
    products_header: list[str] = []
    for header, row in stream_csv(RAW / "products.csv"):
        products_header = header
        if row[0] in product_ids:  # product_id
            products_kept.append(row)
            aisle_ids.add(row[2])      # aisle_id
            dept_ids.add(row[3])       # department_id
    write_csv(OUT / "products.csv", products_header, products_kept)
    print(f"products.csv       -> {len(products_kept)} rows")

    # 4. Filter dimension tables.
    for name, ids in (("aisles.csv", aisle_ids), ("departments.csv", dept_ids)):
        kept: list[list[str]] = []
        kept_header: list[str] = []
        for header, row in stream_csv(RAW / name):
            kept_header = header
            if row[0] in ids:
                kept.append(row)
        write_csv(OUT / name, kept_header, kept)
        print(f"{name:18s} -> {len(kept)} rows")


if __name__ == "__main__":
    main()
