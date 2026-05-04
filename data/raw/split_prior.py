"""
split_prior.py - Split any CSV into N equal parts for Databricks import

Usage:
    python split_prior.py                              # split prior into 10 parts (default)
    python split_prior.py --file orders.csv --parts 3 # split orders into 3 parts
    python split_prior.py --parts 5                    # split prior into 5 parts
"""

import argparse
import os

DIR = os.path.dirname(os.path.abspath(__file__))

FILE_ROW_COUNTS = {
    "order_products__prior.csv": 32_434_489,
    "order_products__train.csv": 1_384_617,
    "orders.csv": 3_421_083,
    "products.csv": 49_688,
}


def split_file(filename: str, parts: int) -> None:
    src = os.path.join(DIR, filename)
    if not os.path.exists(src):
        print(f"[error] file not found: {src}")
        return

    total = FILE_ROW_COUNTS.get(filename)
    if total is None:
        print(f"[info] row count unknown for {filename}, counting now...")
        with open(src, encoding="utf-8") as f:
            total = sum(1 for _ in f) - 1  # exclude header
        print(f"[info] {total:,} data rows found")

    rows_per_part = total // parts
    stem = filename.replace(".csv", "")
    print(f"[split] {filename}: {total:,} rows -> {parts} parts (~{rows_per_part:,} rows each)")

    with open(src, encoding="utf-8") as fin:
        header = fin.readline()
        part_num = 1
        fout = None
        rows_written = 0

        try:
            for i, line in enumerate(fin):
                if fout is None:
                    dst = os.path.join(DIR, f"{stem}_part{part_num}.csv")
                    fout = open(dst, "w", encoding="utf-8")
                    fout.write(header)
                    rows_written = 0

                fout.write(line)
                rows_written += 1

                # close current part when full, except the last part (absorbs remainder)
                if rows_written >= rows_per_part and part_num < parts:
                    fout.close()
                    size_mb = os.path.getsize(os.path.join(DIR, f"{stem}_part{part_num}.csv")) / 1_048_576
                    print(f"  part{part_num}: {rows_written:,} rows  ({size_mb:.1f} MB)")
                    part_num += 1
                    fout = None
        finally:
            if fout and not fout.closed:
                fout.close()
                size_mb = os.path.getsize(os.path.join(DIR, f"{stem}_part{part_num}.csv")) / 1_048_576
                print(f"  part{part_num}: {rows_written:,} rows  ({size_mb:.1f} MB)")

    print(f"[done] {parts} files created in {DIR}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Split a CSV into N parts for Databricks import")
    parser.add_argument("--file", default="order_products__prior.csv",
                        help="CSV filename inside data/raw/ (default: order_products__prior.csv)")
    parser.add_argument("--parts", type=int, default=10,
                        help="number of output parts (default: 10)")
    args = parser.parse_args()
    split_file(args.file, args.parts)


if __name__ == "__main__":
    main()
