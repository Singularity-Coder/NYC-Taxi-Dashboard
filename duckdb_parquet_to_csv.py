"""
DuckDB (Python) Parquet -> CSV batch converter (CSV only, minimal)

- Recursively scans an input directory for *.parquet files
- Writes CSVs to a mirrored path under the output directory
- No gzip, no extra CSV knobs—just plain CSV with header and comma delimiter
- Overwrite control and basic progress logs
- Configurable DuckDB threads

Install:
  python3 -m pip install duckdb

Usage:
  python3 parquet_to_csv_duckdb_min.py \
    --in-dir "/Volumes/alienHD/parquet_output" \
    --out-dir "/Volumes/alienHD/csv_from_parquet" \
    --threads 4
"""

import sys
import time
import argparse
from pathlib import Path
import duckdb


def sql_quote(s: str) -> str:
    return s.replace("'", "''")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def dest_csv_path(out_dir: Path, in_dir: Path, src: Path) -> Path:
    rel = src.relative_to(in_dir)
    return out_dir / rel.with_suffix(".csv")


def parquet_to_csv(con: duckdb.DuckDBPyConnection, src: Path, dst: Path) -> None:
    ensure_parent(dst)
    src_q = sql_quote(str(src))
    dst_q = sql_quote(str(dst))
    # Plain CSV: header, comma delimiter
    con.execute(
        f"COPY (SELECT * FROM read_parquet('{src_q}')) "
        f"TO '{dst_q}' (FORMAT CSV, HEADER TRUE, DELIMITER ',');"
    )


def main():
    ap = argparse.ArgumentParser(description="Convert Parquet files to CSV using DuckDB (CSV only).")
    ap.add_argument("--in-dir",  required=True, help="Input root directory containing Parquet files")
    ap.add_argument("--out-dir", required=True, help="Output root directory for CSV files")
    ap.add_argument("--threads", type=int, default=4, help="DuckDB PRAGMA threads")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing CSV files")
    args = ap.parse_args()

    in_dir  = Path(args.in_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not in_dir.exists():
        print(f"Input directory not found: {in_dir}", file=sys.stderr)
        sys.exit(1)

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={args.threads};")

    total = 0
    converted = 0
    skipped = 0
    t0_all = time.time()

    parquet_files = list(in_dir.rglob("*.parquet"))
    for src in parquet_files:
        total += 1
        dst = dest_csv_path(out_dir, in_dir, src)

        if dst.exists() and not args.overwrite:
            print(f"↷ Skipping (exists): {dst}")
            skipped += 1
            continue

        print(f"→ Converting: {src}")
        t0 = time.time()
        try:
            parquet_to_csv(con, src, dst)
            secs = time.time() - t0
            # rough throughput using Parquet size on disk
            try:
                size_mb = src.stat().st_size / (1024 * 1024)
                mbps = size_mb / secs if secs > 0 else 0.0
                print(f"✔ Wrote: {dst}  |  {secs:.1f}s  |  ~{mbps:.1f} MB/s (from Parquet size)")
            except Exception:
                print(f"✔ Wrote: {dst}  |  {secs:.1f}s")
            converted += 1
        except Exception as e:
            print(f"✖ Failed: {src}  |  Reason: {e}")

    secs_all = time.time() - t0_all
    print(f"\nDone. Total: {total}, Converted: {converted}, Skipped: {skipped}, Elapsed: {secs_all/3600:.2f} h")


if __name__ == "__main__":
    main()
