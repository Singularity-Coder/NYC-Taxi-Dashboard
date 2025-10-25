"""
DuckDB (Python) CSV -> Parquet batch converter

- Recursively walks an input directory for *.csv
- Writes Parquet files to a mirrored path under the output directory
- Uses DuckDB's COPY with ZSTD compression (level configurable)
- Safe on 16 GB RAM; DuckDB streams CSV -> Parquet
- Used caffeinate so the Mac doesn’t sleep.
- Compression range: 1 to 22. Max is 22 and takes long. Typically 9 is balanced with ~4–6× faster writes with only a small size penalty (a few % to ~10%).
- PRAGMA threads=4 avoids thermal throttling; raise to 6–8 if you want to push harder.
- DuckDB streams CSV → Parquet; typical memory is sub-GB to a few GB, depending on columns.
- DuckDB & PyArrow stream data in chunks; they don’t need to load a 90 GB CSV into RAM. With sensible settings, memory stays well under a few GB.
- 90GB compression at level 22 can take upto 10 hours.

Install:
  python3 -m pip install duckdb

Usage examples:
  python3 duckdb_csv_to_parquet.py \
    --in-dir "/Volumes/alienHD/csv_output" \
    --out-dir "/Volumes/alienHD/parquet_output" \
    --compression zstd --level 22 --threads 4

  # Faster writes, nearly same size:
  python3 duckdb_csv_to_parquet.py --level 9
"""

import os
import time
import argparse
from pathlib import Path
import duckdb

def sql_quote(path: str) -> str:
    # Minimal SQL string literal escaping for file paths
    return path.replace("'", "''")

def convert_one(conn: duckdb.DuckDBPyConnection, src: Path, dst: Path,
                compression: str, level: int, ignore_errors: bool):
    dst.parent.mkdir(parents=True, exist_ok=True)

    src_q = sql_quote(str(src))
    dst_q = sql_quote(str(dst))

    # read_csv_auto options we commonly toggle
    ignore = "TRUE" if ignore_errors else "FALSE"

    sql = f"""
    COPY (
      SELECT * FROM read_csv_auto('{src_q}', ignore_errors={ignore})
    )
    TO '{dst_q}' (
      FORMAT PARQUET,
      COMPRESSION {compression.upper()},
      COMPRESSION_LEVEL {level}
    );
    """
    conn.execute(sql)

def main():
    ap = argparse.ArgumentParser(description="Convert CSV files to Parquet using DuckDB (Python).")
    ap.add_argument("--in-dir",  required=True, help="Input root directory containing CSV files")
    ap.add_argument("--out-dir", required=True, help="Output root directory for Parquet files")
    ap.add_argument("--threads", type=int, default=4, help="DuckDB PRAGMA threads (tune for your CPU/thermals)")
    ap.add_argument("--compression", default="zstd", choices=["zstd", "snappy", "gzip", "brotli", "lz4", "uncompressed"],
                    help="Parquet compression codec (DuckDB supports these)")
    ap.add_argument("--level", type=int, default=22, help="Compression level (ZSTD supports 1–22)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing .parquet files")
    ap.add_argument("--ignore-errors", action="store_true", help="Skip malformed CSV rows instead of failing")
    ap.add_argument("--temp-directory", default=None, help="Optional temp dir for DuckDB spills (e.g., on the SSD)")
    args = ap.parse_args()

    in_dir  = Path(args.in_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not in_dir.exists():
        raise SystemExit(f"Input directory not found: {in_dir}")

    # Single in-memory DuckDB connection (no DB file needed)
    conn = duckdb.connect(database=':memory:')
    conn.execute(f"PRAGMA threads={args.threads};")
    if args.temp_directory:
        td = sql_quote(str(Path(args.temp_directory).expanduser().resolve()))
        conn.execute(f"PRAGMA temp_directory='{td}';")

    # Walk and convert
    total = 0
    converted = 0
    skipped = 0
    t0_all = time.time()

    for src in in_dir.rglob("*.csv"):
        rel = src.relative_to(in_dir)
        dst = out_dir / rel.with_suffix(".parquet")

        total += 1
        if dst.exists() and not args.overwrite:
            print(f"↷ Skipping (exists): {dst}")
            skipped += 1
            continue

        print(f"→ Converting: {src}")
        t0 = time.time()
        try:
            convert_one(conn, src, dst, args.compression, args.level, args.ignore_errors)
            secs = time.time() - t0
            # Throughput estimate (best-effort; uses CSV size on disk)
            try:
                size_bytes = src.stat().st_size
                mbps = (size_bytes / (1024 * 1024)) / secs if secs > 0 else 0.0
                print(f"✔ Wrote: {dst}  |  {secs:.1f}s  |  ~{mbps:.1f} MB/s (from CSV size)")
            except Exception:
                print(f"✔ Wrote: {dst}  |  {secs:.1f}s")
            converted += 1
        except Exception as e:
            print(f"✖ Failed: {src}  |  Reason: {e}")

    secs_all = time.time() - t0_all
    print(f"\nDone. Total: {total}, Converted: {converted}, Skipped: {skipped}, Elapsed: {secs_all/3600:.2f} h")

if __name__ == "__main__":
    main()
