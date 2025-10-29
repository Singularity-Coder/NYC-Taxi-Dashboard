"""
Split a Parquet file into multiple equal-row Parquet files (streaming; no full-file load).

Usage examples:
  # Split into 2 equal parts (by rows) with snappy compression
    python parquet_split_equal_rows.py \
    --input your_big.parquet \
    --output-dir out_dir \
    --parts 2 \
    --compression zstd \
    --verify


  # Or: ~1,000,000 rows per file
  python split_parquet.py \
    --input your_big.parquet \
    --output-dir out_dir \
    --rows-per-file 1000000 \
    --compression zstd \
    --verify
"""

"""
Parquet splitter (equal by rows, streaming, codec-aware, PyArrow 21+)

- Auto-detects the input Parquet compression codec and uses it unless overridden.
- Streams record batches via dataset.scanner(...).to_batches() (low memory).
- Buffers batches to form sensible row groups to avoid compression inefficiency.
- Ensures each row is written exactly once to exactly one output part.
"""

import argparse
import math
from typing import Optional, List
from pathlib import Path
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


# Map pyarrow parquet metadata compression enum to readable string if needed
_COMPRESSION_NAME_MAP = {
    0: "UNCOMPRESSED", 1: "SNAPPY", 2: "GZIP", 3: "BROTLI", 4: "LZ4",
    5: "ZSTD", 6: "LZ4_RAW"
}

def detect_input_codec(parquet_path: str) -> Optional[str]:
    """Best-effort detection of input file codec from metadata of first row group/column."""
    try:
        pf = pq.ParquetFile(parquet_path)
        if pf.num_row_groups == 0 or pf.metadata.num_columns == 0:
            return None
        comp_enum = pf.metadata.row_group(0).column(0).compression
        # comp_enum can already be a string in newer pyarrow, handle both
        if isinstance(comp_enum, str):
            name = comp_enum.upper()
        else:
            name = _COMPRESSION_NAME_MAP.get(int(comp_enum), None)
        if not name:
            return None
        # Normalize to pyarrow values
        name = name.lower()
        if name == "uncompressed":
            return None
        return name  # 'zstd', 'snappy', 'gzip', etc.
    except Exception:
        return None


def _open_writer(path: Path, schema: pa.Schema, compression: Optional[str]) -> pq.ParquetWriter:
    return pq.ParquetWriter(path.as_posix(), schema=schema, compression=compression)


def split_parquet_equal_rows(
    input_path: str,
    output_dir: str,
    *,
    parts: Optional[int] = None,
    rows_per_file: Optional[int] = None,
    prefix: str = "part",
    compression: Optional[str] = None,
    buffer_target_rows: int = 1_000_000,  # accumulate up to ~1M rows per write for better row groups
) -> int:
    # Validate options
    if (parts is None) == (rows_per_file is None):
        raise ValueError("Specify exactly one of --parts or --rows-per-file.")

    # Discover schema & total rows
    dataset = ds.dataset(input_path, format="parquet")
    schema = dataset.schema
    total_rows = dataset.count_rows()

    # Determine target rows per output file
    if parts is not None:
        if parts <= 0:
            raise ValueError("--parts must be >= 1")
        target_rows = max(1, math.ceil(total_rows / parts))
        max_files = parts
    else:
        if rows_per_file is None or rows_per_file <= 0:
            raise ValueError("--rows-per-file must be >= 1")
        target_rows = int(rows_per_file)
        max_files = None

    # Choose compression: override > autodetect > default(None)
    autodetected = detect_input_codec(input_path)
    codec_to_use = compression or autodetected

    outdir = Path(output_dir)
    outdir.mkdir(parents=True, exist_ok=True)

    file_index = 0
    rows_in_current = 0
    produced = 0
    writer: Optional[pq.ParquetWriter] = None

    # Batch buffer for forming larger row groups
    buffer_batches: List[pa.RecordBatch] = []
    buffered_rows = 0

    def flush_buffer(row_group_size: Optional[int] = None):
        """Write buffered rows to current writer as a table, then clear buffer."""
        nonlocal buffer_batches, buffered_rows, writer
        if buffered_rows == 0:
            return
        table = pa.Table.from_batches(buffer_batches, schema=schema)
        # Use row_group_size to help produce larger, consistent row groups
        # (pyarrow will split the provided table into row groups of this many rows)
        if row_group_size and row_group_size > 0:
            writer.write_table(table, row_group_size=row_group_size)
        else:
            writer.write_table(table)
        buffer_batches.clear()
        buffered_rows = 0

    def start_new_file() -> bool:
        nonlocal writer, file_index, produced, rows_in_current
        if max_files is not None and file_index >= max_files:
            return False
        if writer is not None:
            # Before closing the current file, flush any remaining buffer
            flush_buffer(row_group_size=buffer_target_rows)
            writer.close()
        path = outdir / f"{prefix}-{file_index:05d}.parquet"
        writer = _open_writer(path, schema, codec_to_use)
        file_index += 1
        produced += 1
        rows_in_current = 0
        return True

    if not start_new_file():
        raise RuntimeError("Failed to open first output file.")

    # Stream input in batches
    scanner = dataset.scanner(use_threads=True)
    for batch in scanner.to_batches():
        offset = 0
        n = batch.num_rows
        while offset < n:
            # How many rows can this file still take?
            if max_files is not None and file_index >= max_files:
                capacity = n - offset  # last file: dump the rest
            else:
                capacity = target_rows - rows_in_current
                if capacity <= 0:
                    # Need to roll to a new file
                    if not start_new_file():
                        capacity = n - offset  # parts limit reached: write into last file

            take = min(capacity, n - offset)
            if take > 0:
                slice_batch = batch.slice(offset, take)
                buffer_batches.append(slice_batch)
                buffered_rows += take
                rows_in_current += take
                offset += take

                # If buffer is big, flush as a cohesive set of row groups
                if buffered_rows >= buffer_target_rows:
                    flush_buffer(row_group_size=buffer_target_rows)

            # If we filled current part and are allowed to open a new one, roll
            if (max_files is None or file_index < max_files) and rows_in_current >= target_rows:
                # Flush whatever is buffered for this file so row-groups are formed well
                flush_buffer(row_group_size=buffer_target_rows)
                start_new_file()

    # Final flush and close
    flush_buffer(row_group_size=buffer_target_rows)
    if writer is not None:
        writer.close()

    # Sanity
    if max_files is not None and produced > max_files:
        raise RuntimeError(f"Produced {produced} files, exceeds requested parts={max_files}.")

    print(
        "Split done. Input rows: {:,}. Target per part: {:,}. Files written: {}. Codec: {} (auto: {})."
        .format(total_rows, target_rows, produced, codec_to_use or "uncompressed", autodetected)
    )
    return produced


def verify_outputs(output_dir: str, prefix: str = "part") -> None:
    """Optional: verify row counts and on-disk sizes."""
    outdir = Path(output_dir)
    files = sorted(outdir.glob(f"{prefix}-*.parquet"))
    total = 0
    print("\nVerification:")
    for p in files:
        rows = ds.dataset(p.as_posix(), format="parquet").count_rows()
        size_gb = p.stat().st_size / (1024.0 ** 3)
        total += rows
        print(f"  {p.name}: rows={rows:,}  size={size_gb:.2f} GB")
    print(f"  TOTAL rows across parts: {total:,}")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Split a Parquet file equally by rows (streaming, codec-aware).")
    ap.add_argument("--input", required=True, help="Path to input Parquet file")
    ap.add_argument("--output-dir", required=True, help="Directory to write outputs")
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--parts", type=int, help="Number of equal parts (by rows)")
    group.add_argument("--rows-per-file", type=int, help="Approx. rows per output file")
    ap.add_argument("--compression", default=None, help='Override codec: snappy | zstd | gzip (default: auto-detect input)')
    ap.add_argument("--prefix", default="part", help="Output filename prefix (default: part)")
    ap.add_argument("--verify", action="store_true", help="After splitting, verify counts & sizes")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    split_parquet_equal_rows(
        input_path=args.input,
        output_dir=args.output_dir,
        parts=args.parts,
        rows_per_file=args.rows_per_file,
        prefix=args.prefix,
        compression=args.compression,
    )
    if args.verify:
        verify_outputs(args.output_dir, args.prefix)


if __name__ == "__main__":
    main()
