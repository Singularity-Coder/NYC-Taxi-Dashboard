"""
Stack many Parquet files into one single Parquet file, safely handling schema differences and memory-efficiently.

Key features:
- Streams row groups, not entire files (handles 10s–100s of GB without huge RAM)
- Unifies schema across files (adds missing columns as nulls, consistent order)
- Optional timestamp unit coercion (s, ms, us, ns)
- Optional column subset selection

IMPORTANT: 
* Make sure the output directory is different from input ones. 
* Else it will consider the output file as an input file during the process.
* Also make sure input directory has the correct files.
* Also make sure the file names are in the correct order in the input directory.

Here you go—sorted into your two buckets:

# 1) Recursive over a **directory** → write **one file**

* Merge a directory:

  ```bash
  python3 concat_parquet.py \
    --input "data/" \
    --output "out/merged.parquet"
  ```

# 2) Only top-level files via **glob** (no subfolders)

* Merge a glob:

  ```bash
  python3 concat_parquet.py \
    --input "data/part-*.parquet" \
    --output "out/merged.parquet"
  ```
* Keep only specific columns:

  ```bash
  python3 concat_parquet.py \
    --input "data/*.parquet" \
    --output "out/merged.parquet" \
    --columns id ts value
  ```
* Coerce all timestamp columns to microseconds:

  ```bash
  python3 concat_parquet.py \
    --input "data/*.parquet" \
    --output "out/merged.parquet" \
    --coerce-timestamps us
  ```
* Tune row group size & compression:

  ```bash
  python3 concat_parquet.py \
    --input "data/*.parquet" \
    --output "out/merged.parquet" \
    --row-group-size 500000 \
    --compression zstd
  ```

* Only the **directory** form (`--input "data/"`) recurses into subfolders in the provided script. The `*.parquet` / `part-*.parquet` globs match files in the top level of `data/` only.

"""

import argparse
import glob
import os
import sys
from typing import Iterable, List, Optional, Sequence

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq


def discover_inputs(input_glob: str) -> List[str]:
    # Accept directory or glob. If directory, recurse and pick *.parquet
    if os.path.isdir(input_glob):
        files = glob.glob(os.path.join(input_glob, "**", "*.parquet"), recursive=True)
    else:
        files = glob.glob(input_glob)

    files = sorted(set(os.path.abspath(f) for f in files if f.lower().endswith(".parquet")))
    return files


def arrow_schema_from_file(path: str) -> pa.Schema:
    pf = pq.ParquetFile(path)
    return pf.schema_arrow


def unify_all_schemas(files: Sequence[str], keep_columns: Optional[Sequence[str]] = None) -> pa.Schema:
    # Gather schemas from all files
    schemas = []
    for f in files:
        try:
            schemas.append(arrow_schema_from_file(f))
        except Exception as e:
            print(f"Warning: skipping {f} due to schema read error: {e}", file=sys.stderr)

    if not schemas:
        raise RuntimeError("No readable Parquet schemas found in inputs.")

    # Unify schemas across all files
    unified = pa.unify_schemas(schemas)

    # Optionally restrict to a subset (preserve the order provided by user)
    if keep_columns:
        fields = []
        keep_set = set(keep_columns)
        for name in keep_columns:
            if name in unified.names:
                fields.append(unified.field(unified.get_field_index(name)))
            else:
                # Add a nullable null-typed placeholder; will materialize as null column during writes
                fields.append(pa.field(name, pa.null(), nullable=True))
        unified = pa.schema(fields)

    return unified


def coerce_timestamp_units(schema: pa.Schema, unit: Optional[str]) -> pa.Schema:
    if unit is None:
        return schema

    if unit not in {"s", "ms", "us", "ns"}:
        raise ValueError("Timestamp unit must be one of: s, ms, us, ns")

    fields = []
    for f in schema:
        typ = f.type
        if pa.types.is_timestamp(typ) and getattr(typ, "unit", None) != unit:
            # Preserve tz if present
            fields.append(pa.field(f.name, pa.timestamp(unit, tz=typ.tz), nullable=f.nullable, metadata=f.metadata))
        else:
            fields.append(f)
    return pa.schema(fields)


def ensure_table_matches_schema(tbl: pa.Table, target: pa.Schema) -> pa.Table:
    # Add missing columns as null, cast mismatched timestamp units, and order columns
    # Build columns in target order
    cols = []
    for f in target:
        if f.name in tbl.schema.names:
            col = tbl.column(f.name)
            # Cast timestamps to match unit/tz if needed; otherwise let Arrow cast where possible
            if pa.types.is_timestamp(col.type) and pa.types.is_timestamp(f.type):
                if (col.type.unit != f.type.unit) or (getattr(col.type, "tz", None) != getattr(f.type, "tz", None)):
                    col = pa.compute.cast(col, f.type)
            elif col.type != f.type:
                try:
                    col = pa.compute.cast(col, f.type)
                except Exception:
                    # If cast fails, fall back to keeping as-is; writer will error if truly incompatible
                    pass
            cols.append(col)
        else:
            # Create a null column of correct length & type
            length = tbl.num_rows
            if pa.types.is_null(f.type):
                col = pa.nulls(length)
            else:
                col = pa.array([None] * length, type=f.type)
            cols.append(col)

    out = pa.table(cols, schema=target)
    return out


def iter_row_groups(path: str, columns: Optional[Sequence[str]] = None) -> Iterable[pa.Table]:
    pf = pq.ParquetFile(path)
    # Use provided columns; ParquetFile will prune to row group column chunks
    num_rgs = pf.num_row_groups
    for i in range(num_rgs):
        yield pf.read_row_group(i, columns=columns)


def main():
    ap = argparse.ArgumentParser(
        description="Concatenate many Parquet files into one (row-wise) with schema unification."
    )
    ap.add_argument(
    "--input",
    required=True,
    help='Glob or directory, e.g. "data/*.parquet" or "data/"'
    )
    ap.add_argument(
        "-o", "--output", required=True, help="Output file, e.g. out/all.parquet"
    )
    ap.add_argument(
        "-c",
        "--columns",
        nargs="*",
        help="Optional subset of columns to keep (missing columns are filled with nulls)",
    )
    ap.add_argument(
        "--coerce-timestamps",
        choices=["s", "ms", "us", "ns"],
        default=None,
        help="Optionally coerce all timestamp columns to a single unit",
    )
    ap.add_argument(
        "--compression",
        default="zstd",
        help="Parquet compression (zstd, snappy, gzip, brotli, lz4, none). Default: zstd",
    )
    ap.add_argument(
        "--row-group-size",
        type=int,
        default=None,
        help="Optional target row group size when writing (rows). If unset, keep source RGs.",
    )
    args = ap.parse_args()

    files = discover_inputs(args.input)
    if not files:
        print("No parquet files matched.", file=sys.stderr)
        sys.exit(1)

    # Prevent accidental reading the output we are writing
    out_abs = os.path.abspath(args.output)
    files = [f for f in files if os.path.abspath(f) != out_abs]
    if not files:
        print("All inputs resolve to the output path; nothing to do.", file=sys.stderr)
        sys.exit(1)

    # Build unified schema (optionally restricted to requested columns)
    unified = unify_all_schemas(files, keep_columns=args.columns)
    unified = coerce_timestamp_units(unified, args.coerce_timestamps)

    # Ensure output directory exists
    out_dir = os.path.dirname(os.path.abspath(args.output))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    # Prepare constants for fast membership checks
    unified_names = [f.name for f in unified]

    # Create writer with unified schema
    writer = pq.ParquetWriter(
        where=args.output,
        schema=unified,
        compression=None if args.compression.lower() in {"none", "uncompressed"} else args.compression,
        use_dictionary=True,
        write_statistics=True,
    )

    files_written = 0
    rowgroups_written = 0
    total_rows = 0

    try:
        for path in files:
            try:
                wrote_any_rg = False
                for rg_tbl in iter_row_groups(path, columns=unified_names):
                    # Align the rg table to the unified schema (order, types, missing columns)
                    rg_tbl = ensure_table_matches_schema(rg_tbl, unified)

                    # Optionally re-chunk into a target RG size for the output
                    if args.row_group_size and rg_tbl.num_rows > args.row_group_size:
                        # Split into multiple row groups
                        offset = 0
                        while offset < rg_tbl.num_rows:
                            end = min(offset + args.row_group_size, rg_tbl.num_rows)
                            writer.write_table(rg_tbl.slice(offset, end - offset))
                            rowgroups_written += 1
                            total_rows += (end - offset)
                            offset = end
                        wrote_any_rg = True
                    else:
                        writer.write_table(rg_tbl)
                        rowgroups_written += 1
                        total_rows += rg_tbl.num_rows
                        wrote_any_rg = True

                if wrote_any_rg:
                    files_written += 1
            except Exception as e:
                print(f"Warning: failed to append {path}: {e}", file=sys.stderr)

    finally:
        writer.close()

    print(
        f"Wrote {args.output} "
        f"(from {files_written}/{len(files)} files, {rowgroups_written} row-groups, {total_rows} rows)."
    )


if __name__ == "__main__":
    main()
