#!/usr/bin/env python3
"""
druid_metadata_to_sql.py

Converts a Druid segment metadata dump (obtained via --dump metadata) into
an equivalent SQL CREATE TABLE statement.

Usage:
    python druid_metadata_to_sql.py <metadata_file> [--dialect {ansi,clickhouse,hive}]
    python druid_metadata_to_sql.py <metadata_file> --table-name my_table

The metadata dump is expected to be a JSON file or a newline-delimited JSON
file where each object represents one segment's metadata, as produced by:
    java -cp druid-...jar org.apache.druid.cli.Main tools dump-segment \
         --directory /path/to/segment --dump metadata
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path


# ---------------------------------------------------------------------------
# Druid type → SQL type mappings per dialect
# ---------------------------------------------------------------------------

TYPE_MAP = {
    "ansi": {
        "STRING":       "VARCHAR",
        "string":       "VARCHAR",
        "LONG":         "BIGINT",
        "long":         "BIGINT",
        "FLOAT":        "REAL",
        "float":        "REAL",
        "DOUBLE":       "DOUBLE PRECISION",
        "double":       "DOUBLE PRECISION",
        "COMPLEX":      "BLOB",
        "complex":      "BLOB",
        "hyperUnique":  "BLOB",
        "thetaSketch":  "BLOB",
        "HLLSketch":    "BLOB",
        "quantilesDoublesSketch": "BLOB",
    },
    "clickhouse": {
        "STRING":       "String",
        "string":       "String",
        "LONG":         "Int64",
        "long":         "Int64",
        "FLOAT":        "Float32",
        "float":        "Float32",
        "DOUBLE":       "Float64",
        "double":       "Float64",
        "COMPLEX":      "String",   # store serialised
        "complex":      "String",
        "hyperUnique":  "AggregateFunction(uniq, String)",
        "thetaSketch":  "String",
        "HLLSketch":    "String",
        "quantilesDoublesSketch": "String",
    },
    "hive": {
        "STRING":       "STRING",
        "string":       "STRING",
        "LONG":         "BIGINT",
        "long":         "BIGINT",
        "FLOAT":        "FLOAT",
        "float":        "FLOAT",
        "DOUBLE":       "DOUBLE",
        "double":       "DOUBLE",
        "COMPLEX":      "BINARY",
        "complex":      "BINARY",
        "hyperUnique":  "BINARY",
        "thetaSketch":  "BINARY",
        "HLLSketch":    "BINARY",
        "quantilesDoublesSketch": "BINARY",
    },
}


def map_type(druid_type: str, dialect: str) -> str:
    """Map a Druid column type to the target SQL dialect type."""
    tmap = TYPE_MAP.get(dialect, TYPE_MAP["ansi"])
    # Handle "COMPLEX<foo>" shapes
    base = druid_type.split("<")[0].strip()
    return tmap.get(druid_type, tmap.get(base, f"VARCHAR  -- unknown druid type: {druid_type}"))


# ---------------------------------------------------------------------------
# Metadata parsing
# ---------------------------------------------------------------------------

def load_metadata(path: Path) -> list[dict]:
    """
    Load segment metadata from a file.  Supports:
      - A JSON array  [ {...}, {...} ]
      - Newline-delimited JSON  (one JSON object per line)
      - A single JSON object    {...}
    """
    text = path.read_text(encoding="utf-8").strip()
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
    except json.JSONDecodeError:
        pass

    # Try NDJSON
    segments = []
    for lineno, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            segments.append(json.loads(line))
        except json.JSONDecodeError as exc:
            sys.exit(f"JSON parse error on line {lineno}: {exc}")
    if not segments:
        sys.exit("No segment metadata found in the input file.")
    return segments


def extract_columns(segments: list[dict]) -> dict:
    """
    Merge column definitions from all segments.
    Returns an ordered dict:  column_name -> {"type": ..., "hasMultipleValues": ...}
    Priority: if the same column appears in multiple segments, the first
    non-null / non-unknown type wins.
    """
    merged: dict[str, dict] = {}

    for seg in segments:
        # The metadata object may be the top-level segment descriptor, or
        # it may be nested under a "metadata" key (depending on dump version).
        meta = seg.get("metadata", seg)
        columns: dict = meta.get("columns", {})

        for col_name, col_info in columns.items():
            if col_name not in merged:
                merged[col_name] = dict(col_info)
            else:
                # Fill in missing type info from later segments
                existing = merged[col_name]
                if not existing.get("type") and col_info.get("type"):
                    existing["type"] = col_info["type"]
                if not existing.get("typeName") and col_info.get("typeName"):
                    existing["typeName"] = col_info["typeName"]

    return merged


def get_druid_type(col_info: dict) -> str:
    """Extract the most descriptive type string from a column info dict."""
    # Prefer typeName (e.g. "hyperUnique") over the generic "type" field
    return (
        col_info.get("typeName")
        or col_info.get("type")
        or "STRING"
    )


# ---------------------------------------------------------------------------
# DDL generation
# ---------------------------------------------------------------------------

def build_create_table(
    table_name: str,
    columns: dict,
    dialect: str,
    include_time: bool,
) -> str:
    lines = []

    # Always put __time first if present, or inject it when requested
    ordered_cols = list(columns.items())
    has_time = "__time" in columns

    if include_time and not has_time:
        # Prepend a synthetic __time column
        ordered_cols = [("__time", {"type": "LONG"})] + ordered_cols

    col_defs = []
    comments = []

    for col_name, col_info in ordered_cols:
        druid_type = get_druid_type(col_info)
        sql_type = map_type(druid_type, dialect)
        multi = col_info.get("hasMultipleValues", False)

        # Quote column names that are reserved words or contain spaces
        quoted = f'"{col_name}"' if not col_name.replace("_", "").isalnum() or col_name.upper() in RESERVED else col_name

        col_def = f"  {quoted:<40} {sql_type}"

        if "__time" in col_name:
            if dialect == "clickhouse":
                col_def = f"  {quoted:<40} DateTime64(3, 'UTC')"
            elif dialect == "hive":
                col_def = f"  {quoted:<40} TIMESTAMP"
            else:
                col_def = f"  {quoted:<40} TIMESTAMP"

        if multi:
            col_def += "  -- multi-value dimension"

        col_defs.append(col_def)

    # Engine / storage clauses per dialect
    if dialect == "clickhouse":
        engine_clause = (
            "\nENGINE = MergeTree()\n"
            'ORDER BY ("__time")\n'
            "SETTINGS index_granularity = 8192;"
        )
    elif dialect == "hive":
        engine_clause = "\nSTORED AS ORC;"
    else:
        engine_clause = ";"

    col_block = ",\n".join(col_defs)
    ddl = f"CREATE TABLE {table_name} (\n{col_block}\n){engine_clause}"
    return ddl


# A minimal set of common SQL reserved words worth quoting
RESERVED = {
    "SELECT", "FROM", "WHERE", "TABLE", "INDEX", "KEY", "ORDER", "GROUP",
    "BY", "HAVING", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON",
    "AS", "AND", "OR", "NOT", "IN", "IS", "NULL", "TRUE", "FALSE",
    "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "PRIMARY",
    "UNIQUE", "DEFAULT", "CONSTRAINT", "FOREIGN", "REFERENCES", "TIME",
    "DATE", "TIMESTAMP", "USER", "VALUE", "VALUES", "SET", "LIKE",
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert a Druid --dump metadata file to a SQL CREATE TABLE statement.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "metadata_file",
        type=Path,
        help="Path to the Druid segment metadata JSON dump.",
    )
    parser.add_argument(
        "--table-name", "-t",
        default=None,
        help="Name for the output table. Defaults to the dataSource field in the metadata, or 'druid_table'.",
    )
    parser.add_argument(
        "--dialect", "-d",
        choices=["ansi", "clickhouse", "hive"],
        default="ansi",
        help="SQL dialect for type mapping (default: ansi).",
    )
    parser.add_argument(
        "--no-time",
        action="store_true",
        help="Do not inject a __time column if one is absent.",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=None,
        help="Write DDL to this file instead of stdout.",
    )
    return parser.parse_args()


def infer_table_name(segments: list[dict]) -> str:
    for seg in segments:
        meta = seg.get("metadata", seg)
        ds = meta.get("dataSource") or seg.get("dataSource")
        if ds:
            return ds
    return "druid_table"


def main():
    args = parse_args()

    if not args.metadata_file.exists():
        sys.exit(f"File not found: {args.metadata_file}")

    segments = load_metadata(args.metadata_file)
    columns = extract_columns(segments)

    if not columns:
        sys.exit("No columns found in the metadata dump.")

    table_name = args.table_name or infer_table_name(segments)
    ddl = build_create_table(
        table_name=table_name,
        columns=columns,
        dialect=args.dialect,
        include_time=not args.no_time,
    )

    # Summary to stderr so it doesn't pollute DDL when piped
    print(f"-- Source file : {args.metadata_file}", file=sys.stderr)
    print(f"-- Segments    : {len(segments)}", file=sys.stderr)
    print(f"-- Columns     : {len(columns)}", file=sys.stderr)
    print(f"-- Dialect     : {args.dialect}", file=sys.stderr)
    print(file=sys.stderr)

    if args.output:
        args.output.write_text(ddl + "\n", encoding="utf-8")
        print(f"DDL written to {args.output}", file=sys.stderr)
    else:
        print(ddl)


if __name__ == "__main__":
    main()
