# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Repository Is

A collection of ClickHouse SQL experiments and a Python utility for migrating from Apache Druid. There is no build system or test framework — the SQL files are meant to be run interactively against a ClickHouse instance.

## Running SQL Files

Execute against a local ClickHouse server:

```bash
clickhouse-client < <file>.sql
# or via HTTP API
curl -X POST 'http://localhost:8123/' --data-binary @<file>.sql
```

## Python Utility: druid_metadata_to_sql.py

Converts Druid segment metadata dumps to SQL `CREATE TABLE` statements.

```bash
python druid_metadata_to_sql.py <metadata_file> [--dialect {ansi,clickhouse,hive}] [--table-name my_table] [--no-time] [--output file.sql]
```

- `--dialect clickhouse` maps Druid types to ClickHouse types (e.g., `STRING` → `String`, `LONG` → `Int64`, `hyperUnique` → `AggregateFunction(uniq, String)`)
- Table name is inferred from the `dataSource` field in the metadata if not specified
- `__time` column is injected as `DateTime64(3, 'UTC')` unless `--no-time` is passed
- Accepts JSON arrays, NDJSON, or single JSON objects as input

## SQL File Contents

| File | Purpose |
|------|---------|
| `is-array-sorted.sql` | UDFs for checking array monotonicity using `arrayFold()` |
| `linear-algebra.sql` | Vector/matrix UDFs (dot product, outer product, matrix multiply, cross product) |
| `bitmap-set-ops.sql` | Bitmap set intersection with `bitmapBuild`/`bitmapAnd` |
| `news-kafka-agg.sql` | Full AggregatingMergeTree + materialized view pattern for streaming aggregation |
| `uk-price-paid.sql` | Loading Parquet data from S3 with `LowCardinality`, `Enum`, partitioning |
| `the-emp-and-dept-tables.sql` | Classic EMP/DEPT test tables with sample data for JOIN testing |
| `unaccent.sql` | Dictionary-based diacritics removal UDF |
| `chc-query-log-all.sql` | Cross-replica query log aggregation with `clusterAllReplicas()` |
| `sessionize-mv-analyzer-bug.sql` | Bug reproduction: materialized view with window functions |

## Key ClickHouse Patterns Used

- **AggregatingMergeTree + materialized views**: State functions (`countIfState`, `uniqState`) written by MV; `*Merge` functions used at query time
- **UDFs**: Defined inline with `CREATE FUNCTION` using lambda syntax
- **Dictionaries**: Created from external sources (e.g., PostgreSQL GitHub raw URLs) for lookup tables
- **Data import**: `s3()` table function for Parquet; URL-based dictionary sources
- **Distributed queries**: `clusterAllReplicas()` and `merge()` for cross-shard aggregation

## Data Files

- `access100.log` — 100 JSON web access log entries for ingestion examples
- `otel_log_sample.json` — OpenTelemetry NDJSON log samples from microservices
