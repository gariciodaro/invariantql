# InvariantQL

> An invariant remains unchanged under transformation: write a read-only query
> once, preview it locally with DuckDB, and compile the same plan for Spark
> across file-backed and database sources.

InvariantQL keeps one typed, immutable logical plan at its core. SQL is a
frontend; DuckDB and Spark are backends; storage providers, file formats, and
native databases are adapters behind narrow ports. Every pushdown decision is
explained, every unsupported operation is rejected before any data is read,
and nothing is collected into the driver unless you ask for it.

## Install

```bash
uv add invariantql[duckdb]                 # local previews (DuckDB + Arrow)
uv add invariantql[duckdb,spark]           # plus Spark compilation
uv add invariantql[duckdb,azure,delta]     # Azure Blob/ADLS + Delta Lake
```

Extras: `duckdb`, `spark`, `pandas`, `polars`, `azure`, `s3`, `sftp`,
`postgres`, `mysql`, `mongodb`, `neo4j`, `delta`, `iceberg`, `all`.
Importing `invariantql` loads no provider SDK; a missing extra raises
`MissingDependencyError` naming the extra to install.

## Quick start

```python
import invariantql as iql

ctx = iql.Context()
storage = iql.local_storage("/data")
ctx.register_source(iql.file_source("orders", storage, "orders.parquet", iql.ParquetFormat()))

query = ctx.sql("""
    SELECT id, customer, amount * 1.2 AS gross
    FROM orders
    WHERE amount > :min AND customer LIKE 'A%'
    LIMIT 100
""")

print(query.explain())                       # where each operation runs and why
result = query.preview(rows=20, params={"min": 5})   # DuckDB, Arrow batches
frame = result.to_pandas()                   # explicit, bounded materialisation

df = query.compile(engine="spark", params={"min": 5})  # after ctx.use_spark(spark): lazy DataFrame
```

The same `Query` object drives both engines. `query.is_portable("duckdb",
"spark")` tells you whether the plan runs unchanged on both.

### Typed builder instead of SQL

```python
q = (
    ctx.query("orders")
    .where((iql.col("amount") > iql.param("min")) & iql.col("customer").like("A%"))
    .select("id", (iql.col("amount") * 1.2).alias("gross"))
    .limit(100)
)
```

### Sources

```python
iql.file_source("events", iql.azure_blob_storage("acct", "container", sas_token=...), "events/*.json", iql.JsonFormat())
iql.file_source("facts", iql.s3_storage("bucket", key=..., secret=...), "facts", iql.DeltaFormat(version=12))
iql.file_source("dim", iql.local_storage("/lake"), "dim_customer", iql.IcebergFormat())
iql.file_source("feed", iql.sftp_storage("host", username=..., password=...), "feed.xml", iql.XmlFormat(row_tag="item"))
iql.postgres_source("customers", host=..., database=..., table="customers", user=..., password=...)
iql.mysql_source(...); iql.mongodb_source(...); iql.neo4j_source(...)
```

Storage, format, source, and engine vary independently (ADR-0004). A file
source composes a storage adapter and a typed `DataFormat`; a native source
compiles pushed operations to its own query language and streams Arrow.

## The SQL profile (version 1)

One `SELECT` over one registered source: `SELECT * | columns | expr AS alias`,
`WHERE` with `= <> < <= > >= AND OR NOT`, `IS [NOT] NULL`, `[NOT] IN (...)`,
`[NOT] LIKE`, `BETWEEN`, `+ - * /`, named parameters `:name`, typed literals
`DATE '...'` and `TIMESTAMP '...'`, and `LIMIT n`. Joins, subqueries,
aggregates, ordering, functions, and casts are rejected with a stable
diagnostic code before any source is contacted.

Semantics are SQL's: three-valued `NULL` logic, case-sensitive strings and
`LIKE`, floating-point division. Adapters that cannot honour a semantic leave
that operation to the engine; `explain()` shows it as residual work.

## Explain

```
engine=duckdb source=orders executable=True
  0-scan       pushed   @source [PUSHDOWN_FULL] scan source 'orders'
  1-filter     partial  @source [PUSHDOWN_PARTIAL] 1 pushed, 1 residual: (customer LIKE 'A%'): unsupported by scan target (like)
                pushed:   (amount > :min)
                residual: (customer LIKE 'A%')
  2-project    partial  @source [RESIDUAL_COMPUTED_PROJECTION] column pruning pushed; computed or aliased expressions evaluated by the engine
  3-limit      residual @engine [RESIDUAL_LIMIT_AFTER_RESIDUAL_FILTER] a residual predicate must run before the limit
```

`explain().to_dict()` is a stable, versioned structure with reason codes.

## Guarantees the tests enforce

- Dependencies point inward; the domain is standard-library only (FF-01).
- `import invariantql` loads no provider module (FF-02).
- Every adapter passes the shared contract suites (FF-03).
- Plans are immutable and fingerprint deterministically (FF-04).
- No logical operation is ever dropped by the planner (FF-05).
- Only the documented SQL profile yields a plan (FF-11).
- Secrets never reach errors, reprs, plans, or explain output (FF-12).
- DuckDB and Spark agree on the portable corpus (FF-07); Spark compilation
  performs no action (FF-08).

Run them:

```bash
uv sync --all-extras
uv run pytest                      # everything; Spark tests need JAVA_HOME (JDK 17)
uv run pytest -m architecture      # fitness functions only
uv run pytest -m "not spark"       # skip the JVM
uv run lint-imports                # import-linter contracts
```

## Coming from the legacy connectors library

There is no compatibility layer (ADR-0014). `Connector.read(reading_dict,
reading_function_dict)` becomes a registered source plus a query; mutable
option dictionaries become typed `DataFormat` values; pandas is a bounded,
explicit conversion instead of the return type; writes, moves, EDIFACT, and
SAP access are not part of InvariantQL.

## Architecture

See [docs/architecture](docs/architecture/README.md): the component model,
characteristics, fitness functions, and the decision record (ADR-0001 to
ADR-0014).

## Licence

Apache License 2.0.
