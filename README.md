<h1 align="center">InvariantQL</h1>

<p align="center">
  <strong>Write SQL once. Explore across systems without relearning every provider API.</strong><br>
  Read-only SQL for files, object storage, databases, DuckDB, and Spark.
</p>

<p align="center">
  <a href="https://www.python.org/downloads/"><img alt="Python 3.10+" src="https://img.shields.io/badge/Python-3.10%2B-3776AB?style=flat-square&amp;logo=python&amp;logoColor=white"></a>
  <a href="https://github.com/gariciodaro/invariantql/blob/main/LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/License-Apache--2.0-D22128?style=flat-square"></a>
  <img alt="Project status: Alpha" src="https://img.shields.io/badge/Status-Alpha-F59E0B?style=flat-square">
  <a href="https://peps.python.org/pep-0561/"><img alt="Typing: py.typed" src="https://img.shields.io/badge/Typing-py.typed-2563EB?style=flat-square"></a>
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#build-locally-scale-the-pipeline-with-spark">Local to Spark</a> ·
  <a href="#sql-for-ai-agents">AI agents</a> ·
  <a href="#integrations">Integrations</a> ·
  <a href="#use-case-cookbook">Use cases</a> ·
  <a href="#sql-support">SQL support</a> ·
  <a href="#architecture">Architecture</a>
</p>

---

## Stop teaching every caller a new data API

> **SQL in. Typed plan out.** Push down only what is safe, explain every
> decision, and stream bounded previews.

Data exploration code often leaks infrastructure details into every caller:
provider SDKs, credential formats, reader options, and different APIs for local
and distributed execution. InvariantQL moves that wiring into application-owned
source registrations. Callers query logical source names with a deliberately
small SQL profile while InvariantQL builds a typed plan, explains every pushdown
decision, streams bounded local previews, and produces lazy Spark DataFrames.

| Built for | What InvariantQL gives them |
| --- | --- |
| **Data and analytics engineers** | Local exploration that preserves the same query meaning when promoted to Spark. |
| **Platform teams** | One consistent, inspectable read layer over approved files, object stores, and database objects. |
| **Tool and AI-agent developers** | SQL-based exploration without exposing provider SDKs or credential models to callers. |

> **Project status:** InvariantQL is alpha software. Version `0.1.0` supports
> read-only, single-source queries; APIs may evolve before `1.0`.

## Why InvariantQL

| Capability | What it means |
| --- | --- |
| **One query model** | SQL and the typed Python builder produce the same immutable logical plan. |
| **Explainable execution** | Every operation is marked as pushed, partially pushed, residual, or rejected—with a stable reason code and capability evidence. |
| **Bounded exploration** | `preview()` adds a row limit and local results stream as Arrow record batches; materialization is explicit. |
| **Local-to-Spark portability** | DuckDB executes locally; after schema discovery, Spark compilation returns a lazy DataFrame. |
| **Provider isolation** | Storage, file formats, native sources, and engines are independent adapters with optional dependencies. |
| **Read-only by design** | The constrained `SELECT` profile rejects writes, multi-statement input, and unsupported syntax. |

InvariantQL is not a federated query engine. Each query reads one registered
source: a file-backed dataset, one database table or view, one MongoDB
collection, or one Neo4j label.

## Install

InvariantQL requires Python 3.10 or newer. Install local execution and add only
the integrations you need:

```bash
python -m pip install "invariantql[duckdb]"

# Examples
python -m pip install "invariantql[duckdb,postgres]"
python -m pip install "invariantql[duckdb,mongodb]"
python -m pip install "invariantql[duckdb,azure]"
python -m pip install "invariantql[all]"
```

With uv:

```bash
uv add "invariantql[duckdb]"
```

Importing `invariantql` loads no provider SDK. If an integration is missing,
InvariantQL raises `MissingDependencyError` with the extra to install.

## Quick start

Create `data/events.ndjson`:

```jsonl
{"id": 1, "kind": "signup", "score": 91}
{"id": 2, "kind": "purchase", "score": 76}
{"id": 3, "kind": "signup", "score": 88}
```

Register it under a logical name and query it:

```python
import invariantql as iql

with iql.Context() as ctx:
    ctx.register_source(
        iql.file_source(
            "events",
            iql.local_storage("./data"),
            "events.ndjson",
            iql.JsonFormat(),
        )
    )

    query = ctx.sql("""
        SELECT id, kind, score
        FROM events
        WHERE score >= :minimum
        LIMIT 20
    """)

    print(query.explain())

    with query.preview(rows=20, params={"minimum": 80}) as result:
        print(result.rows(max_rows=20))
```

The query returns the two qualifying rows. `query.explain()` shows which work
runs at the source and which work remains for DuckDB.

### Prefer typed Python?

The builder creates the same logical plan. Here is the equivalent query in a
fresh context:

```python
with iql.Context() as ctx:
    ctx.register_source(
        iql.file_source(
            "events",
            iql.local_storage("./data"),
            "events.ndjson",
            iql.JsonFormat(),
        )
    )
    query = (
        ctx.query("events")
        .where(iql.col("score") >= iql.param("minimum"))
        .select("id", "kind", "score")
        .limit(20)
    )
```

## Build locally, scale the pipeline with Spark

Keep source registration and query construction independent from execution.
The example below develops a PostgreSQL extraction as a bounded DuckDB preview,
then rebuilds the **same immutable plan with the same parameters** on a
host-owned Spark session for distributed downstream processing and output. The
shared SQL and business rules do not change; only the runner's terminal
execution adapter changes.

Install `invariantql[duckdb,postgres]` on the laptop and
`invariantql[spark,postgres]` on the cluster. Replace the example table and
column names with your own:

Put the source registration and query in shared application code:

```python
# pipeline.py
import os

import invariantql as iql

PARAMS = {"minimum_id": 1_000_000, "status": "paid"}


def register_sources(ctx: iql.Context) -> None:
    ctx.register_source(
        iql.postgres_source(
            "orders",
            host=os.environ["POSTGRES_HOST"],
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.environ["POSTGRES_DATABASE"],
            schema=os.getenv("POSTGRES_SCHEMA", "public"),
            table=os.environ["POSTGRES_TABLE"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            sslmode=os.getenv("POSTGRES_SSLMODE", "require"),
        )
    )


def orders_extract(ctx: iql.Context) -> iql.Query:
    return ctx.sql("""
        SELECT order_id, customer_id, quantity, net_amount,
               net_amount / quantity AS unit_price
        FROM orders
        WHERE order_id >= :minimum_id
          AND status = :status
          AND quantity IS NOT NULL
    """)
```

### Develop with a bounded local preview

```python
# local_dev.py
import invariantql as iql

from pipeline import PARAMS, orders_extract, register_sources

with iql.Context(duckdb_options={"memory_limit": "1GB", "threads": 4}) as ctx:
    register_sources(ctx)
    query = orders_extract(ctx)

    # PostgreSQL handles safe pushdown; DuckDB evaluates any residual work
    # over streamed Arrow batches.
    with query.preview(rows=200, engine="duckdb", params=PARAMS) as preview:
        print(preview.rows(max_rows=10))
```

### Promote the same plan to a Spark job

```python
# spark_job.py
import os

import invariantql as iql

from pipeline import PARAMS, orders_extract, register_sources

# `spark` is an existing SparkSession owned by the cluster application.
with iql.Context() as ctx:
    register_sources(ctx)
    ctx.use_spark(spark)
    query = orders_extract(ctx)
    frame = query.compile(engine="spark", params=PARAMS)
    frame.explain()  # Inspect Spark's physical pushdown before running a job.

    # `compile()` is lazy. This explicit write starts distributed processing.
    (
        frame.repartition("customer_id")
        .write.mode("append")
        .parquet(os.environ["SPARK_OUTPUT_PATH"])
    )
```

The cluster needs network/TLS access from Spark to the database and a compatible
`org.postgresql:postgresql:<version>` JDBC driver installed before the session
starts; the Python extra does not install JVM artifacts. `SPARK_OUTPUT_PATH`
must be cluster-visible. The write is ordinary Spark code outside InvariantQL's
read-only API.

> **Current PostgreSQL scaling boundary:** version `0.1` does not yet expose
> Spark JDBC `partitionColumn`, bounds, or `numPartitions`. Spark distributes
> downstream transformations and output, but the initial PostgreSQL read uses
> one source partition. For high-volume parallel ingress, land the source in
> ADLS or S3 as Parquet, Delta, or Iceberg and register it under the same logical
> name—the `orders_extract()` query stays unchanged.

## SQL for AI agents

An application can register sources and credentials in trusted host code, then
expose one narrow exploration function to an agent. The agent only needs the
allowed logical source and column names, the supported SQL profile, and named
parameters; it never needs provider credentials or SDK-specific calls.

```python
import json

import invariantql as iql

ctx = iql.Context(preview_rows=50)

# Trusted application startup happens here. Register only sources the caller
# is allowed to explore; source construction and credentials stay host-side.


def source_catalog() -> str:
    try:
        schemas = {name: ctx.query(name).schema().to_dict() for name in ctx.sources}
        return json.dumps(schemas, default=str)
    except iql.InvariantQLError as exc:
        return json.dumps({"error": exc.diagnostic.to_dict()}, default=str)


def explore_sql(sql: str, params: dict[str, object] | None = None) -> str:
    try:
        query = ctx.sql(sql)
        unbound_plan = query.explain()

        if not unbound_plan.executable:
            return json.dumps({"unbound_plan": unbound_plan.to_dict(), "rows": []}, default=str)

        with query.preview(params=params or {}) as result:
            schema = [
                {"name": field.name, "type": str(field.type), "nullable": field.nullable}
                for field in result.schema
            ]
            rows = result.rows(max_rows=50)

        return json.dumps(
            {"schema": schema, "unbound_plan": unbound_plan.to_dict(), "rows": rows},
            default=str,
        )
    except iql.InvariantQLError as exc:
        return json.dumps({"error": exc.diagnostic.to_dict()}, default=str)
```

Expose `source_catalog` and `explore_sql` through the agent framework of your
choice. Catalog discovery can contact a source and may sample records when its
schema is not declared. The explain plan is deliberately created before bound
values are sent to the source; the returned schema comes from the bound local
result. This is a host-side integration pattern available today, not a complete
authorization sandbox: the host application must still control source
registration, access policy, query budgets, and result handling.

An MCP adapter is a possible future interface over this same boundary; MCP
support is not included in `0.1.0`.

## Execution model

```text
SQL or typed expressions
          |
          v
immutable, typed logical plan
          |
          v
capability-aware planner -----> structured explain + diagnostics
          |
          +----> safe projection/filter/limit pushdown
          |
          +----> DuckDB execution ----> Arrow record batches
          |
          `----> Spark compilation ---> lazy DataFrame
```

Storage, format, source, and execution engine vary independently. A file source
combines a storage adapter with a typed `DataFormat`. A native database source
translates safe operations into its own query language and streams the result
through Arrow for local execution.

## Integrations

| Area | Built-in support | Extra |
| --- | --- | --- |
| Local engine | DuckDB with Arrow streaming | `duckdb` |
| Distributed engine | Spark `>=3.5,<4.0` lazy compilation | `spark` |
| Storage | Local filesystem | base package |
| Storage | Azure Blob Storage and ADLS Gen2 | `azure` |
| Storage | Amazon S3 and S3-compatible services | `s3` |
| Storage | SFTP | `sftp` |
| File formats | CSV, NDJSON/JSON arrays, Parquet | `duckdb` (local) / `spark` (Spark) |
| File formats | XML | `xml` |
| Table formats | Delta Lake | `delta` |
| Table formats | Apache Iceberg | `iceberg` |
| Native sources | PostgreSQL | `postgres` |
| Native sources | MySQL | `mysql` |
| Native sources | MongoDB; Azure Cosmos DB for MongoDB endpoints under `*.cosmos.azure.com` | `mongodb` |
| Native sources | Neo4j | `neo4j` |
| Materializers | pandas / Polars | `pandas` / `polars` |

Every database source registration targets one table, view, collection, or
label. SFTP is available for local execution; Spark reports that staging is
required because it cannot read an SFTP URI directly.

## Use-case cookbook

### Explore Parquet in ADLS from a laptop

Use a bounded local preview against an existing cloud object—without first
downloading the whole dataset:

```python
import os

import invariantql as iql

storage = iql.adls_storage(
    os.environ["ADLS_ACCOUNT_NAME"],
    os.environ["ADLS_CONTAINER"],
)

with iql.Context(duckdb_options={"memory_limit": "1GB"}) as ctx:
    ctx.register_source(
        iql.file_source(
            "transactions",
            storage,
            os.environ["ADLS_PARQUET_PATH"],  # Existing object inside the container.
            iql.ParquetFormat(),
        )
    )
    query = ctx.sql("""
        SELECT customer_id, amount
        FROM transactions
        WHERE amount >= :minimum
    """)

    with query.preview(rows=100, params={"minimum": 100}) as result:
        print(result.rows(max_rows=100))
```

This requires `invariantql[duckdb,azure]` and a local Azure identity, such as an
Azure CLI login, or supported `AZURE_STORAGE_*` credentials. With no explicit
credential argument, the adapter uses adlfs's default credential chain. Parquet
schema discovery reads object metadata.

### Swap a local fixture for MongoDB without changing the query

Logical source names isolate query code from provider setup. Develop against a
small fixture, then replace only the registered source:

```python
import os

import invariantql as iql

event_schema = iql.Schema.of(
    ("id", iql.IntegerType()),
    ("kind", iql.StringType()),
    ("score", iql.IntegerType()),
)

with iql.Context() as ctx:
    ctx.register_source(
        iql.file_source(
            "events",
            iql.local_storage("./fixtures"),
            "events.ndjson",
            iql.JsonFormat(schema=event_schema),
        )
    )
    query = ctx.sql("""
        SELECT id, kind, score
        FROM events
        WHERE score >= :minimum
        LIMIT 25
    """)

    with query.preview(params={"minimum": 80}, rows=25) as result:
        print(result.rows(max_rows=25))

    ctx.register_source(
        iql.mongodb_source(
            "events",
            uri=os.environ["MONGODB_URI"],
            database=os.environ["MONGODB_DATABASE"],
            collection=os.environ["MONGODB_COLLECTION"],
            schema=event_schema,
        ),
        replace=True,
    )

    # The existing immutable query now resolves `events` through MongoDB.
    with query.preview(params={"minimum": 80}, rows=25) as result:
        print(result.rows(max_rows=25))
```

The declared schema avoids sampling MongoDB and must match the stored BSON
values. Replacing a source closes the previous adapter.

### Reject filters that would fall back to the local engine

Treat explain output as a policy boundary when a workflow forbids filter
evaluation in DuckDB:

```python
import json

import invariantql as iql


def require_no_residual_filters(query: iql.Query) -> None:
    explain = query.explain(engine="duckdb")
    unsafe_filters = [
        node
        for node in explain.nodes
        if node.operation == "filter"
        and node.disposition is not iql.Disposition.PUSHED
    ]

    if not explain.executable or unsafe_filters:
        raise RuntimeError(
            "Query did not pass the no-residual-filter policy:\n"
            + json.dumps(explain.to_dict(), indent=2)
        )
```

Call `require_no_residual_filters(query)` while the query's `Context` is open.
This prevents fallback to the local engine; `PUSHED` means the scan adapter owns
the operation, not necessarily that a remote server executes it. Some adapters
may perform their own semantic recheck. `explain()` does not execute the result
query, but schema discovery can contact the source. For Spark, use
`DataFrame.explain()` as well to inspect the physical pushdown chosen by its
reader.

## SQL support

The accepted language is one `SELECT` over one registered source:

- `*`, columns, arithmetic expressions, and `AS` aliases;
- `WHERE` with `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, and `NOT`;
- `IS NULL`, `IS NOT NULL`, `IN`, `NOT IN`, `LIKE`, `NOT LIKE`, and `BETWEEN`;
- named parameters such as `:minimum`;
- `DATE` and `TIMESTAMP` literals;
- `LIMIT`.

Joins, subqueries, aggregates, grouping, ordering, functions, general casts,
DDL, DML, and multiple statements are not part of version 1. Unsupported SQL
shape is rejected while parsing, before a source is contacted. Type and
capability validation may inspect source metadata; schema inference for some
sources samples records.

Portable semantics include SQL three-valued `NULL` logic, case-sensitive
strings and `LIKE`, floating-point division with `NULL` on a zero denominator,
UTC-normalized aware timestamps, and exact decimals up to 38 digits. Within the
documented source types and collations, an operation the adapter cannot preserve
is left as residual engine work or rejected; it is never silently dropped.
Provider-specific types or collations outside that contract can differ and must
be configured according to the adapter documentation.

## Streaming and memory

Local results are single-consumption Arrow record-batch streams. For example,
stream a large extraction into Parquet without materializing the full table:

```python
from pathlib import Path

import pyarrow.parquet as pq

import invariantql as iql


def export_parquet(
    query: iql.Query,
    output: Path,
    *,
    params: dict[str, object] | None = None,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    with query.execute(
        engine="duckdb",
        params=params,
        batch_size=65_536,
    ) as result:
        with pq.ParquetWriter(output, result.schema) as writer:
            for batch in result.batches():
                writer.write_batch(batch)
```

Pass a query whose `Context` is still open, for example
`export_parquet(query, Path("./exports/events.parquet"), params={"minimum": 80})`.

Use `preview()` for exploration; it adds a limit of 1,000 rows by default, or
the explicit `rows=` value. `execute()` can be unbounded. The explicit
`rows()`, `to_arrow()`, `to_pandas()`, and `to_polars()` materializers default
to a separate one-million-row safety limit.

## Spark execution

When the registered source is reachable by the cluster, attach an existing
`SparkSession`; InvariantQL does not create or mutate it:

```python
event_schema = iql.Schema.of(
    ("id", iql.IntegerType()),
    ("kind", iql.StringType()),
    ("score", iql.IntegerType()),
)

with iql.Context() as spark_ctx:
    spark_ctx.register_source(
        iql.file_source(
            "events",
            iql.local_storage("./data"),
            "events.ndjson",
            iql.JsonFormat(schema=event_schema),
        )
    )
    spark_ctx.use_spark(spark)
    spark_query = spark_ctx.sql("""
        SELECT id, kind, score
        FROM events
        WHERE score >= :minimum
        LIMIT 20
    """)
    dataframe = spark_query.compile(engine="spark", params={"minimum": 80})

    # The returned DataFrame is lazy. Inspect or continue transforming it.
    dataframe.explain()
```

Compilation needs a bound schema. A declared schema, as above, keeps CSV/JSON
compilation metadata-only. Without one, schema discovery may inspect source
metadata or records and Spark may schedule inference work before it returns the
lazy DataFrame.

Python extras install Python-side support, not JVM artifacts on a Spark
cluster. Supply the matching cluster dependency for each integration and pin
it to the cluster's Spark, Hadoop, and Scala 2.12 versions.

| Integration | Spark cluster dependency |
| --- | --- |
| Local CSV, JSON, Parquet | Built into Spark 3.5 |
| XML | `com.databricks:spark-xml_2.12` |
| Azure Blob or ADLS | `org.apache.hadoop:hadoop-azure` matching Hadoop |
| S3 | `org.apache.hadoop:hadoop-aws` and its matching AWS SDK bundle |
| Delta Lake | `io.delta:delta-spark_2.12` plus Delta session configuration |
| Apache Iceberg | `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12` |
| PostgreSQL | `org.postgresql:postgresql:<version>` |
| MySQL | `com.mysql:mysql-connector-j` |
| MongoDB | `org.mongodb.spark:mongo-spark-connector_2.12` |
| Neo4j | `org.neo4j:neo4j-connector-apache-spark_2.12` for Spark 3 |

Spark itself decides the final physical pushdown performed by its readers.
InvariantQL's explain output describes logical placement; use Spark's
`DataFrame.explain()` to inspect the physical plan.

## Security boundaries

- Credentials stay inside adapters and are redacted from representations,
  errors, logical plans, fingerprints, and explain output.
- Relational SQL and Cypher use driver-bound parameters. MongoDB retains values
  as typed BSON query values. Values are never interpolated into generated
  query strings.
- The Spark adapter never mutates a supplied session. Applying storage
  credentials to Hadoop configuration requires an explicit helper call.
- Source reachability, authorization, credential issuance, governance, and
  Spark cluster configuration remain the host application's responsibility.

## Development

Clone the repository and install the development environment:

```bash
git clone https://github.com/gariciodaro/invariantql.git
cd invariantql
uv sync --all-extras
```

Run the checks:

```bash
uv run pytest
uv run pytest -m architecture
uv run pytest -m "not spark"
uv run ruff check .
uv run pyright
uv run lint-imports
```

Spark tests require a JDK 17 installation and `JAVA_HOME`.

Contributions and early feedback are welcome through
[GitHub issues](https://github.com/gariciodaro/invariantql/issues).

## Architecture

The [architecture documentation](https://github.com/gariciodaro/invariantql/tree/main/docs/architecture)
covers the component model, characteristics, executable fitness functions, and
decision records ADR-0001 through ADR-0013.

## Author and license

InvariantQL was created and is maintained by
[Gari Ciodaro Guerra](https://github.com/gariciodaro).

Licensed under the
[Apache License 2.0](https://github.com/gariciodaro/invariantql/blob/main/LICENSE).
