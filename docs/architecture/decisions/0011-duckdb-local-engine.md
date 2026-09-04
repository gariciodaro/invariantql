# ADR-0011: Use DuckDB as the local execution engine

- **Status:** Accepted
- **Date:** 2026-09-04
- **Decision owners:** InvariantQL maintainers

## Context

ADR-0005 fixes Arrow record batches as the local result boundary but leaves
open which runtime evaluates residual projections, predicates, and limits
locally, and which runtime reads CSV, JSON, and Parquet for bounded previews.
The candidates were PyArrow's dataset and compute modules, Polars, and DuckDB.

The local engine must evaluate the whole portable expression profile with SQL
semantics (three-valued logic, `LIKE`, arithmetic), stream results with bounded
memory, read the three native file formats efficiently with projection and
predicate pushdown into the reader, and consume Arrow streams produced by
native sources and by format handlers such as Delta and Iceberg.

## Decision drivers

- Exact SQL semantics for residual evaluation, matching Spark's behaviour.
- Native readers with real pushdown for CSV, JSON, and Parquet.
- Streaming Arrow output with a configurable batch size.
- Ability to register arbitrary Arrow streams as relations.
- A single optional dependency for the whole local path.

## Decision

The local `ExecutionEngine` is DuckDB. It reads CSV, JSON, and Parquet through
DuckDB's native table functions, reaching any `Storage` adapter through an
fsspec bridge over the `Storage` port (`open_read`, `info`, `list`) so that no
storage adapter depends on DuckDB. Native sources and generic format handlers
hand DuckDB an Arrow stream, which DuckDB registers as a relation; residual
operations are compiled to a single SQL statement with every value bound
through numbered placeholders.

Results are exposed as a `LocalResult` streaming Arrow batches; conversions to
Pandas or Polars are explicit and bounded by a materialisation limit.

The DuckDB engine lives in one optional extra (`invariantql[duckdb]`). The
domain, ports, and application layers do not import DuckDB.

## Alternatives considered

### PyArrow dataset and compute only

Smallest footprint and already required for the Arrow boundary. It lacks a SQL
expression evaluator: `LIKE`, arithmetic on mixed types, and three-valued
logic would have to be re-implemented, duplicating semantics that must match
Spark exactly.

### Polars

Fast and expressive, but its expression semantics (null handling, string
matching, integer division) differ from SQL in places, so parity with Spark
would require a translation layer with its own conformance burden.

### DuckDB reading remote storage directly

DuckDB's `httpfs`/`azure` extensions can read object stores natively, which
would be faster than the bridge. Doing so would move credentials and provider
configuration into the engine adapter and couple it to each storage provider.
The bridge keeps the four axes independent; native extensions may later be
offered as an explicit optimisation.

## Consequences and trade-offs

### Benefits

- One engine evaluates residual work with SQL semantics identical to the
  frontend's, which keeps the DuckDB/Spark parity suite tractable.
- File scans push projection, predicates, and limits into DuckDB's readers.
- Every native source and format handler integrates through Arrow streams.

### Costs and risks

- The bridge serialises reads through Python; remote previews are bounded by
  Python I/O rather than DuckDB's parallel readers.
- DuckDB's CSV/JSON type inference differs from Spark's; portable typed results
  for those formats require a declared schema.
- DuckDB is a sizeable dependency for the local extra.

## Connascence and cohesion

The engine adapter shares static connascence of name with the SQL generator's
dialect table and with the `Storage` port. The bridge removes any connascence
between storage adapters and DuckDB. Residual evaluation policy stays cohesive
in the planner; DuckDB-specific SQL rendering stays cohesive in the adapter.

## Fitness functions

- [FF-05: pushdown completeness invariant](../fitness-functions.md#ff-05-pushdown-completeness-invariant)
- [FF-07: local/Spark portability suite](../fitness-functions.md#ff-07-localspark-portability-suite)
- [FF-10: bounded local memory](../fitness-functions.md#ff-10-bounded-local-memory)

## Revisit when

- Measured preview latency over remote storage makes the bridge the bottleneck
  for typical workloads.
- A DuckDB upgrade changes expression semantics relied on by the parity suite.
