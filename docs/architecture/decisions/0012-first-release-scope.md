# ADR-0012: First release scope, supported platforms, and licence

- **Status:** Accepted
- **Date:** 2026-09-04
- **Decision owners:** InvariantQL maintainers

## Context

The architecture documents fix the shape of the system but not which
providers ship first, which Python and Spark versions are supported, or under
which licence the code is published. Those choices must define a coherent,
testable release surface without making every provider a base dependency.

## Decision drivers

- Deliver a coherent first slice that exercises every architectural axis.
- Define a Python and Spark range that can be tested as a release contract.
- Keep the base install small while making every listed provider available.
- Publish under a permissive licence.

## Decision

### Scope

The first release ships the following integrations. Integrations with external
runtime dependencies use named optional extras; domain values, the SQL
frontend, the builder, and local storage remain available from the base
package.

| Axis | Adapters |
| --- | --- |
| Storage | local filesystem, Azure Blob Storage, ADLS Gen2, S3, SFTP |
| Data format | CSV, JSON, Parquet, XML, Delta Lake, Apache Iceberg |
| Native source | PostgreSQL, MySQL, MongoDB, Neo4j |
| Execution engine | DuckDB (local), Spark |
| Frontend | SQL (SQLGlot), typed expression builder |

CSV, JSON, and Parquet are read natively by both engines. XML, Delta, and
Iceberg use generic format handlers: an Arrow-producing local handler and a
domain-level reader specification for Spark. Native sources compile pushed
operations to their own query language and stream Arrow locally; on Spark
they are described as native reader configurations.

### Platforms

Python 3.10 or newer; Spark 3.5. The development environment pins Python 3.11.

### Licence and distribution

Apache License 2.0. The package is built with hatchling and managed with uv
(ADR-0009). The distribution target is PyPI under the name `invariantql`, after
artifact and metadata validation.

## Alternatives considered

### File sources only

The smallest coherent slice, but it would leave the native-source port,
the `NativeRelation` descriptor, and the Spark native-reader path untested.

### Python 3.9 support

Widens the audience but forces the code to avoid `match`, `slots=True`
dataclasses, and `kw_only` fields, and no supported Databricks runtime needs it.

### Proprietary licence

Would restrict adoption and collaboration without providing a corresponding
benefit for a reusable query-planning library.

## Consequences and trade-offs

### Benefits

- Independent implementations exercise the key storage, source, format, and
  engine contracts.
- Users install only the extras they need.

### Costs and risks

- The broad first-release integration surface has a substantial conformance
  cost; tests for cloud and database providers require credentials and run only
  when configured.
- Each provider library's release cadence becomes a compatibility concern.

## Connascence and cohesion

Extras introduce static connascence of name between `pyproject.toml`, the
factories, and the `MissingDependencyError` hints. Each adapter remains
cohesive around one provider; no adapter imports another.

## Fitness functions

- [FF-02: base-install isolation](../fitness-functions.md#ff-02-base-install-isolation)
- [FF-03: port conformance contracts](../fitness-functions.md#ff-03-port-conformance-contracts)

## Revisit when

- A provider's library cannot support the Python or Spark floor.
- Usage shows an adapter is unused and its maintenance cost is not justified.
