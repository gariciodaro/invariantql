# ADR-0012: First release scope, supported platforms, and licence

- **Status:** Accepted
- **Date:** 2026-09-04
- **Decision owners:** InvariantQL maintainers

## Context

The architecture documents fix the shape of the system but not which
providers ship first, which Python and Spark versions are supported, or under
which licence the code is published. The legacy `connectors` library targeted
Python 3.7 to 3.10 with pinned 2023 dependency versions and was closed source.

## Decision drivers

- Deliver a coherent first slice that exercises every architectural axis.
- Support the Databricks runtimes the maintainers actually run.
- Keep the base install small while making every listed provider available.
- Publish under a permissive licence.

## Decision

### Scope

The first release ships these adapters, each behind its own optional extra:

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
they are described as connector reader configurations.

### Platforms

Python 3.10 or newer; Spark 3.5 (Databricks Runtime 13.3 LTS onward). The
development environment pins Python 3.11.

### Licence and distribution

Apache License 2.0. The package is built with hatchling and managed with uv
(ADR-0009). It is not published to an index until the maintainers review it.

## Alternatives considered

### File sources only

The smallest coherent slice, but it would leave the native-source port,
the `NativeRelation` descriptor, and the Spark connector path untested.

### Python 3.9 support

Widens the audience but forces the code to avoid `match`, `slots=True`
dataclasses, and `kw_only` fields, and no supported Databricks runtime needs it.

### Proprietary licence

Consistent with the legacy library, but the project is developed in the open
and its value lies in adoption across teams.

## Consequences and trade-offs

### Benefits

- Every port has at least two real implementations, which makes the contract
  suites meaningful.
- Users install only the extras they need.

### Costs and risks

- Fourteen adapters is a large conformance surface for a first release;
  integration tests for cloud and database providers require credentials and
  run only when configured.
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
