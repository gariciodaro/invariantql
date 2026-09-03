# ADR-0014: Clean break from the legacy connectors library

- **Status:** Accepted
- **Date:** 2026-09-04
- **Decision owners:** InvariantQL maintainers

## Context

InvariantQL re-engineers the closed-source `connectors` library. That library
exposed a `Connector` base class with `read(reading_dict, reading_function_dict)`
and `write(...)` methods, mutable option dictionaries matched case-insensitively
against pandas reader signatures, and behaviour outside the read-only query
model: writes, upserts, copy and move, EDIFACT message parsing and generation,
SAP RFC and SAP IBP OData access, and Databricks cluster administration. It has
no tests, no in-repository callers, and a module layout that does not match its
own imports, so behaviour parity cannot be verified against it.

## Decision drivers

- The first release is read-only and single-source (README scope, ADR-0007).
- Typed, immutable formats and plans replace mutable option dictionaries
  (ADR-0002, ADR-0004).
- A compatibility layer would import the legacy contract's ambiguities.

## Decision

InvariantQL is not a drop-in replacement for `connectors`. No compatibility
shim, no `Connector` class, and no `reading_dict` contract are provided. The
following legacy capabilities are dropped rather than deferred: writes,
upserts, copy/paste/move/delete of objects, EDIFACT parsing and generation, SAP
RFC, SAP IBP OData, Azure DevOps repositories, Delta Sharing, Databricks
cluster and job administration, and Azure File Share.

The legacy library is a reference for behaviours worth preserving (for
example, Delta time travel and hive-partitioned Parquet datasets), not a test
oracle. Users migrate by registering sources and writing SQL or builder
queries; a migration note in the README maps the old concepts to the new ones.

## Alternatives considered

### A compatibility facade over the new core

Would let existing pipelines switch imports only, but the legacy contract
returns pandas objects eagerly, accepts arbitrary reader keyword arguments,
and silently drops unknown options, all of which contradict the new
principles.

### Porting the excluded capabilities behind separate ADRs

Keeps the door open, but each capability (writes, messaging formats, ERP
protocols) is a product decision with its own semantics and none has an
identified user today.

## Consequences and trade-offs

### Benefits

- The public API is small and consistent with the architecture.
- No legacy defects or ambiguities are carried forward.

### Costs and risks

- Existing pipelines must be rewritten to use InvariantQL.
- Workloads that need writes or ERP access must use other tools.

## Connascence and cohesion

Removing the legacy contract eliminates dynamic connascence of dictionary
keys and function names across the code base. Each remaining capability is
cohesive with one port.

## Fitness functions

- [FF-11: SQL safety boundary](../fitness-functions.md#ff-11-sql-safety-boundary)
- [FF-15: public API and diagnostic compatibility](../fitness-functions.md#ff-15-public-api-and-diagnostic-compatibility)

## Revisit when

- A dropped capability gains a concrete user and can be specified with its own
  semantics and fitness functions.
