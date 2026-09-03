# ADR-0005: Use Arrow locally and retain Spark's native lazy relation

- **Status:** Accepted
- **Date:** 2026-08-24
- **Decision owners:** InvariantQL maintainers

## Context

Local preview needs an interoperable, columnar, bounded result. Production Spark
execution needs lazy distributed behavior. Forcing both into Pandas would
collect distributed data and erase Spark semantics; forcing local users to work
through Spark would make preview heavyweight. Returning a union of Pandas and
Spark objects recreates the runtime type checks that the project is intended to
eliminate.

## Decision drivers

- Preserve Spark laziness and prevent accidental driver collection.
- Provide an efficient local/interchange representation.
- Make result behavior visible in types and APIs.
- Interoperate with common Python analytical tools without making one the core.

## Decision

The local engine streams Arrow-compatible record batches and exposes an
Arrow-native bounded result. Conversions such as `to_pandas()` are explicit
terminal materializations with configurable safety limits.

The Spark engine returns a Spark-specific lazy result wrapper or native relation
through its engine-specific API. Compilation performs no action. Distributed
collection, writing, and job submission are explicit operations.

The portable object is the immutable query plan, not the physical result. Code
that must remain engine-independent builds and inspects plans; code that consumes
results chooses a declared engine boundary rather than branching on runtime
result type.

## Alternatives considered

### Always return Pandas

This is familiar for local data science but cannot safely represent datasets
larger than driver memory and would require implicit Spark collection.

### Always return Arrow

Arrow is a strong interchange model, but converting a lazy distributed Spark
relation into Arrow is itself an action and loses distributed transformations.

### One result protocol over every engine

A minimal protocol can normalize a few methods, but either leaks backend
differences or grows into a second DataFrame API. It also obscures which calls
are local versus distributed and eager versus lazy.

## Consequences and trade-offs

### Benefits

- Expensive collection cannot hide behind a common return type.
- Local execution can stream with bounded memory.
- Arrow provides broad interchange without defining query semantics.

### Costs and risks

- Result consumption is not fully uniform across engines.
- Arrow becomes a significant local dependency once the local extra is used.
- Schema normalization and semantic conformance still require explicit tests.

## Connascence and cohesion

Separate result boundaries remove dynamic connascence of runtime type from user
logic. Each engine owns lifecycle and materialization behavior that is cohesive
with its execution model. Shared schema semantics remain statically coupled to
domain types and are verified rather than inferred from native objects.

## Fitness functions

- [FF-07: local/Spark portability suite](../fitness-functions.md#ff-07-localspark-portability-suite)
- [FF-08: no implicit Spark action or collection](../fitness-functions.md#ff-08-no-implicit-spark-action-or-collection)
- [FF-10: bounded local memory](../fitness-functions.md#ff-10-bounded-local-memory)
- [FF-14: explicit staging and collection](../fitness-functions.md#ff-14-explicit-staging-and-collection)

## Revisit when

- A proven cross-engine lazy relation protocol can preserve cost and lifecycle
  semantics without becoming another DataFrame implementation.
- Arrow cannot represent a required local result semantic with acceptable
  fidelity or overhead.
