# ADR-0004: Separate storage, data format, source, and execution engine

- **Status:** Accepted
- **Date:** 2026-08-24
- **Decision owners:** InvariantQL maintainers

## Context

A file-backed dataset varies independently by location protocol, object-store
semantics, serialization format, and execution engine. A database combines
storage and query behavior behind its native source interface. Treating every
combination as a connector class leads to products such as
`SparkAzureCsvConnector` and repeats configuration and logic.

At the other extreme, making every object store pretend to be a POSIX
filesystem hides material differences such as atomic rename, directory
semantics, listing consistency, range access, and engine visibility.

## Decision drivers

- Add a format, storage provider, or engine without multiplying combinations.
- Represent provider semantics honestly.
- Keep responsibilities cohesive and independently testable.
- Preserve a simple common path for users.

## Decision

Model four distinct axes:

- `Storage` locates and transfers byte objects and declares its actual storage
  semantics.
- `DataFormat` is immutable data describing serialization options.
- `FormatHandler` interprets a `DataFormat` for a particular local or native
  runtime.
- `DataSource` exposes queryable schema/capabilities and may either compose
  storage plus format or wrap a native query service such as a database.
- `ExecutionEngine` compiles and runs planned work.

A `FileSource` composes `Storage` and `DataFormat`; public factories hide that
composition on the common path. Adapters may share internal helpers, but one
adapter does not import or switch on another adapter.

## Alternatives considered

### One connector class per combination

This makes construction concrete but causes combinatorial growth and duplicated
behavior across providers, formats, and engines.

### A universal filesystem abstraction

A filesystem-like interface is ergonomic and can be used as an adapter when its
capabilities are explicit. It is not the domain truth because object stores do
not necessarily supply hierarchical directories, atomic rename, or local paths.

### Only structured and unstructured source categories

The taxonomy is intuitive but does not predict executable behavior. Some files
support projection and predicate pushdown; some native sources expose limited
query operations.

## Consequences and trade-offs

### Benefits

- Independent variation prevents a connector-class explosion.
- Storage behavior such as staging and non-atomic moves remains visible.
- Data-format options are reusable across local and distributed handlers.

### Costs and risks

- Users and implementers must learn several related abstractions.
- Some provider SDKs span multiple axes and require deliberate placement.
- Not every storage/format/engine combination is valid, so capability
  negotiation and good construction errors are essential.

## Connascence and cohesion

Composition replaces connascence of identity encoded in compound connector
classes with static connascence of name and type between narrow roles. Storage
semantics, serialization knowledge, source query behavior, and execution policy
each remain functionally cohesive. The facade prevents this internal
decomposition from becoming user ceremony.

## Fitness functions

- [FF-01: inward dependency direction](../fitness-functions.md#ff-01-inward-dependency-direction)
- [FF-03: port conformance contracts](../fitness-functions.md#ff-03-port-conformance-contracts)
- [FF-13: resource lifecycle](../fitness-functions.md#ff-13-resource-lifecycle)
- [FF-14: explicit staging and collection](../fitness-functions.md#ff-14-explicit-staging-and-collection)

## Revisit when

- Real adapters repeatedly require coordinated changes across the same axes,
  showing that the split lowers rather than raises cohesion.
- The public construction facade cannot hide invalid combinations clearly.

