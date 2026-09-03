# ADR-0002: Make an immutable logical query plan the invariant core

- **Status:** Accepted
- **Date:** 2026-08-24
- **Decision owners:** InvariantQL maintainers

## Context

The product promise is to preserve data logic while changing sources and
execution engines. SQL text, a Pandas DataFrame, and a Spark DataFrame are not
equivalent portable models: each embeds syntax, runtime behavior, or an active
execution context. Passing any of them through the core would make that
representation—not query meaning—the system's real invariant.

Planning, validation, explanation, deterministic testing, and safe caching also
need a representation that can be inspected without executing user data.

## Decision drivers

- Preserve query semantics independently of frontend and backend.
- Make plans deterministic, serializable, inspectable, and safe to cache.
- Detect unsupported semantics before execution.
- Avoid runtime type branching between Pandas and Spark objects.

## Decision

Represent every portable query as an immutable, typed logical query plan owned
by the domain. Initial plan operations are source reference, projection,
predicate, and limit. Expressions, schemas, types, null semantics, parameters,
and ordering guarantees are explicit domain values.

The plan contains logical source identifiers and redacted configuration
references, never credentials, open connections, native DataFrames, provider
AST nodes, callbacks, or mutable dictionaries. Structurally equivalent plans
have a deterministic canonical form and fingerprint.

Frontends compile into this plan. Capability planning annotates or transforms it
into an execution plan. Backends compile the execution plan into native work.

## Alternatives considered

### SQL text as the canonical representation

SQL is recognizable and portable within a constrained dialect, but text alone
does not supply resolved types, normalized semantics, capability dispositions,
or safe structural transformation.

### Pandas or Spark DataFrame as the shared object

This improves native ergonomics for one engine. It couples the query to a
runtime and encourages type checks and accidental execution when moving between
local and distributed contexts.

### A generic dictionary plan

Dictionaries are easy to produce and serialize, but misspelled fields and
invalid combinations fail late. They create dynamic connascence of key names
throughout the package.

## Consequences and trade-offs

### Benefits

- Semantic validation and explanation happen before I/O.
- Multiple frontends and engines can evolve around one explicit contract.
- Immutability makes plans safer to share, compare, cache, and test.

### Costs and risks

- The project owns a type system and must define cross-engine semantics.
- Every supported operation requires intentional translators and conformance
  tests.
- The portable language will remain smaller than each native backend.

## Connascence and cohesion

Typed nodes convert widespread connascence of meaning and dictionary keys into
static connascence of name and type. Immutability removes connascence of timing
and execution order caused by shared mutable query state. Query semantics remain
cohesive in the domain instead of leaking into adapters.

## Fitness functions

- [FF-04: deterministic immutable plans](../fitness-functions.md#ff-04-deterministic-immutable-plans)
- [FF-05: pushdown completeness invariant](../fitness-functions.md#ff-05-pushdown-completeness-invariant)
- [FF-07: local/Spark portability suite](../fitness-functions.md#ff-07-localspark-portability-suite)

## Revisit when

- The model cannot express a required portable semantic without systematic
  native leakage.
- Serialization/versioning costs outweigh real uses such as caching,
  explanation, or remote execution.

