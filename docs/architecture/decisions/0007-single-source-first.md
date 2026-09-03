# ADR-0007: Limit the initial query model to one source

- **Status:** Accepted
- **Date:** 2026-08-24
- **Decision owners:** InvariantQL maintainers

## Context

Cross-source joins appear to be a natural extension of common query logic, but
they introduce source placement, data movement, join algorithms, statistics,
cost estimation, partial failure, credential boundaries, and distributed
staging. These concerns are qualitatively different from compiling a plan over
one source.

The first hypothesis to prove is narrower: the same retrieval and simple
transformation semantics can work for local preview and production execution.

## Decision drivers

- Deliver semantic portability before building a federated optimizer.
- Keep data movement and security boundaries explicit.
- Bound the conformance matrix for the initial team and release.
- Leave the domain extensible without implying unsupported behavior.

## Decision

The initial logical query has exactly one source. Projection, filtering, and
limit operate over that source. The SQL frontend rejects joins, unions,
subqueries that introduce another source, and other multi-relation constructs.

The domain may use node shapes that do not prevent future relational operators,
but no public API, capability, or documentation implies cross-source execution.
Adding joins or federation requires a new ADR covering placement, costing,
staging, failure semantics, and new fitness functions.

## Alternatives considered

### Support local joins only

This is feasible for bounded samples, but logic that previews locally and cannot
move to production violates the central portability promise unless explicitly
marked non-portable.

### Push joins when both sources share a backend

This could exploit a database or Spark, but source identity and session/catalog
rules add complexity before single-source semantics are proven.

### Build a federated query planner immediately

Federation is powerful and differentiating, but its optimizer, statistics, and
operational responsibilities greatly expand project scope and risk.

## Consequences and trade-offs

### Benefits

- Planner correctness and local/Spark parity have a tractable first boundary.
- No hidden cross-system movement is required for a normal query.
- The package can establish real adapter evidence before generalizing.

### Costs and risks

- Many practical analytical queries cannot initially be expressed.
- Users may perform joins outside InvariantQL and lose portability guarantees.
- Extending a unary model later may require deliberate API evolution.

## Connascence and cohesion

The scope prevents early connascence of execution order, placement, and shared
statistics across adapters. Single-source planning remains cohesive around
translation and pushdown. Future relational operators must enter through domain
types rather than coordinated provider conditionals.

## Fitness functions

- [FF-05: pushdown completeness invariant](../fitness-functions.md#ff-05-pushdown-completeness-invariant)
- [FF-07: local/Spark portability suite](../fitness-functions.md#ff-07-localspark-portability-suite)
- [FF-11: SQL safety boundary](../fitness-functions.md#ff-11-sql-safety-boundary)

## Revisit when

- Single-source portability is stable and measured users identify joins as the
  highest-value limitation.
- A dominant same-engine use case can add joins without implicit cross-system
  transfer.

