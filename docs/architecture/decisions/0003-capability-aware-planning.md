# ADR-0003: Plan pushdown from explicit capabilities and residuals

- **Status:** Accepted
- **Date:** 2026-08-24
- **Decision owners:** InvariantQL maintainers

## Context

Sources differ by operation, expression, type, and limit—not simply by whether
they are called structured or unstructured. A database may push most predicates
but not a particular function; a columnar file scanner may project and filter;
an engine may read a location but not receive a native query.

Assuming capabilities from a source class risks incorrect results. Treating all
sources as incapable is safe but needlessly transfers data. Silent fallback
makes both performance and semantics unpredictable.

## Decision drivers

- Never lose or weaken a logical operation during optimization.
- Move the least practical amount of data.
- Explain why an operation ran in a particular place.
- Allow adapter support to expand without changing the domain plan.

## Decision

Adapters expose immutable, structured capability descriptors. The planner
matches each logical operation and expression against those descriptors and
classifies it as fully pushed, partially pushed with a residual, residual, or
rejected.

Every executable operation must occur exactly once in the combination of pushed
and residual plans. Partial pushdown is legal only when the pushed predicate is
a demonstrably safe relaxation and the residual restores the complete semantic
condition. Capability evidence, disposition, and stable reason codes appear in
the structured explain output.

Capabilities describe supported semantics, not optimistic performance claims.
Native plan probes and telemetry verify pushdown where providers make evidence
available.

## Alternatives considered

### Taxonomy-based dispatch

Rules such as “databases push filters; files do not” are simple but false for
many adapters and too coarse for expression-level differences.

### Always execute in the selected engine

This centralizes semantics and is easier to reason about, but can transfer whole
datasets and ignores efficient native source operations.

### Adapter-controlled opaque optimization

Letting each adapter decide internally can exploit provider knowledge, but the
application cannot prove completeness or give users a consistent explanation.

## Consequences and trade-offs

### Benefits

- Correctness is a planner invariant rather than adapter convention.
- Performance improves incrementally as capability evidence grows.
- Users can inspect fallback, rejection, and data-movement decisions.

### Costs and risks

- Capability descriptors can become complex and require versioning.
- Partial predicate reasoning is difficult; the safe initial implementation may
  reject opportunities.
- Declared support can drift from real provider behavior without integration
  probes.

## Connascence and cohesion

Planner and adapter share static connascence of named capability values rather
than connascence of algorithm or provider-specific conditionals. Optimization
policy remains cohesive in the planner; native compilation stays cohesive in
the adapter. Stable reason codes localize diagnostic coupling.

## Fitness functions

- [FF-03: port conformance contracts](../fitness-functions.md#ff-03-port-conformance-contracts)
- [FF-05: pushdown completeness invariant](../fitness-functions.md#ff-05-pushdown-completeness-invariant)
- [FF-06: explain-plan completeness and stability](../fitness-functions.md#ff-06-explain-plan-completeness-and-stability)
- [FF-09: pushdown effectiveness probes](../fitness-functions.md#ff-09-pushdown-effectiveness-probes)

## Revisit when

- Static descriptors cannot represent important data-dependent or session-level
  constraints.
- Measured planning overhead is material relative to target workloads.
- Cross-source cost optimization becomes part of product scope.

