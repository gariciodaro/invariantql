# ADR-0001: Use a modular ports-and-adapters library architecture

- **Status:** Accepted
- **Date:** 2026-08-24
- **Decision owners:** InvariantQL maintainers

## Context

InvariantQL must present one query model while integrating with file formats,
object stores, databases, local runtimes, and distributed engines. These
providers have incompatible APIs, dependency weights, resource lifecycles, and
release cadences. If provider objects become the shared model, every new
integration increases coupling in the rest of the package.

The project is initially a Python library maintained by a small team. Service
boundaries or separately deployed components would add operational complexity
without isolating a current scaling or ownership concern.

## Decision drivers

- Keep query semantics independent of third-party provider APIs.
- Allow optional integrations to evolve and fail independently.
- Make boundaries enforceable in ordinary Python tests and tooling.
- Preserve a simple install and debugging experience.

## Decision

Build a modular library using ports and adapters. The modules are `domain`,
`ports`, `application`, `api`, and `adapters`, with dependencies pointing
inward as defined in [the component model](../components.md#dependency-rules).

The domain owns query meaning. Ports use domain-owned values to describe the
behavior required at integration boundaries. Adapters translate provider types
only at those boundaries. The public API is a facade over application use
cases; it does not expose the internal dependency graph.

Modules remain in one distribution until release cadence, team ownership, or
dependency isolation supplies evidence for separate packages.

## Alternatives considered

### Provider-centered inheritance hierarchy

A common base integration class with subclasses is familiar and quick for the
first few providers. It becomes brittle when sources vary across several
independent axes and tends to force unused methods or provider conditionals
into the base.

### Microservices or separately deployed engines

Process boundaries offer strong isolation, independent scaling, and language
freedom. They also introduce network failure, deployment, authentication, and
version negotiation into a library whose first job is local planning.

### One flat package of utility functions

This minimizes initial ceremony but provides no durable ownership of semantics
or enforceable direction for dependencies.

## Consequences and trade-offs

### Benefits

- The semantic core can be tested without optional runtimes or external data.
- Integration failures and dependency churn remain at the edges.
- Ports provide clear places for contract tests and substitutable adapters.

### Costs and risks

- Translation code and value objects add more concepts than direct SDK calls.
- Too many tiny ports could create indirection without useful substitution.
- A single distribution still shares a release train even with internal
  isolation.

## Connascence and cohesion

The design trades connascence of implementation and identity with provider
objects for static connascence of name and type at narrow ports. Domain
semantics stay functionally cohesive; provider translation stays integration
cohesive. Port growth is governed so that interfaces describe consumer needs,
not a least-common-denominator mirror of SDKs.

## Fitness functions

- [FF-01: inward dependency direction](../fitness-functions.md#ff-01-inward-dependency-direction)
- [FF-02: base-install isolation](../fitness-functions.md#ff-02-base-install-isolation)
- [FF-03: port conformance contracts](../fitness-functions.md#ff-03-port-conformance-contracts)

## Revisit when

- Independent teams need incompatible release cadences.
- An adapter cannot be isolated from the base environment within one wheel.
- A remote execution/control plane becomes an explicit product requirement.
