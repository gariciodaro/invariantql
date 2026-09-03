# ADR-0010: Isolate optional integrations and defer plugin discovery

- **Status:** Accepted
- **Date:** 2026-08-24
- **Decision owners:** InvariantQL maintainers

## Context

Spark, Arrow, storage SDKs, and database clients are large, version-sensitive,
and frequently irrelevant to a given user. Importing them from the package root
increases installation conflicts and can make an unused adapter break the whole
library.

A dynamic plugin framework could let external packages register adapters, but
it introduces discovery order, naming collisions, compatibility negotiation,
and a public extension contract before there is evidence of third-party adapter
authors.

## Decision drivers

- Keep the base installation small and importable in isolation.
- Let integrations release and fail at explicit construction boundaries.
- Provide deterministic adapter selection and useful missing-dependency errors.
- Avoid prematurely freezing a third-party plugin API.

## Decision

Package integrations as optional extras and isolate imports inside their adapter
modules. Importing `invariantql` does not import provider SDKs, scan the
environment, emit warnings for absent extras, or establish connections.

The initial release registers built-in adapters explicitly through a
composition/registration facade. Constructing an unavailable adapter raises a
typed diagnostic naming the required extra and preserving the original cause
without exposing secrets.

Do not implement entry-point or directory-based plugin discovery initially.
External adapters can use documented ports experimentally, but those ports are
not declared a stable plugin ABI until at least two independent integrations
demonstrate the needed lifecycle, version, naming, and capability contracts.

## Alternatives considered

### Install all integrations by default

This eliminates missing-extra errors but creates a large, conflict-prone install
and makes unrelated provider releases part of base-package reliability.

### Automatic entry-point discovery now

This enables ecosystem growth early, but also makes import behavior and adapter
selection depend on ambient environment state and freezes immature contracts.

### Lazy imports alone without extras

This postpones import cost, but users still install every heavy dependency and
carry its security and resolver surface.

### Separate distribution for every adapter immediately

This gives maximum dependency isolation, but multiplies release and compatibility
management before the adapter set or maintainership warrants it.

## Consequences and trade-offs

### Benefits

- Base import is deterministic and insulated from provider dependency churn.
- Users install only the integration surface they need.
- The project can learn from real adapters before promising a plugin ABI.

### Costs and risks

- Users must select extras and may encounter errors at adapter construction.
- One distribution still coordinates versions of built-in integrations.
- External adapter authors lack automatic discovery in early releases.

## Connascence and cohesion

Explicit registration replaces connascence of execution order and ambient
environment with static connascence of adapter names. Provider version coupling
is contained in cohesive adapter modules. Deferring discovery avoids temporal
and identity connascence between independently installed packages until a stable
contract can be justified.

## Fitness functions

- [FF-01: inward dependency direction](../fitness-functions.md#ff-01-inward-dependency-direction)
- [FF-02: base-install isolation](../fitness-functions.md#ff-02-base-install-isolation)
- [FF-03: port conformance contracts](../fitness-functions.md#ff-03-port-conformance-contracts)
- [FF-12: secret non-disclosure](../fitness-functions.md#ff-12-secret-non-disclosure)

## Revisit when

- At least two independently maintained external adapters need discovery.
- Built-in adapter release cadence or dependency conflicts justify separate
  distributions.
- Manual registration becomes a measured usability problem.
