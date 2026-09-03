# ADR-0009: Use uv for dependency and contributor workflows

- **Status:** Accepted
- **Date:** 2026-08-24
- **Decision owners:** InvariantQL maintainers

## Context

The project needs reproducible contributor environments, optional integration
dependencies, build/test commands, and a low-friction onboarding path. Multiple
uncoordinated tools for environment creation, dependency resolution, and command
execution add procedural coupling and make local behavior differ from CI.

As a published library, InvariantQL must still declare compatible dependency
ranges for consumers rather than imposing its contributor lockfile on their
environments.

## Decision drivers

- One documented workflow for contributors and CI.
- Reproducible resolution of development and integration test environments.
- Clean separation of base, optional integration, and development dependencies.
- Standards-compatible Python package metadata and wheels.

## Decision

Use uv as the package manager and task execution entry point. `pyproject.toml`
is the source of project metadata and dependency declarations. Commit `uv.lock`
for contributor and CI reproducibility, while published wheel metadata retains
compatible dependency constraints for downstream resolvers.

Provider integrations live in named optional extras; testing, linting, typing,
and documentation tools live in dependency groups. Repository documentation and
CI use `uv sync`, `uv run`, and `uv build` rather than parallel pip/Poetry/Conda
workflows.

The Python build backend is a separate implementation choice to be made with the
initial scaffold; using uv does not require core architecture to depend on a
particular backend.

## Alternatives considered

### pip plus virtualenv and requirement files

This is universal and has few concepts, but reproducible multi-group resolution
and a single project workflow require more manual conventions and files.

### Poetry or PDM

Both provide mature project workflows. Selecting either would also be viable;
uv is chosen for the requested workflow and its ability to manage environments,
locking, execution, and builds around standard metadata.

### Conda as the primary workflow

Conda is valuable for native and data-science stacks, but making it primary
would add a second packaging ecosystem for a Python library. Integration guides
may still explain compatible external environments later.

## Consequences and trade-offs

### Benefits

- Contributors and CI exercise the same commands and lock resolution.
- Optional dependency matrices are visible in one project definition.
- Onboarding requires fewer independently configured tools.

### Costs and risks

- Contributors must install uv.
- uv behavior and lockfile format can evolve.
- Testing every optional extra together does not replace minimum-version and
  selected-version compatibility tests.

## Connascence and cohesion

The decision replaces procedural connascence across several setup commands and
duplicated dependency files with static connascence of name in
`pyproject.toml`, group names, and uv commands. Packaging metadata stays cohesive
in one place; runtime modules remain unaware of the package manager.

## Fitness functions

- The command blueprint in [Architecture fitness functions](../fitness-functions.md#purpose).
- [FF-02: base-install isolation](../fitness-functions.md#ff-02-base-install-isolation)
- [FF-16: architecture-document integrity](../fitness-functions.md#ff-16-architecture-document-integrity)

## Revisit when

- uv cannot produce standards-compatible artifacts required by supported
  indexes or platforms.
- A measured contributor/CI limitation is not resolvable without maintaining a
  second primary workflow.

