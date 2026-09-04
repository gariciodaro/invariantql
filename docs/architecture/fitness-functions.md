# Architecture fitness functions

## Purpose

Fitness functions turn architectural intent into recurring evidence. This file
is a blueprint: checks become executable as soon as the corresponding component
exists, and should be introduced before feature work can violate the boundary.

The future contributor workflow uses uv:

```bash
uv sync --all-groups --all-extras
uv run pytest -m architecture
uv run pytest -m contract
uv run pytest -m portability
uv run import-linter
uv run pyright
```

Exact tools may be replaced through an ADR; the properties being measured are
the durable part.

## Fitness-function catalogue

### FF-01: inward dependency direction

- **Protects:** Evolvability, cohesion, optional dependency isolation.
- **Trigger:** Every pull request.
- **Mechanism:** Import-linter contracts plus an AST/import graph test.
- **Rule:** `domain` imports only the standard library; `ports` may import
  `domain`; `application` may import `domain` and `ports`; none imports
  `adapters`. Adapters never import sibling adapters.
- **Pass condition:** Zero forbidden imports, including imports guarded by
  `TYPE_CHECKING` when they expose provider types in core annotations.
- **Failure response:** Block merge or record a superseding ADR.

### FF-02: base-install isolation

- **Protects:** Installability and integration independence.
- **Trigger:** Every pull request and built-wheel test.
- **Mechanism:** Create a fresh environment with only the base wheel; import the
  package and enumerate loaded modules.
- **Pass condition:** Import succeeds without Spark, Pandas, PyArrow, fsspec,
  cloud SDKs, or database drivers; no warning is emitted for missing extras.
- **Failure response:** Move integration imports behind the adapter construction
  boundary.

### FF-03: port conformance contracts

- **Protects:** Adapter substitutability and manageable connascence.
- **Trigger:** Every adapter test run.
- **Mechanism:** Shared pytest suites for `DataSource`, `Storage`,
  `FormatHandler`, and `ExecutionEngine` implementations.
- **Pass condition:** Every declared capability has a passing positive test;
  every undeclared capability produces the standard unsupported diagnostic.
- **Failure response:** Correct the adapter or reduce its capability declaration.

### FF-04: deterministic immutable plans

- **Protects:** Semantic portability, caching safety, reproducibility.
- **Trigger:** Every domain change.
- **Mechanism:** Property-based tests construct equivalent plans in different
  processes/orders, attempt mutation, and serialize/fingerprint them.
- **Pass condition:** Equivalent plans compare and fingerprint equally; mutation
  is impossible; credentials and provider objects never appear in serialization.

### FF-05: pushdown completeness invariant

- **Protects:** Query correctness.
- **Trigger:** Every planner and adapter change.
- **Mechanism:** Traverse the input and physical/execution plans.
- **Pass condition:** Every logical operation is classified as fully pushed,
  partially pushed plus residual, residual, or rejected. No operation is absent;
  rejected plans cannot execute.
- **Stronger check:** Evaluate generated plans against a reference local engine
  on randomized small data.

### FF-06: explain-plan completeness and stability

- **Protects:** Explainability and supportability.
- **Trigger:** Every planner change.
- **Mechanism:** Schema validation and snapshot tests for structured explain
  output.
- **Pass condition:** Each node contains its stable ID, execution location,
  capability evidence, disposition, and reason code. Human text may evolve;
  codes and structure follow compatibility policy.

### FF-07: local/Spark portability suite

- **Protects:** The core product promise.
- **Trigger:** Every query-model, local-engine, or Spark-adapter change; scheduled
  CI for environments too expensive per commit.
- **Mechanism:** Execute a corpus of portable plans against identical fixtures,
  normalize Arrow/Spark schemas, ordering where specified, nulls, time zones,
  decimals, and floating-point tolerance.
- **Pass condition:** 100% semantic parity for the declared portable profile.
- **Failure response:** Fix translation, narrow capability support, or document a
  deliberate semantic exception through an ADR.

### FF-08: no implicit Spark collection or output action

- **Protects:** Scalability and predictable cost.
- **Trigger:** Every Spark-adapter change.
- **Mechanism:** Static AST scan plus a Spark test double whose collection and
  output methods fail; an integration test confirms the returned DataFrame is
  lazy.
- **Pass condition:** After schema binding, compilation calls no `collect`,
  `toPandas`, `toArrow`, `count`, `show`, or write action. Materialization APIs
  are separate and bounded. Schema discovery is measured separately: it may
  inspect metadata or schedule inference work when no schema is declared.

### FF-09: pushdown effectiveness probes

- **Protects:** Data-movement efficiency.
- **Trigger:** Adapter integration tests and release benchmarks.
- **Mechanism:** Source-specific explain/telemetry probes for selected columns,
  predicates, limits, bytes, and rows.
- **Pass condition:** A capability advertised as full pushdown appears in the
  native plan and the source does not return unrequested columns. Partial
  pushdown reports residual evaluation.

### FF-10: bounded local memory

- **Protects:** Local reliability.
- **Trigger:** Release benchmark and changes to scanners/materializers.
- **Mechanism:** Stream a generated dataset much larger than memory-budget
  batches while sampling peak RSS.
- **Initial pass condition:** Peak incremental memory <= two configured batch
  payloads + 64 MiB, calibrated on the reference CI runner.
- **Exception:** Explicit `to_pandas()` may materialize, but must require or infer
  a configured safety limit and produce a clear limit error.

### FF-11: SQL safety boundary

- **Protects:** Security and predictable semantics.
- **Trigger:** Every SQL-frontend change.
- **Mechanism:** Corpus and property tests covering multiple statements,
  comments, DDL, DML, vendor syntax, nested queries, parameter edge cases, and
  malformed SQL.
- **Pass condition:** Only the documented read-only profile yields a plan;
  rejection happens before any source adapter is invoked. Generated database SQL
  binds values rather than interpolating them.

### FF-12: secret non-disclosure

- **Protects:** Security.
- **Trigger:** Every logging/error/configuration change.
- **Mechanism:** Seed credentials in URLs, options, provider exceptions, and
  environment-derived configuration; scan exceptions, logs, explain output, and
  serialized plans.
- **Pass condition:** No seeded secret appears. Stable redaction markers preserve
  diagnostic usefulness.

### FF-13: resource lifecycle

- **Protects:** Reliability.
- **Trigger:** Every source/storage/execution adapter change.
- **Mechanism:** Fault injection at open, read, translation, cancellation, and
  close boundaries with observable fake resources.
- **Pass condition:** Every owned stream/cursor/job is closed or cancelled once;
  partial move/staging results are reported as partial, never successful.

### FF-14: explicit staging and collection

- **Protects:** Cost predictability and security.
- **Trigger:** Every application/API change.
- **Mechanism:** Static public-API review plus behavioral tests with unreachable
  Spark locations.
- **Pass condition:** Compilation returns a staging-required diagnostic and
  performs zero transfer. Only an explicit staging command moves data; explain
  reports destination, cleanup policy, and estimated/known size.

### FF-15: public API and diagnostic compatibility

- **Protects:** User stability.
- **Trigger:** Every release.
- **Mechanism:** Snapshot exported names, public signatures, explain schema, and
  diagnostic codes; run type-checking examples as tests.
- **Pass condition:** Breaking differences require a major version and migration
  note or are restored before release.

### FF-16: architecture-document integrity

- **Protects:** Decision traceability.
- **Trigger:** Documentation CI.
- **Mechanism:** Validate ADR filenames, statuses, required headings, links, and
  decision-register membership.
- **Pass condition:** Every accepted/superseded decision is indexed and includes
  context, decision, trade-offs, consequences, and fitness functions.

## Characteristic coverage

| Characteristic | Primary fitness functions |
| --- | --- |
| Semantic portability | FF-04, FF-05, FF-07, FF-11 |
| Evolvability | FF-01, FF-02, FF-03, FF-15 |
| Efficiency/scalability | FF-08, FF-09, FF-10, FF-14 |
| Explainability | FF-05, FF-06, FF-09 |
| Security | FF-11, FF-12, FF-14 |
| Reliability | FF-03, FF-10, FF-13 |
| Usability | FF-06, FF-07, FF-14, FF-15 |
| Architectural governance | FF-01, FF-16 |

## Delivery sequence

1. Implement FF-01, FF-02, FF-04, FF-11, and FF-16 with the initial scaffold.
2. Add FF-03 and FF-05 before the first real adapter.
3. Add FF-06, FF-09, FF-10, FF-12, and FF-13 with local file execution.
4. Add FF-07 and FF-08 before advertising Spark portability.
5. Add FF-14 before any staging implementation.

This sequence makes architecture tests lead the risky boundaries instead of
being retrofitted after coupling has formed.
