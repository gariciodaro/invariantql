# Architectural characteristics

## Prioritization method

Architecture cannot maximize every quality simultaneously. The characteristics
below are ranked by their influence on structural decisions. Security and
correctness are constraints; they are not exchanged for convenience or speed.

### Highest-priority differentiators

1. **Semantic portability and predictability**
2. **Evolvability across sources, formats, and engines**
3. **Data-movement efficiency and scalable execution**

### Enabling characteristics

4. **Explainability and observability**
5. **Testability**
6. **Interoperability and dependency isolation**
7. **Resource safety and reliability**
8. **Developer usability**

### Deferred optimizations

- High-throughput async concurrency for every adapter.
- Hosted-service availability and multi-tenant isolation.
- Cost-based planning across multiple sources.

## Characteristic definitions and trade-offs

### 1. Semantic portability and predictability

The same logical plan should produce equivalent schemas and values on every
engine in its declared portable profile. An unsupported operation fails during
validation or remains visible as residual work.

**Trade-off:** The portable profile is smaller than the union of native engine
features. Native escape hatches improve reach at the cost of portability.

### 2. Evolvability

A new source, format handler, or engine should be added behind a stable port and
shared conformance suite without modifying the domain model for provider-only
details.

**Trade-off:** More explicit boundaries and value objects create initial design
overhead. This is accepted to avoid the Cartesian class growth and duplicated
conditionals found when storage, format, and engine are coupled.

### 3. Data-movement efficiency and scalable execution

Projection, predicates, and limits should run as close to the data as source and
engine capabilities safely allow. Distributed relations remain distributed;
collection and staging are explicit.

**Trade-off:** A planner and capability model add complexity. Correctness takes
precedence over optimistic pushdown; uncertain predicates may be re-evaluated
locally.

### 4. Explainability and observability

Every plan exposes which operations are pushed, residual, rejected, or require
staging, together with stable reason codes. Logs and metrics correlate planning
and execution without recording credentials or query secrets.

**Trade-off:** Stable diagnostics become a compatibility surface and require
maintenance. This cost is accepted because invisible optimization is difficult
to trust and test.

### 5. Testability

Domain behavior is deterministic and I/O-free. Every adapter runs the same
contract suite, and portable plans run against common fixtures on local and
Spark engines.

**Trade-off:** Adapter authors implement testing fixtures and capability proofs
in addition to functional code.

### 6. Interoperability and dependency isolation

Arrow is the local data interchange boundary; Spark retains its native lazy
relation. Optional providers do not inflate or break the core installation.

**Trade-off:** The public API exposes explicit materializers rather than one
universal DataFrame type.

### 7. Resource safety and reliability

Streams, cursors, temporary objects, and submitted jobs have explicit ownership,
closing, cancellation, and cleanup semantics. Partial failures must not be
reported as successful moves or complete results.

**Trade-off:** Context managers and execution handles make lifecycle visible to
callers rather than relying on garbage collection.

### 8. Developer usability

The common workflow should not require backend type checks: construct one plan,
preview it locally, validate it, and compile it for Spark. Errors name the
unsupported operation and target.

**Trade-off:** Backend choice is explicit. InvariantQL does not guess whether a
caller intended local collection or distributed execution.

## Quality-attribute scenarios

| ID | Source and stimulus | Expected response | Initial measure |
| --- | --- | --- | --- |
| PORT-01 | A developer runs a plan inside the declared Local+Spark portable profile | Both engines produce equivalent normalized schema and values on the conformance fixture | 100% parity for supported operations; documented tolerance only for floating point |
| PORT-02 | A plan uses an operation unsupported by a selected target | Validation fails before source scanning and identifies the plan node and target | Zero silent drops; zero source reads before failure |
| EXT-01 | A contributor adds a source adapter | The adapter implements one port and the shared contract without provider conditionals in core/application | No imports or source-specific branches added to domain/application |
| EXT-02 | A contributor adds a `DataFormat` | Local and/or Spark handlers register support independently | No storage-adapter modification |
| PERF-01 | A source supports projection, filters, and limit | Planner pushes supported nodes and transfers only the required result plus allowed source overhead | Explain plan proves pushdown; integration probe observes no unrequested columns |
| PERF-02 | A Spark target is selected | Compilation returns a lazy native relation without collecting records to the driver | No `collect`, `toPandas`, or `toArrow` call in the Spark compile path |
| PERF-03 | A 1 GiB local source is streamed with a configured batch size | Peak memory remains bounded by batches and fixed engine overhead | Target: <= 2 batch payloads + 64 MiB after baseline calibration |
| OBS-01 | A developer asks why a predicate was not pushed | `explain()` reports capability evidence, chosen location, residual work, and a stable reason code | All executable plan nodes have a disposition and reason |
| SEC-01 | SQL contains DDL, DML, multiple statements, or an unsupported construct | Frontend rejects it before opening a source connection | 100% rejection in security corpus; zero provider calls |
| SEC-02 | An exception includes a URI or provider error | User-facing error and telemetry redact credentials and secret query parameters | Secret-scanner corpus reports zero leaks |
| REL-01 | Execution fails or is cancelled midway | Owned streams/cursors are closed and temporary staging objects follow declared cleanup policy | Contract test observes no leaked resource handles |
| USE-01 | A user promotes a local preview to Spark | The same immutable `QueryPlan` is reused; only the engine selection changes | No runtime DataFrame type branch in user code |
| ISO-01 | A user installs and imports the base package | Missing Spark/cloud/database extras do not emit warnings or import provider modules | Base import succeeds in a clean environment; optional modules absent from `sys.modules` |

The numerical performance budgets are hypotheses. The first implementation will
record a reproducible baseline; changing their intent or weakening a semantic or
security measure requires an ADR.

## Tensions to monitor

### Portability versus native power

The portable profile must remain explicit. Backend-specific functions live
behind a native namespace and mark the plan non-portable rather than weakening
the meaning of common operations.

### Pushdown versus correctness

An adapter may claim full, partial, or no support for an expression. Partial
support requires residual re-evaluation. Performance never justifies dropping a
predicate.

### Convenience versus hidden data movement

Local preview and Spark execution appear adjacent in the API, but conversion
between them is not automatic. Staging and collection are separate verbs with
budgets and diagnostics.

### Extensibility versus dependency weight

The public facade may know registered adapter names, but provider dependencies
remain outside the base import path. External plugin discovery is deferred until
real third-party adapters justify its operational complexity.

### Async concurrency versus ecosystem compatibility

Planning is synchronous and deterministic. Async execution is offered only by
adapters with genuine non-blocking behavior or a job handle; sync libraries are
not mislabeled as async merely by hiding them in a thread.
