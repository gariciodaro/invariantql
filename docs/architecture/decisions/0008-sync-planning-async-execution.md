# ADR-0008: Keep planning synchronous and make asynchronous execution explicit

- **Status:** Accepted
- **Date:** 2026-08-24
- **Decision owners:** InvariantQL maintainers

## Context

Pushdown analysis is mostly deterministic CPU work over plans and capability
descriptors. Source schema discovery and execution can involve network I/O,
streaming, cancellation, or long-running distributed jobs. Declaring every API
async because some adapters perform I/O would burden notebooks, scripts, and
Spark APIs that are naturally synchronous or job-oriented. Wrapping blocking
SDK calls in `async def` would give false concurrency and poor cancellation.

At the same time, a synchronous-only abstraction can obstruct real asynchronous
database drivers and concurrent metadata work.

## Decision drivers

- Keep the common local/notebook workflow unsurprising.
- Preserve genuine cancellation and backpressure where providers support them.
- Do not fake non-blocking behavior over blocking SDKs.
- Keep scheduling mechanics outside the logical plan.

## Decision

Plan construction, validation, capability matching, and explain are synchronous
and side-effect free once required metadata is available. The baseline execution
port is synchronous and streaming.

Where an integration has genuine asynchronous I/O or remote job submission, it
implements a separate explicit async execution/job protocol. Sync and async are
not a boolean configuration of one ambiguous method. Async wrappers may use
worker threads only in an application-level convenience layer, with documented
cancellation limits; adapters do not disguise blocking calls as native async.

Schema discovery is modeled as an explicit preparation/inspection operation and
may gain sync and async variants based on measured integrations. A query plan
never performs I/O merely by being built.

## Alternatives considered

### Async-first API everywhere

This composes well in async services but adds event-loop ceremony and does not
make Spark or blocking SDKs cancellable.

### Synchronous API only

This is the simplest contract, but it would either block async applications or
force each user to invent offloading and cancellation policy.

### Dual sync/async method on every port

This maximizes surface coverage but doubles adapter contracts even when a
provider has only one meaningful execution model.

## Consequences and trade-offs

### Benefits

- Planning stays deterministic and easy to call and test.
- Asynchronous behavior communicates a real lifecycle rather than syntax.
- Integrations pay async complexity only when it provides value.

### Costs and risks

- Applications supporting both modes may need two orchestration paths.
- The async boundary cannot be finalized until real drivers/jobs are selected.
- Thread-based convenience cancellation is necessarily weaker than native
  cancellation.

## Connascence and cohesion

Separate protocols avoid temporal connascence between plan construction and
event-loop state. Lifecycle and cancellation remain cohesive with execution,
not query semantics. Async providers share named job/result contracts instead
of assuming a particular scheduler or loop implementation.

## Fitness functions

- [FF-04: deterministic immutable plans](../fitness-functions.md#ff-04-deterministic-immutable-plans)
- [FF-08: no implicit Spark collection or output action](../fitness-functions.md#ff-08-no-implicit-spark-collection-or-output-action)
- [FF-13: resource lifecycle](../fitness-functions.md#ff-13-resource-lifecycle)

## Revisit when

- Most target integrations expose native async APIs and sync becomes the costly
  exception.
- Metadata latency requires concurrent planning preparation.
- Python's execution model offers a stable interface that unifies both modes
  without hiding lifecycle differences.
