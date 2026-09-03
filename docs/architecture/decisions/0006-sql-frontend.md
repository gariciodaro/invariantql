# ADR-0006: Treat SQLGlot-backed SQL as a validated frontend

- **Status:** Accepted
- **Date:** 2026-08-24
- **Decision owners:** InvariantQL maintainers

## Context

SQL is the closest common language for the original user problem: consultants
should not relearn retrieval APIs for every source. It is also a large family of
dialects with semantic differences. Implementing a parser is outside the
project's differentiating value, but allowing parser-specific AST nodes into the
domain would bind core semantics and public compatibility to a third party.

Accepting arbitrary SQL would imply joins, subqueries, writes, vendor functions,
and semantics that the initial execution model cannot honor consistently.

## Decision drivers

- Offer a familiar declarative entry point.
- Validate read-only scope before contacting a source.
- Reuse a mature parser without surrendering the domain model.
- Make the supported portable language honest and testable.

## Decision

Use SQLGlot behind a SQL frontend adapter to parse exactly one statement. The
adapter validates a versioned InvariantQL SQL profile and translates accepted
syntax into domain-owned expression and plan nodes. No SQLGlot AST object crosses
the frontend boundary or appears in public/domain types.

The initial profile is a single-source `SELECT` with projection, filtering,
boolean composition, and limit. Exact supported expressions and null/type
semantics will be published with the implementation. DDL, DML, multi-statement
input, unsupported dialect constructs, and ambiguous identifiers are rejected
before source execution.

Values use parameters. Database adapters generate native SQL using bound
parameters; values are never interpolated into query strings. A typed expression
builder may be added as a peer frontend to the same plan.

## Alternatives considered

### Build an InvariantQL SQL parser

This maximizes control and minimizes third-party semantic changes, but parser
correctness and diagnostics would consume effort better spent on portability.

### Pass SQL through to each backend

This preserves native database features but cannot provide portable validation,
local/Spark compilation, or a truthful explain plan.

### Provide only a Python expression API

Typed Python can be discoverable and composable, but it does not meet users at
the common language that motivated the project.

## Consequences and trade-offs

### Benefits

- Users receive a familiar interface while the core remains syntax-independent.
- Read-only and parameter boundaries are enforced centrally.
- The parser can be upgraded or replaced by changing one adapter and its
  conformance suite.

### Costs and risks

- Translation and dialect diagnostics are additional work.
- The supported subset may surprise users who equate “SQL” with full ANSI or
  vendor SQL.
- SQLGlot upgrades can change parsing and normalization behavior.

## Connascence and cohesion

Parser-specific connascence of syntax and algorithm is contained inside the SQL
frontend. It shares static connascence of type with the domain plan. SQL
validation and diagnostics remain cohesive rather than being repeated in each
source or engine adapter.

## Fitness functions

- [FF-04: deterministic immutable plans](../fitness-functions.md#ff-04-deterministic-immutable-plans)
- [FF-07: local/Spark portability suite](../fitness-functions.md#ff-07-localspark-portability-suite)
- [FF-11: SQL safety boundary](../fitness-functions.md#ff-11-sql-safety-boundary)

## Revisit when

- Parser changes repeatedly break accepted-profile semantics despite pinned and
  tested upgrades.
- A second frontend exposes semantic gaps in the domain plan.
- User evidence justifies a broader SQL profile and its conformance cost.

