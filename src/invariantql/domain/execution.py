"""Execution-plan value objects produced by the capability planner (ADR-0003).

An ``ExecutionPlan`` splits a logical plan into operations pushed to the scan
target and operations that remain for the engine. Every logical operation
appears exactly once across the two halves unless the plan was rejected.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from invariantql.domain.explain import Disposition, ExplainPlan
from invariantql.domain.expressions import Expression, conjuncts, output_name
from invariantql.domain.plan import QueryPlan
from invariantql.domain.schema import Schema


@dataclass(frozen=True, slots=True)
class PushedOperations:
    """Operations the scan target performs. ``projection`` lists source columns."""

    projection: tuple[str, ...] | None = None
    predicate: Expression | None = None
    limit: int | None = None

    def __post_init__(self) -> None:
        if self.projection is not None:
            object.__setattr__(self, "projection", tuple(self.projection))

    def to_dict(self) -> dict[str, Any]:
        return {
            "projection": None if self.projection is None else list(self.projection),
            "predicate": None if self.predicate is None else self.predicate.to_dict(),
            "limit": self.limit,
        }

    def __str__(self) -> str:
        parts = []
        if self.projection is not None:
            parts.append("columns=" + ",".join(self.projection))
        if self.predicate is not None:
            parts.append(f"where={self.predicate}")
        if self.limit is not None:
            parts.append(f"limit={self.limit}")
        return " ".join(parts) or "(nothing pushed)"


@dataclass(frozen=True, slots=True)
class ResidualOperations:
    """Operations the engine performs after the scan. ``projection`` lists output expressions."""

    projection: tuple[Expression, ...] | None = None
    predicate: Expression | None = None
    limit: int | None = None

    def __post_init__(self) -> None:
        if self.projection is not None:
            object.__setattr__(self, "projection", tuple(self.projection))

    @property
    def is_empty(self) -> bool:
        return self.projection is None and self.predicate is None and self.limit is None

    def to_dict(self) -> dict[str, Any]:
        return {
            "projection": None
            if self.projection is None
            else [e.to_dict() for e in self.projection],
            "predicate": None if self.predicate is None else self.predicate.to_dict(),
            "limit": self.limit,
        }

    def __str__(self) -> str:
        parts = []
        if self.projection is not None:
            parts.append("select=" + ", ".join(str(e) for e in self.projection))
        if self.predicate is not None:
            parts.append(f"where={self.predicate}")
        if self.limit is not None:
            parts.append(f"limit={self.limit}")
        return " ".join(parts) or "(no residual work)"


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    plan: QueryPlan
    engine: str
    source: str
    schema: Schema
    output_schema: Schema
    pushed: PushedOperations
    residual: ResidualOperations
    explain: ExplainPlan

    @property
    def executable(self) -> bool:
        return self.explain.executable

    @property
    def staging_required(self) -> bool:
        return self.explain.staging_required

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan": self.plan.to_dict(),
            "engine": self.engine,
            "source": self.source,
            "schema": self.schema.to_dict(),
            "output_schema": self.output_schema.to_dict(),
            "pushed": self.pushed.to_dict(),
            "residual": self.residual.to_dict(),
            "explain": self.explain.to_dict(),
        }


def check_completeness(execution_plan: ExecutionPlan) -> tuple[str, ...]:
    """Return violations of the pushdown completeness invariant (FF-05).

    An empty tuple means every logical operation is accounted for exactly once
    (or the plan is rejected and therefore not executable).
    """

    plan = execution_plan.plan
    pushed = execution_plan.pushed
    residual = execution_plan.residual
    problems: list[str] = []

    if not execution_plan.executable:
        if not execution_plan.explain.rejected:
            problems.append("plan marked not executable but no node is rejected")
        return tuple(problems)
    if execution_plan.explain.rejected:
        problems.append("executable plan contains rejected nodes")

    logical = list(conjuncts(plan.predicate))
    covered = list(conjuncts(pushed.predicate)) + list(conjuncts(residual.predicate))
    if sorted(map(str, logical)) != sorted(map(str, covered)):
        problems.append(
            f"predicate conjuncts differ: logical={[str(c) for c in logical]} "
            f"covered={[str(c) for c in covered]}"
        )

    if plan.projection is not None:
        if residual.projection is not None:
            if tuple(residual.projection) != tuple(plan.projection):
                problems.append("residual projection differs from logical projection")
        else:
            names = tuple(output_name(e) for e in plan.projection)
            if pushed.projection != names:
                problems.append("projection neither pushed exactly nor residual")
    elif residual.projection is not None:
        problems.append("residual projection present without a logical projection")

    if plan.limit_count is not None:
        if pushed.limit != plan.limit_count and residual.limit != plan.limit_count:
            problems.append("limit neither pushed nor residual")
    elif pushed.limit is not None or residual.limit is not None:
        problems.append("limit applied without a logical limit")

    for node in execution_plan.explain.nodes:
        if node.disposition is Disposition.REJECTED:
            problems.append(f"rejected node {node.node_id} in executable plan")
    return tuple(problems)


__all__ = ["ExecutionPlan", "PushedOperations", "ResidualOperations", "check_completeness"]
