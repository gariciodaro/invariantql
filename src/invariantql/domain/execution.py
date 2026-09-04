"""Execution-plan value objects produced by the capability planner (ADR-0003).

An ``ExecutionPlan`` splits a logical plan into operations pushed to the scan
target and operations that remain for the engine. Every logical operation
appears exactly once across the two halves unless the plan was rejected.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

from invariantql.domain.explain import Disposition, ExplainPlan
from invariantql.domain.expressions import (
    Column,
    Expression,
    conjuncts,
    output_name,
    referenced_columns,
    walk,
)
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

    scan_capabilities = execution_plan.explain.scan_capabilities or {}
    engine_capabilities = execution_plan.explain.engine_capabilities or {}
    logical = Counter(conjuncts(plan.predicate))
    pushed_predicates = Counter(conjuncts(pushed.predicate))
    residual_predicates = Counter(conjuncts(residual.predicate))
    predicate_support = scan_capabilities.get("predicate")
    if predicate_support not in ("full", "partial", "none"):
        problems.append(f"unknown predicate support: {predicate_support!r}")
    pushed_expression_kinds = set(scan_capabilities.get("expressions", ()))
    pushed_parameters = bool(scan_capabilities.get("parameters", False))
    for clause in pushed_predicates:
        unsupported = {
            node.kind.value
            for node in walk(clause)
            if node.kind.value not in pushed_expression_kinds
            or (node.kind.value == "parameter" and not pushed_parameters)
        }
        if predicate_support == "none" or unsupported:
            problems.append(
                f"pushed predicate exceeds scan capabilities: clause={clause} "
                f"support={predicate_support!r} unsupported={sorted(unsupported)}"
            )
    if predicate_support == "partial":
        # Every predicate evaluated by a partially semantic scan must be
        # rechecked, while predicates the scan cannot handle remain residual
        # only.  The current pushed representation stores the complete
        # predicate and lets the adapter apply its documented safe relaxation.
        if residual_predicates != logical or pushed_predicates - logical:
            problems.append(
                "partial predicate coverage differs: "
                f"logical={dict(logical)} pushed={dict(pushed_predicates)} "
                f"residual={dict(residual_predicates)}"
            )
    else:
        overlap = pushed_predicates & residual_predicates
        covered = pushed_predicates + residual_predicates
        if overlap or covered != logical:
            problems.append(
                "predicate conjuncts differ: "
                f"logical={dict(logical)} pushed={dict(pushed_predicates)} "
                f"residual={dict(residual_predicates)}"
            )
    residual_expression_kinds = set(engine_capabilities.get("residual_expressions", ()))
    for clause in residual_predicates:
        unsupported = {
            node.kind.value
            for node in walk(clause)
            if node.kind.value not in residual_expression_kinds
        }
        if unsupported:
            problems.append(
                f"residual predicate exceeds engine capabilities: clause={clause} "
                f"unsupported={sorted(unsupported)}"
            )

    if plan.projection is not None:
        projection_support = scan_capabilities.get("projection")
        names = tuple(output_name(expression) for expression in plan.projection)
        needed = referenced_columns(
            *plan.projection,
            *((residual.predicate,) if residual.predicate is not None else ()),
        )
        pure_columns = all(isinstance(expression, Column) for expression in plan.projection)
        if projection_support == "full" and pure_columns and needed == names:
            expected_pushed = names
            expected_residual = None
        else:
            expected_pushed = (
                needed if projection_support in ("full", "partial") and needed else None
            )
            expected_residual = plan.projection
        if pushed.projection != expected_pushed or residual.projection != expected_residual:
            problems.append(
                "projection placement differs: "
                f"pushed={pushed.projection} expected_pushed={expected_pushed} "
                f"residual={residual.projection} expected_residual={expected_residual} "
                f"support={projection_support!r}"
            )
    elif residual.projection is not None or pushed.projection is not None:
        problems.append("projection applied without a logical projection")
    if residual.projection is not None:
        if not bool(engine_capabilities.get("residual_projection", False)):
            problems.append("residual projection exceeds engine capabilities")
        unsupported_projection = {
            node.kind.value
            for expression in residual.projection
            for node in walk(expression)
            if node.kind.value not in residual_expression_kinds
        }
        if unsupported_projection:
            problems.append(
                "residual projection expression exceeds engine capabilities: "
                f"unsupported={sorted(unsupported_projection)}"
            )

    if plan.limit_count is not None:
        count = plan.limit_count
        limit_support = scan_capabilities.get("limit")
        if residual.predicate is not None:
            valid_limit = pushed.limit is None and residual.limit == count
        elif limit_support == "full":
            valid_limit = pushed.limit == count and residual.limit is None
        elif limit_support == "partial":
            valid_limit = pushed.limit == count and residual.limit == count
        elif limit_support == "none":
            valid_limit = pushed.limit is None and residual.limit == count
        else:
            valid_limit = False
        if not valid_limit:
            problems.append(
                f"limit placement differs: logical={count} pushed={pushed.limit} "
                f"residual={residual.limit} support={limit_support!r}"
            )
    elif pushed.limit is not None or residual.limit is not None:
        problems.append("limit applied without a logical limit")
    if residual.limit is not None and not bool(engine_capabilities.get("residual_limit", False)):
        problems.append("residual limit exceeds engine capabilities")

    for node in execution_plan.explain.nodes:
        if node.disposition is Disposition.REJECTED:
            problems.append(f"rejected node {node.node_id} in executable plan")
    return tuple(problems)


__all__ = ["ExecutionPlan", "PushedOperations", "ResidualOperations", "check_completeness"]
