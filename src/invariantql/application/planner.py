"""The capability planner (ADR-0003).

For every logical operation the planner decides whether it is pushed to the
scan target, partially pushed with a residual, residual, or rejected, and
records why. The planner never drops an operation.
"""

from __future__ import annotations

from dataclasses import dataclass

from invariantql.application.binding import BoundPlan
from invariantql.domain.capabilities import EngineCapabilities, PushdownCapabilities, Support
from invariantql.domain.diagnostics import Diagnostic, DiagnosticCode
from invariantql.domain.execution import (
    ExecutionPlan,
    PushedOperations,
    ResidualOperations,
    check_completeness,
)
from invariantql.domain.explain import Disposition, ExecutionLocation, ExplainNode, ExplainPlan
from invariantql.domain.expressions import (
    Column,
    Expression,
    and_all,
    conjuncts,
    referenced_columns,
)
from invariantql.domain.plan import Filter, Limit, Project, Scan


@dataclass(frozen=True, slots=True)
class PlanningTarget:
    engine_name: str
    engine: EngineCapabilities
    scan: PushdownCapabilities
    reachable: bool = True
    reach_reason: str = ""


class CapabilityPlanner:
    def plan(self, bound: BoundPlan, target: PlanningTarget) -> ExecutionPlan:
        plan = bound.plan
        nodes: list[ExplainNode] = []
        diagnostics: list[Diagnostic] = []
        pushed_projection: tuple[str, ...] | None = None
        pushed_predicate: Expression | None = None
        pushed_limit: int | None = None
        residual_projection: tuple[Expression, ...] | None = None
        residual_predicate: Expression | None = None
        residual_limit: int | None = None
        staging_required = False

        for node_id, node in plan.node_ids():
            if isinstance(node, Scan):
                if target.reachable:
                    nodes.append(
                        ExplainNode(
                            node_id,
                            "scan",
                            Disposition.PUSHED,
                            ExecutionLocation.SOURCE,
                            DiagnosticCode.PUSHDOWN_FULL,
                            f"scan source {node.source.name!r}",
                            evidence=target.scan.evidence,
                        )
                    )
                else:
                    staging_required = True
                    nodes.append(
                        ExplainNode(
                            node_id,
                            "scan",
                            Disposition.REJECTED,
                            ExecutionLocation.NONE,
                            DiagnosticCode.REJECTED_SOURCE_UNREACHABLE,
                            target.reach_reason
                            or f"engine {target.engine_name!r} cannot reach source {node.source.name!r}",
                            evidence=target.scan.evidence,
                        )
                    )
                    diagnostics.append(
                        Diagnostic.error(
                            DiagnosticCode.STAGING_REQUIRED,
                            f"engine {target.engine_name!r} cannot reach source "
                            f"{node.source.name!r}; stage the data explicitly first",
                            node_id=node_id,
                            target=target.engine_name,
                        )
                    )
                continue

            if isinstance(node, Filter):
                pushed_parts: list[Expression] = []
                residual_parts: list[Expression] = []
                rejected_parts: list[tuple[Expression, str]] = []
                residual_reasons: list[str] = []
                for conjunct in conjuncts(node.predicate):
                    scan_supports = (
                        target.scan.predicate is not Support.NONE
                        and target.scan.supports_expression(conjunct)
                    )
                    if scan_supports and target.scan.predicate is Support.FULL:
                        pushed_parts.append(conjunct)
                    elif scan_supports and target.scan.predicate is Support.PARTIAL:
                        if target.engine.supports_expression(conjunct):
                            # A partial source predicate is only a safe
                            # relaxation.  The engine must recheck the complete
                            # predicate to restore its exact semantics.
                            pushed_parts.append(conjunct)
                            residual_parts.append(conjunct)
                            residual_reasons.append(
                                f"{conjunct}: scan target predicate support is partial; "
                                "engine rechecks"
                            )
                        else:
                            kinds = ", ".join(
                                k.value
                                for k in _kinds(conjunct)
                                if k not in target.engine.residual_expressions
                            )
                            rejected_parts.append((conjunct, kinds))
                    elif target.engine.supports_expression(conjunct):
                        residual_parts.append(conjunct)
                        if target.scan.predicate is Support.NONE:
                            residual_reasons.append(f"{conjunct}: scan target pushes no predicates")
                        else:
                            kinds = ", ".join(target.scan.unsupported_kinds(conjunct))
                            residual_reasons.append(
                                f"{conjunct}: unsupported by scan target ({kinds})"
                            )
                    else:
                        kinds = ", ".join(
                            k.value
                            for k in _kinds(conjunct)
                            if k not in target.engine.residual_expressions
                        )
                        rejected_parts.append((conjunct, kinds))

                pushed_predicate = and_all(pushed_parts)
                residual_predicate = and_all(residual_parts)
                if rejected_parts:
                    detail = "; ".join(
                        f"{c}: engine cannot evaluate {k}" for c, k in rejected_parts
                    )
                    nodes.append(
                        ExplainNode(
                            node_id,
                            "filter",
                            Disposition.REJECTED,
                            ExecutionLocation.NONE,
                            DiagnosticCode.REJECTED_ENGINE_UNSUPPORTED_EXPRESSION,
                            detail,
                            pushed=_fmt(pushed_predicate),
                            residual=_fmt(residual_predicate),
                            evidence=target.engine.evidence,
                        )
                    )
                    diagnostics.append(
                        Diagnostic.error(
                            DiagnosticCode.REJECTED_ENGINE_UNSUPPORTED_EXPRESSION,
                            detail,
                            node_id=node_id,
                            target=target.engine_name,
                        )
                    )
                elif not residual_parts:
                    nodes.append(
                        ExplainNode(
                            node_id,
                            "filter",
                            Disposition.PUSHED,
                            ExecutionLocation.SOURCE,
                            DiagnosticCode.PUSHDOWN_FULL,
                            f"{len(pushed_parts)} predicate(s) pushed to scan target",
                            pushed=_fmt(pushed_predicate),
                            evidence=target.scan.evidence,
                        )
                    )
                elif not pushed_parts:
                    code = (
                        DiagnosticCode.RESIDUAL_NO_CAPABILITY
                        if target.scan.predicate is Support.NONE
                        else DiagnosticCode.RESIDUAL_UNSUPPORTED_EXPRESSION
                    )
                    nodes.append(
                        ExplainNode(
                            node_id,
                            "filter",
                            Disposition.RESIDUAL,
                            ExecutionLocation.ENGINE,
                            code,
                            "; ".join(residual_reasons),
                            residual=_fmt(residual_predicate),
                            evidence=target.scan.evidence,
                        )
                    )
                else:
                    nodes.append(
                        ExplainNode(
                            node_id,
                            "filter",
                            Disposition.PARTIAL,
                            ExecutionLocation.SOURCE,
                            DiagnosticCode.PUSHDOWN_PARTIAL,
                            f"{len(pushed_parts)} pushed, {len(residual_parts)} "
                            "evaluated or rechecked by the engine: " + "; ".join(residual_reasons),
                            pushed=_fmt(pushed_predicate),
                            residual=_fmt(residual_predicate),
                            evidence=target.scan.evidence,
                        )
                    )
                continue

            if isinstance(node, Project):
                needed = referenced_columns(
                    *node.expressions,
                    *((residual_predicate,) if residual_predicate is not None else ()),
                )
                names = node.output_names
                pure_columns = all(isinstance(e, Column) for e in node.expressions)
                if target.scan.projection is Support.FULL and pure_columns and needed == names:
                    pushed_projection = names
                    nodes.append(
                        ExplainNode(
                            node_id,
                            "project",
                            Disposition.PUSHED,
                            ExecutionLocation.SOURCE,
                            DiagnosticCode.PUSHDOWN_FULL,
                            "column selection pushed to scan target",
                            pushed=", ".join(names),
                            evidence=target.scan.evidence,
                        )
                    )
                else:
                    residual_projection = node.expressions
                    pushed_projection = (
                        needed if target.scan.projection is not Support.NONE and needed else None
                    )
                    unsupported = [
                        expression
                        for expression in node.expressions
                        if not target.engine.supports_expression(expression)
                    ]
                    if not target.engine.residual_projection:
                        detail = f"engine {target.engine_name!r} cannot evaluate projections"
                        code = DiagnosticCode.REJECTED_ENGINE_UNSUPPORTED_OPERATION
                    elif unsupported:
                        kinds = sorted(
                            {
                                kind.value
                                for expression in unsupported
                                for kind in _kinds(expression)
                                if kind not in target.engine.residual_expressions
                            }
                        )
                        detail = (
                            f"engine {target.engine_name!r} cannot evaluate projection "
                            f"expression kind(s): {', '.join(kinds)}"
                        )
                        code = DiagnosticCode.REJECTED_ENGINE_UNSUPPORTED_EXPRESSION
                    else:
                        detail = ""
                        code = DiagnosticCode.RESIDUAL_COMPUTED_PROJECTION

                    if detail:
                        nodes.append(
                            ExplainNode(
                                node_id,
                                "project",
                                Disposition.REJECTED,
                                ExecutionLocation.NONE,
                                code,
                                detail,
                                pushed=(
                                    None
                                    if pushed_projection is None
                                    else ", ".join(pushed_projection)
                                ),
                                residual=", ".join(str(e) for e in node.expressions),
                                evidence=target.engine.evidence,
                            )
                        )
                        diagnostics.append(
                            Diagnostic.error(
                                code,
                                detail,
                                node_id=node_id,
                                target=target.engine_name,
                            )
                        )
                    elif target.scan.projection is Support.NONE:
                        nodes.append(
                            ExplainNode(
                                node_id,
                                "project",
                                Disposition.RESIDUAL,
                                ExecutionLocation.ENGINE,
                                DiagnosticCode.RESIDUAL_NO_CAPABILITY,
                                "scan target cannot prune columns; engine projects",
                                residual=", ".join(str(e) for e in node.expressions),
                                evidence=target.scan.evidence,
                            )
                        )
                    elif pushed_projection is None:
                        # Arrow has no portable representation for a non-zero
                        # row count with zero columns.  Reading all source
                        # columns preserves cardinality for ``SELECT 1 FROM t``.
                        nodes.append(
                            ExplainNode(
                                node_id,
                                "project",
                                Disposition.RESIDUAL,
                                ExecutionLocation.ENGINE,
                                DiagnosticCode.RESIDUAL_COMPUTED_PROJECTION,
                                "constant projection evaluated by the engine; source columns "
                                "retained to preserve row cardinality",
                                residual=", ".join(str(e) for e in node.expressions),
                                evidence=target.scan.evidence,
                            )
                        )
                    else:
                        if target.scan.projection is Support.PARTIAL:
                            reason = (
                                "scan target column pruning is partial; engine enforces the "
                                "projection"
                            )
                            code = DiagnosticCode.PUSHDOWN_PARTIAL
                        else:
                            reason = (
                                "computed or aliased expressions evaluated by the engine"
                                if not pure_columns
                                else "extra columns read for the residual predicate; engine trims"
                            )
                            reason = f"column pruning pushed; {reason}"
                            code = DiagnosticCode.RESIDUAL_COMPUTED_PROJECTION
                        nodes.append(
                            ExplainNode(
                                node_id,
                                "project",
                                Disposition.PARTIAL,
                                ExecutionLocation.SOURCE,
                                code,
                                reason,
                                pushed=", ".join(pushed_projection),
                                residual=", ".join(str(e) for e in node.expressions),
                                evidence=target.scan.evidence,
                            )
                        )
                continue

            if isinstance(node, Limit):
                if residual_predicate is not None:
                    if target.engine.residual_limit:
                        residual_limit = node.count
                        nodes.append(
                            ExplainNode(
                                node_id,
                                "limit",
                                Disposition.RESIDUAL,
                                ExecutionLocation.ENGINE,
                                DiagnosticCode.RESIDUAL_LIMIT_AFTER_RESIDUAL_FILTER,
                                "a residual predicate must run before the limit",
                                residual=str(node.count),
                            )
                        )
                    else:
                        _reject_residual_limit(nodes, diagnostics, node_id, node.count, target)
                elif target.scan.limit is Support.FULL:
                    pushed_limit = node.count
                    nodes.append(
                        ExplainNode(
                            node_id,
                            "limit",
                            Disposition.PUSHED,
                            ExecutionLocation.SOURCE,
                            DiagnosticCode.PUSHDOWN_FULL,
                            "limit pushed to scan target",
                            pushed=str(node.count),
                            evidence=target.scan.evidence,
                        )
                    )
                elif target.scan.limit is Support.PARTIAL:
                    if target.engine.residual_limit:
                        pushed_limit = node.count
                        residual_limit = node.count
                        nodes.append(
                            ExplainNode(
                                node_id,
                                "limit",
                                Disposition.PARTIAL,
                                ExecutionLocation.SOURCE,
                                DiagnosticCode.PUSHDOWN_PARTIAL,
                                "scan target treats the limit as a hint; engine re-applies it",
                                pushed=str(node.count),
                                residual=str(node.count),
                                evidence=target.scan.evidence,
                            )
                        )
                    else:
                        _reject_residual_limit(nodes, diagnostics, node_id, node.count, target)
                else:
                    if target.engine.residual_limit:
                        residual_limit = node.count
                        nodes.append(
                            ExplainNode(
                                node_id,
                                "limit",
                                Disposition.RESIDUAL,
                                ExecutionLocation.ENGINE,
                                DiagnosticCode.RESIDUAL_NO_CAPABILITY,
                                "scan target cannot limit; engine applies it",
                                residual=str(node.count),
                                evidence=target.scan.evidence,
                            )
                        )
                    else:
                        _reject_residual_limit(nodes, diagnostics, node_id, node.count, target)
                continue

        executable = not any(n.disposition is Disposition.REJECTED for n in nodes)
        explain = ExplainPlan(
            engine=target.engine_name,
            source=plan.source.name,
            fingerprint=plan.fingerprint(),
            nodes=tuple(nodes),
            executable=executable,
            staging_required=staging_required,
            diagnostics=tuple(diagnostics),
            scan_capabilities=target.scan.to_dict(),
            engine_capabilities=target.engine.to_dict(),
        )
        execution_plan = ExecutionPlan(
            plan=plan,
            engine=target.engine_name,
            source=plan.source.name,
            schema=bound.schema,
            output_schema=bound.output_schema,
            pushed=PushedOperations(pushed_projection, pushed_predicate, pushed_limit),
            residual=ResidualOperations(residual_projection, residual_predicate, residual_limit),
            explain=explain,
        )
        problems = check_completeness(execution_plan)
        if problems:  # pragma: no cover - guarded by FF-05 tests
            raise AssertionError(
                "planner violated the completeness invariant: " + "; ".join(problems)
            )
        return execution_plan


def _fmt(expression: Expression | None) -> str | None:
    return None if expression is None else str(expression)


def _kinds(expression: Expression):
    from invariantql.domain.expressions import walk

    return {node.kind for node in walk(expression)}


def _reject_residual_limit(
    nodes: list[ExplainNode],
    diagnostics: list[Diagnostic],
    node_id: str,
    count: int,
    target: PlanningTarget,
) -> None:
    detail = f"engine {target.engine_name!r} cannot enforce a residual limit"
    code = DiagnosticCode.REJECTED_ENGINE_UNSUPPORTED_OPERATION
    nodes.append(
        ExplainNode(
            node_id,
            "limit",
            Disposition.REJECTED,
            ExecutionLocation.NONE,
            code,
            detail,
            residual=str(count),
            evidence=target.engine.evidence,
        )
    )
    diagnostics.append(Diagnostic.error(code, detail, node_id=node_id, target=target.engine_name))


__all__ = ["CapabilityPlanner", "PlanningTarget"]
