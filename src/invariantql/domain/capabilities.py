"""Capability descriptors (ADR-0003).

Adapters describe what they can evaluate; the planner matches every logical
operation against these descriptors. Capabilities describe supported
semantics, not performance claims.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from invariantql.domain.expressions import (
    ALL_EXPRESSION_KINDS,
    Expression,
    ExpressionKind,
    walk,
)


class Support(str, Enum):
    FULL = "full"
    PARTIAL = "partial"
    NONE = "none"


@dataclass(frozen=True, slots=True)
class PushdownCapabilities:
    """What a scan target (a source or an engine's native reader) can push down.

    ``expressions`` lists the node kinds the target evaluates with portable
    semantics. The planner always splits a top-level conjunction and decides
    per conjunct; a target that pushes predicates at all is assumed to be able
    to conjoin the pushed conjuncts. ``ExpressionKind.AND`` in ``expressions``
    therefore governs conjunctions nested inside ``OR``/``NOT`` only.
    """

    projection: Support = Support.NONE
    predicate: Support = Support.NONE
    limit: Support = Support.NONE
    expressions: frozenset[ExpressionKind] = frozenset()
    parameters: bool = False
    evidence: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "expressions", frozenset(self.expressions))
        object.__setattr__(self, "evidence", tuple(self.evidence))

    def supports_expression(self, expression: Expression) -> bool:
        """True when every node of the expression tree is a supported kind."""

        for node in walk(expression):
            if node.kind not in self.expressions:
                return False
            if node.kind is ExpressionKind.PARAMETER and not self.parameters:
                return False
        return True

    def unsupported_kinds(self, expression: Expression) -> tuple[str, ...]:
        found: dict[str, None] = {}
        for node in walk(expression):
            if node.kind not in self.expressions:
                found.setdefault(node.kind.value, None)
            elif node.kind is ExpressionKind.PARAMETER and not self.parameters:
                found.setdefault("parameter", None)
        return tuple(found)

    def to_dict(self) -> dict[str, Any]:
        return {
            "projection": self.projection.value,
            "predicate": self.predicate.value,
            "limit": self.limit.value,
            "expressions": sorted(k.value for k in self.expressions),
            "parameters": self.parameters,
            "evidence": list(self.evidence),
        }

    @classmethod
    def none(cls, *evidence: str) -> PushdownCapabilities:
        return cls(evidence=tuple(evidence))

    @classmethod
    def full(cls, *evidence: str) -> PushdownCapabilities:
        return cls(
            projection=Support.FULL,
            predicate=Support.FULL,
            limit=Support.FULL,
            expressions=ALL_EXPRESSION_KINDS,
            parameters=True,
            evidence=tuple(evidence),
        )


@dataclass(frozen=True, slots=True)
class EngineCapabilities:
    """What an execution engine can evaluate as residual work."""

    name: str
    residual_expressions: frozenset[ExpressionKind] = ALL_EXPRESSION_KINDS
    residual_projection: bool = True
    residual_limit: bool = True
    lazy: bool = False
    evidence: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "residual_expressions", frozenset(self.residual_expressions))
        object.__setattr__(self, "evidence", tuple(self.evidence))

    def supports_expression(self, expression: Expression) -> bool:
        return all(node.kind in self.residual_expressions for node in walk(expression))

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "residual_expressions": sorted(k.value for k in self.residual_expressions),
            "residual_projection": self.residual_projection,
            "residual_limit": self.residual_limit,
            "lazy": self.lazy,
            "evidence": list(self.evidence),
        }


__all__ = ["EngineCapabilities", "PushdownCapabilities", "Support"]
