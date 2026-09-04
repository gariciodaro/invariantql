"""The immutable logical query plan: the invariant core (ADR-0002, ADR-0007).

A plan is a linear chain over exactly one source::

    Scan(source) -> Filter(predicate)? -> Project(expressions)? -> Limit(count)?

The order encodes SQL semantics: ``WHERE`` sees source columns, ``SELECT``
produces the output columns, ``LIMIT`` bounds the output. The chain shape is
validated on construction so that structurally equivalent plans are equal and
fingerprint identically.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, ClassVar

from invariantql.domain.expressions import (
    Alias,
    Column,
    Expression,
    and_all,
    conjuncts,
    expression_from_dict,
    output_name,
    referenced_columns,
    referenced_parameters,
)

PLAN_FORMAT_VERSION = "1"


@dataclass(frozen=True, slots=True)
class SourceRef:
    """A logical source name. It carries no credentials or connection state."""

    name: str

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("source name must not be empty")

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class PlanNode:
    operation: ClassVar[str] = "node"

    @property
    def input(self) -> PlanNode | None:
        return None

    def to_dict(self) -> dict[str, Any]:  # pragma: no cover - overridden
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class Scan(PlanNode):
    operation: ClassVar[str] = "scan"
    source: SourceRef

    def to_dict(self) -> dict[str, Any]:
        return {"op": "scan", "source": self.source.name}


@dataclass(frozen=True, slots=True)
class Filter(PlanNode):
    operation: ClassVar[str] = "filter"
    child: PlanNode
    predicate: Expression

    @property
    def input(self) -> PlanNode:
        return self.child

    def to_dict(self) -> dict[str, Any]:
        return {
            "op": "filter",
            "input": self.child.to_dict(),
            "predicate": self.predicate.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class Project(PlanNode):
    operation: ClassVar[str] = "project"
    child: PlanNode
    expressions: tuple[Expression, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "expressions", tuple(self.expressions))
        if not self.expressions:
            raise ValueError("projection must have at least one expression")
        names: set[str] = set()
        for expression in self.expressions:
            if not isinstance(expression, (Column, Alias)):
                raise ValueError("projected expressions must be columns or aliases")
            name = output_name(expression)
            if name in names:
                raise ValueError(f"duplicate output column: {name!r}")
            names.add(name)

    @property
    def input(self) -> PlanNode:
        return self.child

    @property
    def output_names(self) -> tuple[str, ...]:
        return tuple(output_name(e) for e in self.expressions)

    def to_dict(self) -> dict[str, Any]:
        return {
            "op": "project",
            "input": self.child.to_dict(),
            "expressions": [e.to_dict() for e in self.expressions],
        }


@dataclass(frozen=True, slots=True)
class Limit(PlanNode):
    operation: ClassVar[str] = "limit"
    child: PlanNode
    count: int

    def __post_init__(self) -> None:
        if isinstance(self.count, bool) or not isinstance(self.count, int) or self.count < 0:
            raise ValueError("limit must be a non-negative integer")

    @property
    def input(self) -> PlanNode:
        return self.child

    def to_dict(self) -> dict[str, Any]:
        return {"op": "limit", "input": self.child.to_dict(), "count": self.count}


_ORDER = {"scan": 0, "filter": 1, "project": 2, "limit": 3}


@dataclass(frozen=True, slots=True)
class QueryPlan:
    """An immutable, deterministic, single-source logical plan."""

    root: PlanNode

    def __post_init__(self) -> None:
        chain = self.nodes
        if not chain or not isinstance(chain[0], Scan):
            raise ValueError("a plan must start with a scan")
        last = -1
        for node in chain:
            rank = _ORDER.get(node.operation)
            if rank is None or rank <= last:
                raise ValueError(
                    "plan nodes must follow the order scan -> filter -> project -> limit"
                )
            last = rank

    # -- construction -------------------------------------------------------

    @classmethod
    def scan(cls, source: str | SourceRef) -> QueryPlan:
        ref = source if isinstance(source, SourceRef) else SourceRef(source)
        return cls(Scan(ref))

    def where(self, predicate: Expression) -> QueryPlan:
        """Add a predicate, conjoined with any existing predicate."""

        combined = and_all((*conjuncts(self.predicate), *conjuncts(predicate)))
        assert combined is not None
        return self._rebuild(predicate=combined)

    def select(self, *expressions: Expression | str) -> QueryPlan:
        exprs = tuple(Column(e) if isinstance(e, str) else e for e in expressions)
        return self._rebuild(projection=exprs)

    def limit(self, count: int) -> QueryPlan:
        """Bound the output; a smaller existing limit is preserved."""

        current = self.limit_count
        new = count if current is None else min(current, count)
        return self._rebuild(limit=new)

    def _rebuild(
        self,
        *,
        predicate: Expression | None = None,
        projection: tuple[Expression, ...] | None = None,
        limit: int | None = None,
    ) -> QueryPlan:
        predicate = self.predicate if predicate is None else predicate
        projection = self.projection if projection is None else projection
        limit = self.limit_count if limit is None else limit
        node: PlanNode = Scan(self.source)
        if predicate is not None:
            node = Filter(node, predicate)
        if projection is not None:
            node = Project(node, projection)
        if limit is not None:
            node = Limit(node, limit)
        return QueryPlan(node)

    # -- inspection -----------------------------------------------------------

    @property
    def nodes(self) -> tuple[PlanNode, ...]:
        """Nodes from the scan outward."""

        out: list[PlanNode] = []
        node: PlanNode | None = self.root
        while node is not None:
            out.append(node)
            node = node.input
        out.reverse()
        return tuple(out)

    def node_ids(self) -> tuple[tuple[str, PlanNode], ...]:
        """Stable identifiers for explain output: ``<index>-<operation>``."""

        return tuple((f"{i}-{n.operation}", n) for i, n in enumerate(self.nodes))

    @property
    def source(self) -> SourceRef:
        first = self.nodes[0]
        assert isinstance(first, Scan)
        return first.source

    @property
    def predicate(self) -> Expression | None:
        for node in self.nodes:
            if isinstance(node, Filter):
                return node.predicate
        return None

    @property
    def projection(self) -> tuple[Expression, ...] | None:
        for node in self.nodes:
            if isinstance(node, Project):
                return node.expressions
        return None

    @property
    def limit_count(self) -> int | None:
        for node in self.nodes:
            if isinstance(node, Limit):
                return node.count
        return None

    @property
    def output_names(self) -> tuple[str, ...] | None:
        projection = self.projection
        return None if projection is None else tuple(output_name(e) for e in projection)

    @property
    def referenced_columns(self) -> tuple[str, ...]:
        """Source columns the plan needs, or ``()`` when it needs every column."""

        exprs: list[Expression] = []
        if self.projection is not None:
            exprs.extend(self.projection)
        if self.predicate is not None:
            exprs.append(self.predicate)
        return referenced_columns(*exprs)

    @property
    def parameters(self) -> tuple[str, ...]:
        exprs: list[Expression] = []
        if self.projection is not None:
            exprs.extend(self.projection)
        if self.predicate is not None:
            exprs.append(self.predicate)
        return referenced_parameters(*exprs)

    # -- serialization ----------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {"version": PLAN_FORMAT_VERSION, "root": self.root.to_dict()}

    def canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"), default=str)

    def fingerprint(self) -> str:
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QueryPlan:
        if str(data.get("version")) != PLAN_FORMAT_VERSION:
            raise ValueError(f"unsupported plan version: {data.get('version')!r}")
        return cls(_node_from_dict(data["root"]))

    def __str__(self) -> str:
        parts = [f"FROM {self.source}"]
        if self.projection is not None:
            parts.insert(0, "SELECT " + ", ".join(str(e) for e in self.projection))
        else:
            parts.insert(0, "SELECT *")
        if self.predicate is not None:
            parts.append(f"WHERE {self.predicate}")
        if self.limit_count is not None:
            parts.append(f"LIMIT {self.limit_count}")
        return " ".join(parts)


def _node_from_dict(data: dict[str, Any]) -> PlanNode:
    op = data["op"]
    if op == "scan":
        return Scan(SourceRef(data["source"]))
    child = _node_from_dict(data["input"])
    if op == "filter":
        return Filter(child, expression_from_dict(data["predicate"]))
    if op == "project":
        return Project(child, tuple(expression_from_dict(e) for e in data["expressions"]))
    if op == "limit":
        return Limit(child, data["count"])
    raise ValueError(f"unknown plan operation: {op!r}")


__all__ = [
    "PLAN_FORMAT_VERSION",
    "Filter",
    "Limit",
    "PlanNode",
    "Project",
    "QueryPlan",
    "Scan",
    "SourceRef",
]
