"""The ``Query``: an immutable plan bound to a context."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from invariantql.api.builder import unwrap
from invariantql.domain.diagnostics import Diagnostic
from invariantql.domain.execution import ExecutionPlan
from invariantql.domain.explain import ExplainPlan
from invariantql.domain.expressions import Column, Expression
from invariantql.domain.plan import QueryPlan
from invariantql.domain.schema import Schema
from invariantql.ports.streams import LocalResult

if TYPE_CHECKING:
    from invariantql.api.context import Context


class Query:
    """A logical query. Every transformation returns a new ``Query``."""

    __slots__ = ("_context", "_plan")

    def __init__(self, plan: QueryPlan, context: Context) -> None:
        self._plan = plan
        self._context = context

    @property
    def plan(self) -> QueryPlan:
        return self._plan

    @property
    def context(self) -> Context:
        return self._context

    # -- transformations ----------------------------------------------------

    def select(self, *expressions: str | Expression | Any) -> Query:
        exprs = tuple(Column(e) if isinstance(e, str) else unwrap(e) for e in expressions)
        return Query(self._plan.select(*exprs), self._context)

    def where(self, predicate: Expression | Any) -> Query:
        return Query(self._plan.where(unwrap(predicate)), self._context)

    def limit(self, count: int) -> Query:
        return Query(self._plan.limit(count), self._context)

    # -- inspection -----------------------------------------------------------

    def explain(self, engine: str | None = None) -> ExplainPlan:
        return self._context.service.explain(self._plan, engine=engine)

    def execution_plan(self, engine: str | None = None) -> ExecutionPlan:
        return self._context.service.execution_plan(self._plan, engine=engine)

    def validate_for(self, engine: str | None = None) -> tuple[Diagnostic, ...]:
        return self._context.service.validate_for(self._plan, engine=engine)

    def is_portable(self, *engines: str) -> bool:
        """True when the plan is executable on every named engine."""

        targets = engines or tuple(self._context.engines)
        return all(not self.validate_for(engine) for engine in targets)

    def schema(self, engine: str | None = None) -> Schema:
        return self._context.service.output_schema(self._plan, engine=engine)

    def fingerprint(self) -> str:
        return self._plan.fingerprint()

    def to_dict(self) -> dict[str, Any]:
        return self._plan.to_dict()

    @property
    def parameters(self) -> tuple[str, ...]:
        return self._plan.parameters

    # -- execution ------------------------------------------------------------

    def preview(
        self,
        rows: int | None = None,
        *,
        engine: str | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> LocalResult:
        return self._context.service.preview(
            self._plan, rows=rows, engine=engine, parameters=params
        )

    def execute(
        self,
        *,
        engine: str | None = None,
        params: Mapping[str, Any] | None = None,
        batch_size: int | None = None,
    ) -> LocalResult:
        return self._context.service.execute(
            self._plan, engine=engine, parameters=params, batch_size=batch_size
        )

    def compile(self, *, engine: str = "spark", params: Mapping[str, Any] | None = None) -> Any:
        return self._context.service.compile(self._plan, engine=engine, parameters=params)

    def __str__(self) -> str:
        return str(self._plan)

    def __repr__(self) -> str:
        return f"Query({self._plan})"


__all__ = ["Query"]
