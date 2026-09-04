"""The query application service: explain, validate, preview, execute, compile.

The service orchestrates use cases without provider knowledge. Planning is
synchronous and side-effect free once the source schema is known (ADR-0008).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from invariantql.application.binding import BoundPlan, bind_plan
from invariantql.application.parameters import bind_parameters
from invariantql.application.planner import CapabilityPlanner, PlanningTarget
from invariantql.application.registry import Registry
from invariantql.domain.diagnostics import (
    Diagnostic,
    DiagnosticCode,
    InvariantQLError,
    SourceError,
    StagingRequiredError,
    UnsupportedOperationError,
)
from invariantql.domain.execution import ExecutionPlan
from invariantql.domain.explain import ExplainPlan
from invariantql.domain.expressions import Literal
from invariantql.domain.plan import QueryPlan
from invariantql.domain.schema import Schema
from invariantql.ports.engine import CompilingExecutionEngine, ExecutionEngine, LocalExecutionEngine
from invariantql.ports.source import DataSource
from invariantql.ports.streams import LocalResult

DEFAULT_PREVIEW_ROWS = 1000
DEFAULT_BATCH_SIZE = 65_536


class QueryService:
    def __init__(
        self,
        registry: Registry,
        *,
        planner: CapabilityPlanner | None = None,
        default_engine: str = "duckdb",
        preview_rows: int = DEFAULT_PREVIEW_ROWS,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        if isinstance(preview_rows, bool) or not isinstance(preview_rows, int) or preview_rows < 0:
            raise ValueError("preview_rows must be a non-negative integer")
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        self.registry = registry
        self.planner = planner or CapabilityPlanner()
        self.default_engine = default_engine
        self.preview_rows = preview_rows
        self.batch_size = batch_size

    # -- frontends ----------------------------------------------------------

    def parse(self, text: str, *, frontend: str = "sql") -> QueryPlan:
        return self.registry.frontend(frontend).parse(text)

    # -- planning -----------------------------------------------------------

    def schema(self, plan: QueryPlan, *, engine: str | None = None) -> Schema:
        engine_obj = self._engine(engine)
        source = self.registry.source(plan.source.name)
        return self._discover_schema(source, engine_obj)

    def _discover_schema(self, source: DataSource, engine_obj: ExecutionEngine) -> Schema:
        """Schema discovery is an inspection step, not execution (ADR-0008).

        Order: the selected engine when it can reach the source, the source's own
        declaration, then the default local engine. The first error is kept when
        every route fails so that the diagnostic names the selected engine.
        """

        errors: list[InvariantQLError] = []
        routes: list[Callable[[], Schema]] = []
        if engine_obj.reachability(source).reachable:
            routes.append(lambda: engine_obj.schema(source))
        routes.append(source.schema)
        if engine_obj.name != self.default_engine and self.registry.has_engine(self.default_engine):
            fallback = self.registry.engine(self.default_engine)
            if fallback.reachability(source).reachable:
                routes.append(lambda: fallback.schema(source))
        for route in routes:
            try:
                return route()
            except InvariantQLError as exc:
                errors.append(exc)
        if errors:
            raise errors[0]
        raise SourceError(
            f"no engine can discover the schema of source {source.name!r}",
            code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
            target=source.name,
        )

    def bind(self, plan: QueryPlan, *, engine: str | None = None) -> BoundPlan:
        return bind_plan(plan, self.schema(plan, engine=engine))

    def output_schema(self, plan: QueryPlan, *, engine: str | None = None) -> Schema:
        return self.bind(plan, engine=engine).output_schema

    def execution_plan(self, plan: QueryPlan, *, engine: str | None = None) -> ExecutionPlan:
        engine_obj = self._engine(engine)
        return self._build_execution_plan(plan, engine_obj)

    def _build_execution_plan(
        self,
        plan: QueryPlan,
        engine_obj: ExecutionEngine,
        parameters: Mapping[str, Literal] | None = None,
    ) -> ExecutionPlan:
        source = self.registry.source(plan.source.name)
        bound = bind_plan(
            plan,
            self._discover_schema(source, engine_obj),
            parameters,
        )
        reach = engine_obj.reachability(source)
        target = PlanningTarget(
            engine_name=engine_obj.name,
            engine=engine_obj.capabilities(),
            scan=engine_obj.scan_capabilities(source),
            reachable=reach.reachable,
            reach_reason=reach.reason,
        )
        return self.planner.plan(bound, target)

    def explain(self, plan: QueryPlan, *, engine: str | None = None) -> ExplainPlan:
        return self.execution_plan(plan, engine=engine).explain

    def validate_for(self, plan: QueryPlan, *, engine: str | None = None) -> tuple[Diagnostic, ...]:
        """Diagnostics that prevent execution on the engine; empty means portable."""

        return self.execution_plan(plan, engine=engine).explain.diagnostics

    # -- execution ----------------------------------------------------------

    def execute(
        self,
        plan: QueryPlan,
        *,
        engine: str | None = None,
        parameters: Mapping[str, Any] | None = None,
        batch_size: int | None = None,
    ) -> LocalResult:
        selected_batch_size = self.batch_size if batch_size is None else batch_size
        if (
            isinstance(selected_batch_size, bool)
            or not isinstance(selected_batch_size, int)
            or selected_batch_size <= 0
        ):
            raise ValueError("batch_size must be a positive integer")
        engine_obj = self._engine(engine)
        if not isinstance(engine_obj, LocalExecutionEngine):
            raise UnsupportedOperationError(
                f"engine {engine_obj.name!r} does not execute locally; use compile()",
                code=DiagnosticCode.ENGINE_PLAN_NOT_EXECUTABLE,
                target=engine_obj.name,
            )
        bound_params = bind_parameters(plan, parameters)
        execution_plan = self._executable(plan, engine_obj, bound_params)
        source = self.registry.source(plan.source.name)
        return engine_obj.execute(
            execution_plan, source, bound_params, batch_size=selected_batch_size
        )

    def preview(
        self,
        plan: QueryPlan,
        *,
        rows: int | None = None,
        engine: str | None = None,
        parameters: Mapping[str, Any] | None = None,
    ) -> LocalResult:
        bounded = plan.limit(self.preview_rows if rows is None else rows)
        return self.execute(bounded, engine=engine, parameters=parameters)

    def compile(
        self,
        plan: QueryPlan,
        *,
        engine: str | None = None,
        parameters: Mapping[str, Any] | None = None,
    ) -> Any:
        engine_obj = self._engine(engine)
        if not isinstance(engine_obj, CompilingExecutionEngine):
            raise UnsupportedOperationError(
                f"engine {engine_obj.name!r} does not compile lazy relations; use execute()",
                code=DiagnosticCode.ENGINE_PLAN_NOT_EXECUTABLE,
                target=engine_obj.name,
            )
        bound_params = bind_parameters(plan, parameters)
        execution_plan = self._executable(plan, engine_obj, bound_params)
        source = self.registry.source(plan.source.name)
        return engine_obj.compile(execution_plan, source, bound_params)

    # -- helpers ------------------------------------------------------------

    def _engine(self, name: str | None) -> ExecutionEngine:
        return self.registry.engine(name or self.default_engine)

    def _executable(
        self,
        plan: QueryPlan,
        engine_obj: ExecutionEngine,
        parameters: Mapping[str, Literal] | None = None,
    ) -> ExecutionPlan:
        execution_plan = self._build_execution_plan(plan, engine_obj, parameters)
        if execution_plan.executable:
            return execution_plan
        first = (
            execution_plan.explain.diagnostics[0] if execution_plan.explain.diagnostics else None
        )
        if execution_plan.staging_required:
            raise StagingRequiredError(
                first.message if first else "source is not reachable by the engine",
                code=DiagnosticCode.STAGING_REQUIRED,
                target=engine_obj.name,
            )
        raise UnsupportedOperationError(
            first.message if first else "plan is not executable on this engine",
            code=first.code if first else DiagnosticCode.ENGINE_PLAN_NOT_EXECUTABLE,
            node_id=first.node_id if first else None,
            target=engine_obj.name,
        )


__all__ = ["DEFAULT_BATCH_SIZE", "DEFAULT_PREVIEW_ROWS", "QueryService"]
