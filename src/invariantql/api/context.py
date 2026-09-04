"""The ``Context``: the composition root and public entry point.

A context owns a registry of sources and engines and a query service. It
wires built-in adapters lazily (ADR-0010): importing this module imports no
provider SDK; a missing extra surfaces when the adapter is first used.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, NoReturn

from invariantql.api.loading import load_adapter
from invariantql.api.query import Query
from invariantql.application.registry import Registry
from invariantql.application.service import DEFAULT_BATCH_SIZE, DEFAULT_PREVIEW_ROWS, QueryService
from invariantql.domain.diagnostics import MissingDependencyError
from invariantql.domain.plan import QueryPlan
from invariantql.ports.engine import ExecutionEngine
from invariantql.ports.source import DataSource

# Generic (engine-agnostic) format handlers keyed by format name. Each entry is
# (module, local handler class, distributed handler class, extra).
_FORMAT_HANDLERS: dict[str, tuple[str, str, str, str | None]] = {
    "xml": (
        "invariantql.adapters.formats.xml",
        "XmlLocalHandler",
        "XmlReaderSpecHandler",
        "xml",
    ),
    "delta": (
        "invariantql.adapters.formats.delta",
        "DeltaLocalHandler",
        "DeltaReaderSpecHandler",
        "delta",
    ),
    "iceberg": (
        "invariantql.adapters.formats.iceberg",
        "IcebergLocalHandler",
        "IcebergReaderSpecHandler",
        "iceberg",
    ),
}


class _UnavailableFormatHandler:
    """Remember a recognized format whose optional dependency is absent.

    Registering this sentinel lets reachability and schema discovery retain the
    original, actionable ``MissingDependencyError`` instead of degrading into
    a generic unsupported-format error on first use.
    """

    def __init__(self, format_name: str, error: MissingDependencyError) -> None:
        self.format_name = format_name
        self._diagnostic = error.diagnostic

    def _raise(self) -> NoReturn:
        raise MissingDependencyError(
            self._diagnostic.message,
            diagnostic=self._diagnostic,
        ) from None

    def capabilities(self, *_args: Any, **_kwargs: Any) -> NoReturn:
        self._raise()

    def schema(self, *_args: Any, **_kwargs: Any) -> NoReturn:
        self._raise()

    def scan(self, *_args: Any, **_kwargs: Any) -> NoReturn:
        self._raise()

    def reader_spec(self, *_args: Any, **_kwargs: Any) -> NoReturn:
        self._raise()


class Context:
    def __init__(
        self,
        *,
        default_engine: str = "duckdb",
        preview_rows: int = DEFAULT_PREVIEW_ROWS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        sql_dialect: str | None = None,
        registry: Registry | None = None,
        duckdb_options: dict[str, Any] | None = None,
    ) -> None:
        self.registry = registry or Registry()
        self._duckdb_options = dict(duckdb_options or {})
        self.registry.register_frontend_factory(
            "sql",
            lambda: load_adapter("invariantql.adapters.sql", "SqlFrontend", extra=None)(
                dialect=sql_dialect
            ),
        )
        self.registry.register_engine_factory("duckdb", self._make_duckdb_engine)
        self.service = QueryService(
            self.registry,
            default_engine=default_engine,
            preview_rows=preview_rows,
            batch_size=batch_size,
        )

    # -- sources ------------------------------------------------------------

    def register_source(self, source: DataSource, *, replace: bool = False) -> DataSource:
        return self.registry.register_source(source, replace=replace)

    def unregister_source(self, name: str) -> None:
        self.registry.unregister_source(name)

    def source(self, name: str) -> DataSource:
        return self.registry.source(name)

    @property
    def sources(self) -> tuple[str, ...]:
        return self.registry.sources

    # -- engines ------------------------------------------------------------

    def register_engine(self, engine: ExecutionEngine, *, replace: bool = False) -> ExecutionEngine:
        self._register_format_handlers(engine)
        return self.registry.register_engine(engine, replace=replace)

    def engine(self, name: str) -> ExecutionEngine:
        return self.registry.engine(name)

    @property
    def engines(self) -> tuple[str, ...]:
        return self.registry.engines

    def use_spark(
        self, spark: Any, *, name: str = "spark", replace: bool = True
    ) -> ExecutionEngine:
        """Register a Spark engine over an existing ``SparkSession``. Never mutates the session."""

        engine_cls = load_adapter("invariantql.adapters.spark_engine", "SparkEngine", extra="spark")
        return self.register_engine(engine_cls(spark, name=name), replace=replace)

    def _make_duckdb_engine(self) -> ExecutionEngine:
        engine_cls = load_adapter(
            "invariantql.adapters.duckdb_engine", "DuckDBEngine", extra="duckdb"
        )
        engine = engine_cls(**self._duckdb_options)
        self._register_format_handlers(engine)
        return engine

    def _register_format_handlers(self, engine: Any) -> None:
        register: Callable[[Any], None] | None = getattr(engine, "register_format_handler", None)
        if register is None:
            return
        wants_local = getattr(engine, "handler_kind", "local") == "local"
        for format_name, (module, local_cls, distributed_cls, extra) in _FORMAT_HANDLERS.items():
            attribute = local_cls if wants_local else distributed_cls
            try:
                handler_cls = load_adapter(module, attribute, extra=extra)
            except MissingDependencyError as exc:
                register(_UnavailableFormatHandler(format_name, exc))
            else:
                register(handler_cls())

    # -- queries ------------------------------------------------------------

    def sql(self, text: str) -> Query:
        return Query(self.service.parse(text), self)

    def query(self, source: str) -> Query:
        """Start a query over a registered source with the expression builder."""

        return Query(QueryPlan.scan(source), self)

    def from_plan(self, plan: QueryPlan | dict[str, Any]) -> Query:
        if isinstance(plan, dict):
            plan = QueryPlan.from_dict(plan)
        return Query(plan, self)

    # -- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        self.registry.close()

    def __enter__(self) -> Context:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"Context(sources={list(self.sources)}, engines={list(self.engines)})"


__all__ = ["Context"]
