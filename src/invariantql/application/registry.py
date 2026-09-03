"""Explicit adapter registration (ADR-0010).

Sources, engines, and frontends are registered by name. Factories allow the
API layer to defer importing provider modules until an adapter is first used;
a missing optional dependency surfaces as ``MissingDependencyError``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from invariantql.domain.diagnostics import DiagnosticCode, RegistryError
from invariantql.ports.engine import ExecutionEngine
from invariantql.ports.frontend import QueryFrontend
from invariantql.ports.source import DataSource

T = TypeVar("T")


class Registry:
    def __init__(self) -> None:
        self._sources: dict[str, DataSource] = {}
        self._engines: dict[str, ExecutionEngine] = {}
        self._engine_factories: dict[str, Callable[[], ExecutionEngine]] = {}
        self._frontends: dict[str, QueryFrontend] = {}
        self._frontend_factories: dict[str, Callable[[], QueryFrontend]] = {}

    # -- sources ------------------------------------------------------------

    def register_source(self, source: DataSource, *, replace: bool = False) -> DataSource:
        name = source.name
        if not name:
            raise RegistryError(
                "source name must not be empty", code=DiagnosticCode.SOURCE_NOT_REGISTERED
            )
        if name in self._sources and not replace:
            raise RegistryError(
                f"a source named {name!r} is already registered (pass replace=True to override)",
                code=DiagnosticCode.SOURCE_ALREADY_REGISTERED,
                target=name,
            )
        self._sources[name] = source
        return source

    def unregister_source(self, name: str) -> None:
        source = self._sources.pop(name, None)
        if source is None:
            raise RegistryError(f"no source named {name!r}", target=name)
        source.close()

    def source(self, name: str) -> DataSource:
        try:
            return self._sources[name]
        except KeyError:
            known = ", ".join(sorted(self._sources)) or "none"
            raise RegistryError(
                f"no source named {name!r} is registered (known: {known})",
                code=DiagnosticCode.SOURCE_NOT_REGISTERED,
                target=name,
            ) from None

    @property
    def sources(self) -> tuple[str, ...]:
        return tuple(self._sources)

    # -- engines ------------------------------------------------------------

    def register_engine(self, engine: ExecutionEngine, *, replace: bool = False) -> ExecutionEngine:
        if engine.name in self._engines and not replace:
            raise RegistryError(
                f"an engine named {engine.name!r} is already registered",
                code=DiagnosticCode.ENGINE_UNKNOWN,
                target=engine.name,
            )
        self._engines[engine.name] = engine
        return engine

    def register_engine_factory(self, name: str, factory: Callable[[], ExecutionEngine]) -> None:
        self._engine_factories[name] = factory

    def engine(self, name: str) -> ExecutionEngine:
        engine = self._engines.get(name)
        if engine is not None:
            return engine
        factory = self._engine_factories.get(name)
        if factory is None:
            known = ", ".join(sorted({*self._engines, *self._engine_factories})) or "none"
            raise RegistryError(
                f"no engine named {name!r} is registered (known: {known})",
                code=DiagnosticCode.ENGINE_UNKNOWN,
                target=name,
            )
        engine = factory()
        self._engines[name] = engine
        return engine

    def has_engine(self, name: str) -> bool:
        return name in self._engines or name in self._engine_factories

    @property
    def engines(self) -> tuple[str, ...]:
        return tuple(sorted({*self._engines, *self._engine_factories}))

    # -- frontends ----------------------------------------------------------

    def register_frontend(self, frontend: QueryFrontend, *, replace: bool = False) -> QueryFrontend:
        if frontend.name in self._frontends and not replace:
            raise RegistryError(
                f"a frontend named {frontend.name!r} is already registered",
                code=DiagnosticCode.ADAPTER_UNKNOWN,
                target=frontend.name,
            )
        self._frontends[frontend.name] = frontend
        return frontend

    def register_frontend_factory(self, name: str, factory: Callable[[], QueryFrontend]) -> None:
        self._frontend_factories[name] = factory

    def frontend(self, name: str = "sql") -> QueryFrontend:
        frontend = self._frontends.get(name)
        if frontend is not None:
            return frontend
        factory = self._frontend_factories.get(name)
        if factory is None:
            raise RegistryError(
                f"no frontend named {name!r} is registered",
                code=DiagnosticCode.ADAPTER_UNKNOWN,
                target=name,
            )
        frontend = factory()
        self._frontends[name] = frontend
        return frontend

    # -- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        errors: list[BaseException] = []
        for source in list(self._sources.values()):
            try:
                source.close()
            except Exception as exc:
                errors.append(exc)
        for engine in list(self._engines.values()):
            try:
                engine.close()
            except Exception as exc:
                errors.append(exc)
        self._sources.clear()
        self._engines.clear()
        if errors:
            raise errors[0]


__all__ = ["Registry"]
