"""The ``ExecutionEngine`` port (ADR-0005, ADR-0008).

Local engines execute synchronously and stream Arrow-compatible batches.
Compiling engines (Spark) return an engine-specific lazy relation and never
perform an action during compilation.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from invariantql.domain.capabilities import EngineCapabilities, PushdownCapabilities
from invariantql.domain.execution import ExecutionPlan
from invariantql.domain.expressions import Literal
from invariantql.domain.schema import Schema
from invariantql.ports.source import DataSource
from invariantql.ports.streams import LocalResult


@dataclass(frozen=True, slots=True)
class Reachability:
    reachable: bool
    reason: str = ""


@runtime_checkable
class ExecutionEngine(Protocol):
    @property
    def name(self) -> str: ...

    def capabilities(self) -> EngineCapabilities: ...

    def reachability(self, source: DataSource) -> Reachability:
        """Whether this engine can read the source directly (no staging)."""
        ...

    def scan_capabilities(self, source: DataSource) -> PushdownCapabilities:
        """What the scan this engine will build for the source can push down."""
        ...

    def schema(self, source: DataSource) -> Schema:
        """The source schema as this engine sees it."""
        ...

    def close(self) -> None: ...


@runtime_checkable
class LocalExecutionEngine(ExecutionEngine, Protocol):
    def execute(
        self,
        execution_plan: ExecutionPlan,
        source: DataSource,
        parameters: Mapping[str, Literal],
        *,
        batch_size: int,
    ) -> LocalResult: ...


@runtime_checkable
class CompilingExecutionEngine(ExecutionEngine, Protocol):
    def compile(
        self,
        execution_plan: ExecutionPlan,
        source: DataSource,
        parameters: Mapping[str, Literal],
    ) -> Any:
        """Return a lazy relation without collecting or writing data."""
        ...


__all__ = ["CompilingExecutionEngine", "ExecutionEngine", "LocalExecutionEngine", "Reachability"]
