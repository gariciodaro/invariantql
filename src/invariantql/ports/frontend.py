"""Query frontends compile text (or a builder) into the domain plan (ADR-0006)."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from invariantql.domain.plan import QueryPlan


@runtime_checkable
class QueryFrontend(Protocol):
    @property
    def name(self) -> str: ...

    def parse(self, text: str) -> QueryPlan: ...


__all__ = ["QueryFrontend"]
