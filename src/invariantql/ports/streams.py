"""The local stream and materialization boundaries (ADR-0005).

``RecordBatchStream`` is the minimal structural shape shared by sources and
format handlers. ``LocalResult`` is the richer result returned to callers by
a local engine. Neither protocol names an Arrow, pandas, or Polars type.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class RecordBatchStream(Protocol):
    @property
    def schema(self) -> Any: ...

    def __iter__(self) -> Iterator[Any]: ...

    def close(self) -> None: ...


@runtime_checkable
class LocalResult(RecordBatchStream, Protocol):
    """A local, Arrow-native result with explicit bounded materializers.

    Return types stay provider-neutral at the port boundary: concrete engines
    may expose richer Arrow, pandas, or Polars types while callers can rely on
    these methods without importing an adapter implementation.
    """

    @property
    def closed(self) -> bool: ...

    def batches(self) -> Iterator[Any]: ...

    def to_arrow(self, *, max_rows: int | None = 1_000_000) -> Any: ...

    def to_pandas(self, *, max_rows: int | None = 1_000_000, **kwargs: Any) -> Any: ...

    def to_polars(self, *, max_rows: int | None = 1_000_000) -> Any: ...

    def rows(self, *, max_rows: int | None = 1_000_000) -> list[dict[str, Any]]: ...

    def __enter__(self) -> LocalResult: ...

    def __exit__(self, *exc: object) -> None: ...


__all__ = ["LocalResult", "RecordBatchStream"]
