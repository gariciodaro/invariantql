"""The local result boundary: Arrow batches with explicit, bounded materialisation."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import Any

import pyarrow as pa

from invariantql.domain.diagnostics import (
    DiagnosticCode,
    MaterializationLimitError,
    MissingDependencyError,
)

DEFAULT_MATERIALIZE_ROWS = 1_000_000


class LocalResult:
    """Streams Arrow record batches; conversions are explicit and bounded."""

    def __init__(
        self,
        reader: pa.RecordBatchReader,
        *,
        on_close: Iterable[Callable[[], None]] = (),
        engine: str = "duckdb",
    ) -> None:
        self._reader = reader
        self._schema = reader.schema
        self._on_close = list(on_close)
        self._closed = False
        self._consumed = False
        self.engine = engine

    @property
    def schema(self) -> pa.Schema:
        return self._schema

    @property
    def closed(self) -> bool:
        return self._closed

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        return self.batches()

    def batches(self) -> Iterator[pa.RecordBatch]:
        self._ensure_open()
        self._consumed = True
        try:
            yield from self._reader
        finally:
            self.close()

    def to_arrow(self, *, max_rows: int | None = DEFAULT_MATERIALIZE_ROWS) -> pa.Table:
        """Materialise into a table; raises when more than ``max_rows`` rows arrive."""

        self._ensure_open()
        batches: list[pa.RecordBatch] = []
        total = 0
        try:
            for batch in self._reader:
                total += batch.num_rows
                if max_rows is not None and total > max_rows:
                    raise MaterializationLimitError(
                        f"result exceeds the materialisation limit of {max_rows} rows; "
                        "raise max_rows explicitly or add a LIMIT",
                        code=DiagnosticCode.RESULT_LIMIT_EXCEEDED,
                        details={"max_rows": max_rows},
                    )
                batches.append(batch)
        finally:
            self.close()
        return pa.Table.from_batches(batches, self._schema)

    def to_pandas(self, *, max_rows: int | None = DEFAULT_MATERIALIZE_ROWS, **kwargs: Any) -> Any:
        try:
            import pandas  # noqa: F401
        except ImportError as exc:
            raise MissingDependencyError(
                "pandas is required for to_pandas(); install invariantql[pandas]",
                details={"extra": "pandas"},
            ) from exc
        return self.to_arrow(max_rows=max_rows).to_pandas(**kwargs)

    def to_polars(self, *, max_rows: int | None = DEFAULT_MATERIALIZE_ROWS) -> Any:
        try:
            import polars
        except ImportError as exc:
            raise MissingDependencyError(
                "polars is required for to_polars(); install invariantql[polars]",
                details={"extra": "polars"},
            ) from exc
        return polars.from_arrow(self.to_arrow(max_rows=max_rows))

    def rows(self, *, max_rows: int | None = DEFAULT_MATERIALIZE_ROWS) -> list[dict[str, Any]]:
        return self.to_arrow(max_rows=max_rows).to_pylist()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        errors: list[BaseException] = []
        try:
            self._reader.close()
        except Exception as exc:
            errors.append(exc)
        for hook in self._on_close:
            try:
                hook()
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise errors[0]

    def _ensure_open(self) -> None:
        if self._closed:
            raise MaterializationLimitError(
                "result is closed; results can be consumed once",
                code=DiagnosticCode.RESULT_CLOSED,
            )

    def __enter__(self) -> LocalResult:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"LocalResult(engine={self.engine!r}, columns={self._schema.names}, {state})"


__all__ = ["DEFAULT_MATERIALIZE_ROWS", "LocalResult"]
