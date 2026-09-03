"""The local result boundary (ADR-0005).

A ``RecordBatchStream`` is the structural shape of an Arrow record-batch
reader: a schema, iteration over batches, and explicit closing. The port
names no Arrow type; ``pyarrow.RecordBatchReader`` satisfies it at runtime.
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


__all__ = ["RecordBatchStream"]
