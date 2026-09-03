"""Format-handler ports (ADR-0004).

``LocalFormatHandler`` turns a data format into an Arrow-compatible scan for
the local engine. ``ReaderSpec`` describes a distributed engine's native reader
in domain terms so that adding a format never requires touching an engine
adapter (EXT-02).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from invariantql.domain.capabilities import PushdownCapabilities
from invariantql.domain.execution import PushedOperations
from invariantql.domain.expressions import Literal
from invariantql.domain.formats import DataFormat
from invariantql.domain.location import Location
from invariantql.domain.schema import Schema
from invariantql.ports.storage import Storage
from invariantql.ports.streams import RecordBatchStream


@runtime_checkable
class LocalFormatHandler(Protocol):
    @property
    def format_name(self) -> str: ...

    def capabilities(self, data_format: DataFormat) -> PushdownCapabilities: ...

    def schema(self, storage: Storage, location: Location, data_format: DataFormat) -> Schema: ...

    def scan(
        self,
        storage: Storage,
        location: Location,
        data_format: DataFormat,
        pushed: PushedOperations,
        parameters: Mapping[str, Literal],
        *,
        batch_size: int,
    ) -> RecordBatchStream: ...


@dataclass(frozen=True, slots=True)
class ReaderSpec:
    """A distributed engine's native reader configuration in domain terms."""

    format: str
    options: Mapping[str, str]
    schema: Schema | None = None
    requires: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "options", dict(self.options))
        object.__setattr__(self, "requires", tuple(self.requires))

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "options": dict(self.options),
            "schema": None if self.schema is None else self.schema.to_dict(),
            "requires": list(self.requires),
        }


@runtime_checkable
class DistributedFormatHandler(Protocol):
    @property
    def format_name(self) -> str: ...

    def reader_spec(self, data_format: DataFormat, uri: str) -> ReaderSpec: ...


__all__ = ["DistributedFormatHandler", "LocalFormatHandler", "ReaderSpec"]
