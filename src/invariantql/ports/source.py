"""The ``DataSource`` port: a logical tabular relation (ADR-0003, ADR-0004).

A source is either *file-backed* (storage plus a data format; the engine's
format handler performs the scan) or *native* (a query service such as a
database; the source compiles pushed operations into its native query).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from invariantql.domain.capabilities import PushdownCapabilities
from invariantql.domain.credentials import EMPTY_SECRETS, SecretOptions
from invariantql.domain.execution import PushedOperations
from invariantql.domain.expressions import Literal
from invariantql.domain.formats import DataFormat
from invariantql.domain.location import Location
from invariantql.domain.schema import Schema
from invariantql.ports.storage import Storage
from invariantql.ports.streams import RecordBatchStream


@dataclass(frozen=True, slots=True)
class FileRelation:
    storage: Storage
    location: Location
    data_format: DataFormat

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "file",
            "storage": self.storage.name,
            "location": self.location.uri,
            "format": self.data_format.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class NativeRelation:
    """Describes how an external engine may reach a native source.

    ``kind`` is a stable identifier such as ``jdbc:postgresql``, ``jdbc:mysql``,
    ``mongodb`` or ``neo4j``. ``options`` are non-secret; ``secrets`` are
    revealed only by engine adapters at the edge.
    """

    kind: str
    options: Mapping[str, str]
    secrets: SecretOptions = EMPTY_SECRETS

    def __post_init__(self) -> None:
        object.__setattr__(self, "options", dict(self.options))

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "options": dict(self.options), "secrets": list(self.secrets)}


@runtime_checkable
class DataSource(Protocol):
    @property
    def name(self) -> str: ...

    def schema(self) -> Schema:
        """Discover the schema. File sources may raise ``SOURCE_SCHEMA_UNAVAILABLE``."""
        ...

    def capabilities(self) -> PushdownCapabilities:
        """What the source's own native scan can push. File sources report none."""
        ...

    def relation(self) -> FileRelation | NativeRelation: ...

    def scan(
        self,
        pushed: PushedOperations,
        parameters: Mapping[str, Literal],
        *,
        batch_size: int,
    ) -> RecordBatchStream:
        """Execute the pushed operations natively (native sources only)."""
        ...

    def close(self) -> None: ...


__all__ = ["DataSource", "FileRelation", "NativeRelation"]
