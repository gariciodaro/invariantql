"""The ``Storage`` port: bytes and objects without tabular meaning (ADR-0004)."""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, BinaryIO, Protocol, runtime_checkable

from invariantql.domain.credentials import SecretOptions
from invariantql.domain.location import Location


@dataclass(frozen=True, slots=True)
class StorageCapabilities:
    """Honest storage semantics; consumed by planners, engines, and staging."""

    range_reads: bool = False
    hierarchical_directories: bool = False
    atomic_rename: bool = False
    listing: bool = True
    engine_visible_uri: bool = False
    evidence: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", tuple(self.evidence))

    def to_dict(self) -> dict[str, Any]:
        return {
            "range_reads": self.range_reads,
            "hierarchical_directories": self.hierarchical_directories,
            "atomic_rename": self.atomic_rename,
            "listing": self.listing,
            "engine_visible_uri": self.engine_visible_uri,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class ObjectInfo:
    location: Location
    size: int | None = None
    modified_at: _dt.datetime | None = None
    is_directory: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "uri": self.location.uri,
            "size": self.size,
            "modified_at": None if self.modified_at is None else self.modified_at.isoformat(),
            "is_directory": self.is_directory,
        }


@runtime_checkable
class Storage(Protocol):
    """Locate and read objects. Implementations hold credentials privately."""

    @property
    def name(self) -> str: ...

    @property
    def capabilities(self) -> StorageCapabilities: ...

    def resolve(self, path: str | Location) -> Location:
        """Turn a user path into an absolute location inside this storage."""
        ...

    def open_read(self, location: Location) -> BinaryIO:
        """Open an object for reading. Seekable when ``range_reads`` is true."""
        ...

    def info(self, location: Location) -> ObjectInfo: ...

    def list(self, location: Location, *, recursive: bool = False) -> Iterator[ObjectInfo]: ...

    def exists(self, location: Location) -> bool: ...

    def native_uri(self, location: Location) -> str | None:
        """The provider-native URI an external engine could read, or ``None``."""
        ...

    def native_options(self) -> SecretOptions:
        """Provider options (secrets included) for libraries that read locations themselves."""
        ...


__all__ = ["ObjectInfo", "Storage", "StorageCapabilities"]
