"""A file-backed source: storage plus a data format (ADR-0004)."""

from __future__ import annotations

from collections.abc import Mapping

from invariantql.domain.capabilities import PushdownCapabilities
from invariantql.domain.diagnostics import DiagnosticCode, SourceError, UnsupportedOperationError
from invariantql.domain.execution import PushedOperations
from invariantql.domain.expressions import Literal
from invariantql.domain.formats import DataFormat
from invariantql.domain.location import Location
from invariantql.domain.schema import Schema
from invariantql.ports.source import FileRelation
from invariantql.ports.storage import Storage
from invariantql.ports.streams import RecordBatchStream


class FileSource:
    def __init__(
        self, name: str, storage: Storage, path: str | Location, data_format: DataFormat
    ) -> None:
        if not name:
            raise ValueError("source name must not be empty")
        self._name = name
        self._storage = storage
        self._location = storage.resolve(path)
        self._format = data_format

    @property
    def name(self) -> str:
        return self._name

    @property
    def storage(self) -> Storage:
        return self._storage

    @property
    def location(self) -> Location:
        return self._location

    @property
    def data_format(self) -> DataFormat:
        return self._format

    def schema(self) -> Schema:
        declared = getattr(self._format, "schema", None)
        if isinstance(declared, Schema):
            return declared
        raise SourceError(
            f"file source {self._name!r} has no declared schema; the execution engine derives it",
            code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
            target=self._name,
        )

    def capabilities(self) -> PushdownCapabilities:
        return PushdownCapabilities.none(
            "file source: the engine's format handler performs the scan"
        )

    def relation(self) -> FileRelation:
        return FileRelation(self._storage, self._location, self._format)

    def scan(
        self,
        pushed: PushedOperations,
        parameters: Mapping[str, Literal],
        *,
        batch_size: int,
    ) -> RecordBatchStream:
        raise UnsupportedOperationError(
            f"file source {self._name!r} is scanned by the engine's format handler, not by itself",
            code=DiagnosticCode.SOURCE_SCAN_UNSUPPORTED,
            target=self._name,
        )

    def close(self) -> None:
        return None

    def __repr__(self) -> str:
        return f"FileSource(name={self._name!r}, location={self._location.uri!r}, format={self._format.format_name!r})"


__all__ = ["FileSource"]
