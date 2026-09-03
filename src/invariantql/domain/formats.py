"""Typed, immutable data-format descriptions (ADR-0004).

A ``DataFormat`` says how file-backed bytes represent tabular data. It carries
no I/O behaviour; engine-facing format handlers interpret it.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, ClassVar

from invariantql.domain.schema import Schema


@dataclass(frozen=True, slots=True)
class DataFormat:
    format_name: ClassVar[str] = "unknown"

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"format": self.format_name}
        for f in fields(self):
            value = getattr(self, f.name)
            if isinstance(value, Schema):
                value = value.to_dict()
            elif isinstance(value, tuple):
                value = list(value)
            out[f.name] = value
        return out


@dataclass(frozen=True, slots=True)
class CsvFormat(DataFormat):
    format_name: ClassVar[str] = "csv"
    delimiter: str = ","
    header: bool = True
    quote: str = '"'
    escape: str | None = None
    encoding: str = "utf-8"
    null_values: tuple[str, ...] = ()
    skip_rows: int = 0
    compression: str | None = None
    date_format: str | None = None
    timestamp_format: str | None = None
    schema: Schema | None = None

    def __post_init__(self) -> None:
        if len(self.delimiter) != 1:
            raise ValueError("csv delimiter must be a single character")
        if len(self.quote) != 1:
            raise ValueError("csv quote must be a single character")
        if self.escape is not None and len(self.escape) != 1:
            raise ValueError("csv escape must be a single character")
        if self.skip_rows < 0:
            raise ValueError("skip_rows must be non-negative")
        if self.compression not in (None, "gzip", "zstd", "bz2", "xz"):
            raise ValueError(f"unsupported compression: {self.compression!r}")
        object.__setattr__(self, "null_values", tuple(self.null_values))


@dataclass(frozen=True, slots=True)
class ParquetFormat(DataFormat):
    format_name: ClassVar[str] = "parquet"
    hive_partitioning: bool = False


@dataclass(frozen=True, slots=True)
class JsonFormat(DataFormat):
    """JSON records: newline-delimited when ``lines`` is true, else a top-level array."""

    format_name: ClassVar[str] = "json"
    lines: bool = True
    compression: str | None = None
    schema: Schema | None = None

    def __post_init__(self) -> None:
        if self.compression not in (None, "gzip", "zstd", "bz2", "xz"):
            raise ValueError(f"unsupported compression: {self.compression!r}")


@dataclass(frozen=True, slots=True)
class XmlFormat(DataFormat):
    """XML documents where every ``row_tag`` element is one record.

    Child elements become columns; attributes become columns prefixed with
    ``attribute_prefix``; text of an element that also has attributes lands in
    ``value_tag``. These defaults match Spark's XML reader.
    """

    format_name: ClassVar[str] = "xml"
    row_tag: str = "row"
    root_tag: str | None = None
    attribute_prefix: str = "_"
    value_tag: str = "_VALUE"
    schema: Schema | None = None

    def __post_init__(self) -> None:
        if not self.row_tag:
            raise ValueError("row_tag must not be empty")


@dataclass(frozen=True, slots=True)
class DeltaFormat(DataFormat):
    """A Delta Lake table directory, optionally pinned to a version or timestamp."""

    format_name: ClassVar[str] = "delta"
    version: int | None = None
    timestamp: str | None = None

    def __post_init__(self) -> None:
        if self.version is not None and self.timestamp is not None:
            raise ValueError("specify either version or timestamp, not both")
        if self.version is not None and self.version < 0:
            raise ValueError("version must be non-negative")


@dataclass(frozen=True, slots=True)
class IcebergFormat(DataFormat):
    """An Apache Iceberg table.

    ``metadata_location`` points at a specific ``metadata/*.metadata.json``
    file. When omitted, the location is a table directory and the newest
    metadata file is used.
    """

    format_name: ClassVar[str] = "iceberg"
    metadata_location: str | None = None
    snapshot_id: int | None = None


FORMATS: dict[str, type[DataFormat]] = {
    cls.format_name: cls
    for cls in (CsvFormat, ParquetFormat, JsonFormat, XmlFormat, DeltaFormat, IcebergFormat)
}


def format_from_dict(data: dict[str, Any]) -> DataFormat:
    name = data.get("format")
    cls = FORMATS.get(str(name))
    if cls is None:
        raise ValueError(f"unknown data format: {name!r}")
    kwargs: dict[str, Any] = {}
    for f in fields(cls):
        if f.name not in data:
            continue
        value = data[f.name]
        if f.name == "schema" and value is not None:
            value = Schema.from_dict(value)
        elif f.name == "null_values":
            value = tuple(value or ())
        kwargs[f.name] = value
    return cls(**kwargs)


__all__ = [
    "FORMATS",
    "CsvFormat",
    "DataFormat",
    "DeltaFormat",
    "IcebergFormat",
    "JsonFormat",
    "ParquetFormat",
    "XmlFormat",
    "format_from_dict",
]
