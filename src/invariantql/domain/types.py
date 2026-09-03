"""Logical data types owned by the domain (ADR-0002).

Types are immutable value objects. They describe the meaning of a column
independently of any engine's physical representation; engine adapters map
them to Arrow, DuckDB, Spark, or driver types at the boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar


@dataclass(frozen=True, slots=True)
class DataType:
    """Base class for every logical type."""

    kind: ClassVar[str] = "unknown"

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind}

    def __str__(self) -> str:
        return self.kind


@dataclass(frozen=True, slots=True)
class UnknownType(DataType):
    """A type that could not be determined (for example from a schemaless source)."""

    kind: ClassVar[str] = "unknown"


@dataclass(frozen=True, slots=True)
class NullType(DataType):
    """The type of the ``NULL`` literal before it is unified with another type."""

    kind: ClassVar[str] = "null"


@dataclass(frozen=True, slots=True)
class BooleanType(DataType):
    kind: ClassVar[str] = "boolean"


@dataclass(frozen=True, slots=True)
class IntegerType(DataType):
    """A signed integer with the given width in bits (8, 16, 32 or 64)."""

    kind: ClassVar[str] = "integer"
    bits: int = 64

    def __post_init__(self) -> None:
        if self.bits not in (8, 16, 32, 64):
            raise ValueError(f"unsupported integer width: {self.bits}")

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "bits": self.bits}

    def __str__(self) -> str:
        return f"int{self.bits}"


@dataclass(frozen=True, slots=True)
class FloatType(DataType):
    """An IEEE floating point number with the given width in bits (32 or 64)."""

    kind: ClassVar[str] = "float"
    bits: int = 64

    def __post_init__(self) -> None:
        if self.bits not in (32, 64):
            raise ValueError(f"unsupported float width: {self.bits}")

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "bits": self.bits}

    def __str__(self) -> str:
        return f"float{self.bits}"


@dataclass(frozen=True, slots=True)
class DecimalType(DataType):
    kind: ClassVar[str] = "decimal"
    precision: int = 38
    scale: int = 9

    def __post_init__(self) -> None:
        if not 1 <= self.precision <= 76:
            raise ValueError(f"decimal precision out of range: {self.precision}")
        if not 0 <= self.scale <= self.precision:
            raise ValueError(f"decimal scale out of range: {self.scale}")

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "precision": self.precision, "scale": self.scale}

    def __str__(self) -> str:
        return f"decimal({self.precision},{self.scale})"


@dataclass(frozen=True, slots=True)
class StringType(DataType):
    kind: ClassVar[str] = "string"


@dataclass(frozen=True, slots=True)
class BinaryType(DataType):
    kind: ClassVar[str] = "binary"


@dataclass(frozen=True, slots=True)
class DateType(DataType):
    kind: ClassVar[str] = "date"


@dataclass(frozen=True, slots=True)
class TimestampType(DataType):
    """A timestamp with microsecond precision, optionally anchored to a time zone."""

    kind: ClassVar[str] = "timestamp"
    timezone: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "timezone": self.timezone}

    def __str__(self) -> str:
        return "timestamp" if self.timezone is None else f"timestamp[{self.timezone}]"


@dataclass(frozen=True, slots=True)
class ListType(DataType):
    kind: ClassVar[str] = "list"
    element: DataType = UnknownType()

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "element": self.element.to_dict()}

    def __str__(self) -> str:
        return f"list<{self.element}>"


@dataclass(frozen=True, slots=True)
class StructType(DataType):
    kind: ClassVar[str] = "struct"
    fields: tuple[tuple[str, DataType], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", tuple((str(n), t) for n, t in self.fields))

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "fields": [{"name": n, "type": t.to_dict()} for n, t in self.fields],
        }

    def __str__(self) -> str:
        inner = ", ".join(f"{n}: {t}" for n, t in self.fields)
        return f"struct<{inner}>"


NUMERIC_KINDS = frozenset({"integer", "float", "decimal"})


def is_numeric(data_type: DataType) -> bool:
    return data_type.kind in NUMERIC_KINDS


def is_comparable(left: DataType, right: DataType) -> bool:
    """Whether two types may be compared in the portable profile.

    ``null`` and ``unknown`` compare with anything; numerics compare among
    themselves; every other kind compares only with itself.
    """

    if left.kind in ("null", "unknown") or right.kind in ("null", "unknown"):
        return True
    if is_numeric(left) and is_numeric(right):
        return True
    return left.kind == right.kind


def unify(left: DataType, right: DataType) -> DataType:
    """Pick the wider of two comparable types for a result column."""

    if left.kind in ("null", "unknown"):
        return right
    if right.kind in ("null", "unknown"):
        return left
    if is_numeric(left) and is_numeric(right):
        order = {"integer": 0, "decimal": 1, "float": 2}
        return left if order[left.kind] >= order[right.kind] else right
    return left


def type_from_dict(data: dict[str, Any]) -> DataType:
    kind = data.get("kind", "unknown")
    if kind == "integer":
        return IntegerType(int(data.get("bits", 64)))
    if kind == "float":
        return FloatType(int(data.get("bits", 64)))
    if kind == "decimal":
        return DecimalType(int(data.get("precision", 38)), int(data.get("scale", 9)))
    if kind == "timestamp":
        return TimestampType(data.get("timezone"))
    if kind == "list":
        return ListType(type_from_dict(data.get("element", {})))
    if kind == "struct":
        return StructType(
            tuple((f["name"], type_from_dict(f["type"])) for f in data.get("fields", []))
        )
    simple: dict[str, DataType] = {
        "unknown": UnknownType(),
        "null": NullType(),
        "boolean": BooleanType(),
        "string": StringType(),
        "binary": BinaryType(),
        "date": DateType(),
    }
    if kind in simple:
        return simple[kind]
    raise ValueError(f"unknown data type kind: {kind!r}")


__all__ = [
    "NUMERIC_KINDS",
    "BinaryType",
    "BooleanType",
    "DataType",
    "DateType",
    "DecimalType",
    "FloatType",
    "IntegerType",
    "ListType",
    "NullType",
    "StringType",
    "StructType",
    "TimestampType",
    "UnknownType",
    "is_comparable",
    "is_numeric",
    "type_from_dict",
    "unify",
]
