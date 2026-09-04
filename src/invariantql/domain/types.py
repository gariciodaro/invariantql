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
PORTABLE_DECIMAL_PRECISION = 38


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


def unify(
    left: DataType,
    right: DataType,
    *,
    operation: str | None = None,
) -> DataType:
    """Return a symmetric common type, optionally for an arithmetic operation.

    ``operation`` is one of ``+``, ``-``, ``*`` or ``/``.  Supplying it makes
    decimal precision/scale derivation operation-aware; omitting it computes a
    type capable of representing values from either input (used by schema
    inference). Arithmetic must fit the Local+Spark 38-digit profile without
    lossy scale reduction; descriptive unification may retain Arrow
    decimal256 precision up to 76.
    """

    if operation not in (None, "+", "-", "*", "/"):
        raise ValueError(f"unsupported numeric operation: {operation!r}")
    if operation == "/":
        return FloatType(64)
    if operation is not None and (isinstance(left, UnknownType) or isinstance(right, UnknownType)):
        # A concrete execution can refine a parameter after substitution, but
        # inspection without parameter values must not promise the other
        # operand's type.
        return UnknownType()

    # Null and unknown are identities when one concrete type is available.
    # For the otherwise ambiguous null/unknown pair, unknown is the honest
    # result regardless of operand order.
    if left.kind in ("null", "unknown") and right.kind in ("null", "unknown"):
        if left.kind == "unknown" or right.kind == "unknown":
            return UnknownType()
        return NullType()
    if left.kind in ("null", "unknown"):
        return right
    if right.kind in ("null", "unknown"):
        return left

    if not (is_numeric(left) and is_numeric(right)):
        return left if left == right else UnknownType()

    if isinstance(left, FloatType) or isinstance(right, FloatType):
        # Mixing a 32-bit float with any exact numeric can require more than
        # 24 bits of mantissa.  Only two float32 inputs safely remain float32.
        if isinstance(left, FloatType) and isinstance(right, FloatType):
            return FloatType(max(left.bits, right.bits))
        return FloatType(64)

    if isinstance(left, IntegerType) and isinstance(right, IntegerType):
        if operation is not None:
            # All integer arithmetic uses the shared signed-64 execution type.
            # Engines apply checked operations and return NULL on int64
            # overflow, avoiding DuckDB errors versus Spark wraparound.
            return IntegerType(64)
        return IntegerType(max(left.bits, right.bits))

    left_decimal = _as_decimal(left)
    right_decimal = _as_decimal(right)
    left_integral = left_decimal.precision - left_decimal.scale
    right_integral = right_decimal.precision - right_decimal.scale

    if operation in ("+", "-"):
        scale = max(left_decimal.scale, right_decimal.scale)
        integral = max(left_integral, right_integral) + 1
    elif operation == "*":
        scale = left_decimal.scale + right_decimal.scale
        integral = left_integral + right_integral + 1
    else:
        scale = max(left_decimal.scale, right_decimal.scale)
        integral = max(left_integral, right_integral)
    # Arrow can describe decimal256 values up to 76 digits, which is useful
    # for faithfully reporting source schemas.  The executable Local+Spark
    # profile is deliberately narrower: both DuckDB and Spark cap arithmetic
    # decimals at 38 digits. Reject operations that would need fractional
    # truncation because DuckDB cannot reproduce Spark's intermediate decimal
    # arithmetic exactly in that range.
    maximum = PORTABLE_DECIMAL_PRECISION if operation is not None else 76
    if integral > maximum:
        raise ValueError(
            f"decimal result needs {integral} integral digits; the portable maximum is {maximum}"
        )
    if operation is not None and integral + scale > maximum:
        raise ValueError(
            f"decimal result needs precision {integral + scale}; "
            f"the portable maximum is {maximum} without scale reduction"
        )
    return _bounded_decimal(integral, scale, maximum=maximum)


def _as_decimal(data_type: DataType) -> DecimalType:
    if isinstance(data_type, DecimalType):
        return data_type
    if isinstance(data_type, IntegerType):
        digits = {8: 3, 16: 5, 32: 10, 64: 19}[data_type.bits]
        return DecimalType(digits, 0)
    raise TypeError(f"cannot convert {data_type} to an exact decimal type")


def _bounded_decimal(integral: int, scale: int, *, maximum: int) -> DecimalType:
    integral = max(integral, 0)
    scale = max(scale, 0)
    if integral > maximum:
        raise ValueError(
            f"decimal result needs {integral} integral digits; the portable maximum is {maximum}"
        )
    if integral + scale <= maximum:
        return DecimalType(max(integral + scale, 1), scale)
    retained_scale = max(0, maximum - integral)
    return DecimalType(maximum, retained_scale)


def is_portable_type(data_type: DataType) -> bool:
    """Whether both first-release engines can represent ``data_type``.

    Decimal256 remains a valid descriptive source type, but DuckDB and Spark
    share a 38-digit executable decimal ceiling.  Nested types inherit the
    restriction recursively.
    """

    if isinstance(data_type, DecimalType):
        return data_type.precision <= PORTABLE_DECIMAL_PRECISION
    if isinstance(data_type, ListType):
        return is_portable_type(data_type.element)
    if isinstance(data_type, StructType):
        return all(is_portable_type(field_type) for _, field_type in data_type.fields)
    return True


def normalise_portable_type(data_type: DataType) -> DataType:
    """Return the engine-independent result-boundary representation.

    Spark timestamps with a time zone represent UTC instants without retaining
    a per-column zone identifier.  Normalize every aware timestamp to UTC so
    Arrow/DuckDB and Spark expose the same declared result type.  Apply the
    rule recursively inside lists and structs.
    """

    if isinstance(data_type, TimestampType) and data_type.timezone is not None:
        return TimestampType("UTC")
    if isinstance(data_type, ListType):
        return ListType(normalise_portable_type(data_type.element))
    if isinstance(data_type, StructType):
        return StructType(
            tuple(
                (name, normalise_portable_type(field_type)) for name, field_type in data_type.fields
            )
        )
    return data_type


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
    "PORTABLE_DECIMAL_PRECISION",
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
    "is_portable_type",
    "normalise_portable_type",
    "type_from_dict",
    "unify",
]
