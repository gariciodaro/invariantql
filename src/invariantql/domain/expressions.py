"""Portable expression nodes (ADR-0002, ADR-0006).

The expression language is deliberately small: column references, typed
literals, named parameters, comparisons, boolean composition, ``IS NULL``,
``IN``, ``LIKE``, and the four arithmetic operators. Anything else is rejected
by the frontend rather than silently approximated.
"""

from __future__ import annotations

import datetime as _dt
import math
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, ClassVar

from invariantql.domain.types import (
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    FloatType,
    IntegerType,
    NullType,
    StringType,
    TimestampType,
    type_from_dict,
)

LiteralValue = bool | int | float | str | bytes | Decimal | _dt.date | _dt.datetime | None


class ExpressionKind(str, Enum):
    """Stable identifiers for expression node kinds, used by capabilities."""

    COLUMN = "column"
    LITERAL = "literal"
    PARAMETER = "parameter"
    COMPARISON = "comparison"
    AND = "and"
    OR = "or"
    NOT = "not"
    IS_NULL = "is_null"
    IN = "in"
    LIKE = "like"
    ARITHMETIC = "arithmetic"
    ALIAS = "alias"


class ComparisonOp(str, Enum):
    EQ = "="
    NE = "<>"
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="


class ArithmeticOp(str, Enum):
    ADD = "+"
    SUB = "-"
    MUL = "*"
    DIV = "/"


@dataclass(frozen=True, slots=True)
class Expression:
    kind: ClassVar[ExpressionKind]

    def children(self) -> tuple[Expression, ...]:
        return ()

    def to_dict(self) -> dict[str, Any]:  # pragma: no cover - overridden
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class Column(Expression):
    kind: ClassVar[ExpressionKind] = ExpressionKind.COLUMN
    name: str

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("column name must not be empty")

    def to_dict(self) -> dict[str, Any]:
        return {"node": "column", "name": self.name}

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True, slots=True)
class Literal(Expression):
    kind: ClassVar[ExpressionKind] = ExpressionKind.LITERAL
    value: LiteralValue
    data_type: DataType

    def __post_init__(self) -> None:
        _validate_literal_value(self.value)
        _validate_literal_data_type(self.value, self.data_type)

    @classmethod
    def of(cls, value: LiteralValue) -> Literal:
        return cls(value, infer_literal_type(value))

    def to_dict(self) -> dict[str, Any]:
        return {
            "node": "literal",
            "value": _encode_literal(self.value),
            "type": self.data_type.to_dict(),
        }

    def __str__(self) -> str:
        if self.value is None:
            return "NULL"
        if isinstance(self.value, str):
            return "'" + self.value.replace("'", "''") + "'"
        if isinstance(self.value, bool):
            return "TRUE" if self.value else "FALSE"
        return str(self.value)


@dataclass(frozen=True, slots=True)
class Parameter(Expression):
    kind: ClassVar[ExpressionKind] = ExpressionKind.PARAMETER
    name: str

    def __post_init__(self) -> None:
        if not self.name.isidentifier():
            raise ValueError(f"parameter name must be an identifier: {self.name!r}")

    def to_dict(self) -> dict[str, Any]:
        return {"node": "parameter", "name": self.name}

    def __str__(self) -> str:
        return f":{self.name}"


@dataclass(frozen=True, slots=True)
class Comparison(Expression):
    kind: ClassVar[ExpressionKind] = ExpressionKind.COMPARISON
    op: ComparisonOp
    left: Expression
    right: Expression

    def children(self) -> tuple[Expression, ...]:
        return (self.left, self.right)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node": "comparison",
            "op": self.op.value,
            "left": self.left.to_dict(),
            "right": self.right.to_dict(),
        }

    def __str__(self) -> str:
        return f"({self.left} {self.op.value} {self.right})"


@dataclass(frozen=True, slots=True)
class And(Expression):
    kind: ClassVar[ExpressionKind] = ExpressionKind.AND
    operands: tuple[Expression, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "operands", tuple(self.operands))
        if len(self.operands) < 2:
            raise ValueError("AND needs at least two operands")

    def children(self) -> tuple[Expression, ...]:
        return self.operands

    def to_dict(self) -> dict[str, Any]:
        return {"node": "and", "operands": [o.to_dict() for o in self.operands]}

    def __str__(self) -> str:
        return "(" + " AND ".join(str(o) for o in self.operands) + ")"


@dataclass(frozen=True, slots=True)
class Or(Expression):
    kind: ClassVar[ExpressionKind] = ExpressionKind.OR
    operands: tuple[Expression, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "operands", tuple(self.operands))
        if len(self.operands) < 2:
            raise ValueError("OR needs at least two operands")

    def children(self) -> tuple[Expression, ...]:
        return self.operands

    def to_dict(self) -> dict[str, Any]:
        return {"node": "or", "operands": [o.to_dict() for o in self.operands]}

    def __str__(self) -> str:
        return "(" + " OR ".join(str(o) for o in self.operands) + ")"


@dataclass(frozen=True, slots=True)
class Not(Expression):
    kind: ClassVar[ExpressionKind] = ExpressionKind.NOT
    operand: Expression

    def children(self) -> tuple[Expression, ...]:
        return (self.operand,)

    def to_dict(self) -> dict[str, Any]:
        return {"node": "not", "operand": self.operand.to_dict()}

    def __str__(self) -> str:
        return f"(NOT {self.operand})"


@dataclass(frozen=True, slots=True)
class IsNull(Expression):
    kind: ClassVar[ExpressionKind] = ExpressionKind.IS_NULL
    operand: Expression
    negated: bool = False

    def children(self) -> tuple[Expression, ...]:
        return (self.operand,)

    def to_dict(self) -> dict[str, Any]:
        return {"node": "is_null", "operand": self.operand.to_dict(), "negated": self.negated}

    def __str__(self) -> str:
        return f"({self.operand} IS {'NOT ' if self.negated else ''}NULL)"


@dataclass(frozen=True, slots=True)
class In(Expression):
    """``operand IN (values)``; values are literals or parameters only."""

    kind: ClassVar[ExpressionKind] = ExpressionKind.IN
    operand: Expression
    values: tuple[Expression, ...]
    negated: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "values", tuple(self.values))
        if not self.values:
            raise ValueError("IN needs at least one value")
        for v in self.values:
            if not isinstance(v, (Literal, Parameter)):
                raise ValueError("IN values must be literals or parameters")

    def children(self) -> tuple[Expression, ...]:
        return (self.operand, *self.values)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node": "in",
            "operand": self.operand.to_dict(),
            "values": [v.to_dict() for v in self.values],
            "negated": self.negated,
        }

    def __str__(self) -> str:
        vals = ", ".join(str(v) for v in self.values)
        return f"({self.operand} {'NOT ' if self.negated else ''}IN ({vals}))"


@dataclass(frozen=True, slots=True)
class Like(Expression):
    """SQL ``LIKE`` with ``%`` and ``_`` wildcards, case-sensitive, no escape clause."""

    kind: ClassVar[ExpressionKind] = ExpressionKind.LIKE
    operand: Expression
    pattern: Expression
    negated: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.pattern, (Literal, Parameter)):
            raise ValueError("LIKE pattern must be a literal or parameter")

    def children(self) -> tuple[Expression, ...]:
        return (self.operand, self.pattern)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node": "like",
            "operand": self.operand.to_dict(),
            "pattern": self.pattern.to_dict(),
            "negated": self.negated,
        }

    def __str__(self) -> str:
        return f"({self.operand} {'NOT ' if self.negated else ''}LIKE {self.pattern})"


@dataclass(frozen=True, slots=True)
class Arithmetic(Expression):
    kind: ClassVar[ExpressionKind] = ExpressionKind.ARITHMETIC
    op: ArithmeticOp
    left: Expression
    right: Expression

    def children(self) -> tuple[Expression, ...]:
        return (self.left, self.right)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node": "arithmetic",
            "op": self.op.value,
            "left": self.left.to_dict(),
            "right": self.right.to_dict(),
        }

    def __str__(self) -> str:
        return f"({self.left} {self.op.value} {self.right})"


@dataclass(frozen=True, slots=True)
class Alias(Expression):
    """Names a projected expression. Only valid at the top of a projection."""

    kind: ClassVar[ExpressionKind] = ExpressionKind.ALIAS
    expression: Expression
    name: str

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("alias name must not be empty")
        if isinstance(self.expression, Alias):
            raise ValueError("alias of an alias is not allowed")

    def children(self) -> tuple[Expression, ...]:
        return (self.expression,)

    def to_dict(self) -> dict[str, Any]:
        return {"node": "alias", "expression": self.expression.to_dict(), "name": self.name}

    def __str__(self) -> str:
        return f"{self.expression} AS {self.name}"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def infer_literal_type(value: LiteralValue) -> DataType:
    _validate_literal_value(value)
    if value is None:
        return NullType()
    if isinstance(value, bool):
        return BooleanType()
    if isinstance(value, int):
        return IntegerType(64)
    if isinstance(value, float):
        return FloatType(64)
    if isinstance(value, Decimal):
        precision, scale = _decimal_shape(value)
        return DecimalType(precision, scale)
    if isinstance(value, str):
        return StringType()
    if isinstance(value, bytes):
        return BinaryType()
    if isinstance(value, _dt.datetime):
        tz = value.tzinfo.tzname(value) if value.tzinfo is not None else None
        return TimestampType(tz)
    if isinstance(value, _dt.date):
        return DateType()
    raise TypeError(f"unsupported literal value: {type(value).__name__}")


def _validate_literal_value(value: Any) -> None:
    if value is None or isinstance(value, (bool, str, bytes, _dt.date)):
        return
    if isinstance(value, int):
        if not -(2**63) <= value <= 2**63 - 1:
            raise ValueError("integer literal is outside the signed 64-bit portable range")
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("floating-point literals must be finite")
        return
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("decimal literals must be finite")
        _decimal_shape(value)
        return
    raise TypeError(f"unsupported literal value: {type(value).__name__}")


def _validate_literal_data_type(value: LiteralValue, data_type: DataType) -> None:
    """Reject falsely typed literals at construction and deserialisation boundaries."""

    if not isinstance(data_type, DataType):
        raise TypeError("literal data_type must be a DataType")
    if value is None:
        return  # contextual typed NULLs are valid
    if isinstance(value, bool):
        valid = isinstance(data_type, BooleanType)
    elif isinstance(value, int):
        valid = isinstance(data_type, IntegerType) and (
            -(2 ** (data_type.bits - 1)) <= value <= 2 ** (data_type.bits - 1) - 1
        )
    elif isinstance(value, float):
        valid = isinstance(data_type, FloatType) and (
            data_type.bits == 64 or abs(value) <= 3.4028234663852886e38
        )
    elif isinstance(value, Decimal):
        if isinstance(data_type, DecimalType):
            precision, scale = _decimal_shape(value)
            integral = 0 if value.is_zero() else precision - scale
            valid = scale <= data_type.scale and integral <= data_type.precision - data_type.scale
        else:
            valid = False
    elif isinstance(value, str):
        valid = isinstance(data_type, StringType)
    elif isinstance(value, bytes):
        valid = isinstance(data_type, BinaryType)
    elif isinstance(value, _dt.datetime):
        aware = value.tzinfo is not None and value.utcoffset() is not None
        valid = isinstance(data_type, TimestampType) and aware == (data_type.timezone is not None)
    elif isinstance(value, _dt.date):
        valid = isinstance(data_type, DateType)
    else:  # guarded by _validate_literal_value
        valid = False
    if not valid:
        raise ValueError(f"literal value {value!r} is not representable as {data_type}")


def _decimal_shape(value: Decimal) -> tuple[int, int]:
    _sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):  # guarded by is_finite(), retained for typing
        raise ValueError("decimal literals must be finite")
    scale = max(-exponent, 0)
    precision = max(len(digits) + max(exponent, 0), scale, 1)
    if precision > 76 or scale > 76:
        raise ValueError("decimal literal exceeds the portable precision of 76 digits")
    return precision, scale


def _encode_literal(value: LiteralValue) -> Any:
    if isinstance(value, Decimal):
        return {"decimal": str(value)}
    if isinstance(value, _dt.datetime):
        return {"datetime": value.isoformat()}
    if isinstance(value, _dt.date):
        return {"date": value.isoformat()}
    if isinstance(value, bytes):
        return {"bytes_hex": value.hex()}
    return value


def _decode_literal(value: Any) -> LiteralValue:
    if isinstance(value, dict):
        if "decimal" in value:
            return Decimal(value["decimal"])
        if "datetime" in value:
            return _dt.datetime.fromisoformat(value["datetime"])
        if "date" in value:
            return _dt.date.fromisoformat(value["date"])
        if "bytes_hex" in value:
            return bytes.fromhex(value["bytes_hex"])
        raise ValueError(f"cannot decode literal: {value!r}")
    return value


def walk(expression: Expression) -> Iterator[Expression]:
    """Pre-order traversal of an expression tree."""

    yield expression
    for child in expression.children():
        yield from walk(child)


def referenced_columns(*expressions: Expression) -> tuple[str, ...]:
    """Column names referenced by the expressions, in first-seen order."""

    seen: dict[str, None] = {}
    for expression in expressions:
        for node in walk(expression):
            if isinstance(node, Column):
                seen.setdefault(node.name, None)
    return tuple(seen)


def referenced_parameters(*expressions: Expression) -> tuple[str, ...]:
    seen: dict[str, None] = {}
    for expression in expressions:
        for node in walk(expression):
            if isinstance(node, Parameter):
                seen.setdefault(node.name, None)
    return tuple(seen)


def conjuncts(expression: Expression | None) -> tuple[Expression, ...]:
    """Flatten nested ``AND`` into a tuple of conjuncts."""

    if expression is None:
        return ()
    if isinstance(expression, And):
        out: list[Expression] = []
        for operand in expression.operands:
            out.extend(conjuncts(operand))
        return tuple(out)
    return (expression,)


def and_all(expressions: Iterable[Expression]) -> Expression | None:
    items = tuple(expressions)
    if not items:
        return None
    if len(items) == 1:
        return items[0]
    return And(items)


def output_name(expression: Expression) -> str:
    if isinstance(expression, Alias):
        return expression.name
    if isinstance(expression, Column):
        return expression.name
    raise ValueError("projected expressions must be columns or aliases")


def unalias(expression: Expression) -> Expression:
    return expression.expression if isinstance(expression, Alias) else expression


def substitute_parameters(expression: Expression, values: dict[str, Literal]) -> Expression:
    """Return a copy with parameters replaced by literals."""

    if isinstance(expression, Parameter):
        try:
            return values[expression.name]
        except KeyError as exc:
            raise KeyError(expression.name) from exc
    if isinstance(expression, Comparison):
        return Comparison(
            expression.op,
            substitute_parameters(expression.left, values),
            substitute_parameters(expression.right, values),
        )
    if isinstance(expression, And):
        return And(tuple(substitute_parameters(o, values) for o in expression.operands))
    if isinstance(expression, Or):
        return Or(tuple(substitute_parameters(o, values) for o in expression.operands))
    if isinstance(expression, Not):
        return Not(substitute_parameters(expression.operand, values))
    if isinstance(expression, IsNull):
        return IsNull(substitute_parameters(expression.operand, values), expression.negated)
    if isinstance(expression, In):
        return In(
            substitute_parameters(expression.operand, values),
            tuple(substitute_parameters(v, values) for v in expression.values),
            expression.negated,
        )
    if isinstance(expression, Like):
        return Like(
            substitute_parameters(expression.operand, values),
            substitute_parameters(expression.pattern, values),
            expression.negated,
        )
    if isinstance(expression, Arithmetic):
        return Arithmetic(
            expression.op,
            substitute_parameters(expression.left, values),
            substitute_parameters(expression.right, values),
        )
    if isinstance(expression, Alias):
        return Alias(substitute_parameters(expression.expression, values), expression.name)
    return expression


def expression_from_dict(data: dict[str, Any]) -> Expression:
    node = data["node"]
    if node == "column":
        return Column(data["name"])
    if node == "literal":
        return Literal(_decode_literal(data["value"]), type_from_dict(data["type"]))
    if node == "parameter":
        return Parameter(data["name"])
    if node == "comparison":
        return Comparison(
            ComparisonOp(data["op"]),
            expression_from_dict(data["left"]),
            expression_from_dict(data["right"]),
        )
    if node == "and":
        return And(tuple(expression_from_dict(o) for o in data["operands"]))
    if node == "or":
        return Or(tuple(expression_from_dict(o) for o in data["operands"]))
    if node == "not":
        return Not(expression_from_dict(data["operand"]))
    if node == "is_null":
        return IsNull(expression_from_dict(data["operand"]), bool(data.get("negated", False)))
    if node == "in":
        return In(
            expression_from_dict(data["operand"]),
            tuple(expression_from_dict(v) for v in data["values"]),
            bool(data.get("negated", False)),
        )
    if node == "like":
        return Like(
            expression_from_dict(data["operand"]),
            expression_from_dict(data["pattern"]),
            bool(data.get("negated", False)),
        )
    if node == "arithmetic":
        return Arithmetic(
            ArithmeticOp(data["op"]),
            expression_from_dict(data["left"]),
            expression_from_dict(data["right"]),
        )
    if node == "alias":
        return Alias(expression_from_dict(data["expression"]), data["name"])
    raise ValueError(f"unknown expression node: {node!r}")


ALL_EXPRESSION_KINDS: frozenset[ExpressionKind] = frozenset(ExpressionKind)

__all__ = [
    "ALL_EXPRESSION_KINDS",
    "Alias",
    "And",
    "Arithmetic",
    "ArithmeticOp",
    "Column",
    "Comparison",
    "ComparisonOp",
    "Expression",
    "ExpressionKind",
    "In",
    "IsNull",
    "Like",
    "Literal",
    "LiteralValue",
    "Not",
    "Or",
    "Parameter",
    "and_all",
    "conjuncts",
    "expression_from_dict",
    "infer_literal_type",
    "output_name",
    "referenced_columns",
    "referenced_parameters",
    "substitute_parameters",
    "unalias",
    "walk",
]
