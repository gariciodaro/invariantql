"""A small typed expression builder: a peer frontend to SQL (ADR-0006).

``col("amount") > 5`` builds domain expressions without SQL text. The wrapper
unwraps to plain domain nodes; the domain itself stays operator-free.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from invariantql.domain.expressions import (
    Alias,
    And,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    ComparisonOp,
    Expression,
    In,
    IsNull,
    Like,
    Literal,
    LiteralValue,
    Not,
    Or,
    Parameter,
)


def _unwrap(value: Any) -> Expression:
    if isinstance(value, Expr):
        return value.node
    if isinstance(value, Expression):
        return value
    return Literal.of(value)


class Expr:
    """Operator sugar over a domain expression."""

    __slots__ = ("node",)

    def __init__(self, node: Expression) -> None:
        self.node = node

    # comparisons
    def __eq__(self, other: object) -> Expr:  # type: ignore[override]
        return Expr(Comparison(ComparisonOp.EQ, self.node, _unwrap(other)))

    def __ne__(self, other: object) -> Expr:  # type: ignore[override]
        return Expr(Comparison(ComparisonOp.NE, self.node, _unwrap(other)))

    def __lt__(self, other: Any) -> Expr:
        return Expr(Comparison(ComparisonOp.LT, self.node, _unwrap(other)))

    def __le__(self, other: Any) -> Expr:
        return Expr(Comparison(ComparisonOp.LE, self.node, _unwrap(other)))

    def __gt__(self, other: Any) -> Expr:
        return Expr(Comparison(ComparisonOp.GT, self.node, _unwrap(other)))

    def __ge__(self, other: Any) -> Expr:
        return Expr(Comparison(ComparisonOp.GE, self.node, _unwrap(other)))

    __hash__ = None  # type: ignore[assignment]

    # boolean composition
    def __and__(self, other: Any) -> Expr:
        return Expr(And((self.node, _unwrap(other))))

    def __or__(self, other: Any) -> Expr:
        return Expr(Or((self.node, _unwrap(other))))

    def __invert__(self) -> Expr:
        return Expr(Not(self.node))

    # arithmetic
    def __add__(self, other: Any) -> Expr:
        return Expr(Arithmetic(ArithmeticOp.ADD, self.node, _unwrap(other)))

    def __radd__(self, other: Any) -> Expr:
        return Expr(Arithmetic(ArithmeticOp.ADD, _unwrap(other), self.node))

    def __sub__(self, other: Any) -> Expr:
        return Expr(Arithmetic(ArithmeticOp.SUB, self.node, _unwrap(other)))

    def __rsub__(self, other: Any) -> Expr:
        return Expr(Arithmetic(ArithmeticOp.SUB, _unwrap(other), self.node))

    def __mul__(self, other: Any) -> Expr:
        return Expr(Arithmetic(ArithmeticOp.MUL, self.node, _unwrap(other)))

    def __rmul__(self, other: Any) -> Expr:
        return Expr(Arithmetic(ArithmeticOp.MUL, _unwrap(other), self.node))

    def __truediv__(self, other: Any) -> Expr:
        return Expr(Arithmetic(ArithmeticOp.DIV, self.node, _unwrap(other)))

    def __rtruediv__(self, other: Any) -> Expr:
        return Expr(Arithmetic(ArithmeticOp.DIV, _unwrap(other), self.node))

    # predicates
    def is_null(self) -> Expr:
        return Expr(IsNull(self.node))

    def is_not_null(self) -> Expr:
        return Expr(IsNull(self.node, negated=True))

    def isin(self, values: Iterable[Any]) -> Expr:
        return Expr(In(self.node, tuple(_unwrap(v) for v in values)))

    def not_in(self, values: Iterable[Any]) -> Expr:
        return Expr(In(self.node, tuple(_unwrap(v) for v in values), negated=True))

    def like(self, pattern: str | Expr | Parameter) -> Expr:
        return Expr(Like(self.node, _unwrap(pattern)))

    def not_like(self, pattern: str | Expr | Parameter) -> Expr:
        return Expr(Like(self.node, _unwrap(pattern), negated=True))

    def between(self, low: Any, high: Any) -> Expr:
        return Expr(
            And(
                (
                    Comparison(ComparisonOp.GE, self.node, _unwrap(low)),
                    Comparison(ComparisonOp.LE, self.node, _unwrap(high)),
                )
            )
        )

    def alias(self, name: str) -> Expr:
        return Expr(Alias(self.node, name))

    def __repr__(self) -> str:
        return f"Expr({self.node})"

    def __str__(self) -> str:
        return str(self.node)


def col(name: str) -> Expr:
    return Expr(Column(name))


def lit(value: LiteralValue) -> Expr:
    return Expr(Literal.of(value))


def param(name: str) -> Expr:
    return Expr(Parameter(name))


def unwrap(value: Any) -> Expression:
    return _unwrap(value)


__all__ = ["Expr", "col", "lit", "param", "unwrap"]
