"""Generate dialect-specific SQL text from domain expressions.

Every literal and parameter value is bound through the driver's placeholder
mechanism; values are never interpolated into SQL (ADR-0006, FF-11). Only
identifiers and format options are rendered as text, with proper quoting.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from invariantql.domain.diagnostics import DiagnosticCode, ParameterError
from invariantql.domain.expressions import (
    Alias,
    And,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    Expression,
    In,
    IsNull,
    Like,
    Literal,
    Not,
    Or,
    Parameter,
)


@dataclass(frozen=True, slots=True)
class SqlDialect:
    name: str
    identifier_quote: str = '"'
    placeholder: str = "?"
    like_operator: str = "LIKE"
    division_cast: str | None = None
    limit_keyword: str = "LIMIT"

    def quote(self, identifier: str) -> str:
        q = self.identifier_quote
        return q + identifier.replace(q, q + q) + q

    def next_placeholder(self, index: int) -> str:
        if self.placeholder == "$n":
            return f"${index + 1}"
        return self.placeholder


DUCKDB = SqlDialect("duckdb", placeholder="$n")
POSTGRES = SqlDialect("postgresql", placeholder="%s", division_cast="DOUBLE PRECISION")
MYSQL = SqlDialect("mysql", identifier_quote="`", placeholder="%s", like_operator="LIKE BINARY")


class SqlGenerator:
    def __init__(
        self, dialect: SqlDialect, parameters: Mapping[str, Literal] | None = None
    ) -> None:
        self.dialect = dialect
        self.parameters = dict(parameters or {})
        self.values: list[Any] = []

    # -- statements ---------------------------------------------------------

    def select(
        self,
        relation_sql: str,
        *,
        columns: Sequence[str] | None = None,
        projection: Sequence[Expression] | None = None,
        predicate: Expression | None = None,
        limit: int | None = None,
    ) -> str:
        """``SELECT ... FROM relation [WHERE ...] [LIMIT n]``.

        ``columns`` selects source columns by name; ``projection`` renders
        output expressions (aliases allowed). Passing neither selects ``*``.
        """

        if projection is not None:
            select_list = ", ".join(self.projected(e) for e in projection)
        elif columns is not None:
            select_list = ", ".join(self.dialect.quote(c) for c in columns) or "*"
        else:
            select_list = "*"
        sql = f"SELECT {select_list} FROM {relation_sql}"
        if predicate is not None:
            sql += " WHERE " + self.expression(predicate)
        if limit is not None:
            sql += f" {self.dialect.limit_keyword} {self.bind(int(limit))}"
        return sql

    # -- expressions --------------------------------------------------------

    def projected(self, expression: Expression) -> str:
        if isinstance(expression, Alias):
            return (
                f"{self.expression(expression.expression)} AS {self.dialect.quote(expression.name)}"
            )
        return self.expression(expression)

    def expression(self, expression: Expression) -> str:
        d = self.dialect
        if isinstance(expression, Column):
            return d.quote(expression.name)
        if isinstance(expression, Literal):
            return self.literal(expression)
        if isinstance(expression, Parameter):
            try:
                literal = self.parameters[expression.name]
            except KeyError:
                raise ParameterError(
                    f"missing parameter {expression.name!r}",
                    code=DiagnosticCode.PARAMETER_MISSING,
                ) from None
            return self.literal(literal)
        if isinstance(expression, Comparison):
            return f"({self.expression(expression.left)} {expression.op.value} {self.expression(expression.right)})"
        if isinstance(expression, And):
            return "(" + " AND ".join(self.expression(o) for o in expression.operands) + ")"
        if isinstance(expression, Or):
            return "(" + " OR ".join(self.expression(o) for o in expression.operands) + ")"
        if isinstance(expression, Not):
            return f"(NOT {self.expression(expression.operand)})"
        if isinstance(expression, IsNull):
            return f"({self.expression(expression.operand)} IS {'NOT ' if expression.negated else ''}NULL)"
        if isinstance(expression, In):
            values = ", ".join(self.expression(v) for v in expression.values)
            return f"({self.expression(expression.operand)} {'NOT ' if expression.negated else ''}IN ({values}))"
        if isinstance(expression, Like):
            op = d.like_operator
            return f"({self.expression(expression.operand)} {'NOT ' if expression.negated else ''}{op} {self.expression(expression.pattern)})"
        if isinstance(expression, Arithmetic):
            left, right = self.expression(expression.left), self.expression(expression.right)
            if expression.op is ArithmeticOp.DIV and d.division_cast:
                left = f"CAST({left} AS {d.division_cast})"
            return f"({left} {expression.op.value} {right})"
        if isinstance(expression, Alias):
            raise ValueError("alias is only valid at the top of a projection")
        raise ValueError(f"unsupported expression: {type(expression).__name__}")

    def literal(self, literal: Literal) -> str:
        if literal.value is None:
            return "NULL"
        if isinstance(literal.value, bool):
            return "TRUE" if literal.value else "FALSE"
        return self.bind(literal.value)

    def bind(self, value: Any) -> str:
        placeholder = self.dialect.next_placeholder(len(self.values))
        self.values.append(value)
        return placeholder


def sql_string(text: str) -> str:
    """Quote a string as a SQL literal (for format options, never user values)."""

    return "'" + text.replace("'", "''") + "'"


__all__ = ["DUCKDB", "MYSQL", "POSTGRES", "SqlDialect", "SqlGenerator", "sql_string"]
