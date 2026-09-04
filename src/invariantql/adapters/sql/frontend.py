"""SQLGlot-backed SQL frontend (ADR-0006, FF-11).

Parses exactly one read-only ``SELECT`` over one source and translates it
into the domain plan. Supported profile (version 1):

* ``SELECT * | column | expr AS alias, ... FROM source [AS alias]``
* ``WHERE`` with ``=, <>, !=, <, <=, >, >=, AND, OR, NOT, IS [NOT] NULL,
  [NOT] IN (literals/parameters), [NOT] LIKE, BETWEEN, + - * /``
* ``LIMIT <non-negative integer>``
* named parameters ``:name``; typed literals ``DATE '...'`` and ``TIMESTAMP '...'``

Everything else is rejected before any source is contacted. No SQLGlot node
escapes this module.
"""

from __future__ import annotations

import datetime as _dt
from decimal import Decimal, InvalidOperation
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError

from invariantql.domain.diagnostics import DiagnosticCode, SqlFrontendError
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
    Not,
    Or,
    Parameter,
)
from invariantql.domain.plan import QueryPlan
from invariantql.domain.types import DateType, TimestampType

SQL_PROFILE_VERSION = "1"

_ALLOWED_SELECT_ARGS = frozenset({"expressions", "from", "from_", "where", "limit", "hint", "kind"})

_COMPARISONS: dict[type[exp.Expression], ComparisonOp] = {
    exp.EQ: ComparisonOp.EQ,
    exp.NEQ: ComparisonOp.NE,
    exp.LT: ComparisonOp.LT,
    exp.LTE: ComparisonOp.LE,
    exp.GT: ComparisonOp.GT,
    exp.GTE: ComparisonOp.GE,
}
_ARITHMETIC: dict[type[exp.Expression], ArithmeticOp] = {
    exp.Add: ArithmeticOp.ADD,
    exp.Sub: ArithmeticOp.SUB,
    exp.Mul: ArithmeticOp.MUL,
    exp.Div: ArithmeticOp.DIV,
}


class SqlFrontend:
    """Parse the InvariantQL SQL profile into a ``QueryPlan``."""

    def __init__(self, dialect: str | None = None) -> None:
        self.dialect = dialect

    @property
    def name(self) -> str:
        return "sql"

    def parse(self, text: str) -> QueryPlan:
        if not text or not text.strip():
            raise SqlFrontendError("empty SQL text", code=DiagnosticCode.SQL_EMPTY)
        try:
            statements = [s for s in sqlglot.parse(text, read=self.dialect) if s is not None]
        except (ParseError, TokenError) as exc:
            raise SqlFrontendError(
                f"SQL could not be parsed: {_first_line(str(exc))}",
                code=DiagnosticCode.SQL_PARSE_ERROR,
            ) from None
        if not statements:
            raise SqlFrontendError("no statement found", code=DiagnosticCode.SQL_EMPTY)
        if len(statements) > 1:
            raise SqlFrontendError(
                f"expected exactly one statement, found {len(statements)}",
                code=DiagnosticCode.SQL_MULTIPLE_STATEMENTS,
            )
        statement = statements[0]
        if not isinstance(statement, exp.Select):
            raise SqlFrontendError(
                f"only SELECT statements are supported, found {type(statement).__name__.upper()}",
                code=DiagnosticCode.SQL_NOT_A_SELECT,
                details={"statement": type(statement).__name__},
            )
        return _Translator(statement).translate()


class _Translator:
    def __init__(self, select: exp.Select) -> None:
        self.select = select
        self.source_name = ""
        self.qualifiers: set[str] = set()

    def translate(self) -> QueryPlan:
        self._reject_unsupported_clauses()
        source = self._source()
        plan = QueryPlan.scan(source)
        where = self.select.args.get("where")
        if where is not None:
            plan = plan.where(self._expression(where.this))
        projection = self._projection()
        if projection is not None:
            plan = plan.select(*projection)
        limit = self.select.args.get("limit")
        if limit is not None:
            plan = plan.limit(self._limit(limit))
        return plan

    # -- clauses ------------------------------------------------------------

    def _reject_unsupported_clauses(self) -> None:
        for key, value in self.select.args.items():
            if key in _ALLOWED_SELECT_ARGS or not value:
                continue
            if key == "joins":
                raise SqlFrontendError(
                    "joins are not supported; the query model has exactly one source (ADR-0007)",
                    code=DiagnosticCode.SQL_MULTI_SOURCE,
                )
            raise SqlFrontendError(
                f"unsupported clause: {key.upper()} (profile {SQL_PROFILE_VERSION} allows "
                "SELECT, FROM, WHERE and LIMIT over one source)",
                code=DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT,
                details={"clause": key},
            )
        for node in self.select.walk():
            if isinstance(node, (exp.Subquery, exp.Union, exp.Except, exp.Intersect)) or (
                isinstance(node, exp.Select) and node is not self.select
            ):
                raise SqlFrontendError(
                    "subqueries, unions and set operations are not supported",
                    code=DiagnosticCode.SQL_MULTI_SOURCE,
                )
            if isinstance(node, exp.Join):
                raise SqlFrontendError(
                    "joins are not supported; the query model has exactly one source (ADR-0007)",
                    code=DiagnosticCode.SQL_MULTI_SOURCE,
                )

    def _source(self) -> str:
        from_ = self.select.args.get("from") or self.select.args.get("from_")
        if from_ is None:
            raise SqlFrontendError(
                "a FROM clause naming one registered source is required",
                code=DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT,
                details={"clause": "from"},
            )
        table = from_.this
        if not isinstance(table, exp.Table):
            raise SqlFrontendError(
                f"FROM must name a source, found {type(table).__name__}",
                code=DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT,
            )
        if table.args.get("db") or table.args.get("catalog"):
            raise SqlFrontendError(
                "qualified source names are not supported; register the source under a plain name",
                code=DiagnosticCode.SQL_QUALIFIED_SOURCE,
            )
        for key in ("joins", "laterals", "pivots", "sample", "version", "ordinality"):
            if table.args.get(key):
                raise SqlFrontendError(
                    f"unsupported table construct: {key}",
                    code=DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT,
                )
        if not isinstance(table.this, exp.Identifier):
            raise SqlFrontendError(
                "FROM must name a source identifier (table functions are not supported)",
                code=DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT,
            )
        self.source_name = table.this.name
        self.qualifiers = {self.source_name}
        alias = table.args.get("alias")
        if alias is not None and alias.this is not None:
            if alias.args.get("columns"):
                raise SqlFrontendError(
                    "table alias column lists are not supported",
                    code=DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT,
                    details={"construct": "table_alias_columns"},
                )
            self.qualifiers.add(alias.this.name)
        return self.source_name

    def _projection(self) -> tuple[Expression, ...] | None:
        expressions = list(self.select.expressions)
        if not expressions:
            raise SqlFrontendError(
                "SELECT list is empty", code=DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT
            )
        if len(expressions) == 1 and isinstance(expressions[0], exp.Star):
            return None
        if (
            len(expressions) == 1
            and isinstance(expressions[0], exp.Column)
            and isinstance(expressions[0].this, exp.Star)
        ):
            self._check_qualifier(expressions[0])
            return None
        out: list[Expression] = []
        for index, item in enumerate(expressions, start=1):
            if isinstance(item, exp.Star) or (
                isinstance(item, exp.Column) and isinstance(item.this, exp.Star)
            ):
                raise SqlFrontendError(
                    "'*' cannot be combined with other select expressions",
                    code=DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT,
                )
            if isinstance(item, exp.Alias):
                inner = self._expression(item.this)
                out.append(Alias(inner, item.alias))
            elif isinstance(item, exp.Column):
                out.append(self._column(item))
            else:
                out.append(Alias(self._expression(item), f"_col{index}"))
        return tuple(out)

    def _limit(self, limit: exp.Limit) -> int:
        value = limit.expression
        if isinstance(value, exp.Literal) and not value.is_string:
            try:
                count = int(value.this)
            except ValueError:
                count = -1
            if count >= 0 and str(count) == str(value.this):
                return count
        raise SqlFrontendError(
            "LIMIT must be a non-negative integer literal",
            code=DiagnosticCode.SQL_INVALID_LIMIT,
        )

    # -- expressions --------------------------------------------------------

    def _expression(self, node: exp.Expression) -> Expression:
        if isinstance(node, exp.Paren):
            return self._expression(node.this)
        if isinstance(node, exp.Column):
            return self._column(node)
        if isinstance(node, exp.Literal):
            return self._literal(node)
        if isinstance(node, exp.Boolean):
            return Literal.of(bool(node.this))
        if isinstance(node, exp.Null):
            return Literal.of(None)
        if isinstance(node, exp.Placeholder):
            if node.this is None or not isinstance(node.this, str):
                raise SqlFrontendError(
                    "positional parameters are not supported; use named parameters like :name",
                    code=DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION,
                )
            return Parameter(node.this)
        if isinstance(node, exp.Neg):
            if isinstance(node.this, exp.Literal) and not node.this.is_string:
                return self._numeric_literal("-" + str(node.this.this))
            inner = self._expression(node.this)
            if (
                isinstance(inner, Literal)
                and isinstance(inner.value, (int, float, Decimal))
                and not isinstance(inner.value, bool)
            ):
                return Literal(-inner.value, inner.data_type)
            return Arithmetic(ArithmeticOp.SUB, Literal.of(0), inner)
        if type(node) in _COMPARISONS:
            return Comparison(
                _COMPARISONS[type(node)],
                self._expression(node.this),
                self._expression(node.expression),
            )
        if isinstance(node, exp.And):
            return And(tuple(self._flatten(node, exp.And)))
        if isinstance(node, exp.Or):
            return Or(tuple(self._flatten(node, exp.Or)))
        if isinstance(node, exp.Not):
            inner = node.this
            while isinstance(inner, exp.Paren):
                inner = inner.this
            if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null):
                return IsNull(self._expression(inner.this), negated=True)
            if isinstance(inner, exp.In):
                translated = self._in(inner)
                return In(translated.operand, translated.values, negated=not translated.negated)
            if isinstance(inner, exp.Like):
                translated = self._like(inner)
                return Like(translated.operand, translated.pattern, negated=not translated.negated)
            return Not(self._expression(inner))
        if isinstance(node, exp.Is):
            if isinstance(node.expression, exp.Null):
                return IsNull(self._expression(node.this))
            raise SqlFrontendError(
                "IS is only supported with NULL",
                code=DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION,
            )
        if isinstance(node, exp.In):
            return self._in(node)
        if isinstance(node, exp.Like):
            return self._like(node)
        if isinstance(node, exp.Between):
            operand = self._expression(node.this)
            low = self._expression(node.args["low"])
            high = self._expression(node.args["high"])
            return And(
                (
                    Comparison(ComparisonOp.GE, operand, low),
                    Comparison(ComparisonOp.LE, operand, high),
                )
            )
        if type(node) in _ARITHMETIC:
            return Arithmetic(
                _ARITHMETIC[type(node)],
                self._expression(node.this),
                self._expression(node.expression),
            )
        if isinstance(node, exp.Cast):
            return self._typed_literal(node)
        raise SqlFrontendError(
            f"unsupported expression: {_describe(node)}",
            code=DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION,
            details={"expression": type(node).__name__},
        )

    def _flatten(self, node: exp.Expression, kind: type[exp.Expression]) -> list[Expression]:
        parts: list[Expression] = []
        for child in (node.this, node.expression):
            inner = child
            while isinstance(inner, exp.Paren):
                inner = inner.this
            if isinstance(inner, kind):
                parts.extend(self._flatten(inner, kind))
            else:
                parts.append(self._expression(child))
        return parts

    def _in(self, node: exp.In) -> In:
        if node.args.get("query") is not None or node.args.get("unnest") is not None:
            raise SqlFrontendError(
                "IN with a subquery is not supported",
                code=DiagnosticCode.SQL_MULTI_SOURCE,
            )
        values: list[Expression] = []
        for value in node.expressions:
            translated = self._expression(value)
            if not isinstance(translated, (Literal, Parameter)):
                raise SqlFrontendError(
                    "IN list values must be literals or parameters",
                    code=DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION,
                )
            values.append(translated)
        if not values:
            raise SqlFrontendError(
                "IN list is empty", code=DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION
            )
        return In(self._expression(node.this), tuple(values), negated=bool(node.args.get("negate")))

    def _like(self, node: exp.Like) -> Like:
        pattern = self._expression(node.expression)
        if not isinstance(pattern, (Literal, Parameter)):
            raise SqlFrontendError(
                "LIKE pattern must be a literal or parameter",
                code=DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION,
            )
        if node.args.get("escape") is not None:
            raise SqlFrontendError(
                "LIKE ... ESCAPE is not supported",
                code=DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION,
            )
        return Like(self._expression(node.this), pattern, negated=bool(node.args.get("negate")))

    def _column(self, node: exp.Column) -> Column:
        self._check_qualifier(node)
        if not isinstance(node.this, exp.Identifier):
            raise SqlFrontendError(
                f"unsupported column reference: {node.sql()}",
                code=DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION,
            )
        return Column(node.this.name)

    def _check_qualifier(self, node: exp.Column) -> None:
        table = node.args.get("table")
        if node.args.get("db") or node.args.get("catalog"):
            raise SqlFrontendError(
                f"column {node.sql()} is qualified beyond the source",
                code=DiagnosticCode.SQL_AMBIGUOUS_IDENTIFIER,
            )
        if table is not None and table.name not in self.qualifiers:
            raise SqlFrontendError(
                f"unknown qualifier {table.name!r} in {node.sql()}; the only source is {self.source_name!r}",
                code=DiagnosticCode.SQL_AMBIGUOUS_IDENTIFIER,
                details={"qualifier": table.name},
            )

    def _literal(self, node: exp.Literal) -> Literal:
        if node.is_string:
            return Literal.of(str(node.this))
        return self._numeric_literal(str(node.this))

    def _numeric_literal(self, text: str) -> Literal:
        try:
            if text.lstrip("-").isdigit():
                return Literal.of(int(text))
            if any(c in text for c in ("e", "E")):
                return Literal.of(float(text))
            return Literal.of(Decimal(text))
        except (ValueError, TypeError, InvalidOperation):
            raise SqlFrontendError(
                f"invalid numeric literal {text!r}",
                code=DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION,
            ) from None

    def _typed_literal(self, node: exp.Cast) -> Literal:
        target = node.to
        inner = node.this
        if isinstance(target, exp.DataType) and isinstance(inner, exp.Literal) and inner.is_string:
            text = str(inner.this)
            try:
                if target.this is exp.DataType.Type.DATE:
                    return Literal(_dt.date.fromisoformat(text), DateType())
                if target.this in (exp.DataType.Type.TIMESTAMP, exp.DataType.Type.DATETIME):
                    return Literal(_dt.datetime.fromisoformat(text), TimestampType(None))
                if target.this is exp.DataType.Type.TIMESTAMPTZ:
                    value = _dt.datetime.fromisoformat(text)
                    if value.tzinfo is None:
                        value = value.replace(tzinfo=_dt.timezone.utc)
                    return Literal(value.astimezone(_dt.timezone.utc), TimestampType("UTC"))
            except ValueError:
                raise SqlFrontendError(
                    f"invalid typed literal {node.sql()}",
                    code=DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION,
                ) from None
        raise SqlFrontendError(
            f"CAST is not supported (only DATE '...' and TIMESTAMP '...' literals): {node.sql()}",
            code=DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION,
            details={"expression": "Cast"},
        )


def _describe(node: exp.Expression) -> str:
    name = type(node).__name__
    if isinstance(node, exp.Func):
        return f"function {node.sql_name()}()"
    return f"{name} in {node.sql()}"


def _first_line(text: str) -> str:
    return text.strip().splitlines()[0] if text.strip() else text


def sql_from_dict(data: dict[str, Any]) -> QueryPlan:  # pragma: no cover - convenience
    return QueryPlan.from_dict(data)


__all__ = ["SQL_PROFILE_VERSION", "SqlFrontend"]
