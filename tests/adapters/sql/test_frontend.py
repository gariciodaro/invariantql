"""FF-11: only the documented read-only profile yields a plan; everything else is rejected first."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from invariantql.adapters.sql import SqlFrontend
from invariantql.domain import (
    Alias,
    And,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    ComparisonOp,
    DiagnosticCode,
    In,
    IsNull,
    Like,
    Literal,
    Not,
    Or,
    Parameter,
    QueryPlan,
    SqlFrontendError,
)

pytestmark = pytest.mark.architecture

FRONTEND = SqlFrontend()

REJECTED = [
    ("", DiagnosticCode.SQL_EMPTY),
    ("   ", DiagnosticCode.SQL_EMPTY),
    ("SELECT 1; SELECT 2", DiagnosticCode.SQL_MULTIPLE_STATEMENTS),
    ("SELECT * FROM t; DROP TABLE t", DiagnosticCode.SQL_MULTIPLE_STATEMENTS),
    ("DROP TABLE t", DiagnosticCode.SQL_NOT_A_SELECT),
    ("DELETE FROM t", DiagnosticCode.SQL_NOT_A_SELECT),
    ("INSERT INTO t VALUES (1)", DiagnosticCode.SQL_NOT_A_SELECT),
    ("UPDATE t SET a = 1", DiagnosticCode.SQL_NOT_A_SELECT),
    ("CREATE TABLE x AS SELECT * FROM t", DiagnosticCode.SQL_NOT_A_SELECT),
    ("SELECT * FROM t UNION SELECT * FROM u", DiagnosticCode.SQL_NOT_A_SELECT),
    ("SELECT * FROM t JOIN u ON t.id = u.id", DiagnosticCode.SQL_MULTI_SOURCE),
    ("SELECT * FROM t, u", DiagnosticCode.SQL_MULTI_SOURCE),
    ("SELECT * FROM (SELECT * FROM t)", DiagnosticCode.SQL_MULTI_SOURCE),
    ("SELECT * FROM t WHERE a IN (SELECT a FROM u)", DiagnosticCode.SQL_MULTI_SOURCE),
    ("SELECT (SELECT 1) FROM t", DiagnosticCode.SQL_MULTI_SOURCE),
    ("WITH c AS (SELECT 1) SELECT * FROM t", DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT),
    ("SELECT * FROM t ORDER BY a", DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT),
    ("SELECT a FROM t GROUP BY a", DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT),
    ("SELECT DISTINCT a FROM t", DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT),
    ("SELECT * FROM t LIMIT 5 OFFSET 2", DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT),
    ("SELECT * FROM t HAVING a > 1", DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT),
    ("SELECT 1", DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT),
    ("SELECT *, a FROM t", DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT),
    ("SELECT * FROM db.t", DiagnosticCode.SQL_QUALIFIED_SOURCE),
    ("SELECT * FROM read_csv('x')", DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT),
    ("SELECT x.foo FROM t AS x(foo)", DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT),
    ("SELECT count(*) FROM t", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT lower(a) FROM t", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT CAST(a AS INT) FROM t", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT a FROM t WHERE a ILIKE 'x'", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT a FROM t WHERE a LIKE 'x' ESCAPE '!'", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT a FROM t WHERE a = ?", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT a FROM t WHERE a IS TRUE", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT a FROM t WHERE a IN ()", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT a FROM t WHERE a IN (b)", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT a FROM t WHERE CASE WHEN a THEN 1 END = 1", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT a FROM t WHERE a = 1 OVER ()", DiagnosticCode.SQL_PARSE_ERROR),
    ("SELECT * FROM t LIMIT -1", DiagnosticCode.SQL_INVALID_LIMIT),
    ("SELECT * FROM t LIMIT 1.5", DiagnosticCode.SQL_INVALID_LIMIT),
    ("SELECT * FROM t LIMIT :n", DiagnosticCode.SQL_INVALID_LIMIT),
    ("SELECT * FROM t LIMIT 'x'", DiagnosticCode.SQL_INVALID_LIMIT),
    ("SELECT 1e309 FROM t", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT 9223372036854775808 FROM t", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT -9223372036854775809 FROM t", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
    ("SELECT u.a FROM t", DiagnosticCode.SQL_AMBIGUOUS_IDENTIFIER),
    ("SELECT db.t.a FROM t", DiagnosticCode.SQL_AMBIGUOUS_IDENTIFIER),
    ("SELEC * FROM t", DiagnosticCode.SQL_PARSE_ERROR),
    ("SELECT * FROM t WHERE", DiagnosticCode.SQL_PARSE_ERROR),
    ("SELECT * FROM t -- comment\n; -- trailing", DiagnosticCode.SQL_MULTIPLE_STATEMENTS),
    ("SELECT a FROM t WHERE DATE 'nope' > a", DiagnosticCode.SQL_UNSUPPORTED_EXPRESSION),
]


@pytest.mark.parametrize(("sql", "code"), REJECTED, ids=[s[:40] for s, _ in REJECTED])
def test_rejected_before_any_source_is_contacted(sql: str, code: DiagnosticCode) -> None:
    # The frontend has no access to sources at all; rejection is structural.
    with pytest.raises(SqlFrontendError) as info:
        FRONTEND.parse(sql)
    assert info.value.code is code, str(info.value)


ACCEPTED = [
    ("SELECT * FROM t", QueryPlan.scan("t")),
    ("select * from T", QueryPlan.scan("T")),
    ("SELECT t.* FROM t", QueryPlan.scan("t")),
    ("SELECT a, b AS c FROM t", QueryPlan.scan("t").select("a", Alias(Column("b"), "c"))),
    (
        "SELECT a + 1 FROM t",
        QueryPlan.scan("t").select(
            Alias(Arithmetic(ArithmeticOp.ADD, Column("a"), Literal.of(1)), "_col1")
        ),
    ),
    ("SELECT x.a FROM t AS x", QueryPlan.scan("t").select("a")),
    ("SELECT t.a FROM t", QueryPlan.scan("t").select("a")),
    ('SELECT "Weird Name" FROM t', QueryPlan.scan("t").select("Weird Name")),
    (
        "SELECT * FROM t WHERE a > 1 AND b <= 2.5 AND c <> 'x' AND d != TRUE",
        QueryPlan.scan("t").where(
            And(
                (
                    Comparison(ComparisonOp.GT, Column("a"), Literal.of(1)),
                    Comparison(
                        ComparisonOp.LE,
                        Column("b"),
                        Literal(Decimal("2.5"), Literal.of(Decimal("2.5")).data_type),
                    ),
                    Comparison(ComparisonOp.NE, Column("c"), Literal.of("x")),
                    Comparison(ComparisonOp.NE, Column("d"), Literal.of(True)),
                )
            )
        ),
    ),
    (
        "SELECT * FROM t WHERE (a = 1 OR b = 2) AND NOT c IS NULL",
        QueryPlan.scan("t").where(
            And(
                (
                    Or(
                        (
                            Comparison(ComparisonOp.EQ, Column("a"), Literal.of(1)),
                            Comparison(ComparisonOp.EQ, Column("b"), Literal.of(2)),
                        )
                    ),
                    IsNull(Column("c"), negated=True),
                )
            )
        ),
    ),
    (
        "SELECT * FROM t WHERE a IS NOT NULL",
        QueryPlan.scan("t").where(IsNull(Column("a"), negated=True)),
    ),
    (
        "SELECT * FROM t WHERE NOT (a = 1)",
        QueryPlan.scan("t").where(Not(Comparison(ComparisonOp.EQ, Column("a"), Literal.of(1)))),
    ),
    (
        "SELECT * FROM t WHERE a IN (1, 2, :p)",
        QueryPlan.scan("t").where(In(Column("a"), (Literal.of(1), Literal.of(2), Parameter("p")))),
    ),
    (
        "SELECT * FROM t WHERE a NOT IN ('x')",
        QueryPlan.scan("t").where(In(Column("a"), (Literal.of("x"),), negated=True)),
    ),
    (
        "SELECT * FROM t WHERE a LIKE 'x%'",
        QueryPlan.scan("t").where(Like(Column("a"), Literal.of("x%"))),
    ),
    (
        "SELECT * FROM t WHERE a NOT LIKE :pat",
        QueryPlan.scan("t").where(Like(Column("a"), Parameter("pat"), negated=True)),
    ),
    (
        "SELECT * FROM t WHERE a BETWEEN 1 AND 5",
        QueryPlan.scan("t").where(
            And(
                (
                    Comparison(ComparisonOp.GE, Column("a"), Literal.of(1)),
                    Comparison(ComparisonOp.LE, Column("a"), Literal.of(5)),
                )
            )
        ),
    ),
    (
        "SELECT * FROM t WHERE a = -3",
        QueryPlan.scan("t").where(Comparison(ComparisonOp.EQ, Column("a"), Literal.of(-3))),
    ),
    (
        "SELECT * FROM t WHERE a = 1e3",
        QueryPlan.scan("t").where(Comparison(ComparisonOp.EQ, Column("a"), Literal.of(1000.0))),
    ),
    (
        "SELECT * FROM t WHERE d >= DATE '2024-01-31'",
        QueryPlan.scan("t").where(
            Comparison(ComparisonOp.GE, Column("d"), Literal.of(dt.date(2024, 1, 31)))
        ),
    ),
    (
        "SELECT * FROM t WHERE ts < TIMESTAMP '2024-01-31 10:00:00'",
        QueryPlan.scan("t").where(
            Comparison(ComparisonOp.LT, Column("ts"), Literal.of(dt.datetime(2024, 1, 31, 10)))
        ),
    ),
    (
        "SELECT * FROM t WHERE a IS NULL LIMIT 10",
        QueryPlan.scan("t").where(IsNull(Column("a"))).limit(10),
    ),
    ("SELECT * FROM t LIMIT 0", QueryPlan.scan("t").limit(0)),
    (
        "SELECT -9223372036854775808 FROM t",
        QueryPlan.scan("t").select(Alias(Literal.of(-(2**63)), "_col1")),
    ),
    ("/* c */ SELECT a -- x\n FROM t", QueryPlan.scan("t").select("a")),
    (
        "SELECT * FROM t WHERE a = :x AND b = :x",
        QueryPlan.scan("t").where(
            And(
                (
                    Comparison(ComparisonOp.EQ, Column("a"), Parameter("x")),
                    Comparison(ComparisonOp.EQ, Column("b"), Parameter("x")),
                )
            )
        ),
    ),
    (
        "SELECT a * (b - 1) / 2 AS r FROM t",
        QueryPlan.scan("t").select(
            Alias(
                Arithmetic(
                    ArithmeticOp.DIV,
                    Arithmetic(
                        ArithmeticOp.MUL,
                        Column("a"),
                        Arithmetic(ArithmeticOp.SUB, Column("b"), Literal.of(1)),
                    ),
                    Literal.of(2),
                ),
                "r",
            )
        ),
    ),
]


@pytest.mark.parametrize(("sql", "expected"), ACCEPTED, ids=[s[:40] for s, _ in ACCEPTED])
def test_accepted_profile_translates_exactly(sql: str, expected: QueryPlan) -> None:
    assert FRONTEND.parse(sql) == expected


def test_parameters_and_columns_are_reported() -> None:
    plan = FRONTEND.parse("SELECT a FROM t WHERE b > :lo AND c LIKE :pat")
    assert plan.parameters == ("lo", "pat")
    assert plan.referenced_columns == ("a", "b", "c")


@settings(max_examples=200, deadline=None)
@given(st.text(max_size=60))
def test_arbitrary_text_never_escapes_as_another_exception(text: str) -> None:
    try:
        FRONTEND.parse(text)
    except SqlFrontendError:
        pass


@settings(max_examples=100, deadline=None)
@given(
    st.sampled_from(
        [
            "DROP",
            "DELETE",
            "INSERT",
            "UPDATE",
            "CREATE",
            "ALTER",
            "TRUNCATE",
            "GRANT",
            "COPY",
            "ATTACH",
            "PRAGMA",
            "CALL",
            "MERGE",
        ]
    ),
    st.text(max_size=30),
)
def test_non_select_statements_are_never_plans(keyword: str, tail: str) -> None:
    with pytest.raises(SqlFrontendError):
        FRONTEND.parse(f"{keyword} {tail}")
