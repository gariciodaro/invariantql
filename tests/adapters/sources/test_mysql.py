"""MySQL source adapter: fast unit tests over fakes, plus gated live integration tests.

The unit tests never open a socket: ``pymysql.connect`` is replaced by a fake
connection/cursor pair that records every statement and its bound values and
serves canned ``information_schema`` and result rows. The integration tests
run only when ``INVARIANTQL_MYSQL_DSN`` (``mysql://user:pass@host:port/db``)
is set; they create throw-away tables and compare the MySQL pushdown results
with DuckDB reading the same rows from Parquet.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
import uuid
from collections.abc import Iterator
from decimal import Decimal
from typing import Any
from urllib.parse import unquote, urlsplit

import pyarrow as pa
import pymysql
import pytest

import invariantql as iql
from invariantql.adapters.sources import mysql as mysql_module
from invariantql.adapters.sources.mysql import (
    JDBC_DRIVER,
    RELATION_KIND,
    SCHEMA_SQL,
    SESSION_SQL_MODE_SQL,
    SESSION_TIME_ZONE_SQL,
    MySQLSource,
    map_column_type,
)
from invariantql.domain.capabilities import Support
from invariantql.domain.credentials import REDACTED
from invariantql.domain.diagnostics import DiagnosticCode, ParameterError, SourceError
from invariantql.domain.execution import PushedOperations
from invariantql.domain.explain import Disposition
from invariantql.domain.expressions import (
    ALL_EXPRESSION_KINDS,
    And,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    ComparisonOp,
    ExpressionKind,
    In,
    IsNull,
    Like,
    Literal,
    Not,
    Or,
    Parameter,
)
from invariantql.domain.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    FloatType,
    IntegerType,
    StringType,
    TimestampType,
)
from invariantql.ports.source import DataSource, NativeRelation

PASSWORD = "S3cret-Passw0rd!xyz"
UTC = dt.timezone.utc
RELATION = "`shop`.`orders`"

# (COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, IS_NULLABLE)
COLUMNS: list[tuple[Any, ...]] = [
    ("id", "bigint", "bigint", 19, 0, None, "NO"),
    ("name", "varchar", "varchar(64)", None, None, None, "YES"),
    ("amount", "double", "double", 22, None, None, "YES"),
    ("qty", "int", "int", 10, 0, None, "YES"),
    ("day", "date", "date", None, None, None, "YES"),
    ("active", "tinyint", "tinyint(1)", 3, 0, None, "YES"),
    ("price", "decimal", "decimal(10,2)", 10, 2, None, "YES"),
    ("flag", "bit", "bit(1)", 1, None, None, "YES"),
    ("created", "datetime", "datetime", None, None, 0, "YES"),
    ("updated", "timestamp", "timestamp(6)", None, None, 6, "YES"),
    ("payload", "blob", "blob", None, None, None, "YES"),
    ("doc", "json", "json", None, None, None, "YES"),
    ("kind", "enum", "enum('a','b')", None, None, None, "YES"),
    ("big", "bigint", "bigint unsigned", 20, 0, None, "YES"),
    ("y", "year", "year", None, None, None, "YES"),
    ("t", "time", "time(3)", None, None, 3, "YES"),
    ("mask", "bit", "bit(8)", 8, None, None, "YES"),
    ("f", "float", "float", 12, None, None, "YES"),
    ("tiny", "tinyint", "tinyint", 3, 0, None, "YES"),
    ("shape", "geometry", "geometry", None, None, None, "YES"),
]


# -- fakes --------------------------------------------------------------------


class FakeCursor:
    """Records statements; serves rows the connection decides on."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.closed = False
        self.fetchmany_sizes: list[int] = []
        self._rows: list[tuple[Any, ...]] = []
        self._pos = 0

    def execute(self, sql: str, args: Any = None) -> int:
        self.connection.executed.append((sql, args))
        self._rows = list(self.connection.respond(sql, args))
        self._pos = 0
        return len(self._rows)

    def fetchone(self) -> tuple[Any, ...] | None:
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int = 1) -> Any:
        self.fetchmany_sizes.append(size)
        chunk = self._rows[self._pos : self._pos + size]
        self._pos += size
        return chunk or ()

    def fetchall(self) -> list[tuple[Any, ...]]:
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class FakeConnection:
    """Stands in for ``pymysql.connections.Connection``."""

    def __init__(
        self,
        *,
        columns: list[tuple[Any, ...]] | None = None,
        rows: list[tuple[Any, ...]] | None = None,
        sql_mode: str = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES",
        fail_scan: BaseException | None = None,
    ) -> None:
        self.columns = COLUMNS if columns is None else columns
        self.rows = [] if rows is None else rows
        self.sql_mode = sql_mode
        self.fail_scan = fail_scan
        self.executed: list[tuple[str, Any]] = []
        self.cursors: list[FakeCursor] = []
        self.closed = False

    def cursor(self, cursorclass: Any = None) -> FakeCursor:
        if self.closed:
            raise pymysql.err.InterfaceError(0, "connection closed")
        cursor = FakeCursor(self)
        self.cursors.append(cursor)
        return cursor

    def respond(self, sql: str, args: Any) -> list[tuple[Any, ...]]:
        if sql == SESSION_TIME_ZONE_SQL:
            return []
        if sql == SESSION_SQL_MODE_SQL:
            return [(self.sql_mode,)]
        if sql == SCHEMA_SQL:
            assert args == ("shop", "orders")
            return self.columns
        if self.fail_scan is not None:
            raise self.fail_scan
        return self.rows

    def close(self) -> None:
        self.closed = True

    @property
    def scan_statements(self) -> list[tuple[str, Any]]:
        return [
            (sql, args)
            for sql, args in self.executed
            if sql not in (SESSION_TIME_ZONE_SQL, SESSION_SQL_MODE_SQL, SCHEMA_SQL)
        ]


def make_source(connection: Any | None = None, **overrides: Any) -> MySQLSource:
    options: dict[str, Any] = {
        "host": "db.internal",
        "port": 3307,
        "database": "shop",
        "table": "orders",
        "user": "analyst",
        "password": PASSWORD,
    }
    options.update(overrides)
    return MySQLSource("orders", connection=connection, **options)


@pytest.fixture()
def connections(monkeypatch: pytest.MonkeyPatch) -> list[FakeConnection]:
    """Replace ``pymysql.connect`` with a factory of fakes; returns the connections made."""

    made: list[FakeConnection] = []

    def connect(**kwargs: Any) -> FakeConnection:
        connection = FakeConnection()
        connection.kwargs = kwargs  # type: ignore[attr-defined]
        made.append(connection)
        return connection

    monkeypatch.setattr(mysql_module.pymysql, "connect", connect)
    return made


def _table(stream: Any) -> pa.Table:
    return pa.Table.from_batches(list(stream), schema=stream.schema)


# -- schema -------------------------------------------------------------------


def test_schema_mapping_and_session_setup() -> None:
    connection = FakeConnection()
    source = make_source(connection)
    assert isinstance(source, DataSource)

    schema = source.schema()

    expected = {
        "id": (IntegerType(64), False),
        "name": (StringType(), True),
        "amount": (FloatType(64), True),
        "qty": (IntegerType(32), True),
        "day": (DateType(), True),
        "active": (BooleanType(), True),
        "price": (DecimalType(10, 2), True),
        "flag": (BooleanType(), True),
        "created": (TimestampType(None), True),
        "updated": (TimestampType("UTC"), True),
        "payload": (BinaryType(), True),
        "doc": (StringType(), True),
        "kind": (StringType(), True),
        "big": (DecimalType(20, 0), True),
        "y": (IntegerType(16), True),
        "t": (StringType(), True),
        "mask": (IntegerType(64), True),
        "f": (FloatType(32), True),
        "tiny": (IntegerType(8), True),
        "shape": (BinaryType(), True),
    }
    assert schema.names == tuple(expected)
    for field in schema:
        assert (field.data_type, field.nullable) == expected[field.name], field.name

    statements = [sql for sql, _ in connection.executed]
    assert statements == [SESSION_TIME_ZONE_SQL, SESSION_SQL_MODE_SQL, SCHEMA_SQL]
    assert connection.executed[-1][1] == ("shop", "orders")

    # cached: a second call issues no statement
    assert source.schema() is schema
    assert len(connection.executed) == 3


def test_schema_missing_table_raises_schema_unavailable() -> None:
    source = make_source(FakeConnection(columns=[]))
    with pytest.raises(SourceError) as excinfo:
        source.schema()
    assert excinfo.value.code is DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE
    assert "`shop`.`orders`" in str(excinfo.value)


@pytest.mark.parametrize(
    ("data_type", "column_type", "precision", "scale", "fsp", "expected"),
    [
        ("tinyint", "tinyint(1)", 3, 0, None, BooleanType()),
        ("tinyint", "tinyint(1) unsigned", 3, 0, None, BooleanType()),
        ("tinyint", "tinyint", 3, 0, None, IntegerType(8)),
        ("tinyint", "tinyint unsigned", 3, 0, None, IntegerType(16)),
        ("smallint", "smallint", 5, 0, None, IntegerType(16)),
        ("mediumint", "mediumint", 7, 0, None, IntegerType(32)),
        ("int", "int", 10, 0, None, IntegerType(32)),
        ("int", "int unsigned", 10, 0, None, IntegerType(64)),
        ("bigint", "bigint", 19, 0, None, IntegerType(64)),
        ("bigint", "bigint unsigned", 20, 0, None, DecimalType(20, 0)),
        ("bit", "bit(1)", 1, None, None, BooleanType()),
        ("bit", "bit(12)", 12, None, None, IntegerType(64)),
        ("float", "float", 12, None, None, FloatType(32)),
        ("double", "double", 22, None, None, FloatType(64)),
        ("decimal", "decimal(65,30)", 65, 30, None, DecimalType(65, 30)),
        ("numeric", "decimal(5,0)", 5, 0, None, DecimalType(5, 0)),
        ("char", "char(3)", None, None, None, StringType()),
        ("text", "text", None, None, None, StringType()),
        ("enum", "enum('x')", None, None, None, StringType()),
        ("set", "set('x','y')", None, None, None, StringType()),
        ("json", "json", None, None, None, StringType()),
        ("varbinary", "varbinary(16)", None, None, None, BinaryType()),
        ("longblob", "longblob", None, None, None, BinaryType()),
        ("point", "point", None, None, None, BinaryType()),
        ("date", "date", None, None, None, DateType()),
        ("datetime", "datetime(3)", None, None, 3, TimestampType(None)),
        ("timestamp", "timestamp", None, None, 0, TimestampType("UTC")),
        ("time", "time", None, None, 0, StringType()),
        ("year", "year", None, None, None, IntegerType(16)),
        ("something_new", "something_new", None, None, None, StringType()),
    ],
)
def test_map_column_type(data_type, column_type, precision, scale, fsp, expected) -> None:
    mapped, convert = map_column_type(data_type, column_type, precision, scale, fsp)
    assert mapped == expected
    assert convert(None) is None


# -- capabilities and relation ---------------------------------------------------


def test_capabilities_are_full_and_parameterised() -> None:
    caps = make_source(FakeConnection()).capabilities()
    assert caps.projection is Support.FULL
    assert caps.predicate is Support.FULL
    assert caps.limit is Support.FULL
    assert caps.expressions == ALL_EXPRESSION_KINDS - {ExpressionKind.ARITHMETIC}
    assert ExpressionKind.ARITHMETIC not in caps.expressions
    assert caps.parameters is True
    assert caps.evidence


def test_relation_describes_spark_jdbc_reader_with_secrets() -> None:
    relation = make_source(FakeConnection()).relation()
    assert isinstance(relation, NativeRelation)
    assert relation.kind == RELATION_KIND == "jdbc:mysql"
    assert relation.options == {
        "url": "jdbc:mysql://db.internal:3307/shop",
        "dbtable": "`orders`",
        "driver": JDBC_DRIVER,
        "sessionInitStatement": SESSION_TIME_ZONE_SQL,
    }
    assert JDBC_DRIVER == "com.mysql.cj.jdbc.Driver"
    assert list(relation.secrets) == ["user", "password"]
    assert relation.secrets["password"] == REDACTED
    assert relation.secrets.reveal() == {"user": "analyst", "password": PASSWORD}
    assert relation.to_dict()["secrets"] == ["user", "password"]


def test_relation_without_password_carries_only_the_user() -> None:
    relation = make_source(FakeConnection(), password=None).relation()
    assert list(relation.secrets) == ["user"]


# -- SQL generation ------------------------------------------------------------------


def test_scan_sql_binds_every_value_and_compensates_semantics() -> None:
    connection = FakeConnection(rows=[(1, "alice")])
    source = make_source(connection)
    predicate = And(
        (
            Comparison(ComparisonOp.EQ, Column("name"), Literal.of("alice")),
            Comparison(ComparisonOp.GT, Column("qty"), Parameter("min")),
            Like(Column("name"), Literal.of("a%")),
            In(Column("name"), (Literal.of("x"), Parameter("other")), negated=True),
            IsNull(Column("qty"), negated=True),
            Comparison(ComparisonOp.EQ, Column("active"), Literal.of(True)),
            Comparison(
                ComparisonOp.GE,
                Arithmetic(ArithmeticOp.DIV, Column("qty"), Literal.of(2)),
                Literal.of(1),
            ),
        )
    )
    pushed = PushedOperations(projection=("id", "name"), predicate=predicate, limit=5)
    parameters = {"min": Literal.of(3), "other": Literal.of("y")}

    stream = source.scan(pushed, parameters, batch_size=100)
    table = _table(stream)

    sql, args = connection.scan_statements[-1]
    assert sql == (
        "SELECT `id`, `name` FROM `shop`.`orders` WHERE ("
        "(CAST(CONVERT(`name` USING utf8mb4) AS BINARY) = CAST(%s AS BINARY))"
        " AND (`qty` > %s)"
        " AND ((CONVERT(`name` USING utf8mb4) COLLATE utf8mb4_bin) LIKE %s ESCAPE '')"
        " AND (CAST(CONVERT(`name` USING utf8mb4) AS BINARY) NOT IN (CAST(%s AS BINARY), CAST(%s AS BINARY)))"
        " AND (`qty` IS NOT NULL)"
        " AND ((`active` <> 0) = TRUE)"
        " AND ((CAST(`qty` AS DOUBLE) / NULLIF(CAST(%s AS DOUBLE), 0.0)) >= %s)"
        ") LIMIT %s"
    )
    assert args == ("alice", 3, "a%", "x", "y", 2, 1, 5)
    assert table.schema.names == ["id", "name"]
    assert table.to_pylist() == [{"id": 1, "name": "alice"}]


def test_scan_without_pushdown_selects_every_column_in_schema_order() -> None:
    connection = FakeConnection()
    stream = make_source(connection).scan(PushedOperations(), {}, batch_size=10)
    sql, args = connection.scan_statements[-1]
    assert sql == "SELECT " + ", ".join(f"`{c[0]}`" for c in COLUMNS) + f" FROM {RELATION}"
    assert args is None
    assert stream.schema.names == [c[0] for c in COLUMNS]
    assert _table(stream).num_rows == 0


@pytest.mark.parametrize(
    ("predicate", "parameters", "expected_sql", "expected_args"),
    [
        # string column vs string parameter: byte-wise on both sides
        (
            Comparison(ComparisonOp.EQ, Column("name"), Parameter("p")),
            {"p": Literal.of("x")},
            "(CAST(CONVERT(`name` USING utf8mb4) AS BINARY) = CAST(%s AS BINARY))",
            ("x",),
        ),
        # string column vs non-string parameter: MySQL's own coercion applies
        (
            Comparison(ComparisonOp.EQ, Column("name"), Parameter("p")),
            {"p": Literal.of(5)},
            "(`name` = %s)",
            (5,),
        ),
        # NULL literal keeps the string wrap on the column
        (
            Comparison(ComparisonOp.NE, Column("name"), Literal.of(None)),
            {},
            "(CAST(CONVERT(`name` USING utf8mb4) AS BINARY) <> NULL)",
            None,
        ),
        # two literals are wrapped too: MySQL would compare them case-insensitively
        (
            Comparison(ComparisonOp.EQ, Literal.of("a"), Literal.of("A")),
            {},
            "(CAST(%s AS BINARY) = CAST(%s AS BINARY))",
            ("a", "A"),
        ),
        # two string columns
        (
            Comparison(ComparisonOp.LT, Column("name"), Column("kind")),
            {},
            "(CAST(CONVERT(`name` USING utf8mb4) AS BINARY) < CAST(CONVERT(`kind` USING utf8mb4) AS BINARY))",
            None,
        ),
        # non-string comparisons are untouched
        (
            Comparison(ComparisonOp.GE, Column("day"), Literal.of(dt.date(2024, 1, 3))),
            {},
            "(`day` >= %s)",
            (dt.date(2024, 1, 3),),
        ),
        # bit(1) booleans need no rewrite; tinyint(1) booleans test <> 0 everywhere
        (
            Or(
                (
                    Comparison(ComparisonOp.NE, Column("flag"), Literal.of(True)),
                    Not(Column("active")),
                )
            ),
            {},
            "((`flag` <> TRUE) OR (NOT (`active` <> 0)))",
            None,
        ),
        (
            IsNull(Column("active")),
            {},
            "((`active` <> 0) IS NULL)",
            None,
        ),
        # NOT LIKE with a parameter pattern
        (
            Like(Column("name"), Parameter("pat"), negated=True),
            {"pat": Literal.of("%a_")},
            "((CONVERT(`name` USING utf8mb4) COLLATE utf8mb4_bin) NOT LIKE %s ESCAPE '')",
            ("%a_",),
        ),
        # Arithmetic is typed before evaluation; division maps zero to NULL.
        (
            Comparison(
                ComparisonOp.GT,
                Arithmetic(
                    ArithmeticOp.MUL,
                    Arithmetic(ArithmeticOp.ADD, Column("qty"), Literal.of(1)),
                    Arithmetic(ArithmeticOp.SUB, Column("amount"), Literal.of(0.5)),
                ),
                Arithmetic(ArithmeticOp.DIV, Literal.of(7), Column("qty")),
            ),
            {},
            "(CAST((CAST(CAST((CAST(`qty` AS SIGNED) + CAST(%s AS SIGNED)) AS SIGNED) AS DOUBLE) "
            "* CAST((CAST(`amount` AS DOUBLE) - CAST(%s AS DOUBLE)) AS DOUBLE)) AS DOUBLE) "
            "> (CAST(%s AS DOUBLE) / NULLIF(CAST(`qty` AS DOUBLE), 0.0)))",
            (1, 0.5, 7),
        ),
        # IN over integers with a NULL member is bound as-is
        (
            In(Column("qty"), (Literal.of(1), Literal.of(None), Parameter("q"))),
            {"q": Literal.of(9)},
            "(`qty` IN (%s, NULL, %s))",
            (1, 9),
        ),
    ],
)
def test_predicate_translation(predicate, parameters, expected_sql, expected_args) -> None:
    connection = FakeConnection()
    make_source(connection).scan(
        PushedOperations(projection=("id",), predicate=predicate), parameters, batch_size=10
    ).close()
    sql, args = connection.scan_statements[-1]
    assert sql == f"SELECT `id` FROM {RELATION} WHERE {expected_sql}"
    assert args == expected_args


def test_arithmetic_widens_before_mysql_evaluates_and_zero_division_is_null() -> None:
    connection = FakeConnection()
    predicate = And(
        (
            Comparison(
                ComparisonOp.GT,
                Arithmetic(ArithmeticOp.ADD, Column("qty"), Parameter("increment")),
                Literal.of(0),
            ),
            Comparison(
                ComparisonOp.GT,
                Arithmetic(ArithmeticOp.ADD, Column("f"), Column("qty")),
                Literal.of(0.0),
            ),
            Comparison(
                ComparisonOp.GT,
                Arithmetic(ArithmeticOp.MUL, Column("price"), Column("price")),
                Literal.of(Decimal("0.00")),
            ),
            Comparison(
                ComparisonOp.GT,
                Arithmetic(ArithmeticOp.DIV, Column("amount"), Parameter("denominator")),
                Literal.of(0.0),
            ),
        )
    )
    make_source(connection).scan(
        PushedOperations(projection=("id",), predicate=predicate),
        {"increment": Literal.of(1), "denominator": Literal.of(0)},
        batch_size=10,
    ).close()

    sql, args = connection.scan_statements[-1]
    assert "CAST((CAST(`qty` AS SIGNED) + CAST(%s AS SIGNED)) AS SIGNED)" in sql
    assert "CAST((CAST(`f` AS DOUBLE) + CAST(`qty` AS DOUBLE)) AS DOUBLE)" in sql
    assert (
        "CAST((CAST(`price` AS DECIMAL(11,2)) * CAST(`price` AS DECIMAL(10,2))) AS DECIMAL(21,4))"
    ) in sql
    assert "CAST(`amount` AS DOUBLE) / NULLIF(CAST(%s AS DOUBLE), 0.0)" in sql
    assert args == (1, 0, 0.0, Decimal("0.00"), 0, 0.0)


def test_like_escape_clause_is_omitted_under_no_backslash_escapes() -> None:
    connection = FakeConnection(sql_mode="NO_BACKSLASH_ESCAPES,STRICT_ALL_TABLES")
    make_source(connection).scan(
        PushedOperations(projection=("id",), predicate=Like(Column("name"), Literal.of("a\\%"))),
        {},
        batch_size=10,
    ).close()
    sql, args = connection.scan_statements[-1]
    assert sql == (
        f"SELECT `id` FROM {RELATION} WHERE "
        "((CONVERT(`name` USING utf8mb4) COLLATE utf8mb4_bin) LIKE %s)"
    )
    assert args == ("a\\%",)


def test_timezone_aware_parameters_bind_as_naive_utc() -> None:
    connection = FakeConnection()
    since = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    make_source(connection).scan(
        PushedOperations(
            projection=("id",),
            predicate=And(
                (
                    Comparison(ComparisonOp.GE, Column("updated"), Parameter("since")),
                    Comparison(ComparisonOp.LT, Column("created"), Literal.of(since)),
                )
            ),
        ),
        {"since": Literal.of(since)},
        batch_size=10,
    ).close()
    _, args = connection.scan_statements[-1]
    assert args == (dt.datetime(2024, 1, 1, 10, 0), dt.datetime(2024, 1, 1, 10, 0))
    assert all(value.tzinfo is None for value in args)


def test_missing_parameter_raises_before_executing(connections: list[FakeConnection]) -> None:
    source = make_source()
    with pytest.raises(ParameterError) as excinfo:
        source.scan(
            PushedOperations(predicate=Comparison(ComparisonOp.EQ, Column("qty"), Parameter("q"))),
            {},
            batch_size=10,
        )
    assert excinfo.value.code is DiagnosticCode.PARAMETER_MISSING
    # schema connection plus the scan connection, which was released again
    assert len(connections) == 2
    assert connections[1].closed
    assert connections[1].scan_statements == []


def test_unknown_projection_column_raises_source_error(connections: list[FakeConnection]) -> None:
    source = make_source()
    with pytest.raises(SourceError) as excinfo:
        source.scan(PushedOperations(projection=("id", "nope")), {}, batch_size=10)
    assert excinfo.value.code is DiagnosticCode.PLAN_UNKNOWN_COLUMN
    assert len(connections) == 1  # no scan connection was opened


def test_batch_size_bounds_fetches_and_batches() -> None:
    rows = [(i, f"n{i}") for i in range(5)]
    connection = FakeConnection(rows=rows)
    stream = make_source(connection).scan(
        PushedOperations(projection=("id", "name")), {}, batch_size=2
    )
    batches = list(stream)
    assert [b.num_rows for b in batches] == [2, 2, 1]
    scan_cursor = connection.cursors[-1]
    assert set(scan_cursor.fetchmany_sizes) == {2}


@pytest.mark.parametrize("batch_size", [0, -1, True, 1.5, "10", None])
def test_scan_rejects_invalid_batch_size(batch_size: Any) -> None:
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        make_source(FakeConnection()).scan(PushedOperations(), {}, batch_size=batch_size)


# -- value conversion ------------------------------------------------------------------


def test_scan_converts_driver_values_into_arrow() -> None:
    full = (
        1,
        "alice",
        10.5,
        3,
        dt.date(2024, 1, 1),
        1,
        Decimal("1.1"),
        b"\x01",
        dt.datetime(2024, 1, 1, 10, 30),
        dt.datetime(2024, 1, 1, 12, 0, 0, 5),
        b"\x00\xff",
        '{"a": 1}',
        "a",
        18446744073709551615,
        2024,
        dt.timedelta(hours=27, minutes=5, seconds=6, microseconds=123456),
        b"\x05",
        1.5,
        7,
        b"\x00\x01",
    )
    truthy = (2, "bob", 0.0, 0, dt.date(2024, 1, 2), 5, Decimal("2"), b"\x00") + (None,) * 12
    nulls = (3, None, None, None, "0000-00-00", None, None, None, "0000-00-00 00:00:00") + (
        None,
    ) * 11
    connection = FakeConnection(rows=[full, truthy, nulls])

    table = _table(make_source(connection).scan(PushedOperations(), {}, batch_size=10))

    assert table.schema.field("updated").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("created").type == pa.timestamp("us")
    rows = table.to_pylist()
    assert rows[0] == {
        "id": 1,
        "name": "alice",
        "amount": 10.5,
        "qty": 3,
        "day": dt.date(2024, 1, 1),
        "active": True,
        "price": Decimal("1.10"),
        "flag": True,
        "created": dt.datetime(2024, 1, 1, 10, 30),
        "updated": dt.datetime(2024, 1, 1, 12, 0, 0, 5, tzinfo=UTC),
        "payload": b"\x00\xff",
        "doc": '{"a": 1}',
        "kind": "a",
        "big": Decimal(18446744073709551615),
        "y": 2024,
        "t": "27:05:06.123",
        "mask": 5,
        "f": 1.5,
        "tiny": 7,
        "shape": b"\x00\x01",
    }
    assert (
        rows[1]["active"] is True
    )  # tinyint(1) holding 5 is TRUE, as the predicate rewrite assumes
    assert rows[1]["flag"] is False
    assert rows[1]["price"] == Decimal("2.00")
    assert rows[2] == {"id": 3, **{c[0]: None for c in COLUMNS[1:]}}  # zero dates read as NULL


def test_time_values_render_like_mysql() -> None:
    _, convert = map_column_type("time", "time(6)", None, None, 6)
    assert convert(dt.timedelta(hours=1, minutes=2, seconds=3)) == "01:02:03.000000"
    assert convert(-dt.timedelta(hours=100, seconds=1)) == "-100:00:01.000000"
    _, plain = map_column_type("time", "time", None, None, 0)
    assert plain(dt.timedelta(seconds=59)) == "00:00:59"
    assert plain("12:00:00") == "12:00:00"


# -- lifecycle -----------------------------------------------------------------------


def test_owned_connections_are_released_per_scan(connections: list[FakeConnection]) -> None:
    source = make_source(ssl=True, connect_timeout=3)
    schema = source.schema()
    assert len(connections) == 1
    primary = connections[0]
    kwargs = primary.kwargs  # type: ignore[attr-defined]
    assert kwargs["host"] == "db.internal"
    assert kwargs["port"] == 3307
    assert kwargs["user"] == "analyst"
    assert kwargs["password"] == PASSWORD
    assert kwargs["database"] == "shop"
    assert kwargs["charset"] == "utf8mb4"
    assert kwargs["connect_timeout"] == 3
    assert kwargs["ssl"] == {"check_hostname": False}
    assert kwargs["ssl_disabled"] is False
    assert kwargs["autocommit"] is True
    assert kwargs["binary_prefix"] is True

    connections[0].rows = []
    stream = source.scan(PushedOperations(projection=("id", "name")), {}, batch_size=10)
    assert len(connections) == 2
    scan_connection = connections[1]
    scan_connection.rows = [(1, "alice")]
    assert [sql for sql, _ in scan_connection.executed][:2] == [
        SESSION_TIME_ZONE_SQL,
        SESSION_SQL_MODE_SQL,
    ]
    assert scan_connection.scan_statements == [(f"SELECT `id`, `name` FROM {RELATION}", None)]
    assert not scan_connection.closed

    list(stream)  # exhausting the stream closes it
    assert stream.closed
    assert scan_connection.closed
    assert not primary.closed

    early = source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)
    assert not connections[2].closed
    early.close()
    assert connections[2].closed
    early.close()  # idempotent

    live = source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)
    source.close()
    assert live.closed
    assert connections[3].closed
    assert primary.closed

    with pytest.raises(SourceError) as excinfo:
        source.scan(PushedOperations(), {}, batch_size=10)
    assert excinfo.value.code is DiagnosticCode.SOURCE_FAILURE
    assert source.schema() is schema  # cached metadata stays readable; no I/O happens
    source.close()  # idempotent


def test_injected_connection_is_shared_and_never_closed() -> None:
    connection = FakeConnection(rows=[(1, "alice")])
    with make_source(connection) as source:
        stream = source.scan(PushedOperations(projection=("id", "name")), {}, batch_size=10)
        scan_cursor = connection.cursors[-1]
        assert _table(stream).num_rows == 1
        assert scan_cursor.closed
        assert not connection.closed
        # the session was initialised once, on first use
        assert [sql for sql, _ in connection.executed].count(SESSION_TIME_ZONE_SQL) == 1
    assert not connection.closed


def test_injected_connection_rejects_concurrent_scans() -> None:
    connection = FakeConnection(rows=[(1,)])
    source = make_source(connection)
    active = source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)

    with pytest.raises(SourceError, match="cannot run concurrent scans") as excinfo:
        source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)

    assert excinfo.value.code is DiagnosticCode.SOURCE_FAILURE
    assert len(connection.scan_statements) == 1
    active.close()

    replacement = source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)
    replacement.close()


def test_ssl_false_is_forwarded_as_an_explicit_disable(connections: list[FakeConnection]) -> None:
    make_source(ssl=False).schema()

    kwargs = connections[0].kwargs  # type: ignore[attr-defined]
    assert kwargs["ssl"] is False
    assert kwargs["ssl_disabled"] is True


def test_scan_execution_failure_is_wrapped_and_released(connections: list[FakeConnection]) -> None:
    source = make_source()
    source.schema()
    connections_before = len(connections)

    def connect(**kwargs: Any) -> FakeConnection:
        connection = FakeConnection(
            fail_scan=pymysql.err.OperationalError(
                1146, f"Table 'shop.orders' doesn't exist ({PASSWORD})"
            )
        )
        connections.append(connection)
        return connection

    mysql_module.pymysql.connect = connect  # type: ignore[assignment]  # restored by the fixture
    with pytest.raises(SourceError) as excinfo:
        source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)
    assert excinfo.value.code is DiagnosticCode.SOURCE_FAILURE
    assert "doesn't exist" in str(excinfo.value)
    assert PASSWORD not in str(excinfo.value)
    assert dict(excinfo.value.diagnostic.details)["sql"] == f"SELECT `id` FROM {RELATION}"
    assert len(connections) == connections_before + 1
    assert connections[-1].closed


def test_streaming_failure_is_wrapped() -> None:
    class BrokenCursor(FakeCursor):
        def fetchmany(self, size: int = 1) -> Any:
            raise pymysql.err.OperationalError(2013, "Lost connection to MySQL server")

    class BrokenConnection(FakeConnection):
        def cursor(self, cursorclass: Any = None) -> FakeCursor:
            if cursorclass is None:
                return super().cursor()
            cursor = BrokenCursor(self)
            self.cursors.append(cursor)
            return cursor

    stream = make_source(BrokenConnection()).scan(
        PushedOperations(projection=("id",)), {}, batch_size=10
    )
    with pytest.raises(SourceError) as excinfo:
        list(stream)
    assert "Lost connection" in str(excinfo.value)
    assert stream.closed


# -- redaction -------------------------------------------------------------------------


def test_password_never_appears_in_repr_or_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_connect(**kwargs: Any) -> Any:
        raise pymysql.err.OperationalError(
            1045,
            f"Access denied for user 'analyst'@'db.internal' (using password: {PASSWORD}); "
            f"password={PASSWORD}",
        )

    monkeypatch.setattr(mysql_module.pymysql, "connect", failing_connect)
    source = make_source()

    assert PASSWORD not in repr(source)
    assert PASSWORD not in str(source)
    assert repr(source) == (
        "MySQLSource(name='orders', host='db.internal', port=3307, database='shop', table='orders')"
    )
    relation = source.relation()
    assert PASSWORD not in repr(relation)
    assert PASSWORD not in str(relation.secrets)
    assert PASSWORD not in json.dumps(relation.to_dict())

    with pytest.raises(SourceError) as excinfo:
        source.schema()
    assert excinfo.value.code is DiagnosticCode.SOURCE_FAILURE
    text = str(excinfo.value)
    assert PASSWORD not in text
    assert REDACTED in text
    assert "Access denied" in text
    assert PASSWORD not in json.dumps(excinfo.value.diagnostic.to_dict())
    assert excinfo.value.__cause__ is None  # provider exception (and its message) not chained


def test_constructor_validates_required_options() -> None:
    with pytest.raises(ValueError):
        MySQLSource("", host="h", database="d", table="t", user="u")
    with pytest.raises(ValueError):
        MySQLSource("s", host="", database="d", table="t", user="u")
    with pytest.raises(ValueError):
        MySQLSource("s", host="h", database="", table="t", user="u")
    with pytest.raises(ValueError):
        MySQLSource("s", host="h", database="d", table="", user="u")
    with pytest.raises(ValueError):
        MySQLSource("s", host="h", database="d", table="t", user="")
    with pytest.raises(ValueError, match="charset must be 'utf8mb4'"):
        MySQLSource("s", host="h", database="d", table="t", user="u", charset="latin1")
    with pytest.raises(ValueError, match="port must be positive"):
        MySQLSource("s", host="h", port=0, database="d", table="t", user="u")
    with pytest.raises(ValueError, match="connect_timeout must be positive"):
        MySQLSource("s", host="h", database="d", table="t", user="u", connect_timeout=0)


# -- through the facade and the DuckDB engine -----------------------------------------

FACADE_SQL = "SELECT id, name FROM mysql_orders WHERE name = 'alice' LIMIT 3"
FACADE_SCAN_SQL = (
    f"SELECT `id`, `name` FROM {RELATION} WHERE "
    "(CAST(CONVERT(`name` USING utf8mb4) AS BINARY) = CAST(%s AS BINARY)) LIMIT %s"
)
RESIDUAL_SQL = "SELECT id, amount / 4 AS quarter FROM mysql_orders WHERE amount > :min"


def facade_source(connection: FakeConnection) -> MySQLSource:
    return MySQLSource(
        "mysql_orders",
        host="db.internal",
        database="shop",
        table="orders",
        user="analyst",
        password=PASSWORD,
        connection=connection,
    )


def test_context_plans_full_pushdown_and_the_scan_executes_it() -> None:
    connection = FakeConnection(rows=[(1, "alice")])
    source = facade_source(connection)
    with iql.Context() as ctx:
        ctx.register_source(source)
        query = ctx.sql(FACADE_SQL)
        assert query.is_portable("duckdb")
        plan = query.execution_plan("duckdb")
        assert {node.disposition for node in plan.explain.nodes} == {Disposition.PUSHED}
        assert plan.pushed.projection == ("id", "name")
        assert plan.pushed.limit == 3
        assert plan.residual.is_empty
        # exactly the call the DuckDB engine makes for a native relation
        table = _table(source.scan(plan.pushed, {}, batch_size=10))
    assert table.to_pylist() == [{"id": 1, "name": "alice"}]
    assert connection.scan_statements[-1] == (FACADE_SCAN_SQL, ("alice", 3))
    assert connection.cursors[-1].closed
    assert not connection.closed


def test_context_keeps_computed_projection_residual_and_pushes_the_rest() -> None:
    connection = FakeConnection(rows=[(1, 10.5), (2, 20.0)])
    source = facade_source(connection)
    with iql.Context() as ctx:
        ctx.register_source(source)
        plan = ctx.sql(RESIDUAL_SQL).execution_plan("duckdb")
        assert plan.pushed.projection == ("id", "amount")
        assert plan.pushed.limit is None
        assert plan.residual.projection is not None
        assert plan.residual.predicate is None
        table = _table(source.scan(plan.pushed, {"min": Literal.of(5)}, batch_size=10))
    assert table.to_pylist() == [{"id": 1, "amount": 10.5}, {"id": 2, "amount": 20.0}]
    assert connection.scan_statements[-1] == (
        f"SELECT `id`, `amount` FROM {RELATION} WHERE (`amount` > %s)",
        (5,),
    )


def test_context_executes_end_to_end_through_duckdb() -> None:
    connection = FakeConnection(rows=[(1, "alice")])
    with iql.Context() as ctx:
        ctx.register_source(facade_source(connection))
        table = _table(ctx.sql(FACADE_SQL).execute())
        assert table.to_pylist() == [{"id": 1, "name": "alice"}]
        assert connection.scan_statements[-1] == (FACADE_SCAN_SQL, ("alice", 3))

        connection.rows = [(1, 10.5), (2, 20.0)]
        table = _table(ctx.sql(RESIDUAL_SQL).execute(params={"min": 5}))
        assert table.to_pylist() == [{"id": 1, "quarter": 2.625}, {"id": 2, "quarter": 5.0}]
    assert connection.cursors[-1].closed
    assert not connection.closed


def test_context_keeps_mysql_integer_arithmetic_residual_for_overflow_to_null() -> None:
    maximum = 2**63 - 1
    connection = FakeConnection(rows=[(maximum,), (1,)])
    with iql.Context() as ctx:
        ctx.register_source(facade_source(connection))
        query = ctx.sql("SELECT id FROM mysql_orders WHERE id + 1 IS NULL")
        plan = query.execution_plan("duckdb")
        assert plan.pushed.predicate is None
        assert plan.residual.predicate is not None
        assert _table(query.execute()).to_pylist() == [{"id": maximum}]

    assert connection.scan_statements[-1] == (f"SELECT `id` FROM {RELATION}", None)


# -- live integration (INVARIANTQL_MYSQL_DSN) ------------------------------------------

LIVE_CORPUS = [
    "SELECT * FROM {src}",
    "SELECT id, name FROM {src} WHERE name IS NOT NULL",
    "SELECT id FROM {src} WHERE name IS NULL",
    "SELECT id, amount * 2 AS twice, qty + 1 AS q1, amount - qty AS diff, amount / 4 AS quarter FROM {src}",
    "SELECT id FROM {src} WHERE amount > 7 AND amount <= 100",
    "SELECT id FROM {src} WHERE name = 'alice'",
    "SELECT id FROM {src} WHERE name = 'alice' OR name = 'Alice'",
    "SELECT id FROM {src} WHERE name <> 'alice'",
    "SELECT id FROM {src} WHERE name > 'b'",
    "SELECT id FROM {src} WHERE NOT (qty > 2)",
    "SELECT id FROM {src} WHERE name LIKE 'a%'",
    "SELECT id FROM {src} WHERE name LIKE 'A%'",
    "SELECT id FROM {src} WHERE name LIKE '%a%'",
    "SELECT id FROM {src} WHERE name LIKE '_ob'",
    "SELECT id FROM {src} WHERE name NOT LIKE 'a%'",
    "SELECT id FROM {src} WHERE id IN (1, 3, 99)",
    "SELECT id FROM {src} WHERE name IN ('Alice')",
    "SELECT id FROM {src} WHERE name NOT IN ('alice', 'bob')",
    "SELECT id FROM {src} WHERE qty BETWEEN 1 AND 3",
    "SELECT id FROM {src} WHERE day >= DATE '2024-01-03'",
    "SELECT id FROM {src} WHERE active = TRUE",
    "SELECT id FROM {src} WHERE active <> TRUE",
    "SELECT id FROM {src} WHERE price > 2",
    "SELECT id FROM {src} WHERE qty / 2 >= 1",
    "SELECT id FROM {src} WHERE qty / 3 > 0.33333",
    "SELECT id FROM {src} WHERE amount > :min AND name LIKE :pat",
    "SELECT id FROM {src} WHERE id = 4 LIMIT 5",
    "SELECT id FROM {src} WHERE id > 100 LIMIT 1",
    "SELECT id, name FROM {src} LIMIT 2",
]
LIVE_PARAMS = {"min": 5, "pat": "%a%"}


def _live_dsn() -> dict[str, Any]:
    raw = os.environ.get("INVARIANTQL_MYSQL_DSN")
    if not raw:
        pytest.skip(
            "set INVARIANTQL_MYSQL_DSN=mysql://user:pass@host:port/db to run MySQL integration tests"
        )
    parts = urlsplit(raw)
    if parts.scheme not in ("mysql", "mariadb"):
        pytest.skip("INVARIANTQL_MYSQL_DSN must be a mysql://user:pass@host:port/db URL")
    return {
        "host": parts.hostname or "localhost",
        "port": parts.port or 3306,
        "user": unquote(parts.username or ""),
        "password": unquote(parts.password) if parts.password else None,
        "database": parts.path.lstrip("/"),
    }


_LIVE_DDL = (
    "CREATE TABLE `{table}` ("
    "`id` BIGINT NOT NULL PRIMARY KEY, `name` VARCHAR(64) NULL, `amount` DOUBLE NULL, "
    "`qty` INT NULL, `day` DATE NULL, `active` BOOLEAN NULL, `price` DECIMAL(10,2) NULL"
    ") CHARACTER SET utf8mb4"
)


def _live_connect(dsn: dict[str, Any]) -> Any:
    return pymysql.connect(
        host=dsn["host"],
        port=dsn["port"],
        user=dsn["user"],
        password=dsn["password"],
        database=dsn["database"],
        charset="utf8mb4",
        autocommit=True,
    )


def _create_live_table(rows: list[dict[str, Any]]) -> Iterator[tuple[dict[str, Any], str]]:
    dsn = _live_dsn()
    table = f"iql_orders_{uuid.uuid4().hex[:12]}"
    connection = _live_connect(dsn)
    try:
        with connection.cursor() as cursor:
            cursor.execute(_LIVE_DDL.format(table=table))
            cursor.executemany(
                f"INSERT INTO `{table}` (`id`, `name`, `amount`, `qty`, `day`, `active`, `price`) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                [
                    (r["id"], r["name"], r["amount"], r["qty"], r["day"], r["active"], r["price"])
                    for r in rows
                ],
            )
        yield dsn, table
    finally:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
        finally:
            connection.close()


@pytest.fixture()
def live_table(sample_rows) -> Iterator[tuple[dict[str, Any], str]]:
    yield from _create_live_table(sample_rows)


@pytest.fixture()
def live_like_table() -> Iterator[tuple[dict[str, Any], str]]:
    rows = [
        {
            "id": 7,
            "name": "a\\c",
            "amount": 1.0,
            "qty": 1,
            "day": None,
            "active": True,
            "price": None,
        },
        {
            "id": 8,
            "name": "élan",
            "amount": 1.0,
            "qty": 1,
            "day": None,
            "active": True,
            "price": None,
        },
        {
            "id": 9,
            "name": "a%c",
            "amount": 1.0,
            "qty": 1,
            "day": None,
            "active": True,
            "price": None,
        },
        {
            "id": 10,
            "name": "abc ",
            "amount": 1.0,
            "qty": 1,
            "day": None,
            "active": True,
            "price": None,
        },
    ]
    yield from _create_live_table(rows)


def _live_source(dsn: dict[str, Any], table: str, name: str = "mysql_orders") -> MySQLSource:
    return MySQLSource(name, table=table, **dsn)


def _normalise(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 9)
    if isinstance(value, Decimal):
        return Decimal(value).quantize(Decimal("0.000000001"))
    return value


def _rows(records: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    out = [tuple(_normalise(v) for v in r.values()) for r in records]
    return sorted(out, key=lambda t: tuple((x is None, str(x)) for x in t))


@pytest.mark.integration
def test_live_schema_and_full_pushdown(live_table: tuple[dict[str, Any], str]) -> None:
    dsn, table = live_table
    with iql.Context() as ctx:
        source = ctx.register_source(_live_source(dsn, table))
        assert source.schema().to_dict() == {
            "fields": [
                {"name": "id", "type": {"kind": "integer", "bits": 64}, "nullable": False},
                {"name": "name", "type": {"kind": "string"}, "nullable": True},
                {"name": "amount", "type": {"kind": "float", "bits": 64}, "nullable": True},
                {"name": "qty", "type": {"kind": "integer", "bits": 32}, "nullable": True},
                {"name": "day", "type": {"kind": "date"}, "nullable": True},
                {"name": "active", "type": {"kind": "boolean"}, "nullable": True},
                {
                    "name": "price",
                    "type": {"kind": "decimal", "precision": 10, "scale": 2},
                    "nullable": True,
                },
            ]
        }
        query = ctx.sql(
            "SELECT id, name FROM mysql_orders WHERE name LIKE 'a%' AND qty > 1 LIMIT 10"
        )
        plan = query.execution_plan("duckdb")
        assert plan.executable
        assert {node.disposition for node in plan.explain.nodes} == {Disposition.PUSHED}
        assert _table(query.execute()).to_pylist() == [{"id": 1, "name": "alice"}]


@pytest.mark.integration
@pytest.mark.parametrize("sql", LIVE_CORPUS)
def test_live_results_match_duckdb_over_parquet(
    ctx, live_table: tuple[dict[str, Any], str], sql: str
) -> None:
    dsn, table = live_table
    ctx.register_source(_live_source(dsn, table))
    via_mysql = ctx.sql(sql.format(src="mysql_orders"))
    reference = ctx.sql(sql.format(src="orders"))
    params = {k: v for k, v in LIVE_PARAMS.items() if k in via_mysql.parameters}
    assert via_mysql.is_portable("duckdb")

    actual = _table(via_mysql.execute(params=params))
    expected = _table(reference.execute(params=params))
    assert actual.schema.names == expected.schema.names
    actual_rows, expected_rows = _rows(actual.to_pylist()), _rows(expected.to_pylist())
    if "LIMIT" in sql and "WHERE" not in sql:
        assert len(actual_rows) == len(
            expected_rows
        )  # which rows a bare LIMIT picks is unspecified
        return
    assert len(actual_rows) == len(expected_rows), (actual_rows, expected_rows)
    for a, b in zip(actual_rows, expected_rows, strict=True):
        for x, y in zip(a, b, strict=True):
            if isinstance(x, float) and isinstance(y, float):
                assert math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-9), (a, b)
            else:
                assert x == y, (a, b)


@pytest.mark.integration
def test_live_like_is_character_wise_case_sensitive_and_escape_free(
    live_like_table: tuple[dict[str, Any], str],
) -> None:
    dsn, table = live_like_table
    with iql.Context() as ctx:
        ctx.register_source(_live_source(dsn, table, name="names"))

        def ids(predicate: Any) -> list[int]:
            query = ctx.query("names").select("id").where(predicate)
            assert query.is_portable("duckdb")
            return sorted(r["id"] for r in _table(query.execute()).to_pylist())

        assert ids(iql.col("name").like("a\\%")) == [7]  # backslash is literal, % wildcard
        assert ids(iql.col("name").like("a%c")) == [7, 9]
        assert ids(iql.col("name").like("_lan")) == [8]  # _ matches one multibyte character
        assert ids(iql.col("name").like("__lan")) == []
        assert ids(iql.col("name").like("A%")) == []  # case-sensitive
        assert ids(iql.col("name") == "abc") == []  # trailing space is significant
        assert ids(iql.col("name") == "abc ") == [10]
        assert ids(iql.col("name") == "ÉLAN") == []


@pytest.mark.integration
def test_live_stream_close_releases_the_connection(live_table: tuple[dict[str, Any], str]) -> None:
    dsn, table = live_table
    source = _live_source(dsn, table)
    try:
        stream = source.scan(PushedOperations(projection=("id",)), {}, batch_size=2)
        first = next(iter(stream))
        assert first.num_rows == 2
        stream.close()
        assert stream.closed
        # the source is still usable afterwards
        assert (
            _table(source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)).num_rows
            == 6
        )
    finally:
        source.close()
    with pytest.raises(SourceError):
        source.scan(PushedOperations(), {}, batch_size=10)
