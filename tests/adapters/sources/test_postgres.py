"""PostgresSource: query generation, type mapping, redaction, lifecycle, and a gated live test."""

from __future__ import annotations

import datetime as dt
import os
import uuid
from decimal import Decimal
from typing import Any

import psycopg
import pyarrow as pa
import pytest

from invariantql.adapters.sources import postgres as pg_module
from invariantql.adapters.sources.postgres import (
    PostgresSource,
    PostgresSqlGenerator,
    data_type_from_pg,
    schema_from_information_schema,
)
from invariantql.domain.capabilities import Support
from invariantql.domain.credentials import REDACTED
from invariantql.domain.diagnostics import DiagnosticCode, SourceError
from invariantql.domain.execution import PushedOperations
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
    ListType,
    StringType,
    TimestampType,
    UnknownType,
)
from invariantql.ports.source import DataSource, NativeRelation

PASSWORD = "s3cret-P@ssw0rd-xyz"

# (column_name, data_type, udt_name, numeric_precision, numeric_scale, is_nullable)
INFO_ROWS: list[tuple[Any, ...]] = [
    ("id", "bigint", "int8", 64, 0, "NO"),
    ("small", "smallint", "int2", 16, 0, "YES"),
    ("qty", "integer", "int4", 32, 0, "YES"),
    ("ratio", "real", "float4", 24, None, "YES"),
    ("amount", "double precision", "float8", 53, None, "YES"),
    ("price", "numeric", "numeric", 10, 2, "YES"),
    ("free_num", "numeric", "numeric", None, None, "YES"),
    ("active", "boolean", "bool", None, None, "YES"),
    ("name", "text", "text", None, None, "YES"),
    ("code", "character varying", "varchar", None, None, "YES"),
    ("fixed", "character", "bpchar", None, None, "YES"),
    ("uid", "uuid", "uuid", None, None, "YES"),
    ("payload", "jsonb", "jsonb", None, None, "YES"),
    ("doc", "json", "json", None, None, "YES"),
    ("blob", "bytea", "bytea", None, None, "YES"),
    ("day", "date", "date", None, None, "YES"),
    ("created", "timestamp without time zone", "timestamp", None, None, "YES"),
    ("updated", "timestamp with time zone", "timestamptz", None, None, "YES"),
    ("tags", "ARRAY", "_int4", None, None, "YES"),
    ("labels", "ARRAY", "_text", None, None, "YES"),
    ("addr", "inet", "inet", None, None, "YES"),
]


# -- fakes ----------------------------------------------------------------------


class FakeCursor:
    def __init__(
        self,
        connection: FakeConnection,
        name: str,
        rows: list[tuple[Any, ...]],
        *,
        fail: BaseException | None = None,
    ) -> None:
        self.connection = connection
        self.name = name
        self.rows = rows
        self.executed: list[tuple[str, list[Any] | None]] = []
        self.itersize: int | None = None
        self.closed = False
        self.fetched = 0
        self._fail = fail

    def execute(self, sql: str, params: Any = None) -> FakeCursor:
        self.executed.append((sql, None if params is None else list(params)))
        if self._fail is not None:
            raise self._fail
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self.rows)

    def __iter__(self):
        for row in self.rows:
            if self.closed:
                raise psycopg.InterfaceError("the cursor is closed")
            self.fetched += 1
            yield row

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class FakeConnection:
    """Records cursors: client-side (unnamed) ones serve the schema query, named ones the scan."""

    def __init__(
        self,
        *,
        schema_rows: list[tuple[Any, ...]] | None = None,
        scan_rows: list[tuple[Any, ...]] | None = None,
        autocommit: bool = False,
        scan_failure: BaseException | None = None,
    ) -> None:
        self.schema_rows = list(INFO_ROWS if schema_rows is None else schema_rows)
        self.scan_rows = list(scan_rows or [])
        self.autocommit = autocommit
        self.scan_failure = scan_failure
        self.cursors: list[FakeCursor] = []
        self.cursor_kwargs: list[dict[str, Any]] = []
        self.closed = False
        self.rollbacks = 0

    def cursor(self, name: str = "", **kwargs: Any) -> FakeCursor:
        rows = self.scan_rows if name else self.schema_rows
        failure = self.scan_failure if name else None
        cursor = FakeCursor(self, name, rows, fail=failure)
        self.cursors.append(cursor)
        self.cursor_kwargs.append({"name": name, **kwargs})
        return cursor

    @property
    def named_cursors(self) -> list[FakeCursor]:
        return [c for c in self.cursors if c.name]

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def make_source(connection: Any = None, **overrides: Any) -> PostgresSource:
    options: dict[str, Any] = {
        "host": "db.example.internal",
        "port": 5433,
        "database": "shop",
        "table": "orders",
        "user": "reporting_user",
        "password": PASSWORD,
        "connection": connection,
    }
    options.update(overrides)
    return PostgresSource("orders_pg", **options)


@pytest.fixture()
def owned(monkeypatch: pytest.MonkeyPatch) -> tuple[list[dict[str, Any]], list[FakeConnection]]:
    """Patch ``psycopg.connect`` so the source's own connections are fakes."""

    calls: list[dict[str, Any]] = []
    connections: list[FakeConnection] = []

    def fake_connect(**kwargs: Any) -> FakeConnection:
        calls.append(dict(kwargs))
        connection = FakeConnection(scan_rows=[(1, "alice", 10.5)])
        connections.append(connection)
        return connection

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    return calls, connections


# -- construction and identity ---------------------------------------------------


def test_implements_the_data_source_port() -> None:
    source = make_source(FakeConnection())
    assert isinstance(source, DataSource)
    assert source.name == "orders_pg"


def test_constructor_rejects_empty_identifiers() -> None:
    with pytest.raises(ValueError):
        make_source(FakeConnection(), table="")
    with pytest.raises(ValueError):
        make_source(FakeConnection(), schema="")
    with pytest.raises(ValueError):
        PostgresSource("", host="h", database="d", table="t", user="u")


def test_construction_does_not_connect(owned) -> None:
    calls, _ = owned
    make_source()
    assert calls == []


def test_relation_sql_quotes_identifiers() -> None:
    source = make_source(FakeConnection(), schema='we"ird', table="Orders")
    assert source.relation_sql == '"we""ird"."Orders"'


# -- capabilities and relation -----------------------------------------------------


def test_capabilities_are_full_and_honest_about_evidence() -> None:
    caps = make_source(FakeConnection()).capabilities()
    assert caps.projection is Support.FULL
    assert caps.predicate is Support.FULL
    assert caps.limit is Support.FULL
    assert caps.expressions == ALL_EXPRESSION_KINDS - {ExpressionKind.ARITHMETIC}
    assert ExpressionKind.ARITHMETIC not in caps.expressions
    assert caps.parameters is True
    assert "PostgreSQL executes safe pushed SELECT operations natively" in caps.evidence


def test_relation_describes_the_jdbc_reader_without_secrets() -> None:
    relation = make_source(FakeConnection()).relation()
    assert isinstance(relation, NativeRelation)
    assert relation.kind == "jdbc:postgresql"
    assert relation.options == {
        "url": "jdbc:postgresql://db.example.internal:5433/shop",
        "dbtable": '"public"."orders"',
        "driver": "org.postgresql.Driver",
    }
    assert set(relation.secrets) == {"user", "password"}
    assert relation.secrets["password"] == REDACTED
    assert relation.secrets.reveal() == {"user": "reporting_user", "password": PASSWORD}
    assert relation.to_dict()["secrets"] == ["user", "password"]


def test_relation_includes_sslmode_only_when_set() -> None:
    relation = make_source(FakeConnection(), sslmode="require").relation()
    assert relation.options["sslmode"] == "require"
    assert "sslmode" not in make_source(FakeConnection()).relation().options


def test_relation_omits_password_secret_when_none() -> None:
    relation = make_source(FakeConnection(), password=None).relation()
    assert list(relation.secrets) == ["user"]


# -- schema ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("data_type", "udt", "precision", "scale", "expected"),
    [
        ("smallint", "int2", 16, 0, IntegerType(16)),
        ("integer", "int4", 32, 0, IntegerType(32)),
        ("bigint", "int8", 64, 0, IntegerType(64)),
        ("real", "float4", 24, None, FloatType(32)),
        ("double precision", "float8", 53, None, FloatType(64)),
        ("numeric", "numeric", 10, 2, DecimalType(10, 2)),
        ("numeric", "numeric", 12, 0, DecimalType(12, 0)),
        ("numeric", "numeric", None, None, DecimalType(38, 18)),
        ("numeric", "numeric", 200, 30, DecimalType(76, 30)),
        ("boolean", "bool", None, None, BooleanType()),
        ("text", "text", None, None, StringType()),
        ("character varying", "varchar", None, None, StringType()),
        ("character", "bpchar", None, None, StringType()),
        ("uuid", "uuid", None, None, StringType()),
        ("json", "json", None, None, StringType()),
        ("jsonb", "jsonb", None, None, StringType()),
        ("bytea", "bytea", None, None, BinaryType()),
        ("date", "date", None, None, DateType()),
        ("timestamp without time zone", "timestamp", None, None, TimestampType(None)),
        ("timestamp with time zone", "timestamptz", None, None, TimestampType("UTC")),
        ("ARRAY", "_int4", None, None, ListType(IntegerType(32))),
        ("ARRAY", "_timestamptz", None, None, ListType(TimestampType("UTC"))),
        ("ARRAY", "_numeric", None, None, ListType(DecimalType(38, 18))),
        ("inet", "inet", None, None, UnknownType()),
        ("USER-DEFINED", "mood", None, None, UnknownType()),
    ],
)
def test_type_mapping(data_type, udt, precision, scale, expected) -> None:
    assert data_type_from_pg(data_type, udt, precision, scale) == expected


def test_schema_from_information_schema_rows_preserves_order_and_nullability() -> None:
    schema = schema_from_information_schema(INFO_ROWS)
    assert schema.names == tuple(row[0] for row in INFO_ROWS)
    assert schema.field("id").nullable is False
    assert schema.field("name").nullable is True
    assert schema.field("price").data_type == DecimalType(10, 2)
    assert schema.field("tags").data_type == ListType(IntegerType(32))


def test_schema_queries_information_schema_with_bound_identifiers_and_caches() -> None:
    connection = FakeConnection()
    source = make_source(connection, schema="sales", table="orders")
    schema = source.schema()
    assert schema == schema_from_information_schema(INFO_ROWS)
    (cursor,) = connection.cursors
    assert cursor.name == ""
    ((sql, params),) = cursor.executed
    assert sql == (
        "SELECT column_name, data_type, udt_name, numeric_precision, numeric_scale, is_nullable "
        "FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s "
        "ORDER BY ordinal_position"
    )
    assert params == ["sales", "orders"]
    assert cursor.closed
    assert connection.rollbacks == 0  # a user's transaction is never touched
    assert source.schema() is schema
    assert len(connection.cursors) == 1


def test_schema_on_owned_connection_ends_the_transaction(owned) -> None:
    calls, connections = owned
    source = make_source()
    source.schema()
    (connection,) = connections
    assert connection.rollbacks == 1
    assert not connection.closed  # kept for the scan that usually follows
    assert calls[0] == {
        "host": "db.example.internal",
        "port": 5433,
        "dbname": "shop",
        "user": "reporting_user",
        "password": PASSWORD,
        "connect_timeout": 10,
        "application_name": "invariantql",
    }
    source.close()
    assert connection.closed


def test_schema_of_missing_relation_raises_schema_unavailable() -> None:
    source = make_source(FakeConnection(schema_rows=[]))
    with pytest.raises(SourceError) as info:
        source.schema()
    assert info.value.code is DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE
    assert '"public"."orders"' in str(info.value)


def test_connect_kwargs_forward_sslmode(owned) -> None:
    calls, _ = owned
    make_source(sslmode="verify-full", connect_timeout=3, application_name="etl").schema()
    assert calls[0]["sslmode"] == "verify-full"
    assert calls[0]["connect_timeout"] == 3
    assert calls[0]["application_name"] == "etl"


def test_connect_kwargs_omit_password_when_none(owned) -> None:
    calls, _ = owned
    make_source(password=None).schema()
    assert "password" not in calls[0]


# -- query generation ---------------------------------------------------------------


def representative_pushed() -> PushedOperations:
    predicate = And(
        (
            Comparison(ComparisonOp.GT, Column("amount"), Literal.of(5.0)),
            Comparison(ComparisonOp.EQ, Column("id"), Parameter("wanted")),
            IsNull(Column("name"), negated=True),
            In(Column("qty"), (Literal.of(1), Literal.of(2), Parameter("extra"))),
            Like(Column("name"), Literal.of("a%")),
            Comparison(
                ComparisonOp.GE,
                Arithmetic(ArithmeticOp.DIV, Column("amount"), Column("qty")),
                Literal.of(Decimal("2.5")),
            ),
        )
    )
    return PushedOperations(projection=("id", "name", "amount"), predicate=predicate, limit=10)


EXPECTED_SQL = (
    'SELECT "id", "name", "amount" FROM "public"."orders" WHERE '
    '(("amount" > %s) AND ("id" = %s) AND ("name" IS NOT NULL) AND ("qty" IN (%s, %s, %s)) '
    "AND (\"name\" LIKE %s ESCAPE '') "
    'AND ((CAST("amount" AS DOUBLE PRECISION) / '
    'NULLIF(CAST("qty" AS DOUBLE PRECISION), 0.0)) >= %s)) LIMIT %s'
)


def test_scan_executes_the_generated_select_on_a_server_side_cursor() -> None:
    connection = FakeConnection(scan_rows=[(1, "alice", 10.5), (2, "andy", 7.0)])
    source = make_source(connection)
    parameters = {"wanted": Literal.of(7), "extra": Literal.of(3)}

    stream = source.scan(representative_pushed(), parameters, batch_size=1)

    (cursor,) = connection.named_cursors
    assert cursor.name.startswith("invariantql_")
    assert connection.cursor_kwargs[-1] == {
        "name": cursor.name,
        "scrollable": False,
        "withhold": False,
    }
    assert cursor.itersize == 1
    ((sql, params),) = cursor.executed
    assert sql == EXPECTED_SQL
    assert params == [5.0, 7, 1, 2, 3, "a%", Decimal("2.5"), 10]

    assert stream.schema == pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("amount", pa.float64()),
        ]
    )
    batches = list(stream)
    assert [b.num_rows for b in batches] == [1, 1]
    assert pa.Table.from_batches(batches).to_pylist() == [
        {"id": 1, "name": "alice", "amount": 10.5},
        {"id": 2, "name": "andy", "amount": 7.0},
    ]


def test_scan_without_projection_selects_every_column_in_schema_order() -> None:
    connection = FakeConnection(schema_rows=INFO_ROWS[:3], scan_rows=[(1, 2, 3)])
    source = make_source(connection)
    stream = source.scan(PushedOperations(), {}, batch_size=100)
    (cursor,) = connection.named_cursors
    assert cursor.executed == [('SELECT * FROM "public"."orders"', [])]
    assert stream.schema.names == ["id", "small", "qty"]
    assert stream.read_all().to_pylist() == [{"id": 1, "small": 2, "qty": 3}]


def test_scan_uses_a_with_hold_cursor_on_autocommit_connections() -> None:
    connection = FakeConnection(schema_rows=INFO_ROWS[:1], autocommit=True)
    make_source(connection).scan(PushedOperations(projection=("id",)), {}, batch_size=10)
    assert connection.cursor_kwargs[-1]["withhold"] is True


def test_like_negation_or_and_not_render_with_placeholders() -> None:
    generator = PostgresSqlGenerator(pg_module.POSTGRES, {"p": Literal.of("x_")})
    predicate = Or(
        (
            Not(Like(Column("name"), Parameter("p"), negated=True)),
            Comparison(ComparisonOp.NE, Column("name"), Literal(None, StringType())),
            Comparison(ComparisonOp.EQ, Column("active"), Literal.of(True)),
        )
    )
    sql = generator.select('"s"."t"', predicate=predicate)
    assert sql == (
        'SELECT * FROM "s"."t" WHERE ((NOT ("name" NOT LIKE %s ESCAPE \'\')) '
        'OR ("name" <> NULL) OR ("active" = TRUE))'
    )
    assert generator.values == ["x_"]


def test_arithmetic_operands_are_typed_before_postgres_evaluates_them() -> None:
    generator = PostgresSqlGenerator(
        pg_module.POSTGRES,
        {"increment": Literal.of(1), "denominator": Literal.of(0)},
        schema=schema_from_information_schema(INFO_ROWS),
    )
    predicate = And(
        (
            Comparison(
                ComparisonOp.GT,
                Arithmetic(ArithmeticOp.ADD, Column("qty"), Parameter("increment")),
                Literal.of(0),
            ),
            Comparison(
                ComparisonOp.GT,
                Arithmetic(ArithmeticOp.ADD, Column("ratio"), Column("qty")),
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

    sql = generator.expression(predicate)

    assert 'CAST((CAST("qty" AS BIGINT) + CAST(%s AS BIGINT)) AS BIGINT)' in sql
    assert (
        'CAST((CAST("ratio" AS DOUBLE PRECISION) + '
        'CAST("qty" AS DOUBLE PRECISION)) AS DOUBLE PRECISION)'
    ) in sql
    assert (
        'CAST((CAST("price" AS NUMERIC(21,2)) * CAST("price" AS NUMERIC(21,2))) AS NUMERIC(21,4))'
    ) in sql
    assert ('CAST("amount" AS DOUBLE PRECISION) / NULLIF(CAST(%s AS DOUBLE PRECISION), 0.0)') in sql
    assert generator.values == [1, 0, 0.0, Decimal("0.00"), 0, 0.0]


def test_scan_rejects_unknown_projection_column() -> None:
    source = make_source(FakeConnection())
    with pytest.raises(SourceError) as info:
        source.scan(PushedOperations(projection=("nope",)), {}, batch_size=10)
    assert "nope" in str(info.value)


def test_context_keeps_postgres_integer_arithmetic_residual_for_overflow_to_null() -> None:
    import invariantql as iql

    maximum = 2**63 - 1
    connection = FakeConnection(schema_rows=INFO_ROWS[:1], scan_rows=[(maximum,), (1,)])
    with iql.Context() as ctx:
        ctx.register_source(make_source(connection))
        query = ctx.sql("SELECT id FROM orders_pg WHERE id + 1 IS NULL")
        plan = query.execution_plan("duckdb")
        assert plan.pushed.predicate is None
        assert plan.residual.predicate is not None
        assert query.execute().to_arrow().to_pylist() == [{"id": maximum}]

    assert connection.named_cursors[-1].executed == [('SELECT "id" FROM "public"."orders"', [])]


@pytest.mark.parametrize("batch_size", [0, -1, True, 1.5, "10", None])
def test_scan_rejects_invalid_batch_size(batch_size: Any) -> None:
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        make_source(FakeConnection()).scan(PushedOperations(), {}, batch_size=batch_size)


# -- value conversion ---------------------------------------------------------------


def test_scan_converts_driver_values_for_arrow() -> None:
    plus_two = dt.timezone(dt.timedelta(hours=2))
    uid = uuid.uuid4()
    row = (
        Decimal("12.34"),
        Decimal("0.123456789012345678"),
        dt.date(2024, 1, 2),
        dt.datetime(2024, 1, 2, 3, 4, 5),
        dt.datetime(2024, 1, 2, 12, 0, 0, tzinfo=plus_two),
        memoryview(b"\x00\x01"),
        uid,
        {"a": 1, "b": [1, 2]},
        [1, None, 3],
        ["x", None],
        "192.168.0.1/32",
    )
    projection = (
        "price",
        "free_num",
        "day",
        "created",
        "updated",
        "blob",
        "uid",
        "payload",
        "tags",
        "labels",
        "addr",
    )
    connection = FakeConnection(scan_rows=[row])
    stream = make_source(connection).scan(
        PushedOperations(projection=projection), {}, batch_size=10
    )
    assert stream.schema.field("updated").type == pa.timestamp("us", tz="UTC")
    assert stream.schema.field("free_num").type == pa.decimal128(38, 18)
    assert stream.schema.field("addr").type == pa.string()
    (record,) = stream.read_all().to_pylist()
    assert record["price"] == Decimal("12.34")
    assert record["free_num"] == Decimal("0.123456789012345678")
    assert record["day"] == dt.date(2024, 1, 2)
    assert record["created"] == dt.datetime(2024, 1, 2, 3, 4, 5)
    assert record["updated"].utcoffset() == dt.timedelta(0)
    assert record["updated"].replace(tzinfo=None) == dt.datetime(2024, 1, 2, 10, 0, 0)
    assert record["blob"] == b"\x00\x01"
    assert record["uid"] == str(uid)
    assert record["payload"] == '{"a": 1, "b": [1, 2]}'
    assert record["tags"] == [1, None, 3]
    assert record["labels"] == ["x", None]
    assert record["addr"] == "192.168.0.1/32"


def test_scan_passes_nulls_through() -> None:
    connection = FakeConnection(scan_rows=[(None, None, None)])
    stream = make_source(connection).scan(
        PushedOperations(projection=("name", "updated", "tags")), {}, batch_size=10
    )
    assert stream.read_all().to_pylist() == [{"name": None, "updated": None, "tags": None}]


# -- lifecycle ---------------------------------------------------------------------


def test_stream_close_releases_cursor_but_keeps_user_connection() -> None:
    connection = FakeConnection(scan_rows=[(1,), (2,), (3,)])
    source = make_source(connection)
    stream = source.scan(PushedOperations(projection=("id",)), {}, batch_size=1)
    (cursor,) = connection.named_cursors
    batches = iter(stream)  # keep the iterator alive: dropping it finalises the stream
    first = next(batches)
    assert first.num_rows == 1
    assert not cursor.closed
    stream.close()
    assert cursor.closed
    assert stream.closed
    assert not connection.closed
    assert list(stream) == []
    source.close()
    assert not connection.closed


def test_source_close_closes_an_active_cursor_but_not_a_user_connection() -> None:
    connection = FakeConnection(scan_rows=[(1,), (2,)])
    source = make_source(connection)
    stream = source.scan(PushedOperations(projection=("id",)), {}, batch_size=1)
    source.close()
    assert stream.closed
    assert connection.named_cursors[0].closed
    assert not connection.closed


def test_exhausting_the_stream_closes_the_cursor() -> None:
    connection = FakeConnection(scan_rows=[(1,), (2,)])
    stream = make_source(connection).scan(PushedOperations(projection=("id",)), {}, batch_size=10)
    assert stream.read_all().num_rows == 2
    assert connection.named_cursors[0].closed


def test_owned_connection_is_released_when_the_stream_closes_and_reopened_lazily(owned) -> None:
    calls, connections = owned
    source = make_source()
    stream = source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)
    assert len(calls) == 1
    assert not connections[0].closed
    stream.close()
    assert connections[0].named_cursors[0].closed
    assert connections[0].closed
    second = source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)
    assert len(calls) == 2
    second.close()
    assert connections[1].closed
    source.close()
    with pytest.raises(SourceError):
        source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)
    assert len(calls) == 2


def test_owned_concurrent_streams_use_independent_connections(owned) -> None:
    calls, connections = owned
    source = make_source()
    first = source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)
    second = source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)
    assert len(calls) == 2
    first.close()
    assert connections[0].closed
    assert not connections[1].closed
    second.close()
    assert connections[1].closed
    source.close()


def test_scan_failure_releases_resources_and_wraps_the_error(owned) -> None:
    _, connections = owned
    source = make_source()
    source.schema()
    connections[0].scan_failure = psycopg.errors.UndefinedTable("relation does not exist")
    with pytest.raises(SourceError) as info:
        source.scan(PushedOperations(projection=("id",)), {}, batch_size=10)
    assert info.value.code is DiagnosticCode.SOURCE_FAILURE
    assert info.value.diagnostic.target == "orders_pg"
    assert dict(info.value.diagnostic.details)["sql"] == 'SELECT "id" FROM "public"."orders"'
    assert info.value.__cause__ is None
    assert connections[0].named_cursors[0].closed
    assert connections[0].closed


def test_closed_user_connection_is_reported_not_reopened(owned) -> None:
    calls, _ = owned
    connection = FakeConnection()
    connection.closed = True
    with pytest.raises(SourceError):
        make_source(connection).schema()
    assert calls == []


def test_source_is_a_context_manager(owned) -> None:
    _, connections = owned
    with make_source() as source:
        source.schema()
    assert connections[0].closed


# -- redaction ----------------------------------------------------------------------


def test_repr_never_shows_credentials() -> None:
    source = make_source(FakeConnection())
    text = repr(source)
    assert PASSWORD not in text
    assert "reporting_user" not in text
    assert text == (
        "PostgresSource(name='orders_pg', host='db.example.internal', port=5433, "
        "database='shop', table='\"public\".\"orders\"')"
    )
    assert PASSWORD not in repr(source.relation())
    assert PASSWORD not in str(source.relation().secrets)


def test_connection_failure_is_a_redacted_source_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_connect(**kwargs: Any) -> Any:
        raise psycopg.OperationalError(
            f"connection failed: FATAL: password={PASSWORD} rejected for user reporting_user"
        )

    monkeypatch.setattr(psycopg, "connect", failing_connect)
    source = make_source()
    with pytest.raises(SourceError) as info:
        source.schema()
    text = str(info.value)
    assert PASSWORD not in text
    assert "connection failed" in text
    assert info.value.code is DiagnosticCode.SOURCE_FAILURE
    assert info.value.__cause__ is None


def test_schema_failure_is_a_redacted_source_error(owned) -> None:
    _, connections = owned
    source = make_source()

    class BrokenCursor(FakeCursor):
        def execute(self, sql: str, params: Any = None) -> FakeCursor:
            raise psycopg.OperationalError(f"server closed the connection; secret {PASSWORD}")

    def cursor(name: str = "", **kwargs: Any) -> FakeCursor:
        return BrokenCursor(connections[0], name, [])

    connection = psycopg.connect()  # the patched fake factory
    connection.cursor = cursor  # type: ignore[method-assign]
    source._connection = connection  # inject the broken fake
    with pytest.raises(SourceError) as info:
        source.schema()
    assert PASSWORD not in str(info.value)
    assert info.value.code is DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE
    assert connection.closed


def test_streaming_failure_is_a_redacted_source_error() -> None:
    class ExplodingCursor(FakeCursor):
        def __iter__(self):
            yield (1,)
            raise psycopg.OperationalError(f"lost connection; password={PASSWORD}")

    connection = FakeConnection()

    def cursor(name: str = "", **kwargs: Any) -> FakeCursor:
        made = ExplodingCursor(connection, name, connection.schema_rows)
        connection.cursors.append(made)
        return made

    connection.cursor = cursor  # type: ignore[method-assign]
    stream = make_source(connection).scan(PushedOperations(projection=("id",)), {}, batch_size=1)
    with pytest.raises(SourceError) as info:
        list(stream)
    assert PASSWORD not in str(info.value)
    assert connection.named_cursors[0].closed


# -- integration ---------------------------------------------------------------------


def _collect(stream: Any) -> list[dict[str, Any]]:
    return pa.Table.from_batches(list(stream), schema=stream.schema).to_pylist()


@pytest.mark.integration
def test_postgres_end_to_end_through_duckdb(sample_rows) -> None:
    dsn = os.environ.get("INVARIANTQL_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set INVARIANTQL_POSTGRES_DSN to run the live PostgreSQL test")

    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict
    from psycopg.types.json import Jsonb

    import invariantql as iql

    params = conninfo_to_dict(dsn)
    password = params.get("password")
    sslmode = params.get("sslmode")
    table = f"invariantql_it_{uuid.uuid4().hex[:8]}"
    ident = sql.Identifier(table)
    admin = psycopg.connect(dsn, autocommit=True)
    try:
        admin.execute(
            sql.SQL(
                "CREATE TABLE {} ("
                "id bigint NOT NULL, name text, amount double precision, qty integer, day date, "
                "active boolean, price numeric(10,2), created timestamptz, tags int4[], "
                "payload jsonb)"
            ).format(ident)
        )
        for row in sample_rows:
            admin.execute(
                sql.SQL("INSERT INTO {} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)").format(
                    ident
                ),
                (
                    row["id"],
                    row["name"],
                    row["amount"],
                    row["qty"],
                    row["day"],
                    row["active"],
                    row["price"],
                    dt.datetime(2024, 1, row["id"], 12, 0, tzinfo=dt.timezone.utc),
                    [row["id"], row["id"] * 10],
                    Jsonb({"id": row["id"]}),
                ),
            )

        with iql.Context() as ctx:
            ctx.register_source(
                iql.postgres_source(
                    "orders_pg",
                    host=str(params.get("host") or "localhost"),
                    port=int(params.get("port") or 5432),
                    database=str(params["dbname"]),
                    user=str(params["user"]),
                    password=None if password is None else str(password),
                    table=table,
                    sslmode=None if sslmode is None else str(sslmode),
                )
            )
            schema = ctx.source("orders_pg").schema()
            assert schema.field("id").data_type == IntegerType(64)
            assert schema.field("price").data_type == DecimalType(10, 2)
            assert schema.field("created").data_type == TimestampType("UTC")
            assert schema.field("tags").data_type == ListType(IntegerType(32))
            assert schema.field("payload").data_type == StringType()

            query = ctx.sql(
                "SELECT id, name, amount / qty AS ratio FROM orders_pg "
                "WHERE name LIKE 'a%' AND qty IS NOT NULL AND id IN (1, 2, 3, 6) "
                "AND amount > :minimum AND NOT (qty <> 3)"
            )
            plan = query.execution_plan()
            assert plan.pushed.predicate is not None
            assert plan.residual.predicate is None
            rows = _collect(query.execute(params={"minimum": 1}))
            assert rows == [{"id": 1, "name": "alice", "ratio": 3.5}]

            everything = _collect(ctx.sql("SELECT * FROM orders_pg").execute())
            by_id = {r["id"]: r for r in sorted(everything, key=lambda r: r["id"])}
            assert len(by_id) == len(sample_rows)
            assert by_id[1]["created"].utcoffset() == dt.timedelta(0)
            assert by_id[1]["tags"] == [1, 10]
            assert by_id[1]["payload"] == '{"id": 1}'
            assert by_id[3]["qty"] is None
            assert by_id[4]["price"] is None

            no_escape = _collect(
                ctx.sql(r"SELECT id FROM orders_pg WHERE name LIKE 'a\%'").execute()
            )
            assert no_escape == []
    finally:
        admin.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(ident))
        admin.close()
