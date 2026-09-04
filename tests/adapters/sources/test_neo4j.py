"""Neo4jSource: Cypher generation, schema mapping, redaction, streaming, and a gated live run."""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import uuid
from collections.abc import Callable, Iterator
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
import pytest
from neo4j import Driver
from neo4j.exceptions import AuthError, ClientError, ServiceUnavailable
from neo4j.graph import Graph, Node
from neo4j.spatial import CartesianPoint
from neo4j.time import Date, DateTime, Duration

from invariantql.adapters.sources import neo4j as neo4j_module
from invariantql.adapters.sources.neo4j import (
    PUSHED_EXPRESSIONS,
    SPARK_KIND,
    CypherGenerator,
    Neo4jSource,
    infer_value_type,
    like_to_regex,
    property_types_to_domain,
    strip_userinfo,
)
from invariantql.domain.capabilities import Support
from invariantql.domain.diagnostics import (
    DiagnosticCode,
    ParameterError,
    PlanValidationError,
    SourceError,
)
from invariantql.domain.execution import PushedOperations
from invariantql.domain.expressions import (
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
    Parameter,
)
from invariantql.domain.schema import Schema
from invariantql.domain.types import (
    BooleanType,
    DateType,
    FloatType,
    IntegerType,
    StringType,
    TimestampType,
)

if TYPE_CHECKING:
    from invariantql.ports.streams import RecordBatchStream

PASSWORD = "Sup3r-Secret-Pw!"
UTC = dt.timezone.utc

Responder = Callable[[str, dict[str, Any]], Any]


# -- fakes ----------------------------------------------------------------------


class FakeRecord:
    def __init__(self, data: dict[str, Any]) -> None:
        self._data = dict(data)

    def keys(self) -> list[str]:
        return list(self._data)

    def values(self) -> list[Any]:
        return list(self._data.values())

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def value(self, key: int | str = 0, default: Any = None) -> Any:
        if isinstance(key, int):
            values = self.values()
            return values[key] if key < len(values) else default
        return self._data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]


class FakeResult:
    def __init__(
        self,
        rows: list[dict[str, Any]],
        *,
        fail_after: int | None = None,
        error: BaseException | None = None,
    ) -> None:
        self._rows = rows
        self._fail_after = fail_after
        self._error = error

    def __iter__(self) -> Iterator[FakeRecord]:
        for index, row in enumerate(self._rows):
            if self._fail_after is not None and index >= self._fail_after:
                raise self._error or RuntimeError("stream broke")
            yield FakeRecord(row)


class FakeSession:
    def __init__(self, driver: FakeDriver, config: dict[str, Any]) -> None:
        self.driver = driver
        self.config = config
        self.closed = False

    def run(
        self, query: str, parameters: dict[str, Any] | None = None, **kwargs: Any
    ) -> FakeResult:
        params = dict(parameters or {})
        self.driver.calls.append((query, params))
        response = self.driver.responder(query, params)
        if isinstance(response, BaseException):
            raise response
        if isinstance(response, FakeResult):
            return response
        return FakeResult(_ordered(query, response))

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> FakeSession:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class FakeDriver:
    def __init__(self, responder: Responder | None = None) -> None:
        self.responder: Responder = responder or (lambda query, params: [])
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.sessions: list[FakeSession] = []
        self.closed = False

    def session(self, **config: Any) -> FakeSession:
        if self.closed:
            raise RuntimeError("driver is closed")
        session = FakeSession(self, config)
        self.sessions.append(session)
        return session

    def close(self) -> None:
        self.closed = True


_ALIAS = re.compile(r" AS `((?:[^`]|``)+)`")


def _ordered(query: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Order row values like the RETURN clause does (the real driver returns values in RETURN order)."""

    keys = [k.replace("``", "`") for k in _ALIAS.findall(query)]
    if not keys:
        return rows
    return [{key: row.get(key) for key in keys} for row in rows]


def make_source(driver: FakeDriver, *, schema: Schema | None = None, **options: Any) -> Neo4jSource:
    options.setdefault("uri", "neo4j://localhost:7687")
    options.setdefault("user", "neo4j")
    options.setdefault("password", PASSWORD)
    options.setdefault("label", "Person")
    return Neo4jSource("people", schema=schema, driver=cast(Driver, driver), **options)


def make_node(properties: dict[str, Any], *, element_id: str = "4:x:1") -> Node:
    return Node(Graph(), element_id, 1, n_labels=["Person"], properties=properties)


def _rows(stream: RecordBatchStream) -> list[dict[str, Any]]:
    """Materialise any record-batch stream (an ArrowStream or a DuckDB LocalResult)."""

    return pa.Table.from_batches(list(stream), stream.schema).to_pylist()


PEOPLE = Schema.of(
    ("name", StringType()),
    ("age", IntegerType(64)),
    ("email", StringType()),
    ("city", StringType()),
    ("score", FloatType(64)),
)


# -- Cypher generation ---------------------------------------------------------------


def test_representative_plan_renders_exact_cypher_with_bound_values() -> None:
    driver = FakeDriver()
    source = make_source(driver, schema=PEOPLE)
    pushed = PushedOperations(
        projection=("name", "age"),
        predicate=And(
            (
                Comparison(ComparisonOp.GT, Column("age"), Parameter("min_age")),
                IsNull(Column("email"), negated=True),
                In(Column("city"), (Literal.of("Paris"), Literal.of("Rome"))),
                Like(Column("name"), Literal.of("Al%_x.")),
            )
        ),
        limit=10,
    )

    stream = source.scan(pushed, {"min_age": Literal.of(30)}, batch_size=100)
    stream.close()

    assert driver.calls == [
        (
            "MATCH (n:`Person`) WHERE ((n.`age` > $p0) AND (n.`email` IS NOT NULL) "
            "AND (n.`city` IN [$p1, $p2]) AND (n.`name` =~ $p3)) "
            "RETURN n.`name` AS `name`, n.`age` AS `age` LIMIT $limit",
            {
                "p0": 30,
                "p1": "Paris",
                "p2": "Rome",
                "p3": "(?s)^Al.*.x\\.$(?!.)",
                "limit": 10,
            },
        )
    ]
    assert driver.sessions[0].config == {"default_access_mode": "READ", "fetch_size": 100}


def test_scan_without_projection_or_predicate_returns_every_column() -> None:
    driver = FakeDriver()
    source = make_source(driver, schema=PEOPLE, database="graphs")
    stream = source.scan(PushedOperations(), {}, batch_size=10)
    assert stream.schema.names == ["name", "age", "email", "city", "score"]
    stream.close()
    cypher, params = driver.calls[0]
    assert cypher == (
        "MATCH (n:`Person`) RETURN n.`name` AS `name`, n.`age` AS `age`, n.`email` AS `email`, "
        "n.`city` AS `city`, n.`score` AS `score`"
    )
    assert params == {}
    assert driver.sessions[0].config == {
        "default_access_mode": "READ",
        "database": "graphs",
        "fetch_size": 10,
    }


def test_identifiers_with_backticks_are_escaped() -> None:
    schema = Schema.of(
        ("we`ird", StringType()),
    )
    generator = CypherGenerator(schema=schema)
    text = generator.match("Odd`Label", columns=["we`ird"], predicate=IsNull(Column("we`ird")))
    assert (
        text == "MATCH (n:`Odd``Label`) WHERE (n.`we``ird` IS NULL) RETURN n.`we``ird` AS `we``ird`"
    )


@pytest.mark.parametrize(
    ("pattern", "regex"),
    [
        ("50%", "(?s)^50.*$(?!.)"),
        ("_b_", "(?s)^.b.$(?!.)"),
        ("a.b", "(?s)^a\\.b$(?!.)"),
        (
            "(x)|[y]{z}^$*+?\\",
            "(?s)^\\(x\\)\\|\\[y\\]\\{z\\}\\^\\$\\*\\+\\?\\\\$(?!.)",
        ),
        ("plain", "(?s)^plain$(?!.)"),
        ("", "(?s)^$(?!.)"),
    ],
)
def test_like_patterns_become_anchored_case_sensitive_regexes(pattern: str, regex: str) -> None:
    assert like_to_regex(pattern) == regex


def test_negations_and_boolean_composition() -> None:
    generator = CypherGenerator({"p": Literal.of("A%")})
    text = generator.expression(
        Not(
            And(
                (
                    Like(Column("name"), Parameter("p"), negated=True),
                    In(Column("age"), (Literal.of(1), Literal.of(None)), negated=True),
                )
            )
        )
    )
    assert text == "(NOT ((NOT (n.`name` =~ $p0)) AND (NOT (n.`age` IN [$p1, null]))))"
    assert generator.values == {"p0": "(?s)^A.*$(?!.)", "p1": 1}


def test_null_literal_is_inline_and_booleans_are_bound() -> None:
    generator = CypherGenerator()
    text = generator.expression(
        And(
            (
                Comparison(ComparisonOp.EQ, Column("x"), Literal.of(None)),
                Comparison(ComparisonOp.NE, Column("ok"), Literal.of(True)),
            )
        )
    )
    assert text == "((n.`x` = null) AND (n.`ok` <> $p0))"
    assert generator.values == {"p0": True}


def test_missing_parameter_is_a_parameter_error() -> None:
    generator = CypherGenerator({})
    with pytest.raises(ParameterError) as info:
        generator.expression(Comparison(ComparisonOp.EQ, Column("x"), Parameter("missing")))
    assert info.value.code is DiagnosticCode.PARAMETER_MISSING


def test_cypher_generator_rejects_arithmetic_that_capabilities_do_not_advertise() -> None:
    generator = CypherGenerator(schema=PEOPLE)
    with pytest.raises(ValueError, match="must be evaluated by the engine"):
        generator.expression(Arithmetic(ArithmeticOp.DIV, Column("score"), Literal.of(0)))


def test_non_string_like_pattern_is_rejected() -> None:
    generator = CypherGenerator({"p": Literal.of(5)})
    with pytest.raises(PlanValidationError):
        generator.expression(Like(Column("name"), Parameter("p")))


def test_decimal_values_bind_as_floats() -> None:
    generator = CypherGenerator()
    generator.expression(Comparison(ComparisonOp.EQ, Column("price"), Literal.of(Decimal("1.50"))))
    assert generator.values == {"p0": 1.5}
    assert isinstance(generator.values["p0"], float)


def test_datetime_values_adopt_the_compared_columns_zone_semantics() -> None:
    schema = Schema.of(("zoned", TimestampType("UTC")), ("local", TimestampType(None)))
    generator = CypherGenerator(schema=schema)
    naive = dt.datetime(2024, 1, 1, 12, 0)
    aware = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    generator.expression(
        And(
            (
                Comparison(ComparisonOp.GT, Column("zoned"), Literal.of(naive)),
                Comparison(ComparisonOp.LT, Literal.of(aware), Column("local")),
                In(Column("zoned"), (Literal.of(naive),)),
            )
        )
    )
    assert generator.values == {
        "p0": dt.datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
        "p1": dt.datetime(2024, 1, 1, 10, 0),
        "p2": dt.datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
    }


def test_empty_return_is_rejected() -> None:
    with pytest.raises(ValueError):
        CypherGenerator().match("Person", columns=[])


# -- schema discovery ------------------------------------------------------------------


def _procedure_rows() -> list[dict[str, Any]]:
    return [
        {"propertyName": "name", "propertyTypes": ["String"]},
        {"propertyName": "age", "propertyTypes": ["Long"]},
        {"propertyName": "score", "propertyTypes": ["Double"]},
        {"propertyName": "active", "propertyTypes": ["Boolean"]},
        {"propertyName": "born", "propertyTypes": ["Date"]},
        {"propertyName": "created", "propertyTypes": ["DateTime"]},
        {"propertyName": "local", "propertyTypes": ["LocalDateTime"]},
        {"propertyName": "span", "propertyTypes": ["Duration"]},
        {"propertyName": "where", "propertyTypes": ["Point"]},
        {"propertyName": "tags", "propertyTypes": ["StringArray"]},
        {"propertyName": "mixed", "propertyTypes": ["String", "Long"]},
        {"propertyName": "amount", "propertyTypes": ["Long"]},
        {"propertyName": "amount", "propertyTypes": ["Double"]},
        {"propertyName": None, "propertyTypes": None},
    ]


def test_schema_is_discovered_from_node_type_properties_and_cached() -> None:
    driver = FakeDriver(
        lambda query, params: _procedure_rows() if "nodeTypeProperties" in query else []
    )
    source = make_source(driver)

    schema = source.schema()

    assert schema == Schema.of(
        ("active", BooleanType()),
        ("age", IntegerType(64)),
        ("amount", FloatType(64)),
        ("born", DateType()),
        ("created", TimestampType("UTC")),
        ("local", TimestampType(None)),
        ("mixed", StringType()),
        ("name", StringType()),
        ("score", FloatType(64)),
        ("span", StringType()),
        ("tags", StringType()),
        ("where", StringType()),
    )
    cypher, params = driver.calls[0]
    assert "CALL db.schema.nodeTypeProperties()" in cypher
    assert "$label IN nodeLabels" in cypher
    assert params == {"label": "Person"}
    assert driver.sessions[0].closed
    assert source.schema() is schema
    assert len(driver.calls) == 1


def test_predicate_on_a_stringified_mixed_property_is_applied_before_limit_locally() -> None:
    def respond(query: str, params: dict[str, Any]) -> Any:
        if query.startswith("CALL db.schema.nodeTypeProperties"):
            return [
                {"propertyName": "name", "propertyTypes": ["String"]},
                {"propertyName": "mixed", "propertyTypes": ["String", "Long"]},
            ]
        return [
            {"name": "first", "mixed": 1},
            {"name": "second", "mixed": "one"},
        ]

    driver = FakeDriver(respond)
    source = make_source(driver)
    assert source.schema().field("mixed").data_type == StringType()
    stream = source.scan(
        PushedOperations(
            projection=("name",),
            predicate=Comparison(ComparisonOp.EQ, Column("mixed"), Literal.of("1")),
            limit=1,
        ),
        {},
        batch_size=2,
    )
    assert stream.read_all().to_pylist() == [{"name": "first"}]
    cypher, _ = driver.calls[-1]
    assert "WHERE" not in cypher
    assert "LIMIT" not in cypher
    assert "n.`mixed` AS `mixed`" in cypher


@pytest.mark.parametrize(
    ("names", "expected"),
    [
        (["String"], StringType()),
        (["Long"], IntegerType(64)),
        (["Integer"], IntegerType(64)),
        (["Double"], FloatType(64)),
        (["Float"], FloatType(64)),
        (["Boolean"], BooleanType()),
        (["Date"], DateType()),
        (["DateTime"], TimestampType("UTC")),
        (["ZonedDateTime"], TimestampType("UTC")),
        (["LocalDateTime"], TimestampType(None)),
        (["Duration"], StringType()),
        (["Point"], StringType()),
        (["LongArray"], StringType()),
        (["Long", "Double"], FloatType(64)),
        (["Long", "String"], StringType()),
        (["Date", "DateTime"], StringType()),
        ([], StringType()),
    ],
)
def test_property_type_mapping(names: list[str], expected: Any) -> None:
    assert property_types_to_domain(names) == expected


def test_schema_falls_back_to_sampling_when_the_procedure_is_unavailable() -> None:
    zoned = DateTime(2024, 1, 2, 3, 4, 5, 0, tzinfo=UTC)

    def respond(query: str, params: dict[str, Any]) -> Any:
        if "nodeTypeProperties" in query:
            return ClientError("There is no procedure with the name `db.schema.nodeTypeProperties`")
        assert query == "MATCH (n:`Person`) RETURN n LIMIT $limit"
        assert params == {"limit": 50}
        return [
            {"n": make_node({"name": "alice", "age": 3, "score": 1, "joined": zoned})},
            {
                "n": make_node(
                    {
                        "name": "bob",
                        "score": 2.5,
                        "flag": True,
                        "born": Date(2024, 1, 2),
                        "local": DateTime(2024, 1, 1),
                        "where": CartesianPoint((1.0, 2.0)),
                    }
                )
            },
        ]

    source = make_source(FakeDriver(respond), sample_size=50)

    assert source.schema() == Schema.of(
        ("age", IntegerType(64)),
        ("born", DateType()),
        ("flag", BooleanType()),
        ("joined", TimestampType("UTC")),
        ("local", TimestampType(None)),
        ("name", StringType()),
        ("score", FloatType(64)),
        ("where", StringType()),
    )


def test_authentication_failure_does_not_fall_back_to_sampling() -> None:
    driver = FakeDriver(
        lambda query, params: AuthError("The client is unauthorized due to authentication failure.")
    )
    source = make_source(driver)
    with pytest.raises(SourceError) as info:
        source.schema()
    assert info.value.code is DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE
    assert len(driver.calls) == 1


def test_connection_failure_during_discovery_is_a_source_error() -> None:
    driver = FakeDriver(
        lambda query, params: ServiceUnavailable("Unable to retrieve routing information")
    )
    with pytest.raises(SourceError) as info:
        make_source(driver).schema()
    assert info.value.code is DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE
    assert "routing information" in str(info.value)
    assert info.value.__cause__ is None


def test_label_without_properties_reports_schema_unavailable() -> None:
    with pytest.raises(SourceError) as info:
        make_source(FakeDriver(lambda query, params: [])).schema()
    assert info.value.code is DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE
    assert "schema=" in str(info.value)


def test_declared_schema_skips_discovery() -> None:
    driver = FakeDriver(lambda query, params: RuntimeError("should not be called"))
    assert make_source(driver, schema=PEOPLE).schema() is PEOPLE
    assert driver.calls == []


def test_sampled_value_types() -> None:
    assert infer_value_type(None) is None
    assert infer_value_type(True) == BooleanType()
    assert infer_value_type(1) == IntegerType(64)
    assert infer_value_type(1.5) == FloatType(64)
    assert infer_value_type("x") == StringType()
    assert infer_value_type(Date(2024, 1, 1)) == DateType()
    assert infer_value_type(DateTime(2024, 1, 1, tzinfo=UTC)) == TimestampType("UTC")
    assert infer_value_type(DateTime(2024, 1, 1)) == TimestampType(None)
    assert infer_value_type(Duration(days=1)) == StringType()
    assert infer_value_type([1, 2]) == StringType()


# -- capabilities and relation ------------------------------------------------------


def test_capabilities_are_honest() -> None:
    caps = make_source(FakeDriver(), schema=PEOPLE).capabilities()
    assert caps.projection is Support.FULL
    assert caps.predicate is Support.FULL
    assert caps.limit is Support.FULL
    assert caps.parameters is True
    assert caps.expressions == PUSHED_EXPRESSIONS
    assert ExpressionKind.ALIAS not in caps.expressions
    assert ExpressionKind.ARITHMETIC not in caps.expressions
    assert caps.evidence


def test_relation_describes_the_spark_connector_without_leaking_secrets() -> None:
    source = make_source(
        FakeDriver(), schema=PEOPLE, uri="neo4j+s://graph.example.com:7687", database="graphs"
    )
    relation = source.relation()
    assert relation.kind == SPARK_KIND == "neo4j"
    assert relation.options == {
        "url": "neo4j+s://graph.example.com:7687",
        "labels": "Person",
        "database": "graphs",
    }
    assert set(relation.secrets) == {
        "authentication.basic.username",
        "authentication.basic.password",
    }
    assert relation.secrets.reveal() == {
        "authentication.basic.username": "neo4j",
        "authentication.basic.password": PASSWORD,
    }
    serialised = json.dumps(relation.to_dict())
    assert PASSWORD not in serialised
    assert PASSWORD not in repr(relation)
    assert "database" not in make_source(FakeDriver(), schema=PEOPLE).relation().options


# -- redaction -----------------------------------------------------------------------


def test_password_and_userinfo_never_appear_in_repr_or_errors() -> None:
    def respond(query: str, params: dict[str, Any]) -> Any:
        return RuntimeError(f"authentication failed for neo4j with password {PASSWORD}")

    source = make_source(
        FakeDriver(respond), schema=PEOPLE, uri=f"neo4j://neo4j:{PASSWORD}@localhost:7687"
    )
    assert PASSWORD not in repr(source)
    assert PASSWORD not in str(source)
    assert (
        repr(source)
        == "Neo4jSource(name='people', uri='neo4j://localhost:7687', label='Person', database=None)"
    )
    assert source.uri == "neo4j://localhost:7687"

    with pytest.raises(SourceError) as info:
        source.scan(PushedOperations(projection=("name",)), {}, batch_size=10)
    assert info.value.code is DiagnosticCode.SOURCE_FAILURE
    assert PASSWORD not in str(info.value)
    assert PASSWORD not in repr(info.value)
    assert "authentication failed" in str(info.value)
    assert info.value.__cause__ is None
    assert "cypher" in dict(info.value.diagnostic.details)


def test_userinfo_is_stripped_before_the_driver_is_created(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    fake = FakeDriver()

    def create(uri: str, **config: Any) -> FakeDriver:
        captured["uri"] = uri
        captured.update(config)
        return fake

    monkeypatch.setattr(neo4j_module.GraphDatabase, "driver", create)
    source = Neo4jSource(
        "people",
        uri=f"bolt://ignored:{PASSWORD}@db.internal:7687",
        user="reader",
        password=PASSWORD,
        label="Person",
        schema=PEOPLE,
    )
    assert captured == {"uri": "bolt://db.internal:7687", "auth": ("reader", PASSWORD)}
    assert PASSWORD not in repr(source)
    source.close()
    assert fake.closed
    source.close()  # idempotent


def test_driver_construction_failure_is_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    def create(uri: str, **config: Any) -> FakeDriver:
        raise ValueError(f"bad URI for {config['auth'][1]}")

    monkeypatch.setattr(neo4j_module.GraphDatabase, "driver", create)
    with pytest.raises(SourceError) as info:
        Neo4jSource("people", uri="neo4j://h:7687", user="u", password=PASSWORD, label="Person")
    assert PASSWORD not in str(info.value)
    assert info.value.__cause__ is None


@pytest.mark.parametrize(
    ("uri", "clean", "password"),
    [
        ("neo4j://localhost:7687", "neo4j://localhost:7687", None),
        ("neo4j://alice:pw@localhost:7687", "neo4j://localhost:7687", "pw"),
        ("neo4j+s://alice@host:7687/?x=1", "neo4j+s://host:7687/?x=1", None),
        ("localhost:7687", "localhost:7687", None),
    ],
)
def test_strip_userinfo(uri: str, clean: str, password: str | None) -> None:
    assert strip_userinfo(uri) == (clean, password)


# -- streaming -------------------------------------------------------------------------


def test_stream_converts_driver_values_and_matches_the_projection() -> None:
    schema = Schema.of(
        ("name", StringType()),
        ("joined", TimestampType("UTC")),
        ("local", TimestampType(None)),
        ("born", DateType()),
        ("span", StringType()),
        ("where", StringType()),
        ("tags", StringType()),
        ("missing", IntegerType(64)),
        ("count", StringType()),
    )
    rows = [
        {
            "name": "alice",
            "joined": DateTime(2024, 1, 2, 3, 4, 5, 0, tzinfo=dt.timezone(dt.timedelta(hours=2))),
            "local": DateTime(2024, 1, 2, 3, 4, 5, 0),
            "born": Date(2024, 1, 2),
            "span": Duration(months=1, days=2, seconds=3),
            "where": CartesianPoint((1.0, 2.0)),
            "tags": ["a", "b"],
            "missing": None,
            "count": 7,
        },
        {
            "name": None,
            "joined": DateTime(2024, 6, 1, 0, 0, 0, 0),
            "local": DateTime(2024, 6, 1, 0, 0, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=-3))),
        },
    ]
    source = make_source(FakeDriver(lambda query, params: rows), schema=schema)

    stream = source.scan(PushedOperations(), {}, batch_size=100)
    table = stream.read_all()

    assert table.schema == stream.schema
    assert table.schema.names == list(schema.names)
    assert table.schema.field("joined").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("local").type == pa.timestamp("us")
    first, second = table.to_pylist()
    assert first["name"] == "alice"
    assert first["joined"] == dt.datetime(2024, 1, 2, 1, 4, 5, tzinfo=UTC)
    assert first["local"] == dt.datetime(2024, 1, 2, 3, 4, 5)
    assert first["born"] == dt.date(2024, 1, 2)
    assert first["span"] == "P1M2DT3S"
    assert first["where"] == str(CartesianPoint((1.0, 2.0)))
    assert first["tags"] == "['a', 'b']"
    assert first["missing"] is None
    assert first["count"] == "7"
    assert second["name"] is None
    assert second["joined"] == dt.datetime(2024, 6, 1, tzinfo=UTC)
    assert second["local"] == dt.datetime(2024, 6, 1, 3, 0, 0)


def test_stream_is_batched_and_closes_the_session_once_exhausted() -> None:
    rows = [{"name": f"p{i}", "age": i} for i in range(5)]
    driver = FakeDriver(lambda query, params: rows)
    source = make_source(driver, schema=PEOPLE)

    stream = source.scan(PushedOperations(projection=("name", "age")), {}, batch_size=2)
    assert stream.schema == pa.schema([pa.field("name", pa.string()), pa.field("age", pa.int64())])
    assert not driver.sessions[0].closed
    batches = list(stream)

    assert [b.num_rows for b in batches] == [2, 2, 1]
    assert batches[2].to_pylist() == [{"name": "p4", "age": 4}]
    assert driver.sessions[0].closed
    assert stream.closed
    assert driver.sessions[0].config["fetch_size"] == 2


def test_closing_an_unread_stream_releases_the_session() -> None:
    driver = FakeDriver(lambda query, params: [{"name": "x", "age": 1}])
    source = make_source(driver, schema=PEOPLE)
    stream = source.scan(PushedOperations(projection=("name",)), {}, batch_size=10)
    stream.close()
    assert driver.sessions[0].closed
    assert list(stream) == []


def test_source_close_leaves_an_injected_driver_open_and_refuses_new_scans() -> None:
    driver = FakeDriver(lambda query, params: [])
    source = make_source(driver, schema=PEOPLE)
    source.close()
    assert not driver.closed
    with pytest.raises(SourceError):
        source.scan(PushedOperations(), {}, batch_size=10)


def test_source_close_closes_active_sessions_but_leaves_injected_driver_open() -> None:
    driver = FakeDriver(lambda query, params: [{"name": "Alice"}])
    source = make_source(driver, schema=PEOPLE)
    stream = source.scan(PushedOperations(projection=("name",)), {}, batch_size=10)
    source.close()
    assert stream.closed
    assert driver.sessions[0].closed
    assert not driver.closed


@pytest.mark.parametrize("batch_size", [0, -1, True, 1.5, "10", None])
def test_invalid_batch_size_is_rejected_before_opening_a_session(batch_size: Any) -> None:
    driver = FakeDriver()
    source = make_source(driver, schema=PEOPLE)
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        source.scan(PushedOperations(), {}, batch_size=batch_size)
    assert driver.sessions == []


def test_failure_while_streaming_is_wrapped_and_the_session_closed() -> None:
    rows = [{"name": "a", "age": 1}, {"name": "b", "age": 2}, {"name": "c", "age": 3}]
    error = ServiceUnavailable(f"connection lost while streaming for {PASSWORD}")
    driver = FakeDriver(lambda query, params: FakeResult(rows, fail_after=2, error=error))
    source = make_source(driver, schema=PEOPLE)
    stream = source.scan(PushedOperations(projection=("name", "age")), {}, batch_size=1)

    with pytest.raises(SourceError) as info:
        list(stream)
    assert "connection lost" in str(info.value)
    assert PASSWORD not in str(info.value)
    assert info.value.__cause__ is None
    assert driver.sessions[0].closed


def test_values_that_do_not_fit_the_declared_type_are_a_source_error() -> None:
    driver = FakeDriver(lambda query, params: [{"name": "a", "age": "not a number"}])
    source = make_source(driver, schema=PEOPLE)
    stream = source.scan(PushedOperations(projection=("name", "age")), {}, batch_size=10)
    with pytest.raises(SourceError) as info:
        list(stream)
    assert info.value.code is DiagnosticCode.SOURCE_FAILURE
    assert driver.sessions[0].closed


def test_unknown_projected_column_is_reported() -> None:
    source = make_source(FakeDriver(), schema=PEOPLE)
    with pytest.raises(SourceError) as info:
        source.scan(PushedOperations(projection=("nope",)), {}, batch_size=10)
    assert info.value.code is DiagnosticCode.PLAN_UNKNOWN_COLUMN


def test_constructor_validation() -> None:
    with pytest.raises(ValueError):
        make_source(FakeDriver(), label="")
    with pytest.raises(ValueError):
        make_source(FakeDriver(), sample_size=0)
    with pytest.raises(ValueError):
        Neo4jSource(
            "",
            uri="neo4j://h",
            user="u",
            password="p",
            label="L",
            driver=cast(Driver, FakeDriver()),
        )


# -- through the facade with DuckDB (fake driver) -------------------------------------


FACADE_SQL = "SELECT name, age FROM people WHERE age > :min_age AND name LIKE 'A%' LIMIT 2"
FACADE_CYPHER = (
    "MATCH (n:`Person`) WHERE ((n.`age` > $p0) AND (n.`name` =~ $p1)) "
    "RETURN n.`name` AS `name`, n.`age` AS `age` LIMIT $limit"
)
FACADE_VALUES = {"p0": 30, "p1": "(?s)^A.*$(?!.)", "limit": 2}


def test_context_plans_the_sql_and_the_source_compiles_it_to_cypher() -> None:
    pytest.importorskip("duckdb")
    import invariantql as iql
    from invariantql.application.parameters import bind_parameters

    rows = [{"name": "Alice", "age": 34}, {"name": "Amy", "age": 40}]
    driver = FakeDriver(lambda query, params: rows)
    source = make_source(driver, schema=PEOPLE)
    context = iql.Context()
    context.register_source(source)
    try:
        execution_plan = context.sql(FACADE_SQL).execution_plan()
        assert execution_plan.residual.is_empty
        assert execution_plan.pushed.projection == ("name", "age")
        assert execution_plan.pushed.limit == 2
        params = bind_parameters(execution_plan.plan, {"min_age": 30})
        assert _rows(source.scan(execution_plan.pushed, params, batch_size=100)) == rows
    finally:
        context.close()

    assert driver.calls == [(FACADE_CYPHER, FACADE_VALUES)]
    assert all(session.closed for session in driver.sessions)
    assert not driver.closed  # an injected driver is never closed by the context


def test_context_executes_the_query_through_duckdb() -> None:
    pytest.importorskip("duckdb")
    import invariantql as iql

    rows = [{"name": "Alice", "age": 34}, {"name": "Amy", "age": 40}]
    driver = FakeDriver(lambda query, params: rows)
    context = iql.Context()
    context.register_source(make_source(driver, schema=PEOPLE))
    try:
        assert _rows(context.sql(FACADE_SQL).execute(params={"min_age": 30})) == rows
    finally:
        context.close()
    assert driver.calls == [(FACADE_CYPHER, FACADE_VALUES)]
    assert all(session.closed for session in driver.sessions)


def test_context_keeps_neo4j_arithmetic_residual_and_zero_division_becomes_null() -> None:
    pytest.importorskip("duckdb")
    import invariantql as iql

    rows = [{"name": "zero", "score": 0.0}, {"name": "four", "score": 4.0}]
    driver = FakeDriver(lambda query, params: rows)
    context = iql.Context()
    context.register_source(make_source(driver, schema=PEOPLE))
    try:
        query = context.sql("SELECT name FROM people WHERE 10 / score > 2")
        plan = query.execution_plan()
        assert plan.pushed.predicate is None
        assert plan.residual.predicate is not None
        assert plan.pushed.projection == ("name", "score")
        assert _rows(query.execute()) == [{"name": "four"}]
    finally:
        context.close()

    cypher, values = driver.calls[-1]
    assert " WHERE " not in cypher
    assert values == {}


# -- live server (opt-in) ---------------------------------------------------------------

NEO4J_URI = os.environ.get("INVARIANTQL_NEO4J_URI")


@pytest.mark.integration
@pytest.mark.skipif(
    not NEO4J_URI, reason="set INVARIANTQL_NEO4J_URI (+ INVARIANTQL_NEO4J_USER/PASSWORD) to run"
)
def test_live_neo4j_end_to_end_through_context_and_duckdb() -> None:
    pytest.importorskip("duckdb")
    from neo4j import GraphDatabase

    import invariantql as iql

    uri = NEO4J_URI or ""
    user = os.environ.get("INVARIANTQL_NEO4J_USER", "neo4j")
    password = os.environ.get("INVARIANTQL_NEO4J_PASSWORD", "")
    database = os.environ.get("INVARIANTQL_NEO4J_DATABASE") or None
    label = f"IqlTest{uuid.uuid4().hex[:10]}"
    quoted = "`" + label + "`"

    driver = GraphDatabase.driver(uri, auth=(user, password))
    session_config: dict[str, Any] = {"database": database} if database else {}
    try:
        with driver.session(**session_config) as session:
            session.run(
                cast(
                    Any,
                    f"""
                CREATE (:{quoted} {{name: 'Alice', age: 34, score: 7, city: 'Paris', nick: 'ali',
                                    joined: datetime('2024-01-02T03:04:05Z')}}),
                       (:{quoted} {{name: 'Bob', age: 25, score: 3, city: 'Rome',
                                    joined: datetime('2024-02-02T00:00:00+02:00')}}),
                       (:{quoted} {{name: 'Amy', age: 40, score: 5.5, city: 'Oslo'}}),
                       (:{quoted} {{name: 'Ann_a', age: 19, score: 1, city: 'Paris', nick: 'x'}})
                """,
                ),
            ).consume()

        context = iql.Context()
        context.register_source(
            iql.neo4j_source(
                "people", uri=uri, user=user, password=password, label=label, database=database
            )
        )
        try:
            schema = context.source("people").schema()
            assert schema.field("age").data_type == IntegerType(64)
            assert schema.field("score").data_type == FloatType(64)
            assert schema.field("joined").data_type == TimestampType("UTC")
            assert schema.field("name").data_type == StringType()

            query = context.sql(
                "SELECT name, age FROM people "
                "WHERE age >= :min_age AND name LIKE 'A%' AND city IN ('Paris', 'Rome')"
            )
            assert query.execution_plan().residual.is_empty
            assert _rows(query.execute(params={"min_age": 20})) == [{"name": "Alice", "age": 34}]

            def run(sql: str) -> list[dict[str, Any]]:
                return _rows(context.sql(sql).execute())

            # NULL <> 'x' is unknown: nodes without a nick are excluded, like SQL.
            assert {r["name"] for r in run("SELECT name FROM people WHERE nick <> 'x'")} == {
                "Alice"
            }

            # '/' is floating-point division; the alias is computed by DuckDB over pushed columns.
            halves = run("SELECT name, score / 2 AS half FROM people WHERE score / 2 > 2")
            assert sorted((r["name"], r["half"]) for r in halves) == [("Alice", 3.5), ("Amy", 2.75)]

            joined = run("SELECT name, joined FROM people WHERE joined IS NOT NULL")
            assert {r["name"]: r["joined"] for r in joined} == {
                "Alice": dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC),
                "Bob": dt.datetime(2024, 2, 1, 22, 0, 0, tzinfo=UTC),
            }
            assert not run("SELECT name FROM people WHERE name LIKE 'a%'")
            assert run("SELECT name FROM people WHERE name LIKE 'Ann_a' LIMIT 1") == [
                {"name": "Ann_a"}
            ]
        finally:
            context.close()
    finally:
        with driver.session(**session_config) as session:
            session.run(cast(Any, f"MATCH (n:{quoted}) DETACH DELETE n")).consume()
        driver.close()
