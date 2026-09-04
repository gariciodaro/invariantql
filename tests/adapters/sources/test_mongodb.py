"""Tests for the MongoDB source adapter.

Unit tests drive the adapter with a fake collection that records ``find()``
arguments and returns canned documents. The integration test needs a live
server named by ``INVARIANTQL_MONGODB_URI`` and is skipped otherwise.
"""

from __future__ import annotations

import datetime as dt
import os
import re
import uuid
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest
from bson import Decimal128, ObjectId
from bson.regex import Regex
from pymongo.errors import OperationFailure

from invariantql.adapters.sources.mongodb import MongoDBSource, like_to_regex
from invariantql.domain.capabilities import Support
from invariantql.domain.diagnostics import (
    DiagnosticCode,
    ParameterError,
    SourceError,
)
from invariantql.domain.execution import PushedOperations
from invariantql.domain.expressions import (
    And,
    Column,
    Comparison,
    ComparisonOp,
    ExpressionKind,
    In,
    IsNull,
    Like,
    Literal,
    Or,
    Parameter,
)
from invariantql.domain.schema import Schema
from invariantql.domain.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    FloatType,
    IntegerType,
    ListType,
    StringType,
    StructType,
    TimestampType,
)

PASSWORD = "s3cr3t-p4ssw0rd"
SECRET_URI = f"mongodb://alice:{PASSWORD}@mongo.example.com:27017/app?authSource=admin"
OID = "5f1d7f3e8a9b0c1d2e3f4a5b"
UTC = dt.timezone.utc

DECLARED = Schema.of(
    ("_id", StringType()),
    ("name", StringType()),
    ("qty", IntegerType(64)),
    ("amount", FloatType(64)),
    ("day", DateType()),
    ("when", TimestampType("UTC")),
    ("price", DecimalType(10, 2)),
)


# -- fakes ----------------------------------------------------------------------


class FakeCursor:
    def __init__(self, docs: list[dict[str, Any]], *, fail: Exception | None = None) -> None:
        self._docs = docs
        self._fail = fail
        self.closed = False

    def __iter__(self):
        if self._fail is not None:
            raise self._fail
        return iter(self._docs)

    def close(self) -> None:
        self.closed = True


class FakeCollection:
    def __init__(self, docs: list[dict[str, Any]] = (), *, fail: Exception | None = None) -> None:  # type: ignore[assignment]
        self.docs = list(docs)
        self.fail = fail
        self.calls: list[tuple[Any, dict[str, Any]]] = []
        self.cursors: list[FakeCursor] = []

    def find(self, filter: Any = None, **kwargs: Any) -> FakeCursor:
        self.calls.append((filter, dict(kwargs)))
        docs = self.docs
        limit = kwargs.get("limit")
        if limit:
            docs = docs[:limit]
        cursor = FakeCursor(docs, fail=self.fail)
        self.cursors.append(cursor)
        return cursor


class FakeDatabase:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    def __getitem__(self, name: str) -> FakeCollection:
        self.client.requested.append(name)
        return self.client.collection


class FakeClient:
    def __init__(self, collection: FakeCollection) -> None:
        self.collection = collection
        self.requested: list[str] = []
        self.closed = False

    def __getitem__(self, name: str) -> FakeDatabase:
        self.requested.append(name)
        return FakeDatabase(self)

    def close(self) -> None:
        self.closed = True


def make_source(
    docs: list[dict[str, Any]] = (),  # type: ignore[assignment]
    *,
    schema: Schema | None = DECLARED,
    fail: Exception | None = None,
    **options: Any,
) -> tuple[MongoDBSource, FakeCollection, FakeClient]:
    collection = FakeCollection(docs, fail=fail)
    client = FakeClient(collection)
    source = MongoDBSource(
        "docs",
        uri=SECRET_URI,
        database="app",
        collection="orders",
        schema=schema,
        client=client,
        **options,
    )
    return source, collection, client


def scan_filter(predicate: Any, params: dict[str, Literal] | None = None) -> Any:
    source, collection, _ = make_source()
    source.scan(PushedOperations(predicate=predicate), params or {}, batch_size=8).read_all()
    return collection.calls[-1][0]


def rows_of(stream: Any) -> list[dict[str, Any]]:
    return pa.Table.from_batches(list(stream), stream.schema).to_pylist()


def col(name: str) -> Column:
    return Column(name)


def lit(value: Any) -> Literal:
    return Literal.of(value)


# -- capabilities and relation --------------------------------------------------


def test_capabilities_are_honest_about_negation_and_arithmetic() -> None:
    source, _, _ = make_source()
    caps = source.capabilities()
    assert caps.projection is Support.FULL
    assert caps.predicate is Support.FULL
    assert caps.limit is Support.FULL
    assert caps.parameters is True
    assert ExpressionKind.NOT not in caps.expressions
    assert ExpressionKind.ARITHMETIC not in caps.expressions
    assert {
        ExpressionKind.COLUMN,
        ExpressionKind.LITERAL,
        ExpressionKind.PARAMETER,
        ExpressionKind.COMPARISON,
        ExpressionKind.AND,
        ExpressionKind.OR,
        ExpressionKind.IS_NULL,
        ExpressionKind.IN,
        ExpressionKind.LIKE,
    } == set(caps.expressions)
    assert caps.evidence


def test_relation_describes_spark_connector_without_revealing_uri() -> None:
    source, _, _ = make_source()
    relation = source.relation()
    assert relation.kind == "mongodb"
    assert relation.options == {"database": "app", "collection": "orders"}
    assert list(relation.secrets) == ["connection.uri"]
    assert relation.secrets["connection.uri"] == "***"
    assert relation.secrets.reveal() == {"connection.uri": SECRET_URI}
    assert PASSWORD not in repr(relation)
    assert PASSWORD not in str(relation.to_dict())


# -- filter translation ---------------------------------------------------------


def test_comparison_with_parameter_is_substituted() -> None:
    predicate = Comparison(ComparisonOp.EQ, col("qty"), Parameter("q"))
    assert scan_filter(predicate, {"q": lit(3)}) == {"qty": {"$eq": 3}}


def test_missing_parameter_raises_parameter_error() -> None:
    source, _, _ = make_source()
    predicate = Comparison(ComparisonOp.EQ, col("qty"), Parameter("q"))
    with pytest.raises(ParameterError) as info:
        source.scan(PushedOperations(predicate=predicate), {}, batch_size=8)
    assert info.value.code is DiagnosticCode.PARAMETER_MISSING


@pytest.mark.parametrize(
    ("op", "expected"),
    [
        (ComparisonOp.LT, "$lt"),
        (ComparisonOp.LE, "$lte"),
        (ComparisonOp.GT, "$gt"),
        (ComparisonOp.GE, "$gte"),
    ],
)
def test_ordering_comparisons(op: ComparisonOp, expected: str) -> None:
    assert scan_filter(Comparison(op, col("qty"), lit(3))) == {"qty": {expected: 3}}


def test_literal_on_the_left_flips_the_operator() -> None:
    assert scan_filter(Comparison(ComparisonOp.LT, lit(3), col("qty"))) == {"qty": {"$gt": 3}}


def test_not_equal_adds_null_guard() -> None:
    predicate = Comparison(ComparisonOp.NE, col("qty"), lit(3))
    assert scan_filter(predicate) == {"$and": [{"qty": {"$ne": 3}}, {"qty": {"$ne": None}}]}


def test_is_null_and_is_not_null() -> None:
    assert scan_filter(IsNull(col("name"))) == {"name": None}
    assert scan_filter(IsNull(col("name"), negated=True)) == {"name": {"$ne": None}}


def test_in_and_not_in() -> None:
    values = (lit("a"), lit("b"))
    assert scan_filter(In(col("name"), values)) == {"name": {"$in": ["a", "b"]}}
    assert scan_filter(In(col("name"), values, negated=True)) == {
        "$and": [{"name": {"$nin": ["a", "b"]}}, {"name": {"$ne": None}}]
    }


def test_in_drops_null_members_because_they_never_match() -> None:
    predicate = In(col("name"), (lit("a"), lit(None)))
    assert scan_filter(predicate) == {"name": {"$in": ["a"]}}


def test_like_becomes_anchored_case_sensitive_regex() -> None:
    assert like_to_regex("%a_c%") == "^.*a.c.*$(?!.)"
    assert like_to_regex("a.b+c%") == r"^a\.b\+c.*$(?!.)"
    predicate = Like(col("name"), lit("%a_c%"))
    assert scan_filter(predicate) == {"name": {"$regex": "^.*a.c.*$(?!.)", "$options": "s"}}
    assert scan_filter(Like(col("name"), lit("a.b+c%"))) == {
        "name": {"$regex": r"^a\.b\+c.*$(?!.)", "$options": "s"}
    }


def test_like_anchor_does_not_accept_an_unmatched_final_newline() -> None:
    pattern = re.compile(like_to_regex("a"), re.DOTALL)
    assert pattern.fullmatch("a") is not None
    assert pattern.search("a\n") is None


def test_not_like_adds_null_guard() -> None:
    predicate = Like(col("name"), lit("a%"), negated=True)
    assert scan_filter(predicate) == {
        "$and": [
            {"name": {"$not": Regex("^a.*$(?!.)", "s")}},
            {"name": {"$ne": None}},
        ]
    }


def test_and_or_nesting() -> None:
    predicate = Or(
        (
            And(
                (Comparison(ComparisonOp.GT, col("qty"), lit(1)), IsNull(col("name"), negated=True))
            ),
            Comparison(ComparisonOp.EQ, col("amount"), lit(2.5)),
        )
    )
    assert scan_filter(predicate) == {
        "$or": [
            {"$and": [{"qty": {"$gt": 1}}, {"name": {"$ne": None}}]},
            {"amount": {"$eq": 2.5}},
        ]
    }


def test_top_level_conjuncts_become_a_single_and() -> None:
    predicate = And((Comparison(ComparisonOp.GT, col("qty"), lit(1)), IsNull(col("name"))))
    assert scan_filter(predicate) == {"$and": [{"qty": {"$gt": 1}}, {"name": None}]}


def test_declared_string_id_is_not_assumed_to_be_an_object_id() -> None:
    assert scan_filter(Comparison(ComparisonOp.EQ, col("_id"), lit(OID))) == {"_id": {"$eq": OID}}
    assert scan_filter(In(col("_id"), (lit(OID), lit("not-an-id")))) == {
        "_id": {"$in": [OID, "not-an-id"]}
    }


def test_date_literal_becomes_utc_midnight() -> None:
    predicate = Comparison(ComparisonOp.GE, col("day"), lit(dt.date(2024, 1, 2)))
    assert scan_filter(predicate) == {"day": {"$gte": dt.datetime(2024, 1, 2, tzinfo=UTC)}}


def test_decimal_literal_becomes_decimal128() -> None:
    predicate = Comparison(ComparisonOp.GT, col("price"), lit(Decimal("1.10")))
    assert scan_filter(predicate) == {"price": {"$gt": Decimal128("1.10")}}


def test_comparison_with_null_literal_matches_nothing() -> None:
    source, collection, _ = make_source([{"name": "a", "qty": 1}])
    predicate = Comparison(ComparisonOp.EQ, col("qty"), lit(None))
    table = source.scan(PushedOperations(predicate=predicate), {}, batch_size=8).read_all()
    assert collection.calls[-1][0] == {}
    assert table.num_rows == 0


# -- projection, limit, and streaming ----------------------------------------


def test_projection_excludes_id_unless_requested() -> None:
    source, collection, _ = make_source()
    source.scan(PushedOperations(projection=("name", "qty"), limit=5), {}, batch_size=16).read_all()
    filter_doc, kwargs = collection.calls[-1]
    assert filter_doc == {}
    assert kwargs == {
        "projection": {"name": 1, "qty": 1, "_id": 0},
        "batch_size": 16,
        "collation": {"locale": "simple"},
        "limit": 5,
    }

    source.scan(PushedOperations(projection=("_id", "name")), {}, batch_size=16).read_all()
    _, kwargs = collection.calls[-1]
    assert kwargs == {
        "projection": {"_id": 1, "name": 1},
        "batch_size": 16,
        "collation": {"locale": "simple"},
    }


def test_scan_pins_simple_collation_for_portable_string_comparisons() -> None:
    source, collection, _ = make_source([{"name": "Alice"}])
    source.scan(
        PushedOperations(
            projection=("name",),
            predicate=Comparison(ComparisonOp.EQ, col("name"), lit("alice")),
        ),
        {},
        batch_size=8,
    ).close()
    assert collection.calls[-1][1]["collation"] == {"locale": "simple"}


def test_limit_zero_returns_empty_stream_without_querying() -> None:
    source, collection, _ = make_source([{"name": "a"}])
    stream = source.scan(PushedOperations(projection=("name",), limit=0), {}, batch_size=8)
    assert stream.read_all().num_rows == 0
    assert collection.calls == []


def test_stream_converts_values_and_fills_missing_fields() -> None:
    when = dt.datetime(2024, 1, 1, 12, 30)
    docs = [
        {
            "_id": ObjectId(OID),
            "name": "alice",
            "qty": 3,
            "amount": 10,
            "day": when,
            "when": when,
            "price": Decimal128("1.10"),
        },
        {"_id": ObjectId(OID), "name": None, "amount": 2.5, "price": Decimal128("2.123")},
    ]
    source, collection, _ = make_source(docs)
    stream = source.scan(PushedOperations(), {}, batch_size=1)
    assert stream.schema == pa.schema(
        [
            pa.field("_id", pa.string()),
            pa.field("name", pa.string()),
            pa.field("qty", pa.int64()),
            pa.field("amount", pa.float64()),
            pa.field("day", pa.date32()),
            pa.field("when", pa.timestamp("us", tz="UTC")),
            pa.field("price", pa.decimal128(10, 2)),
        ]
    )
    batches = list(stream)
    assert len(batches) == 2
    rows = pa.Table.from_batches(batches).to_pylist()
    assert rows[0]["_id"] == OID
    assert rows[0]["qty"] == 3
    assert rows[0]["amount"] == 10.0
    assert rows[0]["day"] == dt.date(2024, 1, 1)
    assert rows[0]["when"] == when.replace(tzinfo=UTC)
    assert rows[0]["price"] == Decimal("1.10")
    assert rows[1]["name"] is None
    assert rows[1]["qty"] is None
    assert rows[1]["day"] is None
    assert rows[1]["price"] == Decimal("2.12")
    assert collection.cursors[-1].closed


def test_stream_schema_matches_projection_exactly() -> None:
    source, _, _ = make_source([{"qty": 1, "name": "a"}])
    stream = source.scan(PushedOperations(projection=("qty", "name")), {}, batch_size=8)
    assert stream.schema.names == ["qty", "name"]
    assert stream.read_all().to_pylist() == [{"qty": 1, "name": "a"}]


def test_closing_the_stream_closes_the_cursor() -> None:
    source, collection, _ = make_source([{"name": "a"}, {"name": "b"}])
    stream = source.scan(PushedOperations(projection=("name",)), {}, batch_size=8)
    assert not collection.cursors[-1].closed
    stream.close()
    assert collection.cursors[-1].closed


def test_source_close_closes_active_cursors_but_not_an_injected_client() -> None:
    source, collection, client = make_source([{"name": "a"}, {"name": "b"}])
    stream = source.scan(PushedOperations(projection=("name",)), {}, batch_size=8)
    source.close()
    assert stream.closed
    assert collection.cursors[-1].closed
    assert not client.closed


def test_unconvertible_value_is_a_source_error() -> None:
    source, _, _ = make_source([{"qty": "not a number"}])
    stream = source.scan(PushedOperations(projection=("qty",)), {}, batch_size=8)
    with pytest.raises(SourceError) as info:
        stream.read_all()
    assert "qty" in str(info.value)


# -- in-process evaluation of conjuncts MongoDB cannot express ----------------


INFERRED_DOCS = [
    {"_id": ObjectId(OID), "name": "alice", "mixed": 1, "qty": 3},
    {"_id": ObjectId("5f1d7f3e8a9b0c1d2e3f4a5c"), "name": "bob", "mixed": "one", "qty": 5},
    {"_id": ObjectId("6f1d7f3e8a9b0c1d2e3f4a5d"), "name": "carol", "qty": 7},
]


def test_conflicting_column_predicate_is_evaluated_in_process() -> None:
    source, collection, _ = make_source(INFERRED_DOCS, schema=None)
    predicate = And(
        (
            Comparison(ComparisonOp.EQ, col("mixed"), lit("1")),
            Comparison(ComparisonOp.GT, col("qty"), lit(1)),
        )
    )
    table = source.scan(
        PushedOperations(projection=("name",), predicate=predicate, limit=5), {}, batch_size=8
    ).read_all()
    filter_doc, kwargs = collection.calls[-1]
    assert filter_doc == {"qty": {"$gt": 1}}
    assert kwargs["projection"] == {"name": 1, "mixed": 1, "_id": 0}
    assert "limit" not in kwargs
    assert table.to_pylist() == [{"name": "alice"}]


def test_like_on_object_id_column_is_evaluated_in_process_with_limit() -> None:
    source, collection, _ = make_source(INFERRED_DOCS, schema=None)
    predicate = Like(col("_id"), lit("5f%"))
    table = source.scan(
        PushedOperations(projection=("name",), predicate=predicate, limit=1), {}, batch_size=8
    ).read_all()
    assert collection.calls[-1][0] == {}
    assert table.to_pylist() == [{"name": "alice"}]


def test_inferred_object_id_columns_convert_hex_literals() -> None:
    source, collection, _ = make_source(INFERRED_DOCS, schema=None)
    predicate = Comparison(ComparisonOp.EQ, col("_id"), lit(OID))
    source.scan(PushedOperations(predicate=predicate), {}, batch_size=8).read_all()
    assert collection.calls[-1][0] == {"_id": {"$eq": ObjectId(OID)}}


def test_not_in_with_null_member_matches_nothing() -> None:
    source, collection, _ = make_source([{"name": "a"}, {"name": "b"}])
    predicate = In(col("name"), (lit("a"), lit(None)), negated=True)
    table = source.scan(
        PushedOperations(projection=("name",), predicate=predicate), {}, batch_size=8
    ).read_all()
    assert collection.calls[-1][0] == {}
    assert table.num_rows == 0


def test_column_to_column_comparison_is_evaluated_in_process() -> None:
    source, collection, _ = make_source(
        [{"name": "a", "qty": 2, "amount": 1.5}, {"name": "b", "qty": 1, "amount": 1.5}]
    )
    predicate = Comparison(ComparisonOp.GT, col("qty"), col("amount"))
    table = source.scan(
        PushedOperations(projection=("name",), predicate=predicate), {}, batch_size=8
    ).read_all()
    assert collection.calls[-1][0] == {}
    assert table.to_pylist() == [{"name": "a"}]


# -- schema inference -----------------------------------------------------------


SAMPLE_DOCS = [
    {
        "_id": ObjectId(OID),
        "name": "alice",
        "qty": 3,
        "amount": 10.5,
        "active": True,
        "price": Decimal128("1.10"),
        "when": dt.datetime(2024, 1, 1, 12, 0, 0),
        "tags": ["a", "b"],
        "address": {"city": "Bogota", "zip": 11001},
        "blob": b"\x00\x01",
        "owner": ObjectId("6f1d7f3e8a9b0c1d2e3f4a5d"),
    },
    {
        "_id": ObjectId(),
        "name": "bob",
        "qty": None,
        "amount": 20,
        "active": False,
        "tags": [],
        "address": {"city": "Lima"},
    },
    {"_id": ObjectId(), "name": None, "qty": 7, "mixed": 1, "empty": None},
    {"_id": ObjectId(), "mixed": "one"},
]


def test_schema_inference_maps_bson_types_and_orders_id_first() -> None:
    source, collection, _ = make_source(SAMPLE_DOCS, schema=None, sample_size=50)
    schema = source.schema()
    assert collection.calls == [({}, {"limit": 50})]
    assert schema.names == (
        "_id",
        "name",
        "qty",
        "amount",
        "active",
        "price",
        "when",
        "tags",
        "address",
        "blob",
        "owner",
        "mixed",
        "empty",
    )
    assert schema.field("_id").data_type == StringType()
    assert schema.field("name").data_type == StringType()
    assert schema.field("qty").data_type == IntegerType(64)
    assert schema.field("amount").data_type == FloatType(64)  # int and float widen to float
    assert schema.field("active").data_type == BooleanType()
    assert schema.field("price").data_type == DecimalType(34, 10)
    assert schema.field("when").data_type == TimestampType("UTC")
    assert schema.field("tags").data_type == ListType(StringType())
    assert schema.field("address").data_type == StructType(
        (("city", StringType()), ("zip", IntegerType(64)))
    )
    assert schema.field("blob").data_type == BinaryType()
    assert schema.field("owner").data_type == StringType()
    assert schema.field("mixed").data_type == StringType()  # int vs str conflict
    assert schema.field("empty").data_type.kind == "unknown"
    assert source.schema() is schema
    assert len(collection.calls) == 1  # cached


def test_declared_schema_skips_sampling() -> None:
    source, collection, _ = make_source()
    assert source.schema() is DECLARED
    assert collection.calls == []


def test_empty_collection_has_no_schema() -> None:
    source, _, _ = make_source([], schema=None)
    with pytest.raises(SourceError) as info:
        source.schema()
    assert info.value.code is DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE


def test_scan_converts_nested_and_special_types() -> None:
    source, _, _ = make_source(SAMPLE_DOCS, schema=None)
    rows = (
        source.scan(
            PushedOperations(
                projection=("_id", "price", "tags", "address", "owner", "mixed", "when")
            ),
            {},
            batch_size=10,
        )
        .read_all()
        .to_pylist()
    )
    assert rows[0]["_id"] == OID
    assert rows[0]["price"] == Decimal("1.10")
    assert rows[0]["tags"] == ["a", "b"]
    assert rows[0]["address"] == {"city": "Bogota", "zip": 11001}
    assert rows[0]["owner"] == "6f1d7f3e8a9b0c1d2e3f4a5d"
    assert rows[0]["when"] == dt.datetime(2024, 1, 1, 12, tzinfo=UTC)
    assert rows[1]["address"] == {"city": "Lima", "zip": None}
    assert rows[1]["price"] is None
    assert rows[2]["mixed"] == "1"
    assert rows[3]["mixed"] == "one"
    assert rows[3]["tags"] is None


# -- redaction and lifecycle ----------------------------------------------------


def test_repr_shows_hosts_only() -> None:
    source, _, _ = make_source()
    text = repr(source)
    assert (
        text
        == "MongoDBSource(name='docs', uri='mongodb://mongo.example.com:27017', database='app', collection='orders')"
    )
    assert PASSWORD not in text
    assert "alice" not in text
    assert "authSource" not in text
    assert PASSWORD not in str(source)


def test_srv_and_multi_host_uris_are_displayed_without_userinfo() -> None:
    srv = MongoDBSource(
        "a",
        uri=f"mongodb+srv://u:{PASSWORD}@cluster0.example.net/db?retryWrites=true",
        database="db",
        collection="c",
        client=object(),
    )
    multi = MongoDBSource(
        "b",
        uri=f"mongodb://u:{PASSWORD}@h1:27017,h2:27017/db",
        database="db",
        collection="c",
        client=object(),
    )
    assert "uri='mongodb+srv://cluster0.example.net'" in repr(srv)
    assert "uri='mongodb://h1:27017,h2:27017'" in repr(multi)
    assert PASSWORD not in repr(srv) + repr(multi)


def test_provider_errors_are_wrapped_and_redacted() -> None:
    failure = OperationFailure(f"authentication failed for {SECRET_URI} password={PASSWORD}")
    source, _, _ = make_source([{"name": "a"}], fail=failure)
    stream = source.scan(PushedOperations(projection=("name",)), {}, batch_size=8)
    with pytest.raises(SourceError) as info:
        stream.read_all()
    assert PASSWORD not in str(info.value)
    assert PASSWORD not in repr(info.value)
    assert info.value.code is DiagnosticCode.SOURCE_FAILURE
    assert info.value.__cause__ is None

    inferring, _, _ = make_source([], schema=None, fail=failure)
    with pytest.raises(SourceError) as info:
        inferring.schema()
    assert PASSWORD not in str(info.value)
    assert info.value.code is DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE


def test_invalid_uri_fails_at_first_use_with_redacted_message() -> None:
    source = MongoDBSource(
        "bad",
        uri=f"http://alice:{PASSWORD}@example.com/db",
        database="db",
        collection="c",
        schema=DECLARED,
    )
    with pytest.raises(SourceError) as info:
        source.scan(PushedOperations(), {}, batch_size=8)
    assert PASSWORD not in str(info.value)


def test_close_only_closes_an_owned_client() -> None:
    source, _, client = make_source()
    source.close()
    assert not client.closed
    with pytest.raises(SourceError):
        source.scan(PushedOperations(), {}, batch_size=8)

    owned = MongoDBSource("own", uri=SECRET_URI, database="db", collection="c", schema=DECLARED)
    assert owned.owns_client
    owned.close()  # never connected: nothing to close
    owned.close()  # idempotent


def test_limit_zero_scan_still_refuses_a_closed_source() -> None:
    source, _, _ = make_source()
    source.close()
    with pytest.raises(SourceError):
        source.scan(PushedOperations(projection=("name",), limit=0), {}, batch_size=8)


@pytest.mark.parametrize("batch_size", [0, -1, True, 1.5, "10", None])
def test_scan_rejects_invalid_batch_size_without_opening_a_cursor(batch_size: Any) -> None:
    source, collection, _ = make_source()

    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        source.scan(PushedOperations(), {}, batch_size=batch_size)

    assert collection.calls == []


def test_constructor_validates_arguments() -> None:
    with pytest.raises(ValueError):
        MongoDBSource("", uri=SECRET_URI, database="db", collection="c")
    with pytest.raises(ValueError):
        MongoDBSource("x", uri="", database="db", collection="c")
    with pytest.raises(ValueError):
        MongoDBSource("x", uri=SECRET_URI, database="", collection="c")
    with pytest.raises(ValueError):
        MongoDBSource("x", uri=SECRET_URI, database="db", collection="")
    with pytest.raises(ValueError):
        MongoDBSource("x", uri=SECRET_URI, database="db", collection="c", sample_size=0)


# -- end to end through Context and DuckDB with a fake client -------------------


def test_context_and_duckdb_consume_the_source_with_a_fake_client() -> None:
    import invariantql as iql

    docs = [
        {
            "_id": ObjectId(OID),
            "name": "alice",
            "qty": 3,
            "amount": 10.5,
            "price": Decimal128("1.10"),
        },
        {"_id": ObjectId(), "name": "bob", "qty": 1, "amount": 20.0, "price": Decimal128("2.20")},
        {"_id": ObjectId(), "name": "carol", "qty": None, "amount": 5.25},
        {"_id": ObjectId(), "name": None, "qty": 2, "amount": 7.0},
    ]
    source, collection, _ = make_source(docs, schema=None)
    with iql.Context() as ctx:
        ctx.register_source(source)
        query = ctx.sql("SELECT name, qty FROM docs WHERE qty <> :skip AND name LIKE 'a%'")
        assert query.schema().names == ("name", "qty")
        explain = query.explain()
        by_operation = {n.operation: n for n in explain.nodes}
        assert by_operation["filter"].disposition.value == "pushed"
        assert by_operation["project"].disposition.value == "pushed"
        rows = rows_of(query.execute(params={"skip": 1}))
        # The fake ignores filters, so every document flows through DuckDB with
        # the pushed projection; the translated filter is what MongoDB would run.
        assert rows == [
            {"name": "alice", "qty": 3},
            {"name": "bob", "qty": 1},
            {"name": "carol", "qty": None},
            {"name": None, "qty": 2},
        ]
        filter_doc, kwargs = collection.calls[-1]
        assert filter_doc == {
            "$and": [
                {"$and": [{"qty": {"$ne": 1}}, {"qty": {"$ne": None}}]},
                {"name": {"$regex": "^a.*$(?!.)", "$options": "s"}},
            ]
        }
        assert kwargs["projection"] == {"name": 1, "qty": 1, "_id": 0}

        residual = ctx.sql("SELECT name FROM docs WHERE NOT (qty = 3)")
        nodes = {n.operation: n for n in residual.explain().nodes}
        assert nodes["filter"].disposition.value == "residual"
        assert nodes["project"].disposition.value == "partial"
        rows = rows_of(residual.execute())
        assert sorted(r["name"] or "" for r in rows) == ["", "bob"]
        filter_doc, kwargs = collection.calls[-1]
        assert filter_doc == {}
        assert kwargs["projection"] == {"name": 1, "qty": 1, "_id": 0}
    assert collection.cursors[-1].closed


# -- integration ---------------------------------------------------------------


@pytest.mark.integration
def test_end_to_end_through_context_and_duckdb() -> None:
    uri = os.environ.get("INVARIANTQL_MONGODB_URI")
    if not uri:
        pytest.skip("set INVARIANTQL_MONGODB_URI to run MongoDB integration tests")
    pymongo = pytest.importorskip("pymongo")
    import invariantql as iql

    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=10_000)
    database = client.get_default_database(default="invariantql_it")
    collection_name = f"it_{uuid.uuid4().hex}"
    collection = database[collection_name]
    when = dt.datetime(2024, 1, 1, 12, 0, 0)
    try:
        collection.insert_many(
            [
                {
                    "name": "alice",
                    "qty": 3,
                    "amount": 10.5,
                    "price": Decimal128("1.10"),
                    "when": when,
                },
                {
                    "name": "bob",
                    "qty": 1,
                    "amount": 20.0,
                    "price": Decimal128("2.20"),
                    "when": when,
                },
                {"name": "carol", "qty": None, "amount": 5.25},
                {"name": None, "qty": 2, "amount": 7.0},
                {"name": "Alice", "qty": 0, "amount": 0.0},
            ]
        )
        with iql.Context() as ctx:
            ctx.register_source(
                MongoDBSource(
                    "docs",
                    uri=uri,
                    database=database.name,
                    collection=collection_name,
                    client=client,
                )
            )
            query = ctx.sql(
                "SELECT name, qty FROM docs WHERE qty <> :skip AND name LIKE 'a%' AND name IS NOT NULL"
            )
            explain = query.explain()
            filter_node = next(n for n in explain.nodes if n.operation == "filter")
            assert filter_node.disposition.value == "pushed"
            rows = rows_of(query.execute(params={"skip": 1}))
            assert rows == [{"name": "alice", "qty": 3}]

            rows = rows_of(
                ctx.sql("SELECT name FROM docs WHERE qty IS NULL OR amount > 15").execute()
            )
            assert sorted(r["name"] for r in rows) == ["bob", "carol"]

            rows = rows_of(
                ctx.sql("SELECT name, price FROM docs WHERE NOT (qty = 3) LIMIT 10").execute()
            )
            assert sorted(r["name"] or "" for r in rows) == ["", "Alice", "bob"]
            assert {r["name"]: r["price"] for r in rows}["bob"] == Decimal("2.20")
    finally:
        collection.drop()
        client.close()
