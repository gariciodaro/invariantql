"""XML format handler: parsing, inference, projection/limit pushdown, redaction, end-to-end."""

from __future__ import annotations

import datetime as dt
import io
import os
from decimal import Decimal
from typing import TYPE_CHECKING, Any, BinaryIO

import pytest

from invariantql.adapters._shared.arrow import from_arrow_schema
from invariantql.adapters.formats.xml import (
    SPARK_XML_REQUIREMENT,
    XmlLocalHandler,
    XmlReaderSpecHandler,
    convert_value,
    infer_schema,
    infer_type,
    iter_records,
)
from invariantql.adapters.storage.local import LocalStorage
from invariantql.domain.capabilities import Support
from invariantql.domain.credentials import SecretOptions
from invariantql.domain.diagnostics import (
    DiagnosticCode,
    SourceError,
    StorageError,
    UnsupportedOperationError,
)
from invariantql.domain.execution import PushedOperations
from invariantql.domain.explain import Disposition
from invariantql.domain.expressions import Column, Comparison, ComparisonOp, Literal
from invariantql.domain.formats import CsvFormat, XmlFormat
from invariantql.domain.schema import Schema
from invariantql.domain.types import (
    BooleanType,
    DateType,
    DecimalType,
    FloatType,
    IntegerType,
    ListType,
    NullType,
    StringType,
    StructType,
    TimestampType,
)
from invariantql.ports.format_handler import DistributedFormatHandler, LocalFormatHandler

if TYPE_CHECKING:
    from pathlib import Path

    from invariantql.domain.location import Location
    from invariantql.ports.streams import RecordBatchStream

ORDERS_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<orders xmlns="urn:example:orders" xmlns:x="urn:example:extra">
  <meta generated="2024-01-01"/>
  <order id="1" x:priority="high">
    <name>alice</name>
    <amount>10.5</amount>
    <qty>3</qty>
    <day>2024-01-01</day>
    <active>true</active>
    <address><city>Paris</city><zip>75001</zip></address>
    <tag>a</tag>
    <tag>b</tag>
  </order>
  <order id="2">
    <name>bob</name>
    <amount>20</amount>
    <qty>1</qty>
    <day>2024-01-02</day>
    <active>false</active>
    <address><city>Berlin</city><zip></zip></address>
    <tag>c</tag>
  </order>
  <order id="3">
    <name/>
    <amount>5.25</amount>
    <qty></qty>
    <day>2024-01-03</day>
    <active>true</active>
    <note lang="en">hello</note>
  </order>
  <order id="4">
    <name> dave </name>
    <amount>100</amount>
    <qty>7</qty>
    <day/>
    <active/>
    <note lang="fr">salut</note>
    <tag>d</tag>
  </order>
</orders>
"""

ORDER_FORMAT = XmlFormat(row_tag="order")

RAW_RECORDS: list[dict[str, Any]] = [
    {
        "_id": "1",
        "_priority": "high",
        "name": "alice",
        "amount": "10.5",
        "qty": "3",
        "day": "2024-01-01",
        "active": "true",
        "address": {"city": "Paris", "zip": "75001"},
        "tag": ["a", "b"],
    },
    {
        "_id": "2",
        "name": "bob",
        "amount": "20",
        "qty": "1",
        "day": "2024-01-02",
        "active": "false",
        "address": {"city": "Berlin", "zip": None},
        "tag": "c",
    },
    {
        "_id": "3",
        "name": None,
        "amount": "5.25",
        "qty": None,
        "day": "2024-01-03",
        "active": "true",
        "note": {"_lang": "en", "_VALUE": "hello"},
    },
    {
        "_id": "4",
        "name": "dave",
        "amount": "100",
        "qty": "7",
        "day": None,
        "active": None,
        "note": {"_lang": "fr", "_VALUE": "salut"},
        "tag": "d",
    },
]

INFERRED_SCHEMA = Schema.of(
    ("_id", IntegerType(64)),
    ("_priority", StringType()),
    ("name", StringType()),
    ("amount", FloatType(64)),
    ("qty", IntegerType(64)),
    ("day", DateType()),
    ("active", BooleanType()),
    ("address", StructType((("city", StringType()), ("zip", IntegerType(64))))),
    ("tag", ListType(StringType())),
    ("note", StructType((("_lang", StringType()), ("_VALUE", StringType())))),
)


class _TrackingStorage(LocalStorage):
    """A LocalStorage that remembers every handle it hands out."""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.handles: list[Any] = []

    def open_read(self, location: Location) -> BinaryIO:
        handle = super().open_read(location)
        self.handles.append(handle)
        return handle

    @property
    def all_closed(self) -> bool:
        return bool(self.handles) and all(h.closed for h in self.handles)


class _FailingStorage(LocalStorage):
    """A storage whose provider error echoes a secret; the handler must scrub it."""

    def __init__(self, root: Path, secret: str) -> None:
        super().__init__(root)
        self._secrets = SecretOptions({"sas_token": secret})

    def open_read(self, location: Location) -> BinaryIO:
        token = self._secrets.reveal()["sas_token"]
        raise RuntimeError(f"provider rejected {location.uri}?sig={token} token={token}")


def _rows(stream: RecordBatchStream) -> list[dict[str, Any]]:
    """Consume a result through the port: every batch, as plain dicts."""

    return [row for batch in stream for row in batch.to_pylist()]


@pytest.fixture()
def xml_dir(tmp_path: Path) -> Path:
    (tmp_path / "orders.xml").write_bytes(ORDERS_XML)
    return tmp_path


@pytest.fixture()
def storage(xml_dir: Path) -> _TrackingStorage:
    return _TrackingStorage(xml_dir)


# -- parsing --------------------------------------------------------------------


def test_iter_records_maps_attributes_children_nested_repeated_and_namespaces() -> None:
    records = list(iter_records(io.BytesIO(ORDERS_XML), ORDER_FORMAT))
    assert records == RAW_RECORDS


def test_iter_records_limit_stops_parsing_early() -> None:
    limited = iter_records(io.BytesIO(ORDERS_XML), ORDER_FORMAT, limit=2)
    assert [r["_id"] for r in limited] == ["1", "2"]
    assert list(iter_records(io.BytesIO(ORDERS_XML), ORDER_FORMAT, limit=0)) == []


def test_iter_records_honours_custom_prefix_and_value_tag() -> None:
    fmt = XmlFormat(row_tag="item", attribute_prefix="@", value_tag="text")
    doc = b"<r><item k='v'>body</item><item>plain</item><item><k>child</k></item></r>"
    assert list(iter_records(io.BytesIO(doc), fmt)) == [
        {"@k": "v", "text": "body"},
        {"text": "plain"},
        {"k": "child"},
    ]


def test_iter_records_treats_nested_row_tag_as_a_child() -> None:
    doc = b"<r><row><id>1</id><row><id>inner</id></row></row><row><id>2</id></row></r>"
    records = list(iter_records(io.BytesIO(doc), XmlFormat()))
    assert records == [{"id": "1", "row": {"id": "inner"}}, {"id": "2"}]


def test_malformed_xml_is_a_source_error() -> None:
    doc = b"<r><row><id>1</id></row><row><id>2"
    with pytest.raises(SourceError) as info:
        list(iter_records(io.BytesIO(doc), XmlFormat()))
    assert info.value.code is DiagnosticCode.FORMAT_INVALID


@pytest.mark.parametrize(
    "document",
    [
        b'<!DOCTYPE r [<!ENTITY value "expanded">]><r><row><v>&value;</v></row></r>',
        b'<!DOCTYPE r SYSTEM "file:///etc/passwd"><r><row><v>safe</v></row></r>',
        b"<!DOCTYPE r><r><row><v>safe</v></row></r>",
    ],
)
def test_iter_records_rejects_dtds_and_entities(document: bytes) -> None:
    with pytest.raises(SourceError) as info:
        list(iter_records(io.BytesIO(document), XmlFormat()))
    assert info.value.code is DiagnosticCode.FORMAT_INVALID
    assert "forbidden" in str(info.value).lower()


def test_iter_records_keeps_predefined_xml_entities() -> None:
    document = b"<r><row><v>&lt;safe &amp; sound&gt;</v></row></r>"
    assert list(iter_records(io.BytesIO(document), XmlFormat())) == [{"v": "<safe & sound>"}]


# -- inference ------------------------------------------------------------------


def test_infer_schema_from_raw_records() -> None:
    assert infer_schema(RAW_RECORDS) == INFERRED_SCHEMA


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        (["1", "-2", "+3"], IntegerType(64)),
        (["1", "2.5"], FloatType(64)),
        (["1e3", ".5", "7."], FloatType(64)),
        ([str(2**63)], FloatType(64)),
        (["1e9999"], StringType()),
        (["true", "False", None], BooleanType()),
        (["2024-01-01", "1999-12-31"], DateType()),
        (["2024-01-01", "not a date"], StringType()),
        (["1", "x"], StringType()),
        (["nan"], StringType()),
        ([None, None], NullType()),
        ([], NullType()),
        (["a", ["b", "c"]], ListType(StringType())),
        ([{"x": "1"}, None], StructType((("x", IntegerType(64)),))),
        (
            ["10", {"_cur": "EUR", "_VALUE": "12"}],
            StructType((("_VALUE", IntegerType(64)), ("_cur", StringType()))),
        ),
    ],
)
def test_infer_type_precedence(values: list[Any], expected: Any) -> None:
    assert infer_type(values) == expected


# -- conversion -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "data_type", "expected"),
    [
        ("42", IntegerType(64), 42),
        ("42", IntegerType(8), 42),
        ("300", IntegerType(8), None),
        ("4.2", IntegerType(64), None),
        ("4.2", FloatType(64), 4.2),
        ("7", FloatType(64), 7.0),
        ("abc", FloatType(64), None),
        ("1e9999", FloatType(64), None),
        ("TRUE", BooleanType(), True),
        ("yes", BooleanType(), None),
        ("2024-02-29", DateType(), dt.date(2024, 2, 29)),
        ("2023-02-29", DateType(), None),
        ("2024-01-01T10:00:00", DateType(), None),
        ("2024-01-01T10:00:00", TimestampType(), dt.datetime(2024, 1, 1, 10, 0)),
        ("2024-01-01T10:00:00+02:00", TimestampType(), dt.datetime(2024, 1, 1, 8, 0)),
        (
            "2024-01-01T10:00:00Z",
            TimestampType("UTC"),
            dt.datetime(2024, 1, 1, 10, tzinfo=dt.timezone.utc),
        ),
        ("garbage", TimestampType(), None),
        ("1.1", DecimalType(10, 2), Decimal("1.10")),
        ("1.005", DecimalType(10, 2), Decimal("1.00")),
        ("12345678901", DecimalType(10, 2), None),
        ("nan", DecimalType(10, 2), None),
        ("x", StringType(), "x"),
        (None, IntegerType(64), None),
        ("1", NullType(), None),
        (["1", "2"], ListType(IntegerType(64)), [1, 2]),
        ("1", ListType(IntegerType(64)), [1]),
        (["1", "2"], IntegerType(64), None),
        (
            {"x": "1"},
            StructType((("x", IntegerType(64)), ("y", StringType()))),
            {"x": 1, "y": None},
        ),
        (
            "5",
            StructType((("_VALUE", IntegerType(64)), ("_cur", StringType()))),
            {"_VALUE": 5, "_cur": None},
        ),
        ({"_VALUE": "5", "_cur": "EUR"}, IntegerType(64), 5),
        ({"_cur": "EUR"}, IntegerType(64), None),
    ],
)
def test_convert_value_follows_schema_and_nulls_invalid(
    value: Any, data_type: Any, expected: Any
) -> None:
    assert convert_value(value, data_type) == expected


# -- handler contract ---------------------------------------------------------------


def test_handlers_satisfy_the_ports() -> None:
    assert isinstance(XmlLocalHandler(), LocalFormatHandler)
    assert isinstance(XmlReaderSpecHandler(), DistributedFormatHandler)
    assert XmlLocalHandler().format_name == "xml"
    assert XmlReaderSpecHandler().format_name == "xml"


def test_capabilities_are_honest() -> None:
    caps = XmlLocalHandler().capabilities(ORDER_FORMAT)
    assert caps.projection is Support.FULL
    assert caps.predicate is Support.NONE
    assert caps.limit is Support.FULL
    assert caps.expressions == frozenset()
    assert caps.parameters is False
    assert caps.evidence and "<order>" in caps.evidence[0]


def test_other_formats_are_rejected() -> None:
    with pytest.raises(UnsupportedOperationError) as info:
        XmlLocalHandler().capabilities(CsvFormat())
    assert info.value.code is DiagnosticCode.FORMAT_UNSUPPORTED
    with pytest.raises(UnsupportedOperationError) as spec_info:
        XmlReaderSpecHandler().reader_spec(CsvFormat(), "file:///x.csv")
    assert spec_info.value.code is DiagnosticCode.FORMAT_UNSUPPORTED
    with pytest.raises(UnsupportedOperationError) as empty:
        XmlLocalHandler().capabilities(XmlFormat(value_tag=""))
    assert empty.value.code is DiagnosticCode.FORMAT_INVALID


def test_schema_is_inferred_from_the_file_and_the_handle_is_closed(
    storage: _TrackingStorage,
) -> None:
    handler = XmlLocalHandler()
    location = storage.resolve("orders.xml")
    assert handler.schema(storage, location, ORDER_FORMAT) == INFERRED_SCHEMA
    assert storage.all_closed


def test_declared_schema_wins(storage: _TrackingStorage) -> None:
    declared = Schema.of(("_id", IntegerType(32)), ("amount", DecimalType(10, 2)))
    fmt = XmlFormat(row_tag="order", schema=declared)
    assert XmlLocalHandler().schema(storage, storage.resolve("orders.xml"), fmt) is declared
    assert storage.handles == []


@pytest.mark.parametrize("batch_size", [True, False, 0, -1, 1.5, "64", None])
def test_scan_rejects_invalid_batch_size_before_opening_file(
    storage: _TrackingStorage, batch_size: Any
) -> None:
    fmt = XmlFormat(schema=INFERRED_SCHEMA)
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        XmlLocalHandler().scan(
            storage,
            storage.resolve("orders.xml"),
            fmt,
            PushedOperations(),
            {},
            batch_size=batch_size,
        )
    assert storage.handles == []


def test_scan_all_columns_converts_following_the_inferred_schema(
    storage: _TrackingStorage,
) -> None:
    handler = XmlLocalHandler()
    stream = handler.scan(
        storage, storage.resolve("orders.xml"), ORDER_FORMAT, PushedOperations(), {}, batch_size=2
    )
    assert from_arrow_schema(stream.schema) == INFERRED_SCHEMA
    rows = stream.read_all().to_pylist()
    assert rows == [
        {
            "_id": 1,
            "_priority": "high",
            "name": "alice",
            "amount": 10.5,
            "qty": 3,
            "day": dt.date(2024, 1, 1),
            "active": True,
            "address": {"city": "Paris", "zip": 75001},
            "tag": ["a", "b"],
            "note": None,
        },
        {
            "_id": 2,
            "_priority": None,
            "name": "bob",
            "amount": 20.0,
            "qty": 1,
            "day": dt.date(2024, 1, 2),
            "active": False,
            "address": {"city": "Berlin", "zip": None},
            "tag": ["c"],
            "note": None,
        },
        {
            "_id": 3,
            "_priority": None,
            "name": None,
            "amount": 5.25,
            "qty": None,
            "day": dt.date(2024, 1, 3),
            "active": True,
            "address": None,
            "tag": None,
            "note": {"_lang": "en", "_VALUE": "hello"},
        },
        {
            "_id": 4,
            "_priority": None,
            "name": "dave",
            "amount": 100.0,
            "qty": 7,
            "day": None,
            "active": None,
            "address": None,
            "tag": ["d"],
            "note": {"_lang": "fr", "_VALUE": "salut"},
        },
    ]
    assert stream.closed
    assert storage.all_closed


def test_scan_emits_only_the_projected_columns_and_stops_at_the_limit(
    storage: _TrackingStorage,
) -> None:
    handler = XmlLocalHandler()
    pushed = PushedOperations(projection=("qty", "name"), limit=2)
    stream = handler.scan(
        storage, storage.resolve("orders.xml"), ORDER_FORMAT, pushed, {}, batch_size=1
    )
    assert stream.schema.names == ["qty", "name"]
    batches = list(stream)
    assert len(batches) == 2
    rows = [row for batch in batches for row in batch.to_pylist()]
    assert rows == [{"qty": 3, "name": "alice"}, {"qty": 1, "name": "bob"}]
    assert storage.all_closed


def test_scan_with_declared_schema_converts_and_nulls_misfits(storage: _TrackingStorage) -> None:
    declared = Schema.of(
        ("_id", IntegerType(16)),
        ("amount", DecimalType(10, 2)),
        ("qty", BooleanType()),
        ("day", DateType()),
        ("tag", StringType()),
        ("missing", StringType()),
    )
    fmt = XmlFormat(row_tag="order", schema=declared)
    stream = XmlLocalHandler().scan(
        storage, storage.resolve("orders.xml"), fmt, PushedOperations(limit=2), {}, batch_size=10
    )
    assert from_arrow_schema(stream.schema) == declared
    assert stream.read_all().to_pylist() == [
        {
            "_id": 1,
            "amount": Decimal("10.50"),
            "qty": None,
            "day": dt.date(2024, 1, 1),
            "tag": None,
            "missing": None,
        },
        {
            "_id": 2,
            "amount": Decimal("20.00"),
            "qty": None,
            "day": dt.date(2024, 1, 2),
            "tag": "c",
            "missing": None,
        },
    ]


def test_scan_closes_the_handle_when_closed_early(storage: _TrackingStorage) -> None:
    stream = XmlLocalHandler().scan(
        storage, storage.resolve("orders.xml"), ORDER_FORMAT, PushedOperations(), {}, batch_size=1
    )
    assert not storage.all_closed
    stream.close()
    assert storage.all_closed
    assert list(stream) == []


def test_scan_refuses_a_pushed_predicate(storage: _TrackingStorage) -> None:
    pushed = PushedOperations(predicate=Comparison(ComparisonOp.GT, Column("qty"), Literal.of(1)))
    fmt = XmlFormat(row_tag="order", schema=INFERRED_SCHEMA)
    with pytest.raises(UnsupportedOperationError) as info:
        XmlLocalHandler().scan(
            storage, storage.resolve("orders.xml"), fmt, pushed, {}, batch_size=1
        )
    assert info.value.code is DiagnosticCode.SOURCE_SCAN_UNSUPPORTED
    assert storage.handles == []


def test_scan_reports_unknown_projected_columns(storage: _TrackingStorage) -> None:
    fmt = XmlFormat(row_tag="order", schema=INFERRED_SCHEMA)
    with pytest.raises(SourceError) as info:
        XmlLocalHandler().scan(
            storage,
            storage.resolve("orders.xml"),
            fmt,
            PushedOperations(projection=("nope",)),
            {},
            batch_size=1,
        )
    assert info.value.code is DiagnosticCode.PLAN_UNKNOWN_COLUMN
    assert storage.handles == []


def test_malformed_file_surfaces_as_source_error_and_releases_the_handle(tmp_path: Path) -> None:
    (tmp_path / "bad.xml").write_bytes(b"<r><row><id>1</id></row><row><id>2")
    storage = _TrackingStorage(tmp_path)
    handler = XmlLocalHandler()
    with pytest.raises(SourceError) as info:
        handler.schema(storage, storage.resolve("bad.xml"), XmlFormat())
    assert info.value.code is DiagnosticCode.FORMAT_INVALID
    assert storage.all_closed

    fmt = XmlFormat(schema=Schema.of(("id", IntegerType(64))))
    stream = handler.scan(
        storage, storage.resolve("bad.xml"), fmt, PushedOperations(), {}, batch_size=1
    )
    with pytest.raises(SourceError):
        stream.read_all()
    assert storage.all_closed


# -- redaction ------------------------------------------------------------------


def test_storage_failures_are_wrapped_and_redacted(tmp_path: Path) -> None:
    secret = "sv=2024-supersecret-signature-value"
    storage = _FailingStorage(tmp_path, secret)
    handler = XmlLocalHandler()
    with pytest.raises(StorageError) as info:
        handler.schema(storage, storage.resolve("orders.xml"), ORDER_FORMAT)
    assert secret not in str(info.value)
    assert secret not in repr(info.value)
    assert "***" in str(info.value)
    assert secret not in repr(handler)
    assert secret not in repr(storage._secrets)


# -- reader spec --------------------------------------------------------------------


def test_reader_spec_maps_options_and_names_the_connector() -> None:
    spec = XmlReaderSpecHandler().reader_spec(
        ORDER_FORMAT, "abfss://c@a.dfs.core.windows.net/x.xml"
    )
    assert spec.format == "xml"
    assert spec.options == {"rowTag": "order", "attributePrefix": "_", "valueTag": "_VALUE"}
    assert spec.schema is None
    assert spec.requires == (SPARK_XML_REQUIREMENT,)
    assert "spark-xml" in SPARK_XML_REQUIREMENT

    declared = Schema.of(("_id", IntegerType(64)))
    fmt = XmlFormat(
        row_tag="order", root_tag="orders", attribute_prefix="@", value_tag="v", schema=declared
    )
    spec = XmlReaderSpecHandler().reader_spec(fmt, "s3a://bucket/x.xml")
    assert spec.options == {
        "rowTag": "order",
        "attributePrefix": "@",
        "valueTag": "v",
        "rootTag": "orders",
    }
    assert spec.schema is declared
    assert spec.to_dict()["schema"] == declared.to_dict()


# -- end to end --------------------------------------------------------------------


def _explain_nodes(explain: Any) -> dict[str, Any]:
    return {node.operation: node for node in explain.nodes}


def test_end_to_end_with_the_context_and_duckdb(xml_dir: Path) -> None:
    import invariantql as iql

    with iql.Context() as ctx:
        storage = iql.local_storage(xml_dir)
        ctx.register_source(
            iql.file_source("orders", storage, "orders.xml", XmlFormat(row_tag="order"))
        )

        query = ctx.sql("SELECT name, qty FROM orders WHERE qty > 1 LIMIT 2")
        nodes = _explain_nodes(query.explain())
        assert nodes["scan"].disposition is Disposition.PUSHED
        assert "ElementTree" in nodes["scan"].evidence[0]
        assert nodes["filter"].disposition is Disposition.RESIDUAL
        assert nodes["filter"].reason_code is DiagnosticCode.RESIDUAL_NO_CAPABILITY
        assert nodes["project"].disposition is Disposition.PUSHED
        assert nodes["limit"].disposition is Disposition.RESIDUAL
        assert _rows(query.execute()) == [{"name": "alice", "qty": 3}, {"name": "dave", "qty": 7}]

        plain = ctx.sql("SELECT _id, name FROM orders LIMIT 2")
        nodes = _explain_nodes(plain.explain())
        assert nodes["project"].disposition is Disposition.PUSHED
        assert nodes["limit"].disposition is Disposition.PUSHED
        assert _rows(plain.execute()) == [{"_id": 1, "name": "alice"}, {"_id": 2, "name": "bob"}]

        # three-valued logic: the NULL qty of order 3 is excluded by <>
        assert _rows(ctx.sql("SELECT _id FROM orders WHERE qty <> 3").execute()) == [
            {"_id": 2},
            {"_id": 4},
        ]
        assert _rows(ctx.sql("SELECT _id FROM orders WHERE qty IS NULL").execute()) == [{"_id": 3}]
        assert _rows(ctx.sql("SELECT _id FROM orders WHERE name LIKE 'a%'").execute()) == [
            {"_id": 1}
        ]

        computed = ctx.sql("SELECT name AS n, qty * 2 AS twice FROM orders WHERE active")
        nodes = _explain_nodes(computed.explain())
        assert nodes["project"].disposition is Disposition.PARTIAL
        assert _rows(computed.execute()) == [
            {"n": "alice", "twice": 6},
            {"n": None, "twice": None},
        ]

        assert query.schema().names == ("name", "qty")


def test_end_to_end_with_an_explicitly_registered_handler_and_parameters(xml_dir: Path) -> None:
    import invariantql as iql
    from invariantql.adapters.duckdb_engine import DuckDBEngine
    from invariantql.adapters.sources.file_source import FileSource

    engine = DuckDBEngine()
    engine.register_format_handler(XmlLocalHandler())
    assert "xml" in engine.format_handlers
    with iql.Context() as ctx:
        ctx.register_engine(engine, replace=True)
        source = FileSource(
            "orders", LocalStorage(xml_dir), "orders.xml", XmlFormat(row_tag="order")
        )
        ctx.register_source(source)
        assert engine.reachability(source).reachable
        assert engine.scan_capabilities(source).predicate is Support.NONE

        query = ctx.sql("SELECT name FROM orders WHERE amount >= :threshold")
        assert query.parameters == ("threshold",)
        rows = _rows(query.execute(engine="duckdb", params={"threshold": 20}))
        assert rows == [{"name": "bob"}, {"name": "dave"}]
        preview = _rows(query.preview(1, params={"threshold": 20}))
        assert preview == [{"name": "bob"}]


@pytest.mark.integration
@pytest.mark.spark
@pytest.mark.skipif(
    not os.environ.get("INVARIANTQL_INTEGRATION"),
    reason="set INVARIANTQL_INTEGRATION=1 to run live integration tests",
)
def test_spark_reads_xml_through_the_reader_spec(spark: Any, xml_dir: Path) -> None:
    import invariantql as iql
    from invariantql.adapters.spark_engine import SparkEngine

    engine = SparkEngine(spark)
    engine.register_format_handler(XmlReaderSpecHandler())
    with iql.Context() as ctx:
        ctx.register_engine(engine)
        ctx.register_source(
            iql.file_source(
                "orders", iql.local_storage(xml_dir), "orders.xml", XmlFormat(row_tag="order")
            )
        )
        query = ctx.sql("SELECT name, qty FROM orders WHERE qty > 1")
        try:
            df = query.compile(engine="spark")
            rows = sorted((r["name"], r["qty"]) for r in df.collect())
        except Exception as exc:
            message = str(exc)
            if "xml" in message.lower() or "ClassNotFound" in message:
                pytest.skip(f"no XML data source on this Spark: {SPARK_XML_REQUIREMENT}")
            raise
    assert rows == [("alice", 3), ("dave", 7)]
