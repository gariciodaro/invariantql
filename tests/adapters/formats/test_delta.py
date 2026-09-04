"""Delta Lake format handlers: delta-rs locally, a ReaderSpec for Spark.

Unit tests write small Delta tables into a temporary directory with
``deltalake.write_deltalake`` and never touch the network. The integration
test at the bottom opens a remote table and runs only when
``INVARIANTQL_INTEGRATION`` and ``INVARIANTQL_DELTA_TABLE_URI`` are set
(optionally ``INVARIANTQL_DELTA_STORAGE_OPTIONS`` as a JSON object of
canonical storage-option keys).
"""

from __future__ import annotations

import datetime as dt
import json
import os
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, BinaryIO

import pyarrow as pa
import pytest

deltalake = pytest.importorskip("deltalake")

from deltalake import DeltaTable, write_deltalake  # noqa: E402
from deltalake.exceptions import DeltaError  # noqa: E402

import invariantql as iql  # noqa: E402
from invariantql.adapters._shared.arrow import to_arrow_schema  # noqa: E402
from invariantql.adapters.formats import delta as delta_module  # noqa: E402
from invariantql.adapters.formats.delta import (  # noqa: E402
    PUSHABLE_EXPRESSIONS,
    DeltaLocalHandler,
    DeltaReaderSpecHandler,
    delta_storage_options,
    delta_table_uri,
    translate_predicate,
)
from invariantql.adapters.storage.local import LocalStorage  # noqa: E402
from invariantql.domain.capabilities import Support  # noqa: E402
from invariantql.domain.credentials import EMPTY_SECRETS, SecretOptions  # noqa: E402
from invariantql.domain.diagnostics import (  # noqa: E402
    DiagnosticCode,
    ParameterError,
    SourceError,
    StorageError,
    UnsupportedOperationError,
)
from invariantql.domain.execution import PushedOperations  # noqa: E402
from invariantql.domain.explain import Disposition  # noqa: E402
from invariantql.domain.expressions import (  # noqa: E402
    And,
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
from invariantql.domain.formats import DeltaFormat, ParquetFormat  # noqa: E402
from invariantql.domain.location import Location  # noqa: E402
from invariantql.ports.storage import ObjectInfo, StorageCapabilities  # noqa: E402

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

SECRET = "SuperSecretAccountKey123456=="
LATEST = DeltaFormat()


# -- fixtures -----------------------------------------------------------------


@pytest.fixture(scope="module")
def delta_root(tmp_path_factory, sample_rows, sample_schema) -> Path:
    """A Delta table with two versions: rows 1-3 at version 0, rows 4-6 appended at version 1."""

    root = tmp_path_factory.mktemp("delta")
    table = pa.Table.from_pylist(sample_rows, schema=to_arrow_schema(sample_schema))
    write_deltalake(root / "orders", table.slice(0, 3), mode="error")
    time.sleep(0.05)  # keep the two commit timestamps distinct for time travel
    write_deltalake(root / "orders", table.slice(3, 3), mode="append")
    write_deltalake(root / "orders_by_active", table, partition_by=["active"])
    return root


@pytest.fixture(scope="module")
def storage(delta_root) -> LocalStorage:
    return LocalStorage(delta_root)


@pytest.fixture()
def handler() -> DeltaLocalHandler:
    return DeltaLocalHandler()


@pytest.fixture()
def arrow_schema(sample_schema) -> pa.Schema:
    return to_arrow_schema(sample_schema)


def _ids(stream) -> list[int]:
    table = stream.read_all()
    return sorted(table.column("id").to_pylist())


def _scan(
    handler,
    storage,
    fmt=LATEST,
    *,
    predicate=None,
    projection=None,
    limit=None,
    params=None,
    batch_size=100,
):
    pushed = PushedOperations(projection=projection, predicate=predicate, limit=limit)
    return handler.scan(
        storage, storage.resolve("orders"), fmt, pushed, params or {}, batch_size=batch_size
    )


class _UriStorage:
    """A Storage double that only knows its native URI and options."""

    def __init__(self, uri: str | None, options: SecretOptions = EMPTY_SECRETS) -> None:
        self._uri = uri
        self._options = options

    name = "fake-storage"
    capabilities = StorageCapabilities(engine_visible_uri=True)

    def resolve(self, path: str | Location) -> Location:
        return path if isinstance(path, Location) else Location(path)

    def open_read(self, location: Location) -> BinaryIO:
        raise NotImplementedError

    def info(self, location: Location) -> ObjectInfo:
        raise NotImplementedError

    def list(self, location: Location, *, recursive: bool = False) -> Iterator[ObjectInfo]:
        raise NotImplementedError

    def exists(self, location: Location) -> bool:
        return True

    def native_uri(self, location: Location) -> str | None:
        return self._uri

    def native_options(self) -> SecretOptions:
        return self._options


# -- capabilities and schema ---------------------------------------------------


def test_format_name_and_capabilities(handler):
    assert handler.format_name == "delta"
    caps = handler.capabilities(DeltaFormat())
    assert caps.projection is Support.FULL
    assert caps.predicate is Support.FULL
    assert caps.limit is Support.NONE
    assert caps.parameters is True
    assert caps.expressions == PUSHABLE_EXPRESSIONS
    assert ExpressionKind.LIKE not in caps.expressions
    assert ExpressionKind.ARITHMETIC not in caps.expressions
    assert ExpressionKind.ALIAS not in caps.expressions
    assert caps.evidence


def test_rejects_other_formats(handler):
    with pytest.raises(UnsupportedOperationError) as info:
        handler.capabilities(ParquetFormat())
    assert info.value.code is DiagnosticCode.FORMAT_INVALID


def test_schema_from_delta_log(handler, storage, sample_schema):
    schema = handler.schema(storage, storage.resolve("orders"), DeltaFormat())
    assert schema.to_dict() == sample_schema.to_dict()


def test_schema_of_partitioned_table_matches_scan(handler, storage, sample_schema):
    location = storage.resolve("orders_by_active")
    schema = handler.schema(storage, location, DeltaFormat())
    assert set(schema.names) == set(sample_schema.names)
    stream = handler.scan(storage, location, DeltaFormat(), PushedOperations(), {}, batch_size=10)
    assert stream.schema == to_arrow_schema(schema)
    assert sorted(stream.read_all().column("id").to_pylist()) == [1, 2, 3, 4, 5, 6]


@pytest.mark.parametrize("batch_size", [True, False, 0, -1, 1.5, "64", None])
def test_scan_rejects_invalid_batch_size_before_opening_table(
    handler, storage, monkeypatch, batch_size
) -> None:
    def must_not_open(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("table was opened")

    monkeypatch.setattr(handler, "_dataset", must_not_open)
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        handler.scan(
            storage,
            storage.resolve("orders"),
            DeltaFormat(),
            PushedOperations(),
            {},
            batch_size=batch_size,
        )


# -- filter translation ---------------------------------------------------------


def test_translation_of_comparison_and_boolean_composition(arrow_schema):
    expr = Or(
        (
            And(
                (
                    Comparison(ComparisonOp.GT, Column("id"), Literal.of(1)),
                    Comparison(ComparisonOp.EQ, Column("name"), Literal.of("bob")),
                )
            ),
            Not(Comparison(ComparisonOp.LT, Column("qty"), Literal.of(3))),
        )
    )
    text = str(translate_predicate(expr, arrow_schema))
    assert text == '(((id > 1) and (name == "bob")) or invert((qty < 3)))'


def test_translation_of_is_null_and_in(arrow_schema):
    assert (
        str(translate_predicate(IsNull(Column("name")), arrow_schema))
        == "is_null(name, {nan_is_null=false})"
    )
    assert (
        str(translate_predicate(IsNull(Column("name"), negated=True), arrow_schema))
        == "is_valid(name)"
    )
    membership = str(
        translate_predicate(In(Column("qty"), (Literal.of(1), Literal.of(3))), arrow_schema)
    )
    assert membership.startswith("if_else(is_valid(qty), is_in(qty, {value_set=int64:[")
    with_null = str(
        translate_predicate(In(Column("qty"), (Literal.of(1), Literal.of(None))), arrow_schema)
    )
    assert with_null.startswith("if_else(is_in(qty, {value_set=int64:[")
    assert with_null.endswith("true, null[bool])")
    negated = str(
        translate_predicate(In(Column("qty"), (Literal.of(1),), negated=True), arrow_schema)
    )
    assert negated.startswith("invert(if_else(is_valid(qty)")


def test_translation_substitutes_parameters(arrow_schema):
    expr = Comparison(ComparisonOp.GT, Column("amount"), Parameter("min_amount"))
    text = str(translate_predicate(expr, arrow_schema, {"min_amount": Literal.of(7.5)}))
    assert text == "(amount > 7.5)"
    with pytest.raises(ParameterError) as info:
        translate_predicate(expr, arrow_schema, {})
    assert info.value.code is DiagnosticCode.PARAMETER_MISSING


def test_translation_casts_literals_only_when_lossless(arrow_schema):
    # an integer literal against a decimal(10,2) column takes the column's scale
    assert (
        str(
            translate_predicate(
                Comparison(ComparisonOp.GT, Column("price"), Literal.of(2)), arrow_schema
            )
        )
        == "(price > 2.00)"
    )
    # a float literal against an integer column is never truncated
    assert (
        str(
            translate_predicate(
                Comparison(ComparisonOp.LT, Column("id"), Literal.of(2.5)), arrow_schema
            )
        )
        == "(id < 2.5)"
    )
    # literal on the left works the same way
    assert (
        str(
            translate_predicate(
                Comparison(ComparisonOp.LT, Literal.of(2), Column("price")), arrow_schema
            )
        )
        == "(2.00 < price)"
    )
    # a NULL literal takes the column type
    assert (
        str(
            translate_predicate(
                Comparison(ComparisonOp.EQ, Column("id"), Literal.of(None)), arrow_schema
            )
        )
        == "(id == null[int64])"
    )


def test_translation_rejects_undeclared_kinds(arrow_schema):
    with pytest.raises(UnsupportedOperationError) as info:
        translate_predicate(Like(Column("name"), Literal.of("a%")), arrow_schema)
    assert info.value.code is DiagnosticCode.FORMAT_UNSUPPORTED


# -- scan semantics --------------------------------------------------------------


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        (
            Comparison(ComparisonOp.NE, Column("qty"), Literal.of(1)),
            [1, 4, 5, 6],
        ),  # NULL <> 1 is unknown
        (
            Not(Comparison(ComparisonOp.EQ, Column("qty"), Literal.of(1))),
            [1, 4, 5, 6],
        ),  # NOT unknown is unknown
        (In(Column("qty"), (Literal.of(1), Literal.of(3))), [1, 2]),
        (
            In(Column("qty"), (Literal.of(1), Literal.of(3)), negated=True),
            [4, 5, 6],
        ),  # NULL NOT IN (...) excluded
        (Not(In(Column("qty"), (Literal.of(1), Literal.of(3)), negated=True)), [1, 2]),
        (In(Column("qty"), (Literal.of(1), Literal.of(None))), [2]),
        (In(Column("qty"), (Literal.of(1), Literal.of(None)), negated=True), []),  # always unknown
        (Comparison(ComparisonOp.EQ, Column("name"), Literal.of("alice")), [1]),  # case-sensitive
        (Comparison(ComparisonOp.GT, Column("name"), Literal.of("b")), [2, 3, 4]),
        (IsNull(Column("name")), [5]),
        (IsNull(Column("name"), negated=True), [1, 2, 3, 4, 6]),
        (Comparison(ComparisonOp.GT, Column("day"), Literal.of(dt.date(2024, 1, 2))), [3, 4, 6]),
        (Comparison(ComparisonOp.GT, Column("price"), Literal.of(2)), [2, 3, 5]),
        (Comparison(ComparisonOp.GE, Column("price"), Literal.of(Decimal("2.2"))), [2, 3, 5]),
        (Comparison(ComparisonOp.GT, Column("amount"), Literal.of(Decimal("7.0"))), [1, 2, 4]),
        (Comparison(ComparisonOp.LT, Column("id"), Literal.of(2.5)), [1, 2]),
        (Comparison(ComparisonOp.EQ, Column("active"), Literal.of(True)), [1, 3, 6]),
        (Comparison(ComparisonOp.EQ, Column("id"), Literal.of(None)), []),
        (
            And(
                (
                    Comparison(ComparisonOp.GT, Column("amount"), Literal.of(5.0)),
                    Or(
                        (
                            IsNull(Column("qty")),
                            Comparison(ComparisonOp.GE, Column("qty"), Literal.of(3)),
                        )
                    ),
                )
            ),
            [1, 3, 4],
        ),
    ],
)
def test_scan_predicate_semantics(handler, storage, predicate, expected):
    assert _ids(_scan(handler, storage, predicate=predicate)) == expected


def test_scan_projection_schema_and_batches(handler, storage, sample_schema):
    stream = _scan(handler, storage, projection=("name", "id"), batch_size=2)
    assert stream.schema == to_arrow_schema(sample_schema.select(["name", "id"]))
    batches = list(stream)
    assert batches and all(b.num_rows <= 2 for b in batches)
    assert sum(b.num_rows for b in batches) == 6
    assert stream.closed


def test_scan_without_projection_returns_every_column(handler, storage, sample_schema):
    stream = _scan(handler, storage)
    assert stream.schema == to_arrow_schema(sample_schema)
    stream.close()
    assert stream.closed


def test_scan_binds_parameters(handler, storage):
    predicate = Comparison(ComparisonOp.GT, Column("amount"), Parameter("min_amount"))
    assert _ids(
        _scan(handler, storage, predicate=predicate, params={"min_amount": Literal.of(7.0)})
    ) == [1, 2, 4]
    with pytest.raises(ParameterError):
        _scan(handler, storage, predicate=predicate)


def test_scan_applies_a_pushed_limit_defensively(handler, storage):
    stream = _scan(handler, storage, limit=2, batch_size=1)
    assert stream.read_all().num_rows == 2


# -- time travel ------------------------------------------------------------------


def test_version_time_travel(handler, storage, delta_root):
    assert _ids(_scan(handler, storage, DeltaFormat(version=0))) == [1, 2, 3]
    assert _ids(_scan(handler, storage, DeltaFormat(version=1))) == [1, 2, 3, 4, 5, 6]
    assert _ids(_scan(handler, storage)) == [1, 2, 3, 4, 5, 6]
    assert (
        handler.schema(storage, storage.resolve("orders"), DeltaFormat(version=0)).names[0] == "id"
    )


def test_timestamp_time_travel(handler, storage, delta_root):
    history = {
        entry["version"]: entry["timestamp"]
        for entry in DeltaTable(delta_root / "orders").history()
    }
    first_commit = dt.datetime.fromtimestamp(history[0] / 1000, tz=dt.timezone.utc)
    assert _ids(_scan(handler, storage, DeltaFormat(timestamp=first_commit.isoformat()))) == [
        1,
        2,
        3,
    ]
    naive_utc = first_commit.replace(tzinfo=None).isoformat()
    assert _ids(_scan(handler, storage, DeltaFormat(timestamp=naive_utc))) == [1, 2, 3]
    assert _ids(_scan(handler, storage, DeltaFormat(timestamp="2999-01-01T00:00:00Z"))) == [
        1,
        2,
        3,
        4,
        5,
        6,
    ]


def test_unknown_version_is_a_source_error(handler, storage):
    with pytest.raises(SourceError) as info:
        _scan(handler, storage, DeltaFormat(version=99))
    assert info.value.code is DiagnosticCode.SOURCE_FAILURE
    assert dict(info.value.diagnostic.details)["version"] == "99"


def test_missing_table_is_not_found(handler, storage):
    with pytest.raises(StorageError) as info:
        handler.schema(storage, storage.resolve("nope"), DeltaFormat())
    assert info.value.code is DiagnosticCode.STORAGE_OBJECT_NOT_FOUND


def test_storage_without_native_uri_is_unsupported(handler):
    with pytest.raises(UnsupportedOperationError) as info:
        handler.schema(_UriStorage(None), Location("/t"), DeltaFormat())
    assert info.value.code is DiagnosticCode.FORMAT_UNSUPPORTED
    assert "delta-rs" in str(info.value)


# -- URI / option mapping and redaction ---------------------------------------------


@pytest.mark.parametrize(
    ("native", "expected"),
    [
        ("file:///data/delta/orders", ("/data/delta/orders", {})),
        ("file:///data/with%20space/t", ("/data/with space/t", {})),
        (
            "abfss://lake@acct.dfs.core.windows.net/silver/orders",
            ("az://lake/silver/orders", {"azure_storage_account_name": "acct"}),
        ),
        (
            "wasbs://blobs@acct.blob.core.windows.net/t",
            ("az://blobs/t", {"azure_storage_account_name": "acct"}),
        ),
        ("abfss://lake/silver/orders", ("abfss://lake/silver/orders", {})),
        ("s3a://bucket/prefix/orders", ("s3://bucket/prefix/orders", {})),
        ("s3n://bucket/prefix/orders", ("s3://bucket/prefix/orders", {})),
        ("s3://bucket/orders", ("s3://bucket/orders", {})),
        ("gs://bucket/orders", ("gs://bucket/orders", {})),
    ],
)
def test_delta_table_uri_mapping(native, expected):
    assert delta_table_uri(native) == expected


def test_delta_storage_options_mapping():
    options = delta_storage_options(
        {
            "account_name": "acct",
            "account_key": "k" * 20,
            "sas_token": "sv=2020&sig=abc",
            "client_id": "cid",
            "client_secret": "csecret",
            "tenant_id": "tid",
            "anon": False,
            "unrelated": "ignored",
            "nothing": None,
        }
    )
    assert options == {
        "azure_storage_account_name": "acct",
        "azure_storage_account_key": "k" * 20,
        "azure_storage_sas_token": "sv=2020&sig=abc",
        "azure_client_id": "cid",
        "azure_client_secret": "csecret",
        "azure_tenant_id": "tid",
    }
    s3 = delta_storage_options(
        {
            "aws_access_key_id": "AKIA",
            "aws_secret_access_key": "s3cr3t",
            "aws_session_token": "tok",
            "aws_region": "eu-west-1",
            "aws_endpoint_url": "http://localhost:9000",
            "AWS_ALLOW_HTTP": True,
            "aws_anonymous": True,
        }
    )
    assert s3 == {
        "AWS_ACCESS_KEY_ID": "AKIA",
        "AWS_SECRET_ACCESS_KEY": "s3cr3t",
        "AWS_SESSION_TOKEN": "tok",
        "AWS_REGION": "eu-west-1",
        "AWS_ENDPOINT_URL": "http://localhost:9000",
        "AWS_ALLOW_HTTP": "true",
        "AWS_SKIP_SIGNATURE": "true",
    }
    assert (
        delta_storage_options({"aws_endpoint_url": "http://minio:9000"})["AWS_ALLOW_HTTP"] == "true"
    )
    assert "AWS_ALLOW_HTTP" not in delta_storage_options({"aws_endpoint_url": "https://s3.example"})
    assert delta_storage_options({"aws_allow_http": False}) == {"AWS_ALLOW_HTTP": "false"}


def test_delta_storage_options_preserve_azure_endpoint_and_connection_string() -> None:
    assert delta_storage_options(
        {"account_name": "acct", "endpoint_suffix": "core.usgovcloudapi.net"}
    ) == {
        "azure_storage_account_name": "acct",
        "azure_storage_endpoint": "https://acct.blob.core.usgovcloudapi.net",
    }
    connection = (
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=super-secret-account-key;EndpointSuffix=example.test"
    )
    assert delta_storage_options({"connection_string": connection}) == {
        "azure_storage_account_name": "devstoreaccount1",
        "azure_storage_account_key": "super-secret-account-key",
        "azure_storage_endpoint": "http://devstoreaccount1.blob.example.test",
    }
    assert delta_storage_options(
        {
            "connection_string": "AccountName=acct;SharedAccessSignature=?sv=1&sig=secret",
            "credential_kind": "anonymous",
        }
    ) == {
        "azure_storage_account_name": "acct",
        "azure_storage_sas_token": "sv=1&sig=secret",
        "azure_skip_signature": "true",
    }


def test_delta_storage_options_accept_provider_keys_case_insensitively() -> None:
    assert delta_storage_options(
        {"AZURE_STORAGE_ACCOUNT_NAME": "acct", "aws_skip_signature": "true"}
    ) == {"azure_storage_account_name": "acct", "AWS_SKIP_SIGNATURE": "true"}


def test_secrets_reach_delta_rs_but_never_errors(handler, monkeypatch):
    received: dict[str, Any] = {}

    class FakeDeltaTable:
        def __init__(self, table_uri, version=None, storage_options=None):
            received["uri"] = table_uri
            received["version"] = version
            received["options"] = dict(storage_options or {})
            raise DeltaError(
                f"connection refused for account_key={SECRET} (azure_storage_account_key: {SECRET})"
            )

    monkeypatch.setattr(delta_module, "DeltaTable", FakeDeltaTable)
    secrets = SecretOptions({"account_key": SECRET}, ref=None)
    storage = _UriStorage("abfss://lake@acct.dfs.core.windows.net/t", secrets)
    with pytest.raises(SourceError) as info:
        handler.schema(
            storage,
            Location("/t", "abfss", "lake@acct.dfs.core.windows.net"),
            DeltaFormat(version=3),
        )
    assert received == {
        "uri": "az://lake/t",
        "version": 3,
        "options": {"azure_storage_account_name": "acct", "azure_storage_account_key": SECRET},
    }
    assert SECRET not in str(info.value)
    assert SECRET not in repr(info.value)
    assert SECRET not in repr(info.value.diagnostic)
    assert SECRET not in repr(handler)
    assert SECRET not in repr(storage.native_options())
    assert info.value.code is DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE


def test_scan_errors_are_wrapped(handler, storage, monkeypatch):
    class BrokenScanner:
        def scanner(self, **kwargs):
            raise pa.ArrowInvalid("scanner exploded")

        schema = pa.schema([("id", pa.int64())])

    monkeypatch.setattr(handler, "_dataset", lambda *a, **k: BrokenScanner())
    with pytest.raises(SourceError) as info:
        _scan(handler, storage)
    assert "scanner exploded" in str(info.value)


# -- end to end through the facade ------------------------------------------------


@pytest.fixture()
def delta_ctx(delta_root, storage):
    context = iql.Context()
    context.register_source(iql.file_source("orders", storage, "orders", iql.DeltaFormat()))
    context.register_source(
        iql.file_source("orders_v0", storage, "orders", iql.DeltaFormat(version=0))
    )
    yield context
    context.close()


def _nodes(explain):
    return {node.operation: node for node in explain.nodes}


def test_context_registers_delta_handler(delta_ctx):
    assert "delta" in delta_ctx.engine("duckdb").format_handlers


def test_end_to_end_schema_and_full_scan(delta_ctx, sample_schema):
    query = delta_ctx.query("orders")
    assert query.schema().to_dict() == sample_schema.to_dict()
    rows = sorted(query.execute().rows(), key=lambda r: r["id"])
    assert [r["id"] for r in rows] == [1, 2, 3, 4, 5, 6]
    assert rows[0]["price"] == Decimal("1.10")
    assert rows[0]["day"] == dt.date(2024, 1, 1)
    assert delta_ctx.query("orders_v0").execute().to_arrow().num_rows == 3


def test_end_to_end_comparison_pushed_limit_residual(delta_ctx):
    query = delta_ctx.sql("SELECT id, name FROM orders WHERE qty <> 1 LIMIT 2")
    nodes = _nodes(query.explain())
    assert nodes["scan"].disposition is Disposition.PUSHED
    assert nodes["filter"].disposition is Disposition.PUSHED
    assert nodes["filter"].reason_code is DiagnosticCode.PUSHDOWN_FULL
    assert nodes["project"].disposition is Disposition.PUSHED
    assert nodes["limit"].disposition is Disposition.RESIDUAL
    assert nodes["limit"].reason_code is DiagnosticCode.RESIDUAL_NO_CAPABILITY
    plan = query.execution_plan()
    assert plan.pushed.projection == ("id", "name")
    assert plan.pushed.limit is None and plan.residual.limit == 2
    rows = query.execute().rows()
    assert len(rows) == 2
    assert {r["id"] for r in rows} <= {1, 4, 5, 6}
    assert set(rows[0]) == {"id", "name"}


def test_end_to_end_like_stays_residual(delta_ctx):
    query = delta_ctx.sql("SELECT id FROM orders WHERE name LIKE 'a%'")
    node = _nodes(query.explain())["filter"]
    assert node.disposition is Disposition.RESIDUAL
    assert node.reason_code is DiagnosticCode.RESIDUAL_UNSUPPORTED_EXPRESSION
    assert [r["id"] for r in query.execute().rows()] == [1]


def test_end_to_end_mixed_predicate_is_partial(delta_ctx):
    query = delta_ctx.sql(
        "SELECT id, name FROM orders WHERE qty <> 1 AND name LIKE 'a%' AND amount / 2 > 1 LIMIT 5"
    )
    nodes = _nodes(query.explain())
    assert nodes["filter"].disposition is Disposition.PARTIAL
    assert nodes["filter"].reason_code is DiagnosticCode.PUSHDOWN_PARTIAL
    assert nodes["filter"].pushed == "(qty <> 1)"
    assert "LIKE" in (nodes["filter"].residual or "")
    assert "/" in (nodes["filter"].residual or "")
    assert nodes["limit"].disposition is Disposition.RESIDUAL
    assert nodes["limit"].reason_code is DiagnosticCode.RESIDUAL_LIMIT_AFTER_RESIDUAL_FILTER
    assert [r["id"] for r in query.execute().rows()] == [1]


def test_end_to_end_parameters_and_in(delta_ctx):
    query = (
        delta_ctx.query("orders")
        .where((iql.col("amount") > iql.param("min_amount")) & iql.col("qty").isin([3, 7, 2]))
        .select("id")
    )
    node = _nodes(query.explain())["filter"]
    assert node.disposition is Disposition.PUSHED
    ids = sorted(r["id"] for r in query.execute(params={"min_amount": 7.0}).rows())
    assert ids == [1, 4]
    with pytest.raises(ParameterError):
        query.execute()


def test_end_to_end_null_semantics_match_duckdb(
    delta_ctx, storage, delta_root, sample_rows, sample_schema
):
    """The pushed evaluation must agree with DuckDB evaluating the same predicate as residual."""

    import pyarrow.parquet as pq

    pq.write_table(
        pa.Table.from_pylist(sample_rows, schema=to_arrow_schema(sample_schema)),
        delta_root / "orders.parquet",
    )
    delta_ctx.register_source(
        iql.file_source("orders_pq", storage, "orders.parquet", iql.ParquetFormat())
    )
    for where in (
        "qty <> 1",
        "NOT (qty = 1)",
        "qty NOT IN (1, 3)",
        "NOT (qty NOT IN (1, 3))",
        "qty IN (1, NULL)",
        "name = 'alice'",
        "name > 'b' OR qty IS NULL",
        "price > 2",
        "active = TRUE AND amount >= 5",
        "day > DATE '2024-01-02'",
    ):
        delta_ids = sorted(
            r["id"] for r in delta_ctx.sql(f"SELECT id FROM orders WHERE {where}").execute().rows()
        )
        duck_ids = sorted(
            r["id"]
            for r in delta_ctx.sql(f"SELECT id FROM orders_pq WHERE {where}").execute().rows()
        )
        assert delta_ids == duck_ids, where


# -- reader spec ----------------------------------------------------------------------


def test_reader_spec():
    handler = DeltaReaderSpecHandler()
    assert handler.format_name == "delta"
    spec = handler.reader_spec(DeltaFormat(version=3), "abfss://lake@acct.dfs.core.windows.net/t")
    assert spec.format == "delta"
    assert spec.options == {"versionAsOf": "3"}
    assert spec.schema is None
    assert spec.requires == ("io.delta:delta-spark_2.12 and the Delta Spark session extensions",)
    assert DeltaReaderSpecHandler().reader_spec(
        DeltaFormat(timestamp="2024-01-01T00:00:00Z"), "s3a://b/t"
    ).options == {"timestampAsOf": "2024-01-01T00:00:00Z"}
    assert DeltaReaderSpecHandler().reader_spec(DeltaFormat(), "file:///t").options == {}
    with pytest.raises(UnsupportedOperationError):
        handler.reader_spec(ParquetFormat(), "file:///t")
    assert spec.to_dict()["requires"] == list(spec.requires)


# -- integration ---------------------------------------------------------------------


@pytest.mark.integration
def test_remote_delta_table_integration():
    if not os.environ.get("INVARIANTQL_INTEGRATION") or not os.environ.get(
        "INVARIANTQL_DELTA_TABLE_URI"
    ):
        pytest.skip("set INVARIANTQL_INTEGRATION=1 and INVARIANTQL_DELTA_TABLE_URI to run")
    uri = os.environ["INVARIANTQL_DELTA_TABLE_URI"]
    raw = os.environ.get("INVARIANTQL_DELTA_STORAGE_OPTIONS", "")
    options = SecretOptions(json.loads(raw)) if raw else EMPTY_SECRETS
    storage = _UriStorage(uri, options)
    location = Location.parse(uri)
    handler = DeltaLocalHandler()
    schema = handler.schema(storage, location, DeltaFormat())
    assert len(schema) >= 1
    first = schema.names[0]
    stream = handler.scan(
        storage, location, DeltaFormat(), PushedOperations(projection=(first,)), {}, batch_size=16
    )
    assert stream.schema.names == [first]
    batches = list(stream)
    assert all(b.num_rows <= 16 for b in batches)
    assert stream.closed
