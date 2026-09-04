"""Tests for the Apache Iceberg format handlers (local pyiceberg scan and Spark reader spec)."""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO

import pyarrow as pa
import pytest

pytest.importorskip("pyiceberg")

from pyiceberg import expressions as _expressions
from pyiceberg.table import StaticTable

import invariantql as iql
from invariantql.adapters._shared.arrow import from_arrow_schema, to_arrow_schema
from invariantql.adapters.formats import iceberg as iceberg_module
from invariantql.adapters.formats.iceberg import (
    IcebergLocalHandler,
    IcebergReaderSpecHandler,
    _ArrowPredicate,
    _fileio_properties,
    _IcebergPredicate,
    _metadata_location,
)
from invariantql.adapters.storage.local import LocalStorage
from invariantql.domain.capabilities import Support
from invariantql.domain.credentials import EMPTY_SECRETS, SecretOptions
from invariantql.domain.diagnostics import (
    DiagnosticCode,
    InvariantQLError,
    ParameterError,
    SourceError,
    UnsupportedOperationError,
)
from invariantql.domain.execution import PushedOperations
from invariantql.domain.explain import Disposition
from invariantql.domain.expressions import (
    And,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    ComparisonOp,
    Expression,
    ExpressionKind,
    In,
    IsNull,
    Literal,
    Not,
    Or,
    Parameter,
)
from invariantql.domain.formats import CsvFormat, IcebergFormat
from invariantql.domain.location import Location
from invariantql.ports.format_handler import (
    DistributedFormatHandler,
    LocalFormatHandler,
)
from invariantql.ports.storage import ObjectInfo, StorageCapabilities

# pyiceberg expressions are pydantic models with keyword-only constructors that
# pyright cannot see through; treat the namespace as untyped in tests.
ice: Any = _expressions

# -- an Iceberg table on local disk ---------------------------------------------


@dataclass(frozen=True)
class IcebergFixture:
    path: Path
    snapshot_ids: tuple[int, ...]
    rows: list[dict[str, Any]]


def _write_iceberg_table(
    path: Path, rows: list[dict[str, Any]], schema: pa.Schema
) -> IcebergFixture:
    """Create a Hadoop-style Iceberg table with two appends (two snapshots).

    pyiceberg's in-memory/sql catalogs need SQLAlchemy, which is not installed,
    so a minimal catalog writes ``metadata/vN.metadata.json`` files and the
    ``version-hint.text`` pointer itself.
    """

    from pyiceberg.catalog import Catalog
    from pyiceberg.catalog.noop import NoopCatalog
    from pyiceberg.io import load_file_io
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
    from pyiceberg.serializers import ToOutputFile
    from pyiceberg.table import CommitTableResponse, Table
    from pyiceberg.table.metadata import new_table_metadata
    from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
    from pyiceberg.table.update import update_table_metadata

    class HadoopLikeCatalog(NoopCatalog):
        def __init__(self) -> None:
            super().__init__("hadoop-like")

        def commit_table(
            self, table: Table, requirements: Any, updates: Any
        ) -> CommitTableResponse:
            for requirement in requirements:
                requirement.validate(table.metadata)
            metadata = update_table_metadata(
                table.metadata, updates, metadata_location=table.metadata_location
            )
            match = re.search(r"/v(\d+)\.metadata\.json$", table.metadata_location)
            assert match is not None
            version = int(match.group(1)) + 1
            location = f"{table.location()}/metadata/v{version}.metadata.json"
            ToOutputFile.table_metadata(metadata, table.io.new_output(location), overwrite=True)
            hint = table.io.new_output(f"{table.location()}/metadata/version-hint.text")
            with hint.create(overwrite=True) as out:
                out.write(str(version).encode("utf-8"))
            return CommitTableResponse(metadata=metadata, **{"metadata-location": location})

    path.mkdir(parents=True)
    location = path.as_uri()
    iceberg_schema = Catalog._convert_schema_if_needed(schema)
    metadata = new_table_metadata(
        iceberg_schema, UNPARTITIONED_PARTITION_SPEC, UNSORTED_SORT_ORDER, location, {}
    )
    io = load_file_io({}, location)
    metadata_location = f"{location}/metadata/v1.metadata.json"
    ToOutputFile.table_metadata(metadata, io.new_output(metadata_location), overwrite=True)
    with io.new_output(f"{location}/metadata/version-hint.text").create(overwrite=True) as out:
        out.write(b"1")
    table = Table(("db", "orders"), metadata, metadata_location, io, HadoopLikeCatalog())
    table.append(pa.Table.from_pylist(rows[:3], schema=schema))
    table.append(pa.Table.from_pylist(rows[3:], schema=schema))
    snapshot_ids = tuple(s.snapshot_id for s in table.snapshots())
    assert len(snapshot_ids) == 2
    return IcebergFixture(path, snapshot_ids, rows)


@pytest.fixture(scope="module")
def iceberg_table(tmp_path_factory, sample_rows, sample_schema) -> IcebergFixture:
    root = tmp_path_factory.mktemp("iceberg")
    return _write_iceberg_table(root / "orders", sample_rows, to_arrow_schema(sample_schema))


@pytest.fixture(scope="module")
def iceberg_table_without_hint(tmp_path_factory, iceberg_table) -> Path:
    """A copy of the table whose ``version-hint.text`` was removed (catalog-style directory)."""

    root = tmp_path_factory.mktemp("iceberg-nohint")
    target = root / "orders"
    shutil.copytree(iceberg_table.path, target)
    (target / "metadata" / "version-hint.text").unlink()
    return target


@pytest.fixture(scope="module")
def iceberg_schema(iceberg_table):
    return StaticTable.from_metadata(iceberg_table.path.as_uri()).schema()


@pytest.fixture()
def storage(iceberg_table) -> LocalStorage:
    return LocalStorage(iceberg_table.path.parent)


@pytest.fixture()
def handler() -> IcebergLocalHandler:
    return IcebergLocalHandler()


def _scan(
    handler: IcebergLocalHandler,
    storage: Any,
    location: Location,
    fmt: IcebergFormat | None = None,
    *,
    projection: tuple[str, ...] | None = None,
    predicate: Expression | None = None,
    limit: int | None = None,
    parameters: dict[str, Literal] | None = None,
    batch_size: int = 1024,
) -> pa.Table:
    stream = handler.scan(
        storage,
        location,
        fmt or IcebergFormat(),
        PushedOperations(projection, predicate, limit),
        parameters or {},
        batch_size=batch_size,
    )
    return pa.Table.from_batches(list(stream), stream.schema)


def _ids(table: pa.Table) -> list[int]:
    return sorted(table.column("id").to_pylist())


# -- storage doubles ------------------------------------------------------------


class _NoNativeUriStorage:
    """A storage (like SFTP) that pyiceberg cannot read itself."""

    name = "opaque"
    capabilities = StorageCapabilities(listing=True)

    def __init__(self, inner: LocalStorage) -> None:
        self._inner = inner

    def resolve(self, path: str | Location) -> Location:
        return self._inner.resolve(path)

    def open_read(self, location: Location) -> BinaryIO:
        return self._inner.open_read(location)

    def info(self, location: Location) -> ObjectInfo:
        return self._inner.info(location)

    def list(self, location: Location, *, recursive: bool = False) -> Iterator[ObjectInfo]:
        return self._inner.list(location, recursive=recursive)

    def exists(self, location: Location) -> bool:
        return self._inner.exists(location)

    def native_uri(self, location: Location) -> str | None:
        return None

    def native_options(self) -> SecretOptions:
        return EMPTY_SECRETS


class _SecretStorage(LocalStorage):
    """Local files, but with object-store style credentials attached."""

    def __init__(self, root: Path, secrets: SecretOptions) -> None:
        super().__init__(root, name="secret-storage")
        self._secrets = secrets

    def native_options(self) -> SecretOptions:
        return self._secrets


# -- ports and capabilities ---------------------------------------------------------


def test_handlers_conform_to_the_format_handler_ports() -> None:
    local = IcebergLocalHandler()
    distributed = IcebergReaderSpecHandler()
    assert isinstance(local, LocalFormatHandler)
    assert isinstance(distributed, DistributedFormatHandler)
    assert local.format_name == "iceberg"
    assert distributed.format_name == "iceberg"


def test_capabilities_declare_only_kinds_the_scan_evaluates(handler) -> None:
    caps = handler.capabilities(IcebergFormat())
    assert caps.projection is Support.FULL
    assert caps.predicate is Support.FULL
    assert caps.limit is Support.FULL
    assert caps.parameters is True
    assert ExpressionKind.LIKE not in caps.expressions
    assert ExpressionKind.ARITHMETIC not in caps.expressions
    assert ExpressionKind.ALIAS not in caps.expressions
    assert caps.expressions == {
        ExpressionKind.COLUMN,
        ExpressionKind.LITERAL,
        ExpressionKind.PARAMETER,
        ExpressionKind.COMPARISON,
        ExpressionKind.AND,
        ExpressionKind.OR,
        ExpressionKind.NOT,
        ExpressionKind.IS_NULL,
        ExpressionKind.IN,
    }
    assert caps.evidence


def test_other_formats_are_refused(handler) -> None:
    with pytest.raises(UnsupportedOperationError) as info:
        handler.capabilities(CsvFormat())
    assert info.value.code is DiagnosticCode.FORMAT_UNSUPPORTED
    with pytest.raises(UnsupportedOperationError):
        IcebergReaderSpecHandler().reader_spec(CsvFormat(), "file:///x")


# -- schema ---------------------------------------------------------------------


def test_schema_maps_iceberg_types_to_domain_types(handler, storage, sample_schema) -> None:
    schema = handler.schema(storage, storage.resolve("orders"), IcebergFormat())
    assert schema == sample_schema


# -- metadata discovery -----------------------------------------------------------


def test_version_hint_selects_the_current_metadata_file(storage, iceberg_table) -> None:
    location = storage.resolve("orders")
    found = _metadata_location(storage, location, IcebergFormat())
    assert found.uri == (iceberg_table.path / "metadata" / "v3.metadata.json").as_uri()


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("2", "v2.metadata.json"),
        ("2\n", "v2.metadata.json"),
        ("v2.metadata.json", "v2.metadata.json"),
        ("00002-abc", "00002-abc.metadata.json"),
    ],
)
def test_version_hint_content_variants(tmp_path, content, expected) -> None:
    (tmp_path / "t" / "metadata").mkdir(parents=True)
    (tmp_path / "t" / "metadata" / "version-hint.text").write_text(content)
    local = LocalStorage(tmp_path)
    found = _metadata_location(local, local.resolve("t"), IcebergFormat())
    assert found.name == expected


def test_empty_version_hint_is_invalid(tmp_path) -> None:
    (tmp_path / "t" / "metadata").mkdir(parents=True)
    (tmp_path / "t" / "metadata" / "version-hint.text").write_text("  \n")
    local = LocalStorage(tmp_path)
    with pytest.raises(SourceError) as info:
        _metadata_location(local, local.resolve("t"), IcebergFormat())
    assert info.value.code is DiagnosticCode.FORMAT_INVALID


@pytest.mark.parametrize(
    "content",
    ["../v1", "..\\v1", "v1/../../outside", ".", "..", "https://example.test/meta"],
)
def test_version_hint_cannot_escape_the_metadata_directory(tmp_path, content) -> None:
    metadata = tmp_path / "t" / "metadata"
    metadata.mkdir(parents=True)
    (metadata / "version-hint.text").write_text(content)
    local = LocalStorage(tmp_path)
    with pytest.raises(SourceError) as info:
        _metadata_location(local, local.resolve("t"), IcebergFormat())
    assert info.value.code is DiagnosticCode.FORMAT_INVALID


def test_version_hint_must_be_utf8(tmp_path) -> None:
    metadata = tmp_path / "t" / "metadata"
    metadata.mkdir(parents=True)
    (metadata / "version-hint.text").write_bytes(b"\xff\xfe")
    local = LocalStorage(tmp_path)
    with pytest.raises(SourceError) as info:
        _metadata_location(local, local.resolve("t"), IcebergFormat())
    assert info.value.code is DiagnosticCode.FORMAT_INVALID


def test_listing_fallback_picks_the_highest_version(handler, iceberg_table_without_hint) -> None:
    local = LocalStorage(iceberg_table_without_hint.parent)
    location = local.resolve("orders")
    found = _metadata_location(local, location, IcebergFormat())
    assert found.name == "v3.metadata.json"
    assert _ids(_scan(handler, local, location)) == [1, 2, 3, 4, 5, 6]


def test_listing_fallback_orders_by_numeric_version(tmp_path) -> None:
    metadata = tmp_path / "t" / "metadata"
    metadata.mkdir(parents=True)
    for name in ("v9.metadata.json", "v10.metadata.json", "notes.txt", "snap-1.avro"):
        (metadata / name).write_text("{}")
    (metadata / "subdir").mkdir()
    local = LocalStorage(tmp_path)
    found = _metadata_location(local, local.resolve("t"), IcebergFormat())
    assert found.name == "v10.metadata.json"


def test_listing_fallback_without_metadata_files_is_invalid(tmp_path) -> None:
    (tmp_path / "t" / "metadata").mkdir(parents=True)
    local = LocalStorage(tmp_path)
    with pytest.raises(SourceError) as info:
        _metadata_location(local, local.resolve("t"), IcebergFormat())
    assert info.value.code is DiagnosticCode.FORMAT_INVALID


def test_missing_table_directory_raises_a_diagnostic(handler, tmp_path) -> None:
    local = LocalStorage(tmp_path)
    with pytest.raises(InvariantQLError) as info:
        handler.schema(local, local.resolve("missing"), IcebergFormat())
    assert info.value.code in (
        DiagnosticCode.STORAGE_OBJECT_NOT_FOUND,
        DiagnosticCode.FORMAT_INVALID,
    )


def test_explicit_relative_metadata_location_pins_a_version(handler, storage) -> None:
    location = storage.resolve("orders")
    fmt = IcebergFormat(metadata_location="metadata/v2.metadata.json")
    assert _metadata_location(storage, location, fmt).name == "v2.metadata.json"
    assert _ids(_scan(handler, storage, location, fmt)) == [1, 2, 3]


def test_explicit_absolute_metadata_uri(handler, storage, iceberg_table) -> None:
    location = storage.resolve("orders")
    uri = (iceberg_table.path / "metadata" / "v2.metadata.json").as_uri()
    fmt = IcebergFormat(metadata_location=uri)
    assert _metadata_location(storage, location, fmt).uri == uri
    assert _ids(_scan(handler, storage, location, fmt)) == [1, 2, 3]


def test_explicit_absolute_metadata_path(handler, storage, iceberg_table) -> None:
    location = storage.resolve("orders")
    path = str(iceberg_table.path / "metadata" / "v2.metadata.json")
    fmt = IcebergFormat(metadata_location=path)
    assert _metadata_location(storage, location, fmt).path == path
    assert _ids(_scan(handler, storage, location, fmt)) == [1, 2, 3]


def test_location_may_point_at_a_metadata_file(handler, storage) -> None:
    location = storage.resolve("orders/metadata/v2.metadata.json")
    assert _metadata_location(storage, location, IcebergFormat()) == location
    assert _ids(_scan(handler, storage, location)) == [1, 2, 3]


def test_storage_without_native_uri_is_unsupported(handler, storage) -> None:
    opaque = _NoNativeUriStorage(storage)
    with pytest.raises(UnsupportedOperationError) as info:
        handler.schema(opaque, storage.resolve("orders"), IcebergFormat())
    assert info.value.code is DiagnosticCode.FORMAT_UNSUPPORTED


def test_corrupt_metadata_is_reported_as_invalid(handler, tmp_path) -> None:
    (tmp_path / "t" / "metadata").mkdir(parents=True)
    (tmp_path / "t" / "metadata" / "v1.metadata.json").write_text("not json")
    (tmp_path / "t" / "metadata" / "version-hint.text").write_text("1")
    local = LocalStorage(tmp_path)
    with pytest.raises(SourceError) as info:
        handler.schema(local, local.resolve("t"), IcebergFormat())
    assert info.value.code is DiagnosticCode.FORMAT_INVALID


# -- credentials ----------------------------------------------------------------


def test_canonical_storage_options_become_fileio_properties() -> None:
    options = SecretOptions(
        {
            "aws_access_key_id": "AKIAEXAMPLEKEY123",
            "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "aws_session_token": "sessiontoken1234",
            "aws_region": "eu-west-1",
            "aws_endpoint_url": "http://localhost:9000",
            "aws_anonymous": "true",
            "account_name": "myaccount",
            "account_key": "accountkey1234567890",
            "sas_token": "sv=2020&sig=abcdefgh",
            "client_id": "client-id-1234",
            "client_secret": "client-secret-1234",
            "tenant_id": "tenant-1234",
            "connection_string": "DefaultEndpointsProtocol=https;AccountName=x",
            "endpoint_suffix": "core.usgovcloudapi.net",
            "s3.proxy-uri": "http://proxy:3128",
            "anon": False,
            "unrelated": "value",
            "empty": None,
        }
    )
    properties = _fileio_properties(options)
    assert properties == {
        "s3.access-key-id": "AKIAEXAMPLEKEY123",
        "s3.secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "s3.session-token": "sessiontoken1234",
        "s3.region": "eu-west-1",
        "s3.endpoint": "http://localhost:9000",
        "s3.anonymous": "true",
        "adls.account-name": "myaccount",
        "adls.account-key": "accountkey1234567890",
        "adls.sas-token": "sv=2020&sig=abcdefgh",
        "adls.client-id": "client-id-1234",
        "adls.client-secret": "client-secret-1234",
        "adls.tenant-id": "tenant-1234",
        "adls.connection-string": "DefaultEndpointsProtocol=https;AccountName=x",
        "adls.account-host": "myaccount.blob.core.usgovcloudapi.net",
        "s3.proxy-uri": "http://proxy:3128",
    }


def test_anonymous_azure_credentials_become_fileio_properties() -> None:
    properties = _fileio_properties(
        SecretOptions(
            {
                "account_name": "publicaccount",
                "endpoint_suffix": "core.windows.net",
                "credential_kind": "anonymous",
            }
        )
    )
    assert properties == {
        "adls.account-name": "publicaccount",
        "adls.account-host": "publicaccount.blob.core.windows.net",
        "adls.anon": "true",
    }


def test_secrets_never_appear_in_errors_or_reprs(handler, iceberg_table, monkeypatch) -> None:
    secret = "wJalrXUtnFEMI-super-secret-value-0123456789"
    secrets = SecretOptions(
        {"aws_access_key_id": "AKIA0123456789", "aws_secret_access_key": secret}
    )
    secret_storage = _SecretStorage(iceberg_table.path.parent, secrets)
    seen: dict[str, Any] = {}

    class ExplodingStaticTable:
        @staticmethod
        def from_metadata(metadata_location: str, properties: dict[str, str]) -> Any:
            seen["properties"] = dict(properties)
            raise RuntimeError(f"access denied for {properties}")

    monkeypatch.setattr(iceberg_module, "StaticTable", ExplodingStaticTable)
    with pytest.raises(SourceError) as info:
        handler.schema(secret_storage, secret_storage.resolve("orders"), IcebergFormat())

    assert seen["properties"]["s3.secret-access-key"] == secret
    message = str(info.value)
    assert secret not in message
    assert "AKIA0123456789" not in message
    assert "***" in message
    assert info.value.__cause__ is None
    assert secret not in repr(secret_storage)
    assert secret not in repr(secret_storage.native_options())
    assert secret not in repr(handler)


# -- predicate translation into pyiceberg -------------------------------------------


def _lit(value: Any) -> Literal:
    return Literal.of(value)


def _cmp(op: ComparisonOp, left: Expression, right: Expression) -> Comparison:
    return Comparison(op, left, right)


ID = Column("id")
QTY = Column("qty")
NAME = Column("name")
AMOUNT = Column("amount")
PRICE = Column("price")
DAY = Column("day")


@pytest.mark.parametrize(
    ("expression", "expected", "exact"),
    [
        (_cmp(ComparisonOp.EQ, ID, _lit(3)), ice.EqualTo("id", 3), True),
        (_cmp(ComparisonOp.NE, ID, _lit(3)), ice.NotEqualTo("id", 3), True),
        (_cmp(ComparisonOp.LT, ID, _lit(3)), ice.LessThan("id", 3), True),
        (_cmp(ComparisonOp.LE, ID, _lit(3)), ice.LessThanOrEqual("id", 3), True),
        (_cmp(ComparisonOp.GT, ID, _lit(3)), ice.GreaterThan("id", 3), True),
        (_cmp(ComparisonOp.GE, ID, _lit(3)), ice.GreaterThanOrEqual("id", 3), True),
        # literal on the left mirrors the operator
        (_cmp(ComparisonOp.LT, _lit(3), ID), ice.GreaterThan("id", 3), True),
        (_cmp(ComparisonOp.GE, _lit(3), ID), ice.LessThanOrEqual("id", 3), True),
        # NULL literals never match; NOT of unknown stays unknown
        (_cmp(ComparisonOp.EQ, ID, _lit(None)), ice.AlwaysFalse(), True),
        (_cmp(ComparisonOp.NE, ID, _lit(None)), ice.AlwaysFalse(), True),
        (Not(_cmp(ComparisonOp.EQ, ID, _lit(None))), ice.AlwaysFalse(), True),
        # NOT is pushed to the leaves
        (Not(_cmp(ComparisonOp.EQ, ID, _lit(3))), ice.NotEqualTo("id", 3), True),
        (Not(_cmp(ComparisonOp.LT, ID, _lit(3))), ice.GreaterThanOrEqual("id", 3), True),
        (Not(Not(_cmp(ComparisonOp.LT, ID, _lit(3)))), ice.LessThan("id", 3), True),
        (
            Not(And((_cmp(ComparisonOp.EQ, ID, _lit(1)), IsNull(QTY)))),
            ice.Or(ice.NotEqualTo("id", 1), ice.NotNull("qty")),
            True,
        ),
        (
            Not(Or((_cmp(ComparisonOp.EQ, ID, _lit(1)), IsNull(QTY)))),
            ice.And(ice.NotEqualTo("id", 1), ice.NotNull("qty")),
            True,
        ),
        (IsNull(QTY), ice.IsNull("qty"), True),
        (IsNull(QTY, negated=True), ice.NotNull("qty"), True),
        (Not(IsNull(QTY)), ice.NotNull("qty"), True),
        (IsNull(_lit(None)), ice.AlwaysTrue(), True),
        (IsNull(_lit(1)), ice.AlwaysFalse(), True),
        # IN: NULL members are dropped; NOT IN needs a NOT NULL guard
        (In(ID, (_lit(1), _lit(2))), ice.In("id", {1, 2}), True),
        (In(ID, (_lit(1), _lit(None))), ice.EqualTo("id", 1), True),
        (In(ID, (_lit(None),)), ice.AlwaysFalse(), True),
        (
            In(ID, (_lit(1), _lit(2)), negated=True),
            ice.And(ice.NotNull("id"), ice.NotIn("id", {1, 2})),
            True,
        ),
        (In(ID, (_lit(1), _lit(None)), negated=True), ice.AlwaysFalse(), True),
        (
            Not(In(ID, (_lit(1), _lit(2)))),
            ice.And(ice.NotNull("id"), ice.NotIn("id", {1, 2})),
            True,
        ),
        (Not(In(ID, (_lit(1), _lit(2)), negated=True)), ice.In("id", {1, 2}), True),
        # type coercion: only lossless conversions are exact
        (_cmp(ComparisonOp.GT, AMOUNT, _lit(1)), ice.GreaterThan("amount", 1.0), True),
        (_cmp(ComparisonOp.GT, QTY, _lit(1.5)), ice.AlwaysTrue(), False),
        (_cmp(ComparisonOp.GT, QTY, _lit(2.0)), ice.GreaterThan("qty", 2), True),
        (
            _cmp(ComparisonOp.EQ, PRICE, _lit(Decimal("1.10"))),
            ice.EqualTo("price", Decimal("1.10")),
            True,
        ),
        (
            _cmp(ComparisonOp.EQ, PRICE, _lit(Decimal("1.1"))),
            ice.EqualTo("price", Decimal("1.10")),
            True,
        ),
        (_cmp(ComparisonOp.EQ, PRICE, _lit(2)), ice.EqualTo("price", Decimal("2.00")), True),
        (_cmp(ComparisonOp.EQ, PRICE, _lit(Decimal("1.123"))), ice.AlwaysTrue(), False),
        (_cmp(ComparisonOp.EQ, PRICE, _lit(1.1)), ice.AlwaysTrue(), False),
        (_cmp(ComparisonOp.EQ, ID, _lit("3")), ice.AlwaysTrue(), False),
        (
            _cmp(ComparisonOp.EQ, DAY, _lit(dt.date(2024, 1, 1))),
            ice.EqualTo("day", dt.date(2024, 1, 1)),
            True,
        ),
        (_cmp(ComparisonOp.EQ, DAY, _lit(dt.datetime(2024, 1, 1))), ice.AlwaysTrue(), False),
        (In(QTY, (_lit(1), _lit(1.5))), ice.AlwaysTrue(), False),
        # shapes pyiceberg cannot express relax to AlwaysTrue
        (_cmp(ComparisonOp.EQ, ID, QTY), ice.AlwaysTrue(), False),
        (_cmp(ComparisonOp.EQ, _lit(1), _lit(1)), ice.AlwaysTrue(), False),
        (_cmp(ComparisonOp.EQ, Column("nope"), _lit(1)), ice.AlwaysTrue(), False),
        (IsNull(Column("nope")), ice.AlwaysTrue(), False),
        (Not(_cmp(ComparisonOp.EQ, ID, QTY)), ice.AlwaysTrue(), False),
        # composition keeps the exact conjuncts for pruning
        (
            And((_cmp(ComparisonOp.EQ, ID, QTY), _cmp(ComparisonOp.GT, ID, _lit(2)))),
            ice.GreaterThan("id", 2),
            False,
        ),
        (
            Or((_cmp(ComparisonOp.EQ, ID, QTY), _cmp(ComparisonOp.GT, ID, _lit(2)))),
            ice.AlwaysTrue(),
            False,
        ),
        (
            And((_cmp(ComparisonOp.GT, ID, _lit(2)), _cmp(ComparisonOp.EQ, NAME, _lit("bob")))),
            ice.And(ice.GreaterThan("id", 2), ice.EqualTo("name", "bob")),
            True,
        ),
        (
            Or((_cmp(ComparisonOp.GT, ID, _lit(2)), IsNull(NAME))),
            ice.Or(ice.GreaterThan("id", 2), ice.IsNull("name")),
            True,
        ),
    ],
)
def test_iceberg_filter_translation(iceberg_schema, expression, expected, exact) -> None:
    translated, is_exact = _IcebergPredicate(iceberg_schema, {}).translate(expression)
    assert translated == expected
    assert is_exact is exact


def test_iceberg_filter_binds_parameters(iceberg_schema) -> None:
    translator = _IcebergPredicate(
        iceberg_schema, {"target": Literal.of(3), "gone": Literal.of(None)}
    )
    assert translator.translate(_cmp(ComparisonOp.EQ, ID, Parameter("target")))[0] == ice.EqualTo(
        "id", 3
    )
    assert translator.translate(In(ID, (Parameter("target"), _lit(1))))[0] == ice.In("id", {1, 3})
    assert (
        translator.translate(_cmp(ComparisonOp.EQ, ID, Parameter("gone")))[0] == ice.AlwaysFalse()
    )
    with pytest.raises(ParameterError) as info:
        translator.translate(_cmp(ComparisonOp.EQ, ID, Parameter("missing")))
    assert info.value.code is DiagnosticCode.PARAMETER_MISSING


def test_iceberg_filter_matches_sql_semantics_on_the_table(iceberg_table, iceberg_schema) -> None:
    """The relaxed filter pyiceberg applies never drops rows SQL would keep."""

    table = StaticTable.from_metadata(iceberg_table.path.as_uri())
    cases: list[tuple[Expression, list[int]]] = [
        (_cmp(ComparisonOp.NE, QTY, _lit(1)), [1, 4, 5, 6]),
        (Not(_cmp(ComparisonOp.EQ, QTY, _lit(1))), [1, 4, 5, 6]),
        (In(QTY, (_lit(1), _lit(2)), negated=True), [1, 4, 6]),
        (Not(In(QTY, (_lit(1), _lit(2)))), [1, 4, 6]),
        (In(QTY, (_lit(1), _lit(None))), [2]),
        (_cmp(ComparisonOp.EQ, NAME, _lit("alice")), [1]),
        (Or((_cmp(ComparisonOp.EQ, ID, _lit(1)), IsNull(QTY))), [1, 3]),
        (Not(Or((_cmp(ComparisonOp.LT, ID, _lit(3)), IsNull(QTY)))), [4, 5, 6]),
    ]
    for expression, expected in cases:
        row_filter, exact = _IcebergPredicate(iceberg_schema, {}).translate(expression)
        assert exact, str(expression)
        ids = sorted(table.scan(row_filter=row_filter).to_arrow().column("id").to_pylist())
        assert ids == expected, str(expression)


# -- Arrow re-check -------------------------------------------------------------------


@pytest.fixture(scope="module")
def sample_batch(sample_rows, sample_schema) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist(sample_rows, schema=to_arrow_schema(sample_schema))


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        (_cmp(ComparisonOp.NE, QTY, _lit(1)), [1, 4, 5, 6]),
        (Not(_cmp(ComparisonOp.EQ, QTY, _lit(1))), [1, 4, 5, 6]),
        (_cmp(ComparisonOp.EQ, NAME, _lit("alice")), [1]),
        (_cmp(ComparisonOp.EQ, NAME, _lit(None)), []),
        (In(ID, (_lit(1), _lit(None))), [1]),
        (In(QTY, (_lit(1), _lit(2)), negated=True), [1, 4, 6]),
        (In(QTY, (_lit(1), _lit(None)), negated=True), []),
        (IsNull(Column("active")), [4]),
        (IsNull(Column("active"), negated=True), [1, 2, 3, 5, 6]),
        (_cmp(ComparisonOp.EQ, ID, QTY), []),
        (_cmp(ComparisonOp.GT, ID, QTY), [2, 5, 6]),
        (_cmp(ComparisonOp.EQ, _lit(1), _lit(1)), [1, 2, 3, 4, 5, 6]),
        (_cmp(ComparisonOp.EQ, _lit(1), _lit(2)), []),
        (_cmp(ComparisonOp.EQ, _lit(1), _lit(None)), []),
        (Not(_cmp(ComparisonOp.EQ, _lit(1), _lit(None))), []),
        (_cmp(ComparisonOp.GT, QTY, _lit(1.5)), [1, 4, 5]),
        (_cmp(ComparisonOp.GT, PRICE, _lit(2)), [2, 3, 5]),
        (_cmp(ComparisonOp.EQ, PRICE, _lit(Decimal("1.1"))), [1]),
        (_cmp(ComparisonOp.GE, AMOUNT, _lit(Decimal("20"))), [2, 4]),
        (_cmp(ComparisonOp.EQ, DAY, _lit(dt.date(2024, 1, 3))), [3]),
        (Or((_cmp(ComparisonOp.EQ, QTY, _lit(1)), IsNull(QTY))), [2, 3]),
        (Not(Or((_cmp(ComparisonOp.EQ, QTY, _lit(1)), IsNull(QTY)))), [1, 4, 5, 6]),
        (And((_cmp(ComparisonOp.GT, ID, _lit(2)), _cmp(ComparisonOp.NE, QTY, _lit(7)))), [5, 6]),
    ],
)
def test_arrow_recheck_uses_three_valued_logic(sample_batch, expression, expected) -> None:
    filtered = _ArrowPredicate(expression, {}).filter(sample_batch)
    assert sorted(filtered.column("id").to_pylist()) == expected


def test_arrow_recheck_binds_parameters(sample_batch) -> None:
    params = {"target": Literal.of("bob"), "nothing": Literal.of(None)}
    filtered = _ArrowPredicate(_cmp(ComparisonOp.EQ, NAME, Parameter("target")), params).filter(
        sample_batch
    )
    assert filtered.column("id").to_pylist() == [2]
    filtered = _ArrowPredicate(_cmp(ComparisonOp.NE, NAME, Parameter("nothing")), params).filter(
        sample_batch
    )
    assert filtered.num_rows == 0
    with pytest.raises(ParameterError):
        _ArrowPredicate(_cmp(ComparisonOp.EQ, NAME, Parameter("missing")), params).filter(
            sample_batch
        )


def test_arrow_recheck_refuses_residual_kinds(sample_batch) -> None:
    expression = _cmp(ComparisonOp.GT, Arithmetic(ArithmeticOp.DIV, AMOUNT, _lit(2)), _lit(1))
    with pytest.raises(UnsupportedOperationError) as info:
        _ArrowPredicate(expression, {}).filter(sample_batch)
    assert info.value.code is DiagnosticCode.FORMAT_UNSUPPORTED


def test_arrow_recheck_reports_type_errors_as_source_errors(sample_batch) -> None:
    with pytest.raises(SourceError):
        _ArrowPredicate(_cmp(ComparisonOp.EQ, NAME, _lit(1)), {}).filter(sample_batch)


# -- scan -------------------------------------------------------------------------


def test_scan_without_pushdown_streams_every_row(
    handler, storage, iceberg_table, sample_schema
) -> None:
    table = _scan(handler, storage, storage.resolve("orders"))
    assert table.schema.names == list(sample_schema.names)
    assert from_arrow_schema(table.schema) == sample_schema
    assert _ids(table) == [1, 2, 3, 4, 5, 6]
    rows = sorted(table.to_pylist(), key=lambda r: r["id"])
    assert rows == iceberg_table.rows


@pytest.mark.parametrize("batch_size", [True, False, 0, -1, 1.5, "64", None])
def test_scan_rejects_invalid_batch_size_before_loading_table(
    handler, storage, monkeypatch, batch_size
) -> None:
    def must_not_load(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("table was loaded")

    monkeypatch.setattr(iceberg_module, "_load_table", must_not_load)
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        handler.scan(
            storage,
            storage.resolve("orders"),
            IcebergFormat(),
            PushedOperations(),
            {},
            batch_size=batch_size,
        )


def test_scan_projection_is_returned_in_the_requested_order(handler, storage) -> None:
    table = _scan(handler, storage, storage.resolve("orders"), projection=("name", "id"))
    assert table.schema.names == ["name", "id"]
    assert table.schema.field("name").metadata is None
    assert _ids(table) == [1, 2, 3, 4, 5, 6]


def test_scan_predicate_columns_outside_the_projection_are_read_then_dropped(
    handler, storage
) -> None:
    table = _scan(
        handler,
        storage,
        storage.resolve("orders"),
        projection=("name",),
        predicate=_cmp(ComparisonOp.NE, QTY, _lit(1)),
    )
    assert table.schema.names == ["name"]
    names = table.column("name").to_pylist()
    assert None in names
    assert sorted(n for n in names if n is not None) == ["Alice", "alice", "dave"]
    assert table.num_rows == 4


def test_scan_limit_is_enforced(handler, storage) -> None:
    location = storage.resolve("orders")
    assert _scan(handler, storage, location, limit=2).num_rows == 2
    assert _scan(handler, storage, location, limit=0).num_rows == 0
    assert _scan(handler, storage, location, limit=100).num_rows == 6
    exact = _scan(handler, storage, location, predicate=_cmp(ComparisonOp.GT, ID, _lit(1)), limit=3)
    assert exact.num_rows == 3
    assert all(i > 1 for i in exact.column("id").to_pylist())


def test_scan_limit_with_an_inexact_filter_still_returns_enough_rows(handler, storage) -> None:
    # id > qty is not expressible in pyiceberg: rows 2, 5, 6 match after the Arrow re-check.
    table = _scan(
        handler,
        storage,
        storage.resolve("orders"),
        predicate=_cmp(ComparisonOp.GT, ID, QTY),
        limit=2,
    )
    assert table.num_rows == 2
    assert set(table.column("id").to_pylist()) <= {2, 5, 6}
    everything = _scan(
        handler, storage, storage.resolve("orders"), predicate=_cmp(ComparisonOp.GT, ID, QTY)
    )
    assert _ids(everything) == [2, 5, 6]


def test_scan_respects_batch_size(handler, storage) -> None:
    stream = handler.scan(
        storage,
        storage.resolve("orders"),
        IcebergFormat(),
        PushedOperations(),
        {},
        batch_size=2,
    )
    batches = list(stream)
    assert all(b.num_rows <= 2 for b in batches)
    assert sum(b.num_rows for b in batches) == 6
    assert stream.closed


def test_scan_close_releases_the_reader(handler, storage) -> None:
    stream = handler.scan(
        storage, storage.resolve("orders"), IcebergFormat(), PushedOperations(), {}, batch_size=1024
    )
    assert not stream.closed
    stream.close()
    assert stream.closed
    assert list(stream) == []


def test_scan_with_an_empty_projection_keeps_row_counts(handler, storage) -> None:
    table = _scan(handler, storage, storage.resolve("orders"), projection=())
    assert table.num_rows == 6
    assert len(table.schema) == 1


def test_scan_with_parameters(handler, storage) -> None:
    table = _scan(
        handler,
        storage,
        storage.resolve("orders"),
        projection=("id",),
        predicate=_cmp(ComparisonOp.EQ, ID, Parameter("target")),
        parameters={"target": Literal.of(4)},
    )
    assert table.column("id").to_pylist() == [4]
    with pytest.raises(ParameterError):
        _scan(
            handler,
            storage,
            storage.resolve("orders"),
            predicate=_cmp(ComparisonOp.EQ, ID, Parameter("target")),
        )


def test_snapshot_pinning(handler, storage, iceberg_table, sample_schema) -> None:
    first, second = iceberg_table.snapshot_ids
    location = storage.resolve("orders")
    assert handler.schema(storage, location, IcebergFormat(snapshot_id=first)) == sample_schema
    assert _ids(_scan(handler, storage, location, IcebergFormat(snapshot_id=first))) == [1, 2, 3]
    assert _ids(_scan(handler, storage, location, IcebergFormat(snapshot_id=second))) == [
        1,
        2,
        3,
        4,
        5,
        6,
    ]


def test_unknown_snapshot_is_invalid(handler, storage) -> None:
    location = storage.resolve("orders")
    with pytest.raises(SourceError) as info:
        handler.schema(storage, location, IcebergFormat(snapshot_id=123456789))
    assert info.value.code is DiagnosticCode.FORMAT_INVALID
    with pytest.raises(SourceError):
        _scan(handler, storage, location, IcebergFormat(snapshot_id=123456789))


def test_scan_bypasses_pyicebergs_parallel_batch_materialisation(
    handler, storage, monkeypatch
) -> None:
    from pyiceberg.io.pyarrow import ArrowScan

    def must_not_materialise(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("parallel per-file batch materialisation was used")

    monkeypatch.setattr(ArrowScan, "to_record_batches", must_not_materialise)
    table = _scan(handler, storage, storage.resolve("orders"), projection=("id",), limit=4)
    assert table.num_rows == 4


def test_batch_reader_never_falls_back_to_an_eager_arrow_table() -> None:
    class UnsupportedLegacyScan:
        def to_arrow(self) -> Any:
            raise AssertionError("the full table was materialised")

    with pytest.raises(UnsupportedOperationError) as info:
        iceberg_module._batch_reader(UnsupportedLegacyScan())
    assert info.value.code is DiagnosticCode.FORMAT_UNSUPPORTED


# -- end to end through the facade -------------------------------------------------


@pytest.fixture()
def iceberg_ctx(iceberg_table):
    context = iql.Context()
    storage = iql.local_storage(iceberg_table.path.parent)
    context.register_source(iql.file_source("ice", storage, "orders", iql.IcebergFormat()))
    context.register_source(
        iql.file_source(
            "ice_v1",
            storage,
            "orders",
            iql.IcebergFormat(snapshot_id=iceberg_table.snapshot_ids[0]),
        )
    )
    yield context
    context.close()


def _dispositions(explain) -> dict[str, Disposition]:
    return {node.operation: node.disposition for node in explain.nodes}


def test_context_schema_and_full_pushdown(iceberg_ctx, sample_schema) -> None:
    query = iceberg_ctx.sql("SELECT name FROM ice WHERE id > 2 LIMIT 2")
    assert query.schema().names == ("name",)
    assert iceberg_ctx.sql("SELECT * FROM ice").schema() == sample_schema
    explain = query.explain()
    assert explain.executable
    assert _dispositions(explain) == {
        "scan": Disposition.PUSHED,
        "filter": Disposition.PUSHED,
        "project": Disposition.PUSHED,
        "limit": Disposition.PUSHED,
    }
    rows = query.execute().rows()
    assert len(rows) == 2
    assert all(set(row) == {"name"} for row in rows)
    assert {row["name"] for row in rows} <= {"carol", "dave", None, "Alice"}


def test_context_partial_pushdown_keeps_like_residual(iceberg_ctx) -> None:
    query = iceberg_ctx.sql("SELECT id, name FROM ice WHERE qty <> 1 AND name LIKE 'a%' LIMIT 5")
    explain = query.explain()
    assert explain.executable
    dispositions = _dispositions(explain)
    assert dispositions["filter"] is Disposition.PARTIAL
    assert dispositions["limit"] is Disposition.RESIDUAL
    filter_node = next(n for n in explain.nodes if n.operation == "filter")
    assert filter_node.pushed == "(qty <> 1)"
    assert filter_node.residual == "(name LIKE 'a%')"
    assert query.execute().rows() == [{"id": 1, "name": "alice"}]


def test_context_three_valued_logic_and_case_sensitivity(iceberg_ctx) -> None:
    rows = (
        iceberg_ctx.sql("SELECT id FROM ice WHERE NOT (qty = 1) OR name = 'alice'").execute().rows()
    )
    assert sorted(r["id"] for r in rows) == [1, 4, 5, 6]
    rows = iceberg_ctx.sql("SELECT id FROM ice WHERE qty NOT IN (1, 2)").execute().rows()
    assert sorted(r["id"] for r in rows) == [1, 4, 6]
    rows = (
        iceberg_ctx.sql("SELECT id FROM ice WHERE name IS NULL OR active IS NULL").execute().rows()
    )
    assert sorted(r["id"] for r in rows) == [4, 5]


def test_context_parameters(iceberg_ctx) -> None:
    query = iceberg_ctx.sql("SELECT id, amount FROM ice WHERE id = :target")
    assert query.parameters == ("target",)
    assert query.execute(params={"target": 3}).rows() == [{"id": 3, "amount": 5.25}]


def test_context_computed_projection_reads_only_needed_columns(iceberg_ctx) -> None:
    query = iceberg_ctx.sql("SELECT amount / 2 AS half FROM ice WHERE id = 2")
    explain = query.explain()
    dispositions = _dispositions(explain)
    assert dispositions["project"] is Disposition.PARTIAL
    assert dispositions["filter"] is Disposition.PUSHED
    assert query.execute().rows() == [{"half": 10.0}]


def test_context_pinned_snapshot(iceberg_ctx) -> None:
    rows = iceberg_ctx.sql("SELECT id FROM ice_v1").execute().rows()
    assert sorted(r["id"] for r in rows) == [1, 2, 3]
    rows = iceberg_ctx.sql("SELECT id FROM ice").execute().rows()
    assert sorted(r["id"] for r in rows) == [1, 2, 3, 4, 5, 6]


def test_context_preview_is_bounded(iceberg_ctx) -> None:
    stream = iceberg_ctx.sql("SELECT id FROM ice").preview(rows=2)
    assert sum(b.num_rows for b in stream) == 2


# -- Spark reader spec ------------------------------------------------------------------


def test_reader_spec_for_hadoop_table() -> None:
    spec = IcebergReaderSpecHandler().reader_spec(IcebergFormat(), "file:///warehouse/orders")
    assert spec.format == "iceberg"
    assert spec.options == {}
    assert spec.schema is None
    assert spec.requires == ("org.apache.iceberg:iceberg-spark-runtime",)
    assert spec.to_dict()["requires"] == ["org.apache.iceberg:iceberg-spark-runtime"]


def test_reader_spec_pins_snapshot() -> None:
    spec = IcebergReaderSpecHandler().reader_spec(
        IcebergFormat(snapshot_id=8691000292061121330), "abfss://c@a.dfs.core.windows.net/t"
    )
    assert spec.options == {"snapshot-id": "8691000292061121330"}


def test_reader_spec_refuses_metadata_location() -> None:
    with pytest.raises(UnsupportedOperationError) as info:
        IcebergReaderSpecHandler().reader_spec(
            IcebergFormat(metadata_location="metadata/v2.metadata.json"), "file:///t"
        )
    assert info.value.code is DiagnosticCode.FORMAT_UNSUPPORTED


# -- integration ---------------------------------------------------------------------


class _RemoteStorage:
    """A Storage double for one remote metadata file pyiceberg reads directly."""

    name = "remote"
    capabilities = StorageCapabilities(engine_visible_uri=True)

    def __init__(self, options: SecretOptions) -> None:
        self._options = options

    def resolve(self, path: str | Location) -> Location:
        return path if isinstance(path, Location) else Location.parse(path)

    def open_read(self, location: Location) -> BinaryIO:
        raise NotImplementedError

    def info(self, location: Location) -> ObjectInfo:
        raise NotImplementedError

    def list(self, location: Location, *, recursive: bool = False) -> Iterator[ObjectInfo]:
        raise NotImplementedError

    def exists(self, location: Location) -> bool:
        return False

    def native_uri(self, location: Location) -> str | None:
        return location.uri

    def native_options(self) -> SecretOptions:
        return self._options


@pytest.mark.integration
def test_remote_iceberg_table_integration() -> None:
    """Read a real object-store table.

    Set ``INVARIANTQL_INTEGRATION=1``, ``INVARIANTQL_ICEBERG_METADATA_URI`` to an
    ``s3://`` or ``abfss://`` ``*.metadata.json`` URI and optionally
    ``INVARIANTQL_ICEBERG_STORAGE_OPTIONS`` to a JSON object of canonical
    storage option keys (``aws_access_key_id``, ``account_key``, ...).
    """

    if not os.environ.get("INVARIANTQL_INTEGRATION"):
        pytest.skip("set INVARIANTQL_INTEGRATION=1 to run integration tests")
    metadata_uri = os.environ.get("INVARIANTQL_ICEBERG_METADATA_URI")
    if not metadata_uri:
        pytest.skip("set INVARIANTQL_ICEBERG_METADATA_URI to a remote *.metadata.json URI")
    assert metadata_uri is not None
    options = json.loads(os.environ.get("INVARIANTQL_ICEBERG_STORAGE_OPTIONS", "{}"))
    remote = _RemoteStorage(SecretOptions(options))
    handler = IcebergLocalHandler()
    location = Location.parse(metadata_uri)
    schema = handler.schema(remote, location, IcebergFormat())
    assert len(schema) > 0
    first = schema.names[0]
    table = _scan(handler, remote, location, projection=(first,), limit=5)
    assert table.schema.names == [first]
    assert table.num_rows <= 5
