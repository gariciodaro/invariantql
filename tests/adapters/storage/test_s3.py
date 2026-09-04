"""S3 storage adapter: fast unit tests over fakes, plus gated live integration tests.

The unit tests never touch the network: ``s3fs.S3FileSystem`` is replaced by a
fake that records its constructor keywords and serves an fsspec ``memory``
filesystem. The integration tests run only when ``INVARIANTQL_S3_BUCKET`` is set
and standard AWS environment variables (or a profile) provide credentials.
"""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING, Any

import fsspec
import pyarrow as pa
import pytest
import s3fs
from fsspec.implementations.memory import MemoryFileSystem

import invariantql as iql
from invariantql.adapters.storage import s3 as s3_module
from invariantql.adapters.storage.fsspec_storage import FsspecStorage
from invariantql.adapters.storage.s3 import S3_CAPABILITIES, s3_storage
from invariantql.domain.credentials import REDACTED, SecretOptions
from invariantql.domain.diagnostics import DiagnosticCode, StorageError
from invariantql.domain.location import Location
from invariantql.ports.storage import Storage, StorageCapabilities

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

BUCKET = "my-bucket"
FAKE_KEY = "AKIAFAKEACCESSKEYID00"
FAKE_SECRET = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYFAKESECRETKEY"
FAKE_TOKEN = "FwoGZXIvYXdzEFAKESESSIONTOKENabcdefghijk"
REGION = "eu-north-1"


def loc(path: str) -> Location:
    """A relative location, resolved by the storage under test."""

    return Location(path)


# -- fakes --------------------------------------------------------------------


class FakeS3FileSystem:
    """Stands in for ``s3fs.S3FileSystem``: records kwargs, serves an in-memory store."""

    instances: list[FakeS3FileSystem] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self._mem: MemoryFileSystem = fsspec.filesystem("memory")
        FakeS3FileSystem.instances.append(self)

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        return self._mem.open(path, mode, **kwargs)

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return self._mem.info(path, **kwargs)

    def ls(self, path: str, detail: bool = False, **kwargs: Any) -> Any:
        return self._mem.ls(path, detail=detail, **kwargs)

    def find(self, path: str, detail: bool = False, **kwargs: Any) -> Any:
        return self._mem.find(path, detail=detail, **kwargs)

    def exists(self, path: str, **kwargs: Any) -> bool:
        return self._mem.exists(path, **kwargs)

    def pipe(self, path: str, value: bytes) -> None:
        self._mem.pipe(path, value)


class ExplodingS3FileSystem(FakeS3FileSystem):
    """Every operation fails with a provider error that echoes the credentials."""

    def _boom(self) -> None:
        raise RuntimeError(
            f"AccessDenied for key={self.kwargs.get('key')} secret={self.kwargs.get('secret')} "
            f"(raw {self.kwargs.get('secret')})"
        )

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> Any:
        self._boom()

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        self._boom()
        return {}

    def ls(self, path: str, detail: bool = False, **kwargs: Any) -> Any:
        self._boom()

    def exists(self, path: str, **kwargs: Any) -> bool:
        self._boom()
        return False


class UnconstructibleS3FileSystem(FakeS3FileSystem):
    def __init__(self, **kwargs: Any) -> None:
        raise ValueError(f"bad profile; secret={kwargs.get('secret')} token={kwargs.get('token')}")


class EnvironmentExplodingS3FileSystem(FakeS3FileSystem):
    def __init__(self, **kwargs: Any) -> None:
        raise ValueError(f"ambient credential was {os.environ['AWS_SECRET_ACCESS_KEY']}")


def _reset_memory() -> None:
    MemoryFileSystem.store.clear()
    MemoryFileSystem.pseudo_dirs[:] = [""]


@pytest.fixture()
def fake_s3(monkeypatch: pytest.MonkeyPatch) -> Iterator[type[FakeS3FileSystem]]:
    _reset_memory()
    FakeS3FileSystem.instances.clear()
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_ACCESS_KEY",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECRET_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_DEFAULT_REGION",
        "AWS_REGION",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(s3fs, "S3FileSystem", FakeS3FileSystem)
    yield FakeS3FileSystem
    _reset_memory()
    FakeS3FileSystem.instances.clear()


def _captured() -> dict[str, Any]:
    assert len(FakeS3FileSystem.instances) == 1
    return FakeS3FileSystem.instances[-1].kwargs


# -- construction: provider keywords -----------------------------------------


def test_static_credentials_region_endpoint_and_client_kwargs_reach_s3fs(fake_s3) -> None:
    storage = s3_storage(
        BUCKET,
        key=FAKE_KEY,
        secret=FAKE_SECRET,
        token=FAKE_TOKEN,
        region=REGION,
        endpoint_url="https://s3.example.test",
        verify=False,
    )
    assert isinstance(storage, FsspecStorage)
    assert _captured() == {
        "key": FAKE_KEY,
        "secret": FAKE_SECRET,
        "token": FAKE_TOKEN,
        "client_kwargs": {
            "region_name": REGION,
            "endpoint_url": "https://s3.example.test",
            "verify": False,
        },
    }


def test_defaults_pass_nothing_so_the_botocore_chain_applies(fake_s3) -> None:
    s3_storage(BUCKET)
    assert _captured() == {}


def test_anonymous_and_profile_access(fake_s3) -> None:
    s3_storage(BUCKET, anon=True)
    assert _captured() == {"anon": True}
    FakeS3FileSystem.instances.clear()
    s3_storage(BUCKET, profile="analytics")
    assert _captured() == {"profile": "analytics"}


def test_s3fs_level_options_are_routed_to_the_filesystem_not_the_client(fake_s3) -> None:
    s3_storage(
        BUCKET,
        requester_pays=True,
        version_aware=True,
        config_kwargs={"signature_version": "s3v4"},
        client_kwargs={"verify": "/etc/ssl/ca.pem"},
        region=REGION,
    )
    assert _captured() == {
        "requester_pays": True,
        "version_aware": True,
        "config_kwargs": {"signature_version": "s3v4"},
        "client_kwargs": {"verify": "/etc/ssl/ca.pem", "region_name": REGION},
    }


def test_plain_http_requires_an_explicit_opt_in_for_a_custom_endpoint(fake_s3) -> None:
    with pytest.raises(ValueError, match="allow_http=True"):
        s3_storage(BUCKET, endpoint_url="http://localhost:9000")
    with pytest.raises(ValueError, match="endpoint_url"):
        s3_storage(BUCKET, allow_http=True)
    assert FakeS3FileSystem.instances == []

    storage = s3_storage(BUCKET, endpoint_url="http://localhost:9000", allow_http=True)
    assert _captured() == {
        "use_ssl": False,
        "client_kwargs": {"endpoint_url": "http://localhost:9000"},
    }
    assert storage.native_options().reveal() == {
        "aws_endpoint_url": "http://localhost:9000",
        "aws_allow_http": "true",
    }


@pytest.mark.parametrize(
    "kwargs",
    [
        {"key": FAKE_KEY},
        {"secret": FAKE_SECRET},
        {"token": FAKE_TOKEN},
        {"anon": True, "key": FAKE_KEY, "secret": FAKE_SECRET},
        {"anon": True, "profile": "dev"},
    ],
)
def test_inconsistent_credential_arguments_are_rejected(fake_s3, kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        s3_storage(BUCKET, **kwargs)
    assert FakeS3FileSystem.instances == []


@pytest.mark.parametrize(
    "bucket",
    ["", "ab", "a/b", " spaced", "UPPERCASE", "a..b", "192.168.1.1", "s3://x"],
)
def test_invalid_bucket_names_are_rejected(fake_s3, bucket: str) -> None:
    with pytest.raises(ValueError):
        s3_storage(bucket)


def test_provider_construction_failure_is_wrapped_and_redacted(monkeypatch) -> None:
    monkeypatch.setattr(s3fs, "S3FileSystem", UnconstructibleS3FileSystem)
    with pytest.raises(StorageError) as excinfo:
        s3_storage(BUCKET, key=FAKE_KEY, secret=FAKE_SECRET, token=FAKE_TOKEN, profile="broken")
    exc = excinfo.value
    assert exc.code is DiagnosticCode.STORAGE_FAILURE
    assert exc.__cause__ is None and exc.__suppress_context__
    text = str(exc)
    assert BUCKET in text and "ValueError" in text
    assert FAKE_SECRET not in text and FAKE_TOKEN not in text
    assert REDACTED in text


def test_environment_credentials_are_registered_before_provider_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment_secret = "ambient-aws-secret-value"
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", environment_secret)
    monkeypatch.setattr(s3fs, "S3FileSystem", EnvironmentExplodingS3FileSystem)

    with pytest.raises(StorageError) as excinfo:
        s3_storage(BUCKET)

    assert environment_secret not in str(excinfo.value)
    assert REDACTED in str(excinfo.value)


def test_environment_credentials_are_available_to_native_engines(
    fake_s3, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", FAKE_KEY)
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", FAKE_SECRET)
    monkeypatch.setenv("AWS_SESSION_TOKEN", FAKE_TOKEN)
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)

    options = s3_storage(BUCKET).native_options().reveal()

    assert options == {
        "aws_access_key_id": FAKE_KEY,
        "aws_secret_access_key": FAKE_SECRET,
        "aws_session_token": FAKE_TOKEN,
        "aws_region": REGION,
    }
    assert _captured() == {}  # botocore still owns environment resolution


def test_facade_factory_loads_this_adapter(fake_s3) -> None:
    storage = iql.s3_storage(BUCKET, key=FAKE_KEY, secret=FAKE_SECRET, region=REGION)
    assert isinstance(storage, FsspecStorage)
    assert storage.name == "s3:my-bucket"
    assert _captured()["key"] == FAKE_KEY


# -- identity, capabilities and native options ---------------------------------


def test_identity_and_capabilities(fake_s3) -> None:
    storage = s3_storage(BUCKET)
    assert isinstance(storage, Storage)
    assert storage.name == "s3:my-bucket"
    assert storage.scheme == "s3"
    assert storage.netloc == BUCKET
    assert s3_storage(BUCKET, name="lake").name == "lake"

    caps = storage.capabilities
    assert caps is S3_CAPABILITIES
    assert isinstance(caps, StorageCapabilities)
    assert caps.range_reads is True
    assert caps.listing is True
    assert caps.engine_visible_uri is True
    assert caps.hierarchical_directories is False
    assert caps.atomic_rename is False
    assert len(caps.evidence) >= 4
    assert any("CopyObject" in e for e in caps.evidence)
    assert any("Range" in e for e in caps.evidence)
    assert caps.to_dict()["evidence"] == list(caps.evidence)


def test_native_options_use_the_canonical_vocabulary_and_only_set_keys(fake_s3) -> None:
    full = s3_storage(
        BUCKET,
        key=FAKE_KEY,
        secret=FAKE_SECRET,
        token=FAKE_TOKEN,
        region=REGION,
        endpoint_url="http://s3.example.test",
        allow_http=True,
    ).native_options()
    assert isinstance(full, SecretOptions)
    assert full.ref is not None and full.ref.name == "s3:my-bucket"
    assert full.reveal() == {
        "aws_access_key_id": FAKE_KEY,
        "aws_secret_access_key": FAKE_SECRET,
        "aws_session_token": FAKE_TOKEN,
        "aws_region": REGION,
        "aws_endpoint_url": "http://s3.example.test",
        "aws_allow_http": "true",
    }
    assert full["aws_secret_access_key"] == REDACTED
    assert dict(full) == dict.fromkeys(full.reveal(), REDACTED)

    FakeS3FileSystem.instances.clear()
    partial = s3_storage(BUCKET, key=FAKE_KEY, secret=FAKE_SECRET).native_options()
    assert set(partial) == {"aws_access_key_id", "aws_secret_access_key"}

    FakeS3FileSystem.instances.clear()
    assert dict(s3_storage(BUCKET).native_options()) == {}
    FakeS3FileSystem.instances.clear()
    assert dict(s3_storage(BUCKET, anon=True, region=REGION).native_options()) == {
        "aws_region": REDACTED,
        "aws_anonymous": REDACTED,
    }
    FakeS3FileSystem.instances.clear()
    assert s3_storage(BUCKET, anon=True).native_options().reveal() == {"aws_anonymous": "true"}


# -- path and URI mapping ------------------------------------------------------


def test_relative_paths_resolve_inside_the_bucket(fake_s3) -> None:
    storage = s3_storage(BUCKET)
    location = storage.resolve("data/orders.parquet")
    assert location == Location("/data/orders.parquet", "s3", BUCKET)
    assert location.uri == "s3://my-bucket/data/orders.parquet"
    assert storage.resolve("/data/orders.parquet") == location
    assert storage.resolve(loc("data/orders.parquet")) == location
    assert storage.fs_path(location) == "my-bucket/data/orders.parquet"
    assert storage.fs_path(loc("data/orders.parquet")) == "my-bucket/data/orders.parquet"
    assert storage.fs_path(storage.resolve("")) == "my-bucket"
    assert storage.native_uri(location) == "s3a://my-bucket/data/orders.parquet"
    assert storage.native_uri(loc("data/orders.parquet")) == "s3a://my-bucket/data/orders.parquet"


def test_root_prefix_is_prepended_to_relative_paths(fake_s3) -> None:
    storage = s3_storage(BUCKET, root="/warehouse/")
    assert storage.resolve("x.csv") == Location("/warehouse/x.csv", "s3", BUCKET)
    assert storage.fs_path(loc("x.csv")) == "my-bucket/warehouse/x.csv"
    assert storage.native_uri(loc("x.csv")) == "s3a://my-bucket/warehouse/x.csv"
    assert storage.fs_path(storage.resolve("")) == "my-bucket/warehouse"


def test_absolute_locations_must_belong_to_this_bucket(fake_s3) -> None:
    storage = s3_storage(BUCKET)
    own = Location.parse("s3://my-bucket/raw/events.json")
    assert storage.resolve(own) == Location("/raw/events.json", "s3", BUCKET)
    assert storage.fs_path(own) == "my-bucket/raw/events.json"
    for foreign in ("s3://other-bucket/raw/events.json", "file:///tmp/events.json"):
        with pytest.raises(StorageError) as excinfo:
            storage.resolve(Location.parse(foreign))
        assert excinfo.value.code is DiagnosticCode.STORAGE_UNSUPPORTED_OPERATION


def test_dot_segments_and_control_characters_are_rejected(fake_s3) -> None:
    storage = s3_storage(BUCKET, root="warehouse")
    for path in ("../outside.csv", "a/../../outside.csv", "bad\nname.csv"):
        with pytest.raises(StorageError):
            storage.resolve(path)
    assert storage.resolve("a/./same.csv").path == "/warehouse/a/same.csv"
    with pytest.raises(StorageError):
        s3_storage(BUCKET, root="warehouse/../outside")


@pytest.mark.parametrize(
    "endpoint",
    ["localhost:9000", "ftp://example.test", "https://user:pass@example.test", "https://x/?sig=x"],
)
def test_invalid_or_credential_bearing_endpoints_are_rejected(fake_s3, endpoint: str) -> None:
    with pytest.raises(ValueError):
        s3_storage(BUCKET, endpoint_url=endpoint)
    assert FakeS3FileSystem.instances == []


def test_named_endpoint_and_region_options_cannot_be_hidden_in_client_kwargs(fake_s3) -> None:
    with pytest.raises(ValueError, match="named factory arguments"):
        s3_storage(BUCKET, client_kwargs={"endpoint_url": "https://hidden.example"})
    with pytest.raises(ValueError, match="allow_http"):
        s3_storage(BUCKET, use_ssl=False)
    assert FakeS3FileSystem.instances == []


@pytest.mark.parametrize(
    ("fs_path", "expected"),
    [
        ("my-bucket/data/a.csv", "/data/a.csv"),
        ("/my-bucket/data/a.csv", "/data/a.csv"),
        ("s3://my-bucket/data/a.csv", "/data/a.csv"),
        ("s3a://my-bucket/data/a.csv", "/data/a.csv"),
        ("my-bucket", "/"),
        ("my-bucket/", "/"),
    ],
)
def test_provider_entry_names_map_back_to_locations(fs_path: str, expected: str) -> None:
    convert = s3_module._from_fs_path_for(BUCKET)  # unit test of the private mapping
    assert convert(fs_path) == Location(expected, "s3", BUCKET)


# -- storage operations over an in-memory filesystem ----------------------------


@pytest.fixture()
def populated(fake_s3) -> FsspecStorage:
    storage = s3_storage(BUCKET, key=FAKE_KEY, secret=FAKE_SECRET, region=REGION)
    fs = FakeS3FileSystem.instances[-1]
    fs.pipe("my-bucket/data/b.csv", b"id,name\n2,bob\n")
    fs.pipe("my-bucket/data/a.csv", b"id,name\n1,alice\n")
    fs.pipe("my-bucket/data/nested/c.csv", b"id,name\n3,carol\n")
    fs.pipe("my-bucket/top.txt", b"hello world")
    return storage


def test_exists_info_and_range_reads(populated: FsspecStorage) -> None:
    assert populated.exists(loc("data/a.csv"))
    assert not populated.exists(loc("data/missing.csv"))

    info = populated.info(loc("top.txt"))
    assert info.location.uri == "s3://my-bucket/top.txt"
    assert info.size == len(b"hello world")
    assert info.is_directory is False

    with populated.open_read(loc("top.txt")) as handle:
        assert handle.seekable()
        assert handle.read(5) == b"hello"
        handle.seek(6)
        assert handle.read() == b"world"


def test_listing_is_sorted_and_recursion_is_explicit(populated: FsspecStorage) -> None:
    shallow = list(populated.list(loc("data")))
    assert [e.location.uri for e in shallow] == [
        "s3://my-bucket/data/a.csv",
        "s3://my-bucket/data/b.csv",
        "s3://my-bucket/data/nested",
    ]
    assert [e.is_directory for e in shallow] == [False, False, True]
    assert shallow[0].size == len(b"id,name\n1,alice\n")
    assert shallow[2].size is None

    deep = list(populated.list(loc("data"), recursive=True))
    assert [e.location.path for e in deep] == ["/data/a.csv", "/data/b.csv", "/data/nested/c.csv"]
    assert all(not e.is_directory for e in deep)


def test_missing_objects_raise_not_found(populated: FsspecStorage) -> None:
    for operation in (populated.open_read, populated.info):
        with pytest.raises(StorageError) as excinfo:
            operation(loc("data/missing.csv"))
        assert excinfo.value.code is DiagnosticCode.STORAGE_OBJECT_NOT_FOUND
        assert "s3://my-bucket/data/missing.csv" in str(excinfo.value)
    with pytest.raises(StorageError) as excinfo:
        list(populated.list(loc("nowhere")))
    assert excinfo.value.code is DiagnosticCode.STORAGE_OBJECT_NOT_FOUND


# -- secret non-disclosure (FF-12) ------------------------------------------------


def test_secrets_never_appear_in_repr(fake_s3) -> None:
    storage = s3_storage(BUCKET, key=FAKE_KEY, secret=FAKE_SECRET, token=FAKE_TOKEN, region=REGION)
    for text in (
        repr(storage),
        str(storage),
        repr(storage.native_options()),
        str(storage.native_options()),
    ):
        assert FAKE_SECRET not in text
        assert FAKE_TOKEN not in text
        assert FAKE_KEY not in text
    assert repr(storage) == "FsspecStorage(name='s3:my-bucket', scheme='s3', netloc='my-bucket')"


def test_provider_errors_are_wrapped_with_redacted_messages(monkeypatch) -> None:
    monkeypatch.setattr(s3fs, "S3FileSystem", ExplodingS3FileSystem)
    storage = s3_storage(BUCKET, key=FAKE_KEY, secret=FAKE_SECRET)

    def check(exc: StorageError) -> None:
        assert exc.code is DiagnosticCode.STORAGE_FAILURE
        assert exc.__cause__ is None and exc.__suppress_context__
        text = str(exc)
        assert FAKE_SECRET not in text
        assert REDACTED in text
        assert "RuntimeError" in text and "s3://my-bucket/data/a.csv" in text

    with pytest.raises(StorageError) as excinfo:
        storage.open_read(loc("data/a.csv"))
    check(excinfo.value)
    with pytest.raises(StorageError) as excinfo:
        storage.info(loc("data/a.csv"))
    check(excinfo.value)
    with pytest.raises(StorageError) as excinfo:
        list(storage.list(loc("data/a.csv")))
    check(excinfo.value)
    with pytest.raises(StorageError) as excinfo:
        storage.exists(loc("data/a.csv"))
    check(excinfo.value)


# -- end to end: DuckDB reads an S3 location through the storage bridge ----------


def test_duckdb_previews_parquet_from_s3_storage(fake_s3, data_dir: Path, sample_rows) -> None:
    storage = s3_storage(BUCKET, key=FAKE_KEY, secret=FAKE_SECRET, region=REGION)
    FakeS3FileSystem.instances[-1].pipe(
        "my-bucket/data/orders.parquet", (data_dir / "orders.parquet").read_bytes()
    )
    with iql.Context() as ctx:
        ctx.register_source(
            iql.file_source("s3_orders", storage, "data/orders.parquet", iql.ParquetFormat())
        )
        query = ctx.sql("SELECT id, name FROM s3_orders WHERE id > 2")
        stream = query.preview()
        table = pa.Table.from_batches(list(stream), schema=stream.schema)
    expected = [{"id": r["id"], "name": r["name"]} for r in sample_rows if r["id"] > 2]
    assert sorted(table.to_pylist(), key=lambda r: r["id"]) == expected


# -- live integration (INVARIANTQL_S3_BUCKET) --------------------------------------


def _live_bucket() -> str:
    bucket = os.environ.get("INVARIANTQL_S3_BUCKET")
    if not bucket:
        pytest.skip("set INVARIANTQL_S3_BUCKET (and AWS credentials) to run S3 integration tests")
    return bucket


@pytest.fixture()
def live_storage() -> Iterator[FsspecStorage]:
    bucket = _live_bucket()
    storage = s3_storage(
        bucket,
        region=os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION"),
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL"),
        profile=os.environ.get("AWS_PROFILE"),
        root=f"invariantql-tests/{uuid.uuid4().hex}",
    )
    yield storage
    try:
        storage.fs.rm(storage.fs_path(storage.resolve("")), recursive=True)
    except Exception:  # best-effort cleanup of the test prefix
        pass


@pytest.mark.integration
def test_live_object_round_trip(live_storage: FsspecStorage) -> None:
    payload = b"hello from invariantql\n" * 64
    live_storage.fs.pipe(live_storage.fs_path(loc("hello.txt")), payload)

    assert live_storage.exists(loc("hello.txt"))
    info = live_storage.info(loc("hello.txt"))
    assert info.size == len(payload)
    assert info.modified_at is not None
    assert info.location.uri.startswith("s3://")
    with live_storage.open_read(loc("hello.txt")) as handle:
        handle.seek(6)
        assert handle.read(4) == b"from"
        handle.seek(0)
        assert handle.read() == payload
    listed = list(live_storage.list(loc("")))
    assert [e.location.name for e in listed] == ["hello.txt"]
    uri = live_storage.native_uri(loc("hello.txt"))
    assert uri is not None and uri.startswith(f"s3a://{live_storage.netloc}/")
    with pytest.raises(StorageError) as excinfo:
        live_storage.info(loc("missing.txt"))
    assert excinfo.value.code is DiagnosticCode.STORAGE_OBJECT_NOT_FOUND


@pytest.mark.integration
def test_live_duckdb_preview_over_parquet(
    live_storage: FsspecStorage, data_dir: Path, sample_rows
) -> None:
    live_storage.fs.pipe(
        live_storage.fs_path(loc("orders.parquet")), (data_dir / "orders.parquet").read_bytes()
    )
    with iql.Context() as ctx:
        ctx.register_source(
            iql.file_source("s3_orders", live_storage, "orders.parquet", iql.ParquetFormat())
        )
        stream = ctx.sql("SELECT id, amount FROM s3_orders WHERE amount >= 10").preview()
        table = pa.Table.from_batches(list(stream), schema=stream.schema)
    expected = [{"id": r["id"], "amount": r["amount"]} for r in sample_rows if r["amount"] >= 10]
    assert sorted(table.to_pylist(), key=lambda r: r["id"]) == expected
