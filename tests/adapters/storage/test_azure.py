"""Azure Blob / ADLS Gen2 storage factories: query-free unit tests plus a gated live probe."""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator

adlfs = pytest.importorskip("adlfs")
fsspec = pytest.importorskip("fsspec")

from invariantql.adapters.storage import azure as azure_module  # noqa: E402
from invariantql.adapters.storage.fsspec_storage import FsspecStorage  # noqa: E402
from invariantql.domain.credentials import REDACTED, SecretOptions  # noqa: E402
from invariantql.domain.diagnostics import DiagnosticCode, StorageError  # noqa: E402
from invariantql.domain.location import Location  # noqa: E402
from tests.contracts.contracts import StorageContract  # noqa: E402

ACCOUNT = "acct"
CONTAINER = "lake"
NETLOC = f"{CONTAINER}@{ACCOUNT}.dfs.core.windows.net"
SAS = "sv=2024-01-01&ss=b&sig=SuperSecretSignatureValue123abc"
KEY = "Q2hhbmdlTWVQbGVhc2VUaGlzSXNBU2VjcmV0S2V5MTIzNDU2Nzg5MA=="
CONNECTION_STRING = (
    f"DefaultEndpointsProtocol=https;AccountName={ACCOUNT};AccountKey={KEY};"
    "EndpointSuffix=core.windows.net"
)
CLIENT_SECRET = "sp-secret-value-9f8e7d6c5b4a"
ENV_KEY = "EnvironmentAccountKeyValue0123456789ABCDEF=="


class FakeAzureFileSystem:
    """Stands in for adlfs: captures kwargs, mirrors adlfs attributes, fails loudly on I/O."""

    instances: list[FakeAzureFileSystem] = []
    fail_on_connect: Exception | None = None

    def __init__(self, **kwargs: Any) -> None:
        if FakeAzureFileSystem.fail_on_connect is not None:
            raise FakeAzureFileSystem.fail_on_connect
        self.kwargs = kwargs
        # What adlfs would have seen in the environment at construction time.
        self.environment = {key: os.environ.get(key) for key in azure_module._ADLFS_ENVIRONMENT}
        for key in (
            "account_name",
            "account_key",
            "sas_token",
            "connection_string",
            "client_id",
            "client_secret",
            "tenant_id",
            "credential",
        ):
            setattr(
                self, key, kwargs.get(key) or self.environment.get(f"AZURE_STORAGE_{key.upper()}")
            )
        if isinstance(self.sas_token, str) and not self.sas_token.startswith("?"):
            self.sas_token = "?" + self.sas_token  # adlfs does this too
        FakeAzureFileSystem.instances.append(self)

    def _fail(self, operation: str) -> None:
        # Provider errors often echo the credentials they used.
        raise RuntimeError(f"{operation} failed with {self.kwargs!r}")

    def open(self, path: str, mode: str = "rb") -> Any:
        self._fail(f"open {path}")

    def info(self, path: str) -> Any:
        self._fail(f"info {path}")

    def ls(self, path: str, detail: bool = True) -> Any:
        self._fail(f"ls {path}")

    def find(self, path: str, detail: bool = True) -> Any:
        self._fail(f"find {path}")

    def exists(self, path: str) -> Any:
        self._fail(f"exists {path}")


@pytest.fixture()
def fake_adlfs(monkeypatch: pytest.MonkeyPatch) -> Iterator[type[FakeAzureFileSystem]]:
    FakeAzureFileSystem.instances.clear()
    FakeAzureFileSystem.fail_on_connect = None
    monkeypatch.setattr(adlfs, "AzureBlobFileSystem", FakeAzureFileSystem)
    for key in azure_module._ADLFS_ENVIRONMENT:
        monkeypatch.delenv(key, raising=False)
    yield FakeAzureFileSystem
    FakeAzureFileSystem.instances.clear()
    FakeAzureFileSystem.fail_on_connect = None


# -- construction / adlfs kwargs ---------------------------------------------


def test_blob_factory_passes_credentials_through_to_adlfs(
    fake_adlfs: type[FakeAzureFileSystem],
) -> None:
    storage = azure_module.azure_blob_storage(ACCOUNT, CONTAINER, sas_token=SAS, blocksize=1024)

    assert isinstance(storage, FsspecStorage)
    (fs,) = fake_adlfs.instances
    assert fs.kwargs == {
        "account_name": ACCOUNT,
        "sas_token": SAS,
        "anon": False,
        "blocksize": 1024,
    }
    assert storage.name == f"azure:{ACCOUNT}/{CONTAINER}"
    assert storage.scheme == "abfs"
    assert storage.netloc == NETLOC


def test_every_adlfs_credential_kind_is_forwarded(fake_adlfs: type[FakeAzureFileSystem]) -> None:
    class TokenCredential:  # stands in for azure.identity credentials
        pass

    token = TokenCredential()
    cases = (
        ({"account_key": KEY}, {"account_key": KEY}),
        ({"sas_token": SAS}, {"sas_token": SAS}),
        ({"connection_string": CONNECTION_STRING}, {"connection_string": CONNECTION_STRING}),
        ({"credential": token}, {"credential": token}),
        (
            {"client_id": "cid", "client_secret": CLIENT_SECRET, "tenant_id": "tid"},
            {"client_id": "cid", "client_secret": CLIENT_SECRET, "tenant_id": "tid"},
        ),
        ({"anon": True}, {"anon": True}),
    )
    for options, expected in cases:
        fake_adlfs.instances.clear()
        azure_module.adls_storage(ACCOUNT, CONTAINER, **options)
        (fs,) = fake_adlfs.instances
        for key, value in expected.items():
            assert fs.kwargs[key] is value if key == "credential" else fs.kwargs[key] == value
        assert "account_host" not in fs.kwargs


def test_credential_methods_are_unambiguous_and_service_principal_is_complete(
    fake_adlfs: type[FakeAzureFileSystem],
) -> None:
    with pytest.raises(ValueError, match="mutually exclusive"):
        azure_module.azure_blob_storage(ACCOUNT, CONTAINER, account_key=KEY, sas_token=SAS)
    with pytest.raises(ValueError, match="requires client_id"):
        azure_module.azure_blob_storage(ACCOUNT, CONTAINER, client_id="cid")
    with pytest.raises(ValueError, match="non-empty"):
        azure_module.azure_blob_storage(ACCOUNT, CONTAINER, account_key="")
    assert fake_adlfs.instances == []


def test_sovereign_cloud_endpoint_suffix(fake_adlfs: type[FakeAzureFileSystem]) -> None:
    storage = azure_module.azure_blob_storage(
        ACCOUNT, CONTAINER, account_key=KEY, endpoint_suffix="core.chinacloudapi.cn"
    )
    (fs,) = fake_adlfs.instances
    assert fs.kwargs["account_host"] == f"{ACCOUNT}.blob.core.chinacloudapi.cn"
    assert storage.netloc == f"{CONTAINER}@{ACCOUNT}.dfs.core.chinacloudapi.cn"
    assert storage.native_uri(Location("x.parquet")) == (
        f"wasbs://{CONTAINER}@{ACCOUNT}.blob.core.chinacloudapi.cn/x.parquet"
    )
    assert storage.native_options().reveal()["endpoint_suffix"] == "core.chinacloudapi.cn"

    fake_adlfs.instances.clear()
    adls = azure_module.adls_storage(
        ACCOUNT, CONTAINER, account_key=KEY, endpoint_suffix="core.chinacloudapi.cn"
    )
    assert adls.native_uri(Location("x.parquet")) == (
        f"abfss://{CONTAINER}@{ACCOUNT}.dfs.core.chinacloudapi.cn/x.parquet"
    )


def test_connection_string_identity_is_validated_and_can_supply_sovereign_suffix(
    fake_adlfs: type[FakeAzureFileSystem],
) -> None:
    china = CONNECTION_STRING.replace("core.windows.net", "core.chinacloudapi.cn")
    storage = azure_module.azure_blob_storage(ACCOUNT, CONTAINER, connection_string=china)
    assert storage.native_uri(Location("x")) == (
        f"wasbs://{CONTAINER}@{ACCOUNT}.blob.core.chinacloudapi.cn/x"
    )

    fake_adlfs.instances.clear()
    other_account = CONNECTION_STRING.replace(f"AccountName={ACCOUNT}", "AccountName=otheracct")
    with pytest.raises(ValueError, match="does not match"):
        azure_module.azure_blob_storage(ACCOUNT, CONTAINER, connection_string=other_account)
    assert fake_adlfs.instances == []


def test_connection_string_credentials_are_translated_for_native_engines(
    fake_adlfs: type[FakeAzureFileSystem],
) -> None:
    options = (
        azure_module.azure_blob_storage(ACCOUNT, CONTAINER, connection_string=CONNECTION_STRING)
        .native_options()
        .reveal()
    )

    assert options["credential_kind"] == "connection_string"
    assert options["connection_string"] == CONNECTION_STRING
    assert options["account_key"] == KEY

    fake_adlfs.instances.clear()
    sas_connection = (
        f"DefaultEndpointsProtocol=https;AccountName={ACCOUNT};"
        f"SharedAccessSignature=?{SAS};EndpointSuffix=core.windows.net"
    )
    sas_options = (
        azure_module.adls_storage(ACCOUNT, CONTAINER, connection_string=sas_connection)
        .native_options()
        .reveal()
    )
    assert sas_options["sas_token"] == SAS


def test_missing_account_or_container_is_a_storage_error(
    fake_adlfs: type[FakeAzureFileSystem],
) -> None:
    with pytest.raises(StorageError):
        azure_module.azure_blob_storage("", CONTAINER, account_key=KEY)
    with pytest.raises(StorageError):
        azure_module.adls_storage(ACCOUNT, "", account_key=KEY)
    assert fake_adlfs.instances == []


def test_connection_failure_is_wrapped_and_redacted(fake_adlfs: type[FakeAzureFileSystem]) -> None:
    fake_adlfs.fail_on_connect = ValueError(f"unable to connect to account for {CONNECTION_STRING}")
    with pytest.raises(StorageError) as excinfo:
        azure_module.azure_blob_storage(ACCOUNT, CONTAINER, connection_string=CONNECTION_STRING)
    message = str(excinfo.value)
    assert excinfo.value.code is DiagnosticCode.STORAGE_FAILURE
    assert excinfo.value.__cause__ is None
    assert KEY not in message
    assert "ValueError" in message


def test_environment_secret_is_registered_before_provider_construction(
    fake_adlfs: type[FakeAzureFileSystem], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", ENV_KEY)
    fake_adlfs.fail_on_connect = ValueError(f"raw credential was {ENV_KEY}")

    with pytest.raises(StorageError) as excinfo:
        azure_module.azure_blob_storage(ACCOUNT, CONTAINER)

    assert ENV_KEY not in str(excinfo.value)
    assert REDACTED in str(excinfo.value)


# -- environment handling ------------------------------------------------------


def test_environment_is_ignored_when_credentials_are_explicit(
    fake_adlfs: type[FakeAzureFileSystem], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", ENV_KEY)
    monkeypatch.setenv("AZURE_STORAGE_ANON", "true")

    storage = azure_module.azure_blob_storage(ACCOUNT, CONTAINER, sas_token=SAS)

    (fs,) = fake_adlfs.instances
    assert fs.environment["AZURE_STORAGE_ACCOUNT_KEY"] is None
    assert fs.environment["AZURE_STORAGE_ANON"] is None
    assert fs.kwargs["anon"] is False
    # Restored afterwards, untouched.
    assert os.environ["AZURE_STORAGE_ACCOUNT_KEY"] == ENV_KEY
    assert os.environ["AZURE_STORAGE_ANON"] == "true"
    revealed = storage.native_options().reveal()
    assert "account_key" not in revealed
    assert revealed["sas_token"] == SAS


def test_environment_is_honoured_when_nothing_is_passed(
    fake_adlfs: type[FakeAzureFileSystem], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_KEY", ENV_KEY)

    storage = azure_module.azure_blob_storage(ACCOUNT, CONTAINER)

    (fs,) = fake_adlfs.instances
    assert fs.environment["AZURE_STORAGE_ACCOUNT_KEY"] == ENV_KEY
    assert "anon" not in fs.kwargs  # adlfs decides from AZURE_STORAGE_ANON
    revealed = storage.native_options().reveal()
    assert revealed["account_key"] == ENV_KEY
    assert ENV_KEY not in repr(storage.native_options())
    assert ENV_KEY not in repr(storage)


def test_nothing_passed_and_nothing_in_environment(fake_adlfs: type[FakeAzureFileSystem]) -> None:
    storage = azure_module.adls_storage(ACCOUNT, CONTAINER)
    assert storage.native_options().reveal() == {
        "account_name": ACCOUNT,
        "container": CONTAINER,
        "endpoint_suffix": "core.windows.net",
        "endpoint_kind": "dfs",
        "credential_kind": "default",
    }


# -- path and URI mapping --------------------------------------------------------


def test_resolve_fs_path_and_native_uri(fake_adlfs: type[FakeAzureFileSystem]) -> None:
    storage = azure_module.azure_blob_storage(ACCOUNT, CONTAINER, sas_token=SAS)

    resolved = storage.resolve("raw/orders.csv")
    assert resolved == Location("/raw/orders.csv", "abfs", NETLOC)
    assert resolved.uri == f"abfs://{NETLOC}/raw/orders.csv"
    assert storage.fs_path(resolved) == f"{CONTAINER}/raw/orders.csv"
    assert storage.fs_path(Location("/")) == CONTAINER
    assert storage.fs_path(Location("")) == CONTAINER
    blob_netloc = f"{CONTAINER}@{ACCOUNT}.blob.core.windows.net"
    assert storage.native_uri(resolved) == f"wasbs://{blob_netloc}/raw/orders.csv"
    assert storage.native_uri(Location("raw/orders.csv")) == (
        f"wasbs://{blob_netloc}/raw/orders.csv"
    )
    # An absolute location belonging to this storage resolves to itself.
    assert storage.resolve(Location.parse(f"abfs://{NETLOC}/a/b.parquet")) == Location(
        "/a/b.parquet", "abfs", NETLOC
    )


def test_root_prefix(fake_adlfs: type[FakeAzureFileSystem]) -> None:
    storage = azure_module.adls_storage(ACCOUNT, CONTAINER, account_key=KEY, root="/landing/")
    resolved = storage.resolve("2024/orders.parquet")
    assert resolved.path == "/landing/2024/orders.parquet"
    assert storage.fs_path(resolved) == f"{CONTAINER}/landing/2024/orders.parquet"
    assert storage.native_uri(resolved) == f"abfss://{NETLOC}/landing/2024/orders.parquet"


def test_dot_segments_and_control_characters_are_rejected(
    fake_adlfs: type[FakeAzureFileSystem],
) -> None:
    storage = azure_module.adls_storage(ACCOUNT, CONTAINER, account_key=KEY, root="landing")
    for path in ("../outside.csv", "a/../../outside.csv", "bad\nname.csv"):
        with pytest.raises(StorageError):
            storage.resolve(path)
    assert storage.resolve("a/./same.csv").path == "/landing/a/same.csv"
    with pytest.raises(StorageError):
        azure_module.azure_blob_storage(ACCOUNT, CONTAINER, account_key=KEY, root="a/../b")


def test_foreign_locations_are_rejected(fake_adlfs: type[FakeAzureFileSystem]) -> None:
    storage = azure_module.azure_blob_storage(ACCOUNT, CONTAINER, sas_token=SAS)
    with pytest.raises(StorageError) as excinfo:
        storage.resolve(Location.parse("s3://bucket/key"))
    assert excinfo.value.code is DiagnosticCode.STORAGE_UNSUPPORTED_OPERATION
    with pytest.raises(StorageError):
        storage.resolve(Location.parse(f"abfs://other@{ACCOUNT}.dfs.core.windows.net/x"))


def test_from_fs_path_handles_adlfs_and_memory_naming(
    fake_adlfs: type[FakeAzureFileSystem],
) -> None:
    expected = Location("/dir/file.csv", "abfs", NETLOC)
    assert azure_module._from_fs_path(CONTAINER, NETLOC, f"{CONTAINER}/dir/file.csv") == expected
    assert azure_module._from_fs_path(CONTAINER, NETLOC, f"/{CONTAINER}/dir/file.csv") == expected
    assert azure_module._from_fs_path(CONTAINER, NETLOC, CONTAINER) == Location("/", "abfs", NETLOC)
    assert azure_module._from_fs_path(CONTAINER, NETLOC, f"/{CONTAINER}") == Location(
        "/", "abfs", NETLOC
    )


# -- capabilities ------------------------------------------------------------------


def test_capabilities_are_honest_per_namespace(fake_adlfs: type[FakeAzureFileSystem]) -> None:
    blob = azure_module.azure_blob_storage(ACCOUNT, CONTAINER, account_key=KEY).capabilities
    adls = azure_module.adls_storage(ACCOUNT, CONTAINER, account_key=KEY).capabilities

    for caps in (blob, adls):
        assert caps.range_reads is True
        assert caps.listing is True
        assert caps.engine_visible_uri is True
        assert caps.evidence and all(isinstance(line, str) and line for line in caps.evidence)
    assert blob.hierarchical_directories is False
    assert blob.atomic_rename is False
    assert adls.hierarchical_directories is True
    assert adls.atomic_rename is True
    assert any("copy" in line for line in blob.evidence)
    assert any("atomic" in line for line in adls.evidence)


# -- native options / redaction ----------------------------------------------------


def test_native_options_use_canonical_keys_and_only_set_ones(
    fake_adlfs: type[FakeAzureFileSystem],
) -> None:
    storage = azure_module.azure_blob_storage(
        ACCOUNT,
        CONTAINER,
        client_id="cid",
        client_secret=CLIENT_SECRET,
        tenant_id="tid",
    )
    options = storage.native_options()
    assert isinstance(options, SecretOptions)
    assert options.ref is not None and options.ref.name == f"azure:{ACCOUNT}"
    assert set(options) == {
        "account_name",
        "container",
        "client_id",
        "client_secret",
        "tenant_id",
        "endpoint_suffix",
        "endpoint_kind",
        "credential_kind",
    }
    assert options["client_secret"] == REDACTED
    assert options.reveal() == {
        "account_name": ACCOUNT,
        "container": CONTAINER,
        "client_id": "cid",
        "client_secret": CLIENT_SECRET,
        "tenant_id": "tid",
        "endpoint_suffix": "core.windows.net",
        "endpoint_kind": "blob",
        "credential_kind": "service_principal",
    }
    assert CLIENT_SECRET not in repr(options)
    assert CLIENT_SECRET not in str(options)


def test_sas_token_is_exposed_without_leading_question_mark(
    fake_adlfs: type[FakeAzureFileSystem],
) -> None:
    storage = azure_module.azure_blob_storage(ACCOUNT, CONTAINER, sas_token="?" + SAS)
    assert storage.native_options().reveal()["sas_token"] == SAS


def test_credential_objects_are_not_translated_into_native_options(
    fake_adlfs: type[FakeAzureFileSystem],
) -> None:
    storage = azure_module.azure_blob_storage(ACCOUNT, CONTAINER, credential=object())
    assert set(storage.native_options()) == {
        "account_name",
        "container",
        "endpoint_suffix",
        "endpoint_kind",
        "credential_kind",
    }
    assert storage.native_options().reveal()["credential_kind"] == "token_credential"


def test_secrets_never_leak_through_repr_or_errors(fake_adlfs: type[FakeAzureFileSystem]) -> None:
    storage = azure_module.azure_blob_storage(
        ACCOUNT,
        CONTAINER,
        sas_token=SAS,
    )
    secrets = (SAS, "SuperSecretSignatureValue123abc")

    text = (
        repr(storage) + str(storage) + repr(storage.native_options()) + repr(storage.capabilities)
    )
    for secret in secrets:
        assert secret not in text

    location = storage.resolve("raw/orders.csv")
    for operation in (
        lambda: storage.open_read(location),
        lambda: storage.info(location),
        lambda: list(storage.list(location)),
        lambda: list(storage.list(location, recursive=True)),
        lambda: storage.exists(location),
    ):
        with pytest.raises(StorageError) as excinfo:
            operation()
        message = (
            str(excinfo.value) + repr(excinfo.value) + repr(excinfo.value.diagnostic.to_dict())
        )
        assert "***" in message
        for secret in secrets:
            assert secret not in message
        assert excinfo.value.__cause__ is None


# -- FsspecStorage over fsspec's memory filesystem ---------------------------------


@pytest.fixture()
def memory_storage(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[FsspecStorage, str]]:
    """An Azure storage whose adlfs filesystem is replaced by fsspec's in-memory one."""

    container = f"c{uuid.uuid4().hex[:8]}"
    fs = fsspec.filesystem("memory")
    fs.pipe(f"/{container}/raw/orders.csv", b"id,name\n1,alice\n2,bob\n")
    fs.pipe(f"/{container}/raw/2024/part-0.csv", b"id,name\n3,carol\n")
    fs.pipe(f"/{container}/other.txt", b"not data")
    monkeypatch.setattr(adlfs, "AzureBlobFileSystem", lambda **kwargs: fs)
    storage = azure_module.azure_blob_storage(ACCOUNT, container, sas_token=SAS)
    yield storage, container
    fs.rm(f"/{container}", recursive=True)


def test_open_read_info_list_exists_over_memory_filesystem(
    memory_storage: tuple[FsspecStorage, str],
) -> None:
    storage, container = memory_storage
    netloc = f"{container}@{ACCOUNT}.dfs.core.windows.net"

    with storage.open_read(Location("raw/orders.csv")) as handle:
        payload = handle.read()
    assert payload == b"id,name\n1,alice\n2,bob\n"
    with storage.open_read(storage.resolve("raw/orders.csv")) as handle:
        handle.seek(3)
        assert handle.read(4) == b"name"

    info = storage.info(Location("raw/orders.csv"))
    assert info.location == Location("/raw/orders.csv", "abfs", netloc)
    assert info.size == len(payload)
    assert info.is_directory is False
    assert storage.info(Location("raw")).is_directory is True

    listed = list(storage.list(Location("raw")))
    assert [entry.location.path for entry in listed] == ["/raw/2024", "/raw/orders.csv"]
    assert [entry.is_directory for entry in listed] == [True, False]
    recursive = list(storage.list(Location("raw"), recursive=True))
    assert sorted(entry.location.path for entry in recursive) == [
        "/raw/2024/part-0.csv",
        "/raw/orders.csv",
    ]
    assert all(entry.location.netloc == netloc for entry in recursive)
    assert {entry.location.path for entry in storage.list(Location("/"))} == {"/raw", "/other.txt"}

    assert storage.exists(Location("raw/orders.csv")) is True
    assert storage.exists(Location("raw/missing.csv")) is False


def test_missing_objects_over_memory_filesystem(memory_storage: tuple[FsspecStorage, str]) -> None:
    storage, _ = memory_storage
    for operation in (
        lambda: storage.open_read(Location("raw/missing.csv")),
        lambda: storage.info(Location("raw/missing.csv")),
        lambda: list(storage.list(Location("nope"))),
    ):
        with pytest.raises(StorageError) as excinfo:
            operation()
        assert excinfo.value.code is DiagnosticCode.STORAGE_OBJECT_NOT_FOUND


def test_file_source_preview_reads_through_storage(
    memory_storage: tuple[FsspecStorage, str],
) -> None:
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    import invariantql as iql

    storage, _ = memory_storage
    context = iql.Context()
    try:
        context.register_source(
            iql.file_source("orders", storage, "raw/orders.csv", iql.CsvFormat())
        )
        result = context.sql("SELECT id, name FROM orders WHERE id > 1").preview()
        try:
            rows = [row for batch in result for row in batch.to_pylist()]
        finally:
            result.close()
    finally:
        context.close()
    assert rows == [{"id": 2, "name": "bob"}]


# -- shared port conformance suite (FF-03) over the memory filesystem ---------------


class _AzureStorageContract(StorageContract):
    factory: Any = staticmethod(azure_module.azure_blob_storage)

    @pytest.fixture()
    def storage(
        self, monkeypatch: pytest.MonkeyPatch, sample_bytes: bytes
    ) -> Iterator[FsspecStorage]:
        container = f"c{uuid.uuid4().hex[:8]}"
        fs = fsspec.filesystem("memory")
        fs.pipe(f"/{container}/dir/sample.csv", sample_bytes)
        monkeypatch.setattr(adlfs, "AzureBlobFileSystem", lambda **kwargs: fs)
        yield self.factory(ACCOUNT, container, account_key=KEY)
        fs.rm(f"/{container}", recursive=True)

    @pytest.fixture()
    def sample_path(self) -> str:
        return "dir/sample.csv"

    @pytest.fixture()
    def missing_path(self) -> str:
        return "dir/missing.csv"


@pytest.mark.contract
class TestAzureBlobStorageContract(_AzureStorageContract):
    factory = staticmethod(azure_module.azure_blob_storage)


@pytest.mark.contract
class TestAdlsStorageContract(_AzureStorageContract):
    factory = staticmethod(azure_module.adls_storage)


# -- facade -----------------------------------------------------------------------------


def test_facade_factories_load_this_adapter(fake_adlfs: type[FakeAzureFileSystem]) -> None:
    import invariantql as iql

    blob = iql.azure_blob_storage(ACCOUNT, CONTAINER, sas_token=SAS)
    adls = iql.adls_storage(ACCOUNT, CONTAINER, account_key=KEY, root="landing")
    assert isinstance(blob, FsspecStorage) and isinstance(adls, FsspecStorage)
    assert blob.capabilities.atomic_rename is False
    assert adls.capabilities.atomic_rename is True
    assert adls.resolve("x").path == "/landing/x"
    assert len(fake_adlfs.instances) == 2


# -- live integration (opt-in) -------------------------------------------------------------


def _account_name_from(connection_string: str) -> str | None:
    for part in connection_string.split(";"):
        key, _, value = part.partition("=")
        if key.strip().lower() == "accountname":
            return value.strip()
    return None


@pytest.mark.integration
def test_live_azure_container_listing_and_reads() -> None:
    connection_string = os.environ.get("INVARIANTQL_AZURE_CONNECTION_STRING")
    container = os.environ.get("INVARIANTQL_AZURE_CONTAINER")
    if not connection_string or not container:
        pytest.skip("set INVARIANTQL_AZURE_CONNECTION_STRING and INVARIANTQL_AZURE_CONTAINER")
    account_name = os.environ.get("INVARIANTQL_AZURE_ACCOUNT_NAME") or _account_name_from(
        connection_string
    )
    if not account_name:
        pytest.skip("cannot determine the account name from the connection string")

    storage = azure_module.azure_blob_storage(
        account_name, container, connection_string=connection_string
    )
    assert connection_string not in repr(storage)
    assert storage.exists(Location(f"invariantql-missing-{uuid.uuid4().hex}")) is False

    entries = list(storage.list(Location("/"), recursive=True))
    files = [entry for entry in entries if not entry.is_directory]
    if not files:
        pytest.skip("container is empty; nothing to read")
    first = files[0]
    assert first.location.netloc == f"{container}@{account_name}.dfs.core.windows.net"
    info = storage.info(first.location)
    assert info.size == first.size
    with storage.open_read(first.location) as handle:
        head = handle.read(16)
    assert isinstance(head, bytes)
    assert storage.native_uri(first.location) == (
        f"wasbs://{container}@{account_name}.blob.core.windows.net{first.location.path}"
    )
