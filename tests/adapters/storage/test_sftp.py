"""SFTP storage adapter: unit tests over fakes, live tests gated on ``INVARIANTQL_SFTP_HOST``.

The unit tests never open a network connection: the fsspec/paramiko
filesystem class is replaced by an in-memory fake and paramiko's ``SSHClient``
by a recorder. Live tests need ``INVARIANTQL_SFTP_HOST`` plus
``INVARIANTQL_SFTP_USER`` and ``INVARIANTQL_SFTP_PASSWORD`` (or
``INVARIANTQL_SFTP_KEY_FILE``); ``INVARIANTQL_SFTP_ROOT`` must be writable.
"""

from __future__ import annotations

import datetime as dt
import io
import os
import uuid
from typing import Any

import pytest

from invariantql.adapters.storage.fsspec_storage import FsspecStorage
from invariantql.domain.credentials import EMPTY_SECRETS
from invariantql.domain.diagnostics import DiagnosticCode, StorageError
from invariantql.domain.location import Location
from invariantql.ports.storage import Storage

paramiko = pytest.importorskip("paramiko")
pytest.importorskip("fsspec")
pytest.importorskip("cryptography")
sftp_module = pytest.importorskip("invariantql.adapters.storage.sftp")

HOST = "sftp.example.test"
PASSWORD = "hunter2-Sup3r-Secret-Pa55w0rd"
PASSPHRASE = "key-passphrase-0xDEADBEEF"
CSV = b"id,name,amount\n1,alice,10.5\n2,bob,20.0\n3,,5.25\n"


# -- fakes ----------------------------------------------------------------------


class FakeSftpFileSystem:
    """Stands in for ``SftpFileSystem``: records constructor arguments, serves an in-memory tree."""

    instances: list[FakeSftpFileSystem] = []
    constructor_error: BaseException | None = None
    open_error: BaseException | None = None
    files: dict[str, bytes] = {
        "/srv/data/orders.csv": CSV,
        "/srv/data/nested/part-0.csv": b"id\n4\n",
        "/etc/motd": b"welcome\n",
    }

    def __init__(self, host: str, **kwargs: Any) -> None:
        if FakeSftpFileSystem.constructor_error is not None:
            raise FakeSftpFileSystem.constructor_error
        self.host = host
        self.kwargs = kwargs
        self.opened: list[str] = []
        self.closed = False
        FakeSftpFileSystem.instances.append(self)

    def close(self) -> None:
        self.closed = True

    @classmethod
    def _directories(cls) -> set[str]:
        dirs = {"/"}
        for path in cls.files:
            parent = path.rsplit("/", 1)[0]
            while parent:
                dirs.add(parent)
                parent = parent.rsplit("/", 1)[0]
        return dirs

    def _entry(self, path: str) -> dict[str, Any]:
        if path in self.files:
            return {
                "name": path,
                "size": len(self.files[path]),
                "type": "file",
                "mtime": dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            }
        if path in self._directories():
            return {"name": path, "size": 0, "type": "directory", "mtime": None}
        raise FileNotFoundError(path)

    def _children(self, path: str) -> list[str]:
        prefix = path.rstrip("/") + "/"
        return sorted(
            {prefix + p[len(prefix) :].split("/", 1)[0] for p in self.files if p.startswith(prefix)}
        )

    def info(self, path: str) -> dict[str, Any]:
        return self._entry(path)

    def ls(self, path: str, detail: bool = False) -> list[Any]:
        if path not in self._directories():
            entry = self._entry(path)
            return [entry] if detail else [path]
        entries = [self._entry(child) for child in self._children(path)]
        return entries if detail else [e["name"] for e in entries]

    def find(self, path: str, detail: bool = False) -> Any:
        prefix = path.rstrip("/") + "/"
        found = {p: self._entry(p) for p in sorted(self.files) if p.startswith(prefix)}
        return found if detail else list(found)

    def open(self, path: str, mode: str = "rb") -> io.BytesIO:
        if FakeSftpFileSystem.open_error is not None:
            raise FakeSftpFileSystem.open_error
        if mode != "rb":
            raise ValueError(mode)
        if path not in self.files:
            raise FileNotFoundError(path)
        self.opened.append(path)
        return io.BytesIO(self.files[path])

    def exists(self, path: str) -> bool:
        return path in self.files or path in self._directories()


class RecordingSSHClient:
    """Stands in for ``paramiko.SSHClient``: records the host-key set-up and the connect call."""

    instances: list[RecordingSSHClient] = []
    fail_open_sftp = False

    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []
        RecordingSSHClient.instances.append(self)

    def load_system_host_keys(self, filename: str | None = None) -> None:
        self.calls.append(("load_system_host_keys", filename))

    def load_host_keys(self, filename: str) -> None:
        self.calls.append(("load_host_keys", filename))

    def set_missing_host_key_policy(self, policy: Any) -> None:
        self.calls.append(("policy", policy))

    def connect(self, hostname: str, **kwargs: Any) -> None:
        self.calls.append(("connect", hostname, kwargs))

    def open_sftp(self) -> object:
        self.calls.append(("open_sftp",))
        if self.fail_open_sftp:
            raise RuntimeError("SFTP subsystem unavailable")
        return object()

    def close(self) -> None:
        self.calls.append(("close",))


@pytest.fixture()
def fake_fs(monkeypatch: pytest.MonkeyPatch) -> type[FakeSftpFileSystem]:
    FakeSftpFileSystem.instances.clear()
    FakeSftpFileSystem.constructor_error = None
    FakeSftpFileSystem.open_error = None
    monkeypatch.setattr(sftp_module, "SftpFileSystem", FakeSftpFileSystem)
    return FakeSftpFileSystem


def make_storage(**options: Any) -> FsspecStorage:
    return sftp_module.sftp_storage(HOST, username="alice", password=PASSWORD, **options)


def ed25519_pem(passphrase: str | None = None) -> str:
    """An OpenSSH-format Ed25519 private key (paramiko cannot generate one itself)."""

    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519

    encryption: serialization.KeySerializationEncryption = (
        serialization.NoEncryption()
        if passphrase is None
        else serialization.BestAvailableEncryption(passphrase.encode())
    )
    key = ed25519.Ed25519PrivateKey.generate()
    return key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.OpenSSH, encryption
    ).decode()


def paramiko_pem(key: Any, passphrase: str | None = None) -> str:
    buffer = io.StringIO()
    key.write_private_key(buffer, password=passphrase)
    return buffer.getvalue()


# -- construction --------------------------------------------------------------


def test_factory_forwards_connection_arguments(fake_fs: type[FakeSftpFileSystem]) -> None:
    storage = make_storage(timeout=5)

    fs = fake_fs.instances[-1]
    assert storage.fs is fs
    assert fs.host == HOST
    assert fs.kwargs["port"] == 22
    assert fs.kwargs["username"] == "alice"
    assert fs.kwargs["password"] == PASSWORD
    assert fs.kwargs["timeout"] == 5
    assert fs.kwargs["skip_instance_cache"] is True
    assert fs.kwargs["host_key_policy"] == "reject"
    assert fs.kwargs["known_hosts"] is None
    assert "pkey" not in fs.kwargs
    assert "key_filename" not in fs.kwargs
    assert "private_key" not in fs.kwargs


def test_default_identity(fake_fs: type[FakeSftpFileSystem]) -> None:
    storage = make_storage()

    assert isinstance(storage, FsspecStorage)
    assert isinstance(storage, Storage)
    assert storage.name == f"sftp:alice@{HOST}:22/"
    assert storage.scheme == "sftp"
    assert storage.netloc == f"{HOST}:22"


def test_port_root_and_name_options(fake_fs: type[FakeSftpFileSystem]) -> None:
    storage = sftp_module.sftp_storage(HOST, port=2222, root="/srv/data/", name="landing")

    assert storage.netloc == f"{HOST}:2222"
    assert storage.name == "landing"
    assert fake_fs.instances[-1].kwargs["port"] == 2222
    assert "username" not in fake_fs.instances[-1].kwargs
    assert sftp_module.sftp_storage(HOST, root="/srv/data").name == f"sftp:{HOST}:22/srv/data"


def test_extra_paramiko_kwargs_and_host_key_options_are_forwarded(
    fake_fs: type[FakeSftpFileSystem], tmp_path: Any
) -> None:
    known = tmp_path / "known_hosts"
    make_storage(
        key_filename=tmp_path / "id_ed25519",
        known_hosts=known,
        host_key_policy="reject",
        look_for_keys=False,
        allow_agent=False,
    )

    kwargs = fake_fs.instances[-1].kwargs
    assert kwargs["key_filename"] == str(tmp_path / "id_ed25519")
    assert kwargs["known_hosts"] == known
    assert kwargs["host_key_policy"] == "reject"
    assert kwargs["look_for_keys"] is False
    assert kwargs["allow_agent"] is False


@pytest.mark.parametrize("root", ["data", "", "~/data"])
def test_root_must_be_absolute(fake_fs: type[FakeSftpFileSystem], root: str) -> None:
    with pytest.raises(ValueError, match="absolute"):
        make_storage(root=root)
    assert fake_fs.instances == []


def test_invalid_arguments_are_rejected_before_connecting(
    fake_fs: type[FakeSftpFileSystem],
) -> None:
    with pytest.raises(ValueError, match="host_key_policy"):
        make_storage(host_key_policy="trust-everyone")
    with pytest.raises(ValueError, match="pkey"):
        make_storage(pkey=object())
    with pytest.raises(ValueError, match="host"):
        sftp_module.sftp_storage("")
    with pytest.raises(ValueError, match="port"):
        sftp_module.sftp_storage(HOST, port=0)
    with pytest.raises(ValueError, match="host"):
        sftp_module.sftp_storage("user@example.test")
    assert fake_fs.instances == []


def test_ipv6_hosts_are_rendered_unambiguously(fake_fs: type[FakeSftpFileSystem]) -> None:
    storage = sftp_module.sftp_storage("2001:db8::10", port=2222)

    assert fake_fs.instances[-1].host == "2001:db8::10"
    assert storage.netloc == "[2001:db8::10]:2222"
    assert storage.resolve("x.csv").uri == "sftp://[2001:db8::10]:2222/x.csv"


def test_facade_factory_loads_this_adapter(fake_fs: type[FakeSftpFileSystem]) -> None:
    import invariantql as iql

    storage = iql.sftp_storage(HOST, username="alice", password=PASSWORD, root="/srv")

    assert isinstance(storage, FsspecStorage)
    assert storage.resolve("x.csv").uri == f"sftp://{HOST}:22/srv/x.csv"
    assert fake_fs.instances[-1].kwargs["password"] == PASSWORD


# -- path mapping ----------------------------------------------------------------


def test_paths_resolve_to_absolute_remote_paths(fake_fs: type[FakeSftpFileSystem]) -> None:
    storage = make_storage()

    for path in ("data/orders.csv", "/data/orders.csv", Location("data/orders.csv")):
        location = storage.resolve(path)
        assert location == Location("/data/orders.csv", "sftp", f"{HOST}:22")
        assert location.uri == f"sftp://{HOST}:22/data/orders.csv"
        assert storage.fs_path(location) == "/data/orders.csv"
    assert storage.resolve("").path == "/"
    assert storage.fs_path(storage.resolve("")) == "/"


def test_paths_are_jailed_under_root(fake_fs: type[FakeSftpFileSystem]) -> None:
    storage = make_storage(root="/srv/data")

    assert storage.resolve("orders.csv").uri == f"sftp://{HOST}:22/srv/data/orders.csv"
    assert storage.resolve("/orders.csv").uri == f"sftp://{HOST}:22/srv/data/orders.csv"
    assert storage.fs_path(storage.resolve("nested/part-0.csv")) == "/srv/data/nested/part-0.csv"

    absolute = Location.parse(f"sftp://{HOST}:22/srv/data/orders.csv")
    assert storage.resolve(absolute) == absolute
    assert storage.fs_path(absolute) == "/srv/data/orders.csv"

    for escaping in (
        "../etc/motd",
        "nested/../../../etc/motd",
        Location.parse(f"sftp://{HOST}:22/etc/motd"),
    ):
        with pytest.raises(StorageError) as info:
            storage.resolve(escaping)
        assert info.value.code is DiagnosticCode.STORAGE_UNSUPPORTED_OPERATION
    assert fake_fs.instances[-1].opened == []


def test_foreign_locations_are_rejected(fake_fs: type[FakeSftpFileSystem]) -> None:
    storage = make_storage()

    for foreign in ("s3://bucket/orders.csv", "sftp://other.example.test:22/orders.csv"):
        with pytest.raises(StorageError) as info:
            storage.resolve(Location.parse(foreign))
        assert info.value.code is DiagnosticCode.STORAGE_UNSUPPORTED_OPERATION


# -- capabilities and engine visibility -------------------------------------------


def test_capabilities_are_honest(fake_fs: type[FakeSftpFileSystem]) -> None:
    capabilities = make_storage().capabilities

    assert capabilities.range_reads is True
    assert capabilities.hierarchical_directories is True
    assert capabilities.atomic_rename is False
    assert capabilities.listing is True
    assert capabilities.engine_visible_uri is False
    assert len(capabilities.evidence) == 4
    assert any("seek" in item for item in capabilities.evidence)
    assert any("staged" in item for item in capabilities.evidence)


def test_no_native_uri_and_no_native_options(fake_fs: type[FakeSftpFileSystem]) -> None:
    storage = make_storage()

    assert storage.native_uri(storage.resolve("orders.csv")) is None
    assert storage.native_options() is EMPTY_SECRETS
    assert len(storage.native_options()) == 0


def test_spark_reports_staging_required(fake_fs: type[FakeSftpFileSystem]) -> None:
    pytest.importorskip("pyspark")
    import invariantql as iql
    from invariantql.adapters.spark_engine.engine import SparkEngine

    source = iql.file_source("orders", make_storage(), "orders.csv", iql.CsvFormat())
    reachability = SparkEngine(object()).reachability(source)  # type: ignore[arg-type]

    assert reachability.reachable is False
    assert "stage" in reachability.reason


# -- operations through the fake filesystem ----------------------------------------


def test_listing_info_open_and_exists(fake_fs: type[FakeSftpFileSystem]) -> None:
    storage = make_storage(root="/srv/data")

    listed = list(storage.list(storage.resolve("")))
    assert [entry.location.uri for entry in listed] == [
        f"sftp://{HOST}:22/srv/data/nested",
        f"sftp://{HOST}:22/srv/data/orders.csv",
    ]
    assert listed[0].is_directory and listed[0].size is None
    assert listed[1].size == len(CSV)
    assert listed[1].modified_at == dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)

    recursive = list(storage.list(storage.resolve(""), recursive=True))
    assert [entry.location.path for entry in recursive] == [
        "/srv/data/nested/part-0.csv",
        "/srv/data/orders.csv",
    ]

    info = storage.info(storage.resolve("orders.csv"))
    assert info.location.uri == f"sftp://{HOST}:22/srv/data/orders.csv"
    assert info.size == len(CSV) and not info.is_directory

    with storage.open_read(storage.resolve("orders.csv")) as handle:
        handle.seek(3)
        assert handle.read(4) == b"name"
    assert fake_fs.instances[-1].opened == ["/srv/data/orders.csv"]

    assert storage.exists(storage.resolve("orders.csv"))
    assert storage.exists(storage.resolve("nested"))
    assert not storage.exists(storage.resolve("missing.csv"))


def test_missing_objects_raise_not_found(fake_fs: type[FakeSftpFileSystem]) -> None:
    storage = make_storage(root="/srv/data")

    for operation in (storage.info, storage.open_read, lambda loc: list(storage.list(loc))):
        with pytest.raises(StorageError) as info:
            operation(storage.resolve("missing.csv"))
        assert info.value.code is DiagnosticCode.STORAGE_OBJECT_NOT_FOUND
        assert f"sftp://{HOST}:22/srv/data/missing.csv" in str(info.value)


def test_duckdb_reads_csv_through_the_storage_port(fake_fs: type[FakeSftpFileSystem]) -> None:
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    import invariantql as iql

    context = iql.Context()
    try:
        storage = make_storage(root="/srv/data")
        context.register_source(iql.file_source("orders", storage, "orders.csv", iql.CsvFormat()))
        rows = context.sql("SELECT id, name FROM orders WHERE amount > 6").execute().rows()
    finally:
        context.close()

    assert rows == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    assert fake_fs.instances[-1].opened and set(fake_fs.instances[-1].opened) == {
        "/srv/data/orders.csv"
    }


# -- redaction ---------------------------------------------------------------------


def test_password_never_appears_in_repr_or_name(fake_fs: type[FakeSftpFileSystem]) -> None:
    storage = make_storage()

    for text in (repr(storage), str(storage), storage.name, repr(storage.native_options())):
        assert PASSWORD not in text
        assert "hunter2" not in text


def test_even_short_passwords_are_redacted(fake_fs: type[FakeSftpFileSystem]) -> None:
    short_secret = "p4ss"
    fake_fs.constructor_error = RuntimeError(f"authentication failed with {short_secret}")

    with pytest.raises(StorageError) as info:
        sftp_module.sftp_storage(HOST, password=short_secret)

    assert short_secret not in str(info.value)
    assert "***" in str(info.value)


def test_connection_failure_is_wrapped_and_redacted(fake_fs: type[FakeSftpFileSystem]) -> None:
    fake_fs.constructor_error = paramiko.AuthenticationException(
        f"Authentication failed for alice with password={PASSWORD} ({PASSWORD})"
    )

    with pytest.raises(StorageError) as info:
        make_storage()

    message = str(info.value)
    assert PASSWORD not in message
    assert "AuthenticationException" in message
    assert f"sftp://{HOST}:22" in message
    assert info.value.code is DiagnosticCode.STORAGE_FAILURE
    assert info.value.__cause__ is None
    assert info.value.__suppress_context__ is True


def test_provider_errors_after_connecting_are_redacted(
    fake_fs: type[FakeSftpFileSystem],
) -> None:
    storage = make_storage(root="/srv/data")
    fake_fs.open_error = OSError(f"channel closed while authenticating with {PASSWORD}")

    with pytest.raises(StorageError) as info:
        storage.open_read(storage.resolve("orders.csv"))

    assert PASSWORD not in str(info.value)
    assert "OSError" in str(info.value)


def test_private_key_text_never_leaks(fake_fs: type[FakeSftpFileSystem]) -> None:
    pem = ed25519_pem(PASSPHRASE)
    body_line = pem.splitlines()[1]
    fake_fs.constructor_error = paramiko.SSHException(f"server said: {pem} / {PASSPHRASE}")

    with pytest.raises(StorageError) as info:
        sftp_module.sftp_storage(HOST, private_key=pem, passphrase=PASSPHRASE)

    message = str(info.value)
    assert pem not in message
    assert body_line not in message
    assert PASSPHRASE not in message


# -- PEM to paramiko key ---------------------------------------------------------------


def test_private_key_pem_becomes_a_paramiko_pkey(fake_fs: type[FakeSftpFileSystem]) -> None:
    pem = ed25519_pem()
    expected = paramiko.Ed25519Key.from_private_key(io.StringIO(pem))

    storage = sftp_module.sftp_storage(HOST, username="alice", private_key=pem)

    kwargs = fake_fs.instances[-1].kwargs
    assert isinstance(kwargs["pkey"], paramiko.Ed25519Key)
    assert kwargs["pkey"].fingerprint == expected.fingerprint
    assert "password" not in kwargs
    assert "passphrase" not in kwargs
    assert not any(isinstance(v, str) and "PRIVATE KEY" in v for v in kwargs.values())
    assert "PRIVATE KEY" not in repr(storage)


def test_encrypted_ed25519_key_needs_its_passphrase(fake_fs: type[FakeSftpFileSystem]) -> None:
    pem = ed25519_pem(PASSPHRASE)
    expected = paramiko.Ed25519Key.from_private_key(io.StringIO(pem), password=PASSPHRASE)

    key = sftp_module.private_key_from_pem(pem, PASSPHRASE)
    assert isinstance(key, paramiko.Ed25519Key)
    assert key.fingerprint == expected.fingerprint

    with pytest.raises(StorageError, match="passphrase"):
        sftp_module.private_key_from_pem(pem)
    with pytest.raises(StorageError, match="passphrase") as info:
        sftp_module.private_key_from_pem(pem, "not-the-passphrase")
    assert pem.splitlines()[1] not in str(info.value)

    sftp_module.sftp_storage(HOST, private_key=pem, passphrase=PASSPHRASE)
    assert fake_fs.instances[-1].kwargs["pkey"].fingerprint == expected.fingerprint
    assert fake_fs.instances[-1].kwargs["passphrase"] == PASSPHRASE


def test_traditional_pem_rsa_and_ecdsa_keys_are_supported() -> None:
    rsa = paramiko.RSAKey.generate(2048)
    ecdsa = paramiko.ECDSAKey.generate()

    loaded_rsa = sftp_module.private_key_from_pem(paramiko_pem(rsa))
    assert isinstance(loaded_rsa, paramiko.RSAKey)
    assert loaded_rsa.fingerprint == rsa.fingerprint

    loaded_encrypted = sftp_module.private_key_from_pem(paramiko_pem(rsa, PASSPHRASE), PASSPHRASE)
    assert loaded_encrypted.fingerprint == rsa.fingerprint

    loaded_ecdsa = sftp_module.private_key_from_pem(paramiko_pem(ecdsa))
    assert isinstance(loaded_ecdsa, paramiko.ECDSAKey)
    assert loaded_ecdsa.fingerprint == ecdsa.fingerprint


def test_key_class_is_chosen_from_the_header() -> None:
    assert sftp_module._candidate_key_classes(ed25519_pem()) == (paramiko.Ed25519Key,)
    assert sftp_module._candidate_key_classes(paramiko_pem(paramiko.ECDSAKey.generate())) == (
        paramiko.ECDSAKey,
    )
    assert sftp_module._candidate_key_classes(
        "-----BEGIN RSA PRIVATE KEY-----\nAAAA\n-----END"
    ) == (paramiko.RSAKey,)
    assert sftp_module._candidate_key_classes(
        "-----BEGIN OPENSSH PRIVATE KEY-----\n!!\n-----END"
    ) == (
        paramiko.Ed25519Key,
        paramiko.ECDSAKey,
        paramiko.RSAKey,
    )


def test_unsupported_key_material_is_rejected() -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519

    pkcs8 = (
        ed25519.Ed25519PrivateKey.generate()
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        .decode()
    )
    with pytest.raises(StorageError, match="PKCS#8"):
        sftp_module.private_key_from_pem(pkcs8)
    with pytest.raises(StorageError, match="Ed25519, ECDSA or RSA"):
        sftp_module.private_key_from_pem("this is not a key at all")
    with pytest.raises(StorageError, match="empty"):
        sftp_module.private_key_from_pem("   \n")


# -- host keys (real SftpFileSystem, fake paramiko client) -------------------------------


def test_filesystem_loads_known_hosts_before_connecting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    monkeypatch.setattr(sftp_module.paramiko, "SSHClient", RecordingSSHClient)
    RecordingSSHClient.instances.clear()
    known = tmp_path / "known_hosts"
    known.write_text("")

    fs = sftp_module.SftpFileSystem(
        HOST,
        port=2222,
        username="alice",
        known_hosts=known,
        host_key_policy="reject",
        skip_instance_cache=True,
    )

    client = RecordingSSHClient.instances[-1]
    assert fs.client is client
    assert [call[0] for call in client.calls] == [
        "load_system_host_keys",
        "load_host_keys",
        "policy",
        "connect",
        "open_sftp",
    ]
    assert client.calls[1][1] == str(known)
    assert isinstance(client.calls[2][1], paramiko.RejectPolicy)
    assert client.calls[3][1:] == (HOST, {"port": 2222, "username": "alice"})


def test_filesystem_rejects_unknown_hosts_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sftp_module.paramiko, "SSHClient", RecordingSSHClient)
    RecordingSSHClient.instances.clear()

    sftp_module.SftpFileSystem(HOST, skip_instance_cache=True)

    client = RecordingSSHClient.instances[-1]
    assert [call[0] for call in client.calls] == [
        "load_system_host_keys",
        "policy",
        "connect",
        "open_sftp",
    ]
    assert isinstance(client.calls[1][1], paramiko.RejectPolicy)


def test_filesystem_closes_ssh_client_when_sftp_channel_open_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sftp_module.paramiko, "SSHClient", RecordingSSHClient)
    RecordingSSHClient.instances.clear()
    RecordingSSHClient.fail_open_sftp = True
    try:
        with pytest.raises(RuntimeError, match="subsystem unavailable"):
            sftp_module.SftpFileSystem(HOST, skip_instance_cache=True)
    finally:
        RecordingSSHClient.fail_open_sftp = False

    client = RecordingSSHClient.instances[-1]
    assert [call[0] for call in client.calls][-2:] == ["open_sftp", "close"]


def test_storage_close_releases_the_filesystem(fake_fs: type[FakeSftpFileSystem]) -> None:
    storage = make_storage()
    filesystem = fake_fs.instances[-1]

    storage.close()
    storage.close()

    assert filesystem.closed is True


def test_host_key_policy_names() -> None:
    assert isinstance(sftp_module.missing_host_key_policy("auto-add"), paramiko.AutoAddPolicy)
    assert isinstance(sftp_module.missing_host_key_policy("reject"), paramiko.RejectPolicy)
    assert isinstance(sftp_module.missing_host_key_policy("warn"), paramiko.WarningPolicy)
    with pytest.raises(ValueError, match="auto-add, reject, warn"):
        sftp_module.missing_host_key_policy("ask")


# -- live server ---------------------------------------------------------------------------

live = pytest.mark.skipif(
    not os.environ.get("INVARIANTQL_SFTP_HOST"),
    reason="set INVARIANTQL_SFTP_HOST (+ INVARIANTQL_SFTP_USER / INVARIANTQL_SFTP_PASSWORD)",
)


@pytest.fixture()
def live_storage() -> FsspecStorage:
    env = os.environ
    return sftp_module.sftp_storage(
        env["INVARIANTQL_SFTP_HOST"],
        port=int(env.get("INVARIANTQL_SFTP_PORT", "22")),
        username=env.get("INVARIANTQL_SFTP_USER"),
        password=env.get("INVARIANTQL_SFTP_PASSWORD"),
        key_filename=env.get("INVARIANTQL_SFTP_KEY_FILE"),
        root=env.get("INVARIANTQL_SFTP_ROOT", "/"),
        host_key_policy=env.get("INVARIANTQL_SFTP_HOST_KEY_POLICY", "reject"),
    )


@pytest.fixture()
def live_dataset(live_storage: FsspecStorage) -> Any:
    """Upload a small CSV into a fresh directory under the root; remove it afterwards."""

    directory = f"invariantql-it-{uuid.uuid4().hex[:8]}"
    remote_dir = live_storage.fs_path(live_storage.resolve(directory))
    live_storage.fs.makedirs(remote_dir, exist_ok=True)
    live_storage.fs.pipe(f"{remote_dir}/orders.csv", CSV)
    try:
        yield directory
    finally:
        live_storage.fs.rm(remote_dir, recursive=True)


@pytest.mark.integration
@live
def test_live_listing_info_and_range_reads(live_storage: FsspecStorage, live_dataset: str) -> None:
    location = live_storage.resolve(f"{live_dataset}/orders.csv")

    assert live_storage.exists(location)
    info = live_storage.info(location)
    assert info.size == len(CSV) and not info.is_directory and info.modified_at is not None

    listed = list(live_storage.list(live_storage.resolve(live_dataset)))
    assert [entry.location for entry in listed] == [location]

    with live_storage.open_read(location) as handle:
        handle.seek(len(CSV) - 5)
        assert handle.read() == CSV[-5:]
        handle.seek(0)
        assert handle.read(2) == b"id"

    assert live_storage.native_uri(location) is None
    with pytest.raises(StorageError) as failure:
        live_storage.info(live_storage.resolve(f"{live_dataset}/missing.csv"))
    assert failure.value.code is DiagnosticCode.STORAGE_OBJECT_NOT_FOUND


@pytest.mark.integration
@live
def test_live_duckdb_query(live_storage: FsspecStorage, live_dataset: str) -> None:
    pytest.importorskip("duckdb")
    import invariantql as iql

    context = iql.Context()
    try:
        context.register_source(
            iql.file_source("orders", live_storage, f"{live_dataset}/orders.csv", iql.CsvFormat())
        )
        rows = (
            context.sql("SELECT id, name FROM orders WHERE amount > 6 ORDER BY id").execute().rows()
        )
    finally:
        context.close()

    assert rows == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


@pytest.mark.integration
@live
def test_live_wrong_password_is_redacted() -> None:
    bogus = "definitely-not-the-password-XyZ123"
    with pytest.raises(StorageError) as failure:
        sftp_module.sftp_storage(
            os.environ["INVARIANTQL_SFTP_HOST"],
            port=int(os.environ.get("INVARIANTQL_SFTP_PORT", "22")),
            username=os.environ.get("INVARIANTQL_SFTP_USER"),
            password=bogus,
            look_for_keys=False,
            allow_agent=False,
        )
    assert bogus not in str(failure.value)
