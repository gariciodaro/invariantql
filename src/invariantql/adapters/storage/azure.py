"""Azure Blob Storage and ADLS Gen2 storage factories over adlfs.

Both factories configure :class:`~invariantql.adapters.storage.fsspec_storage.FsspecStorage`
with an ``adlfs.AzureBlobFileSystem``. They differ only in the storage
semantics they declare: a flat-namespace Blob account has neither real
directories nor atomic rename, an ADLS Gen2 account (hierarchical namespace
enabled) has both.

Locations
    Internal locations use the stable ``abfs`` scheme with the netloc
    ``<container>@<account>.dfs.<endpoint_suffix>``. ``native_uri`` reflects
    the actual Spark/Hadoop endpoint: ADLS Gen2 returns ``abfss://`` at the DFS
    endpoint, while flat-namespace Blob storage returns ``wasbs://`` at the
    Blob endpoint. The adlfs path for either is ``<container>/<path>``.

Credentials
    One of the credential kinds adlfs accepts is passed through:
    ``account_key``, ``sas_token``, ``connection_string``, ``credential`` (an
    ``azure.identity`` token credential or a string), the service-principal
    triple ``client_id``/``client_secret``/``tenant_id``, and ``anon``. Secret
    values never appear in ``repr``, ``str`` or error messages; they are held
    privately by the filesystem and exposed only through
    :meth:`FsspecStorage.native_options`, a redacting ``SecretOptions`` whose
    keys follow the canonical vocabulary (``account_name``, ``account_key``,
    ``sas_token``, ``client_id``, ``client_secret``, ``tenant_id``,
    ``connection_string``), plus non-secret endpoint metadata (``container``,
    ``endpoint_suffix``, ``endpoint_kind`` and ``credential_kind``). Only
    credential keys with a value are present. A ``credential`` object cannot
    be translated for other libraries and is therefore not itself part of
    ``native_options``.

Environment variables
    adlfs reads ``AZURE_STORAGE_ACCOUNT_KEY``, ``AZURE_STORAGE_SAS_TOKEN``,
    ``AZURE_STORAGE_CONNECTION_STRING``, ``AZURE_STORAGE_CLIENT_ID``,
    ``AZURE_STORAGE_CLIENT_SECRET``, ``AZURE_STORAGE_TENANT_ID`` and
    ``AZURE_STORAGE_ANON`` and, failing those, ``azure.identity``'s
    ``DefaultAzureCredential``. That behaviour is honoured **only when the
    caller passes no credential argument at all**; whatever adlfs resolved from
    the environment then also feeds ``native_options`` so other libraries can
    reach the same account. As soon as any credential argument is given the
    environment is ignored entirely (adlfs would otherwise let an ambient
    ``AZURE_STORAGE_ACCOUNT_KEY`` override an explicit SAS token): the
    ``AZURE_STORAGE_*`` variables are removed from ``os.environ`` for the
    duration of the filesystem construction, under a process-wide lock, and
    restored afterwards.

Spark
    Reading ``abfss://`` URIs requires the ``hadoop-azure`` connector jar
    matching the Hadoop version bundled with Spark (for example
    ``org.apache.hadoop:hadoop-azure:3.3.4``) on the driver and executor
    classpath. Credentials are applied explicitly with
    ``SparkEngine.apply_storage_credentials(storage)``.
"""

from __future__ import annotations

import contextlib
import os
import re
import threading
from typing import TYPE_CHECKING, Any

import adlfs

from invariantql.adapters.storage.fsspec_storage import FsspecStorage
from invariantql.domain.credentials import CredentialRef, SecretOptions
from invariantql.domain.diagnostics import DiagnosticCode, StorageError
from invariantql.domain.location import Location
from invariantql.domain.redaction import redact_exception, register_secret
from invariantql.ports.storage import StorageCapabilities

if TYPE_CHECKING:
    from collections.abc import Iterator

DEFAULT_ENDPOINT_SUFFIX = "core.windows.net"

# Environment variables adlfs consults when a credential argument is missing.
_ADLFS_ENVIRONMENT: tuple[str, ...] = (
    "AZURE_STORAGE_ACCOUNT_NAME",
    "AZURE_STORAGE_ACCOUNT_KEY",
    "AZURE_STORAGE_CONNECTION_STRING",
    "AZURE_STORAGE_SAS_TOKEN",
    "AZURE_STORAGE_CLIENT_ID",
    "AZURE_STORAGE_CLIENT_SECRET",
    "AZURE_STORAGE_TENANT_ID",
    "AZURE_STORAGE_ANON",
)

# Canonical native_options keys in the order they are emitted.
_CREDENTIAL_KEYS: tuple[str, ...] = (
    "account_key",
    "sas_token",
    "client_id",
    "client_secret",
    "tenant_id",
    "connection_string",
)

_ACCOUNT_NAME = re.compile(r"[a-z0-9]{3,24}\Z")
_CONTAINER_NAME = re.compile(r"[a-z0-9](?:[a-z0-9]|-(?!-)){1,61}[a-z0-9]\Z")
_SYSTEM_CONTAINER = re.compile(r"\$[a-z0-9-]{1,62}\Z")
_DNS_SUFFIX = re.compile(
    r"(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)*"
    r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\Z"
)

_BLOB_CAPABILITIES = StorageCapabilities(
    range_reads=True,
    hierarchical_directories=False,
    atomic_rename=False,
    listing=True,
    engine_visible_uri=True,
    evidence=(
        "Azure Blob Storage (flat namespace): ranged GET reads, prefix listing",
        "no real directories: '/' is a naming convention on flat blob names",
        "rename is copy-then-delete, not atomic",
        "wasbs:// Blob-endpoint URIs are readable by Spark 3.5's hadoop-azure connector",
    ),
)

_ADLS_CAPABILITIES = StorageCapabilities(
    range_reads=True,
    hierarchical_directories=True,
    atomic_rename=True,
    listing=True,
    engine_visible_uri=True,
    evidence=(
        "ADLS Gen2 (hierarchical namespace): ranged GET reads, directory listing",
        "hierarchical namespace: directories are first-class objects",
        "rename is an atomic path operation of the Data Lake Storage Gen2 API",
        "abfss:// URIs are readable by Spark through the hadoop-azure connector",
    ),
)

_ENVIRONMENT_LOCK = threading.Lock()


class _AzureStorage(FsspecStorage):
    """Azure storage with distinct internal and engine-visible endpoint identities."""

    def __init__(self, *args: Any, native_netloc: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._native_netloc = native_netloc

    def resolve(self, path: str | Location) -> Location:
        location = super().resolve(path)
        _validate_object_path(location.path)
        return location

    def native_uri(self, location: Location) -> str | None:
        if self._native_scheme is None:
            return None
        resolved = self.resolve(location)
        return f"{self._native_scheme}://{self._native_netloc}{resolved.path}"


@contextlib.contextmanager
def _without_adlfs_environment() -> Iterator[None]:
    """Hide adlfs's ``AZURE_STORAGE_*`` variables while a filesystem is built.

    adlfs merges the environment into explicit arguments (``arg or getenv``),
    so an ambient account key would silently take precedence over an explicit
    SAS token. The variables are restored when construction finishes.
    """

    with _ENVIRONMENT_LOCK:
        saved = {key: os.environ.pop(key) for key in _ADLFS_ENVIRONMENT if key in os.environ}
        try:
            yield
        finally:
            os.environ.update(saved)


def _build(
    *,
    kind: str,
    capabilities: StorageCapabilities,
    native_scheme: str,
    endpoint_kind: str,
    account_name: str,
    container: str,
    account_key: str | None,
    sas_token: str | None,
    connection_string: str | None,
    credential: Any | None,
    client_id: str | None,
    client_secret: str | None,
    tenant_id: str | None,
    anon: bool,
    endpoint_suffix: str,
    root: str,
    name: str | None,
    filesystem_options: dict[str, Any],
) -> FsspecStorage:
    _validate_account_and_container(account_name, container)
    if not isinstance(endpoint_suffix, str):
        raise ValueError("Azure endpoint_suffix must be a DNS suffix string")
    endpoint_suffix = endpoint_suffix.strip(".") or DEFAULT_ENDPOINT_SUFFIX
    if connection_string is not None:
        endpoint_suffix = _validated_connection_string_suffix(
            connection_string, account_name=account_name, endpoint_suffix=endpoint_suffix
        )
    if not _DNS_SUFFIX.fullmatch(endpoint_suffix):
        raise ValueError(f"invalid Azure endpoint suffix: {endpoint_suffix!r}")
    root = _normalize_root(root)
    credential_kind = _validate_credentials(
        account_key=account_key,
        sas_token=sas_token,
        connection_string=connection_string,
        credential=credential,
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id,
        anon=anon,
    )
    if "account_host" in filesystem_options:
        raise ValueError(
            "pass endpoint_suffix instead of account_host so storage and native URIs agree"
        )

    explicit: dict[str, str] = {
        key: value
        for key, value in (
            ("account_key", account_key),
            ("sas_token", sas_token),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("tenant_id", tenant_id),
            ("connection_string", connection_string),
        )
        if value
    }
    if isinstance(credential, str) and credential:
        register_secret(credential)
    caller_passed_something = credential_kind != "default"

    native_values = dict(explicit)
    if connection_string is not None:
        native_values.update(_connection_string_credentials(connection_string))

    # Register secrets with the redaction service *before* the provider can
    # echo them in a connection error.
    _register_values(native_values)
    native_options = _secret_options(
        account_name,
        container,
        endpoint_suffix,
        endpoint_kind,
        credential_kind,
        native_values,
    )

    kwargs: dict[str, Any] = {"account_name": account_name, **filesystem_options}
    kwargs.update(explicit)
    if credential is not None:
        kwargs["credential"] = credential
    if endpoint_suffix != DEFAULT_ENDPOINT_SUFFIX:
        kwargs["account_host"] = f"{account_name}.blob.{endpoint_suffix}"

    if caller_passed_something:
        kwargs["anon"] = anon
        with _without_adlfs_environment():
            filesystem = _connect(kwargs)
    else:
        # Nothing given: let adlfs consult AZURE_STORAGE_* / DefaultAzureCredential
        # and mirror what it resolved so other libraries can reach the account.
        # Serialize this with the explicit-credential construction above: that
        # path temporarily hides adlfs's process-global environment variables.
        with _ENVIRONMENT_LOCK:
            environment_values = _environment_credentials()
            _register_values(environment_values)
            filesystem = _connect(kwargs)
            resolved = {
                key: value
                for key in _CREDENTIAL_KEYS
                if isinstance(value := getattr(filesystem, key, None), str) and value
            }
            if connection := resolved.get("connection_string"):
                resolved.update(_connection_string_credentials(connection))
            credential_kind = _resolved_credential_kind(filesystem, resolved)
            native_options = _secret_options(
                account_name,
                container,
                endpoint_suffix,
                endpoint_kind,
                credential_kind,
                resolved,
            )

    netloc = f"{container}@{account_name}.dfs.{endpoint_suffix}"
    native_host = "blob" if endpoint_kind == "blob" else "dfs"
    native_netloc = f"{container}@{account_name}.{native_host}.{endpoint_suffix}"
    return _AzureStorage(
        filesystem,
        name=name or f"{kind}:{account_name}/{container}",
        scheme="abfs",
        netloc=netloc,
        root=root,
        capabilities=capabilities,
        native_scheme=native_scheme,
        native_netloc=native_netloc,
        native_options=native_options,
        fs_path=lambda location: _fs_path(container, location),
        from_fs_path=lambda path: _from_fs_path(container, netloc, path),
    )


def _connect(kwargs: dict[str, Any]) -> Any:
    try:
        return adlfs.AzureBlobFileSystem(**kwargs)
    except Exception as exc:  # provider error translated at the edge
        raise StorageError(f"cannot connect to azure storage: {redact_exception(exc)}") from None


def _secret_options(
    account_name: str,
    container: str,
    endpoint_suffix: str,
    endpoint_kind: str,
    credential_kind: str,
    values: dict[str, str],
) -> SecretOptions:
    options: dict[str, str] = {
        "account_name": account_name,
        "container": container,
        "endpoint_suffix": endpoint_suffix,
        "endpoint_kind": endpoint_kind,
        "credential_kind": credential_kind,
    }
    for key in _CREDENTIAL_KEYS:
        value = values.get(key)
        if not value:
            continue
        if key == "sas_token":
            value = value.lstrip("?")  # adlfs prepends '?'; Hadoop wants the bare token
        options[key] = value
    return SecretOptions(
        options,
        ref=CredentialRef(f"azure:{account_name}"),
        public_keys={
            "account_name",
            "container",
            "endpoint_suffix",
            "endpoint_kind",
            "credential_kind",
        },
    )


def _validate_account_and_container(account_name: str, container: str) -> None:
    if not isinstance(account_name, str) or not _ACCOUNT_NAME.fullmatch(account_name):
        raise StorageError("azure storage account names must be 3-24 lowercase letters or digits")
    if not isinstance(container, str) or not (
        _CONTAINER_NAME.fullmatch(container) or _SYSTEM_CONTAINER.fullmatch(container)
    ):
        raise StorageError(
            "azure container names must be 3-63 lowercase letters, digits or single hyphens"
        )


def _validated_connection_string_suffix(
    connection_string: str, *, account_name: str, endpoint_suffix: str
) -> str:
    settings = _connection_string_settings(connection_string)
    configured_account = settings.get("accountname")
    if configured_account and configured_account != account_name:
        raise ValueError("Azure connection string AccountName does not match account_name")
    configured_suffix = settings.get("endpointsuffix")
    if not configured_suffix:
        return endpoint_suffix
    configured_suffix = configured_suffix.strip(".")
    if endpoint_suffix != DEFAULT_ENDPOINT_SUFFIX and configured_suffix != endpoint_suffix:
        raise ValueError("Azure connection string EndpointSuffix conflicts with endpoint_suffix")
    return configured_suffix


def _connection_string_settings(connection_string: str) -> dict[str, str]:
    settings: dict[str, str] = {}
    for part in connection_string.split(";"):
        key, separator, value = part.partition("=")
        if separator:
            settings[key.strip().lower()] = value.strip()
    return settings


def _connection_string_credentials(connection_string: str) -> dict[str, str]:
    settings = _connection_string_settings(connection_string)
    values: dict[str, str] = {}
    if account_key := settings.get("accountkey"):
        values["account_key"] = account_key
    if sas_token := settings.get("sharedaccesssignature"):
        values["sas_token"] = sas_token
    return values


def _validate_credentials(
    *,
    account_key: str | None,
    sas_token: str | None,
    connection_string: str | None,
    credential: Any | None,
    client_id: str | None,
    client_secret: str | None,
    tenant_id: str | None,
    anon: bool,
) -> str:
    strings = {
        "account_key": account_key,
        "sas_token": sas_token,
        "connection_string": connection_string,
        "client_id": client_id,
        "client_secret": client_secret,
        "tenant_id": tenant_id,
    }
    for label, value in strings.items():
        if value is not None and (not isinstance(value, str) or not value):
            raise ValueError(f"Azure {label} must be a non-empty string when supplied")
    if isinstance(credential, str) and not credential:
        raise ValueError("Azure credential must not be an empty string")
    if not isinstance(anon, bool):
        raise ValueError("Azure anon must be a boolean")

    service_principal = (client_id, client_secret, tenant_id)
    if any(value is not None for value in service_principal) and not all(
        value is not None for value in service_principal
    ):
        raise ValueError(
            "Azure service-principal authentication requires client_id, client_secret and tenant_id"
        )

    methods = [
        ("account_key", account_key is not None),
        ("sas_token", sas_token is not None),
        ("connection_string", connection_string is not None),
        ("token_credential", credential is not None),
        ("service_principal", all(value is not None for value in service_principal)),
        ("anonymous", anon),
    ]
    selected = [name for name, present in methods if present]
    if len(selected) > 1:
        raise ValueError(
            "Azure credential methods are mutually exclusive; received " + ", ".join(selected)
        )
    return selected[0] if selected else "default"


def _normalize_root(root: str) -> str:
    if not isinstance(root, str):
        raise ValueError("Azure root must be a string")
    _validate_object_path(root)
    return root.strip("/")


def _validate_object_path(path: str) -> None:
    if "\x00" in path or any(ord(character) < 32 for character in path):
        raise StorageError("Azure object paths cannot contain control characters")
    if any(part in {".", ".."} for part in path.split("/")):
        raise StorageError(
            "Azure object paths cannot contain '.' or '..' segments",
            code=DiagnosticCode.STORAGE_UNSUPPORTED_OPERATION,
        )


def _environment_credentials() -> dict[str, str]:
    mapping = {
        "account_key": "AZURE_STORAGE_ACCOUNT_KEY",
        "sas_token": "AZURE_STORAGE_SAS_TOKEN",
        "connection_string": "AZURE_STORAGE_CONNECTION_STRING",
        "client_id": "AZURE_STORAGE_CLIENT_ID",
        "client_secret": "AZURE_STORAGE_CLIENT_SECRET",
        "tenant_id": "AZURE_STORAGE_TENANT_ID",
    }
    return {
        key: value
        for key, environment_name in mapping.items()
        if (value := os.environ.get(environment_name))
    }


def _register_values(values: dict[str, str]) -> None:
    for value in values.values():
        register_secret(value)


def _resolved_credential_kind(filesystem: Any, values: dict[str, str]) -> str:
    if values.get("connection_string"):
        return "connection_string"
    if getattr(filesystem, "credential", None) is not None:
        if values.get("client_id"):
            return "service_principal"
        return "token_credential"
    if values.get("account_key"):
        return "account_key"
    if values.get("sas_token"):
        return "sas_token"
    if bool(getattr(filesystem, "anon", False)):
        return "anonymous"
    return "default"


def _fs_path(container: str, location: Location) -> str:
    path = location.path.strip("/")
    return f"{container}/{path}" if path else container


def _from_fs_path(container: str, netloc: str, fs_path: str) -> Location:
    stripped = fs_path.lstrip("/")
    if stripped == container:
        stripped = ""
    elif stripped.startswith(container + "/"):
        stripped = stripped[len(container) + 1 :]
    return Location("/" + stripped, "abfs", netloc)


def azure_blob_storage(
    account_name: str,
    container: str,
    *,
    account_key: str | None = None,
    sas_token: str | None = None,
    connection_string: str | None = None,
    credential: Any | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
    tenant_id: str | None = None,
    anon: bool = False,
    endpoint_suffix: str = DEFAULT_ENDPOINT_SUFFIX,
    root: str = "",
    name: str | None = None,
    **filesystem_options: Any,
) -> FsspecStorage:
    """Azure Blob Storage (flat namespace) through adlfs.

    Parameters
        account_name, container
            The storage account and the container (``abfs://container@account...``).
        account_key, sas_token, connection_string, credential,
        client_id / client_secret / tenant_id, anon
            Credential kinds accepted by adlfs, passed through unchanged. When
            none is given adlfs's ``AZURE_STORAGE_*`` environment variables and
            ``DefaultAzureCredential`` apply; otherwise the environment is
            ignored (see the module docstring).
        endpoint_suffix
            ``core.windows.net`` by default; sovereign clouds use for example
            ``core.chinacloudapi.cn``. It selects both the ``dfs`` host in
            locations and the ``blob`` host adlfs connects to.
        root
            Optional prefix inside the container that relative paths resolve
            against.
        name
            Storage name shown in explain output (default ``azure:<account>/<container>``).
        **filesystem_options
            Any further ``adlfs.AzureBlobFileSystem`` keyword (``blocksize``,
            ``timeout``, ``assume_container_exists`` ...).

    Semantics
        Range reads and listing are supported and ``wasbs://`` Blob-endpoint
        URIs are engine visible to the Hadoop version bundled with Spark 3.5.
        A flat-namespace account has no real directories and no
        atomic rename (moves are copy-then-delete); the capabilities say so.
        Use :func:`adls_storage` for accounts with the hierarchical namespace.

    Spark
        Needs the ``hadoop-azure`` connector jar (``org.apache.hadoop:hadoop-azure``
        matching Spark's Hadoop version) on the classpath.
    """

    return _build(
        kind="azure",
        capabilities=_BLOB_CAPABILITIES,
        native_scheme="wasbs",
        endpoint_kind="blob",
        account_name=account_name,
        container=container,
        account_key=account_key,
        sas_token=sas_token,
        connection_string=connection_string,
        credential=credential,
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id,
        anon=anon,
        endpoint_suffix=endpoint_suffix,
        root=root,
        name=name,
        filesystem_options=filesystem_options,
    )


def adls_storage(
    account_name: str,
    container: str,
    *,
    account_key: str | None = None,
    sas_token: str | None = None,
    connection_string: str | None = None,
    credential: Any | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
    tenant_id: str | None = None,
    anon: bool = False,
    endpoint_suffix: str = DEFAULT_ENDPOINT_SUFFIX,
    root: str = "",
    name: str | None = None,
    **filesystem_options: Any,
) -> FsspecStorage:
    """Azure Data Lake Storage Gen2 (hierarchical namespace) through adlfs.

    Same parameters, credential handling and Spark requirements as
    :func:`azure_blob_storage`. The account must have the hierarchical
    namespace enabled: the adapter then declares real directories and atomic
    rename, which a flat Blob account cannot honour. adlfs talks to the Blob
    endpoint for both; the declared semantics come from the account type, so
    pick the factory that matches the account. Default name:
    ``adls:<account>/<container>``.
    """

    return _build(
        kind="adls",
        capabilities=_ADLS_CAPABILITIES,
        native_scheme="abfss",
        endpoint_kind="dfs",
        account_name=account_name,
        container=container,
        account_key=account_key,
        sas_token=sas_token,
        connection_string=connection_string,
        credential=credential,
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id,
        anon=anon,
        endpoint_suffix=endpoint_suffix,
        root=root,
        name=name,
        filesystem_options=filesystem_options,
    )


__all__ = ["DEFAULT_ENDPOINT_SUFFIX", "adls_storage", "azure_blob_storage"]
