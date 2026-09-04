"""Amazon S3 (and S3-compatible) object storage through ``s3fs``.

This module configures the generic :class:`FsspecStorage` adapter with an
``s3fs.S3FileSystem``, the ``s3://bucket/key`` location mapping, honest
object-store capabilities and the canonical ``aws_*`` native options other
libraries (Spark, DuckDB) translate into their own credential settings.

``s3fs`` is imported at module top; the facade imports this module lazily
(ADR-0010), so a missing ``s3`` extra surfaces only when S3 storage is used.
"""

from __future__ import annotations

import ipaddress
import os
import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import s3fs

from invariantql.adapters.storage.fsspec_storage import FsspecStorage
from invariantql.domain.credentials import CredentialRef, SecretOptions
from invariantql.domain.diagnostics import DiagnosticCode, StorageError
from invariantql.domain.location import Location
from invariantql.domain.redaction import redact_exception, register_secret
from invariantql.ports.storage import StorageCapabilities

if TYPE_CHECKING:
    from collections.abc import Callable

S3_SCHEME = "s3"
"""Scheme of the locations this storage resolves (``s3://bucket/key``)."""

S3_NATIVE_SCHEME = "s3a"
"""Scheme of the engine-visible URI (``s3a://bucket/key``, read by Spark/Hadoop)."""

SPARK_CONNECTOR = "org.apache.hadoop:hadoop-aws (plus com.amazonaws:aws-java-sdk-bundle)"
"""The connector jar Spark needs on its classpath to read ``s3a://`` URIs."""

S3_CAPABILITIES = StorageCapabilities(
    range_reads=True,
    hierarchical_directories=False,
    atomic_rename=False,
    listing=True,
    engine_visible_uri=True,
    evidence=(
        "GetObject honours HTTP Range requests; s3fs file handles are seekable",
        "ListObjectsV2 lists keys by prefix; 'directories' are emulated from the '/' "
        "delimiter and do not exist as objects",
        "S3 has no rename: a move is CopyObject followed by DeleteObject and is not atomic",
        f"s3a://bucket/key URIs are readable by Spark through {SPARK_CONNECTOR}",
    ),
)
"""Object-store semantics declared for every S3 storage instance."""

# Keyword options that belong to ``s3fs.S3FileSystem`` itself rather than to the
# botocore client. Anything else in ``**client_kwargs`` goes to ``create_client``.
_S3FS_OPTIONS = frozenset(
    {
        "cache_regions",
        "config_kwargs",
        "default_block_size",
        "default_cache_type",
        "default_fill_cache",
        "max_concurrency",
        "requester_pays",
        "s3_additional_kwargs",
        "version_aware",
    }
)

_BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]\Z")
_RESERVED_BUCKET_PREFIXES = ("xn--", "sthree-", "amzn-s3-demo-")
_RESERVED_BUCKET_SUFFIXES = ("-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3")


class _S3Storage(FsspecStorage):
    """S3 storage that rejects paths with engine-dependent traversal semantics."""

    def resolve(self, path: str | Location) -> Location:
        location = super().resolve(path)
        _validate_object_path(location.path)
        return location


def s3_storage(
    bucket: str,
    *,
    key: str | None = None,
    secret: str | None = None,
    token: str | None = None,
    region: str | None = None,
    endpoint_url: str | None = None,
    anon: bool = False,
    profile: str | None = None,
    allow_http: bool = False,
    root: str = "",
    name: str | None = None,
    **client_kwargs: Any,
) -> FsspecStorage:
    """Build storage over one S3 bucket (or an S3-compatible service such as MinIO).

    Options
    -------
    bucket
        The bucket name. Every location resolves to ``s3://<bucket>/<key>``.
    key, secret, token
        Static AWS credentials (access key id, secret access key, optional
        session token). ``key`` and ``secret`` must be given together; ``token``
        requires both. When omitted, ``s3fs``/botocore resolve credentials from
        the standard chain: ``AWS_ACCESS_KEY_ID``/``AWS_SECRET_ACCESS_KEY``/
        ``AWS_SESSION_TOKEN``, the shared credentials/config files, SSO, container
        or instance metadata.
    region
        The bucket region (``client_kwargs["region_name"]``). Optional for AWS
        when the credential chain or bucket lookup provides it; usually required
        for S3-compatible services.
    endpoint_url
        A non-AWS endpoint such as ``http://localhost:9000`` for MinIO
        (``client_kwargs["endpoint_url"]``).
    anon
        Use unsigned requests for public buckets. Cannot be combined with static
        credentials or a profile.
    profile
        A named profile from the shared AWS config file.
    allow_http
        Explicitly permit plain-HTTP transport to a custom ``http://`` endpoint.
        Static credentials are never sent over HTTP merely because an endpoint
        string uses that scheme. The effective opt-in is reported as
        ``aws_allow_http`` in :meth:`FsspecStorage.native_options`.
    root
        A key prefix inside the bucket; relative paths resolve underneath it.
    name
        The storage's registry name (default ``s3:<bucket>``).
    **client_kwargs
        Extra botocore ``create_client`` keywords (``verify``, ``config`` ...).
        The ``s3fs`` options ``requester_pays``, ``version_aware``,
        ``default_block_size``, ``default_cache_type``, ``default_fill_cache``,
        ``cache_regions``, ``max_concurrency``, ``config_kwargs`` and
        ``s3_additional_kwargs`` are routed to ``S3FileSystem`` instead.

    Credential handling
    -------------------
    Secrets are held by the ``s3fs`` filesystem and by a
    :class:`~invariantql.domain.credentials.SecretOptions` mapping. They never
    appear in ``repr``, diagnostics or logs: every provider exception is
    translated into :class:`~invariantql.domain.diagnostics.StorageError` with
    its message redacted and its cause dropped. ``native_options()`` exposes
    values passed explicitly, or standard environment credentials when the
    factory is otherwise left unconfigured, under the canonical keys
    ``aws_access_key_id``, ``aws_secret_access_key``, ``aws_session_token``,
    ``aws_region``, ``aws_endpoint_url``, ``aws_allow_http`` (``"true"``), and
    ``aws_anonymous`` (``"true"`` when ``anon=True``);
    credentials resolved from shared files, SSO, container metadata, or instance
    metadata are not read back, so an engine reading ``s3a://`` URIs relies on
    its own provider chain in those cases. Standard ``AWS_*`` environment
    credentials are registered with the redaction service before construction.

    Semantics
    ---------
    S3 is a flat key space: ``hierarchical_directories`` is ``False`` (prefixes
    are emulated from ``/``), ``atomic_rename`` is ``False`` (a move is copy
    then delete), ``range_reads`` and ``listing`` are ``True``. Reads see the
    strong read-after-write consistency S3 provides; S3-compatible services may
    differ. ``native_uri`` returns ``s3a://<bucket>/<key>``.

    Spark
    -----
    Spark reads ``s3a://`` URIs through ``org.apache.hadoop:hadoop-aws`` (with
    ``com.amazonaws:aws-java-sdk-bundle``) matching the cluster's Hadoop
    version. ``SparkEngine.apply_storage_credentials`` copies the canonical
    ``aws_*`` options into ``fs.s3a.*`` Hadoop settings; ``aws_allow_http``
    corresponds to ``fs.s3a.connection.ssl.enabled=false``.
    """

    _validate_bucket(bucket)
    _validate_optional_string("key", key)
    _validate_optional_string("secret", secret)
    _validate_optional_string("token", token)
    _validate_optional_string("region", region)
    _validate_optional_string("profile", profile)
    if not isinstance(anon, bool):
        raise ValueError("S3 anon must be a boolean")
    if (key is None) != (secret is None):
        raise ValueError("S3 'key' and 'secret' must be given together")
    if token is not None and key is None:
        raise ValueError("S3 'token' requires 'key' and 'secret'")
    if anon and (key is not None or profile is not None):
        raise ValueError("anon=True cannot be combined with static credentials or a profile")

    storage_name = name or f"{S3_SCHEME}:{bucket}"
    plain_http = _validate_endpoint(endpoint_url, allow_http=allow_http)
    root = _normalize_root(root)
    _validate_extra_options(client_kwargs)

    # Register the secrets with the redaction service *before* touching the
    # provider so that a construction error echoing them is scrubbed.
    environment: dict[str, str] = {}
    if key is None and profile is None and not anon:
        environment = _environment_credentials()
    for value in (key, secret, token, *environment.values()):
        if value is not None:
            register_secret(value)
    native = _native_options(
        storage_name,
        key=key or environment.get("aws_access_key_id"),
        secret=secret or environment.get("aws_secret_access_key"),
        token=token or environment.get("aws_session_token"),
        region=region or environment.get("aws_region"),
        endpoint_url=endpoint_url,
        allow_http=plain_http,
        anon=anon,
    )
    filesystem_kwargs = _filesystem_kwargs(
        key=key,
        secret=secret,
        token=token,
        region=region,
        endpoint_url=endpoint_url,
        anon=anon,
        profile=profile,
        allow_http=plain_http,
        client_kwargs=client_kwargs,
    )
    try:
        filesystem = s3fs.S3FileSystem(**filesystem_kwargs)
    except Exception as exc:  # provider error translated at the edge
        raise StorageError(
            f"cannot configure S3 storage for bucket {bucket!r}: {redact_exception(exc)}"
        ) from None

    return _S3Storage(
        filesystem,
        name=storage_name,
        scheme=S3_SCHEME,
        netloc=bucket,
        root=root,
        capabilities=S3_CAPABILITIES,
        native_scheme=S3_NATIVE_SCHEME,
        native_options=native,
        fs_path=_fs_path_for(bucket),
        from_fs_path=_from_fs_path_for(bucket),
    )


# -- helpers ------------------------------------------------------------------


def _validate_bucket(bucket: str) -> None:
    if not isinstance(bucket, str) or not _BUCKET_NAME.fullmatch(bucket):
        raise ValueError(f"invalid S3 bucket name: {bucket!r}")
    if ".." in bucket or ".-" in bucket or "-." in bucket:
        raise ValueError(f"invalid S3 bucket name: {bucket!r}")
    if bucket.startswith(_RESERVED_BUCKET_PREFIXES) or bucket.endswith(_RESERVED_BUCKET_SUFFIXES):
        raise ValueError(f"reserved S3 bucket name: {bucket!r}")
    try:
        ipaddress.ip_address(bucket)
    except ValueError:
        return
    raise ValueError(f"S3 bucket names cannot be IP addresses: {bucket!r}")


def _environment_credentials() -> dict[str, str]:
    """Read the standard environment subset that can be translated to Hadoop."""

    values: dict[str, str] = {}
    access_key = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_KEY")
    session_token = os.environ.get("AWS_SESSION_TOKEN") or os.environ.get("AWS_SECURITY_TOKEN")
    region = os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
    if access_key:
        values["aws_access_key_id"] = access_key
    if secret_key:
        values["aws_secret_access_key"] = secret_key
    if session_token:
        values["aws_session_token"] = session_token
    if region:
        values["aws_region"] = region
    return values


def _validate_optional_string(label: str, value: str | None) -> None:
    if value is not None and (not isinstance(value, str) or not value):
        raise ValueError(f"S3 {label} must be a non-empty string when supplied")


def _validate_endpoint(endpoint_url: str | None, *, allow_http: bool) -> bool:
    if not isinstance(allow_http, bool):
        raise ValueError("S3 allow_http must be a boolean")
    if endpoint_url is None:
        if allow_http:
            raise ValueError("allow_http=True requires an explicit custom endpoint_url")
        return False
    if (
        not isinstance(endpoint_url, str)
        or not endpoint_url
        or endpoint_url != endpoint_url.strip()
    ):
        raise ValueError("S3 endpoint_url must be a non-empty absolute HTTP(S) URL")
    parsed = urlsplit(endpoint_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("S3 endpoint_url must be an absolute HTTP(S) URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("S3 endpoint_url must not contain credentials")
    try:
        _ = parsed.port
    except ValueError:
        raise ValueError("S3 endpoint_url contains an invalid port") from None
    if parsed.query or parsed.fragment:
        raise ValueError("S3 endpoint_url must not contain a query string or fragment")
    if parsed.scheme == "http" and not allow_http:
        raise ValueError("plain-HTTP S3 endpoints require allow_http=True")
    return parsed.scheme == "http"


def _normalize_root(root: str) -> str:
    if not isinstance(root, str):
        raise ValueError("S3 root must be a string")
    _validate_object_path(root)
    return root.strip("/")


def _validate_object_path(path: str) -> None:
    if "\x00" in path or any(ord(character) < 32 for character in path):
        raise StorageError("S3 object paths cannot contain control characters")
    if any(part in {".", ".."} for part in path.split("/")):
        raise StorageError(
            "S3 object paths cannot contain '.' or '..' segments",
            code=DiagnosticCode.STORAGE_UNSUPPORTED_OPERATION,
        )


def _validate_extra_options(options: Mapping[str, Any]) -> None:
    if "use_ssl" in options:
        raise ValueError("pass allow_http=True instead of the s3fs use_ssl option")
    nested = options.get("client_kwargs")
    if nested is not None and not isinstance(nested, Mapping):
        raise ValueError("S3 client_kwargs must be a mapping")
    if isinstance(nested, Mapping):
        reserved = {"endpoint_url", "region_name"}.intersection(nested)
        if reserved:
            names = ", ".join(sorted(reserved))
            raise ValueError(f"pass S3 {names} through the named factory arguments")


def _native_options(
    storage_name: str,
    *,
    key: str | None,
    secret: str | None,
    token: str | None,
    region: str | None,
    endpoint_url: str | None,
    allow_http: bool,
    anon: bool,
) -> SecretOptions:
    """The canonical ``aws_*`` vocabulary; only explicitly set values are included."""

    values: dict[str, str] = {}
    if key is not None:
        values["aws_access_key_id"] = key
    if secret is not None:
        values["aws_secret_access_key"] = secret
    if token is not None:
        values["aws_session_token"] = token
    if region:
        values["aws_region"] = region
    if endpoint_url:
        values["aws_endpoint_url"] = endpoint_url
    if allow_http:
        values["aws_allow_http"] = "true"
    if anon:
        values["aws_anonymous"] = "true"
    return SecretOptions(
        values,
        ref=CredentialRef(storage_name),
        public_keys={
            "aws_region",
            "aws_endpoint_url",
            "aws_allow_http",
            "aws_anonymous",
        },
    )


def _filesystem_kwargs(
    *,
    key: str | None,
    secret: str | None,
    token: str | None,
    region: str | None,
    endpoint_url: str | None,
    anon: bool,
    profile: str | None,
    allow_http: bool,
    client_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    """Keyword arguments for ``s3fs.S3FileSystem``; only set options are passed."""

    extra = dict(client_kwargs)
    nested = extra.pop("client_kwargs", None)
    client: dict[str, Any] = dict(nested or {})
    kwargs: dict[str, Any] = {}
    for option, value in extra.items():
        if option in _S3FS_OPTIONS:
            kwargs[option] = value
        else:
            client[option] = value
    if region is not None:
        client["region_name"] = region
    if endpoint_url is not None:
        client["endpoint_url"] = endpoint_url

    if key is not None:
        kwargs["key"] = key
    if secret is not None:
        kwargs["secret"] = secret
    if token is not None:
        kwargs["token"] = token
    if anon:
        kwargs["anon"] = True
    if profile is not None:
        kwargs["profile"] = profile
    if allow_http:
        kwargs["use_ssl"] = False
    if client:
        kwargs["client_kwargs"] = client
    return kwargs


def _fs_path_for(bucket: str) -> Callable[[Location], str]:
    """``s3://bucket/key`` -> the ``bucket/key`` path s3fs expects."""

    def fs_path(location: Location) -> str:
        key = location.path.strip("/")
        return f"{bucket}/{key}" if key else bucket

    return fs_path


def _from_fs_path_for(bucket: str) -> Callable[[str], Location]:
    """An s3fs entry name (``bucket/key``, ``/bucket/key`` or ``s3://bucket/key``) -> location."""

    def from_fs_path(fs_path: str) -> Location:
        text = fs_path
        for prefix in (f"{S3_SCHEME}://", f"{S3_NATIVE_SCHEME}://"):
            if text.startswith(prefix):
                text = text[len(prefix) :]
                break
        text = text.lstrip("/")
        if text == bucket:
            text = ""
        elif text.startswith(bucket + "/"):
            text = text[len(bucket) + 1 :]
        return Location("/" + text, S3_SCHEME, bucket)

    return from_fs_path


__all__ = [
    "S3_CAPABILITIES",
    "S3_NATIVE_SCHEME",
    "S3_SCHEME",
    "SPARK_CONNECTOR",
    "s3_storage",
]
