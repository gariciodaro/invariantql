"""SFTP storage: fsspec's ``SFTPFileSystem`` (paramiko) behind the ``Storage`` port.

Build it with :func:`sftp_storage`. The adapter reads objects from an SSH/SFTP
server. No execution engine has an ``sftp://`` reader, so the storage exposes
no engine-visible URI: DuckDB streams the bytes through the storage bridge and
the planner reports ``STAGING_REQUIRED`` for Spark (ADR-0004, FF-14).
"""

from __future__ import annotations

import base64
import binascii
import contextlib
import io
import ipaddress
import os
import posixpath
from typing import Any

import paramiko
from fsspec.implementations.sftp import SFTPFileSystem

from invariantql.adapters.storage.fsspec_storage import FsspecStorage
from invariantql.domain.credentials import EMPTY_SECRETS
from invariantql.domain.diagnostics import DiagnosticCode, StorageError
from invariantql.domain.location import Location
from invariantql.domain.redaction import redact_exception, register_secret
from invariantql.ports.storage import StorageCapabilities

SCHEME = "sftp"

HOST_KEY_POLICIES: dict[str, type[paramiko.MissingHostKeyPolicy]] = {
    "auto-add": paramiko.AutoAddPolicy,
    "reject": paramiko.RejectPolicy,
    "warn": paramiko.WarningPolicy,
}

CAPABILITIES = StorageCapabilities(
    range_reads=True,
    hierarchical_directories=True,
    atomic_rename=False,
    listing=True,
    engine_visible_uri=False,
    evidence=(
        "range reads: paramiko SFTPFile is seekable (SSH_FXP_READ carries a byte offset)",
        "hierarchical directories: SFTP lists the server's real directory tree (SSH_FXP_READDIR)",
        "no atomic rename: SFTPv3 RENAME fails on an existing target and fsspec relies on the "
        "posix-rename@openssh.com extension, which not every server implements",
        "no engine-visible URI: neither Spark nor DuckDB has an sftp:// reader; bytes stream "
        "through the storage port or are staged explicitly",
    ),
)

_PKCS8_HEADERS = ("BEGIN PRIVATE KEY", "BEGIN ENCRYPTED PRIVATE KEY")
_OPENSSH_KEY_MARKERS: tuple[tuple[bytes, type[paramiko.PKey]], ...] = (
    (b"ssh-ed25519", paramiko.Ed25519Key),
    (b"ecdsa-sha2-", paramiko.ECDSAKey),
    (b"ssh-rsa", paramiko.RSAKey),
)
_ALL_KEY_CLASSES: tuple[type[paramiko.PKey], ...] = (
    paramiko.Ed25519Key,
    paramiko.ECDSAKey,
    paramiko.RSAKey,
)


def missing_host_key_policy(name: str) -> paramiko.MissingHostKeyPolicy:
    """Resolve a policy name (``auto-add``, ``reject``, ``warn``) to a paramiko policy."""

    try:
        return HOST_KEY_POLICIES[name]()
    except KeyError:
        raise ValueError(
            f"unknown host_key_policy {name!r}; choose one of {', '.join(HOST_KEY_POLICIES)}"
        ) from None


class SftpFileSystem(SFTPFileSystem):
    """fsspec's SFTP filesystem with host-key verification.

    Stock ``fsspec.implementations.sftp.SFTPFileSystem`` creates a bare
    ``paramiko.SSHClient`` with ``AutoAddPolicy`` and never loads a known-hosts
    file: every server key is accepted and a changed key is never noticed.
    This subclass loads ``~/.ssh/known_hosts`` (paramiko's
    ``load_system_host_keys``) plus an optional extra ``known_hosts`` file
    before connecting. A server key that contradicts a known entry always
    fails the connection (``BadHostKeyException``); ``host_key_policy``
    decides what happens to hosts that are still unknown.

    Every other keyword argument goes to ``paramiko.SSHClient.connect``.
    """

    def __init__(
        self,
        host: str,
        *,
        known_hosts: str | os.PathLike[str] | None = None,
        host_key_policy: str = "reject",
        **ssh_kwargs: Any,
    ) -> None:
        self._known_hosts = None if known_hosts is None else os.fspath(known_hosts)
        self._missing_host_key_policy = missing_host_key_policy(host_key_policy)
        super().__init__(host, **ssh_kwargs)

    def _connect(self) -> None:
        client = paramiko.SSHClient()
        try:
            client.load_system_host_keys()
            if self._known_hosts is not None:
                client.load_host_keys(self._known_hosts)
            client.set_missing_host_key_policy(self._missing_host_key_policy)
            client.connect(self.host, **self.ssh_kwargs)
            ftp = client.open_sftp()
        except BaseException:
            # A successful SSH handshake followed by an SFTP-channel failure
            # still owns a live socket. Close it before construction unwinds.
            with contextlib.suppress(Exception):
                client.close()
            raise
        self.client = client
        self.ftp = ftp

    def close(self) -> None:
        """Close the SFTP channel and its owning SSH transport, idempotently."""

        ftp = getattr(self, "ftp", None)
        client = getattr(self, "client", None)
        self.ftp = None
        self.client = None
        if ftp is not None:
            with contextlib.suppress(Exception):
                ftp.close()
        if client is not None:
            with contextlib.suppress(Exception):
                client.close()


class _SftpStorage(FsspecStorage):
    """An SFTP storage whose configured root is a lexical containment boundary."""

    def resolve(self, path: str | Location) -> Location:
        location = super().resolve(path)
        if "\x00" in location.path:
            raise StorageError("SFTP paths cannot contain NUL bytes")
        normalized = posixpath.normpath("/" + location.path.lstrip("/"))
        root = "/" + self._root if self._root else "/"
        if root != "/" and normalized != root and not normalized.startswith(root + "/"):
            raise StorageError(
                f"location {location.uri} escapes the configured SFTP root",
                code=DiagnosticCode.STORAGE_UNSUPPORTED_OPERATION,
            )
        return Location(normalized, location.scheme, location.netloc)

    def close(self) -> None:
        close = getattr(self.fs, "close", None)
        if close is not None:
            close()

    def __enter__(self) -> _SftpStorage:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


def private_key_from_pem(pem: str, passphrase: str | None = None) -> paramiko.PKey:
    """Turn private-key text into a paramiko key (Ed25519, ECDSA or RSA).

    Accepts the OpenSSH container written by ``ssh-keygen`` (``BEGIN OPENSSH
    PRIVATE KEY``) and the traditional PEM encodings (``BEGIN RSA PRIVATE
    KEY``, ``BEGIN EC PRIVATE KEY``), encrypted or not. PKCS#8 (``BEGIN
    PRIVATE KEY``) is not understood by paramiko; convert such a key with
    ``ssh-keygen -p -f <file> -m RSA`` (or ``-m PEM``) first. The text is
    registered with the redaction service so that no error can echo it.
    """

    if not isinstance(pem, str):
        raise ValueError("private_key must be PEM text")
    if passphrase is not None and (not isinstance(passphrase, str) or not passphrase):
        raise ValueError("passphrase, when supplied, must be a non-empty string")
    text = pem.strip()
    register_secret(pem)
    register_secret(text)
    if passphrase is not None:
        register_secret(passphrase)
    # Provider exceptions sometimes include only one base64 line rather than
    # the whole PEM. Register the payload fragments as well as the container.
    for line in text.splitlines()[1:-1]:
        if line:
            register_secret(line)
    if not text:
        raise StorageError("private_key is empty")
    header = text.splitlines()[0]
    if any(marker in header for marker in _PKCS8_HEADERS):
        raise StorageError(
            "private key is PKCS#8 encoded, which paramiko cannot load; convert it to the "
            "OpenSSH or traditional PEM format first (ssh-keygen -p -f <file> -m RSA)"
        )
    failures: list[BaseException] = []
    for key_class in _candidate_key_classes(text):
        try:
            return key_class.from_private_key(io.StringIO(text), password=passphrase)
        except paramiko.PasswordRequiredException:
            raise StorageError("private key is encrypted; pass its passphrase") from None
        except (paramiko.SSHException, ValueError) as exc:
            failures.append(exc)
    detail = redact_exception(failures[0]) if failures else "no key class accepted the material"
    hint = " (check the passphrase)" if passphrase is not None else ""
    raise StorageError(
        f"private key could not be loaded as Ed25519, ECDSA or RSA{hint}: {detail}"
    ) from None


def _candidate_key_classes(text: str) -> tuple[type[paramiko.PKey], ...]:
    """Pick the paramiko key class from the PEM header so that errors name the right type."""

    lines = text.splitlines()
    header = lines[0]
    if "RSA PRIVATE KEY" in header:
        return (paramiko.RSAKey,)
    if "EC PRIVATE KEY" in header:
        return (paramiko.ECDSAKey,)
    if "OPENSSH PRIVATE KEY" in header:
        # The OpenSSH container names the key type in its (unencrypted) public part.
        try:
            blob = base64.b64decode("".join(lines[1:-1]))
        except (binascii.Error, ValueError):
            blob = b""
        for marker, key_class in _OPENSSH_KEY_MARKERS:
            if marker in blob:
                return (key_class,)
    return _ALL_KEY_CLASSES


def _remote_path(location: Location) -> str:
    return location.path or "/"


def _host_and_netloc(host: str, port: int) -> tuple[str, str]:
    if not isinstance(host, str) or not host or host != host.strip():
        raise ValueError("host must be a non-empty hostname or IP address without whitespace")
    if any(character in host for character in "/\\?#@") or any(c.isspace() for c in host):
        raise ValueError(f"invalid SFTP host: {host!r}")
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise ValueError(f"SFTP port must be an integer from 1 to 65535, got {port!r}")

    connection_host = host
    if host.startswith("[") or host.endswith("]"):
        if not (host.startswith("[") and host.endswith("]")):
            raise ValueError(f"invalid bracketed IPv6 SFTP host: {host!r}")
        connection_host = host[1:-1]
        try:
            address = ipaddress.ip_address(connection_host)
        except ValueError:
            raise ValueError(f"invalid bracketed IPv6 SFTP host: {host!r}") from None
        if address.version != 6:
            raise ValueError(f"brackets are only valid around an IPv6 SFTP host: {host!r}")
    elif ":" in host:
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            raise ValueError(f"invalid SFTP host: {host!r}") from None
        if address.version != 6:  # pragma: no cover - ':' cannot occur in IPv4
            raise ValueError(f"invalid SFTP host: {host!r}")

    rendered_host = f"[{connection_host}]" if ":" in connection_host else connection_host
    return connection_host, f"{rendered_host}:{port}"


def _normalize_root(root: str) -> str:
    if not isinstance(root, str) or not root.startswith("/"):
        raise ValueError(f"root must be an absolute remote path, got {root!r}")
    if "\x00" in root:
        raise ValueError("root cannot contain NUL bytes")
    normalized = posixpath.normpath(root)
    if normalized != root.rstrip("/") and not (root == "/" and normalized == "/"):
        raise ValueError(f"root must not contain '.' or '..' path segments, got {root!r}")
    return normalized


def sftp_storage(
    host: str,
    *,
    port: int = 22,
    username: str | None = None,
    password: str | None = None,
    key_filename: str | os.PathLike[str] | None = None,
    private_key: str | None = None,
    passphrase: str | None = None,
    timeout: float | None = 30,
    root: str = "/",
    name: str | None = None,
    known_hosts: str | os.PathLike[str] | None = None,
    host_key_policy: str = "reject",
    **paramiko_kwargs: Any,
) -> FsspecStorage:
    """Storage over an SSH/SFTP server (fsspec ``SFTPFileSystem``, paramiko).

    The connection is opened when this factory returns; a failure surfaces as
    ``StorageError`` with a redacted provider message.

    **Options**

    - ``host`` / ``port``: the server; ``netloc`` of every location is ``host:port``.
    - ``username``: SSH user (paramiko falls back to the local user name).
    - ``password``: password authentication; also used to decrypt ``key_filename``
      when no ``passphrase`` is given (paramiko behaviour).
    - ``key_filename``: path(s) to a private-key file, handed to paramiko as is.
    - ``private_key``: private-key *text* (OpenSSH or traditional PEM; Ed25519,
      ECDSA or RSA); it is parsed in memory with :func:`private_key_from_pem`
      and never written to disk. ``passphrase`` decrypts it.
    - ``timeout``: TCP connect timeout in seconds (paramiko ``timeout``).
    - ``root``: absolute remote directory every path is resolved under
      (default ``/``). Paths are jailed under ``root``: ``resolve("a/b.csv")``
      and ``resolve("/a/b.csv")`` both give ``sftp://host:port<root>/a/b.csv``.
      Paths relative to the login home (``~``) are not supported; use the
      absolute path the server reports. This is lexical containment, not a
      server-side sandbox: a server symlink below the root can still target a
      path elsewhere.
    - ``name``: storage name shown in plans and explain output
      (default ``sftp:[user@]host:port<root>``).
    - ``known_hosts`` / ``host_key_policy``: see *Host keys* below.
    - ``**paramiko_kwargs``: forwarded to ``paramiko.SSHClient.connect``
      (``allow_agent``, ``look_for_keys``, ``banner_timeout``,
      ``disabled_algorithms``, ...). ``pkey`` is rejected: pass ``private_key``
      or ``key_filename`` instead.

    **Credentials.** paramiko tries, in order, ``private_key``, ``key_filename``,
    the SSH agent, ``~/.ssh/id_*`` (unless ``look_for_keys=False``) and finally
    ``password``. Secrets are held only by the paramiko client; the password,
    passphrase and key text are registered with the redaction service so that
    provider errors echoing them are scrubbed, and none of them appear in
    ``repr``, the storage name or diagnostics. ``native_options()`` is empty:
    no external library can read SFTP locations on the engine's behalf.

    **Host keys.** Stock fsspec accepts every server key (``AutoAddPolicy`` on
    a client that loads no known-hosts file). This adapter loads
    ``~/.ssh/known_hosts`` and, when given, the extra ``known_hosts`` file
    (OpenSSH format, ``[host]:port`` entries for non-default ports; hashed
    entries are fine). A key that contradicts a known entry always fails the
    connection. For hosts that are still unknown ``host_key_policy`` applies:
    ``"reject"`` (the secure default), ``"auto-add"`` (explicitly accept an
    unknown key for this in-memory client) or ``"warn"`` (log and continue).
    ``auto-add`` is not persistent trust-on-first-use because this adapter does
    not write the accepted key back to a known-hosts file.

    **Semantics.** Range reads are real (paramiko ``SFTPFile.seek``), listing
    is hierarchical, rename is *not* atomic on every server, and there is no
    engine-visible URI: Spark has no SFTP connector (no jar makes ``sftp://``
    readable), so compiling for Spark yields ``STAGING_REQUIRED`` and the
    explicit staging use case must copy the data first. DuckDB reads through
    the storage bridge in-process. One SSH session backs the storage object;
    paramiko's SFTP channel is not safe for concurrent use from several
    threads, so create one storage per thread. The fsspec instance cache is
    bypassed: every call opens its own connection.
    """

    connection_host, netloc = _host_and_netloc(host, port)
    normalized_root = _normalize_root(root)
    for label, value in (
        ("username", username),
        ("password", password),
        ("passphrase", passphrase),
    ):
        if value is not None and (not isinstance(value, str) or not value):
            raise ValueError(f"{label}, when supplied, must be a non-empty string")
    if username is not None and any(character.isspace() for character in username):
        raise ValueError("username must not contain whitespace")
    if timeout is not None and (
        not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0
    ):
        raise ValueError("timeout, when supplied, must be a positive number")
    if "pkey" in paramiko_kwargs:
        raise ValueError("pass the key as private_key (PEM text) or key_filename, not pkey")
    missing_host_key_policy(host_key_policy)  # validate before touching the network
    for secret in (password, passphrase):
        if secret:
            register_secret(secret)
    pkey = private_key_from_pem(private_key, passphrase) if private_key else None

    ssh_kwargs: dict[str, Any] = {"port": int(port)}
    optional: tuple[tuple[str, Any], ...] = (
        ("username", username),
        ("password", password),
        ("pkey", pkey),
        ("key_filename", None if key_filename is None else os.fspath(key_filename)),
        ("passphrase", passphrase),
        ("timeout", timeout),
    )
    for key, value in optional:
        if value is not None:
            ssh_kwargs[key] = value
    ssh_kwargs.update(paramiko_kwargs)

    try:
        filesystem = SftpFileSystem(
            connection_host,
            known_hosts=known_hosts,
            host_key_policy=host_key_policy,
            skip_instance_cache=True,
            **ssh_kwargs,
        )
    except Exception as exc:  # provider error translated at the edge
        raise StorageError(
            f"cannot connect to {SCHEME}://{netloc}: {redact_exception(exc)}"
        ) from None

    user_prefix = f"{username}@" if username else ""
    return _SftpStorage(
        filesystem,
        name=name or f"{SCHEME}:{user_prefix}{netloc}{normalized_root}",
        scheme=SCHEME,
        netloc=netloc,
        root=normalized_root,
        capabilities=CAPABILITIES,
        native_scheme=None,
        native_options=EMPTY_SECRETS,
        fs_path=_remote_path,
        from_fs_path=lambda path: Location("/" + path.lstrip("/"), SCHEME, netloc),
    )


__all__ = [
    "CAPABILITIES",
    "HOST_KEY_POLICIES",
    "SCHEME",
    "SftpFileSystem",
    "missing_host_key_policy",
    "private_key_from_pem",
    "sftp_storage",
]
