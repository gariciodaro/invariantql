"""Generic storage adapter over an fsspec filesystem.

Provider-specific factories (Azure Blob / ADLS, S3, SFTP) configure this
class with an fsspec filesystem, a path mapping, honest capabilities and the
secrets other libraries need to read the same locations themselves.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Callable, Iterator
from typing import Any, BinaryIO

from invariantql.domain.credentials import EMPTY_SECRETS, SecretOptions
from invariantql.domain.diagnostics import DiagnosticCode, StorageError
from invariantql.domain.location import Location
from invariantql.domain.redaction import redact_exception
from invariantql.ports.storage import ObjectInfo, StorageCapabilities


class FsspecStorage:
    def __init__(
        self,
        filesystem: Any,
        *,
        name: str,
        scheme: str,
        netloc: str = "",
        root: str = "",
        capabilities: StorageCapabilities,
        native_scheme: str | None = None,
        native_options: SecretOptions = EMPTY_SECRETS,
        fs_path: Callable[[Location], str] | None = None,
        from_fs_path: Callable[[str], Location] | None = None,
    ) -> None:
        self.fs = filesystem
        self._name = name
        self._scheme = scheme
        self._netloc = netloc
        self._root = root.strip("/")
        self._capabilities = capabilities
        self._native_scheme = native_scheme
        self._native_options = native_options
        self._fs_path = fs_path or self._default_fs_path
        self._from_fs_path = from_fs_path or self._default_from_fs_path

    # -- identity -----------------------------------------------------------

    @property
    def name(self) -> str:
        return self._name

    @property
    def scheme(self) -> str:
        return self._scheme

    @property
    def netloc(self) -> str:
        return self._netloc

    @property
    def capabilities(self) -> StorageCapabilities:
        return self._capabilities

    # -- paths ----------------------------------------------------------------

    def resolve(self, path: str | Location) -> Location:
        if isinstance(path, Location):
            if path.scheme:
                if path.scheme != self._scheme or (path.netloc and path.netloc != self._netloc):
                    raise StorageError(
                        f"location {path.uri} does not belong to storage {self._name!r}",
                        code=DiagnosticCode.STORAGE_UNSUPPORTED_OPERATION,
                    )
                return Location(path.path, self._scheme, self._netloc)
            raw = path.path
        else:
            raw = path
        raw = raw.lstrip("/")
        full = "/" + "/".join(p for p in (self._root, raw) if p)
        return Location(full, self._scheme, self._netloc)

    def _default_fs_path(self, location: Location) -> str:
        path = location.path.lstrip("/")
        return f"{self._netloc}/{path}" if self._netloc else path

    def _default_from_fs_path(self, fs_path: str) -> Location:
        stripped = fs_path
        if self._netloc and stripped.startswith(self._netloc + "/"):
            stripped = stripped[len(self._netloc) + 1 :]
        elif self._netloc and stripped == self._netloc:
            stripped = ""
        return Location("/" + stripped.lstrip("/"), self._scheme, self._netloc)

    def fs_path(self, location: Location) -> str:
        return self._fs_path(self.resolve(location))

    # -- operations ------------------------------------------------------------

    def open_read(self, location: Location) -> BinaryIO:
        path = self.fs_path(location)
        try:
            return self.fs.open(path, "rb")
        except FileNotFoundError:
            raise StorageError(
                f"object not found: {self.resolve(location).uri}",
                code=DiagnosticCode.STORAGE_OBJECT_NOT_FOUND,
            ) from None
        except Exception as exc:
            raise StorageError(
                f"cannot open {self.resolve(location).uri}: {redact_exception(exc)}"
            ) from None

    def info(self, location: Location) -> ObjectInfo:
        path = self.fs_path(location)
        try:
            raw = self.fs.info(path)
        except FileNotFoundError:
            raise StorageError(
                f"object not found: {self.resolve(location).uri}",
                code=DiagnosticCode.STORAGE_OBJECT_NOT_FOUND,
            ) from None
        except Exception as exc:
            raise StorageError(
                f"cannot stat {self.resolve(location).uri}: {redact_exception(exc)}"
            ) from None
        return self._object_info(raw)

    def list(self, location: Location, *, recursive: bool = False) -> Iterator[ObjectInfo]:
        path = self.fs_path(location)
        try:
            if recursive:
                entries = self.fs.find(path, detail=True)
                raw_entries = list(entries.values()) if isinstance(entries, dict) else list(entries)
            else:
                raw_entries = self.fs.ls(path, detail=True)
        except FileNotFoundError:
            raise StorageError(
                f"object not found: {self.resolve(location).uri}",
                code=DiagnosticCode.STORAGE_OBJECT_NOT_FOUND,
            ) from None
        except Exception as exc:
            raise StorageError(
                f"cannot list {self.resolve(location).uri}: {redact_exception(exc)}"
            ) from None
        for raw in sorted(raw_entries, key=lambda e: str(e.get("name", ""))):
            yield self._object_info(raw)

    def exists(self, location: Location) -> bool:
        try:
            return bool(self.fs.exists(self.fs_path(location)))
        except Exception as exc:
            raise StorageError(
                f"cannot check {self.resolve(location).uri}: {redact_exception(exc)}"
            ) from None

    def native_uri(self, location: Location) -> str | None:
        if self._native_scheme is None:
            return None
        resolved = self.resolve(location)
        return f"{self._native_scheme}://{resolved.netloc}{resolved.path}"

    def native_options(self) -> SecretOptions:
        return self._native_options

    # -- helpers ----------------------------------------------------------------

    def _object_info(self, raw: dict[str, Any]) -> ObjectInfo:
        name = str(raw.get("name", ""))
        is_dir = raw.get("type") == "directory"
        size = raw.get("size")
        modified = raw.get("mtime") or raw.get("LastModified") or raw.get("last_modified")
        return ObjectInfo(
            self._from_fs_path(name),
            None if is_dir or size is None else int(size),
            _to_datetime(modified),
            is_dir,
        )

    def __repr__(self) -> str:
        return (
            f"FsspecStorage(name={self._name!r}, scheme={self._scheme!r}, netloc={self._netloc!r})"
        )


def _to_datetime(value: Any) -> _dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=_dt.timezone.utc)
    if isinstance(value, (int, float)):
        return _dt.datetime.fromtimestamp(value, tz=_dt.timezone.utc)
    if isinstance(value, str):
        try:
            parsed = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=_dt.timezone.utc)
        except ValueError:
            return None
    return None


__all__ = ["FsspecStorage"]
