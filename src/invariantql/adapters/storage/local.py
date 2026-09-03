"""Local filesystem storage adapter."""

from __future__ import annotations

import datetime as _dt
import os
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

from invariantql.domain.credentials import EMPTY_SECRETS, SecretOptions
from invariantql.domain.diagnostics import DiagnosticCode, StorageError
from invariantql.domain.location import Location
from invariantql.ports.storage import ObjectInfo, StorageCapabilities


class LocalStorage:
    """Files on the local machine, rooted at ``root`` (default: current directory)."""

    def __init__(
        self, root: str | os.PathLike[str] | None = None, *, name: str | None = None
    ) -> None:
        self._root = Path(root or ".").expanduser().resolve()
        self._name = name or f"local:{self._root}"

    @property
    def name(self) -> str:
        return self._name

    @property
    def root(self) -> Path:
        return self._root

    @property
    def capabilities(self) -> StorageCapabilities:
        return StorageCapabilities(
            range_reads=True,
            hierarchical_directories=True,
            atomic_rename=True,
            listing=True,
            engine_visible_uri=True,
            evidence=(
                "POSIX filesystem: seekable files, directories, atomic rename, file:// URIs",
            ),
        )

    def resolve(self, path: str | Location) -> Location:
        if isinstance(path, Location):
            if path.scheme and path.scheme != "file":
                raise StorageError(
                    f"location {path.uri} does not belong to local storage",
                    code=DiagnosticCode.STORAGE_UNSUPPORTED_OPERATION,
                )
            raw = path.path
        else:
            raw = path
        candidate = Path(raw).expanduser()
        if not candidate.is_absolute():
            candidate = self._root / candidate
        return Location(str(candidate.resolve()), "file", "")

    def _path(self, location: Location) -> Path:
        return Path(self.resolve(location).path)

    def open_read(self, location: Location) -> BinaryIO:
        path = self._path(location)
        try:
            return open(path, "rb")
        except FileNotFoundError:
            raise StorageError(
                f"object not found: {path}", code=DiagnosticCode.STORAGE_OBJECT_NOT_FOUND
            ) from None
        except OSError as exc:
            raise StorageError(f"cannot open {path}: {exc.strerror}") from None

    def info(self, location: Location) -> ObjectInfo:
        path = self._path(location)
        try:
            stat = path.stat()
        except FileNotFoundError:
            raise StorageError(
                f"object not found: {path}", code=DiagnosticCode.STORAGE_OBJECT_NOT_FOUND
            ) from None
        is_dir = path.is_dir()
        return ObjectInfo(
            Location(str(path), "file"),
            None if is_dir else stat.st_size,
            _dt.datetime.fromtimestamp(stat.st_mtime, tz=_dt.timezone.utc),
            is_dir,
        )

    def list(self, location: Location, *, recursive: bool = False) -> Iterator[ObjectInfo]:
        base = self._path(location)
        if not base.exists():
            raise StorageError(
                f"object not found: {base}", code=DiagnosticCode.STORAGE_OBJECT_NOT_FOUND
            )
        if base.is_file():
            yield self.info(location)
            return
        entries = base.rglob("*") if recursive else base.iterdir()
        for entry in sorted(entries):
            yield self.info(Location(str(entry), "file"))

    def exists(self, location: Location) -> bool:
        return self._path(location).exists()

    def native_uri(self, location: Location) -> str | None:
        return self._path(location).as_uri()

    def native_options(self) -> SecretOptions:
        return EMPTY_SECRETS

    def __repr__(self) -> str:
        return f"LocalStorage(root={str(self._root)!r})"


__all__ = ["LocalStorage"]
