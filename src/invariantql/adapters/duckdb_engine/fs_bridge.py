"""Expose ``Storage`` ports to DuckDB through a minimal fsspec filesystem.

DuckDB can read from any registered fsspec filesystem. This bridge translates
``iql://<key>/<sub path>`` into ``Storage.open_read`` calls so that no storage
adapter has to know about DuckDB and DuckDB never sees provider types.
"""

from __future__ import annotations

import fnmatch
import hashlib
from typing import Any

from fsspec.spec import AbstractFileSystem

from invariantql.domain.location import Location
from invariantql.ports.storage import Storage

PROTOCOL = "iql"


class StorageBridge(AbstractFileSystem):
    protocol = PROTOCOL
    cachable = False

    def __init__(self) -> None:
        super().__init__()
        self._mounts: dict[str, tuple[Storage, Location]] = {}

    # -- mounting -----------------------------------------------------------

    def mount(self, storage: Storage, location: Location) -> str:
        """Register a storage location; returns the DuckDB-readable URI."""

        key = hashlib.sha1(f"{storage.name}\0{location.uri}".encode()).hexdigest()[:16]
        self._mounts[key] = (storage, location)
        return f"{PROTOCOL}://{key}"

    def unmount(self, uri: str) -> None:
        key = self._strip_protocol(uri).split("/", 1)[0]
        self._mounts.pop(key, None)

    def _resolve(self, path: str) -> tuple[Storage, Location]:
        path = self._strip_protocol(path)
        key, _, sub = path.partition("/")
        try:
            storage, base = self._mounts[key]
        except KeyError:
            raise FileNotFoundError(path) from None
        return storage, (base.join(sub) if sub else base)

    # -- fsspec interface -----------------------------------------------------

    @classmethod
    def _strip_protocol(cls, path: Any) -> str:
        text = str(path)
        if text.startswith(PROTOCOL + "://"):
            text = text[len(PROTOCOL) + 3 :]
        return text.rstrip("/")

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        if "r" not in mode or "w" in mode or "a" in mode:
            raise OSError("InvariantQL storage bridge is read-only")
        storage, location = self._resolve(path)
        return storage.open_read(location)

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        storage, location = self._resolve(path)
        meta = storage.info(location)
        return {
            "name": self._strip_protocol(path),
            "size": meta.size or 0,
            "type": "directory" if meta.is_directory else "file",
            "mtime": None if meta.modified_at is None else meta.modified_at.timestamp(),
        }

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[Any]:
        stripped = self._strip_protocol(path)
        storage, location = self._resolve(stripped)
        key = stripped.split("/", 1)[0]
        _, base = self._mounts[key]
        out = []
        for entry in storage.list(location):
            rel = (
                entry.location.path[len(base.path) :].lstrip("/")
                if entry.location.path.startswith(base.path)
                else entry.location.name
            )
            name = f"{key}/{rel}" if rel else key
            out.append(
                {
                    "name": name,
                    "size": entry.size or 0,
                    "type": "directory" if entry.is_directory else "file",
                    "mtime": None if entry.modified_at is None else entry.modified_at.timestamp(),
                }
            )
        return out if detail else [e["name"] for e in out]

    def glob(self, path: str, maxdepth: int | None = None, **kwargs: Any) -> list[str]:
        stripped = self._strip_protocol(path)
        key, _, pattern = stripped.partition("/")
        if not any(ch in pattern for ch in "*?["):
            return [stripped] if self.exists(stripped) else []
        storage, base = self._mounts[key]
        base_len = len(base.path)
        matches = []
        for entry in storage.list(base, recursive=True):
            if entry.is_directory:
                continue
            rel = entry.location.path[base_len:].lstrip("/")
            if fnmatch.fnmatchcase(rel, pattern) or fnmatch.fnmatchcase(
                rel, pattern.replace("**/", "")
            ):
                matches.append(f"{key}/{rel}")
        return sorted(matches)

    def exists(self, path: str, **kwargs: Any) -> bool:
        try:
            storage, location = self._resolve(path)
        except FileNotFoundError:
            return False
        return storage.exists(location)

    def modified(self, path: str) -> Any:
        import datetime as _dt

        mtime = self.info(path).get("mtime")
        if mtime is None:
            return _dt.datetime.fromtimestamp(0, tz=_dt.timezone.utc)
        return _dt.datetime.fromtimestamp(mtime, tz=_dt.timezone.utc)

    def created(self, path: str) -> Any:
        return self.modified(path)

    def isdir(self, path: str) -> bool:
        try:
            return self.info(path)["type"] == "directory"
        except FileNotFoundError:
            return False

    def isfile(self, path: str) -> bool:
        try:
            return self.info(path)["type"] == "file"
        except FileNotFoundError:
            return False


__all__ = ["PROTOCOL", "StorageBridge"]
