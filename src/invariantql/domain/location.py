"""Storage locations.

A ``Location`` identifies an object or prefix inside a storage adapter. It is
a plain value: no credentials, no provider client. Paths keep their meaning
exactly as written; there is no implicit leading-slash stripping.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit


@dataclass(frozen=True, slots=True)
class Location:
    path: str
    scheme: str = ""
    netloc: str = ""

    def __post_init__(self) -> None:
        if self.scheme and not self.scheme.isalnum():
            raise ValueError(f"invalid scheme: {self.scheme!r}")

    @classmethod
    def parse(cls, text: str) -> Location:
        """Parse ``scheme://netloc/path`` or a bare path."""

        if "://" not in text:
            return cls(text)
        parts = urlsplit(text)
        return cls(parts.path, parts.scheme, parts.netloc)

    @property
    def is_absolute(self) -> bool:
        return bool(self.scheme)

    @property
    def uri(self) -> str:
        if not self.scheme:
            return self.path
        return f"{self.scheme}://{self.netloc}{self.path}"

    @property
    def name(self) -> str:
        return self.path.rstrip("/").rsplit("/", 1)[-1]

    @property
    def suffix(self) -> str:
        name = self.name
        return name[name.rfind(".") :].lower() if "." in name else ""

    def join(self, *segments: str) -> Location:
        base = self.path
        for segment in segments:
            if not segment:
                continue
            base = base.rstrip("/") + "/" + segment.lstrip("/") if base else segment
        return Location(base, self.scheme, self.netloc)

    def to_dict(self) -> dict[str, Any]:
        return {"uri": self.uri}

    def __str__(self) -> str:
        return self.uri


__all__ = ["Location"]
