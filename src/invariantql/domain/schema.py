"""Schema and field value objects."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from invariantql.domain.types import DataType, type_from_dict


@dataclass(frozen=True, slots=True)
class Field:
    name: str
    data_type: DataType
    nullable: bool = True

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("field name must not be empty")

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "type": self.data_type.to_dict(), "nullable": self.nullable}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Field:
        return cls(data["name"], type_from_dict(data["type"]), bool(data.get("nullable", True)))


@dataclass(frozen=True, slots=True)
class Schema:
    """An ordered collection of uniquely named fields."""

    fields: tuple[Field, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "fields", tuple(self.fields))
        seen: set[str] = set()
        for field in self.fields:
            if field.name in seen:
                raise ValueError(f"duplicate field name: {field.name!r}")
            seen.add(field.name)

    @classmethod
    def of(cls, *pairs: tuple[str, DataType]) -> Schema:
        return cls(tuple(Field(name, data_type) for name, data_type in pairs))

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(f.name for f in self.fields)

    def __iter__(self) -> Iterator[Field]:
        return iter(self.fields)

    def __len__(self) -> int:
        return len(self.fields)

    def __contains__(self, name: object) -> bool:
        return any(f.name == name for f in self.fields)

    def field(self, name: str) -> Field:
        for f in self.fields:
            if f.name == name:
                return f
        raise KeyError(name)

    def resolve(self, name: str) -> Field | None:
        """Resolve a column name exactly, then case-insensitively when unambiguous."""

        for f in self.fields:
            if f.name == name:
                return f
        lowered = name.lower()
        matches = [f for f in self.fields if f.name.lower() == lowered]
        if len(matches) == 1:
            return matches[0]
        return None

    def select(self, names: Iterable[str]) -> Schema:
        return Schema(tuple(self.field(n) for n in names))

    def to_dict(self) -> dict[str, Any]:
        return {"fields": [f.to_dict() for f in self.fields]}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Schema:
        return cls(tuple(Field.from_dict(f) for f in data.get("fields", [])))

    def __str__(self) -> str:
        return ", ".join(f"{f.name}: {f.data_type}" for f in self.fields)


__all__ = ["Field", "Schema"]
