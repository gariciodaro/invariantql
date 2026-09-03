"""Opaque credential references.

The domain never sees secret values. Adapters hold provider credentials
privately and expose only a ``CredentialRef`` label; ``SecretOptions`` wraps
provider option mappings so that they cannot leak through ``repr``, logs,
plans, fingerprints, or explain output.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from invariantql.domain.redaction import register_secret

REDACTED = "***"


@dataclass(frozen=True, slots=True)
class CredentialRef:
    """A label naming a credential; safe to appear anywhere."""

    name: str

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("credential reference name must not be empty")

    def __str__(self) -> str:
        return f"credential:{self.name}"


class SecretOptions(Mapping[str, Any]):
    """A provider option mapping whose values are treated as secrets.

    Only adapters call :meth:`reveal`. Values are registered with the
    redaction service so that provider error messages echoing them are scrubbed.
    """

    __slots__ = ("_ref", "_values")

    def __init__(self, values: Mapping[str, Any] | None = None, ref: CredentialRef | None = None):
        self._values: dict[str, Any] = dict(values or {})
        self._ref = ref
        for value in self._values.values():
            if isinstance(value, str) and len(value) >= 8:
                register_secret(value)

    @property
    def ref(self) -> CredentialRef | None:
        return self._ref

    def reveal(self) -> dict[str, Any]:
        return dict(self._values)

    # Mapping protocol: keys are visible, values are redacted.
    def __getitem__(self, key: str) -> Any:
        if key in self._values:
            return REDACTED
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        label = self._ref.name if self._ref else "anonymous"
        return f"SecretOptions(<{label}: {len(self._values)} redacted values>)"

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        return self is other

    def __hash__(self) -> int:
        return id(self)

    def __reduce__(self) -> Any:  # never pickle secrets by accident
        raise TypeError("SecretOptions cannot be serialised")


EMPTY_SECRETS = SecretOptions()

__all__ = ["EMPTY_SECRETS", "REDACTED", "CredentialRef", "SecretOptions"]
