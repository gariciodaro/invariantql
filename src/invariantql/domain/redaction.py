"""Credential redaction for errors, logs, and diagnostics (FF-12).

Two mechanisms cooperate: pattern-based scrubbing of common secret carriers
(query parameters, URL userinfo, ``password=`` style options, bearer tokens)
and exact-match scrubbing of values that adapters registered as secrets.
"""

from __future__ import annotations

import re
import threading
from collections.abc import Mapping
from typing import Any

REDACTED = "***"

_SECRET_KEYS = (
    "password",
    "passwd",
    "pwd",
    "secret",
    "client_secret",
    "token",
    "sas_token",
    "sas",
    "sig",
    "signature",
    "account_key",
    "accountkey",
    "key",
    "access_key",
    "secret_key",
    "aws_secret_access_key",
    "credential",
    "authorization",
    "private_key",
    "passphrase",
    "api_key",
    "apikey",
)

_KEY_PATTERN = "|".join(re.escape(k) for k in _SECRET_KEYS)
_PATTERNS = (
    # key=value inside URLs, connection strings and option dumps
    re.compile(rf"(?i)\b({_KEY_PATTERN})\s*[=:]\s*([^\s;&,'\"]+)"),
    # 'key': 'value' or "key": "value" in mapping reprs
    re.compile(rf"(?i)(['\"]({_KEY_PATTERN})['\"]\s*:\s*['\"])([^'\"]*)(['\"])"),
    # userinfo in URIs: scheme://user:password@host
    re.compile(r"(\w+://[^/\s:@]+:)([^@/\s]+)(@)"),
    # bearer tokens
    re.compile(r"(?i)(bearer\s+)([A-Za-z0-9\-._~+/]+=*)"),
)

_registry: set[str] = set()
_lock = threading.Lock()


def register_secret(value: str) -> None:
    """Register a literal secret value so it is scrubbed wherever it appears."""

    if value and value != REDACTED:
        with _lock:
            _registry.add(value)


def clear_secrets() -> None:
    with _lock:
        _registry.clear()


def redact(text: str) -> str:
    """Scrub registered secret values and common secret patterns from text."""

    if not text:
        return text
    with _lock:
        secrets = sorted(_registry, key=len, reverse=True)
    for secret in secrets:
        if len(secret) >= 8:
            text = text.replace(secret, REDACTED)
        else:
            # Replacing a short value as a raw substring would turn a secret
            # such as ``a`` into noise throughout every diagnostic.  Short
            # credentials are still scrubbed when they occur as a complete
            # token (including inside URI user-info and key/value text).
            text = re.sub(
                rf"(?<![A-Za-z0-9]){re.escape(secret)}(?![A-Za-z0-9])",
                REDACTED,
                text,
            )
    text = _PATTERNS[3].sub(lambda m: f"{m.group(1)}{REDACTED}", text)
    text = _PATTERNS[2].sub(lambda m: f"{m.group(1)}{REDACTED}{m.group(3)}", text)
    text = _PATTERNS[1].sub(lambda m: f"{m.group(1)}{REDACTED}{m.group(4)}", text)
    text = _PATTERNS[0].sub(lambda m: f"{m.group(1)}={REDACTED}", text)
    return text


def redact_mapping(values: Mapping[str, Any]) -> dict[str, Any]:
    """Return a copy with secret-looking keys redacted and all values scrubbed."""

    out: dict[str, Any] = {}
    for key, value in values.items():
        lowered = key.lower()
        if any(lowered == k or lowered.endswith("_" + k) for k in _SECRET_KEYS):
            out[key] = REDACTED
        elif isinstance(value, str):
            out[key] = redact(value)
        elif isinstance(value, Mapping):
            out[key] = redact_mapping(value)
        else:
            out[key] = value
    return out


def redact_exception(exc: BaseException) -> str:
    """A redacted one-line description of an exception, without provider types leaking secrets."""

    return redact(f"{type(exc).__name__}: {exc}")


__all__ = [
    "REDACTED",
    "clear_secrets",
    "redact",
    "redact_exception",
    "redact_mapping",
    "register_secret",
]
