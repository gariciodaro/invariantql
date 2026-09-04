"""Lazy adapter loading with typed missing-dependency errors (ADR-0010)."""

from __future__ import annotations

import importlib
from typing import Any

from invariantql.domain.diagnostics import DiagnosticCode, MissingDependencyError
from invariantql.domain.redaction import redact_exception


def load_adapter(module: str, attribute: str, *, extra: str | None) -> Any:
    """Import ``module`` and return ``attribute``; explain which extra is missing on failure."""

    try:
        mod = importlib.import_module(module)
    except ImportError as exc:
        hint = (
            f"install it with: pip install 'invariantql[{extra}]'"
            if extra
            else "the adapter could not be imported"
        )
        raise MissingDependencyError(
            f"adapter {module!r} is unavailable ({redact_exception(exc)}); {hint}",
            code=DiagnosticCode.ADAPTER_DEPENDENCY_MISSING,
            details={"module": module, "extra": extra or ""},
        ) from exc
    try:
        return getattr(mod, attribute)
    except AttributeError as exc:  # pragma: no cover - programming error
        raise MissingDependencyError(
            f"adapter module {module!r} has no attribute {attribute!r}",
            code=DiagnosticCode.ADAPTER_UNKNOWN,
        ) from exc


__all__ = ["load_adapter"]
