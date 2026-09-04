"""Parameter binding: every plan parameter must be supplied exactly once."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from invariantql.domain.diagnostics import DiagnosticCode, ParameterError
from invariantql.domain.expressions import Literal
from invariantql.domain.plan import QueryPlan
from invariantql.domain.types import PORTABLE_DECIMAL_PRECISION, is_portable_type


def bind_parameters(plan: QueryPlan, values: Mapping[str, Any] | None) -> dict[str, Literal]:
    supplied = dict(values or {})
    expected = plan.parameters
    missing = [name for name in expected if name not in supplied]
    if missing:
        raise ParameterError(
            f"missing parameter(s): {', '.join(missing)}",
            code=DiagnosticCode.PARAMETER_MISSING,
            details={"missing": ",".join(missing)},
        )
    unexpected = [name for name in supplied if name not in expected]
    if unexpected:
        raise ParameterError(
            f"unexpected parameter(s): {', '.join(unexpected)}",
            code=DiagnosticCode.PARAMETER_UNEXPECTED,
            details={"unexpected": ",".join(unexpected)},
        )
    bound: dict[str, Literal] = {}
    for name in expected:
        value = supplied[name]
        try:
            literal = value if isinstance(value, Literal) else Literal.of(value)
        except (TypeError, ValueError) as exc:
            raise ParameterError(
                f"parameter {name!r} has unsupported type {type(value).__name__}",
                code=DiagnosticCode.PARAMETER_INVALID,
                details={"parameter": name},
            ) from exc
        if not is_portable_type(literal.data_type):
            raise ParameterError(
                f"parameter {name!r} exceeds the Local+Spark decimal precision "
                f"limit of {PORTABLE_DECIMAL_PRECISION}",
                code=DiagnosticCode.PARAMETER_INVALID,
                details={"parameter": name, "type": str(literal.data_type)},
            )
        bound[name] = literal
    return bound


__all__ = ["bind_parameters"]
