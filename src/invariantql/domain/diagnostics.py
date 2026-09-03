"""Stable diagnostic codes and the error hierarchy.

Codes are a compatibility surface (FF-06, FF-15): human text may change, codes
may only be added. Every InvariantQL error carries a ``Diagnostic``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any


class DiagnosticCode(str, Enum):
    # SQL frontend (FF-11)
    SQL_PARSE_ERROR = "SQL_PARSE_ERROR"
    SQL_MULTIPLE_STATEMENTS = "SQL_MULTIPLE_STATEMENTS"
    SQL_NOT_A_SELECT = "SQL_NOT_A_SELECT"
    SQL_MULTI_SOURCE = "SQL_MULTI_SOURCE"
    SQL_QUALIFIED_SOURCE = "SQL_QUALIFIED_SOURCE"
    SQL_UNSUPPORTED_CONSTRUCT = "SQL_UNSUPPORTED_CONSTRUCT"
    SQL_UNSUPPORTED_EXPRESSION = "SQL_UNSUPPORTED_EXPRESSION"
    SQL_INVALID_LIMIT = "SQL_INVALID_LIMIT"
    SQL_AMBIGUOUS_IDENTIFIER = "SQL_AMBIGUOUS_IDENTIFIER"
    SQL_EMPTY = "SQL_EMPTY"

    # Plan validation
    PLAN_UNKNOWN_COLUMN = "PLAN_UNKNOWN_COLUMN"
    PLAN_TYPE_MISMATCH = "PLAN_TYPE_MISMATCH"
    PLAN_INVALID_SHAPE = "PLAN_INVALID_SHAPE"

    # Planner dispositions (explain reason codes)
    PUSHDOWN_FULL = "PUSHDOWN_FULL"
    PUSHDOWN_PARTIAL = "PUSHDOWN_PARTIAL"
    PUSHDOWN_TRIVIAL = "PUSHDOWN_TRIVIAL"
    RESIDUAL_NO_CAPABILITY = "RESIDUAL_NO_CAPABILITY"
    RESIDUAL_UNSUPPORTED_EXPRESSION = "RESIDUAL_UNSUPPORTED_EXPRESSION"
    RESIDUAL_COMPUTED_PROJECTION = "RESIDUAL_COMPUTED_PROJECTION"
    RESIDUAL_LIMIT_AFTER_RESIDUAL_FILTER = "RESIDUAL_LIMIT_AFTER_RESIDUAL_FILTER"
    REJECTED_ENGINE_UNSUPPORTED_EXPRESSION = "REJECTED_ENGINE_UNSUPPORTED_EXPRESSION"
    REJECTED_SOURCE_UNREACHABLE = "REJECTED_SOURCE_UNREACHABLE"

    # Engine / execution
    ENGINE_UNKNOWN = "ENGINE_UNKNOWN"
    ENGINE_CANNOT_REACH_SOURCE = "ENGINE_CANNOT_REACH_SOURCE"
    ENGINE_UNSUPPORTED_SOURCE = "ENGINE_UNSUPPORTED_SOURCE"
    ENGINE_PLAN_NOT_EXECUTABLE = "ENGINE_PLAN_NOT_EXECUTABLE"
    ENGINE_EXECUTION_FAILED = "ENGINE_EXECUTION_FAILED"

    # Sources, storage, formats
    SOURCE_NOT_REGISTERED = "SOURCE_NOT_REGISTERED"
    SOURCE_ALREADY_REGISTERED = "SOURCE_ALREADY_REGISTERED"
    SOURCE_SCHEMA_UNAVAILABLE = "SOURCE_SCHEMA_UNAVAILABLE"
    SOURCE_SCAN_UNSUPPORTED = "SOURCE_SCAN_UNSUPPORTED"
    SOURCE_FAILURE = "SOURCE_FAILURE"
    STORAGE_OBJECT_NOT_FOUND = "STORAGE_OBJECT_NOT_FOUND"
    STORAGE_UNSUPPORTED_OPERATION = "STORAGE_UNSUPPORTED_OPERATION"
    STORAGE_FAILURE = "STORAGE_FAILURE"
    FORMAT_UNSUPPORTED = "FORMAT_UNSUPPORTED"
    FORMAT_INVALID = "FORMAT_INVALID"

    # Adapters and registration (ADR-0010)
    ADAPTER_DEPENDENCY_MISSING = "ADAPTER_DEPENDENCY_MISSING"
    ADAPTER_UNKNOWN = "ADAPTER_UNKNOWN"
    ADAPTER_FAILURE = "ADAPTER_FAILURE"

    # Parameters and results
    PARAMETER_MISSING = "PARAMETER_MISSING"
    PARAMETER_UNEXPECTED = "PARAMETER_UNEXPECTED"
    PARAMETER_INVALID = "PARAMETER_INVALID"
    RESULT_LIMIT_EXCEEDED = "RESULT_LIMIT_EXCEEDED"
    RESULT_CLOSED = "RESULT_CLOSED"

    # Staging (FF-14)
    STAGING_REQUIRED = "STAGING_REQUIRED"


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class Diagnostic:
    code: DiagnosticCode
    message: str
    severity: Severity = Severity.ERROR
    node_id: str | None = None
    target: str | None = None
    details: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "details", tuple((str(k), str(v)) for k, v in self.details))

    @classmethod
    def error(
        cls,
        code: DiagnosticCode,
        message: str,
        *,
        node_id: str | None = None,
        target: str | None = None,
        **details: Any,
    ) -> Diagnostic:
        return cls(
            code,
            message,
            Severity.ERROR,
            node_id,
            target,
            tuple(sorted((k, str(v)) for k, v in details.items())),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code.value,
            "severity": self.severity.value,
            "message": self.message,
            "node_id": self.node_id,
            "target": self.target,
            "details": dict(self.details),
        }

    def __str__(self) -> str:
        where = f" at {self.node_id}" if self.node_id else ""
        target = f" for {self.target}" if self.target else ""
        return f"[{self.code.value}]{where}{target}: {self.message}"


class InvariantQLError(Exception):
    """Base class for every InvariantQL error. Carries a stable diagnostic."""

    default_code: DiagnosticCode = DiagnosticCode.ADAPTER_FAILURE

    def __init__(
        self,
        message: str,
        *,
        code: DiagnosticCode | None = None,
        node_id: str | None = None,
        target: str | None = None,
        details: Mapping[str, Any] | None = None,
        diagnostic: Diagnostic | None = None,
    ) -> None:
        if diagnostic is None:
            diagnostic = Diagnostic.error(
                code or self.default_code,
                message,
                node_id=node_id,
                target=target,
                **(dict(details) if details else {}),
            )
        self.diagnostic = diagnostic
        super().__init__(str(diagnostic))

    @property
    def code(self) -> DiagnosticCode:
        return self.diagnostic.code


class SqlFrontendError(InvariantQLError):
    default_code = DiagnosticCode.SQL_UNSUPPORTED_CONSTRUCT


class PlanValidationError(InvariantQLError):
    default_code = DiagnosticCode.PLAN_INVALID_SHAPE


class UnsupportedOperationError(InvariantQLError):
    default_code = DiagnosticCode.ENGINE_PLAN_NOT_EXECUTABLE


class StagingRequiredError(UnsupportedOperationError):
    default_code = DiagnosticCode.STAGING_REQUIRED


class MissingDependencyError(InvariantQLError):
    default_code = DiagnosticCode.ADAPTER_DEPENDENCY_MISSING


class RegistryError(InvariantQLError):
    default_code = DiagnosticCode.SOURCE_NOT_REGISTERED


class ParameterError(InvariantQLError):
    default_code = DiagnosticCode.PARAMETER_MISSING


class MaterializationLimitError(InvariantQLError):
    default_code = DiagnosticCode.RESULT_LIMIT_EXCEEDED


class AdapterError(InvariantQLError):
    """A provider failure translated at an adapter boundary (message redacted)."""

    default_code = DiagnosticCode.ADAPTER_FAILURE


class StorageError(AdapterError):
    default_code = DiagnosticCode.STORAGE_FAILURE


class SourceError(AdapterError):
    default_code = DiagnosticCode.SOURCE_FAILURE


class ExecutionError(AdapterError):
    default_code = DiagnosticCode.ENGINE_EXECUTION_FAILED


__all__ = [
    "AdapterError",
    "Diagnostic",
    "DiagnosticCode",
    "ExecutionError",
    "InvariantQLError",
    "MaterializationLimitError",
    "MissingDependencyError",
    "ParameterError",
    "PlanValidationError",
    "RegistryError",
    "Severity",
    "SourceError",
    "SqlFrontendError",
    "StagingRequiredError",
    "StorageError",
    "UnsupportedOperationError",
]
