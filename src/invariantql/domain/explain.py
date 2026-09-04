"""Structured explain output (FF-06).

Every executable plan node receives a disposition, an execution location, a
stable reason code and capability evidence. The structure and codes follow the
compatibility policy; the rendered text is free to evolve.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from invariantql.domain.diagnostics import Diagnostic, DiagnosticCode

EXPLAIN_FORMAT_VERSION = "1"


class Disposition(str, Enum):
    PUSHED = "pushed"
    PARTIAL = "partial"
    RESIDUAL = "residual"
    REJECTED = "rejected"


class ExecutionLocation(str, Enum):
    SOURCE = "source"
    ENGINE = "engine"
    NONE = "none"


@dataclass(frozen=True, slots=True)
class ExplainNode:
    node_id: str
    operation: str
    disposition: Disposition
    location: ExecutionLocation
    reason_code: DiagnosticCode
    detail: str = ""
    pushed: str | None = None
    residual: str | None = None
    evidence: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", tuple(self.evidence))

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "operation": self.operation,
            "disposition": self.disposition.value,
            "location": self.location.value,
            "reason_code": self.reason_code.value,
            "detail": self.detail,
            "pushed": self.pushed,
            "residual": self.residual,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class ExplainPlan:
    engine: str
    source: str
    fingerprint: str
    nodes: tuple[ExplainNode, ...]
    executable: bool
    staging_required: bool = False
    diagnostics: tuple[Diagnostic, ...] = ()
    scan_capabilities: dict[str, Any] | None = None
    engine_capabilities: dict[str, Any] | None = None
    version: str = EXPLAIN_FORMAT_VERSION

    def __post_init__(self) -> None:
        object.__setattr__(self, "nodes", tuple(self.nodes))
        object.__setattr__(self, "diagnostics", tuple(self.diagnostics))

    @property
    def rejected(self) -> tuple[ExplainNode, ...]:
        return tuple(n for n in self.nodes if n.disposition is Disposition.REJECTED)

    def node(self, node_id: str) -> ExplainNode:
        for n in self.nodes:
            if n.node_id == node_id:
                return n
        raise KeyError(node_id)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "engine": self.engine,
            "source": self.source,
            "fingerprint": self.fingerprint,
            "executable": self.executable,
            "staging_required": self.staging_required,
            "nodes": [n.to_dict() for n in self.nodes],
            "diagnostics": [d.to_dict() for d in self.diagnostics],
            "scan_capabilities": self.scan_capabilities,
            "engine_capabilities": self.engine_capabilities,
        }

    def render(self) -> str:
        lines = [
            f"engine={self.engine} source={self.source} executable={self.executable}"
            + (" staging_required" if self.staging_required else ""),
        ]
        for n in self.nodes:
            lines.append(
                f"  {n.node_id:<12} {n.disposition.value:<8} @{n.location.value:<6} "
                f"[{n.reason_code.value}] {n.detail}"
            )
            if n.pushed:
                lines.append(f"{'':16}pushed:   {n.pushed}")
            if n.residual:
                lines.append(f"{'':16}residual: {n.residual}")
            for e in n.evidence:
                lines.append(f"{'':16}evidence: {e}")
        for d in self.diagnostics:
            lines.append(f"  ! {d}")
        return "\n".join(lines)

    def __str__(self) -> str:
        return self.render()


__all__ = [
    "EXPLAIN_FORMAT_VERSION",
    "Disposition",
    "ExecutionLocation",
    "ExplainNode",
    "ExplainPlan",
]
