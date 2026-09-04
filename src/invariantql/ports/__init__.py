"""Ports: the behaviour InvariantQL requires at its integration boundaries."""

from invariantql.ports.engine import (
    CompilingExecutionEngine,
    ExecutionEngine,
    LocalExecutionEngine,
    Reachability,
)
from invariantql.ports.format_handler import (
    DistributedFormatHandler,
    LocalFormatHandler,
    ReaderSpec,
)
from invariantql.ports.frontend import QueryFrontend
from invariantql.ports.source import DataSource, FileRelation, NativeRelation
from invariantql.ports.storage import ObjectInfo, Storage, StorageCapabilities
from invariantql.ports.streams import LocalResult, RecordBatchStream

__all__ = [
    "CompilingExecutionEngine",
    "DataSource",
    "DistributedFormatHandler",
    "ExecutionEngine",
    "FileRelation",
    "LocalExecutionEngine",
    "LocalFormatHandler",
    "LocalResult",
    "NativeRelation",
    "ObjectInfo",
    "QueryFrontend",
    "Reachability",
    "ReaderSpec",
    "RecordBatchStream",
    "Storage",
    "StorageCapabilities",
]
