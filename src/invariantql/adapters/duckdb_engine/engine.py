"""DuckDB local execution engine (ADR-0005, ADR-0011).

DuckDB is the local engine: it reads CSV/Parquet/JSON natively through a
storage bridge, consumes Arrow streams from generic format handlers and native
sources, and evaluates every residual operation. Results stream as Arrow
record batches.
"""

from __future__ import annotations

import itertools
import threading
from collections.abc import Mapping
from typing import Any

import duckdb
import pyarrow as pa

from invariantql.adapters._shared.arrow import from_arrow_schema
from invariantql.adapters._shared.sqltext import DUCKDB, SqlGenerator
from invariantql.adapters.duckdb_engine.fs_bridge import StorageBridge
from invariantql.adapters.duckdb_engine.native_formats import NATIVE_FORMATS, relation_sql
from invariantql.adapters.duckdb_engine.result import LocalResult
from invariantql.domain.capabilities import EngineCapabilities, PushdownCapabilities
from invariantql.domain.diagnostics import (
    DiagnosticCode,
    ExecutionError,
    UnsupportedOperationError,
)
from invariantql.domain.execution import ExecutionPlan
from invariantql.domain.expressions import Literal
from invariantql.domain.location import Location
from invariantql.domain.redaction import redact_exception
from invariantql.domain.schema import Schema
from invariantql.ports.engine import Reachability
from invariantql.ports.format_handler import LocalFormatHandler
from invariantql.ports.source import DataSource, FileRelation, NativeRelation
from invariantql.ports.storage import Storage

_ENGINE_NAME = "duckdb"


class DuckDBEngine:
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection | None = None,
        *,
        memory_limit: str | None = None,
        threads: int | None = None,
    ) -> None:
        self._con = connection or duckdb.connect(database=":memory:")
        if memory_limit:
            self._con.execute(f"SET memory_limit = '{memory_limit}'")
        if threads:
            self._con.execute(f"SET threads = {int(threads)}")
        self._bridge = StorageBridge()
        self._con.register_filesystem(self._bridge)
        self._handlers: dict[str, LocalFormatHandler] = {}
        self._lock = threading.Lock()
        self._counter = itertools.count(1)

    # -- identity -----------------------------------------------------------

    @property
    def name(self) -> str:
        return _ENGINE_NAME

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._con

    def register_format_handler(self, handler: LocalFormatHandler) -> None:
        self._handlers[handler.format_name] = handler

    @property
    def format_handlers(self) -> tuple[str, ...]:
        return tuple(sorted({*NATIVE_FORMATS, *self._handlers}))

    def capabilities(self) -> EngineCapabilities:
        return EngineCapabilities(
            _ENGINE_NAME,
            lazy=False,
            evidence=("DuckDB evaluates the full portable expression profile as residual work",),
        )

    def reachability(self, source: DataSource) -> Reachability:
        relation = source.relation()
        if isinstance(relation, FileRelation):
            fmt = relation.data_format.format_name
            if fmt not in NATIVE_FORMATS and fmt not in self._handlers:
                return Reachability(
                    False,
                    f"no local handler registered for format {fmt!r} (install/enable its extra)",
                )
            return Reachability(True, "storage read through the DuckDB storage bridge")
        return Reachability(True, "native source scanned in-process")

    def scan_capabilities(self, source: DataSource) -> PushdownCapabilities:
        relation = source.relation()
        if isinstance(relation, NativeRelation):
            return source.capabilities()
        fmt = relation.data_format.format_name
        if fmt in NATIVE_FORMATS:
            return PushdownCapabilities.full(
                f"DuckDB native {fmt} reader: projection, predicate and limit evaluated inside the scan",
            )
        handler = self._handler(fmt)
        return handler.capabilities(relation.data_format)

    def schema(self, source: DataSource) -> Schema:
        relation = source.relation()
        if isinstance(relation, NativeRelation):
            return source.schema()
        declared = getattr(relation.data_format, "schema", None)
        if isinstance(declared, Schema) and relation.data_format.format_name in NATIVE_FORMATS:
            return declared
        fmt = relation.data_format.format_name
        if fmt in NATIVE_FORMATS:
            uri, release = self._mount(relation.storage, relation.location)
            try:
                sql = f"SELECT * FROM {relation_sql(relation.data_format, uri)} LIMIT 0"
                with self._lock:
                    arrow_schema = _arrow_table(self._con.execute(sql)).schema
            except duckdb.Error as exc:
                raise ExecutionError(
                    f"DuckDB could not read {relation.location.uri}: {redact_exception(exc)}",
                    code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
                    target=source.name,
                ) from None
            finally:
                release()
            return from_arrow_schema(arrow_schema)
        return self._handler(fmt).schema(relation.storage, relation.location, relation.data_format)

    # -- execution ----------------------------------------------------------

    def execute(
        self,
        execution_plan: ExecutionPlan,
        source: DataSource,
        parameters: Mapping[str, Literal],
        *,
        batch_size: int,
    ) -> LocalResult:
        if not execution_plan.executable:
            raise UnsupportedOperationError(
                "execution plan is not executable on DuckDB",
                code=DiagnosticCode.ENGINE_PLAN_NOT_EXECUTABLE,
                target=_ENGINE_NAME,
            )
        relation = source.relation()
        cleanup: list[Any] = []
        generator = SqlGenerator(DUCKDB, parameters)
        try:
            if isinstance(relation, NativeRelation):
                stream = source.scan(execution_plan.pushed, parameters, batch_size=batch_size)
                inner_sql = self._register_stream(stream, cleanup)
            elif relation.data_format.format_name in NATIVE_FORMATS:
                uri, release = self._mount(relation.storage, relation.location)
                cleanup.append(release)
                pushed = execution_plan.pushed
                inner_sql = (
                    "("
                    + generator.select(
                        relation_sql(relation.data_format, uri),
                        columns=pushed.projection,
                        predicate=pushed.predicate,
                        limit=pushed.limit,
                    )
                    + ")"
                )
            else:
                handler = self._handler(relation.data_format.format_name)
                stream = handler.scan(
                    relation.storage,
                    relation.location,
                    relation.data_format,
                    execution_plan.pushed,
                    parameters,
                    batch_size=batch_size,
                )
                inner_sql = self._register_stream(stream, cleanup)

            residual = execution_plan.residual
            sql = generator.select(
                inner_sql,
                projection=residual.projection,
                predicate=residual.predicate,
                limit=residual.limit,
            )
            with self._lock:
                cursor = self._con.cursor()
                cleanup.append(cursor.close)
                try:
                    cursor.execute(sql, generator.values)
                    reader = _arrow_reader(cursor, batch_size)
                except duckdb.Error as exc:
                    raise ExecutionError(
                        f"DuckDB execution failed: {redact_exception(exc)}",
                        target=_ENGINE_NAME,
                        details={"sql": sql},
                    ) from None
        except BaseException:
            for hook in reversed(cleanup):
                try:
                    hook()
                except Exception:
                    pass
            raise
        return LocalResult(reader, on_close=list(reversed(cleanup)), engine=_ENGINE_NAME)

    def close(self) -> None:
        with self._lock:
            self._con.close()

    # -- helpers ------------------------------------------------------------

    def _handler(self, fmt: str) -> LocalFormatHandler:
        try:
            return self._handlers[fmt]
        except KeyError:
            raise UnsupportedOperationError(
                f"no local handler registered for format {fmt!r}",
                code=DiagnosticCode.FORMAT_UNSUPPORTED,
                target=_ENGINE_NAME,
                details={"format": fmt},
            ) from None

    def _mount(self, storage: Storage, location: Location) -> tuple[str, Any]:
        native = storage.native_uri(location)
        if native and native.startswith("file://"):
            from urllib.request import url2pathname

            path = url2pathname(native[len("file://") :])
            return path, lambda: None
        uri = self._bridge.mount(storage, location)
        return uri, lambda: self._bridge.unmount(uri)

    def _register_stream(self, stream: Any, cleanup: list[Any]) -> str:
        name = f"iql_stream_{next(self._counter)}"
        reader = stream if isinstance(stream, pa.RecordBatchReader) else _as_reader(stream)
        with self._lock:
            self._con.register(name, reader)
        cleanup.append(stream.close)
        cleanup.append(lambda: self._unregister(name))
        return f'"{name}"'

    def _unregister(self, name: str) -> None:
        with self._lock:
            try:
                self._con.unregister(name)
            except duckdb.Error:  # pragma: no cover - already gone
                pass


def _arrow_table(connection: duckdb.DuckDBPyConnection) -> pa.Table:
    method = getattr(connection, "to_arrow_table", None) or connection.fetch_arrow_table
    return method()


def _arrow_reader(connection: duckdb.DuckDBPyConnection, batch_size: int) -> pa.RecordBatchReader:
    method = getattr(connection, "to_arrow_reader", None) or connection.fetch_record_batch
    return method(batch_size)


def _as_reader(stream: Any) -> pa.RecordBatchReader:
    schema = stream.schema
    if not isinstance(schema, pa.Schema):
        raise ExecutionError("stream schema is not an Arrow schema")
    return pa.RecordBatchReader.from_batches(schema, iter(stream))


__all__ = ["DuckDBEngine"]
