"""Apache Iceberg format handlers (ADR-0004).

``IcebergLocalHandler`` scans an Iceberg table through pyiceberg for the local
engine; ``IcebergReaderSpecHandler`` describes Spark's native Iceberg reader.
Both interpret :class:`~invariantql.domain.formats.IcebergFormat`.
"""

from __future__ import annotations

import datetime as _dt
import math
import re
import struct
from collections.abc import Callable, Iterator, Mapping
from decimal import Decimal
from functools import reduce
from typing import Any

import pyarrow as pa
import pyarrow.compute as _compute
from pyiceberg import __version__ as _PYICEBERG_RELEASE
from pyiceberg import expressions as _expressions
from pyiceberg import types as icetypes
from pyiceberg.expressions import BooleanExpression
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import StaticTable

from invariantql.adapters._shared.arrow import from_arrow_schema, stream_from_batches, to_arrow_type
from invariantql.domain.capabilities import PushdownCapabilities, Support
from invariantql.domain.credentials import SecretOptions
from invariantql.domain.diagnostics import (
    DiagnosticCode,
    InvariantQLError,
    ParameterError,
    SourceError,
    UnsupportedOperationError,
)
from invariantql.domain.execution import PushedOperations
from invariantql.domain.expressions import (
    And,
    Column,
    Comparison,
    ComparisonOp,
    Expression,
    ExpressionKind,
    In,
    IsNull,
    Literal,
    Not,
    Or,
    Parameter,
    referenced_columns,
)
from invariantql.domain.formats import DataFormat, IcebergFormat
from invariantql.domain.location import Location
from invariantql.domain.redaction import redact, redact_exception
from invariantql.domain.schema import Schema
from invariantql.ports.format_handler import ReaderSpec
from invariantql.ports.storage import Storage
from invariantql.ports.streams import RecordBatchStream

# pyarrow.compute builds its kernels at import time and pyiceberg's expression
# classes are pydantic models with keyword-only constructors; neither namespace is
# visible to static type checkers, so both are used through ``Any``.
pc: Any = _compute
ice: Any = _expressions

_FORMAT = "iceberg"
_SPARK_JAR = "org.apache.iceberg:iceberg-spark-runtime"
_METADATA_SUFFIX = ".metadata.json"
_VERSION_HINT = "version-hint.text"
_VERSION_PATTERN = re.compile(r"^v?(\d+)")
_VERSION_HINT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

# Expression kinds whose semantics the Iceberg scan reproduces exactly. LIKE and
# arithmetic stay residual: pyiceberg has no LIKE and no computed terms.
_EXPRESSIONS: frozenset[ExpressionKind] = frozenset(
    {
        ExpressionKind.COLUMN,
        ExpressionKind.LITERAL,
        ExpressionKind.PARAMETER,
        ExpressionKind.COMPARISON,
        ExpressionKind.AND,
        ExpressionKind.OR,
        ExpressionKind.NOT,
        ExpressionKind.IS_NULL,
        ExpressionKind.IN,
    }
)

# Canonical storage-adapter option keys -> pyiceberg FileIO properties. Keys that
# already contain a dot are passed through as FileIO properties untouched.
_PROPERTY_KEYS: dict[str, str] = {
    "aws_access_key_id": "s3.access-key-id",
    "aws_secret_access_key": "s3.secret-access-key",
    "aws_session_token": "s3.session-token",
    "aws_region": "s3.region",
    "aws_endpoint_url": "s3.endpoint",
    "aws_anonymous": "s3.anonymous",
    "account_name": "adls.account-name",
    "account_key": "adls.account-key",
    "sas_token": "adls.sas-token",
    "client_id": "adls.client-id",
    "client_secret": "adls.client-secret",
    "tenant_id": "adls.tenant-id",
    "connection_string": "adls.connection-string",
}


class IcebergLocalHandler:
    """Scan an Apache Iceberg table with pyiceberg for the local (DuckDB) engine.

    Constructor: no arguments. Everything is described by the source's storage
    and its :class:`IcebergFormat`:

    - ``metadata_location``: a specific ``*.metadata.json`` file. Absolute
      URIs (``file://``, ``abfss://``, ``s3://``) are resolved by the storage;
      relative paths are joined to the source location. When omitted the
      source location is a table directory: ``metadata/version-hint.text``
      names the current version (Hadoop tables); otherwise the newest
      ``metadata/*.metadata.json`` found by listing the storage is used.
      A source location that itself ends in ``.metadata.json`` is used as is.
    - ``snapshot_id``: pins the scan (and the schema) to that snapshot.

    Credentials: pyiceberg reads the table files itself, so the storage's
    ``native_uri`` and ``native_options`` are handed to its FileIO. Canonical
    option keys (``aws_access_key_id``, ``aws_secret_access_key``,
    ``aws_session_token``, ``aws_region``, ``aws_endpoint_url``, ``aws_anonymous``,
    ``account_name``, ``account_key``, ``sas_token``, ``client_id``,
    ``client_secret``, ``tenant_id``, ``connection_string``, ``endpoint_suffix``) become the
    matching ``s3.*`` / ``adls.*`` FileIO properties; dotted keys pass
    through. Secret values live only inside pyiceberg's properties and are
    registered with the redaction service, so provider errors are scrubbed.
    A storage without a native URI cannot be read (``FORMAT_UNSUPPORTED``).

    Semantics: the pushed predicate is translated into a pyiceberg row filter
    that is a *safe relaxation* of the SQL predicate (file/row-group pruning),
    and every batch is then re-checked with Arrow compute using SQL
    three-valued logic (``NULL <> 5`` excludes the row, ``NOT unknown`` is
    unknown, case-sensitive strings). Column-to-column comparisons, lossy
    literal coercions (a float against an integer column, a decimal with too
    many digits) and unknown columns simply fall back to the Arrow check, so
    the capabilities below are honest. LIKE and arithmetic stay residual.
    The limit is pushed into pyiceberg only when the row filter is exact and
    is always enforced locally. An empty pushed projection reads the first
    table column so that row counts survive.
    """

    @property
    def format_name(self) -> str:
        return _FORMAT

    def capabilities(self, data_format: DataFormat) -> PushdownCapabilities:
        _as_iceberg(data_format)
        return PushdownCapabilities(
            projection=Support.FULL,
            predicate=Support.FULL,
            limit=Support.FULL,
            expressions=_EXPRESSIONS,
            parameters=True,
            evidence=(
                "pyiceberg table scan: column projection, row filter with metadata pruning "
                "re-checked in Arrow with SQL NULL semantics, limit; LIKE and arithmetic residual",
            ),
        )

    def schema(self, storage: Storage, location: Location, data_format: DataFormat) -> Schema:
        fmt = _as_iceberg(data_format)
        table = _load_table(storage, location, fmt)
        try:
            arrow_schema = _snapshot_schema(table, fmt).as_arrow()
        except InvariantQLError:
            raise
        except Exception as exc:
            raise SourceError(
                f"cannot read Iceberg schema of {location.uri}: {redact_exception(exc)}",
                code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
                target=_FORMAT,
            ) from None
        return from_arrow_schema(arrow_schema)

    def scan(
        self,
        storage: Storage,
        location: Location,
        data_format: DataFormat,
        pushed: PushedOperations,
        parameters: Mapping[str, Literal],
        *,
        batch_size: int,
    ) -> RecordBatchStream:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        fmt = _as_iceberg(data_format)
        table = _load_table(storage, location, fmt)
        projection = pushed.projection
        predicate = pushed.predicate
        limit = pushed.limit

        reader: pa.RecordBatchReader | None = None
        try:
            iceberg_schema = _snapshot_schema(table, fmt)
            read_columns = _read_columns(iceberg_schema, projection, predicate)
            row_filter: BooleanExpression = ice.AlwaysTrue()
            exact = True
            if predicate is not None:
                row_filter, exact = _IcebergPredicate(iceberg_schema, parameters).translate(
                    predicate
                )
            data_scan = table.scan(
                row_filter=row_filter,
                selected_fields=read_columns if read_columns is not None else ("*",),
                snapshot_id=fmt.snapshot_id,
                limit=limit if exact else None,
            )
            reader = _batch_reader(data_scan)
            assert reader is not None
            output_names = list(projection) if projection else [f.name for f in reader.schema]
            output_schema = pa.schema(
                [_plain_field(reader.schema.field(name)) for name in output_names]
            )
            checker = _ArrowPredicate(predicate, parameters) if predicate is not None else None
        except InvariantQLError:
            if reader is not None:
                reader.close()
            raise
        except Exception as exc:
            if reader is not None:
                reader.close()
            raise SourceError(
                f"cannot scan Iceberg table {redact(location.uri)}: {redact_exception(exc)}",
                target=_FORMAT,
            ) from None

        assert reader is not None
        batches = _batches(reader, output_schema, checker, limit, batch_size, location.uri)
        return stream_from_batches(output_schema, batches, on_close=[reader.close])


class IcebergReaderSpecHandler:
    """Spark's native Iceberg reader: ``spark.read.format("iceberg").load(<table dir>)``.

    Constructor: no arguments. Requires the
    ``org.apache.iceberg:iceberg-spark-runtime-<spark>_<scala>`` jar matching the
    cluster's Spark and Scala versions (for example
    ``iceberg-spark-runtime-3.5_2.12``). Spark loads a Hadoop table by its
    directory path; ``snapshot_id`` becomes the ``snapshot-id`` read option.

    ``metadata_location`` cannot be honoured: Spark resolves the table's
    current version from the directory, so a format pinned to a metadata file
    is refused (``FORMAT_UNSUPPORTED``) rather than silently read at a
    different version. Point the source path at the metadata file itself, or
    pin with ``snapshot_id``. Credentials are not part of the reader spec; use
    ``SparkEngine.apply_storage_credentials`` for the Hadoop configuration.
    """

    @property
    def format_name(self) -> str:
        return _FORMAT

    def reader_spec(self, data_format: DataFormat, uri: str) -> ReaderSpec:
        fmt = _as_iceberg(data_format)
        if fmt.metadata_location is not None:
            raise UnsupportedOperationError(
                "Spark loads Iceberg tables by directory and cannot pin a metadata file; "
                "point the source at the metadata file or use snapshot_id",
                code=DiagnosticCode.FORMAT_UNSUPPORTED,
                target=_FORMAT,
                details={"format": _FORMAT, "uri": uri},
            )
        options: dict[str, str] = {}
        if fmt.snapshot_id is not None:
            options["snapshot-id"] = str(fmt.snapshot_id)
        return ReaderSpec(_FORMAT, options, None, requires=(_SPARK_JAR,))


# -- table loading ------------------------------------------------------------


def _as_iceberg(data_format: DataFormat) -> IcebergFormat:
    if not isinstance(data_format, IcebergFormat):
        raise UnsupportedOperationError(
            f"Iceberg handler cannot interpret format {data_format.format_name!r}",
            code=DiagnosticCode.FORMAT_UNSUPPORTED,
            target=_FORMAT,
            details={"format": data_format.format_name},
        )
    return data_format


def _fileio_properties(options: SecretOptions) -> dict[str, str]:
    """Translate canonical storage options into pyiceberg FileIO properties."""

    properties: dict[str, str] = {}
    revealed = options.reveal()
    for key, value in revealed.items():
        if value is None:
            continue
        if key in _PROPERTY_KEYS:
            properties[_PROPERTY_KEYS[key]] = str(value)
        elif "." in key:
            properties[key] = str(value)
    account = revealed.get("account_name")
    suffix = revealed.get("endpoint_suffix")
    if isinstance(account, str) and account and isinstance(suffix, str) and suffix.strip("."):
        properties["adls.account-host"] = f"{account}.blob.{suffix.strip('.')}"
    if revealed.get("credential_kind") == "anonymous":
        properties["adls.anon"] = "true"
    return properties


def _metadata_version_key(location: Location) -> tuple[int, str]:
    match = _VERSION_PATTERN.match(location.name)
    return (int(match.group(1)) if match else -1, location.name)


def _metadata_location(storage: Storage, location: Location, fmt: IcebergFormat) -> Location:
    """Find the ``*.metadata.json`` file describing the table version to read."""

    if fmt.metadata_location:
        explicit = fmt.metadata_location
        if "://" in explicit:
            return storage.resolve(Location.parse(explicit))
        if explicit.startswith("/"):
            return storage.resolve(explicit)
        return location.join(explicit)
    if location.name.endswith(_METADATA_SUFFIX):
        return location

    metadata_dir = location.join("metadata")
    hint = metadata_dir.join(_VERSION_HINT)
    if storage.exists(hint):
        try:
            with storage.open_read(hint) as handle:
                content = handle.read().decode("utf-8").strip()
        except UnicodeDecodeError as exc:
            raise SourceError(
                f"Iceberg version hint at {redact(hint.uri)} is not UTF-8: {redact_exception(exc)}",
                code=DiagnosticCode.FORMAT_INVALID,
                target=_FORMAT,
            ) from None
        if not content:
            raise SourceError(
                f"empty Iceberg version hint at {redact(hint.uri)}",
                code=DiagnosticCode.FORMAT_INVALID,
                target=_FORMAT,
            )
        if (
            not _VERSION_HINT_NAME.fullmatch(content)
            or ".." in content
            or "/" in content
            or "\\" in content
        ):
            raise SourceError(
                f"invalid Iceberg version hint at {redact(hint.uri)}",
                code=DiagnosticCode.FORMAT_INVALID,
                target=_FORMAT,
            )
        if content.endswith(_METADATA_SUFFIX):
            name = content
        elif content.isdigit():
            name = f"v{content}{_METADATA_SUFFIX}"
        else:
            name = f"{content}{_METADATA_SUFFIX}"
        return metadata_dir.join(name)

    candidates = [
        info.location
        for info in storage.list(metadata_dir)
        if not info.is_directory and info.location.name.endswith(_METADATA_SUFFIX)
    ]
    if not candidates:
        raise SourceError(
            f"no Iceberg metadata file found under {redact(metadata_dir.uri)}",
            code=DiagnosticCode.FORMAT_INVALID,
            target=_FORMAT,
        )
    return max(candidates, key=_metadata_version_key)


def _load_table(storage: Storage, location: Location, fmt: IcebergFormat) -> StaticTable:
    metadata = _metadata_location(storage, location, fmt)
    uri = storage.native_uri(metadata)
    if uri is None:
        raise UnsupportedOperationError(
            f"storage {storage.name!r} exposes no native URI; pyiceberg cannot read "
            f"Iceberg table {redact(location.uri)}",
            code=DiagnosticCode.FORMAT_UNSUPPORTED,
            target=_FORMAT,
            details={"format": _FORMAT},
        )
    properties = _fileio_properties(storage.native_options())
    try:
        table = StaticTable.from_metadata(uri, properties=properties)
    except Exception as exc:
        raise SourceError(
            f"cannot load Iceberg metadata {redact(metadata.uri)}: {redact_exception(exc)}",
            code=DiagnosticCode.FORMAT_INVALID,
            target=_FORMAT,
        ) from None
    if fmt.snapshot_id is not None and table.snapshot_by_id(fmt.snapshot_id) is None:
        raise SourceError(
            f"Iceberg table {redact(location.uri)} has no snapshot {fmt.snapshot_id}",
            code=DiagnosticCode.FORMAT_INVALID,
            target=_FORMAT,
            details={"snapshot_id": fmt.snapshot_id},
        )
    return table


def _snapshot_schema(table: StaticTable, fmt: IcebergFormat) -> IcebergSchema:
    """The table schema as of the pinned snapshot (or the current one)."""

    return table.scan(snapshot_id=fmt.snapshot_id).projection()


def _read_columns(
    schema: IcebergSchema, projection: tuple[str, ...] | None, predicate: Expression | None
) -> tuple[str, ...] | None:
    """Columns to request from pyiceberg: the projection plus predicate columns."""

    if projection is None:
        return None
    columns = list(projection)
    if predicate is not None:
        columns.extend(c for c in referenced_columns(predicate) if c not in columns)
    if not columns:
        names = [field.name for field in schema.fields]
        columns = names[:1]
    return tuple(columns)


def _release_pair(release: str) -> tuple[int, int]:
    match = re.match(r"^(\d+)\.(\d+)", release)
    return (int(match.group(1)), int(match.group(2))) if match else (0, 0)


def _batch_reader(data_scan: Any) -> pa.RecordBatchReader:
    """Build a reader without materialising the result or whole data files.

    PyIceberg 0.10 introduced parallel task evaluation whose worker function
    converts every batch from a data file to a list. With many workers or
    large files that defeats the memory contract of this adapter. For affected
    releases, use the same Arrow scanner's sequential generator directly.
    Only planned task metadata and positional-delete indexes are retained;
    data batches are read one at a time. Earlier supported releases already
    implement ``to_arrow_batch_reader`` sequentially.
    """

    to_reader = getattr(data_scan, "to_arrow_batch_reader", None)
    if to_reader is None:
        raise UnsupportedOperationError(
            "Iceberg streaming requires pyiceberg 0.7 or newer",
            code=DiagnosticCode.FORMAT_UNSUPPORTED,
            target=_FORMAT,
            details={"format": _FORMAT},
        )
    if _release_pair(_PYICEBERG_RELEASE) < (0, 10):
        return to_reader()

    from pyiceberg.io import pyarrow as iceberg_arrow

    scanner_type = iceberg_arrow.ArrowScan
    read_deletes = getattr(iceberg_arrow, "_read_all_delete_files", None)
    sequential = getattr(scanner_type, "_record_batches_from_scan_tasks_and_deletes", None)
    if read_deletes is None or sequential is None:
        # A future release may remove the private compatibility hooks after
        # fixing its public streaming implementation.
        return to_reader()

    projected = data_scan.projection()
    tasks = tuple(data_scan.plan_files())
    scanner = scanner_type(
        data_scan.table_metadata,
        data_scan.io,
        projected,
        data_scan.row_filter,
        data_scan.case_sensitive,
        data_scan.limit,
    )
    deletes = read_deletes(data_scan.io, tasks)
    batches = sequential(scanner, tasks, deletes)
    target_schema = iceberg_arrow.schema_to_pyarrow(projected)
    return pa.RecordBatchReader.from_batches(target_schema, batches).cast(target_schema)


def _plain_field(field: pa.Field) -> pa.Field:
    return pa.field(field.name, field.type, field.nullable)


def _batches(
    reader: pa.RecordBatchReader,
    output_schema: pa.Schema,
    checker: _ArrowPredicate | None,
    limit: int | None,
    batch_size: int,
    uri: str,
) -> Iterator[pa.RecordBatch]:
    remaining = limit
    names = output_schema.names
    try:
        for batch in reader:
            if remaining is not None and remaining <= 0:
                break
            if checker is not None:
                batch = checker.filter(batch)
            if batch.num_rows == 0:
                continue
            if batch.schema.names != names:
                batch = batch.select(names)
            batch = pa.RecordBatch.from_arrays(batch.columns, schema=output_schema)
            if remaining is not None:
                if batch.num_rows > remaining:
                    batch = batch.slice(0, remaining)
                remaining -= batch.num_rows
            for offset in range(0, batch.num_rows, max(1, batch_size)):
                yield batch.slice(offset, batch_size)
    except InvariantQLError:
        raise
    except Exception as exc:
        raise SourceError(
            f"Iceberg scan of {redact(uri)} failed: {redact_exception(exc)}", target=_FORMAT
        ) from None


# -- predicate translation ------------------------------------------------------

_NOT_EXACT = object()
_RELAXED: tuple[BooleanExpression, bool] = (ice.AlwaysTrue(), False)

_MIRROR: dict[ComparisonOp, ComparisonOp] = {
    ComparisonOp.EQ: ComparisonOp.EQ,
    ComparisonOp.NE: ComparisonOp.NE,
    ComparisonOp.LT: ComparisonOp.GT,
    ComparisonOp.LE: ComparisonOp.GE,
    ComparisonOp.GT: ComparisonOp.LT,
    ComparisonOp.GE: ComparisonOp.LE,
}
_NEGATE: dict[ComparisonOp, ComparisonOp] = {
    ComparisonOp.EQ: ComparisonOp.NE,
    ComparisonOp.NE: ComparisonOp.EQ,
    ComparisonOp.LT: ComparisonOp.GE,
    ComparisonOp.LE: ComparisonOp.GT,
    ComparisonOp.GT: ComparisonOp.LE,
    ComparisonOp.GE: ComparisonOp.LT,
}
_ICEBERG_PREDICATES: dict[ComparisonOp, Any] = {
    ComparisonOp.EQ: ice.EqualTo,
    ComparisonOp.NE: ice.NotEqualTo,
    ComparisonOp.LT: ice.LessThan,
    ComparisonOp.LE: ice.LessThanOrEqual,
    ComparisonOp.GT: ice.GreaterThan,
    ComparisonOp.GE: ice.GreaterThanOrEqual,
}

_INT32 = (-(2**31), 2**31 - 1)
_INT64 = (-(2**63), 2**63 - 1)
_EXACT_DOUBLE_INT = 2**53
_EXACT_FLOAT_INT = 2**24


def _parameter_value(expression: Expression, parameters: Mapping[str, Literal]) -> Any:
    """The Python value of a literal or bound parameter."""

    if isinstance(expression, Parameter):
        try:
            return parameters[expression.name].value
        except KeyError:
            raise ParameterError(
                f"missing parameter {expression.name!r}",
                code=DiagnosticCode.PARAMETER_MISSING,
            ) from None
    if isinstance(expression, Literal):
        return expression.value
    raise UnsupportedOperationError(
        f"expected a literal or parameter, got {expression.kind.value}",
        code=DiagnosticCode.FORMAT_UNSUPPORTED,
        target=_FORMAT,
    )


def _fits_float32(value: float) -> bool:
    try:
        return struct.unpack("<f", struct.pack("<f", value))[0] == value
    except (OverflowError, struct.error):
        return False


def _decimal_value(value: Decimal, field_type: icetypes.DecimalType) -> Any:
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int) or -exponent > field_type.scale:
        return _NOT_EXACT
    quantized = value.quantize(Decimal(1).scaleb(-field_type.scale))
    if len(quantized.as_tuple().digits) > field_type.precision:
        return _NOT_EXACT
    return quantized


def _integer_value(value: int, field_type: icetypes.IcebergType) -> Any:
    if isinstance(field_type, icetypes.LongType):
        return value if _INT64[0] <= value <= _INT64[1] else _NOT_EXACT
    if isinstance(field_type, icetypes.IntegerType):
        return value if _INT32[0] <= value <= _INT32[1] else _NOT_EXACT
    if isinstance(field_type, icetypes.DoubleType):
        return float(value) if abs(value) <= _EXACT_DOUBLE_INT else _NOT_EXACT
    if isinstance(field_type, icetypes.FloatType):
        return float(value) if abs(value) <= _EXACT_FLOAT_INT else _NOT_EXACT
    if isinstance(field_type, icetypes.DecimalType):
        return _decimal_value(Decimal(value), field_type)
    return _NOT_EXACT


def _iceberg_value(value: Any, field_type: icetypes.IcebergType) -> Any:
    """A Python value pyiceberg binds to ``field_type`` without changing its meaning.

    Returns ``_NOT_EXACT`` when the comparison would be lossy (a float against an
    integer column, a decimal with more digits than the column, mismatched time
    zones) so that the Arrow re-check alone decides.
    """

    if isinstance(value, bool):
        return value if isinstance(field_type, icetypes.BooleanType) else _NOT_EXACT
    if isinstance(value, int):
        return _integer_value(value, field_type)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return _NOT_EXACT
        if isinstance(field_type, icetypes.DoubleType):
            return value
        if isinstance(field_type, icetypes.FloatType):
            return value if _fits_float32(value) else _NOT_EXACT
        if isinstance(field_type, (icetypes.LongType, icetypes.IntegerType)):
            return _integer_value(int(value), field_type) if value.is_integer() else _NOT_EXACT
        return _NOT_EXACT
    if isinstance(value, Decimal):
        if not value.is_finite():
            return _NOT_EXACT
        if isinstance(field_type, icetypes.DecimalType):
            return _decimal_value(value, field_type)
        if isinstance(field_type, (icetypes.LongType, icetypes.IntegerType)):
            if value == value.to_integral_value():
                return _integer_value(int(value), field_type)
        return _NOT_EXACT
    if isinstance(value, str):
        return value if isinstance(field_type, icetypes.StringType) else _NOT_EXACT
    if isinstance(value, bytes):
        return (
            value
            if isinstance(field_type, (icetypes.BinaryType, icetypes.FixedType))
            else _NOT_EXACT
        )
    if isinstance(value, _dt.datetime):
        if isinstance(field_type, icetypes.TimestamptzType):
            return value if value.tzinfo is not None else _NOT_EXACT
        if isinstance(field_type, icetypes.TimestampType):
            return value if value.tzinfo is None else _NOT_EXACT
        return _NOT_EXACT
    if isinstance(value, _dt.date):
        return value if isinstance(field_type, icetypes.DateType) else _NOT_EXACT
    return _NOT_EXACT


class _IcebergPredicate:
    """Translate a domain predicate into a pyiceberg row filter.

    The result is always a *relaxation* of the SQL predicate (it never drops a
    row SQL would keep); the accompanying flag says whether it is exact.
    ``NOT`` is pushed to the leaves (De Morgan) so the filter is built only from
    positive predicates whose null behaviour matches SQL. Anything pyiceberg
    cannot express exactly becomes ``AlwaysTrue`` and is marked inexact.
    """

    def __init__(self, schema: IcebergSchema, parameters: Mapping[str, Literal]) -> None:
        self._schema = schema
        self._parameters = parameters

    def translate(self, expression: Expression) -> tuple[BooleanExpression, bool]:
        return self._visit(expression, negated=False)

    def _visit(self, expression: Expression, *, negated: bool) -> tuple[BooleanExpression, bool]:
        if isinstance(expression, And):
            return self._compose(expression.operands, negated, ice.Or if negated else ice.And)
        if isinstance(expression, Or):
            return self._compose(expression.operands, negated, ice.And if negated else ice.Or)
        if isinstance(expression, Not):
            return self._visit(expression.operand, negated=not negated)
        if isinstance(expression, Comparison):
            return self._comparison(expression, negated)
        if isinstance(expression, IsNull):
            return self._is_null(expression, negated)
        if isinstance(expression, In):
            return self._in(expression, negated)
        return _RELAXED

    def _compose(
        self,
        operands: tuple[Expression, ...],
        negated: bool,
        combine: Callable[..., BooleanExpression],
    ) -> tuple[BooleanExpression, bool]:
        parts = [self._visit(operand, negated=negated) for operand in operands]
        return combine(*(part for part, _ in parts)), all(exact for _, exact in parts)

    def _field_type(self, name: str) -> icetypes.IcebergType | None:
        try:
            return self._schema.find_field(name, case_sensitive=True).field_type
        except ValueError:
            return None

    def _convert(self, column: str, value: Any) -> Any:
        field_type = self._field_type(column)
        if field_type is None:
            return _NOT_EXACT
        return _iceberg_value(value, field_type)

    def _comparison(self, expression: Comparison, negated: bool) -> tuple[BooleanExpression, bool]:
        op = expression.op
        left, right = expression.left, expression.right
        if isinstance(left, Column) and isinstance(right, (Literal, Parameter)):
            column, operand = left, right
        elif isinstance(right, Column) and isinstance(left, (Literal, Parameter)):
            column, operand = right, left
            op = _MIRROR[op]
        else:
            return _RELAXED
        if negated:
            op = _NEGATE[op]
        value = _parameter_value(operand, self._parameters)
        if value is None:
            return ice.AlwaysFalse(), True
        converted = self._convert(column.name, value)
        if converted is _NOT_EXACT:
            return _RELAXED
        return _ICEBERG_PREDICATES[op](column.name, converted), True

    def _is_null(self, expression: IsNull, negated: bool) -> tuple[BooleanExpression, bool]:
        wants_null = expression.negated == negated
        operand = expression.operand
        if isinstance(operand, (Literal, Parameter)):
            value = _parameter_value(operand, self._parameters)
            return (ice.AlwaysTrue() if (value is None) == wants_null else ice.AlwaysFalse()), True
        if not isinstance(operand, Column) or self._field_type(operand.name) is None:
            return _RELAXED
        return (ice.IsNull(operand.name) if wants_null else ice.NotNull(operand.name)), True

    def _in(self, expression: In, negated: bool) -> tuple[BooleanExpression, bool]:
        operand = expression.operand
        if not isinstance(operand, Column) or self._field_type(operand.name) is None:
            return _RELAXED
        excluded = expression.negated != negated
        values = [_parameter_value(v, self._parameters) for v in expression.values]
        has_null = any(v is None for v in values)
        converted = [self._convert(operand.name, v) for v in values if v is not None]
        if excluded:
            if has_null:
                return ice.AlwaysFalse(), True
            if any(c is _NOT_EXACT for c in converted):
                return _RELAXED
            return ice.And(ice.NotNull(operand.name), ice.NotIn(operand.name, set(converted))), True
        if any(c is _NOT_EXACT for c in converted):
            return _RELAXED
        if not converted:
            return ice.AlwaysFalse(), True
        return ice.In(operand.name, set(converted)), True


# -- Arrow re-check -------------------------------------------------------------

_ARROW_COMPARE: dict[ComparisonOp, Callable[[Any, Any], Any]] = {
    ComparisonOp.EQ: pc.equal,
    ComparisonOp.NE: pc.not_equal,
    ComparisonOp.LT: pc.less,
    ComparisonOp.LE: pc.less_equal,
    ComparisonOp.GT: pc.greater,
    ComparisonOp.GE: pc.greater_equal,
}


def _is_null_scalar(value: Any) -> bool:
    return isinstance(value, pa.Scalar) and not value.is_valid


class _ArrowPredicate:
    """Evaluate a domain predicate on record batches with SQL three-valued logic."""

    def __init__(self, predicate: Expression, parameters: Mapping[str, Literal]) -> None:
        self._predicate = predicate
        self._parameters = parameters

    def filter(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        try:
            mask = self._evaluate(self._predicate, batch)
        except InvariantQLError:
            raise
        except (pa.ArrowException, TypeError, ValueError, KeyError) as exc:
            raise SourceError(
                f"cannot evaluate predicate {self._predicate}: {redact_exception(exc)}",
                target=_FORMAT,
            ) from None
        if isinstance(mask, pa.Scalar):
            return batch if mask.is_valid and bool(mask.as_py()) else batch.slice(0, 0)
        return batch.filter(mask)

    def _unknown(self, batch: pa.RecordBatch) -> pa.Array:
        return pa.nulls(batch.num_rows, pa.bool_())

    def _evaluate(self, expression: Expression, batch: pa.RecordBatch) -> Any:
        if isinstance(expression, Column):
            return batch.column(expression.name)
        if isinstance(expression, Literal):
            return pa.scalar(expression.value, type=to_arrow_type(expression.data_type))
        if isinstance(expression, Parameter):
            _parameter_value(expression, self._parameters)
            return self._evaluate(self._parameters[expression.name], batch)
        if isinstance(expression, Comparison):
            left = self._evaluate(expression.left, batch)
            right = self._evaluate(expression.right, batch)
            if _is_null_scalar(left) or _is_null_scalar(right):
                return self._unknown(batch)
            return _ARROW_COMPARE[expression.op](left, right)
        if isinstance(expression, And):
            return reduce(pc.and_kleene, (self._evaluate(o, batch) for o in expression.operands))
        if isinstance(expression, Or):
            return reduce(pc.or_kleene, (self._evaluate(o, batch) for o in expression.operands))
        if isinstance(expression, Not):
            return pc.invert(self._evaluate(expression.operand, batch))
        if isinstance(expression, IsNull):
            operand = self._evaluate(expression.operand, batch)
            if _is_null_scalar(operand):
                return pa.scalar(not expression.negated)
            return pc.is_valid(operand) if expression.negated else pc.is_null(operand)
        if isinstance(expression, In):
            operand = self._evaluate(expression.operand, batch)
            if _is_null_scalar(operand):
                return self._unknown(batch)
            masks = []
            for value in expression.values:
                candidate = self._evaluate(value, batch)
                if _is_null_scalar(candidate):
                    masks.append(self._unknown(batch))
                else:
                    masks.append(pc.equal(operand, candidate))
            mask = reduce(pc.or_kleene, masks)
            return pc.invert(mask) if expression.negated else mask
        raise UnsupportedOperationError(
            f"Iceberg handler cannot evaluate {expression.kind.value} expressions",
            code=DiagnosticCode.FORMAT_UNSUPPORTED,
            target=_FORMAT,
            details={"expression": str(expression)},
        )


__all__ = ["IcebergLocalHandler", "IcebergReaderSpecHandler"]
