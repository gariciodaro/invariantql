"""Delta Lake format handlers (ADR-0004).

Locally, :class:`DeltaLocalHandler` opens a Delta table through delta-rs (the
``deltalake`` package) and hands the local engine an Arrow record-batch stream
produced by a ``pyarrow.dataset`` scanner. Distributed engines receive a
:class:`~invariantql.ports.format_handler.ReaderSpec` from
:class:`DeltaReaderSpecHandler` that describes Spark's native Delta reader.
"""

from __future__ import annotations

import datetime as _dt
import operator
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from urllib.request import url2pathname

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from invariantql.adapters._shared.arrow import (
    ArrowStream,
    from_arrow_schema,
    stream_from_batches,
    to_arrow_type,
)
from invariantql.domain.capabilities import PushdownCapabilities, Support
from invariantql.domain.diagnostics import (
    DiagnosticCode,
    InvariantQLError,
    ParameterError,
    SourceError,
    StorageError,
    UnsupportedOperationError,
)
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
    substitute_parameters,
)
from invariantql.domain.formats import DataFormat, DeltaFormat
from invariantql.domain.redaction import redact, redact_exception, register_secret
from invariantql.ports.format_handler import ReaderSpec

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

    from invariantql.domain.execution import PushedOperations
    from invariantql.domain.location import Location
    from invariantql.domain.schema import Schema
    from invariantql.ports.storage import Storage

_FORMAT_NAME = "delta"

# pyarrow.compute materialises most kernels (``if_else`` included) at import
# time, so static checkers cannot see them; go through an untyped alias.
_compute: Any = pc

SPARK_DELTA_REQUIREMENT = "io.delta:delta-spark_2.12 and the Delta Spark session extensions"

#: Expression kinds the delta-rs scanner evaluates with portable-profile semantics.
PUSHABLE_EXPRESSIONS: frozenset[ExpressionKind] = frozenset(
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

# Canonical storage-option keys (as storage adapters reveal them) mapped onto
# the delta-rs / object_store ``storage_options`` vocabulary.
_AZURE_OPTION_KEYS: dict[str, str] = {
    "account_name": "azure_storage_account_name",
    "account_key": "azure_storage_account_key",
    "sas_token": "azure_storage_sas_token",
    "client_id": "azure_client_id",
    "client_secret": "azure_client_secret",
    "tenant_id": "azure_tenant_id",
}
_S3_OPTION_KEYS: dict[str, str] = {
    "aws_access_key_id": "AWS_ACCESS_KEY_ID",
    "aws_secret_access_key": "AWS_SECRET_ACCESS_KEY",
    "aws_session_token": "AWS_SESSION_TOKEN",
    "aws_region": "AWS_REGION",
    "aws_endpoint_url": "AWS_ENDPOINT_URL",
    "aws_allow_http": "AWS_ALLOW_HTTP",
    "aws_anonymous": "AWS_SKIP_SIGNATURE",
}
_OPTION_KEYS: dict[str, str] = {**_AZURE_OPTION_KEYS, **_S3_OPTION_KEYS}
_DELTA_KEYS: frozenset[str] = frozenset(
    (*_OPTION_KEYS.values(), "azure_storage_endpoint", "azure_skip_signature")
)
_DELTA_KEYS_CASEFOLD: dict[str, str] = {key.lower(): key for key in _DELTA_KEYS}

_AZURE_SCHEMES = frozenset({"abfs", "abfss", "wasb", "wasbs"})
_S3_SCHEMES = frozenset({"s3a", "s3n"})

_COMPARISONS: dict[ComparisonOp, Callable[[Any, Any], Any]] = {
    ComparisonOp.EQ: operator.eq,
    ComparisonOp.NE: operator.ne,
    ComparisonOp.LT: operator.lt,
    ComparisonOp.LE: operator.le,
    ComparisonOp.GT: operator.gt,
    ComparisonOp.GE: operator.ge,
}


# -- URI and option mapping ---------------------------------------------------


def delta_table_uri(native_uri: str) -> tuple[str, dict[str, str]]:
    """Map a storage adapter's native URI onto a URI delta-rs can open.

    Returns the delta-rs URI and any non-secret storage options derived from
    it (the Azure account name when the URI carried one).

    * ``file:///path`` becomes the local filesystem path.
    * ``abfs[s]://`` and ``wasb[s]://container@account.<host>/path`` become
      ``az://container/path`` with ``azure_storage_account_name=account``.
    * ``s3a://`` and ``s3n://bucket/key`` become ``s3://bucket/key``.
    * Anything else (``az://``, ``s3://``, ``gs://``, ``https://`` ...) passes through.
    """

    if native_uri.startswith("file://"):
        return url2pathname(native_uri[len("file://") :]), {}
    parts = urlsplit(native_uri)
    scheme = parts.scheme.lower()
    if scheme in _AZURE_SCHEMES and "@" in parts.netloc:
        container, host = parts.netloc.split("@", 1)
        account = host.split(".", 1)[0]
        derived = {"azure_storage_account_name": account} if account else {}
        return f"az://{container}{parts.path}", derived
    if scheme in _S3_SCHEMES:
        return f"s3://{parts.netloc}{parts.path}", {}
    return native_uri, {}


def delta_storage_options(options: Mapping[str, Any]) -> dict[str, str]:
    """Translate revealed canonical storage options into delta-rs ``storage_options``.

    Only recognised keys are forwarded; keys already spelled the delta-rs way
    pass through unchanged. An ``http://`` S3 endpoint implies ``AWS_ALLOW_HTTP``.
    The result holds secrets: build it at the call site and never store or log it.
    """

    out: dict[str, str] = {}
    normalised = {str(key).lower(): value for key, value in options.items()}
    for key, value in options.items():
        if value is None:
            continue
        target = _OPTION_KEYS.get(key.lower())
        if target is None:
            target = _DELTA_KEYS_CASEFOLD.get(key.lower())
            if target is None:
                continue
        out[target] = _option_text(value)

    connection = normalised.get("connection_string")
    connection_fields = _connection_string_fields(connection)
    for field, target in (
        ("accountname", "azure_storage_account_name"),
        ("accountkey", "azure_storage_account_key"),
        ("sharedaccesssignature", "azure_storage_sas_token"),
    ):
        value = connection_fields.get(field)
        if value:
            value = value.lstrip("?") if field == "sharedaccesssignature" else value
            out.setdefault(target, value)
            if field != "accountname":
                register_secret(value)

    if normalised.get("credential_kind") == "anonymous":
        out.setdefault("azure_skip_signature", "true")

    endpoint = connection_fields.get("blobendpoint")
    account = out.get("azure_storage_account_name")
    suffix = normalised.get("endpoint_suffix") or connection_fields.get("endpointsuffix")
    if not endpoint and account and isinstance(suffix, str) and suffix.strip("."):
        protocol = connection_fields.get("defaultendpointsprotocol", "https")
        if protocol.lower() in {"http", "https"}:
            endpoint = f"{protocol.lower()}://{account}.blob.{suffix.strip('.')}"
    if endpoint:
        out.setdefault("azure_storage_endpoint", endpoint)
        if "?" in endpoint:
            register_secret(endpoint)

    endpoint = out.get("AWS_ENDPOINT_URL", "")
    if endpoint.lower().startswith("http://"):
        out.setdefault("AWS_ALLOW_HTTP", "true")
    return out


def _connection_string_fields(value: Any) -> dict[str, str]:
    """Parse the non-escaped key/value grammar used by Azure connection strings."""

    if not isinstance(value, str):
        return {}
    fields: dict[str, str] = {}
    for part in value.split(";"):
        key, separator, setting = part.partition("=")
        if separator and key.strip() and setting.strip():
            fields[key.strip().lower()] = setting.strip()
    return fields


def _option_text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _parse_timestamp(text: str) -> _dt.datetime | str:
    """ISO 8601 text to an aware datetime (naive means UTC); unparsable text is passed to delta-rs."""

    try:
        parsed = _dt.datetime.fromisoformat(text.strip().replace("Z", "+00:00"))
    except ValueError:
        return text
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=_dt.timezone.utc)


# -- predicate translation ---------------------------------------------------


def translate_predicate(
    expression: Expression,
    schema: pa.Schema,
    parameters: Mapping[str, Literal] | None = None,
) -> pc.Expression:
    """Translate a pushed predicate into a ``pyarrow.compute`` expression.

    Parameters are substituted first; every remaining node must be one of
    :data:`PUSHABLE_EXPRESSIONS`. The translation keeps SQL three-valued logic.
    """

    return _FilterTranslator(schema).translate(_bind(expression, parameters or {}))


def _bind(expression: Expression, parameters: Mapping[str, Literal]) -> Expression:
    try:
        return substitute_parameters(expression, dict(parameters))
    except KeyError as exc:
        name = exc.args[0] if exc.args else "?"
        raise ParameterError(
            f"missing parameter {name!r}",
            code=DiagnosticCode.PARAMETER_MISSING,
            details={"parameter": str(name)},
        ) from None


class _FilterTranslator:
    """Domain predicate -> ``pyarrow.compute.Expression`` against a dataset schema."""

    def __init__(self, schema: pa.Schema) -> None:
        self._schema = schema

    def translate(self, expression: Expression) -> pc.Expression:
        if isinstance(expression, Column):
            return pc.field(expression.name)
        if isinstance(expression, Literal):
            return pc.scalar(self._scalar(expression, None))
        if isinstance(expression, Parameter):
            raise ParameterError(
                f"unbound parameter {expression.name!r}",
                code=DiagnosticCode.PARAMETER_MISSING,
                details={"parameter": expression.name},
            )
        if isinstance(expression, Comparison):
            return self._comparison(expression)
        if isinstance(expression, And):
            return self._fold(operator.and_, expression.operands)
        if isinstance(expression, Or):
            return self._fold(operator.or_, expression.operands)
        if isinstance(expression, Not):
            return ~self.translate(expression.operand)
        if isinstance(expression, IsNull):
            operand = self.translate(expression.operand)
            return operand.is_valid() if expression.negated else operand.is_null()
        if isinstance(expression, In):
            return self._in(expression)
        raise UnsupportedOperationError(
            f"the delta-rs scanner cannot evaluate {expression.kind.value} expressions; "
            "the planner must keep them residual",
            code=DiagnosticCode.FORMAT_UNSUPPORTED,
            target=_FORMAT_NAME,
            details={"expression": expression.kind.value},
        )

    def _fold(
        self, op: Callable[[Any, Any], Any], operands: tuple[Expression, ...]
    ) -> pc.Expression:
        out = self.translate(operands[0])
        for operand in operands[1:]:
            out = op(out, self.translate(operand))
        return out

    def _comparison(self, expression: Comparison) -> pc.Expression:
        left, right = expression.left, expression.right
        if isinstance(left, Column) and isinstance(right, Literal):
            lhs = self.translate(left)
            rhs = pc.scalar(self._scalar(right, self._type_of(left)))
        elif isinstance(left, Literal) and isinstance(right, Column):
            lhs = pc.scalar(self._scalar(left, self._type_of(right)))
            rhs = self.translate(right)
        else:
            lhs, rhs = self.translate(left), self.translate(right)
        return _COMPARISONS[expression.op](lhs, rhs)

    def _in(self, expression: In) -> pc.Expression:
        operand = self.translate(expression.operand)
        target = (
            self._type_of(expression.operand) if isinstance(expression.operand, Column) else None
        )
        scalars = [self._scalar(self._literal(value), target) for value in expression.values]
        present = [s for s in scalars if s.is_valid]
        has_null = len(present) < len(scalars)
        null_bool = pc.scalar(pa.scalar(None, pa.bool_()))
        value_set = _value_set(present)
        if value_set is None:
            # Heterogeneous or all-NULL value list: an OR chain of equalities has
            # exact SQL semantics (``x == NULL`` is unknown, as is ``NULL == v``).
            chain: pc.Expression | None = None
            for scalar in scalars:
                term = operand == pc.scalar(scalar)
                chain = term if chain is None else chain | term
            result = null_bool if chain is None else chain
        else:
            membership = operand.isin(value_set)
            if has_null:
                # ``x IN (1, NULL)``: true on a match, otherwise unknown.
                result = _compute.if_else(membership, pc.scalar(True), null_bool)
            else:
                # A NULL operand is unknown, not false (``is_in`` would say false).
                result = _compute.if_else(operand.is_valid(), membership, null_bool)
        return ~result if expression.negated else result

    @staticmethod
    def _literal(expression: Expression) -> Literal:
        if isinstance(expression, Literal):
            return expression
        if isinstance(expression, Parameter):
            raise ParameterError(
                f"unbound parameter {expression.name!r}",
                code=DiagnosticCode.PARAMETER_MISSING,
                details={"parameter": expression.name},
            )
        raise UnsupportedOperationError(
            "IN values must be literals or parameters",
            code=DiagnosticCode.FORMAT_UNSUPPORTED,
            target=_FORMAT_NAME,
        )

    def _type_of(self, expression: Expression) -> pa.DataType | None:
        if not isinstance(expression, Column):
            return None
        index = self._schema.get_field_index(expression.name)
        return None if index < 0 else self._schema.field(index).type

    @staticmethod
    def _scalar(literal: Literal, target: pa.DataType | None) -> pa.Scalar:
        value = literal.value
        if value is None:
            return pa.scalar(None, type=target if target is not None else pa.null())
        if target is not None and _should_cast(value, target):
            try:
                return pa.scalar(value, type=target)
            except (pa.ArrowException, ValueError, TypeError, OverflowError):
                pass
        try:
            return pa.scalar(value, type=to_arrow_type(literal.data_type))
        except (pa.ArrowException, ValueError, TypeError, OverflowError):
            return pa.scalar(value)


def _should_cast(value: Any, target: pa.DataType) -> bool:
    """Cast a literal to the column type only when Arrow cannot compare natively.

    Integer and decimal literals against decimal columns need the column's
    scale; timestamp literals need the column's time zone. Floats are never
    cast (``pa.scalar(2.5, int64)`` would silently truncate).
    """

    if isinstance(value, bool):
        return False
    if isinstance(value, (int, Decimal)):
        return pa.types.is_decimal(target)
    if isinstance(value, _dt.datetime):
        return pa.types.is_timestamp(target)
    return False


def _value_set(scalars: list[pa.Scalar]) -> pa.Array | None:
    if not scalars:
        return None
    first = scalars[0].type
    if any(not scalar.type.equals(first) for scalar in scalars):
        return None
    try:
        return pa.array([scalar.as_py() for scalar in scalars], type=first)
    except (pa.ArrowException, ValueError, TypeError):
        return None


# -- handlers ------------------------------------------------------------------


class DeltaLocalHandler:
    """Read Delta Lake tables locally through delta-rs (the ``deltalake`` package).

    **Constructor options.** None. Everything the reader needs comes from the
    :class:`~invariantql.domain.formats.DeltaFormat` (``version`` or
    ``timestamp`` for time travel) and from the ``Storage`` the file source
    composes.

    **Storage and credentials.** delta-rs opens the transaction log and the data
    files itself, so the storage adapter must expose an engine-visible URI
    (``Storage.native_uri``); storage without one raises ``FORMAT_UNSUPPORTED``.
    ``file://`` URIs are read from the local filesystem; ``abfs[s]://`` and
    ``wasb[s]://container@account.<host>/path`` become ``az://container/path``
    with the account name as ``azure_storage_account_name``; ``s3a://`` and
    ``s3n://`` become ``s3://``. Credentials are taken from
    ``Storage.native_options()`` at open time only: the canonical keys
    ``account_name``, ``account_key``, ``sas_token``, ``client_id``,
    ``client_secret``, ``tenant_id``, ``aws_access_key_id``,
    ``aws_secret_access_key``, ``aws_session_token``, ``aws_region``,
    ``aws_endpoint_url``, ``aws_allow_http`` and ``aws_anonymous`` map onto
    delta-rs ``storage_options``. Azure connection strings are reduced to the
    account/key-or-SAS/BlobEndpoint fields delta-rs understands, and
    ``endpoint_suffix`` preserves sovereign-cloud endpoints. The handler stores
    no credential, and provider errors are redacted before they become
    ``SourceError``/``StorageError``.

    **Semantics.** Column projection and predicates run inside a
    ``pyarrow.dataset`` scanner with the portable profile's semantics: SQL
    three-valued logic (a comparison involving NULL is unknown, ``NOT`` of
    unknown stays unknown, both exclude the row); byte-wise, case-sensitive
    string comparison; ``IN`` as SQL evaluates it (a NULL operand, or no match
    against a list containing NULL, is unknown). A literal compared to a column
    is cast to the column's Arrow type only when that is lossless and required
    (integers and decimals against decimal columns, timestamps for time-zone
    alignment); floats are never truncated to integers. ``LIKE``, arithmetic
    and ``LIMIT`` are not declared, so the engine evaluates them on the stream.
    Time travel: ``version`` loads that exact version; ``timestamp`` loads the
    last version committed at or before the instant (ISO 8601; naive text is
    read as UTC; an instant before the first commit yields version 0). Tables
    that need column mapping or deletion vectors are rejected by delta-rs and
    surface as ``SourceError``.

    The distributed counterpart is :class:`DeltaReaderSpecHandler`.
    """

    @property
    def format_name(self) -> str:
        return _FORMAT_NAME

    def capabilities(self, data_format: DataFormat) -> PushdownCapabilities:
        _as_delta(data_format)
        return PushdownCapabilities(
            projection=Support.FULL,
            predicate=Support.FULL,
            limit=Support.NONE,
            expressions=PUSHABLE_EXPRESSIONS,
            parameters=True,
            evidence=(
                "delta-rs pyarrow dataset scanner: column projection and comparison/boolean/"
                "IS NULL/IN predicates evaluated inside the scan with SQL NULL semantics; "
                "partition and Parquet statistics prune files where possible",
                "LIKE, arithmetic and LIMIT are not translated and stay residual",
            ),
        )

    def schema(self, storage: Storage, location: Location, data_format: DataFormat) -> Schema:
        dataset = self._dataset(
            storage, location, _as_delta(data_format), code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE
        )
        return from_arrow_schema(dataset.schema)

    def scan(
        self,
        storage: Storage,
        location: Location,
        data_format: DataFormat,
        pushed: PushedOperations,
        parameters: Mapping[str, Literal],
        *,
        batch_size: int,
    ) -> ArrowStream:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        dataset = self._dataset(storage, location, _as_delta(data_format))
        predicate = None
        if pushed.predicate is not None:
            predicate = translate_predicate(pushed.predicate, dataset.schema, parameters)
        columns = list(pushed.projection) if pushed.projection is not None else None
        try:
            scanner = dataset.scanner(columns=columns, filter=predicate, batch_size=batch_size)
            reader = scanner.to_reader()
        except InvariantQLError:
            raise
        except Exception as exc:  # provider error translated at the edge
            raise SourceError(
                f"delta-rs could not scan {redact(location.uri)}: {redact_exception(exc)}",
                target=_FORMAT_NAME,
                details={"format": _FORMAT_NAME},
            ) from None
        return stream_from_batches(
            reader.schema,
            _batches(reader, pushed.limit, location),
            on_close=(reader.close,),
        )

    # -- helpers ------------------------------------------------------------

    def _dataset(
        self,
        storage: Storage,
        location: Location,
        data_format: DeltaFormat,
        *,
        code: DiagnosticCode = DiagnosticCode.SOURCE_FAILURE,
    ) -> Any:
        table = self._open(storage, location, data_format, code=code)
        try:
            return table.to_pyarrow_dataset()
        except Exception as exc:
            raise SourceError(
                f"delta-rs cannot expose Delta table {redact(location.uri)} as an Arrow dataset: "
                f"{redact_exception(exc)}",
                code=code,
                target=_FORMAT_NAME,
                details={"format": _FORMAT_NAME},
            ) from None

    def _open(
        self,
        storage: Storage,
        location: Location,
        data_format: DeltaFormat,
        *,
        code: DiagnosticCode,
    ) -> DeltaTable:
        native = storage.native_uri(location)
        if native is None:
            raise UnsupportedOperationError(
                f"storage {storage.name!r} exposes no native URI for {redact(location.uri)}; "
                "delta-rs must open the Delta log and data files itself and needs a URI it "
                "can reach (file://, az://, s3:// ...). Stage the table or use a storage "
                "adapter with an engine-visible URI",
                code=DiagnosticCode.FORMAT_UNSUPPORTED,
                target=_FORMAT_NAME,
                details={"format": _FORMAT_NAME, "storage": storage.name},
            )
        uri, derived = delta_table_uri(native)
        # Secrets live only in this local mapping for the duration of the call.
        options = {**derived, **delta_storage_options(storage.native_options().reveal())}
        try:
            table = DeltaTable(uri, version=data_format.version, storage_options=options or None)
            if data_format.timestamp is not None:
                table.load_as_version(_parse_timestamp(data_format.timestamp))
        except TableNotFoundError as exc:
            raise StorageError(
                f"Delta table not found at {redact(location.uri)}: {redact_exception(exc)}",
                code=DiagnosticCode.STORAGE_OBJECT_NOT_FOUND,
                target=_FORMAT_NAME,
                details={"format": _FORMAT_NAME},
            ) from None
        except Exception as exc:  # provider error translated at the edge
            raise SourceError(
                f"cannot open Delta table {redact(location.uri)} ({_describe(data_format)}): "
                f"{redact_exception(exc)}",
                code=code,
                target=_FORMAT_NAME,
                details={"format": _FORMAT_NAME, **_time_travel_details(data_format)},
            ) from None
        return table

    def __repr__(self) -> str:
        return "DeltaLocalHandler()"


class DeltaReaderSpecHandler:
    """Describe Spark's native Delta reader for a ``DeltaFormat``.

    Produces ``spark.read.format("delta")`` with ``versionAsOf`` or
    ``timestampAsOf`` when the format pins a version or timestamp. The cluster
    needs the ``io.delta:delta-spark_2.12`` package (``_2.13`` on a Scala 2.13
    build) and the Delta session extensions
    (``spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension``,
    ``spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog``).
    The spec carries no credentials; apply storage credentials explicitly with
    ``SparkEngine.apply_storage_credentials``.
    """

    @property
    def format_name(self) -> str:
        return _FORMAT_NAME

    def reader_spec(self, data_format: DataFormat, uri: str) -> ReaderSpec:
        fmt = _as_delta(data_format)
        options: dict[str, str] = {}
        if fmt.version is not None:
            options["versionAsOf"] = str(fmt.version)
        elif fmt.timestamp is not None:
            options["timestampAsOf"] = fmt.timestamp
        return ReaderSpec(_FORMAT_NAME, options, None, requires=(SPARK_DELTA_REQUIREMENT,))

    def __repr__(self) -> str:
        return "DeltaReaderSpecHandler()"


# -- module helpers -----------------------------------------------------------


def _as_delta(data_format: DataFormat) -> DeltaFormat:
    if isinstance(data_format, DeltaFormat):
        return data_format
    raise UnsupportedOperationError(
        f"Delta handlers accept DeltaFormat, got {data_format.format_name!r}",
        code=DiagnosticCode.FORMAT_INVALID,
        target=_FORMAT_NAME,
        details={"format": data_format.format_name},
    )


def _describe(data_format: DeltaFormat) -> str:
    if data_format.version is not None:
        return f"version {data_format.version}"
    if data_format.timestamp is not None:
        return f"as of {data_format.timestamp}"
    return "latest version"


def _time_travel_details(data_format: DeltaFormat) -> dict[str, str]:
    if data_format.version is not None:
        return {"version": str(data_format.version)}
    if data_format.timestamp is not None:
        return {"timestamp": data_format.timestamp}
    return {}


def _batches(
    reader: pa.RecordBatchReader, limit: int | None, location: Location
) -> Iterator[pa.RecordBatch]:
    """Yield scanner batches, honouring a pushed limit defensively and wrapping Arrow errors."""

    remaining = limit
    try:
        for batch in reader:
            if remaining is None:
                yield batch
                continue
            if remaining <= 0:
                return
            if batch.num_rows > remaining:
                batch = batch.slice(0, remaining)
            remaining -= batch.num_rows
            yield batch
    except pa.ArrowException as exc:
        raise SourceError(
            f"delta-rs scan of {redact(location.uri)} failed: {redact_exception(exc)}",
            target=_FORMAT_NAME,
            details={"format": _FORMAT_NAME},
        ) from None


__all__ = [
    "PUSHABLE_EXPRESSIONS",
    "SPARK_DELTA_REQUIREMENT",
    "DeltaLocalHandler",
    "DeltaReaderSpecHandler",
    "delta_storage_options",
    "delta_table_uri",
    "translate_predicate",
]
