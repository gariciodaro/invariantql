"""PostgreSQL native source through psycopg 3 (ADR-0003, ADR-0004, ADR-0010).

The source compiles the planner's pushed operations into one ``SELECT`` that
PostgreSQL executes natively and streams the rows back as Arrow batches through
a server-side cursor. Every literal and parameter is bound through psycopg's
``%s`` placeholders; only identifiers are rendered as text, quoted.
"""

from __future__ import annotations

import datetime as _dt
import json
import threading
import uuid
from collections.abc import Callable, Iterator, Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import psycopg

from invariantql.adapters._shared.arrow import ArrowStream, stream_from_rows, to_arrow_schema
from invariantql.adapters._shared.sqltext import POSTGRES, SqlGenerator
from invariantql.domain.capabilities import PushdownCapabilities, Support
from invariantql.domain.credentials import CredentialRef, SecretOptions
from invariantql.domain.diagnostics import DiagnosticCode, SourceError
from invariantql.domain.expressions import (
    ALL_EXPRESSION_KINDS,
    Arithmetic,
    ArithmeticOp,
    Expression,
    ExpressionKind,
    Like,
    Literal,
    substitute_parameters,
)
from invariantql.domain.redaction import redact_exception
from invariantql.domain.schema import Field, Schema
from invariantql.domain.semantics import expression_type
from invariantql.domain.types import (
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    FloatType,
    IntegerType,
    ListType,
    StringType,
    TimestampType,
    UnknownType,
)
from invariantql.ports.source import NativeRelation

if TYPE_CHECKING:
    from invariantql.domain.execution import PushedOperations

_RELATION_KIND = "jdbc:postgresql"
_JDBC_DRIVER = "org.postgresql.Driver"

_SCHEMA_SQL = (
    "SELECT column_name, data_type, udt_name, numeric_precision, numeric_scale, is_nullable "
    "FROM information_schema.columns "
    "WHERE table_schema = %s AND table_name = %s "
    "ORDER BY ordinal_position"
)

# Unconstrained ``numeric`` has no declared precision/scale. Spark's JDBC reader
# maps it to DecimalType(38, 18); we do the same so both engines agree.
_UNCONSTRAINED_DECIMAL = DecimalType(38, 18)

_STRING_UDTS = frozenset(
    {"text", "varchar", "bpchar", "char", "name", "uuid", "json", "jsonb", "xml", "citext"}
)
_PUSHED_EXPRESSIONS = ALL_EXPRESSION_KINDS - {ExpressionKind.ARITHMETIC}


class PostgresSqlGenerator(SqlGenerator):
    """PostgreSQL SQL with portable ``LIKE`` and numeric arithmetic.

    PostgreSQL treats a backslash in a ``LIKE`` pattern as the default escape
    character; the portable profile has no escape character at all (DuckDB
    matches ``'a\\b' LIKE 'a\\b'``). Rendering ``ESCAPE ''`` disables the
    escape mechanism so the pushed predicate keeps the portable semantics.
    Numeric operands are cast before arithmetic. This is important for a
    small integer bound through psycopg: PostgreSQL otherwise infers the
    narrowest wire type that fits the Python value, even though the logical
    literal type is always int64. Division uses double precision and turns a
    zero denominator into NULL, matching the portable engine semantics.
    """

    def __init__(
        self,
        dialect: Any = POSTGRES,
        parameters: Mapping[str, Literal] | None = None,
        *,
        schema: Schema | None = None,
    ) -> None:
        super().__init__(dialect, parameters)
        self.schema = schema or Schema()

    def expression(self, expression: Expression) -> str:
        if isinstance(expression, Like):
            operand = self.expression(expression.operand)
            pattern = self.expression(expression.pattern)
            negated = "NOT " if expression.negated else ""
            return f"({operand} {negated}{self.dialect.like_operator} {pattern} ESCAPE '')"
        if isinstance(expression, Arithmetic):
            return self._arithmetic(expression)
        return super().expression(expression)

    def _arithmetic(self, expression: Arithmetic) -> str:
        # Render first so a missing Parameter follows SqlGenerator's public
        # ParameterError contract rather than leaking substitute_parameters'
        # internal KeyError.
        left = self.expression(expression.left)
        right = self.expression(expression.right)
        typed = substitute_parameters(expression, self.parameters)
        if not isinstance(typed, Arithmetic):  # pragma: no cover - structural invariant
            raise TypeError("arithmetic substitution changed the expression kind")
        left_type = expression_type(typed.left, self.schema)
        right_type = expression_type(typed.right, self.schema)
        result_type = expression_type(typed, self.schema)

        if expression.op is ArithmeticOp.DIV:
            # PostgreSQL raises on floating-point division by zero. NULLIF is
            # side-effect free here because the portable language has no
            # volatile expression nodes.
            return (
                f"(CAST({left} AS DOUBLE PRECISION) / "
                f"NULLIF(CAST({right} AS DOUBLE PRECISION), 0.0))"
            )

        target = _postgres_numeric_type(result_type)
        if target is None:
            return f"({left} {expression.op.value} {right})"

        if isinstance(result_type, DecimalType):
            left_target = f"NUMERIC({result_type.precision},{_decimal_scale(left_type)})"
            right_target = f"NUMERIC({result_type.precision},{_decimal_scale(right_type)})"
        else:
            # In particular, int32 + an int64 literal and float32 + an exact
            # numeric must widen before PostgreSQL selects an operator.
            left_target = right_target = target
        left = f"CAST({left} AS {left_target})"
        right = f"CAST({right} AS {right_target})"
        return f"CAST(({left} {expression.op.value} {right}) AS {target})"


def _postgres_numeric_type(data_type: DataType) -> str | None:
    if isinstance(data_type, IntegerType):
        return {8: "SMALLINT", 16: "SMALLINT", 32: "INTEGER", 64: "BIGINT"}[data_type.bits]
    if isinstance(data_type, FloatType):
        return "REAL" if data_type.bits == 32 else "DOUBLE PRECISION"
    if isinstance(data_type, DecimalType):
        return f"NUMERIC({data_type.precision},{data_type.scale})"
    return None


def _decimal_scale(data_type: DataType) -> int:
    return data_type.scale if isinstance(data_type, DecimalType) else 0


class PostgresSource:
    """A PostgreSQL table (or view) as a native, pushdown-capable source.

    Constructor options
    -------------------
    ``name``
        The registry name of the source.
    ``host``, ``port`` (5432), ``database``, ``user``, ``password`` (``None``)
        libpq connection parameters. When ``password`` is ``None`` libpq falls
        back to ``PGPASSWORD``/``~/.pgpass``.
    ``table``, ``schema`` (``"public"``)
        The relation to scan; both identifiers are always double-quoted, so
        pass them exactly as they are defined (case included).
    ``sslmode`` (``None``), ``connect_timeout`` (10 s), ``application_name``
        Forwarded to libpq.
    ``connection`` (``None``)
        An existing :class:`psycopg.Connection`. When supplied, the source
        never opens or closes a connection of its own and never commits or
        rolls back: scans run inside whatever transaction the connection is in
        (a ``WITH HOLD`` cursor is used when the connection is in autocommit
        mode). Without it, the source connects lazily on first use, releases
        its connection when a scan's stream is closed, and reconnects on the
        next call; :meth:`close` closes it for good.

    Credential handling
    -------------------
    ``user`` and ``password`` are held privately and never appear in ``repr``,
    diagnostics, or plans. They reach external engines only through the
    :class:`~invariantql.domain.credentials.SecretOptions` of the
    :class:`~invariantql.ports.source.NativeRelation`, which redacts on
    display and registers the values with the redaction service so provider
    error text echoing them is scrubbed. Provider exceptions are translated
    into :class:`~invariantql.domain.diagnostics.SourceError` at the boundary.

    Semantics
    ---------
    The pushed ``SELECT`` follows the portable profile: three-valued ``NULL``
    logic, case-sensitive string comparison, ``LIKE`` with ``%``/``_`` and no
    escape character (rendered as ``ESCAPE ''``), floating-point ``/`` (the
    ``IN`` with bound values. Arithmetic predicates are deliberately left to
    the engine: PostgreSQL cannot turn every int64 overflow into the portable
    NULL result. Known deviations that cannot be compensated here:

    * columns declared ``citext`` or with a non-deterministic collation compare
      case-insensitively in PostgreSQL; declare such columns as ``text`` for
      portable results.

    Type mapping (``information_schema.columns``): ``int2/int4/int8`` ->
    integers, ``float4/float8`` -> floats, ``numeric(p,s)`` -> decimal
    (unconstrained ``numeric`` -> ``decimal(38,18)``), ``bool`` -> boolean,
    ``text/varchar/char/uuid/json/jsonb/xml`` -> string (``json``/``jsonb`` are
    re-serialised as JSON text, ``uuid`` as its canonical string), ``bytea`` ->
    binary, ``date`` -> date, ``timestamp`` -> naive timestamp, ``timestamptz``
    -> ``timestamp[UTC]`` (values are converted to UTC), arrays -> lists of the
    element type, everything else -> unknown (rendered as text).

    Spark
    -----
    :meth:`relation` describes a ``jdbc:postgresql`` reader; the Spark cluster
    needs the ``org.postgresql:postgresql`` JDBC driver jar on its classpath.
    """

    def __init__(
        self,
        name: str,
        *,
        host: str,
        database: str,
        table: str,
        user: str,
        port: int = 5432,
        schema: str = "public",
        password: str | None = None,
        sslmode: str | None = None,
        connect_timeout: int = 10,
        application_name: str = "invariantql",
        connection: psycopg.Connection[Any] | None = None,
    ) -> None:
        if not name:
            raise ValueError("source name must not be empty")
        if not table:
            raise ValueError("table must not be empty")
        if not schema:
            raise ValueError("schema must not be empty")
        self._name = name
        self._host = host
        self._port = int(port)
        self._database = database
        self._schema_name = schema
        self._table = table
        self._user = user
        self._password = password
        self._sslmode = sslmode
        self._connect_timeout = int(connect_timeout)
        self._application_name = application_name
        self._connection = connection
        self._owns_connection = connection is None
        self._closed = False
        self._lock = threading.RLock()
        self._cached_schema: Schema | None = None
        self._streams: set[ArrowStream] = set()
        secret_values: dict[str, Any] = {"user": user}
        if password is not None:
            secret_values["password"] = password
        self._secrets = SecretOptions(secret_values, ref=CredentialRef(f"postgres:{name}"))

    # -- identity -----------------------------------------------------------

    @property
    def name(self) -> str:
        return self._name

    @property
    def relation_sql(self) -> str:
        """The quoted ``"schema"."table"`` reference used in every query."""

        return f"{POSTGRES.quote(self._schema_name)}.{POSTGRES.quote(self._table)}"

    # -- DataSource port ----------------------------------------------------

    def schema(self) -> Schema:
        with self._lock:
            if self._cached_schema is not None:
                return self._cached_schema
            connection = self._connect()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(_SCHEMA_SQL, (self._schema_name, self._table))
                    rows = cursor.fetchall()
                if self._owns_connection:
                    connection.rollback()
            except psycopg.Error as exc:
                self._release_connection()
                raise SourceError(
                    f"could not read the schema of {self.relation_sql}: {redact_exception(exc)}",
                    code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
                    target=self._name,
                ) from None
            if not rows:
                if self._owns_connection:
                    self._release_connection()
                raise SourceError(
                    f"relation {self.relation_sql} was not found in database {self._database!r}",
                    code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
                    target=self._name,
                )
            self._cached_schema = schema_from_information_schema(rows)
            return self._cached_schema

    def capabilities(self) -> PushdownCapabilities:
        return PushdownCapabilities(
            projection=Support.FULL,
            predicate=Support.FULL,
            limit=Support.FULL,
            expressions=_PUSHED_EXPRESSIONS,
            parameters=True,
            evidence=(
                "PostgreSQL executes safe pushed SELECT operations natively",
                "LIKE is rendered with ESCAPE ''; arithmetic remains engine-residual so int64 "
                "overflow consistently becomes NULL",
            ),
        )

    def relation(self) -> NativeRelation:
        options = {
            "url": f"jdbc:postgresql://{self._host}:{self._port}/{self._database}",
            "dbtable": self.relation_sql,
            "driver": _JDBC_DRIVER,
        }
        if self._sslmode:
            options["sslmode"] = self._sslmode
        return NativeRelation(_RELATION_KIND, options, self._secrets)

    def scan(
        self,
        pushed: PushedOperations,
        parameters: Mapping[str, Literal],
        *,
        batch_size: int,
    ) -> ArrowStream:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        full_schema = self.schema()
        columns = tuple(pushed.projection) if pushed.projection else None
        if columns is not None:
            try:
                scan_schema = full_schema.select(columns)
            except KeyError as exc:
                raise SourceError(
                    f"unknown column {exc.args[0]!r} in relation {self.relation_sql}",
                    code=DiagnosticCode.SOURCE_FAILURE,
                    target=self._name,
                ) from None
        else:
            scan_schema = full_schema

        generator = PostgresSqlGenerator(POSTGRES, parameters, schema=full_schema)
        sql = generator.select(
            self.relation_sql,
            columns=columns,
            predicate=pushed.predicate,
            limit=pushed.limit,
        )
        values = list(generator.values)

        with self._lock:
            connection = self._take_scan_connection()
            cursor: Any = None
            try:
                cursor = connection.cursor(
                    name=f"invariantql_{uuid.uuid4().hex}",
                    scrollable=False,
                    withhold=bool(getattr(connection, "autocommit", False)),
                )
                cursor.itersize = batch_size
                cursor.execute(sql, values)
            except psycopg.Error as exc:
                self._close_quietly(cursor)
                if self._owns_connection:
                    self._close_quietly(connection)
                raise SourceError(
                    f"PostgreSQL rejected the scan of {self.relation_sql}: {redact_exception(exc)}",
                    code=DiagnosticCode.SOURCE_FAILURE,
                    target=self._name,
                    details={"sql": sql},
                ) from None
            except BaseException:
                self._close_quietly(cursor)
                if self._owns_connection:
                    self._close_quietly(connection)
                raise

        converters = [(field.name, _converter(field.data_type)) for field in scan_schema]

        def rows() -> Iterator[dict[str, Any]]:
            try:
                for record in cursor:
                    yield {
                        name: (None if value is None else convert(value))
                        for (name, convert), value in zip(converters, record, strict=True)
                    }
            except psycopg.Error as exc:
                raise SourceError(
                    f"PostgreSQL failed while streaming {self.relation_sql}: {redact_exception(exc)}",
                    code=DiagnosticCode.SOURCE_FAILURE,
                    target=self._name,
                ) from None

        holder: list[ArrowStream] = []

        def release() -> None:
            with self._lock:
                if holder:
                    self._streams.discard(holder[0])
                self._close_quietly(cursor)
                if self._owns_connection:
                    self._close_quietly(connection)

        stream = stream_from_rows(
            to_arrow_schema(scan_schema), rows(), batch_size=batch_size, on_close=[release]
        )
        with self._lock:
            if self._closed:
                stream.close()
                raise SourceError(
                    f"source {self._name!r} is closed",
                    code=DiagnosticCode.SOURCE_FAILURE,
                    target=self._name,
                )
            holder.append(stream)
            self._streams.add(stream)
        return stream

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            streams = list(self._streams)
            self._streams.clear()
            self._release_connection()
        for stream in streams:
            try:
                stream.close()
            except Exception:
                pass

    # -- connection lifecycle -----------------------------------------------

    def _connect(self) -> psycopg.Connection[Any]:
        if self._closed:
            raise SourceError(
                f"source {self._name!r} is closed",
                code=DiagnosticCode.SOURCE_FAILURE,
                target=self._name,
            )
        connection = self._connection
        if connection is not None and not getattr(connection, "closed", False):
            return connection
        if not self._owns_connection:
            raise SourceError(
                f"the connection supplied to source {self._name!r} is closed",
                code=DiagnosticCode.SOURCE_FAILURE,
                target=self._name,
            )
        connection = self._open_connection()
        self._connection = connection
        return connection

    def _open_connection(self) -> psycopg.Connection[Any]:
        try:
            return psycopg.connect(**self._connect_kwargs())
        except Exception as exc:
            raise SourceError(
                f"could not connect to PostgreSQL at {self._host}:{self._port}/{self._database}: "
                f"{redact_exception(exc)}",
                code=DiagnosticCode.SOURCE_FAILURE,
                target=self._name,
            ) from None

    def _take_scan_connection(self) -> psycopg.Connection[Any]:
        """Give an owned scan its own connection; injected connections remain shared."""

        if not self._owns_connection:
            return self._connect()
        if self._closed:
            raise SourceError(
                f"source {self._name!r} is closed",
                code=DiagnosticCode.SOURCE_FAILURE,
                target=self._name,
            )
        connection, self._connection = self._connection, None
        if connection is not None and not getattr(connection, "closed", False):
            return connection
        return self._open_connection()

    def _connect_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "host": self._host,
            "port": self._port,
            "dbname": self._database,
            "user": self._user,
            "connect_timeout": self._connect_timeout,
            "application_name": self._application_name,
        }
        if self._password is not None:
            kwargs["password"] = self._password
        if self._sslmode is not None:
            kwargs["sslmode"] = self._sslmode
        return kwargs

    def _release_connection(self) -> None:
        """Close the connection when this source owns it; keep a user's connection open."""

        if not self._owns_connection:
            return
        connection, self._connection = self._connection, None
        self._close_quietly(connection)

    @staticmethod
    def _close_quietly(resource: Any) -> None:
        if resource is None:
            return
        try:
            resource.close()
        except Exception:  # best-effort release at the edge; the original error wins
            pass

    def __enter__(self) -> PostgresSource:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"PostgresSource(name={self._name!r}, host={self._host!r}, port={self._port}, "
            f"database={self._database!r}, table={self.relation_sql!r})"
        )


# -- schema mapping ------------------------------------------------------------


def data_type_from_pg(
    data_type: str,
    udt_name: str,
    numeric_precision: int | None = None,
    numeric_scale: int | None = None,
) -> DataType:
    """Map an ``information_schema.columns`` description to a domain type."""

    if data_type.upper() == "ARRAY" or udt_name.startswith("_"):
        element = udt_name[1:] if udt_name.startswith("_") else udt_name
        return ListType(_scalar_type(element, None, None))
    return _scalar_type(udt_name, numeric_precision, numeric_scale)


def _scalar_type(udt_name: str, precision: int | None, scale: int | None) -> DataType:
    udt = udt_name.lower()
    if udt == "int2":
        return IntegerType(16)
    if udt == "int4":
        return IntegerType(32)
    if udt == "int8":
        return IntegerType(64)
    if udt == "float4":
        return FloatType(32)
    if udt == "float8":
        return FloatType(64)
    if udt == "numeric":
        if precision is None:
            return _UNCONSTRAINED_DECIMAL
        clamped = max(1, min(int(precision), 76))
        return DecimalType(clamped, max(0, min(int(scale or 0), clamped)))
    if udt == "bool":
        return BooleanType()
    if udt in _STRING_UDTS:
        return StringType()
    if udt == "bytea":
        return BinaryType()
    if udt == "date":
        return DateType()
    if udt == "timestamp":
        return TimestampType(None)
    if udt == "timestamptz":
        return TimestampType("UTC")
    return UnknownType()


def schema_from_information_schema(rows: list[tuple[Any, ...]]) -> Schema:
    """Build a schema from ``(column_name, data_type, udt_name, precision, scale, is_nullable)`` rows."""

    fields = []
    for column_name, data_type, udt_name, precision, scale, is_nullable in rows:
        fields.append(
            Field(
                str(column_name),
                data_type_from_pg(str(data_type), str(udt_name), precision, scale),
                str(is_nullable).upper() != "NO",
            )
        )
    return Schema(tuple(fields))


# -- value conversion -----------------------------------------------------------

Converter = Callable[[Any], Any]


def _converter(data_type: DataType) -> Converter:
    """A converter from psycopg's Python value to what Arrow expects for ``data_type``."""

    if isinstance(data_type, ListType):
        element = _converter(data_type.element)
        return lambda value: [None if v is None else element(v) for v in value]
    if isinstance(data_type, TimestampType):
        return _to_utc if data_type.timezone else _identity
    if isinstance(data_type, BinaryType):
        return _to_bytes
    if isinstance(data_type, StringType):
        return _to_text
    if isinstance(data_type, (IntegerType, FloatType, DecimalType, BooleanType, DateType)):
        return _identity
    return _to_text


def _identity(value: Any) -> Any:
    return value


def _to_utc(value: Any) -> Any:
    if isinstance(value, _dt.datetime) and value.tzinfo is not None:
        return value.astimezone(_dt.timezone.utc)
    return value


def _to_bytes(value: Any) -> Any:
    if isinstance(value, (memoryview, bytearray)):
        return bytes(value)
    return value


def _to_text(value: Any) -> Any:
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8")
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=_json_default)
    if isinstance(value, uuid.UUID):
        return str(value)
    return str(value)


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (_dt.date, _dt.datetime)):
        return value.isoformat()
    return str(value)


__all__ = [
    "PostgresSource",
    "PostgresSqlGenerator",
    "data_type_from_pg",
    "schema_from_information_schema",
]
