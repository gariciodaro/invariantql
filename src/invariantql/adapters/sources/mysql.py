"""MySQL native source over PyMySQL (ADR-0003, ADR-0004, ADR-0010).

The source discovers its schema from ``information_schema.columns``, compiles
the planner's pushed operations into one parameterised ``SELECT`` per scan,
and streams the result through an unbuffered server-side cursor as Arrow
record batches with bounded memory.

Every literal and parameter value is bound through PyMySQL's placeholder
mechanism; only identifiers are rendered as text (with backtick quoting).
Provider exceptions are translated into :class:`SourceError` at the boundary
with credential redaction; the password never appears in ``repr``, ``str``,
diagnostics, or logs.
"""

from __future__ import annotations

import datetime as _dt
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

import pymysql
import pymysql.cursors

from invariantql.adapters._shared.arrow import ArrowStream, stream_from_rows, to_arrow_schema
from invariantql.adapters._shared.sqltext import MYSQL, SqlGenerator
from invariantql.domain.capabilities import PushdownCapabilities, Support
from invariantql.domain.credentials import SecretOptions
from invariantql.domain.diagnostics import DiagnosticCode, SourceError
from invariantql.domain.execution import PushedOperations
from invariantql.domain.expressions import (
    ALL_EXPRESSION_KINDS,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    Expression,
    ExpressionKind,
    In,
    Like,
    Literal,
    Parameter,
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
    StringType,
    TimestampType,
)
from invariantql.ports.source import NativeRelation

RELATION_KIND = "jdbc:mysql"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
SESSION_TIME_ZONE_SQL = "SET time_zone = '+00:00'"
SESSION_SQL_MODE_SQL = "SELECT @@SESSION.sql_mode"
SCHEMA_SQL = (
    "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, "
    "DATETIME_PRECISION, IS_NULLABLE FROM information_schema.columns "
    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION"
)

# The shared MYSQL dialect spells LIKE as ``LIKE BINARY``; this adapter renders
# LIKE itself (see ``_MySqlGenerator``) and casts the dividend to DOUBLE so
# that ``/`` is floating-point division rather than MySQL's 4-extra-digit
# decimal division.
_DIALECT = replace(MYSQL, like_operator="LIKE", division_cast="DOUBLE")
_UTC = _dt.timezone.utc

_INT_BITS = {"tinyint": 8, "smallint": 16, "mediumint": 32, "int": 32, "integer": 32, "bigint": 64}
_UNSIGNED_INT_BITS = {"tinyint": 16, "smallint": 32, "mediumint": 32, "int": 64, "integer": 64}
_STRING_TYPES = frozenset(
    {"char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set", "json"}
)
_BINARY_TYPES = frozenset({"binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob"})
_GEOMETRY_TYPES = frozenset(
    {
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
        "geomcollection",
    }
)
_MYSQL_ERRORS: tuple[type[BaseException], ...] = (pymysql.MySQLError, OSError)
_PUSHED_EXPRESSIONS = ALL_EXPRESSION_KINDS - {ExpressionKind.ARITHMETIC}


# -- value conversion -----------------------------------------------------------


def _int_to_bool(value: Any) -> bool | None:
    return None if value is None else bool(value)


def _bit_to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        return int.from_bytes(bytes(value), "big")
    return int(value)


def _bit_to_bool(value: Any) -> bool | None:
    as_int = _bit_to_int(value)
    return None if as_int is None else as_int != 0


def _to_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _to_float(value: Any) -> float | None:
    return None if value is None else float(value)


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    return Decimal(value)


def _to_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", "replace")
    return str(value)


def _to_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value.encode("utf-8")
    return bytes(value)


def _to_date(value: Any) -> _dt.date | None:
    # PyMySQL hands back the raw text for zero dates ('0000-00-00'); treat them as NULL.
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    return None


def _to_datetime(value: Any) -> _dt.datetime | None:
    if isinstance(value, _dt.datetime):
        if value.tzinfo is not None:
            return value.astimezone(_UTC).replace(tzinfo=None)
        return value
    if isinstance(value, _dt.date):
        return _dt.datetime(value.year, value.month, value.day)
    return None


def _time_formatter(fsp: int) -> Callable[[Any], str | None]:
    """Render a TIME value exactly as MySQL's ``CAST(t AS CHAR)`` does."""

    def convert(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if not isinstance(value, _dt.timedelta):
            return str(value)
        sign = "-" if value < _dt.timedelta(0) else ""
        total = abs(value)
        seconds = total.days * 86_400 + total.seconds
        hours, rest = divmod(seconds, 3600)
        minutes, secs = divmod(rest, 60)
        text = f"{sign}{hours:02d}:{minutes:02d}:{secs:02d}"
        if fsp > 0:
            text += "." + f"{total.microseconds:06d}"[:fsp]
        return text

    return convert


def map_column_type(
    data_type: str,
    column_type: str,
    numeric_precision: int | None = None,
    numeric_scale: int | None = None,
    datetime_precision: int | None = None,
) -> tuple[DataType, Callable[[Any], Any]]:
    """Map an ``information_schema.columns`` row to a domain type and a value converter.

    ``tinyint(1)`` and ``bit(1)`` become booleans; other integers keep their
    width (unsigned types widen, ``bigint unsigned`` becomes ``decimal(20,0)``);
    ``datetime`` is a naive timestamp; ``timestamp`` is UTC because the session
    time zone is pinned to ``+00:00``; ``time`` is rendered as text the way
    MySQL itself casts it; ``year`` is an integer; unknown types are textual.
    """

    kind = data_type.strip().lower()
    column = column_type.strip().lower()
    unsigned = "unsigned" in column
    if kind == "tinyint" and column.startswith("tinyint(1)"):
        return BooleanType(), _int_to_bool
    if kind == "bit":
        if column in ("bit", "bit(1)"):
            return BooleanType(), _bit_to_bool
        return IntegerType(64), _bit_to_int
    if kind in _INT_BITS:
        if unsigned:
            if kind == "bigint":
                return DecimalType(20, 0), _to_decimal
            return IntegerType(_UNSIGNED_INT_BITS[kind]), _to_int
        return IntegerType(_INT_BITS[kind]), _to_int
    if kind == "year":
        return IntegerType(16), _to_int
    if kind == "float":
        return FloatType(32), _to_float
    if kind in ("double", "real", "double precision"):
        return FloatType(64), _to_float
    if kind in ("decimal", "numeric", "dec", "fixed"):
        precision = min(max(int(numeric_precision or 10), 1), 76)
        scale = min(max(int(numeric_scale or 0), 0), precision)
        return DecimalType(precision, scale), _to_decimal
    if kind in _STRING_TYPES:
        return StringType(), _to_str
    if kind in _BINARY_TYPES or kind in _GEOMETRY_TYPES:
        return BinaryType(), _to_bytes
    if kind == "date":
        return DateType(), _to_date
    if kind == "datetime":
        return TimestampType(None), _to_datetime
    if kind == "timestamp":
        return TimestampType("UTC"), _to_datetime
    if kind == "time":
        return StringType(), _time_formatter(int(datetime_precision or 0))
    return StringType(), _to_str


@dataclass(frozen=True, slots=True)
class _ColumnInfo:
    field: Field
    data_type: str
    column_type: str
    convert: Callable[[Any], Any]

    @property
    def boolean_int(self) -> bool:
        """A ``tinyint(1)`` column: any non-zero value is TRUE, so predicates test ``<> 0``."""

        return self.data_type == "tinyint" and isinstance(self.field.data_type, BooleanType)


# -- SQL generation ---------------------------------------------------------------


class _MySqlGenerator(SqlGenerator):
    """MySQL SQL with the portable profile's string and division semantics.

    * Comparisons and ``IN`` between string operands wrap every operand in
      ``CAST(... AS BINARY)`` (columns are first converted to utf8mb4) so the
      comparison is byte-wise: case-sensitive, trailing spaces significant,
      ordered by code point. MySQL's default collations are case-insensitive.
    * ``LIKE`` runs on ``CONVERT(x USING utf8mb4) COLLATE utf8mb4_bin`` with
      ``ESCAPE ''``: character-wise (``_`` matches one character, not one
      byte as ``LIKE BINARY`` would), case-sensitive, and without MySQL's
      default backslash escape. Under ``NO_BACKSLASH_ESCAPES`` the clause is
      omitted because no escape character applies and the clause is rejected.
    * ``tinyint(1)`` columns render as ``(col <> 0)`` inside predicates.
    * Numeric operands are cast before arithmetic: integer literals keep their
      logical int64 type, mixed floating-point expressions widen to ``DOUBLE``,
      and decimal multiplication reserves Spark's carry digit.
    * ``/`` casts both operands to ``DOUBLE`` and protects the denominator with
      ``NULLIF(..., 0)``.
    * Time-zone-aware datetime values are bound as naive UTC, matching the
      ``+00:00`` session time zone.
    """

    def __init__(
        self,
        columns: Mapping[str, _ColumnInfo],
        parameters: Mapping[str, Literal] | None,
        *,
        like_escape: bool = True,
    ) -> None:
        super().__init__(_DIALECT, parameters)
        self._columns = columns
        self._schema = Schema(tuple(info.field for info in columns.values()))
        self._like_escape = " ESCAPE ''" if like_escape else ""

    def expression(self, expression: Expression) -> str:
        if isinstance(expression, Column):
            sql = self.dialect.quote(expression.name)
            info = self._columns.get(expression.name)
            return f"({sql} <> 0)" if info is not None and info.boolean_int else sql
        if isinstance(expression, Comparison):
            left, right = self._comparison_operands((expression.left, expression.right))
            return f"({left} {expression.op.value} {right})"
        if isinstance(expression, In):
            operand, *values = self._comparison_operands((expression.operand, *expression.values))
            negation = "NOT " if expression.negated else ""
            return f"({operand} {negation}IN ({', '.join(values)}))"
        if isinstance(expression, Like):
            operand = f"(CONVERT({self.expression(expression.operand)} USING utf8mb4) COLLATE utf8mb4_bin)"
            pattern = self.expression(expression.pattern)
            negation = "NOT " if expression.negated else ""
            return f"({operand} {negation}LIKE {pattern}{self._like_escape})"
        if isinstance(expression, Arithmetic):
            return self._arithmetic(expression)
        return super().expression(expression)

    def _arithmetic(self, expression: Arithmetic) -> str:
        # Render first so missing parameters retain SqlGenerator's documented
        # ParameterError instead of leaking an internal KeyError.
        left = self.expression(expression.left)
        right = self.expression(expression.right)
        typed = substitute_parameters(expression, self.parameters)
        if not isinstance(typed, Arithmetic):  # pragma: no cover - structural invariant
            raise TypeError("arithmetic substitution changed the expression kind")
        left_type = expression_type(typed.left, self._schema)
        right_type = expression_type(typed.right, self._schema)
        result_type = expression_type(typed, self._schema)

        if expression.op is ArithmeticOp.DIV:
            # MySQL normally returns NULL with a warning for division by zero;
            # NULLIF makes that contract explicit under every sql_mode.
            return f"(CAST({left} AS DOUBLE) / NULLIF(CAST({right} AS DOUBLE), 0.0))"

        target = _mysql_numeric_type(result_type)
        if target is None:
            return f"({left} {expression.op.value} {right})"

        if isinstance(result_type, DecimalType):
            left_target = _mysql_decimal_operand(left_type, result_type)
            right_target = _mysql_decimal_operand(right_type, result_type)
            if expression.op is ArithmeticOp.MUL:
                # MySQL derives multiplication precision as p1+p2, while the
                # portable/Spark result reserves one carry digit. Add it to an
                # operand before multiplication so it exists in the
                # intermediate, not merely in the final cast.
                left_target = _mysql_decimal_operand(left_type, result_type, guard_digit=True)
        else:
            # SIGNED gives integer literals/parameters int64 semantics; DOUBLE
            # prevents float32 rounding before a float64 result is produced.
            left_target = right_target = target
        if not (
            isinstance(expression.left, Arithmetic)
            and _mysql_numeric_type(left_type) == left_target
        ):
            left = f"CAST({left} AS {left_target})"
        if not (
            isinstance(expression.right, Arithmetic)
            and _mysql_numeric_type(right_type) == right_target
        ):
            right = f"CAST({right} AS {right_target})"
        return f"CAST(({left} {expression.op.value} {right}) AS {target})"

    def bind(self, value: Any) -> str:
        if isinstance(value, _dt.datetime) and value.tzinfo is not None:
            value = value.astimezone(_UTC).replace(tzinfo=None)
        return super().bind(value)

    def _comparison_operands(self, operands: Sequence[Expression]) -> list[str]:
        rendered = [self.expression(operand) for operand in operands]
        kinds = [self._string_kind(operand) for operand in operands]
        if "string" in kinds and "other" not in kinds:
            rendered = [self._binary(o, sql) for o, sql in zip(operands, rendered, strict=True)]
        return rendered

    def _string_kind(self, expression: Expression) -> str:
        """Classify an operand as ``string``, ``other`` (known non-string) or ``unknown``."""

        if isinstance(expression, Column):
            info = self._columns.get(expression.name)
            if info is None:
                return "unknown"
            kind = info.field.data_type.kind
            if kind == "string":
                return "string"
            return "unknown" if kind in ("unknown", "null") else "other"
        if isinstance(expression, Literal):
            value: Any = expression.value
        elif isinstance(expression, Parameter):
            literal = self.parameters.get(expression.name)
            if literal is None:
                return "unknown"
            value = literal.value
        else:
            return "other"
        if value is None:
            return "unknown"
        return "string" if isinstance(value, str) else "other"

    @staticmethod
    def _binary(expression: Expression, sql: str) -> str:
        if isinstance(expression, Column):
            return f"CAST(CONVERT({sql} USING utf8mb4) AS BINARY)"
        if isinstance(expression, Literal) and expression.value is None:
            return sql
        return f"CAST({sql} AS BINARY)"


def _mysql_numeric_type(data_type: DataType) -> str | None:
    if isinstance(data_type, IntegerType):
        return "SIGNED"
    if isinstance(data_type, FloatType):
        return "FLOAT" if data_type.bits == 32 else "DOUBLE"
    if isinstance(data_type, DecimalType):
        return f"DECIMAL({data_type.precision},{data_type.scale})"
    return None


def _mysql_decimal_operand(
    data_type: DataType,
    fallback: DecimalType,
    *,
    guard_digit: bool = False,
) -> str:
    if isinstance(data_type, DecimalType):
        precision, scale = data_type.precision, data_type.scale
    elif isinstance(data_type, IntegerType):
        precision = {8: 3, 16: 5, 32: 10, 64: 19}[data_type.bits]
        scale = 0
    else:
        precision, scale = fallback.precision, fallback.scale
    if guard_digit:
        precision += 1
    return f"DECIMAL({precision},{scale})"


# -- the source ------------------------------------------------------------------


@dataclass(slots=True)
class _Session:
    connection: Any
    owned: bool
    like_escape: bool


class MySQLSource:
    """A MySQL table as a native, pushdown-capable data source.

    Constructor options
    -------------------
    ``name``
        The registry name of the source.
    ``host``, ``port`` (default 3306), ``database``, ``table``
        The server and the table (``database`` is the MySQL schema; ``table``
        is a bare table name, quoted with backticks in every statement).
    ``user``, ``password`` (default ``None``)
        Credentials. The password is held privately, exposed only through the
        :class:`SecretOptions` of :meth:`relation`, registered with the
        redaction service, and never printed by ``repr``/``str`` or echoed in
        diagnostics.
    ``ssl`` (default ``None``)
        Passed to ``pymysql.connect``: a mapping such as
        ``{"ca": "/path/ca.pem"}`` or an ``ssl.SSLContext``. ``True`` requests
        TLS without certificate verification, while ``False`` explicitly
        disables TLS. ``None`` leaves TLS to the server's requirements.
    ``connect_timeout`` (default 10), ``charset`` (default ``utf8mb4``)
        Passed to ``pymysql.connect``. The connection charset must be
        ``utf8mb4`` for the string-semantics compensation below to hold.
    ``connection`` (default ``None``)
        An existing PyMySQL-compatible connection to use instead of opening
        one. It is shared by schema discovery and every scan (so only one scan
        may be in flight at a time) and is *not* closed by :meth:`close`.

    Connections are opened lazily on first use, with ``autocommit`` on and the
    session ``time_zone`` pinned to ``+00:00``. When the source owns its
    connections, every scan gets a dedicated connection whose unbuffered
    cursor streams rows; closing the stream closes that connection without
    draining the result set. :meth:`close` closes every live stream and the
    primary connection; afterwards ``scan`` (and an uncached ``schema``) raise
    ``SourceError`` while an already discovered schema stays readable.
    PyMySQL needs the ``cryptography`` package to authenticate with
    ``caching_sha2_password`` accounts over non-TLS connections.

    Schema mapping
    --------------
    ``tinyint(1)`` and ``bit(1)`` -> boolean; other integers keep their width,
    unsigned integers widen and ``bigint unsigned`` becomes ``decimal(20,0)``;
    ``float``/``double``; ``decimal(p,s)``; ``char``/``varchar``/``text``/
    ``enum``/``set``/``json`` -> string; ``binary``/``blob`` and geometry
    types -> binary; ``date``; ``datetime`` -> naive timestamp; ``timestamp``
    -> ``timestamp[UTC]`` (values are converted to the pinned UTC session time
    zone by the server); ``time`` -> string rendered as MySQL casts it;
    ``year`` -> integer; ``bit(n>1)`` -> integer. Zero dates read as NULL.
    Unknown types are read as text. The schema is cached after first discovery.

    Pushdown semantics
    ------------------
    Projection, predicates and limit are pushed when their expressions are in
    the safe subset of the portable profile:

    * Three-valued NULL logic is native to MySQL.
    * String comparisons and ``IN`` are made byte-wise (case-sensitive,
      trailing spaces significant, code-point order) by wrapping operands in
      ``CAST(... AS BINARY)``; columns are converted to utf8mb4 first so
      non-utf8mb4 columns compare correctly with utf8mb4 literals. MySQL's
      default collations are case-insensitive, so this is a deliberate
      deviation from the server default. Wrapping a column in a function
      prevents index use for that predicate.
    * ``LIKE`` runs character-wise and case-sensitively under
      ``utf8mb4_bin`` with no escape character (``ESCAPE ''``), rather than
      ``LIKE BINARY``, which matches ``_`` against one byte and honours
      MySQL's default ``\\`` escape.
    * Arithmetic is not advertised for pushdown. MySQL cannot reliably turn
      every signed int64 overflow into the portable NULL result, so DuckDB
      evaluates arithmetic predicates after the scan.
    * ``tinyint(1)`` columns are tested as ``(col <> 0)`` so values other than
      0 and 1 behave as TRUE, matching the booleans the scan returns.
    * Time-zone-aware datetime parameters are bound as naive UTC.

    Spark
    -----
    :meth:`relation` returns ``NativeRelation("jdbc:mysql", ...)`` with the
    JDBC URL, the backtick-quoted ``dbtable``, the Connector/J driver class and
    a ``sessionInitStatement`` pinning the session time zone; the user and
    password are carried as secrets. The cluster needs the
    ``com.mysql:mysql-connector-j`` jar on its classpath.
    """

    def __init__(
        self,
        name: str,
        *,
        host: str,
        port: int = 3306,
        database: str,
        table: str,
        user: str,
        password: str | None = None,
        ssl: Mapping[str, Any] | bool | Any | None = None,
        connect_timeout: int = 10,
        charset: str = "utf8mb4",
        connection: Any | None = None,
    ) -> None:
        if not name:
            raise ValueError("source name must not be empty")
        if not host:
            raise ValueError("host must not be empty")
        if not database:
            raise ValueError("database must not be empty")
        if not table:
            raise ValueError("table must not be empty")
        if not user:
            raise ValueError("user must not be empty")
        if charset.lower() != "utf8mb4":
            raise ValueError("charset must be 'utf8mb4' to preserve portable string semantics")
        if int(port) <= 0:
            raise ValueError("port must be positive")
        if int(connect_timeout) <= 0:
            raise ValueError("connect_timeout must be positive")
        self._name = name
        self._host = host
        self._port = int(port)
        self._database = database
        self._table = table
        self._user = user
        self._password = password
        # A non-empty mapping makes PyMySQL require TLS.  An empty mapping is
        # treated like its opportunistic default, so it cannot represent the
        # documented ``ssl=True`` contract.
        self._ssl: Any = {"check_hostname": False} if ssl is True else ssl
        self._ssl_disabled = ssl is False
        self._connect_timeout = int(connect_timeout)
        self._charset = charset
        secrets: dict[str, str] = {"user": user}
        if password is not None:
            secrets["password"] = password
        self._secrets = SecretOptions(secrets)
        self._connection: Any | None = connection
        self._owns_connection = connection is None
        self._prepared = False
        self._like_escape = True
        self._schema: Schema | None = None
        self._columns: dict[str, _ColumnInfo] = {}
        self._streams: set[ArrowStream] = set()
        self._closed = False
        self._lock = threading.RLock()

    # -- identity -----------------------------------------------------------

    @property
    def name(self) -> str:
        return self._name

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def database(self) -> str:
        return self._database

    @property
    def table(self) -> str:
        return self._table

    @property
    def relation_sql(self) -> str:
        return f"{_DIALECT.quote(self._database)}.{_DIALECT.quote(self._table)}"

    # -- port ---------------------------------------------------------------

    def schema(self) -> Schema:
        with self._lock:
            if self._schema is not None:
                return self._schema
            self._check_open()
            session = self._primary_session()
            try:
                with session.connection.cursor() as cursor:
                    cursor.execute(SCHEMA_SQL, (self._database, self._table))
                    rows = list(cursor.fetchall())
            except _MYSQL_ERRORS as exc:
                if self._owns_connection:
                    self._discard_primary_connection()
                raise SourceError(
                    f"cannot read the schema of {self.relation_sql} for MySQL source "
                    f"{self._name!r}: {redact_exception(exc)}",
                    code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
                    target=self._name,
                ) from None
            if not rows:
                raise SourceError(
                    f"table {self.relation_sql} was not found (or is not readable) "
                    f"for MySQL source {self._name!r}",
                    code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
                    target=self._name,
                )
            columns: dict[str, _ColumnInfo] = {}
            for row in rows:
                name, data_type, column_type, precision, scale, fsp, nullable = row[:7]
                data_type = str(data_type or "").strip().lower()
                column_type = str(column_type or "").strip().lower()
                mapped, convert = map_column_type(
                    data_type,
                    column_type,
                    None if precision is None else int(precision),
                    None if scale is None else int(scale),
                    None if fsp is None else int(fsp),
                )
                field = Field(str(name), mapped, str(nullable).strip().upper() != "NO")
                columns[field.name] = _ColumnInfo(field, data_type, column_type, convert)
            self._columns = columns
            self._schema = Schema(tuple(info.field for info in columns.values()))
            return self._schema

    def capabilities(self) -> PushdownCapabilities:
        return PushdownCapabilities(
            projection=Support.FULL,
            predicate=Support.FULL,
            limit=Support.FULL,
            expressions=_PUSHED_EXPRESSIONS,
            parameters=True,
            evidence=(
                "MySQL SELECT with bound parameters: projection, WHERE and LIMIT run server-side",
                "string comparisons wrapped in CAST(... AS BINARY); LIKE under utf8mb4_bin with no "
                "escape; arithmetic stays engine-residual for portable overflow semantics; "
                "tinyint(1) tested as <> 0",
            ),
        )

    def relation(self) -> NativeRelation:
        return NativeRelation(
            RELATION_KIND,
            {
                "url": f"jdbc:mysql://{self._host}:{self._port}/{self._database}",
                "dbtable": _DIALECT.quote(self._table),
                "driver": JDBC_DRIVER,
                "sessionInitStatement": SESSION_TIME_ZONE_SQL,
            },
            self._secrets,
        )

    def scan(
        self,
        pushed: PushedOperations,
        parameters: Mapping[str, Literal],
        *,
        batch_size: int,
    ) -> ArrowStream:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        schema = self.schema()
        columns = tuple(pushed.projection) if pushed.projection else schema.names
        try:
            selected = schema.select(columns)
        except KeyError as exc:
            raise SourceError(
                f"MySQL source {self._name!r} has no column {exc.args[0]!r}",
                code=DiagnosticCode.PLAN_UNKNOWN_COLUMN,
                target=self._name,
            ) from None
        arrow_schema = to_arrow_schema(selected)
        converters = [self._columns[c].convert for c in columns]

        with self._lock:
            self._check_open()
            if not self._owns_connection and self._streams:
                raise SourceError(
                    f"MySQL source {self._name!r} cannot run concurrent scans on its "
                    "injected connection; close the active stream first",
                    target=self._name,
                )
            session = self._scan_session()
        generator = _MySqlGenerator(self._columns, parameters, like_escape=session.like_escape)
        try:
            sql = generator.select(
                self.relation_sql,
                columns=columns,
                predicate=pushed.predicate,
                limit=pushed.limit,
            )
        except Exception:
            self._release_session(session, cursor=None)
            raise
        values = tuple(generator.values) or None
        cursor: Any = None
        try:
            cursor = session.connection.cursor(pymysql.cursors.SSCursor)
            cursor.execute(sql, values)
        except _MYSQL_ERRORS as exc:
            self._release_session(session, cursor)
            raise SourceError(
                f"MySQL source {self._name!r} failed to execute the scan: {redact_exception(exc)}",
                target=self._name,
                details={"sql": sql},
            ) from None
        except BaseException:
            self._release_session(session, cursor)
            raise

        holder: list[ArrowStream] = []

        def release() -> None:
            with self._lock:
                if holder:
                    self._streams.discard(holder[0])
            self._release_session(session, cursor)

        stream = stream_from_rows(
            arrow_schema,
            self._rows(cursor, columns, converters, batch_size),
            batch_size=batch_size,
            on_close=[release],
        )
        with self._lock:
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
            connection, owned = self._connection, self._owns_connection
            self._connection = None
        for stream in streams:
            try:
                stream.close()
            except Exception:
                pass
        if connection is not None and owned:
            try:
                connection.close()
            except Exception:
                pass

    def __enter__(self) -> MySQLSource:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"MySQLSource(name={self._name!r}, host={self._host!r}, port={self._port}, "
            f"database={self._database!r}, table={self._table!r})"
        )

    # -- connections --------------------------------------------------------

    def _check_open(self) -> None:
        if self._closed:
            raise SourceError(
                f"MySQL source {self._name!r} is closed",
                code=DiagnosticCode.SOURCE_FAILURE,
                target=self._name,
            )

    def _connect(self) -> Any:
        try:
            return pymysql.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password or "",
                database=self._database,
                charset=self._charset,
                connect_timeout=self._connect_timeout,
                ssl=self._ssl,
                ssl_disabled=self._ssl_disabled,
                autocommit=True,
                binary_prefix=True,
            )
        except _MYSQL_ERRORS as exc:
            raise SourceError(
                f"cannot connect to MySQL source {self._name!r} at "
                f"{self._host}:{self._port}/{self._database}: {redact_exception(exc)}",
                target=self._name,
            ) from None

    def _prepare(self, connection: Any) -> bool:
        """Pin the session time zone; return whether ``LIKE ... ESCAPE ''`` may be used."""

        try:
            with connection.cursor() as cursor:
                cursor.execute(SESSION_TIME_ZONE_SQL)
                cursor.execute(SESSION_SQL_MODE_SQL)
                row = cursor.fetchone()
        except _MYSQL_ERRORS as exc:
            raise SourceError(
                f"cannot initialise the session for MySQL source {self._name!r}: "
                f"{redact_exception(exc)}",
                target=self._name,
            ) from None
        mode = str(row[0] if row else "").upper()
        return "NO_BACKSLASH_ESCAPES" not in mode

    def _primary_session(self) -> _Session:
        """The connection used for schema discovery (and for scans when injected)."""

        if self._connection is None:
            connection = self._connect()
            try:
                self._like_escape = self._prepare(connection)
            except BaseException:
                try:
                    connection.close()
                except Exception:
                    pass
                raise
            self._connection = connection
            self._prepared = True
        elif not self._prepared:
            self._like_escape = self._prepare(self._connection)
            self._prepared = True
        return _Session(self._connection, owned=False, like_escape=self._like_escape)

    def _discard_primary_connection(self) -> None:
        connection, self._connection = self._connection, None
        self._prepared = False
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass

    def _scan_session(self) -> _Session:
        if not self._owns_connection:
            return self._primary_session()
        connection = self._connect()
        try:
            like_escape = self._prepare(connection)
        except BaseException:
            try:
                connection.close()
            except Exception:
                pass
            raise
        return _Session(connection, owned=True, like_escape=like_escape)

    @staticmethod
    def _release_session(session: _Session, cursor: Any) -> None:
        if session.owned:
            # Dropping the connection discards the unbuffered result without draining it.
            try:
                session.connection.close()
            except Exception:
                pass
            return
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass

    def _rows(
        self,
        cursor: Any,
        columns: Sequence[str],
        converters: Sequence[Callable[[Any], Any]],
        batch_size: int,
    ) -> Iterator[dict[str, Any]]:
        while True:
            try:
                chunk = cursor.fetchmany(batch_size)
            except _MYSQL_ERRORS as exc:
                raise SourceError(
                    f"MySQL source {self._name!r} failed while streaming rows: "
                    f"{redact_exception(exc)}",
                    target=self._name,
                ) from None
            if not chunk:
                return
            for raw in chunk:
                try:
                    yield {
                        name: convert(value)
                        for name, convert, value in zip(columns, converters, raw, strict=True)
                    }
                except (TypeError, ValueError, ArithmeticError) as exc:
                    raise SourceError(
                        f"MySQL source {self._name!r} could not convert a streamed row: "
                        f"{redact_exception(exc)}",
                        target=self._name,
                    ) from None


__all__ = ["MySQLSource", "map_column_type"]
