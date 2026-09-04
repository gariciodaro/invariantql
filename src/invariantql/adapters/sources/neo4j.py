"""Neo4j node-label source: the nodes carrying one label form a relation.

``Neo4jSource`` wraps the official ``neo4j`` Python driver (v5/v6 API:
``GraphDatabase.driver`` and ``session.run``). It discovers a schema from
``db.schema.nodeTypeProperties()`` (or by sampling nodes), compiles the pushed
projection, predicate and limit into one Cypher statement whose values are all
bound as Cypher parameters, and streams the records as Arrow batches. Spark
reaches the same nodes through the Neo4j Connector for Apache Spark.
"""

from __future__ import annotations

import datetime as _dt
import operator
import re
import threading
from collections.abc import Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit, urlunsplit

import pyarrow as pa
from neo4j import READ_ACCESS, GraphDatabase
from neo4j import time as neo4j_time  # type: ignore[reportAttributeAccessIssue]
from neo4j.exceptions import AuthError, ClientError
from neo4j.graph import Node, Path, Relationship
from neo4j.spatial import Point

from invariantql.adapters._shared.arrow import stream_from_batches, to_arrow_schema
from invariantql.domain.capabilities import PushdownCapabilities, Support
from invariantql.domain.credentials import SecretOptions
from invariantql.domain.diagnostics import (
    DiagnosticCode,
    ParameterError,
    PlanValidationError,
    SourceError,
)
from invariantql.domain.expressions import (
    Alias,
    And,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    ExpressionKind,
    In,
    IsNull,
    Like,
    Literal,
    Not,
    Or,
    Parameter,
    and_all,
    conjuncts,
    referenced_columns,
    substitute_parameters,
)
from invariantql.domain.redaction import redact_exception, register_secret
from invariantql.domain.schema import Field, Schema
from invariantql.domain.types import (
    BooleanType,
    DateType,
    FloatType,
    IntegerType,
    StringType,
    TimestampType,
)
from invariantql.ports.source import NativeRelation

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Sequence

    from invariantql.adapters._shared.arrow import ArrowStream
    from invariantql.domain.execution import PushedOperations
    from invariantql.domain.expressions import Expression
    from invariantql.domain.types import DataType

SPARK_KIND = "neo4j"
"""The ``NativeRelation.kind`` the Spark engine maps to ``org.neo4j.spark.DataSource``."""

SPARK_CONNECTOR = "org.neo4j:neo4j-connector-apache-spark_2.12"
"""Maven coordinates of the connector jar Spark needs to read this source."""

PUSHED_EXPRESSIONS: frozenset[ExpressionKind] = frozenset(
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
        ExpressionKind.LIKE,
    }
)

# Property type names reported by db.schema.nodeTypeProperties(); anything
# else (Duration, Point, Time, LocalTime, *Array, ...) becomes a string column.
PROPERTY_TYPES: dict[str, DataType] = {
    "String": StringType(),
    "Long": IntegerType(64),
    "Integer": IntegerType(64),
    "Double": FloatType(64),
    "Float": FloatType(64),
    "Boolean": BooleanType(),
    "Date": DateType(),
    "DateTime": TimestampType("UTC"),
    "ZonedDateTime": TimestampType("UTC"),
    "LocalDateTime": TimestampType(None),
}

_SCHEMA_PROCEDURE = (
    "CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName, propertyTypes "
    "WHERE $label IN nodeLabels RETURN propertyName, propertyTypes"
)
_UTC = _dt.timezone.utc
_REGEX_SPECIAL = frozenset("\\.^$*+?{}[]|()")
_NODE = "n"


# -- text helpers -------------------------------------------------------------


def quote_identifier(identifier: str) -> str:
    """Backtick-quote a Cypher identifier (label or property name)."""

    return "`" + identifier.replace("`", "``") + "`"


def like_to_regex(pattern: str) -> str:
    """Translate a SQL ``LIKE`` pattern into an anchored, case-sensitive Java regex.

    ``%`` becomes ``.*`` and ``_`` becomes ``.``; every regex metacharacter is
    escaped. ``(?s)`` lets the wildcards span line breaks like SQL does.
    """

    out = ["(?s)^"]
    for ch in pattern:
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        elif ch in _REGEX_SPECIAL:
            out.append("\\" + ch)
        else:
            out.append(ch)
    # Java/Python ``$`` also matches before a final newline.  This lookahead
    # turns it into an absolute end-of-input assertion in both regex engines.
    out.append("$(?!.)")
    return "".join(out)


def strip_userinfo(uri: str) -> tuple[str, str | None]:
    """Remove ``user:password@`` from a URI; return the clean URI and the password, if any."""

    parts = urlsplit(uri)
    if "@" not in parts.netloc:
        return uri, None
    netloc = parts.netloc.rsplit("@", 1)[1]
    clean = urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    return clean, parts.password


# -- type mapping -------------------------------------------------------------


def merge_domain_types(types: Iterable[DataType]) -> DataType:
    """Collapse the types seen for one property into a single column type.

    One type stands; a purely numeric mix widens to ``float64``; any other mix
    becomes a string column (values are ``str()``-converted while streaming).
    """

    distinct = set(types)
    if not distinct:
        return StringType()
    if len(distinct) == 1:
        return next(iter(distinct))
    if all(isinstance(t, (IntegerType, FloatType)) for t in distinct):
        return FloatType(64)
    return StringType()


def property_types_to_domain(type_names: Iterable[str]) -> DataType:
    """Map ``propertyTypes`` names from ``db.schema.nodeTypeProperties()`` to a domain type."""

    return merge_domain_types(PROPERTY_TYPES.get(name, StringType()) for name in type_names)


def infer_value_type(value: Any) -> DataType | None:
    """The domain type of one sampled property value (``None`` for a missing value)."""

    if value is None:
        return None
    if isinstance(value, bool):
        return BooleanType()
    if isinstance(value, int):
        return IntegerType(64)
    if isinstance(value, float):
        return FloatType(64)
    if isinstance(value, str):
        return StringType()
    if isinstance(value, neo4j_time.DateTime):
        return TimestampType("UTC") if value.tzinfo is not None else TimestampType(None)
    if isinstance(value, neo4j_time.Date):
        return DateType()
    return StringType()


def _value_needs_local_predicate(value: Any) -> bool:
    """Whether converting ``value`` changes comparison/LIKE semantics."""

    return value is not None and not isinstance(
        value, (bool, int, float, str, neo4j_time.DateTime, neo4j_time.Date)
    )


def _types_need_local_predicate(type_names: Iterable[str]) -> bool:
    names = set(type_names)
    if any(name not in PROPERTY_TYPES for name in names):
        return True
    mapped = {PROPERTY_TYPES[name] for name in names}
    return len(mapped) > 1 and not all(isinstance(t, (IntegerType, FloatType)) for t in mapped)


# -- value conversion -----------------------------------------------------------


def to_python(value: Any) -> Any:
    """Convert one driver value into what Arrow accepts.

    Temporal values go through ``to_native()`` (zoned ``DateTime`` becomes an
    aware UTC ``datetime``); durations, points, nodes, relationships, paths and
    collections become their string form.
    """

    if value is None:
        return None
    if isinstance(value, neo4j_time.DateTime):
        native = value.to_native()
        return native.astimezone(_UTC) if native.tzinfo is not None else native
    if isinstance(value, (neo4j_time.Date, neo4j_time.Time)):
        return value.to_native()
    if isinstance(value, (neo4j_time.Duration, Point, Node, Relationship, Path, list, tuple, dict)):
        return str(value)
    return value


def _normalise_datetime(value: _dt.datetime, *, aware: bool) -> _dt.datetime:
    if aware:
        return value.replace(tzinfo=_UTC) if value.tzinfo is None else value.astimezone(_UTC)
    return value if value.tzinfo is None else value.astimezone(_UTC).replace(tzinfo=None)


def converter_for(data_type: DataType) -> Callable[[Any], Any]:
    """A per-column converter: driver value -> Python value matching the Arrow column."""

    if isinstance(data_type, StringType):

        def to_string(value: Any) -> Any:
            converted = to_python(value)
            if converted is None or isinstance(converted, str):
                return converted
            return str(converted)

        return to_string
    if isinstance(data_type, TimestampType):
        aware = data_type.timezone is not None

        def to_timestamp(value: Any) -> Any:
            converted = to_python(value)
            if isinstance(converted, _dt.datetime):
                return _normalise_datetime(converted, aware=aware)
            return converted

        return to_timestamp
    return to_python


def _coerce_to_column_type(value: Any, data_type: DataType | None) -> Any:
    """Align a bound comparison value with the column it is compared to."""

    if isinstance(value, _dt.datetime) and isinstance(data_type, TimestampType):
        return _normalise_datetime(value, aware=data_type.timezone is not None)
    return value


def _bind_value(value: Any) -> Any:
    # Neo4j has no decimal type; properties are Long or Double.
    if isinstance(value, Decimal):
        return float(value)
    return value


# -- Cypher generation ---------------------------------------------------------


class CypherGenerator:
    """Render domain expressions as Cypher over one node variable.

    Every literal and parameter value becomes a Cypher parameter (``$p0``,
    ``$p1``, ...; the limit is ``$limit``) collected in :attr:`values`; only
    identifiers are rendered as text. ``schema`` (optional) lets values compared
    with a timestamp column adopt that column's zone semantics.
    """

    def __init__(
        self,
        parameters: Mapping[str, Literal] | None = None,
        *,
        schema: Schema | None = None,
        node: str = _NODE,
    ) -> None:
        self.parameters = dict(parameters or {})
        self.schema = schema
        self.node = node
        self.values: dict[str, Any] = {}
        self._counter = 0

    # -- statements ---------------------------------------------------------

    def match(
        self,
        label: str,
        *,
        columns: Sequence[str],
        predicate: Expression | None = None,
        limit: int | None = None,
    ) -> str:
        """``MATCH (n:Label) [WHERE ...] RETURN n.a AS a, ... [LIMIT $limit]``."""

        if not columns:
            raise ValueError("a Cypher RETURN clause needs at least one column")
        text = f"MATCH ({self.node}:{quote_identifier(label)})"
        if predicate is not None:
            text += " WHERE " + self.expression(predicate)
        text += " RETURN " + ", ".join(
            f"{self.column(name)} AS {quote_identifier(name)}" for name in columns
        )
        if limit is not None:
            self.values["limit"] = int(limit)
            text += " LIMIT $limit"
        return text

    # -- expressions --------------------------------------------------------

    def column(self, name: str) -> str:
        return f"{self.node}.{quote_identifier(name)}"

    def expression(self, expression: Expression) -> str:
        if isinstance(expression, Column):
            return self.column(expression.name)
        if isinstance(expression, (Literal, Parameter)):
            return self.value(self.literal(expression).value)
        if isinstance(expression, Comparison):
            left = self._operand(expression.left, expression.right)
            right = self._operand(expression.right, expression.left)
            return f"({left} {expression.op.value} {right})"
        if isinstance(expression, And):
            return "(" + " AND ".join(self.expression(o) for o in expression.operands) + ")"
        if isinstance(expression, Or):
            return "(" + " OR ".join(self.expression(o) for o in expression.operands) + ")"
        if isinstance(expression, Not):
            return f"(NOT {self.expression(expression.operand)})"
        if isinstance(expression, IsNull):
            keyword = "IS NOT NULL" if expression.negated else "IS NULL"
            return f"({self.expression(expression.operand)} {keyword})"
        if isinstance(expression, In):
            values = ", ".join(self._operand(v, expression.operand) for v in expression.values)
            text = f"({self.expression(expression.operand)} IN [{values}])"
            return f"(NOT {text})" if expression.negated else text
        if isinstance(expression, Like):
            text = f"({self.expression(expression.operand)} =~ {self._regex(expression.pattern)})"
            return f"(NOT {text})" if expression.negated else text
        if isinstance(expression, Arithmetic):
            raise ValueError(
                "arithmetic is not portable in Cypher and must be evaluated by the engine"
            )
        if isinstance(expression, Alias):
            raise ValueError("alias is only valid at the top of a projection")
        raise ValueError(f"unsupported expression: {type(expression).__name__}")

    def literal(self, expression: Literal | Parameter) -> Literal:
        if isinstance(expression, Literal):
            return expression
        try:
            return self.parameters[expression.name]
        except KeyError:
            raise ParameterError(
                f"missing parameter {expression.name!r}",
                code=DiagnosticCode.PARAMETER_MISSING,
            ) from None

    def value(self, value: Any) -> str:
        """Bind a value as the next ``$pN`` parameter; ``None`` renders as ``null``."""

        if value is None:
            return "null"
        name = f"p{self._counter}"
        self._counter += 1
        self.values[name] = _bind_value(value)
        return f"${name}"

    # -- helpers --------------------------------------------------------------

    def _operand(self, expression: Expression, other: Expression) -> str:
        """Render one side of a comparison, aligning a value with the column it faces."""

        if isinstance(expression, (Literal, Parameter)) and isinstance(other, Column):
            return self.value(
                _coerce_to_column_type(
                    self.literal(expression).value, self._column_type(other.name)
                )
            )
        return self.expression(expression)

    def _column_type(self, name: str) -> DataType | None:
        if self.schema is None:
            return None
        field = self.schema.resolve(name)
        return None if field is None else field.data_type

    def _regex(self, pattern: Expression) -> str:
        if not isinstance(pattern, (Literal, Parameter)):
            raise ValueError("LIKE pattern must be a literal or parameter")
        value = self.literal(pattern).value
        if value is None:
            return "null"
        if not isinstance(value, str):
            raise PlanValidationError(
                f"LIKE pattern must be a string, got {type(value).__name__}",
                code=DiagnosticCode.PLAN_TYPE_MISMATCH,
            )
        return self.value(like_to_regex(value))


# -- the source ------------------------------------------------------------------


class Neo4jSource:
    """A Neo4j node label exposed as a tabular relation.

    The relation is the set of nodes carrying ``label``; every node property is
    a column, and a property a node lacks reads as ``NULL``.

    Constructor options
        ``name``
            Registry name of the source.
        ``uri``
            Driver URI such as ``neo4j://host:7687``, ``neo4j+s://...`` or
            ``bolt://...``. Userinfo (``user:password@``) is stripped: the
            driver rejects it, and it must never reach ``repr`` or Spark
            options. Pass credentials through ``user``/``password``.
        ``user``, ``password``
            Basic authentication for the driver, and the credentials Spark's
            connector receives (through ``SecretOptions`` only).
        ``label``
            The node label that forms the relation.
        ``database``
            Database name; ``None`` selects the server default.
        ``schema``
            A declared :class:`Schema`; skips discovery entirely.
        ``sample_size``
            Nodes sampled to infer a schema when the
            ``db.schema.nodeTypeProperties()`` procedure is unavailable.
        ``driver``
            An existing ``neo4j.Driver`` to share. The source never closes a
            driver it did not create.

    Credential handling
        The password is held privately and never appears in ``repr``, errors
        or logs. It is exposed only through :class:`SecretOptions`, which
        registers it with the redaction service so provider messages echoing
        it are scrubbed; provider exceptions are wrapped into
        :class:`SourceError` with redacted text and no chained cause.

    Schema discovery
        ``db.schema.nodeTypeProperties()`` rows whose ``nodeLabels`` contain
        the label are mapped as String -> string, Long/Integer -> int64,
        Double/Float -> float64, Boolean -> boolean, Date -> date,
        DateTime/ZonedDateTime -> timestamp[UTC], LocalDateTime -> timestamp,
        anything else (Duration, Point, Time, lists, ...) -> string. A property
        seen with several types becomes float64 when all are numeric and string
        otherwise. When the procedure is missing or forbidden the source samples
        ``MATCH (n:Label) RETURN n LIMIT sample_size`` and infers the same way.
        Columns are ordered by name; the result is cached.

    Semantics of the pushed Cypher
        ``MATCH (n:`Label`) WHERE <predicate> RETURN n.`col` AS `col`, ... LIMIT $limit``
        with every literal and parameter value bound as a Cypher parameter.
        Cypher shares SQL's three-valued NULL logic and compares strings
        case-sensitively. ``LIKE`` becomes an anchored, case-sensitive regex
        match (``=~``). Arithmetic is deliberately not advertised for pushdown:
        Cypher has no decimal type and its division-by-zero and overflow rules
        cannot implement the portable contract without lossy conversions, so
        DuckDB evaluates arithmetic predicates after the scan. ``Decimal``
        comparison values bind as floats (Neo4j has no decimal type); a value
        compared with a timestamp column adopts that column's zone semantics
        (naive means UTC).

    Spark
        :meth:`relation` returns ``NativeRelation('neo4j', ...)`` with the
        ``url``, ``labels`` and ``database`` options and the basic-auth secrets.
        The cluster needs the ``org.neo4j:neo4j-connector-apache-spark_2.12`` jar
        (Spark format ``org.neo4j.spark.DataSource``).
    """

    def __init__(
        self,
        name: str,
        *,
        uri: str,
        user: str,
        password: str,
        label: str,
        database: str | None = None,
        schema: Schema | None = None,
        sample_size: int = 1000,
        driver: Any | None = None,
    ) -> None:
        if not name:
            raise ValueError("source name must not be empty")
        if not label:
            raise ValueError("label must not be empty")
        if sample_size < 1:
            raise ValueError("sample_size must be positive")
        self._name = name
        self._uri, uri_password = strip_userinfo(uri)
        self._label = label
        self._database = database or None
        self._declared = schema
        self._sample_size = int(sample_size)
        self._secrets = SecretOptions(
            {
                "authentication.basic.username": user,
                "authentication.basic.password": password,
            }
        )
        if uri_password:
            register_secret(uri_password)
        self._cached: Schema | None = None
        self._local_columns: frozenset[str] = frozenset()
        self._streams: set[ArrowStream] = set()
        self._closed = False
        self._lock = threading.RLock()
        if driver is None:
            try:
                self._driver = GraphDatabase.driver(  # type: ignore[reportAttributeAccessIssue]
                    self._uri, auth=(user, password)
                )
            except Exception as exc:
                raise SourceError(
                    f"cannot create a Neo4j driver for {self._uri}: {redact_exception(exc)}",
                    target=name,
                ) from None
            self._owns_driver = True
        else:
            self._driver = driver
            self._owns_driver = False

    # -- identity -----------------------------------------------------------

    @property
    def name(self) -> str:
        return self._name

    @property
    def uri(self) -> str:
        """The driver URI without userinfo."""

        return self._uri

    @property
    def label(self) -> str:
        return self._label

    @property
    def database(self) -> str | None:
        return self._database

    # -- port ---------------------------------------------------------------

    def schema(self) -> Schema:
        if self._declared is not None:
            return self._declared
        with self._lock:
            if self._cached is None:
                self._cached = self._discover()
            return self._cached

    def capabilities(self) -> PushdownCapabilities:
        return PushdownCapabilities(
            projection=Support.FULL,
            predicate=Support.FULL,
            limit=Support.FULL,
            expressions=PUSHED_EXPRESSIONS,
            parameters=True,
            evidence=(
                "Cypher MATCH (n:Label) WHERE ... RETURN ... LIMIT: three-valued NULL logic, "
                "case-sensitive string comparison, LIKE as an anchored case-sensitive regex (=~), "
                "arithmetic left for engine residual evaluation; every value bound as a Cypher "
                "parameter; "
                "predicates on properties whose values are string-normalised run in the source's "
                "bounded local stream before LIMIT",
            ),
        )

    def relation(self) -> NativeRelation:
        options = {"url": self._uri, "labels": self._label}
        if self._database is not None:
            options["database"] = self._database
        return NativeRelation(
            SPARK_KIND,
            options,
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
        self._check_open()
        schema = self.schema()
        names = tuple(pushed.projection) if pushed.projection is not None else schema.names
        predicate = pushed.predicate
        if predicate is not None:
            try:
                predicate = substitute_parameters(predicate, dict(parameters))
            except KeyError as exc:
                raise ParameterError(
                    f"missing parameter {exc.args[0]!r}",
                    code=DiagnosticCode.PARAMETER_MISSING,
                ) from None
        native_parts = []
        local_parts = []
        for conjunct in conjuncts(predicate):
            if self._local_columns.intersection(referenced_columns(conjunct)):
                local_parts.append(conjunct)
            else:
                native_parts.append(conjunct)
        native_predicate = and_all(native_parts)
        fetched = tuple(dict.fromkeys((*names, *referenced_columns(*local_parts))))
        try:
            selected = schema.select(fetched)
            output_schema = schema.select(names)
        except KeyError as exc:
            raise SourceError(
                f"unknown column {exc.args[0]!r} for label {self._label!r}",
                code=DiagnosticCode.PLAN_UNKNOWN_COLUMN,
                target=self._name,
            ) from None
        if not names:
            raise SourceError(
                f"label {self._label!r} has no columns to return; declare a schema",
                code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
                target=self._name,
            )
        generator = CypherGenerator(schema=schema)
        try:
            cypher = generator.match(
                self._label,
                columns=fetched,
                predicate=native_predicate,
                limit=None if local_parts else pushed.limit,
            )
        except ValueError as exc:
            raise SourceError(
                f"cannot translate the pushed operations to Cypher: {exc}",
                code=DiagnosticCode.SOURCE_SCAN_UNSUPPORTED,
                target=self._name,
            ) from None
        arrow_schema = to_arrow_schema(output_schema)
        converters = tuple(converter_for(field.data_type) for field in selected)

        session = self._session(fetch_size=batch_size)
        try:
            result = self._run(session, cypher, generator.values)
        except Exception as exc:
            _close_quietly(session)
            raise SourceError(
                f"Neo4j rejected the scan of {self._name!r}: {redact_exception(exc)}",
                target=self._name,
                details={"cypher": cypher},
            ) from None
        batches = self._batches(
            result,
            fetched,
            converters,
            arrow_schema,
            batch_size,
            cypher,
            local_parts,
            names,
            pushed.limit if local_parts else None,
        )
        holder: list[ArrowStream] = []

        def release() -> None:
            with self._lock:
                if holder:
                    self._streams.discard(holder[0])
            try:
                session.close()
            except Exception as exc:
                raise SourceError(
                    f"closing a Neo4j session of {self._name!r} failed: {redact_exception(exc)}",
                    target=self._name,
                ) from None

        stream = stream_from_batches(arrow_schema, batches, on_close=(release,))
        with self._lock:
            if self._closed:
                stream.close()
                raise SourceError(f"source {self._name!r} is closed", target=self._name)
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
        first_error: BaseException | None = None
        for stream in streams:
            try:
                stream.close()
            except Exception as exc:
                first_error = first_error or exc
        if self._owns_driver:
            try:
                self._driver.close()
            except Exception as exc:
                first_error = first_error or exc
        if first_error is not None:
            raise SourceError(
                f"closing Neo4j resources of {self._name!r} failed: "
                f"{redact_exception(first_error)}",
                target=self._name,
            ) from None

    def __repr__(self) -> str:
        return (
            f"Neo4jSource(name={self._name!r}, uri={self._uri!r}, "
            f"label={self._label!r}, database={self._database!r})"
        )

    # -- discovery ------------------------------------------------------------

    def _discover(self) -> Schema:
        discovered = self._discover_from_procedure()
        if discovered is None:
            discovered = self._discover_by_sampling()
        fields, local_columns = discovered
        if not fields:
            raise SourceError(
                f"no properties discovered for label {self._label!r}; "
                "pass schema=... to declare the columns",
                code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
                target=self._name,
            )
        self._local_columns = frozenset(local_columns)
        return Schema(tuple(Field(name, fields[name]) for name in sorted(fields)))

    def _discover_from_procedure(self) -> tuple[dict[str, DataType], set[str]] | None:
        """Property types from ``db.schema.nodeTypeProperties()``; ``None`` when unavailable."""

        seen: dict[str, set[str]] = {}
        try:
            with self._session() as session:
                for record in self._run(session, _SCHEMA_PROCEDURE, {"label": self._label}):
                    name = record.get("propertyName")
                    if not name:
                        continue
                    types = record.get("propertyTypes") or ()
                    seen.setdefault(str(name), set()).update(str(t) for t in types)
        except AuthError as exc:
            raise self._schema_error(exc) from None
        except ClientError:
            # The procedure is missing (older/embedded servers) or forbidden: sample instead.
            return None
        except Exception as exc:
            raise self._schema_error(exc) from None
        mapped = {name: property_types_to_domain(types) for name, types in seen.items()}
        local = {name for name, types in seen.items() if _types_need_local_predicate(types)}
        return mapped, local

    def _discover_by_sampling(self) -> tuple[dict[str, DataType], set[str]]:
        cypher = f"MATCH ({_NODE}:{quote_identifier(self._label)}) RETURN {_NODE} LIMIT $limit"
        seen: dict[str, set[DataType]] = {}
        local: set[str] = set()
        try:
            with self._session() as session:
                for record in self._run(session, cypher, {"limit": self._sample_size}):
                    for key, value in _properties_of(record.value()).items():
                        inferred = infer_value_type(value)
                        if inferred is not None:
                            seen.setdefault(key, set()).add(inferred)
                        if _value_needs_local_predicate(value):
                            local.add(key)
        except Exception as exc:
            raise self._schema_error(exc) from None
        for name, types in seen.items():
            if len(types) > 1 and not all(isinstance(t, (IntegerType, FloatType)) for t in types):
                local.add(name)
        return {name: merge_domain_types(types) for name, types in seen.items()}, local

    def _schema_error(self, exc: BaseException) -> SourceError:
        return SourceError(
            f"cannot discover the schema of label {self._label!r}: {redact_exception(exc)}",
            code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
            target=self._name,
        )

    # -- driver access ----------------------------------------------------------

    def _check_open(self) -> None:
        if self._closed:
            raise SourceError(f"source {self._name!r} is closed", target=self._name)

    def _session(self, *, fetch_size: int | None = None) -> Any:
        self._check_open()
        config: dict[str, Any] = {"default_access_mode": READ_ACCESS}
        if self._database:
            config["database"] = self._database
        if fetch_size is not None:
            config["fetch_size"] = fetch_size
        try:
            return self._driver.session(**config)
        except Exception as exc:
            raise SourceError(
                f"cannot open a Neo4j session for {self._name!r}: {redact_exception(exc)}",
                target=self._name,
            ) from None

    @staticmethod
    def _run(session: Any, cypher: str, values: Mapping[str, Any]) -> Any:
        # The driver types query text as LiteralString; ours is built from identifiers only,
        # with every value bound through ``values``.
        return session.run(cast(Any, cypher), dict(values))

    def _batches(
        self,
        result: Any,
        names: Sequence[str],
        converters: Sequence[Callable[[Any], Any]],
        output_schema: pa.Schema,
        batch_size: int,
        cypher: str,
        local: Sequence[Expression],
        output_names: Sequence[str],
        limit: int | None,
    ) -> Iterator[pa.RecordBatch]:
        buffer: list[dict[str, Any]] = []
        produced = 0
        try:
            for record in result:
                row = {
                    name: convert(value)
                    for name, convert, value in zip(names, converters, record.values(), strict=True)
                }
                if local and any(_evaluate(expression, row) is not True for expression in local):
                    continue
                buffer.append({name: row[name] for name in output_names})
                produced += 1
                if len(buffer) >= batch_size:
                    yield _to_batch(buffer, output_schema, self._name)
                    buffer = []
                if limit is not None and produced >= limit:
                    break
            if buffer:
                yield _to_batch(buffer, output_schema, self._name)
        except SourceError:
            raise
        except Exception as exc:
            raise SourceError(
                f"Neo4j scan of {self._name!r} failed: {redact_exception(exc)}",
                target=self._name,
                details={"cypher": cypher},
            ) from None


# -- module helpers -------------------------------------------------------------


_COMPARISONS = {
    "=": operator.eq,
    "<>": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
}
_ARITHMETIC = {
    ArithmeticOp.ADD: operator.add,
    ArithmeticOp.SUB: operator.sub,
    ArithmeticOp.MUL: operator.mul,
    ArithmeticOp.DIV: operator.truediv,
}


def _evaluate(expression: Expression, row: Mapping[str, Any]) -> Any:
    """Evaluate a predicate/expression with SQL-style three-valued null logic."""

    if isinstance(expression, And):
        result: bool | None = True
        for operand in expression.operands:
            value = _evaluate(operand, row)
            if value is False:
                return False
            if value is None:
                result = None
        return result
    if isinstance(expression, Or):
        result = False
        for operand in expression.operands:
            value = _evaluate(operand, row)
            if value is True:
                return True
            if value is None:
                result = None
        return result
    if isinstance(expression, Not):
        value = _evaluate(expression.operand, row)
        return None if value is None else not bool(value)
    if isinstance(expression, IsNull):
        is_null = _evaluate(expression.operand, row) is None
        return not is_null if expression.negated else is_null
    if isinstance(expression, Comparison):
        left, right = _evaluate(expression.left, row), _evaluate(expression.right, row)
        if left is None or right is None:
            return None
        try:
            return bool(_COMPARISONS[expression.op.value](left, right))
        except TypeError:
            return None
    if isinstance(expression, In):
        operand = _evaluate(expression.operand, row)
        if operand is None:
            return None
        saw_null = False
        for item in expression.values:
            value = _evaluate(item, row)
            if value is None:
                saw_null = True
            elif operand == value:
                return not expression.negated
        return None if saw_null else expression.negated
    if isinstance(expression, Like):
        operand, pattern = _evaluate(expression.operand, row), _evaluate(expression.pattern, row)
        if operand is None or pattern is None:
            return None
        matched = re.fullmatch(like_to_regex(str(pattern)), str(operand)) is not None
        return not matched if expression.negated else matched
    if isinstance(expression, Arithmetic):
        left, right = _evaluate(expression.left, row), _evaluate(expression.right, row)
        if left is None or right is None:
            return None
        try:
            return _ARITHMETIC[expression.op](left, right)
        except (ArithmeticError, TypeError):
            return None
    if isinstance(expression, Column):
        return row.get(expression.name)
    if isinstance(expression, Literal):
        return expression.value
    raise SourceError(f"cannot evaluate {type(expression).__name__} in the Neo4j source")


def _properties_of(value: Any) -> dict[str, Any]:
    if isinstance(value, Node):
        return dict(value.items())
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _to_batch(rows: list[dict[str, Any]], schema: pa.Schema, source: str) -> pa.RecordBatch:
    try:
        return pa.RecordBatch.from_pylist(rows, schema=schema)
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise SourceError(
            f"Neo4j values of {source!r} do not fit the column types: {redact_exception(exc)}",
            target=source,
        ) from None


def _close_quietly(session: Any) -> None:
    try:
        session.close()
    except Exception:
        pass


__all__ = [
    "PROPERTY_TYPES",
    "PUSHED_EXPRESSIONS",
    "SPARK_CONNECTOR",
    "SPARK_KIND",
    "CypherGenerator",
    "Neo4jSource",
    "converter_for",
    "infer_value_type",
    "like_to_regex",
    "merge_domain_types",
    "property_types_to_domain",
    "quote_identifier",
    "strip_userinfo",
    "to_python",
]
