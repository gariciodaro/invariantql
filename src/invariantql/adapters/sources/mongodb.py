"""MongoDB native source adapter (ADR-0003, ADR-0004, ADR-0010).

A ``MongoDBSource`` exposes one collection as a logical relation. Pushed
operations become a ``find()`` call: projection, filter and limit are
evaluated by the server whenever MongoDB's semantics match the portable
profile, and compensated in-process when they do not. The module imports
``pymongo`` at the top; the facade imports this module lazily.
"""

from __future__ import annotations

import datetime as _dt
import functools
import ipaddress
import operator
import re
import threading
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import unquote

import pyarrow as pa
from bson import Decimal128, ObjectId
from bson.regex import Regex
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from invariantql.adapters._shared.arrow import (
    ArrowStream,
    empty_stream,
    stream_from_batches,
    to_arrow_schema,
)
from invariantql.domain.capabilities import PushdownCapabilities, Support
from invariantql.domain.credentials import CredentialRef, SecretOptions
from invariantql.domain.diagnostics import DiagnosticCode, ParameterError, SourceError
from invariantql.domain.expressions import (
    And,
    Column,
    Comparison,
    ComparisonOp,
    Expression,
    ExpressionKind,
    In,
    IsNull,
    Like,
    Literal,
    Not,
    Or,
    conjuncts,
    referenced_columns,
    substitute_parameters,
)
from invariantql.domain.redaction import redact_exception, register_secret
from invariantql.domain.schema import Field, Schema
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
    StructType,
    TimestampType,
    UnknownType,
    unify,
)
from invariantql.ports.source import NativeRelation

if TYPE_CHECKING:
    from invariantql.domain.execution import PushedOperations

_KIND = "mongodb"
_SPARK_CONNECTOR = "org.mongodb.spark:mongo-spark-connector_2.12"
_UTC = _dt.timezone.utc
_COSMOS_HOST_SUFFIX = ".cosmos.azure.com"

# Expression kinds whose MongoDB translation matches the portable profile.
# NOT and ARITHMETIC stay residual: ``$not``/``$ne`` match documents where the
# field is null or missing, which violates three-valued logic, and MongoDB
# cannot evaluate arithmetic inside a plain ``find()`` filter.
_PUSHED_EXPRESSIONS: frozenset[ExpressionKind] = frozenset(
    {
        ExpressionKind.COLUMN,
        ExpressionKind.LITERAL,
        ExpressionKind.PARAMETER,
        ExpressionKind.COMPARISON,
        ExpressionKind.AND,
        ExpressionKind.OR,
        ExpressionKind.IS_NULL,
        ExpressionKind.IN,
        ExpressionKind.LIKE,
    }
)

_OPERATORS: dict[ComparisonOp, str] = {
    ComparisonOp.EQ: "$eq",
    ComparisonOp.LT: "$lt",
    ComparisonOp.LE: "$lte",
    ComparisonOp.GT: "$gt",
    ComparisonOp.GE: "$gte",
}

_FLIPPED: dict[ComparisonOp, ComparisonOp] = {
    ComparisonOp.EQ: ComparisonOp.EQ,
    ComparisonOp.NE: ComparisonOp.NE,
    ComparisonOp.LT: ComparisonOp.GT,
    ComparisonOp.LE: ComparisonOp.GE,
    ComparisonOp.GT: ComparisonOp.LT,
    ComparisonOp.GE: ComparisonOp.LE,
}

_PYTHON_COMPARE: dict[ComparisonOp, Callable[[Any, Any], bool]] = {
    ComparisonOp.EQ: operator.eq,
    ComparisonOp.NE: operator.ne,
    ComparisonOp.LT: operator.lt,
    ComparisonOp.LE: operator.le,
    ComparisonOp.GT: operator.gt,
    ComparisonOp.GE: operator.ge,
}

_SCALAR_TYPES: dict[str, DataType] = {
    "boolean": BooleanType(),
    "integer": IntegerType(64),
    "float": FloatType(64),
    "string": StringType(),
    "binary": BinaryType(),
    "timestamp": TimestampType("UTC"),
    "decimal": DecimalType(34, 10),
}
_NUMERIC_KINDS = frozenset({"integer", "float", "decimal"})


class MongoDBSource:
    """One MongoDB collection as a logical relation, scanned through ``find()``.

    Constructor options:

    - ``name``: the source name used in queries.
    - ``uri``: a ``mongodb://`` or ``mongodb+srv://`` connection string. It may
      carry ``user:password@`` userinfo and query options; it is treated as a
      secret in its entirety (see below).
    - ``database`` and ``collection``: the collection to expose.
    - ``schema``: an optional declared :class:`Schema`. When omitted the schema
      is inferred once by sampling ``sample_size`` documents in natural order
      (``find().limit()``) and cached for the lifetime of the source.
    - ``client``: an existing ``pymongo.MongoClient`` to reuse. The source then
      does not close it; without it a client is created lazily from ``uri``
      on first use and closed by :meth:`close`.

    Credential handling: the URI is never echoed. ``repr`` shows only the
    scheme and hosts (no userinfo, no path, no query options); the URI is
    registered with the redaction service through :class:`SecretOptions` so
    provider error messages that quote it are scrubbed; provider exceptions
    are wrapped into :class:`SourceError` with a redacted message.
    :meth:`relation` reveals the URI only inside ``SecretOptions`` under the
    Spark connector's ``connection.uri`` key.

    Schema inference maps top-level fields to columns: ``bool`` to boolean,
    ``int`` to int64, ``float`` to float64, ``str`` to string, ``bytes`` to
    binary, ``datetime`` to ``timestamp[UTC]``, ``ObjectId`` to string
    (``str()`` at scan time), ``Decimal128`` to ``decimal(34,10)``
    (``to_decimal()``; fractional digits beyond the scale are rounded), dicts
    to structs of their inferred children and lists to lists of the inferred
    element type (unknown when no element was seen). Mixed numeric kinds widen
    to the wider numeric type; any other conflict across documents becomes a
    string column whose values are ``str()``-converted. Fields hold ``_id``
    first, then first-seen order. An empty collection has no schema evidence
    and raises ``SOURCE_SCHEMA_UNAVAILABLE``; declare a schema for it.

    Pushdown semantics (all with SQL three-valued logic):

    - ``=``, ``<``, ``<=``, ``>``, ``>=`` map to ``$eq``/``$lt``/``$lte``/
      ``$gt``/``$gte``; MongoDB's type bracketing already excludes null and
      missing fields. ``<>``, ``NOT IN`` and ``NOT LIKE`` add a
      ``{field: {$ne: null}}`` guard so null/missing rows are excluded as in
      SQL. ``IS NULL`` is ``{field: null}`` (null or missing); ``IS NOT NULL``
      is ``{field: {$ne: null}}``.
    - ``IN`` becomes ``$in``; a ``NULL`` member never matches and is dropped.
    - ``LIKE`` becomes an anchored, case-sensitive ``$regex`` (``%`` to
      ``.*``, ``_`` to ``.``, everything else escaped) with the ``s`` option so
      wildcards span newlines. An end guard compensates for PCRE's special
      pre-final-newline interpretation of ``$``.
    - Comparing an ObjectId-typed column (``_id`` and, for inferred schemas,
      any column that only held ObjectIds) with a 24-hex string converts the
      string to an ``ObjectId``. Date literals become UTC midnight datetimes;
      decimal literals become ``Decimal128``. MongoDB stores datetimes with
      millisecond precision, so sub-millisecond timestamp literals compare
      against truncated values.
    - Conjuncts MongoDB cannot evaluate faithfully (predicates on conflicting
      "stringified" columns, ``LIKE`` on ObjectId columns, column-to-column
      or constant comparisons, ``NULL`` literals) are evaluated in-process on
      the converted rows, before the limit is applied, so the scan still
      honours the full pushed predicate.
    - Azure Cosmos DB's Mongo API does not support ``find`` collation. Hosts
      below ``*.cosmos.azure.com`` are detected from the URI, the unsupported
      option is omitted, and string-literal comparisons, ``IN`` and ``LIKE``
      are evaluated in-process before ``LIMIT``. Numeric and null predicates
      remain native; inferred ObjectId equality/``IN`` keeps its BSON coercion.

    Spark: :meth:`relation` returns ``NativeRelation('mongodb', ...)``; the
    Spark engine needs the ``org.mongodb.spark:mongo-spark-connector_2.12`` jar.
    """

    def __init__(
        self,
        name: str,
        *,
        uri: str,
        database: str,
        collection: str,
        schema: Schema | None = None,
        sample_size: int = 1000,
        client: Any = None,
    ) -> None:
        if not name:
            raise ValueError("source name must not be empty")
        if not uri:
            raise ValueError("uri must not be empty")
        if not database:
            raise ValueError("database must not be empty")
        if not collection:
            raise ValueError("collection must not be empty")
        if int(sample_size) < 1:
            raise ValueError("sample_size must be at least 1")
        self._name = name
        self._uri = uri
        self._database = database
        self._collection = collection
        self._sample_size = int(sample_size)
        self._client: Any = client
        self._owns_client = client is None
        # Azure Cosmos DB's Mongo API rejects the ``collation`` option on
        # ``find`` (error code 115).  Without the explicit simple collation we
        # cannot assume portable case-sensitive string comparison semantics,
        # so those predicates are rechecked over converted rows below.
        self._supports_find_collation = not _is_cosmos_mongo_uri(uri)
        self._closed = False
        self._display_uri = _display_uri(uri)
        self._secrets = SecretOptions({"connection.uri": uri}, ref=CredentialRef(f"mongodb:{name}"))
        for password in _uri_passwords(uri):
            register_secret(password)
        self._schema: Schema | None = schema
        # Columns whose string literals denote ObjectIds, and columns whose
        # predicates must be evaluated in-process (see the class docstring).
        # A declared ``StringType`` cannot tell us whether the stored value is
        # an ObjectId or a string, even for ``_id``.  Only inference supplies
        # enough evidence to perform the string -> ObjectId coercion safely.
        self._objectid_columns: frozenset[str] = frozenset()
        self._local_columns: frozenset[str] = frozenset()
        self._streams: set[ArrowStream] = set()
        self._lock = threading.RLock()

    # -- identity -----------------------------------------------------------

    @property
    def name(self) -> str:
        return self._name

    @property
    def database(self) -> str:
        return self._database

    @property
    def collection(self) -> str:
        return self._collection

    @property
    def owns_client(self) -> bool:
        return self._owns_client

    # -- port ---------------------------------------------------------------

    def schema(self) -> Schema:
        with self._lock:
            self._check_open()
            if self._schema is None:
                self._schema, self._objectid_columns, self._local_columns = self._infer_schema()
            return self._schema

    def capabilities(self) -> PushdownCapabilities:
        return PushdownCapabilities(
            projection=Support.FULL,
            predicate=Support.FULL,
            limit=Support.FULL,
            expressions=_PUSHED_EXPRESSIONS,
            parameters=True,
            evidence=(
                "MongoDB find(): projection, limit, $eq/$lt/$lte/$gt/$gte, $in and anchored "
                "case-sensitive $regex for LIKE; <>, NOT IN and NOT LIKE carry a $ne null guard; "
                "NOT and arithmetic stay residual because MongoDB negation matches null/missing fields; "
                "Cosmos DB endpoints omit unsupported collation and recheck string predicates locally",
            ),
        )

    def relation(self) -> NativeRelation:
        return NativeRelation(
            _KIND,
            {"database": self._database, "collection": self._collection},
            self._secrets,
        )

    def scan(
        self,
        pushed: PushedOperations,
        parameters: Mapping[str, Literal],
        *,
        batch_size: int,
    ) -> ArrowStream:
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        self._check_open()
        schema = self.schema()
        names = schema.names if pushed.projection is None else tuple(pushed.projection)
        for column in names:
            if column not in schema:
                raise SourceError(
                    f"unknown column {column!r} in {self._describe()}",
                    code=DiagnosticCode.PLAN_UNKNOWN_COLUMN,
                    target=self._name,
                )
        arrow_schema = to_arrow_schema(schema.select(names))
        if pushed.limit == 0:
            return empty_stream(arrow_schema)

        predicate = pushed.predicate
        if predicate is not None:
            try:
                predicate = substitute_parameters(predicate, dict(parameters))
            except KeyError as exc:
                raise ParameterError(
                    f"missing parameter {exc.args[0]!r}", code=DiagnosticCode.PARAMETER_MISSING
                ) from None
        translator = _Translator(
            self._objectid_columns,
            self._local_columns,
            force_local_string_predicates=not self._supports_find_collation,
        )
        native: list[dict[str, Any]] = []
        local: list[Expression] = []
        for conjunct in conjuncts(predicate):
            document = translator.conjunct(conjunct)
            if document is None:
                local.append(conjunct)
            else:
                native.append(document)
        filter_document = _and_all(native)

        fetched = tuple(dict.fromkeys((*names, *referenced_columns(*local))))
        projection_document: dict[str, int] = dict.fromkeys(fetched, 1)
        if "_id" not in fetched:
            projection_document["_id"] = 0
        # A collection may have a locale-aware default collation.  Pin the
        # binary/simple collation so equality and ordering keep the portable,
        # case-sensitive string semantics advertised by this adapter.
        find_options: dict[str, Any] = {
            "projection": projection_document,
            "batch_size": batch_size,
        }
        if self._supports_find_collation:
            find_options["collation"] = {"locale": "simple"}
        if pushed.limit is not None and not local:
            find_options["limit"] = pushed.limit

        converters = [(column, _converter(schema.field(column).data_type)) for column in fetched]
        handle = self._handle()
        try:
            cursor = handle.find(filter_document, **find_options)
        except PyMongoError as exc:
            raise SourceError(
                f"cannot query {self._describe()}: {redact_exception(exc)}", target=self._name
            ) from None
        rows = self._rows(cursor, converters, local, names, pushed.limit if local else None)
        holder: list[ArrowStream] = []

        def release() -> None:
            with self._lock:
                if holder:
                    self._streams.discard(holder[0])
            try:
                cursor.close()
            except Exception as exc:
                raise SourceError(
                    f"cannot close the cursor for {self._describe()}: {redact_exception(exc)}",
                    target=self._name,
                ) from None

        try:
            stream = stream_from_batches(
                arrow_schema, _batches(rows, arrow_schema, batch_size), on_close=[release]
            )
        except BaseException:
            try:
                cursor.close()
            except Exception:
                pass
            raise
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
            client, self._client = self._client, None
        first_error: BaseException | None = None
        for stream in streams:
            try:
                stream.close()
            except Exception as exc:
                first_error = first_error or exc
        if client is not None and self._owns_client:
            try:
                client.close()
            except Exception as exc:
                first_error = first_error or exc
        if first_error is not None:
            raise SourceError(
                f"cannot close MongoDB resources for {self._describe()}: "
                f"{redact_exception(first_error)}",
                target=self._name,
            ) from None

    def __repr__(self) -> str:
        return (
            f"MongoDBSource(name={self._name!r}, uri={self._display_uri!r}, "
            f"database={self._database!r}, collection={self._collection!r})"
        )

    # -- helpers ------------------------------------------------------------

    def _describe(self) -> str:
        return f"MongoDB collection {self._database}.{self._collection}"

    def _handle(self) -> Any:
        with self._lock:
            self._check_open()
            if self._client is None:
                try:
                    self._client = MongoClient(self._uri)
                except Exception as exc:  # invalid URIs raise several provider types
                    raise SourceError(
                        f"cannot create a MongoDB client for source {self._name!r}: "
                        f"{redact_exception(exc)}",
                        target=self._name,
                    ) from None
            return self._client[self._database][self._collection]

    def _check_open(self) -> None:
        if self._closed:
            raise SourceError(f"source {self._name!r} is closed", target=self._name)

    def _infer_schema(self) -> tuple[Schema, frozenset[str], frozenset[str]]:
        observers: dict[str, _Observer] = {}
        sampled = 0
        handle = self._handle()
        try:
            cursor = handle.find({}, limit=self._sample_size)
            try:
                for document in cursor:
                    sampled += 1
                    for key, value in document.items():
                        observers.setdefault(str(key), _Observer()).observe(value)
            finally:
                cursor.close()
        except PyMongoError as exc:
            raise SourceError(
                f"cannot sample {self._describe()}: {redact_exception(exc)}",
                code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
                target=self._name,
            ) from None
        if sampled == 0:
            raise SourceError(
                f"{self._describe()} is empty, so its schema cannot be inferred; declare one",
                code=DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE,
                target=self._name,
            )
        ordered = [k for k in ("_id",) if k in observers] + [k for k in observers if k != "_id"]
        fields: list[Field] = []
        objectid: set[str] = set()
        local: set[str] = set()
        for key in ordered:
            resolved = observers[key].resolve()
            fields.append(Field(key, resolved.data_type))
            if resolved.object_id:
                objectid.add(key)
            if resolved.local_only:
                local.add(key)
        return Schema(tuple(fields)), frozenset(objectid), frozenset(local)

    def _rows(
        self,
        cursor: Any,
        converters: list[tuple[str, Callable[[Any], Any]]],
        local: list[Expression],
        names: tuple[str, ...],
        limit: int | None,
    ) -> Iterator[dict[str, Any]]:
        trim = len(converters) != len(names)
        produced = 0
        try:
            for document in cursor:
                row = self._convert(document, converters)
                if local and any(_evaluate(conjunct, row) is not True for conjunct in local):
                    continue
                yield {column: row[column] for column in names} if trim else row
                produced += 1
                if limit is not None and produced >= limit:
                    return
        except PyMongoError as exc:
            raise SourceError(
                f"reading {self._describe()} failed: {redact_exception(exc)}", target=self._name
            ) from None

    def _convert(
        self, document: Mapping[str, Any], converters: list[tuple[str, Callable[[Any], Any]]]
    ) -> dict[str, Any]:
        row: dict[str, Any] = {}
        for column, convert in converters:
            try:
                row[column] = convert(document.get(column))
            except (TypeError, ValueError, ArithmeticError) as exc:
                raise SourceError(
                    f"cannot convert field {column!r} of {self._describe()}: {redact_exception(exc)}",
                    target=self._name,
                ) from None
        return row


# -- URI display ---------------------------------------------------------------


def _display_uri(uri: str) -> str:
    """``scheme://hosts`` only: no userinfo, no path, no query options."""

    scheme, separator, rest = uri.partition("://")
    if not separator or scheme.lower() not in {"mongodb", "mongodb+srv"}:
        return "<redacted>"
    hosts = rest.split("/", 1)[0].split("?", 1)[0].rsplit("@", 1)[-1]
    if not _safe_display_hosts(hosts, srv=scheme.lower() == "mongodb+srv"):
        # URI validation is deliberately lazy.  Do not mistake malformed
        # ``user:password`` without its trailing ``@`` for ``host:port`` and
        # echo the apparent password from repr/error-adjacent diagnostics.
        return "<redacted>"
    return f"{scheme}://{hosts}"


def _safe_display_hosts(hosts: str, *, srv: bool) -> bool:
    """Whether an authority is unambiguously a display-safe Mongo host list."""

    seeds = hosts.split(",")
    if not hosts or not seeds or (srv and len(seeds) != 1):
        return False
    for seed in seeds:
        seed = seed.strip()
        if not seed or "@" in seed or any(char.isspace() for char in seed):
            return False
        if seed.startswith("["):
            end = seed.find("]")
            if end <= 1:
                return False
            host = seed[1:end]
            try:
                ipaddress.IPv6Address(host)
            except ValueError:
                return False
            remainder = seed[end + 1 :]
            if not remainder:
                continue
            if srv or not remainder.startswith(":") or not _valid_port(remainder[1:]):
                return False
            continue
        if any(char in seed for char in "[]/?#"):
            return False
        if ":" not in seed:
            continue
        if srv or seed.count(":") != 1:
            return False
        host, port = seed.rsplit(":", 1)
        if not host or not _valid_port(port):
            return False
    return True


def _valid_port(value: str) -> bool:
    return 1 <= len(value) <= 5 and value.isascii() and value.isdigit() and 1 <= int(value) <= 65535


def _uri_passwords(uri: str) -> tuple[str, ...]:
    """Encoded and decoded URI password tokens, without validating or resolving hosts."""

    _, separator, rest = uri.partition("://")
    if not separator:
        return ()
    authority = rest.split("/", 1)[0].split("?", 1)[0]
    if "@" not in authority:
        return ()
    userinfo = authority.rsplit("@", 1)[0]
    if ":" not in userinfo:
        return ()
    encoded = userinfo.split(":", 1)[1]
    decoded = unquote(encoded)
    return tuple(dict.fromkeys((encoded, decoded)))


def _is_cosmos_mongo_uri(uri: str) -> bool:
    """Whether a Mongo URI targets Azure Cosmos DB's Mongo-compatible API.

    URI userinfo and ports are ignored, host matching is case-insensitive, and
    every seed in a standard multi-host URI is checked.  Parsing stays local:
    unlike PyMongo's SRV parser this helper never performs DNS resolution.
    """

    _, separator, rest = uri.partition("://")
    if not separator:
        return False
    authority = rest.split("/", 1)[0].split("?", 1)[0].rsplit("@", 1)[-1]
    for seed in authority.split(","):
        seed = seed.strip()
        if seed.startswith("["):
            end = seed.find("]")
            host = seed[1:end] if end >= 0 else seed
        else:
            host = seed.rsplit(":", 1)[0] if ":" in seed else seed
        if host.rstrip(".").lower().endswith(_COSMOS_HOST_SUFFIX):
            return True
    return False


# -- schema inference ------------------------------------------------------------


class _Resolved:
    __slots__ = ("data_type", "local_only", "object_id")

    def __init__(self, data_type: DataType, *, object_id: bool = False, local_only: bool = False):
        self.data_type = data_type
        self.object_id = object_id
        self.local_only = local_only


class _Observer:
    """Accumulates the value kinds seen for one field (and its children)."""

    __slots__ = ("children", "element", "kinds")

    def __init__(self) -> None:
        self.kinds: dict[str, None] = {}
        self.children: dict[str, _Observer] = {}
        self.element: _Observer | None = None

    def observe(self, value: Any) -> None:
        if value is None:
            return
        if isinstance(value, bool):
            kind = "boolean"
        elif isinstance(value, int):
            kind = "integer"
        elif isinstance(value, float):
            kind = "float"
        elif isinstance(value, str):
            kind = "string"
        elif isinstance(value, bytes):
            kind = "binary"
        elif isinstance(value, _dt.datetime):
            kind = "timestamp"
        elif isinstance(value, ObjectId):
            kind = "objectid"
        elif isinstance(value, Decimal128):
            kind = "decimal"
        elif isinstance(value, Mapping):
            kind = "struct"
            for key, child in value.items():
                self.children.setdefault(str(key), _Observer()).observe(child)
        elif isinstance(value, (list, tuple)):
            kind = "list"
            if self.element is None:
                self.element = _Observer()
            for item in value:
                self.element.observe(item)
        else:
            kind = "other"
        self.kinds.setdefault(kind)

    def resolve(self) -> _Resolved:
        kinds = set(self.kinds)
        if not kinds:
            return _Resolved(UnknownType())
        if len(kinds) == 1:
            kind = next(iter(kinds))
            if kind == "struct":
                fields = tuple(
                    (name, child.resolve().data_type) for name, child in self.children.items()
                )
                return _Resolved(StructType(fields))
            if kind == "list":
                element = (
                    UnknownType() if self.element is None else self.element.resolve().data_type
                )
                return _Resolved(ListType(element))
            if kind == "objectid":
                return _Resolved(StringType(), object_id=True)
            if kind == "other":
                return _Resolved(StringType(), local_only=True)
            return _Resolved(_SCALAR_TYPES[kind])
        if kinds <= _NUMERIC_KINDS:
            widest: DataType = IntegerType(64)
            for kind in kinds:
                widest = unify(widest, _SCALAR_TYPES[kind])
            return _Resolved(widest)
        # Conflicting kinds: stringify, and never let MongoDB compare the raw values.
        return _Resolved(StringType(), local_only=True)


# -- value conversion ---------------------------------------------------------


def _converter(data_type: DataType) -> Callable[[Any], Any]:
    if isinstance(data_type, BooleanType):
        return _to_bool
    if isinstance(data_type, IntegerType):
        return _to_int
    if isinstance(data_type, FloatType):
        return _to_float
    if isinstance(data_type, DecimalType):
        return functools.partial(_to_decimal, scale=data_type.scale)
    if isinstance(data_type, TimestampType):
        return _to_utc if data_type.timezone else _to_naive
    if isinstance(data_type, DateType):
        return _to_date
    if isinstance(data_type, BinaryType):
        return _to_binary
    if isinstance(data_type, ListType):
        element = _converter(data_type.element)

        def to_list(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, (list, tuple)):
                return [element(item) for item in value]
            raise TypeError(f"expected a list, got {type(value).__name__}")

        return to_list
    if isinstance(data_type, StructType):
        children = [(name, _converter(child)) for name, child in data_type.fields]

        def to_struct(value: Any) -> Any:
            if value is None:
                return None
            if isinstance(value, Mapping):
                return {name: convert(value.get(name)) for name, convert in children}
            raise TypeError(f"expected a document, got {type(value).__name__}")

        return to_struct
    return _to_string


def _to_string(value: Any) -> Any:
    if value is None or isinstance(value, str):
        return value
    return str(value)


def _to_bool(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    raise TypeError(f"expected a boolean, got {type(value).__name__}")


def _to_int(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, Decimal128):
        as_decimal = cast(Any, value).to_decimal()
        if as_decimal.is_finite() and as_decimal == as_decimal.to_integral_value():
            return int(as_decimal)
    raise TypeError(f"cannot represent {type(value).__name__} value {value!r} as an integer")


def _to_float(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, float)):
        return float(value)
    if isinstance(value, Decimal128):
        return float(value.to_decimal())
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"cannot represent {type(value).__name__} value as a float")


def _to_decimal(value: Any, *, scale: int) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal128):
        result = value.to_decimal()
    elif isinstance(value, Decimal):
        result = value
    elif isinstance(value, bool):
        raise TypeError("cannot represent a boolean as a decimal")
    elif isinstance(value, int):
        result = Decimal(value)
    elif isinstance(value, float):
        result = Decimal(repr(value))
    else:
        raise TypeError(f"cannot represent {type(value).__name__} value as a decimal")
    if not result.is_finite():
        return None
    exponent = result.as_tuple().exponent
    if isinstance(exponent, int) and exponent < -scale:
        result = result.quantize(Decimal(1).scaleb(-scale))
    return result


def _to_utc(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.replace(tzinfo=_UTC) if value.tzinfo is None else value.astimezone(_UTC)
    raise TypeError(f"expected a datetime, got {type(value).__name__}")


def _to_naive(value: Any) -> Any:
    converted = _to_utc(value)
    return None if converted is None else converted.replace(tzinfo=None)


def _to_date(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return _to_utc(value).date()
    if isinstance(value, _dt.date):
        return value
    raise TypeError(f"expected a datetime, got {type(value).__name__}")


def _to_binary(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value if type(value) is bytes else bytes(value)
    if isinstance(value, uuid.UUID):
        return value.bytes
    raise TypeError(f"expected bytes, got {type(value).__name__}")


def _batches(
    rows: Iterable[dict[str, Any]], arrow_schema: pa.Schema, batch_size: int
) -> Iterator[pa.RecordBatch]:
    buffer: list[dict[str, Any]] = []
    for row in rows:
        buffer.append(row)
        if len(buffer) >= batch_size:
            yield _batch(buffer, arrow_schema)
            buffer = []
    if buffer:
        yield _batch(buffer, arrow_schema)


def _batch(rows: list[dict[str, Any]], arrow_schema: pa.Schema) -> pa.RecordBatch:
    try:
        return pa.RecordBatch.from_pylist(rows, schema=arrow_schema)
    except (pa.ArrowException, TypeError, ValueError) as exc:
        raise SourceError(
            f"cannot convert MongoDB documents to Arrow: {redact_exception(exc)}"
        ) from None


# -- filter translation ----------------------------------------------------------


class _NotNative(Exception):
    """The conjunct has no faithful ``find()`` translation; evaluate it in-process."""


def _and_all(documents: list[dict[str, Any]]) -> dict[str, Any]:
    if not documents:
        return {}
    if len(documents) == 1:
        return documents[0]
    return {"$and": documents}


def like_to_regex(pattern: str) -> str:
    """Anchored regex for a SQL ``LIKE`` pattern: ``%`` to ``.*``, ``_`` to ``.``."""

    parts = ["^"]
    for char in pattern:
        if char == "%":
            parts.append(".*")
        elif char == "_":
            parts.append(".")
        else:
            parts.append(re.escape(char))
    # ``$`` alone also matches immediately before a final newline in PCRE and
    # Python.  The negative lookahead makes the anchor an absolute end-of-input
    # assertion while remaining valid in both engines.
    parts.append("$(?!.)")
    return "".join(parts)


class _Translator:
    def __init__(
        self,
        objectid_columns: frozenset[str],
        local_columns: frozenset[str],
        *,
        force_local_string_predicates: bool = False,
    ) -> None:
        self._objectid = objectid_columns
        self._local = local_columns
        self._force_local_strings = force_local_string_predicates

    def conjunct(self, expression: Expression) -> dict[str, Any] | None:
        try:
            return self._translate(expression)
        except _NotNative:
            return None

    def _translate(self, expression: Expression) -> dict[str, Any]:
        if isinstance(expression, And):
            return {"$and": [self._translate(o) for o in expression.operands]}
        if isinstance(expression, Or):
            return {"$or": [self._translate(o) for o in expression.operands]}
        if isinstance(expression, Comparison):
            return self._comparison(expression)
        if isinstance(expression, IsNull):
            # Null-ness does not depend on the stored value's type.
            column = self._column(expression.operand, typed=False)
            return {column: {"$ne": None}} if expression.negated else {column: None}
        if isinstance(expression, In):
            return self._in(expression)
        if isinstance(expression, Like):
            return self._like(expression)
        raise _NotNative

    def _column(self, expression: Expression, *, typed: bool = True) -> str:
        if not isinstance(expression, Column):
            raise _NotNative
        if typed and expression.name in self._local:
            raise _NotNative
        return expression.name

    def _comparison(self, expression: Comparison) -> dict[str, Any]:
        if isinstance(expression.left, Column) and isinstance(expression.right, Literal):
            column, literal, op = expression.left.name, expression.right, expression.op
        elif isinstance(expression.left, Literal) and isinstance(expression.right, Column):
            column, literal, op = expression.right.name, expression.left, _FLIPPED[expression.op]
        else:
            raise _NotNative
        if column in self._local or literal.value is None:
            raise _NotNative
        if (
            self._force_local_strings
            and column not in self._objectid
            and isinstance(literal.value, str)
        ):
            raise _NotNative
        value = self._value(column, literal.value)
        if op is ComparisonOp.NE:
            return {"$and": [{column: {"$ne": value}}, {column: {"$ne": None}}]}
        return {column: {_OPERATORS[op]: value}}

    def _in(self, expression: In) -> dict[str, Any]:
        column = self._column(expression.operand)
        values: list[Any] = []
        for item in expression.values:
            if not isinstance(item, Literal):
                raise _NotNative
            if item.value is None:
                if expression.negated:
                    raise _NotNative  # NOT IN with a NULL member is never true
                continue  # a NULL member of IN never matches
            if (
                self._force_local_strings
                and column not in self._objectid
                and isinstance(item.value, str)
            ):
                raise _NotNative
            values.append(self._value(column, item.value))
        if not values:
            raise _NotNative
        if expression.negated:
            return {"$and": [{column: {"$nin": values}}, {column: {"$ne": None}}]}
        return {column: {"$in": values}}

    def _like(self, expression: Like) -> dict[str, Any]:
        column = self._column(expression.operand)
        if column in self._objectid or self._force_local_strings:
            # $regex never matches ObjectId values; Cosmos DB cannot accept
            # the simple collation that guarantees portable string semantics.
            raise _NotNative
        pattern = expression.pattern
        if not isinstance(pattern, Literal) or not isinstance(pattern.value, str):
            raise _NotNative
        regex = like_to_regex(pattern.value)
        if expression.negated:
            return {"$and": [{column: {"$not": Regex(regex, "s")}}, {column: {"$ne": None}}]}
        return {column: {"$regex": regex, "$options": "s"}}

    def _value(self, column: str, value: Any) -> Any:
        if isinstance(value, str):
            if column in self._objectid and ObjectId.is_valid(value):
                return ObjectId(value)
            return value
        if isinstance(value, Decimal):
            return Decimal128(value)
        if isinstance(value, _dt.datetime):
            return value
        if isinstance(value, _dt.date):
            return _dt.datetime(value.year, value.month, value.day, tzinfo=_UTC)
        return value


# -- in-process evaluation (three-valued) ---------------------------------------


def _evaluate(expression: Expression, row: Mapping[str, Any]) -> bool | None:
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
        return None if value is None else not value
    if isinstance(expression, IsNull):
        is_null = _scalar(expression.operand, row) is None
        return (not is_null) if expression.negated else is_null
    if isinstance(expression, Comparison):
        left, right = _scalar(expression.left, row), _scalar(expression.right, row)
        if left is None or right is None:
            return None
        try:
            return bool(_PYTHON_COMPARE[expression.op](left, right))
        except TypeError:
            return None
    if isinstance(expression, In):
        operand = _scalar(expression.operand, row)
        if operand is None:
            return None
        saw_null = False
        for item in expression.values:
            value = _scalar(item, row)
            if value is None:
                saw_null = True
                continue
            try:
                if operand == value:
                    return not expression.negated
            except TypeError:
                continue
        return None if saw_null else expression.negated
    if isinstance(expression, Like):
        operand, pattern = _scalar(expression.operand, row), _scalar(expression.pattern, row)
        if operand is None or pattern is None:
            return None
        matched = re.fullmatch(like_to_regex(str(pattern)), str(operand), re.DOTALL) is not None
        return (not matched) if expression.negated else matched
    raise SourceError(f"cannot evaluate {type(expression).__name__} in-process for MongoDB")


def _scalar(expression: Expression, row: Mapping[str, Any]) -> Any:
    if isinstance(expression, Column):
        return row.get(expression.name)
    if isinstance(expression, Literal):
        return expression.value
    raise SourceError(f"cannot evaluate {type(expression).__name__} in-process for MongoDB")


__all__ = ["MongoDBSource", "like_to_regex"]
