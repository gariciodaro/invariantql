"""XML format handlers (ADR-0004).

``XmlLocalHandler`` streams ``row_tag`` elements out of an XML document with
the hardened :func:`defusedxml.ElementTree.iterparse` and turns them into Arrow batches for
the local engine. ``XmlReaderSpecHandler`` describes the same format to a
distributed engine as a native reader configuration (EXT-02).

Both handlers interpret :class:`~invariantql.domain.formats.XmlFormat`:

* every element whose local (namespace-stripped) name equals ``row_tag`` is
  one record;
* child elements become columns named by their local tag: leaf text is the
  value, nested children become a struct, a repeated tag becomes a list;
* attributes become columns named ``attribute_prefix + name``;
* the text of an element that also carries attributes lands in ``value_tag``.

The module imports only the standard library, ``defusedxml``, ``pyarrow``
(through the shared Arrow helpers), the domain, and the ports.
"""

from __future__ import annotations

import datetime as _dt
import math
import re
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from defusedxml import ElementTree as ET
from defusedxml.common import DefusedXmlException

from invariantql.adapters._shared.arrow import ArrowStream, stream_from_rows, to_arrow_schema
from invariantql.domain.capabilities import PushdownCapabilities, Support
from invariantql.domain.diagnostics import (
    DiagnosticCode,
    InvariantQLError,
    SourceError,
    StorageError,
    UnsupportedOperationError,
)
from invariantql.domain.formats import XmlFormat
from invariantql.domain.redaction import redact_exception
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
    NullType,
    StringType,
    StructType,
    TimestampType,
    UnknownType,
)
from invariantql.ports.format_handler import ReaderSpec

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Mapping
    from typing import BinaryIO
    from xml.etree.ElementTree import Element

    from invariantql.domain.execution import PushedOperations
    from invariantql.domain.expressions import Literal
    from invariantql.domain.formats import DataFormat
    from invariantql.domain.location import Location
    from invariantql.ports.storage import Storage

_FORMAT_NAME = "xml"
_INFERENCE_SAMPLE = 1000

SPARK_XML_REQUIREMENT = "Spark 4 built-in XML, or com.databricks:spark-xml for Spark 3.5"

_INT_RE = re.compile(r"[+-]?\d+")
_FLOAT_RE = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?")
_BOOLEANS = {"true": True, "false": False}


# -- element access -------------------------------------------------------------


def _local_name(tag: str) -> str:
    """Strip a ``{namespace}`` prefix from an element or attribute name."""

    return tag.rsplit("}", 1)[-1] if tag.startswith("{") else tag


def _text(raw: str | None) -> str | None:
    """Element/attribute text with surrounding whitespace removed; blank is null."""

    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


def _element_to_record(element: Element, fmt: XmlFormat) -> dict[str, Any]:
    """Turn an element into a raw record: values are ``str``, ``None``, dict or list."""

    record: dict[str, Any] = {}
    for name, value in element.attrib.items():
        record[f"{fmt.attribute_prefix}{_local_name(name)}"] = _text(value)
    children = list(element)
    text = _text(element.text)
    if text is not None and (element.attrib or not children):
        record[fmt.value_tag] = text
    for child in children:
        name = _local_name(child.tag)
        value = _element_to_value(child, fmt)
        if name not in record:
            record[name] = value
        elif isinstance(record[name], list):
            record[name].append(value)
        else:
            record[name] = [record[name], value]
    return record


def _element_to_value(element: Element, fmt: XmlFormat) -> Any:
    if element.attrib or len(element):
        return _element_to_record(element, fmt)
    return _text(element.text)


def _iter_row_elements(handle: BinaryIO, row_tag: str, describe: str) -> Iterator[Element]:
    """Yield each completed ``row_tag`` element, releasing it once the consumer is done.

    A depth counter identifies the row element among its descendants; every
    element is cleared and detached from its parent once it is no longer
    needed, so memory stays bounded by the largest single row.
    """

    stack: list[Element] = []
    row_depth: int | None = None
    try:
        for event, element in ET.iterparse(
            handle,
            events=("start", "end"),
            forbid_dtd=True,
            forbid_entities=True,
            forbid_external=True,
        ):
            if event == "start":
                stack.append(element)
                if row_depth is None and _local_name(element.tag) == row_tag:
                    row_depth = len(stack)
                continue
            stack.pop()
            if row_depth is not None:
                if len(stack) + 1 != row_depth:
                    continue  # a descendant of the current row: keep it until the row ends
                row_depth = None
                yield element
            element.clear()
            if stack:
                stack[-1].remove(element)
    except (ET.ParseError, DefusedXmlException) as exc:
        raise SourceError(
            f"malformed XML in {describe}: {redact_exception(exc)}",
            code=DiagnosticCode.FORMAT_INVALID,
            details={"format": _FORMAT_NAME},
        ) from None
    except InvariantQLError:
        raise
    except Exception as exc:
        raise StorageError(f"cannot read {describe}: {redact_exception(exc)}") from None


def iter_records(
    handle: BinaryIO,
    data_format: XmlFormat,
    *,
    limit: int | None = None,
    describe: str = "<xml>",
) -> Iterator[dict[str, Any]]:
    """Stream raw records from an XML byte stream; stops after ``limit`` records.

    The caller owns ``handle``. Values are strings (stripped text), ``None``
    for empty elements, dicts for nested/attributed elements and lists for
    repeated tags; :func:`convert_value` applies a schema to them.
    """

    if limit is not None and limit <= 0:
        return
    emitted = 0
    for element in _iter_row_elements(handle, data_format.row_tag, describe):
        yield _element_to_record(element, data_format)
        emitted += 1
        if limit is not None and emitted >= limit:
            return


# -- schema inference ------------------------------------------------------------


def infer_type(values: Iterable[Any], *, value_tag: str = "_VALUE") -> DataType:
    """Infer one column's type from raw values (``None`` ignored).

    Lists win over everything (a scalar becomes a one-element list); dicts make
    a struct (a scalar becomes ``{value_tag: scalar}``); otherwise all integers
    -> int64, all numbers -> float64, all ``true``/``false`` -> boolean, all ISO
    dates -> date, else string. No value at all -> null.
    """

    present = [v for v in values if v is not None]
    if not present:
        return NullType()
    if any(isinstance(v, list) for v in present):
        flat = [item for v in present for item in (v if isinstance(v, list) else [v])]
        return ListType(infer_type(flat, value_tag=value_tag))
    if any(isinstance(v, dict) for v in present):
        dicts = [v if isinstance(v, dict) else {value_tag: v} for v in present]
        return _infer_struct(dicts, value_tag)
    texts = [str(v) for v in present]
    if all(_INT_RE.fullmatch(t) and _fits(int(t), 64) for t in texts):
        return IntegerType(64)
    if all(_finite_float(t) for t in texts):
        return FloatType(64)
    if all(t.lower() in _BOOLEANS for t in texts):
        return BooleanType()
    if all(_parse_date(t) is not None for t in texts):
        return DateType()
    return StringType()


def _infer_struct(dicts: list[dict[str, Any]], value_tag: str) -> StructType:
    keys: dict[str, None] = {}
    for d in dicts:
        for key in d:
            keys.setdefault(key, None)
    return StructType(
        tuple((key, infer_type((d.get(key) for d in dicts), value_tag=value_tag)) for key in keys)
    )


def infer_schema(records: Iterable[Mapping[str, Any]], *, value_tag: str = "_VALUE") -> Schema:
    """Infer a schema from raw records; columns keep first-seen order."""

    sample = list(records)
    columns: dict[str, None] = {}
    for record in sample:
        for key in record:
            columns.setdefault(key, None)
    return Schema(
        tuple(
            Field(name, infer_type((r.get(name) for r in sample), value_tag=value_tag))
            for name in columns
        )
    )


# -- value conversion -----------------------------------------------------------


def convert_value(value: Any, data_type: DataType, *, value_tag: str = "_VALUE") -> Any:
    """Coerce a raw record value to ``data_type``; anything that does not fit is null."""

    if value is None:
        return None
    if isinstance(data_type, ListType):
        items = value if isinstance(value, list) else [value]
        return [convert_value(v, data_type.element, value_tag=value_tag) for v in items]
    if isinstance(value, list):
        return None
    if isinstance(data_type, StructType):
        mapping = value if isinstance(value, dict) else {value_tag: value}
        return {
            name: convert_value(mapping.get(name), field_type, value_tag=value_tag)
            for name, field_type in data_type.fields
        }
    if isinstance(value, dict):
        scalar = value.get(value_tag)
        if scalar is None or isinstance(scalar, (dict, list)):
            return None
        return _convert_scalar(str(scalar), data_type)
    return _convert_scalar(str(value), data_type)


def _convert_scalar(text: str, data_type: DataType) -> Any:
    if isinstance(data_type, (StringType, UnknownType)):
        return text
    if isinstance(data_type, IntegerType):
        if not _INT_RE.fullmatch(text):
            return None
        number = int(text)
        return number if _fits(number, data_type.bits) else None
    if isinstance(data_type, FloatType):
        return _finite_float_value(text)
    if isinstance(data_type, BooleanType):
        return _BOOLEANS.get(text.lower())
    if isinstance(data_type, DateType):
        return _parse_date(text)
    if isinstance(data_type, TimestampType):
        return _parse_timestamp(text, data_type.timezone)
    if isinstance(data_type, DecimalType):
        return _parse_decimal(text, data_type)
    if isinstance(data_type, BinaryType):
        return text.encode("utf-8")
    if isinstance(data_type, NullType):
        return None
    return text


def _fits(number: int, bits: int) -> bool:
    bound = 1 << (bits - 1)
    return -bound <= number < bound


def _finite_float(text: str) -> bool:
    return _finite_float_value(text) is not None


def _finite_float_value(text: str) -> float | None:
    if not _FLOAT_RE.fullmatch(text):
        return None
    value = float(text)
    return value if math.isfinite(value) else None


def _parse_date(text: str) -> _dt.date | None:
    if len(text) != 10:
        return None
    try:
        return _dt.date.fromisoformat(text)
    except ValueError:
        return None


def _parse_timestamp(text: str, timezone: str | None) -> _dt.datetime | None:
    try:
        value = _dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if timezone is None and value.tzinfo is not None:
        return value.astimezone(_dt.timezone.utc).replace(tzinfo=None)
    return value


def _parse_decimal(text: str, data_type: DecimalType) -> Decimal | None:
    try:
        value = Decimal(text)
        if not value.is_finite():
            return None
        quantized = value.quantize(Decimal(1).scaleb(-data_type.scale))
    except InvalidOperation:
        return None
    if len(quantized.as_tuple().digits) > data_type.precision:
        return None
    return quantized


# -- handlers ------------------------------------------------------------------------


def _xml_format(data_format: DataFormat) -> XmlFormat:
    if not isinstance(data_format, XmlFormat):
        raise UnsupportedOperationError(
            f"the XML handler cannot read format {data_format.format_name!r}",
            code=DiagnosticCode.FORMAT_UNSUPPORTED,
            details={"format": data_format.format_name},
        )
    if not data_format.value_tag:
        raise UnsupportedOperationError(
            "XmlFormat.value_tag must not be empty",
            code=DiagnosticCode.FORMAT_INVALID,
            details={"format": _FORMAT_NAME, "option": "value_tag"},
        )
    return data_format


def _open(storage: Storage, location: Location) -> BinaryIO:
    try:
        return storage.open_read(location)
    except InvariantQLError:
        raise
    except Exception as exc:
        raise StorageError(f"cannot open {location.uri}: {redact_exception(exc)}") from None


class XmlLocalHandler:
    """Local reader for :class:`~invariantql.domain.formats.XmlFormat` (DuckDB side).

    Constructor options
        None. Behaviour comes from the ``XmlFormat`` given to each call:
        ``row_tag``, ``attribute_prefix``, ``value_tag`` and ``schema``.
        ``root_tag`` is a writer option and is ignored when reading.

    Credential handling
        The handler holds no credentials. It receives an open byte stream from
        the ``Storage`` port, which keeps its own secrets; storage and parser
        failures are translated to ``StorageError`` / ``SourceError`` with
        redacted messages.

    Semantics
        * A record is every element whose local name equals ``row_tag``
          (namespace prefixes are stripped from element and attribute names).
          Elements outside rows are skipped; a ``row_tag`` nested inside a row
          is an ordinary child.
        * Columns: child elements by local tag (leaf text), attributes as
          ``attribute_prefix + name``, and ``value_tag`` for the text of an
          element that also has attributes (or of a row with no children).
          Nested children become a struct; a repeated tag becomes a list.
        * Text is stripped of surrounding whitespace; blank is null. Tail text,
          comments and processing instructions are ignored.
        * Schema: the declared ``schema`` wins. Otherwise it is inferred from
          the first 1000 records: all integers -> int64, numeric -> float64,
          ``true``/``false`` -> boolean, ISO dates -> date, else string; a
          column without values -> null. At scan time every value follows the
          schema: a value that does not fit becomes null, a repeated value
          where a scalar is expected becomes null, a scalar where a struct is
          expected becomes ``{value_tag: scalar}``, a scalar where a list is
          expected becomes a one-element list. Decimals are quantized to the
          declared scale.
        * Pushdown: projection FULL (only projected columns are emitted), limit
          FULL (parsing stops after ``limit`` records), predicate NONE (every
          filter stays residual and the engine evaluates it).
        * Memory: the document is parsed incrementally and each row element is
          released after it is emitted, so memory is bounded by the largest row
          plus one Arrow batch.
        * Security: ``defusedxml`` rejects DTDs, entity declarations and
          external references before they can expand or access another
          resource. Ordinary predefined XML entities remain supported.
    """

    @property
    def format_name(self) -> str:
        return _FORMAT_NAME

    def capabilities(self, data_format: DataFormat) -> PushdownCapabilities:
        fmt = _xml_format(data_format)
        return PushdownCapabilities(
            projection=Support.FULL,
            predicate=Support.NONE,
            limit=Support.FULL,
            expressions=frozenset(),
            parameters=False,
            evidence=(
                f"streaming ElementTree parser over <{fmt.row_tag}> elements: emits only the "
                "projected columns and stops parsing after the pushed limit",
                "the XML reader evaluates no predicates; every filter is residual (engine)",
            ),
        )

    def schema(self, storage: Storage, location: Location, data_format: DataFormat) -> Schema:
        fmt = _xml_format(data_format)
        if fmt.schema is not None:
            return fmt.schema
        handle = _open(storage, location)
        try:
            sample = list(iter_records(handle, fmt, limit=_INFERENCE_SAMPLE, describe=location.uri))
        finally:
            handle.close()
        return infer_schema(sample, value_tag=fmt.value_tag)

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
        fmt = _xml_format(data_format)
        if pushed.predicate is not None:
            raise UnsupportedOperationError(
                "the XML reader evaluates no predicates; the planner must keep them residual",
                code=DiagnosticCode.SOURCE_SCAN_UNSUPPORTED,
                details={"format": _FORMAT_NAME},
            )
        full = self.schema(storage, location, fmt)
        fields = full if pushed.projection is None else _select(full, pushed.projection)
        arrow_schema = to_arrow_schema(fields)
        handle = _open(storage, location)
        try:
            records = iter_records(handle, fmt, limit=pushed.limit, describe=location.uri)
            rows = _rows(records, fields, fmt.value_tag)
            return stream_from_rows(
                arrow_schema, rows, batch_size=batch_size, on_close=(handle.close,)
            )
        except BaseException:
            handle.close()
            raise

    def __repr__(self) -> str:
        return "XmlLocalHandler()"


def _select(schema: Schema, names: tuple[str, ...]) -> Schema:
    try:
        return schema.select(names)
    except KeyError as exc:
        raise SourceError(
            f"projected column {exc.args[0]!r} is not in the XML schema; "
            f"available: {', '.join(schema.names)}",
            code=DiagnosticCode.PLAN_UNKNOWN_COLUMN,
            details={"column": exc.args[0]},
        ) from None


def _rows(
    records: Iterable[Mapping[str, Any]], fields: Schema, value_tag: str
) -> Iterator[dict[str, Any]]:
    for record in records:
        yield {
            f.name: convert_value(record.get(f.name), f.data_type, value_tag=value_tag)
            for f in fields
        }


class XmlReaderSpecHandler:
    """Describe an :class:`~invariantql.domain.formats.XmlFormat` to a distributed engine.

    Constructor options
        None. ``reader_spec`` maps ``row_tag`` -> ``rowTag``,
        ``attribute_prefix`` -> ``attributePrefix``, ``value_tag`` ->
        ``valueTag`` and, when set, ``root_tag`` -> ``rootTag``; a declared
        ``schema`` is passed through so the engine does not infer one.

    Credential handling
        None; the URI is opaque and no option carries a secret.

    Connector requirement
        Spark 4 ships the ``xml`` data source. On Spark 3.5 the cluster needs
        the ``com.databricks:spark-xml_2.12`` package (its reader understands
        the same option names). The requirement is reported in ``requires``.

    Caveats
        Spark's reader infers types from the whole file, while the local
        handler samples the first 1000 records; declare a ``schema`` when the
        two engines must agree exactly.
    """

    @property
    def format_name(self) -> str:
        return _FORMAT_NAME

    def reader_spec(self, data_format: DataFormat, uri: str) -> ReaderSpec:
        fmt = _xml_format(data_format)
        options: dict[str, str] = {
            "rowTag": fmt.row_tag,
            "attributePrefix": fmt.attribute_prefix,
            "valueTag": fmt.value_tag,
        }
        if fmt.root_tag:
            options["rootTag"] = fmt.root_tag
        return ReaderSpec(_FORMAT_NAME, options, fmt.schema, requires=(SPARK_XML_REQUIREMENT,))

    def __repr__(self) -> str:
        return "XmlReaderSpecHandler()"


__all__ = [
    "SPARK_XML_REQUIREMENT",
    "XmlLocalHandler",
    "XmlReaderSpecHandler",
    "convert_value",
    "infer_schema",
    "infer_type",
    "iter_records",
]
