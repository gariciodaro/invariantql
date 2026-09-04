"""Arrow helpers shared by adapters: type mapping and batch streams."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from typing import Any

import pyarrow as pa

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


def to_arrow_type(data_type: DataType) -> pa.DataType:
    if isinstance(data_type, BooleanType):
        return pa.bool_()
    if isinstance(data_type, IntegerType):
        return {8: pa.int8(), 16: pa.int16(), 32: pa.int32(), 64: pa.int64()}[data_type.bits]
    if isinstance(data_type, FloatType):
        return pa.float32() if data_type.bits == 32 else pa.float64()
    if isinstance(data_type, DecimalType):
        if data_type.precision <= 38:
            return pa.decimal128(data_type.precision, data_type.scale)
        return pa.decimal256(data_type.precision, data_type.scale)
    if isinstance(data_type, StringType):
        return pa.string()
    if isinstance(data_type, BinaryType):
        return pa.binary()
    if isinstance(data_type, DateType):
        return pa.date32()
    if isinstance(data_type, TimestampType):
        return pa.timestamp("us", tz=data_type.timezone)
    if isinstance(data_type, ListType):
        return pa.list_(to_arrow_type(data_type.element))
    if isinstance(data_type, StructType):
        return pa.struct([pa.field(n, to_arrow_type(t)) for n, t in data_type.fields])
    if isinstance(data_type, NullType):
        return pa.null()
    return pa.string()


def from_arrow_type(arrow_type: pa.DataType) -> DataType:
    if pa.types.is_boolean(arrow_type):
        return BooleanType()
    if pa.types.is_integer(arrow_type):
        return IntegerType(
            min(
                64,
                max(
                    8,
                    arrow_type.bit_width
                    if not pa.types.is_unsigned_integer(arrow_type)
                    else min(64, arrow_type.bit_width * 2),
                ),
            )
        )
    if pa.types.is_floating(arrow_type):
        return FloatType(
            32 if pa.types.is_float32(arrow_type) or pa.types.is_float16(arrow_type) else 64
        )
    if pa.types.is_decimal(arrow_type):
        return DecimalType(arrow_type.precision, arrow_type.scale)
    if (
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_string_view(arrow_type)
    ):
        return StringType()
    if (
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_fixed_size_binary(arrow_type)
        or pa.types.is_binary_view(arrow_type)
    ):
        return BinaryType()
    if pa.types.is_date(arrow_type):
        return DateType()
    if pa.types.is_timestamp(arrow_type):
        return TimestampType(arrow_type.tz)
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return ListType(from_arrow_type(arrow_type.value_type))
    if pa.types.is_struct(arrow_type):
        return StructType(tuple((f.name, from_arrow_type(f.type)) for f in arrow_type))
    if pa.types.is_null(arrow_type):
        return NullType()
    if pa.types.is_dictionary(arrow_type):
        return from_arrow_type(arrow_type.value_type)
    return UnknownType()


def to_arrow_schema(schema: Schema) -> pa.Schema:
    return pa.schema([pa.field(f.name, to_arrow_type(f.data_type), f.nullable) for f in schema])


def from_arrow_schema(schema: pa.Schema) -> Schema:
    return Schema(tuple(Field(f.name, from_arrow_type(f.type), f.nullable) for f in schema))


class ArrowStream:
    """A ``RecordBatchStream`` over a ``pyarrow.RecordBatchReader`` with close hooks."""

    def __init__(
        self, reader: pa.RecordBatchReader, *, on_close: Iterable[Callable[[], None]] = ()
    ):
        self._reader = reader
        self._on_close = list(on_close)
        self._closed = False

    @property
    def schema(self) -> pa.Schema:
        return self._reader.schema

    @property
    def closed(self) -> bool:
        return self._closed

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        if self._closed:
            return iter(())
        return self._iterate()

    def _iterate(self) -> Iterator[pa.RecordBatch]:
        try:
            yield from self._reader
        finally:
            self.close()

    def read_all(self) -> pa.Table:
        try:
            return pa.Table.from_batches(list(self), self._reader.schema)
        finally:
            self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        errors: list[BaseException] = []
        try:
            self._reader.close()
        except Exception as exc:
            errors.append(exc)
        for hook in self._on_close:
            try:
                hook()
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise errors[0]

    def __enter__(self) -> ArrowStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def stream_from_batches(
    schema: pa.Schema,
    batches: Iterable[pa.RecordBatch],
    *,
    on_close: Iterable[Callable[[], None]] = (),
) -> ArrowStream:
    return ArrowStream(pa.RecordBatchReader.from_batches(schema, batches), on_close=on_close)


def stream_from_rows(
    schema: pa.Schema,
    rows: Iterable[dict[str, Any]],
    *,
    batch_size: int,
    on_close: Iterable[Callable[[], None]] = (),
) -> ArrowStream:
    """Turn an iterator of row dicts into a batch stream with bounded memory."""

    def batches() -> Iterator[pa.RecordBatch]:
        buffer: list[dict[str, Any]] = []
        for row in rows:
            buffer.append(row)
            if len(buffer) >= batch_size:
                yield pa.RecordBatch.from_pylist(buffer, schema=schema)
                buffer = []
        if buffer:
            yield pa.RecordBatch.from_pylist(buffer, schema=schema)

    return stream_from_batches(schema, batches(), on_close=on_close)


def empty_stream(schema: pa.Schema) -> ArrowStream:
    return stream_from_batches(schema, [])


def stream_from_table(table: pa.Table, *, batch_size: int) -> ArrowStream:
    return stream_from_batches(table.schema, table.to_batches(max_chunksize=batch_size))


__all__ = [
    "ArrowStream",
    "empty_stream",
    "from_arrow_schema",
    "from_arrow_type",
    "stream_from_batches",
    "stream_from_rows",
    "stream_from_table",
    "to_arrow_schema",
    "to_arrow_type",
]
