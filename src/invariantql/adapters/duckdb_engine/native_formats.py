"""DuckDB-native readers for CSV, Parquet, and JSON."""

from __future__ import annotations

from invariantql.adapters._shared.sqltext import sql_string
from invariantql.domain.diagnostics import DiagnosticCode, UnsupportedOperationError
from invariantql.domain.formats import CsvFormat, DataFormat, JsonFormat, ParquetFormat
from invariantql.domain.schema import Schema
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
)

NATIVE_FORMATS = frozenset({"csv", "parquet", "json"})


def duckdb_type(data_type: DataType) -> str:
    if isinstance(data_type, BooleanType):
        return "BOOLEAN"
    if isinstance(data_type, IntegerType):
        return {8: "TINYINT", 16: "SMALLINT", 32: "INTEGER", 64: "BIGINT"}[data_type.bits]
    if isinstance(data_type, FloatType):
        return "FLOAT" if data_type.bits == 32 else "DOUBLE"
    if isinstance(data_type, DecimalType):
        return f"DECIMAL({data_type.precision},{data_type.scale})"
    if isinstance(data_type, StringType):
        return "VARCHAR"
    if isinstance(data_type, BinaryType):
        return "BLOB"
    if isinstance(data_type, DateType):
        return "DATE"
    if isinstance(data_type, TimestampType):
        return "TIMESTAMPTZ" if data_type.timezone else "TIMESTAMP"
    if isinstance(data_type, ListType):
        return f"{duckdb_type(data_type.element)}[]"
    if isinstance(data_type, StructType):
        inner = ", ".join(
            '"' + n.replace('"', '""') + '" ' + duckdb_type(t) for n, t in data_type.fields
        )
        return f"STRUCT({inner})"
    return "VARCHAR"


def _columns_clause(schema: Schema) -> str:
    items = ", ".join(
        f"{sql_string(f.name)}: {sql_string(duckdb_type(f.data_type))}" for f in schema
    )
    return "{" + items + "}"


def relation_sql(data_format: DataFormat, path: str) -> str:
    """The DuckDB table-function call that reads ``path`` as ``data_format``."""

    p = sql_string(path)
    if isinstance(data_format, ParquetFormat):
        return f"read_parquet({p}, hive_partitioning={'true' if data_format.hive_partitioning else 'false'})"
    if isinstance(data_format, CsvFormat):
        opts = [
            f"delim={sql_string(data_format.delimiter)}",
            f"header={'true' if data_format.header else 'false'}",
            f"quote={sql_string(data_format.quote)}",
            f"skip={int(data_format.skip_rows)}",
            f"encoding={sql_string(data_format.encoding)}",
        ]
        if data_format.escape is not None:
            opts.append(f"escape={sql_string(data_format.escape)}")
        if data_format.null_values:
            opts.append(
                "nullstr=[" + ", ".join(sql_string(v) for v in data_format.null_values) + "]"
            )
        if data_format.compression is not None:
            opts.append(f"compression={sql_string(data_format.compression)}")
        if data_format.date_format is not None:
            opts.append(f"dateformat={sql_string(data_format.date_format)}")
        if data_format.timestamp_format is not None:
            opts.append(f"timestampformat={sql_string(data_format.timestamp_format)}")
        if data_format.schema is not None:
            opts.append(f"columns={_columns_clause(data_format.schema)}")
        else:
            opts.append("auto_detect=true")
        return f"read_csv({p}, {', '.join(opts)})"
    if isinstance(data_format, JsonFormat):
        opts = [f"format={sql_string('newline_delimited' if data_format.lines else 'array')}"]
        if data_format.compression is not None:
            opts.append(f"compression={sql_string(data_format.compression)}")
        if data_format.schema is not None:
            opts.append(f"columns={_columns_clause(data_format.schema)}")
        return f"read_json({p}, {', '.join(opts)})"
    raise UnsupportedOperationError(
        f"DuckDB has no native reader for format {data_format.format_name!r}",
        code=DiagnosticCode.FORMAT_UNSUPPORTED,
        details={"format": data_format.format_name},
    )


__all__ = ["NATIVE_FORMATS", "duckdb_type", "relation_sql"]
