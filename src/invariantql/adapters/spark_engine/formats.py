"""Spark reader specifications for the formats Spark reads natively."""

from __future__ import annotations

from invariantql.domain.diagnostics import DiagnosticCode, UnsupportedOperationError
from invariantql.domain.formats import CsvFormat, JsonFormat, ParquetFormat
from invariantql.ports.format_handler import ReaderSpec

NATIVE_FORMATS = frozenset({"csv", "parquet", "json"})


def csv_options(fmt: CsvFormat) -> ReaderSpec:
    options: dict[str, str] = {
        "sep": fmt.delimiter,
        "header": "true" if fmt.header else "false",
        "quote": fmt.quote,
        "encoding": fmt.encoding,
        "mode": "FAILFAST",
    }
    if fmt.escape is not None:
        options["escape"] = fmt.escape
    if fmt.skip_rows:
        raise UnsupportedOperationError(
            "Spark's CSV reader cannot skip leading rows; remove skip_rows or preprocess the file",
            code=DiagnosticCode.FORMAT_INVALID,
            details={"format": "csv", "option": "skip_rows"},
        )
    if fmt.null_values:
        if len(fmt.null_values) != 1:
            raise UnsupportedOperationError(
                "Spark's CSV reader supports a single null marker; declare at most one null value",
                code=DiagnosticCode.FORMAT_INVALID,
                details={"format": "csv", "option": "null_values"},
            )
        (options["nullValue"],) = fmt.null_values
    if fmt.date_format is not None:
        options["dateFormat"] = fmt.date_format
    if fmt.timestamp_format is not None:
        options["timestampFormat"] = fmt.timestamp_format
    if fmt.schema is None:
        options["inferSchema"] = "true"
    return ReaderSpec("csv", options, fmt.schema)


def parquet_options(fmt: ParquetFormat) -> ReaderSpec:
    return ReaderSpec("parquet", {}, None)


def json_options(fmt: JsonFormat) -> ReaderSpec:
    options = {"multiLine": "false" if fmt.lines else "true", "mode": "FAILFAST"}
    return ReaderSpec("json", options, fmt.schema)


__all__ = ["NATIVE_FORMATS", "csv_options", "json_options", "parquet_options"]
