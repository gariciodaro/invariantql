"""FF-07: the declared portable profile produces equivalent results on DuckDB and Spark."""

from __future__ import annotations

import datetime as dt
import math
from decimal import Decimal
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import invariantql as iql
from invariantql.adapters._shared.arrow import from_arrow_schema
from invariantql.adapters.spark_engine.engine import from_spark_schema

pytestmark = [pytest.mark.portability, pytest.mark.spark]

CORPUS = [
    "SELECT * FROM {src}",
    "SELECT id, name FROM {src} WHERE name IS NOT NULL",
    "SELECT id FROM {src} WHERE name IS NULL",
    "SELECT id, amount * 2 AS twice, qty + 1 AS q1, amount - qty AS diff, amount / 4 AS quarter FROM {src}",
    "SELECT id FROM {src} WHERE amount > 7 AND amount <= 100",
    "SELECT id FROM {src} WHERE name = 'alice' OR name = 'Alice'",
    "SELECT id FROM {src} WHERE name <> 'alice'",
    "SELECT id FROM {src} WHERE NOT (qty > 2)",
    "SELECT id FROM {src} WHERE name LIKE 'a%'",
    "SELECT id FROM {src} WHERE name LIKE '%a%'",
    "SELECT id FROM {src} WHERE name LIKE '_ob'",
    "SELECT id FROM {src} WHERE name NOT LIKE 'a%'",
    "SELECT id FROM {src} WHERE id IN (1, 3, 99)",
    "SELECT id FROM {src} WHERE name NOT IN ('alice', 'bob')",
    "SELECT id FROM {src} WHERE qty BETWEEN 1 AND 3",
    "SELECT id FROM {src} WHERE day >= DATE '2024-01-03'",
    "SELECT id FROM {src} WHERE active = TRUE",
    "SELECT id FROM {src} WHERE active <> TRUE",
    "SELECT id FROM {src} WHERE price > 2",
    "SELECT id FROM {src} WHERE qty / 2 >= 1",
    "SELECT id, price / qty AS ratio FROM {src}",
    "SELECT id FROM {src} WHERE amount > :min AND name LIKE :pat",
    "SELECT id FROM {src} WHERE id = 4 LIMIT 5",
    "SELECT id FROM {src} WHERE id > 100 LIMIT 1",
]
PARAMS = {"min": 5, "pat": "%a%"}


def _normalise(value):
    if isinstance(value, float):
        return round(value, 9)
    if isinstance(value, Decimal):
        return Decimal(value).quantize(Decimal("0.000000001"))
    if isinstance(value, dt.datetime):
        return value.replace(tzinfo=None).isoformat()
    return value


def _rows(records: list[dict]) -> list[tuple]:
    out = [tuple(_normalise(v) for v in r.values()) for r in records]
    return sorted(out, key=lambda t: tuple((x is None, str(x)) for x in t))


@pytest.mark.parametrize("src", ["orders", "orders_csv", "orders_json"])
@pytest.mark.parametrize("sql", CORPUS)
def test_duckdb_and_spark_agree(ctx, spark, src: str, sql: str) -> None:
    ctx.use_spark(spark)
    query = ctx.sql(sql.format(src=src))
    params = {k: v for k, v in PARAMS.items() if k in query.parameters}
    assert query.is_portable("duckdb", "spark")

    local = query.execute(engine="duckdb", params=params).to_arrow()
    df = query.compile(engine="spark", params=params)
    remote = [row.asDict() for row in df.collect()]

    assert list(local.schema.names) == df.columns
    local_rows = _rows(local.to_pylist())
    remote_rows = _rows(remote)
    assert len(local_rows) == len(remote_rows), (local_rows, remote_rows)
    for a, b in zip(local_rows, remote_rows, strict=True):
        for x, y in zip(a, b, strict=True):
            if isinstance(x, float) and isinstance(y, float):
                assert math.isclose(x, y, rel_tol=1e-9, abs_tol=1e-9), (a, b)
            else:
                assert x == y, (a, b)


def test_output_schema_agrees(ctx, spark, sample_schema) -> None:
    ctx.use_spark(spark)
    query = ctx.sql(
        "SELECT id, price + price AS added, price * price AS multiplied, "
        "price / qty AS ratio, price + qty AS mixed, "
        "2147483647 + 1 AS widened, 9223372036854775807 + 1 AS overflowed, "
        "NULL AS absent FROM orders"
    )
    logical = query.schema(engine="duckdb")
    assert logical == query.schema(engine="spark")

    local = query.execute(engine="duckdb").to_arrow()
    remote = query.compile(engine="spark")
    assert from_arrow_schema(local.schema) == logical
    assert from_spark_schema(remote.schema) == logical

    local_rows = {row["id"]: row for row in local.to_pylist()}
    remote_rows = {row["id"]: row.asDict() for row in remote.collect()}
    assert local_rows[6]["ratio"] is None
    assert remote_rows[6]["ratio"] is None
    assert local_rows[1]["widened"] == remote_rows[1]["widened"] == 2_147_483_648
    assert local_rows[1]["overflowed"] is remote_rows[1]["overflowed"] is None
    assert local_rows[1]["absent"] is remote_rows[1]["absent"] is None


def test_parameter_type_is_resolved_at_execution(ctx, spark) -> None:
    ctx.use_spark(spark)
    query = ctx.sql("SELECT :value AS value FROM orders LIMIT 1")

    local = query.execute(engine="duckdb", params={"value": 7}).to_arrow()
    remote = query.compile(engine="spark", params={"value": 7})

    assert from_arrow_schema(local.schema) == from_spark_schema(remote.schema)
    assert str(from_arrow_schema(local.schema).field("value").data_type) == "int64"
    assert local.to_pylist() == [{"value": 7}]
    assert [row.asDict() for row in remote.collect()] == [{"value": 7}]


def test_aware_timestamp_schema_and_values_normalise_to_utc(ctx, spark, tmp_path) -> None:
    instant = dt.datetime(2024, 3, 31, 1, 30, tzinfo=ZoneInfo("Europe/Berlin"))
    pq.write_table(
        pa.table({"occurred_at": pa.array([instant], type=pa.timestamp("us", "Europe/Berlin"))}),
        tmp_path / "timestamps.parquet",
    )
    ctx.register_source(
        iql.file_source(
            "timestamps",
            iql.local_storage(tmp_path),
            "timestamps.parquet",
            iql.ParquetFormat(),
        )
    )
    ctx.use_spark(spark)
    query = ctx.sql("SELECT occurred_at FROM timestamps")

    logical = query.schema(engine="duckdb")
    assert logical == query.schema(engine="spark")
    assert str(logical.field("occurred_at").data_type) == "timestamp[UTC]"

    local = query.execute(engine="duckdb").to_arrow()
    remote = query.compile(engine="spark")
    assert from_arrow_schema(local.schema) == logical
    assert from_spark_schema(remote.schema) == logical
    from pyspark.sql import functions as F

    local_value = local.to_pylist()[0]["occurred_at"]
    spark_micros = remote.select(F.unix_micros("occurred_at")).collect()[0][0]
    assert int(local_value.timestamp() * 1_000_000) == spark_micros


def test_small_integer_arithmetic_widens_before_evaluation(ctx, spark, tmp_path) -> None:
    pq.write_table(
        pa.table({"tiny": pa.array([100], type=pa.int8())}),
        tmp_path / "tiny.parquet",
    )
    ctx.register_source(
        iql.file_source(
            "tiny",
            iql.local_storage(tmp_path),
            "tiny.parquet",
            iql.ParquetFormat(),
        )
    )
    ctx.use_spark(spark)
    query = ctx.sql("SELECT tiny + tiny AS added, tiny * tiny AS multiplied FROM tiny")

    local = query.execute(engine="duckdb").to_arrow()
    remote = query.compile(engine="spark")
    assert from_arrow_schema(local.schema) == from_spark_schema(remote.schema)
    assert local.to_pylist() == [{"added": 200, "multiplied": 10_000}]
    assert [row.asDict() for row in remote.collect()] == [{"added": 200, "multiplied": 10_000}]


def test_decimal_and_int64_arithmetic_preserves_all_digits(ctx, spark, tmp_path) -> None:
    pq.write_table(
        pa.table(
            {
                "fraction": pa.array(
                    [Decimal("0.123456789012345678")],
                    type=pa.decimal128(18, 18),
                ),
                "maximum": pa.array([9_223_372_036_854_775_807], type=pa.int64()),
            }
        ),
        tmp_path / "decimal_int64.parquet",
    )
    ctx.register_source(
        iql.file_source(
            "decimal_int64",
            iql.local_storage(tmp_path),
            "decimal_int64.parquet",
            iql.ParquetFormat(),
        )
    )
    ctx.use_spark(spark)
    query = ctx.sql(
        "SELECT fraction + maximum AS added, maximum - fraction AS subtracted, "
        "fraction * maximum AS multiplied FROM decimal_int64"
    )

    expected = {
        "added": Decimal("9223372036854775807.123456789012345678"),
        "subtracted": Decimal("9223372036854775806.876543210987654322"),
        "multiplied": Decimal("1138687895536349061.688316917875412146"),
    }
    logical = query.schema(engine="duckdb")
    assert logical == query.schema(engine="spark")
    local = query.execute(engine="duckdb").to_arrow()
    remote = query.compile(engine="spark")
    assert from_arrow_schema(local.schema) == logical
    assert from_spark_schema(remote.schema) == logical
    assert local.to_pylist() == [expected]
    assert [row.asDict() for row in remote.collect()] == [expected]
