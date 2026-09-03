"""FF-07: the declared portable profile produces equivalent results on DuckDB and Spark."""

from __future__ import annotations

import datetime as dt
import math
from decimal import Decimal

import pytest

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
    query = ctx.sql("SELECT id, name, amount / 2 AS half, price, day, active FROM orders")
    assert query.schema(engine="duckdb") == query.schema(engine="spark")
