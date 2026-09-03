"""DuckDB engine behaviour over local files and bridged storage."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import invariantql as iql
from invariantql.adapters.storage.fsspec_storage import FsspecStorage
from invariantql.domain import DiagnosticCode, Disposition
from invariantql.ports.storage import StorageCapabilities


def _rows(result) -> list[dict]:
    return sorted(result.to_arrow().to_pylist(), key=lambda r: r["id"])


@pytest.mark.parametrize("source", ["orders", "orders_csv", "orders_json"])
def test_same_query_same_rows_on_every_native_format(ctx, source: str, sample_rows) -> None:
    q = ctx.sql(
        f"SELECT id, name, amount * 2 AS doubled FROM {source} WHERE amount > :min AND name IS NOT NULL"
    )
    rows = _rows(q.execute(params={"min": 6}))
    expected = [
        {"id": r["id"], "name": r["name"], "doubled": r["amount"] * 2}
        for r in sample_rows
        if r["amount"] > 6 and r["name"] is not None
    ]
    assert rows == expected


def test_typed_values_survive(ctx) -> None:
    rows = _rows(ctx.sql("SELECT id, day, active, price FROM orders WHERE id IN (1, 4)").execute())
    assert rows[0] == {
        "id": 1,
        "day": dt.date(2024, 1, 1),
        "active": True,
        "price": Decimal("1.10"),
    }
    assert rows[1] == {"id": 4, "day": dt.date(2024, 1, 4), "active": None, "price": None}


def test_predicate_semantics(ctx) -> None:
    sql = "SELECT id FROM orders WHERE {}"
    cases = {
        "name = 'alice'": [1],
        "name <> 'alice'": [2, 3, 4, 6],  # NULL name excluded, case-sensitive
        "name LIKE 'a%'": [1],
        "name LIKE '%a%'": [1, 3, 4],
        "name LIKE '_ob'": [2],
        "NOT (amount > 7)": [3, 5, 6],
        "qty IS NULL": [3],
        "qty IS NOT NULL AND qty / 2 > 1": [1, 4],
        "day BETWEEN DATE '2024-01-02' AND DATE '2024-01-03'": [2, 3],
        "active = TRUE OR qty = 0": [1, 3, 6],
        "id IN (1, 2) AND id NOT IN (2)": [1],
        "price > 2": [2, 3, 5],
    }
    for predicate, expected in cases.items():
        got = sorted(
            r["id"] for r in ctx.sql(sql.format(predicate)).execute().to_arrow().to_pylist()
        )
        assert got == expected, predicate


def test_case_insensitive_column_resolution_and_output_names(ctx) -> None:
    result = ctx.sql("SELECT ID, Amount AS total FROM orders WHERE id = 1").execute()
    assert result.schema.names == ["id", "total"]
    assert result.to_arrow().to_pylist() == [{"id": 1, "total": 10.5}]


def test_explain_dispositions_for_duckdb(ctx) -> None:
    explain = ctx.sql(
        "SELECT id, amount + 1 AS a1 FROM orders WHERE name LIKE 'a%' LIMIT 2"
    ).explain()
    assert explain.engine == "duckdb" and explain.executable
    dispositions = {n.operation: n.disposition for n in explain.nodes}
    assert dispositions == {
        "scan": Disposition.PUSHED,
        "filter": Disposition.PUSHED,
        "project": Disposition.PARTIAL,
        "limit": Disposition.PUSHED,
    }
    assert "DuckDB native parquet reader" in explain.nodes[0].evidence[0]


def test_preview_is_bounded_and_materialisation_is_explicit(ctx) -> None:
    result = ctx.sql("SELECT * FROM orders").preview(2)
    assert result.to_arrow().num_rows == 2
    with pytest.raises(iql.MaterializationLimitError) as info:
        ctx.sql("SELECT * FROM orders").execute().to_arrow(max_rows=3)
    assert info.value.code is DiagnosticCode.RESULT_LIMIT_EXCEEDED
    assert ctx.sql("SELECT * FROM orders").execute().to_arrow(max_rows=None).num_rows == 6
    frame = ctx.sql("SELECT id FROM orders WHERE id < 3").execute().to_pandas()
    assert list(frame["id"]) == [1, 2]
    polars = pytest.importorskip("polars")
    df = ctx.sql("SELECT id FROM orders WHERE id < 3").execute().to_polars()
    assert isinstance(df, polars.DataFrame) and df.height == 2


def test_results_stream_in_batches_and_close_once(ctx) -> None:
    result = ctx.sql("SELECT id FROM orders").execute(batch_size=2)
    batches = list(result)
    assert sum(b.num_rows for b in batches) == 6
    assert all(b.num_rows <= 2 for b in batches)
    assert result.closed
    result.close()  # idempotent
    with pytest.raises(iql.MaterializationLimitError) as info:
        result.to_arrow()
    assert info.value.code is DiagnosticCode.RESULT_CLOSED
    with ctx.sql("SELECT id FROM orders").execute() as managed:
        assert managed.schema.names == ["id"]
    assert managed.closed


def test_hive_partitioned_directory(ctx, data_dir) -> None:
    storage = iql.local_storage(data_dir)
    ctx.register_source(
        iql.file_source(
            "parts", storage, "partitioned/**/*.parquet", iql.ParquetFormat(hive_partitioning=True)
        )
    )
    rows = ctx.sql("SELECT id, year FROM parts WHERE year = 2023").execute().to_arrow().to_pylist()
    assert sorted(r["id"] for r in rows) == [1, 2, 3]
    assert {r["year"] for r in rows} == {2023}


def test_inferred_csv_schema_runs(ctx) -> None:
    schema = ctx.sql("SELECT * FROM orders_csv_inferred").schema()
    assert schema.names == ("id", "name", "amount", "qty", "day", "active", "price")
    assert (
        ctx.sql("SELECT id FROM orders_csv_inferred WHERE id = 2").execute().to_arrow().num_rows
        == 1
    )


def test_storage_without_native_uri_goes_through_the_bridge(
    ctx, sample_rows, sample_schema
) -> None:
    from invariantql.adapters._shared.arrow import to_arrow_schema

    fs = fsspec.filesystem("memory")
    with fs.open("bucket/data/orders.parquet", "wb") as handle:
        pq.write_table(
            pa.Table.from_pylist(sample_rows, schema=to_arrow_schema(sample_schema)), handle
        )
    storage = FsspecStorage(
        fs,
        name="memory-bucket",
        scheme="memory",
        netloc="bucket",
        capabilities=StorageCapabilities(range_reads=True, engine_visible_uri=False),
    )
    assert storage.native_uri(storage.resolve("data/orders.parquet")) is None
    ctx.register_source(iql.file_source("mem", storage, "data/orders.parquet", iql.ParquetFormat()))
    explain = ctx.sql("SELECT id FROM mem WHERE id > :n").explain()
    assert explain.executable and not explain.staging_required
    rows = (
        ctx.sql("SELECT id FROM mem WHERE id > :n").execute(params={"n": 4}).to_arrow().to_pylist()
    )
    assert sorted(r["id"] for r in rows) == [5, 6]
    # bridge mounts are released after execution
    assert ctx.engine("duckdb")._bridge._mounts == {}


def test_missing_file_is_a_storage_diagnostic(ctx, data_dir) -> None:
    ctx.register_source(
        iql.file_source("nope", iql.local_storage(data_dir), "missing.parquet", iql.ParquetFormat())
    )
    with pytest.raises(iql.InvariantQLError) as info:
        ctx.sql("SELECT * FROM nope").explain()
    assert info.value.code is DiagnosticCode.SOURCE_SCHEMA_UNAVAILABLE


def test_unknown_source_and_engine(ctx) -> None:
    with pytest.raises(iql.InvariantQLError) as info:
        ctx.sql("SELECT * FROM ghost").explain()
    assert info.value.code is DiagnosticCode.SOURCE_NOT_REGISTERED
    with pytest.raises(iql.InvariantQLError) as info:
        ctx.sql("SELECT * FROM orders").explain(engine="nope")
    assert info.value.code is DiagnosticCode.ENGINE_UNKNOWN


def test_builder_frontend_matches_sql(ctx) -> None:
    built = (
        ctx.query("orders")
        .where((iql.col("amount") >= 10) & iql.col("name").like("%a%"))
        .select("id", (iql.col("amount") / 3).alias("third"))
        .limit(10)
    )
    sql = ctx.sql(
        "SELECT id, amount / 3 AS third FROM orders WHERE amount >= 10 AND name LIKE '%a%' LIMIT 10"
    )
    assert built.fingerprint() == sql.fingerprint()
    assert _rows(built.execute()) == _rows(sql.execute())
    assert [r["id"] for r in _rows(built.execute())] == [1, 4]
