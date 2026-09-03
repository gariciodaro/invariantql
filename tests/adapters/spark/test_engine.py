"""Spark engine: lazy compilation (FF-08), staging diagnostics, expression translation."""

from __future__ import annotations

import pytest

import invariantql as iql
from invariantql.domain import DiagnosticCode, Disposition

pytestmark = pytest.mark.spark

ACTIONS = [
    "collect",
    "count",
    "show",
    "toPandas",
    "first",
    "take",
    "head",
    "foreach",
    "toLocalIterator",
    "write",
]


@pytest.fixture()
def spark_ctx(ctx, spark):
    ctx.use_spark(spark)
    return ctx


def test_compile_returns_a_lazy_dataframe_without_actions(spark_ctx, monkeypatch) -> None:
    from pyspark.sql import DataFrame

    def boom(*_a, **_k):  # pragma: no cover - must never run
        raise AssertionError("Spark action during compile")

    for action in ACTIONS:
        if hasattr(DataFrame, action):
            monkeypatch.setattr(DataFrame, action, boom, raising=True)
    df = spark_ctx.sql(
        "SELECT id, amount * 2 AS d FROM orders WHERE name LIKE 'a%' AND id IN (:a, :b) LIMIT 3"
    ).compile(engine="spark", params={"a": 1, "b": 2})
    assert isinstance(df, DataFrame)
    assert df.columns == ["id", "d"]
    monkeypatch.undo()
    assert df.collect()[0].asDict() == {"id": 1, "d": 21.0}


def test_explain_for_spark_is_executable_on_local_files(spark_ctx) -> None:
    explain = spark_ctx.sql("SELECT id FROM orders WHERE amount > 1 LIMIT 2").explain(
        engine="spark"
    )
    assert explain.executable and explain.engine == "spark"
    assert all(n.disposition is Disposition.PUSHED for n in explain.nodes)
    assert spark_ctx.sql("SELECT id FROM orders").is_portable("duckdb", "spark")


def test_unreachable_storage_requires_explicit_staging(
    spark_ctx, sample_rows, sample_schema
) -> None:
    import fsspec
    import pyarrow as pa
    import pyarrow.parquet as pq

    from invariantql.adapters._shared.arrow import to_arrow_schema
    from invariantql.adapters.storage.fsspec_storage import FsspecStorage
    from invariantql.ports.storage import StorageCapabilities

    fs = fsspec.filesystem("memory")
    with fs.open("spark-bucket/orders.parquet", "wb") as handle:
        pq.write_table(
            pa.Table.from_pylist(sample_rows, schema=to_arrow_schema(sample_schema)), handle
        )
    storage = FsspecStorage(
        fs,
        name="mem",
        scheme="memory",
        netloc="spark-bucket",
        capabilities=StorageCapabilities(range_reads=True),
    )
    spark_ctx.register_source(
        iql.file_source("mem", storage, "orders.parquet", iql.ParquetFormat())
    )
    explain = spark_ctx.sql("SELECT id FROM mem").explain(engine="spark")
    assert explain.staging_required and not explain.executable
    assert explain.diagnostics[0].code is DiagnosticCode.STAGING_REQUIRED
    with pytest.raises(iql.StagingRequiredError):
        spark_ctx.sql("SELECT id FROM mem").compile(engine="spark")
    assert not spark_ctx.sql("SELECT id FROM mem").is_portable("duckdb", "spark")
    assert spark_ctx.sql("SELECT id FROM mem").is_portable("duckdb")


def test_execute_on_a_compiling_engine_is_refused(spark_ctx) -> None:
    with pytest.raises(iql.UnsupportedOperationError):
        spark_ctx.sql("SELECT id FROM orders").execute(engine="spark")
    with pytest.raises(iql.UnsupportedOperationError):
        spark_ctx.sql("SELECT id FROM orders").compile(engine="duckdb")


def test_schema_roundtrip(spark, sample_schema) -> None:
    from invariantql.adapters.spark_engine.engine import from_spark_schema, to_spark_schema

    assert from_spark_schema(to_spark_schema(sample_schema)) == sample_schema


def test_session_is_not_mutated_by_default(spark_ctx, spark, data_dir) -> None:
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    before = conf.get("fs.s3a.access.key")
    spark_ctx.sql("SELECT id FROM orders").compile(engine="spark")
    assert conf.get("fs.s3a.access.key") == before
