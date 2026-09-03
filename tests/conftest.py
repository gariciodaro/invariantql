"""Shared fixtures: sample datasets, a context, and an optional local Spark session."""

from __future__ import annotations

import datetime as dt
import glob
import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src" / "invariantql"
DOCS_ROOT = REPO_ROOT / "docs" / "architecture"


def _ensure_java_home() -> str | None:
    if os.environ.get("JAVA_HOME"):
        return os.environ["JAVA_HOME"]
    for candidate in sorted(glob.glob(os.path.expanduser("~/.jdk/jdk-17*"))) + sorted(
        glob.glob("/usr/lib/jvm/java-17*")
    ):
        if Path(candidate, "bin", "java").exists():
            os.environ["JAVA_HOME"] = candidate
            os.environ["PATH"] = f"{candidate}/bin:" + os.environ.get("PATH", "")
            return candidate
    return None


@pytest.fixture(scope="session")
def sample_rows() -> list[dict]:
    return [
        {
            "id": 1,
            "name": "alice",
            "amount": 10.5,
            "qty": 3,
            "day": dt.date(2024, 1, 1),
            "active": True,
            "price": Decimal("1.10"),
        },
        {
            "id": 2,
            "name": "bob",
            "amount": 20.0,
            "qty": 1,
            "day": dt.date(2024, 1, 2),
            "active": False,
            "price": Decimal("2.20"),
        },
        {
            "id": 3,
            "name": "carol",
            "amount": 5.25,
            "qty": None,
            "day": dt.date(2024, 1, 3),
            "active": True,
            "price": Decimal("3.30"),
        },
        {
            "id": 4,
            "name": "dave",
            "amount": 100.0,
            "qty": 7,
            "day": dt.date(2024, 1, 4),
            "active": None,
            "price": None,
        },
        {
            "id": 5,
            "name": None,
            "amount": 7.0,
            "qty": 2,
            "day": None,
            "active": False,
            "price": Decimal("5.50"),
        },
        {
            "id": 6,
            "name": "Alice",
            "amount": 0.0,
            "qty": 0,
            "day": dt.date(2024, 1, 6),
            "active": True,
            "price": Decimal("0.00"),
        },
    ]


@pytest.fixture(scope="session")
def sample_schema():
    from invariantql.domain import (
        BooleanType,
        DateType,
        DecimalType,
        FloatType,
        IntegerType,
        Schema,
        StringType,
    )

    return Schema.of(
        ("id", IntegerType(64)),
        ("name", StringType()),
        ("amount", FloatType(64)),
        ("qty", IntegerType(64)),
        ("day", DateType()),
        ("active", BooleanType()),
        ("price", DecimalType(10, 2)),
    )


@pytest.fixture(scope="session")
def data_dir(tmp_path_factory, sample_rows, sample_schema) -> Path:
    """Sample data as Parquet, CSV, and NDJSON files."""

    import pyarrow as pa
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq

    from invariantql.adapters._shared.arrow import to_arrow_schema

    root = tmp_path_factory.mktemp("data")
    table = pa.Table.from_pylist(sample_rows, schema=to_arrow_schema(sample_schema))
    pq.write_table(table, root / "orders.parquet")
    pcsv.write_csv(table, root / "orders.csv")
    with open(root / "orders.ndjson", "w", encoding="utf-8") as handle:
        for row in sample_rows:
            encoded = {
                **row,
                "day": None if row["day"] is None else row["day"].isoformat(),
                "price": None if row["price"] is None else str(row["price"]),
            }
            handle.write(json.dumps(encoded) + "\n")
    (root / "partitioned").mkdir()
    for year in (2023, 2024):
        part = root / "partitioned" / f"year={year}"
        part.mkdir()
        pq.write_table(table.slice(0, 3 if year == 2023 else 6), part / "part-0.parquet")
    return root


@pytest.fixture()
def ctx(data_dir, sample_schema):
    import invariantql as iql

    context = iql.Context()
    storage = iql.local_storage(data_dir)
    context.register_source(
        iql.file_source("orders", storage, "orders.parquet", iql.ParquetFormat())
    )
    context.register_source(
        iql.file_source("orders_csv", storage, "orders.csv", iql.CsvFormat(schema=sample_schema))
    )
    context.register_source(
        iql.file_source(
            "orders_json", storage, "orders.ndjson", iql.JsonFormat(schema=sample_schema)
        )
    )
    context.register_source(
        iql.file_source("orders_csv_inferred", storage, "orders.csv", iql.CsvFormat())
    )
    yield context
    context.close()


@pytest.fixture(scope="session")
def spark():
    """A local SparkSession, or skip when no JVM/pyspark is available."""

    if _ensure_java_home() is None:
        pytest.skip("no JDK 17 found (set JAVA_HOME)")
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("invariantql-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()
    del pyspark
