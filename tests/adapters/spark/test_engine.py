"""Spark engine: lazy compilation (FF-08), staging diagnostics, expression translation."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import invariantql as iql
from invariantql.domain import DiagnosticCode, Disposition, SecretOptions

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


def test_compile_returns_a_lazy_dataframe_without_collection_or_write(
    spark_ctx, monkeypatch
) -> None:
    from pyspark.sql import DataFrame

    def boom(*_a, **_k):  # pragma: no cover - must never run
        raise AssertionError("Spark collection or write during compile")

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


class _FakeHadoopConfiguration:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def set(self, key: str, value: str) -> None:
        self.values[key] = value


class _StorageOptions:
    def __init__(self, **options: str) -> None:
        self._options = SecretOptions(options)

    def native_options(self) -> SecretOptions:
        return self._options


def _credential_engine():
    from invariantql.adapters.spark_engine.engine import SparkEngine

    conf = _FakeHadoopConfiguration()
    jsc = SimpleNamespace(hadoopConfiguration=lambda: conf)
    spark = SimpleNamespace(sparkContext=SimpleNamespace(_jsc=jsc))
    return SparkEngine(spark), conf


def test_explicit_azure_blob_credentials_use_wasb_configuration() -> None:
    engine, conf = _credential_engine()
    storage = _StorageOptions(
        account_name="lake",
        container="raw",
        endpoint_kind="blob",
        endpoint_suffix="core.chinacloudapi.cn",
        sas_token="sig=secret",
    )
    expected = {
        "fs.azure.sas.raw.lake.blob.core.chinacloudapi.cn": "sig=secret",
    }
    assert engine.apply_storage_credentials(storage) == expected  # type: ignore[arg-type]
    assert conf.values == expected


@pytest.mark.parametrize(
    ("suffix", "authority"),
    [
        ("core.windows.net", "login.microsoftonline.com"),
        ("core.chinacloudapi.cn", "login.chinacloudapi.cn"),
        ("core.usgovcloudapi.net", "login.microsoftonline.us"),
    ],
)
def test_explicit_adls_service_principal_uses_abfs_configuration(
    suffix: str, authority: str
) -> None:
    engine, conf = _credential_engine()
    storage = _StorageOptions(
        account_name="lake",
        container="raw",
        endpoint_kind="dfs",
        endpoint_suffix=suffix,
        client_id="client",
        client_secret="secret",
        tenant_id="tenant",
    )
    host = f"lake.dfs.{suffix}"
    expected = {
        f"fs.azure.account.auth.type.{host}": "OAuth",
        f"fs.azure.account.oauth.provider.type.{host}": (
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
        ),
        f"fs.azure.account.oauth2.client.id.{host}": "client",
        f"fs.azure.account.oauth2.client.secret.{host}": "secret",
        f"fs.azure.account.oauth2.client.endpoint.{host}": (
            f"https://{authority}/tenant/oauth2/token"
        ),
    }
    assert engine.apply_storage_credentials(storage) == expected  # type: ignore[arg-type]
    assert conf.values == expected


def test_explicit_adls_shared_key_resets_the_authentication_type() -> None:
    engine, conf = _credential_engine()
    storage = _StorageOptions(
        account_name="lake",
        endpoint_kind="dfs",
        endpoint_suffix="core.windows.net",
        account_key="secret",
    )
    expected = {
        "fs.azure.account.key.lake.dfs.core.windows.net": "secret",
        "fs.azure.account.auth.type.lake.dfs.core.windows.net": "SharedKey",
    }
    assert engine.apply_storage_credentials(storage) == expected  # type: ignore[arg-type]
    assert conf.values == expected


def test_explicit_s3_temporary_and_http_options_are_complete() -> None:
    engine, conf = _credential_engine()
    storage = _StorageOptions(
        aws_access_key_id="key",
        aws_secret_access_key="secret",
        aws_session_token="token",
        aws_endpoint_url="http://localhost:9000",
        aws_allow_http="true",
        aws_region="eu-west-1",
    )
    applied = engine.apply_storage_credentials(storage)  # type: ignore[arg-type]
    assert applied["fs.s3a.aws.credentials.provider"].endswith(".TemporaryAWSCredentialsProvider")
    assert applied["fs.s3a.connection.ssl.enabled"] == "false"
    assert applied["fs.s3a.endpoint"] == "http://localhost:9000"
    assert applied["fs.s3a.endpoint.region"] == "eu-west-1"
    assert conf.values == applied


@pytest.mark.parametrize(
    ("options", "provider"),
    [
        (
            {"aws_access_key_id": "key", "aws_secret_access_key": "secret"},
            "SimpleAWSCredentialsProvider",
        ),
        ({"aws_anonymous": "true"}, "AnonymousAWSCredentialsProvider"),
    ],
)
def test_explicit_s3_provider_does_not_inherit_an_old_session_mode(
    options: dict[str, str], provider: str
) -> None:
    engine, conf = _credential_engine()
    applied = engine.apply_storage_credentials(_StorageOptions(**options))  # type: ignore[arg-type]
    assert applied["fs.s3a.aws.credentials.provider"].endswith("." + provider)
    assert conf.values == applied
