"""Named construction helpers that load adapters on demand (ADR-0010).

Each helper imports its adapter only when called, so ``import invariantql``
never touches a provider SDK. A missing extra raises ``MissingDependencyError``
naming the extra to install.
"""

from __future__ import annotations

import os
from typing import Any

from invariantql.api.loading import load_adapter
from invariantql.domain.formats import DataFormat
from invariantql.domain.location import Location
from invariantql.ports.source import DataSource
from invariantql.ports.storage import Storage

# -- storage ------------------------------------------------------------------


def local_storage(
    root: str | os.PathLike[str] | None = None, *, name: str | None = None
) -> Storage:
    cls = load_adapter("invariantql.adapters.storage.local", "LocalStorage", extra=None)
    return cls(root, name=name)


def azure_blob_storage(account_name: str, container: str, **options: Any) -> Storage:
    """Azure Blob Storage (flat namespace) through adlfs. See the adapter for credential options."""

    factory = load_adapter(
        "invariantql.adapters.storage.azure", "azure_blob_storage", extra="azure"
    )
    return factory(account_name, container, **options)


def adls_storage(account_name: str, container: str, **options: Any) -> Storage:
    """Azure Data Lake Storage Gen2 (hierarchical namespace) through adlfs."""

    factory = load_adapter("invariantql.adapters.storage.azure", "adls_storage", extra="azure")
    return factory(account_name, container, **options)


def s3_storage(bucket: str, **options: Any) -> Storage:
    factory = load_adapter("invariantql.adapters.storage.s3", "s3_storage", extra="s3")
    return factory(bucket, **options)


def sftp_storage(host: str, **options: Any) -> Storage:
    factory = load_adapter("invariantql.adapters.storage.sftp", "sftp_storage", extra="sftp")
    return factory(host, **options)


# -- sources ------------------------------------------------------------------


def file_source(
    name: str, storage: Storage, path: str | Location, data_format: DataFormat
) -> DataSource:
    cls = load_adapter("invariantql.adapters.sources.file_source", "FileSource", extra=None)
    return cls(name, storage, path, data_format)


def postgres_source(name: str, **options: Any) -> DataSource:
    cls = load_adapter("invariantql.adapters.sources.postgres", "PostgresSource", extra="postgres")
    return cls(name, **options)


def mysql_source(name: str, **options: Any) -> DataSource:
    cls = load_adapter("invariantql.adapters.sources.mysql", "MySQLSource", extra="mysql")
    return cls(name, **options)


def mongodb_source(name: str, **options: Any) -> DataSource:
    cls = load_adapter("invariantql.adapters.sources.mongodb", "MongoDBSource", extra="mongodb")
    return cls(name, **options)


def neo4j_source(name: str, **options: Any) -> DataSource:
    cls = load_adapter("invariantql.adapters.sources.neo4j", "Neo4jSource", extra="neo4j")
    return cls(name, **options)


# -- engines ------------------------------------------------------------------


def duckdb_engine(**options: Any) -> Any:
    cls = load_adapter("invariantql.adapters.duckdb_engine", "DuckDBEngine", extra="duckdb")
    return cls(**options)


def spark_engine(spark: Any, **options: Any) -> Any:
    cls = load_adapter("invariantql.adapters.spark_engine", "SparkEngine", extra="spark")
    return cls(spark, **options)


__all__ = [
    "adls_storage",
    "azure_blob_storage",
    "duckdb_engine",
    "file_source",
    "local_storage",
    "mongodb_source",
    "mysql_source",
    "neo4j_source",
    "postgres_source",
    "s3_storage",
    "sftp_storage",
    "spark_engine",
]
