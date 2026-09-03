from __future__ import annotations

import fsspec
import pytest

import invariantql as iql
from invariantql.adapters.storage.fsspec_storage import FsspecStorage
from invariantql.ports.storage import StorageCapabilities
from tests.contracts.contracts import EngineContract, SourceContract, StorageContract

pytestmark = pytest.mark.contract


class TestLocalStorageContract(StorageContract):
    @pytest.fixture()
    def storage(self, tmp_path, sample_bytes):
        (tmp_path / "dir").mkdir()
        (tmp_path / "dir" / "sample.csv").write_bytes(sample_bytes)
        return iql.local_storage(tmp_path)

    @pytest.fixture()
    def sample_path(self) -> str:
        return "dir/sample.csv"

    @pytest.fixture()
    def missing_path(self) -> str:
        return "dir/missing.csv"


class TestMemoryFsspecStorageContract(StorageContract):
    @pytest.fixture()
    def storage(self, sample_bytes):
        fs = fsspec.filesystem("memory")
        with fs.open("contract-bucket/dir/sample.csv", "wb") as handle:
            handle.write(sample_bytes)
        return FsspecStorage(
            fs,
            name="memory-contract",
            scheme="memory",
            netloc="contract-bucket",
            capabilities=StorageCapabilities(range_reads=True, hierarchical_directories=True),
        )

    @pytest.fixture()
    def sample_path(self) -> str:
        return "dir/sample.csv"

    @pytest.fixture()
    def missing_path(self) -> str:
        return "dir/missing.csv"


class TestFileSourceContract(SourceContract):
    @pytest.fixture()
    def source(self, data_dir):
        return iql.file_source(
            "orders", iql.local_storage(data_dir), "orders.parquet", iql.ParquetFormat()
        )


class TestDuckDBEngineContract(EngineContract):
    @pytest.fixture()
    def engine(self):
        engine = iql.duckdb_engine()
        yield engine
        engine.close()

    @pytest.fixture()
    def file_source(self, data_dir):
        return iql.file_source(
            "orders", iql.local_storage(data_dir), "orders.parquet", iql.ParquetFormat()
        )


@pytest.mark.spark
class TestSparkEngineContract(EngineContract):
    @pytest.fixture()
    def engine(self, spark):
        return iql.spark_engine(spark)

    @pytest.fixture()
    def file_source(self, data_dir):
        return iql.file_source(
            "orders", iql.local_storage(data_dir), "orders.parquet", iql.ParquetFormat()
        )
