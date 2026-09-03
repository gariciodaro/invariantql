"""FF-03: shared conformance suites for the ports. Subclass and provide fixtures."""

from __future__ import annotations

import pytest

from invariantql.application import CapabilityPlanner, PlanningTarget, bind_plan
from invariantql.domain import (
    DiagnosticCode,
    EngineCapabilities,
    InvariantQLError,
    PushdownCapabilities,
    QueryPlan,
    Schema,
    SecretOptions,
    StorageError,
    UnsupportedOperationError,
)
from invariantql.domain.location import Location
from invariantql.ports import (
    CompilingExecutionEngine,
    DataSource,
    ExecutionEngine,
    FileRelation,
    LocalExecutionEngine,
    NativeRelation,
    Reachability,
    RecordBatchStream,
    Storage,
    StorageCapabilities,
)


class StorageContract:
    """Provide ``storage`` (a Storage), ``sample_path`` (a file with ``sample_bytes``) and ``missing_path``."""

    @pytest.fixture()
    def sample_bytes(self) -> bytes:
        return b"id,name\n1,alice\n"

    def test_satisfies_protocol(self, storage) -> None:
        assert isinstance(storage, Storage)
        assert isinstance(storage.name, str) and storage.name
        assert isinstance(storage.capabilities, StorageCapabilities)
        assert isinstance(storage.native_options(), SecretOptions)

    def test_resolve_is_idempotent_and_absolute(self, storage, sample_path) -> None:
        loc = storage.resolve(sample_path)
        assert isinstance(loc, Location)
        assert storage.resolve(loc) == loc
        assert loc.scheme, "resolved locations carry the storage scheme"

    def test_open_read_returns_the_bytes(self, storage, sample_path, sample_bytes) -> None:
        with storage.open_read(storage.resolve(sample_path)) as handle:
            assert handle.read() == sample_bytes

    def test_info_and_exists_and_list(self, storage, sample_path, sample_bytes) -> None:
        loc = storage.resolve(sample_path)
        info = storage.info(loc)
        assert info.size == len(sample_bytes) and not info.is_directory
        assert storage.exists(loc)
        parent = Location(loc.path.rsplit("/", 1)[0] or "/", loc.scheme, loc.netloc)
        names = [entry.location.name for entry in storage.list(parent)]
        assert loc.name in names

    def test_missing_objects_produce_the_standard_diagnostic(self, storage, missing_path) -> None:
        loc = storage.resolve(missing_path)
        assert not storage.exists(loc)
        with pytest.raises(StorageError) as info:
            storage.open_read(loc)
        assert info.value.code is DiagnosticCode.STORAGE_OBJECT_NOT_FOUND
        with pytest.raises(StorageError):
            storage.info(loc)

    def test_native_uri_matches_capability(self, storage, sample_path) -> None:
        uri = storage.native_uri(storage.resolve(sample_path))
        if storage.capabilities.engine_visible_uri:
            assert isinstance(uri, str) and "://" in uri
        else:
            assert uri is None

    def test_range_reads_when_declared(self, storage, sample_path, sample_bytes) -> None:
        if not storage.capabilities.range_reads:
            pytest.skip("storage declares no range reads")
        with storage.open_read(storage.resolve(sample_path)) as handle:
            handle.seek(3)
            assert handle.read(4) == sample_bytes[3:7]


class SourceContract:
    """Provide ``source`` (a DataSource)."""

    def test_satisfies_protocol(self, source) -> None:
        assert isinstance(source, DataSource)
        assert source.name
        caps = source.capabilities()
        assert isinstance(caps, PushdownCapabilities)
        relation = source.relation()
        assert isinstance(relation, (FileRelation, NativeRelation))

    def test_file_sources_delegate_scanning_to_the_engine(self, source) -> None:
        relation = source.relation()
        if not isinstance(relation, FileRelation):
            pytest.skip("native source")
        from invariantql.domain.execution import PushedOperations

        with pytest.raises(UnsupportedOperationError) as info:
            source.scan(PushedOperations(), {}, batch_size=10)
        assert info.value.code is DiagnosticCode.SOURCE_SCAN_UNSUPPORTED

    def test_native_sources_scan_to_streams(self, source) -> None:
        relation = source.relation()
        if not isinstance(relation, NativeRelation):
            pytest.skip("file source")
        from invariantql.domain.execution import PushedOperations

        schema = source.schema()
        assert isinstance(schema, Schema) and len(schema) > 0
        stream = source.scan(
            PushedOperations(projection=(schema.names[0],), limit=1), {}, batch_size=10
        )
        assert isinstance(stream, RecordBatchStream)
        assert stream.schema.names == [schema.names[0]]
        stream.close()

    def test_close_is_idempotent(self, source) -> None:
        source.close()
        source.close()


class EngineContract:
    """Provide ``engine`` (an ExecutionEngine) and ``file_source`` (a FileSource readable by it)."""

    def test_satisfies_protocol(self, engine) -> None:
        assert isinstance(engine, ExecutionEngine)
        assert isinstance(engine.capabilities(), EngineCapabilities)
        assert isinstance(engine, (LocalExecutionEngine, CompilingExecutionEngine))

    def test_reachability_schema_and_scan_capabilities(self, engine, file_source) -> None:
        reach = engine.reachability(file_source)
        assert isinstance(reach, Reachability)
        if not reach.reachable:
            pytest.skip(reach.reason)
        assert isinstance(engine.schema(file_source), Schema)
        assert isinstance(engine.scan_capabilities(file_source), PushdownCapabilities)

    def test_non_executable_plans_are_refused(self, engine, file_source) -> None:
        plan = QueryPlan.scan(file_source.name).limit(1)
        bound = bind_plan(plan, engine.schema(file_source))
        target = PlanningTarget(
            engine.name,
            engine.capabilities(),
            engine.scan_capabilities(file_source),
            False,
            "unreachable",
        )
        rejected = CapabilityPlanner().plan(bound, target)
        assert not rejected.executable
        with pytest.raises(InvariantQLError) as info:
            if isinstance(engine, LocalExecutionEngine):
                engine.execute(rejected, file_source, {}, batch_size=10)
            else:
                engine.compile(rejected, file_source, {})
        assert info.value.code is DiagnosticCode.ENGINE_PLAN_NOT_EXECUTABLE
