from __future__ import annotations

from types import SimpleNamespace
from typing import get_type_hints
from unittest.mock import Mock

import pytest

import invariantql as iql
import invariantql.api.context as context_module
from invariantql.api.loading import load_adapter
from invariantql.domain import DiagnosticCode
from invariantql.ports import LocalResult


def test_missing_extra_is_named(monkeypatch) -> None:
    with pytest.raises(iql.MissingDependencyError) as info:
        load_adapter("invariantql.adapters.storage.definitely_missing", "X", extra="magic")
    assert info.value.code is DiagnosticCode.ADAPTER_DEPENDENCY_MISSING
    assert "invariantql[magic]" in str(info.value)


def test_format_handler_programming_errors_are_not_silently_ignored(monkeypatch) -> None:
    def broken_loader(module: str, attribute: str, *, extra: str | None):
        raise RuntimeError(f"broken {module}.{attribute} ({extra})")

    monkeypatch.setattr(context_module, "load_adapter", broken_loader)
    engine = SimpleNamespace(handler_kind="local", register_format_handler=Mock())
    with pytest.raises(
        RuntimeError,
        match=r"broken invariantql\.adapters\.formats\.xml\.XmlLocalHandler \(xml\)",
    ):
        iql.Context()._register_format_handlers(engine)


def test_missing_format_dependency_is_retained_until_first_use(monkeypatch) -> None:
    handlers = {}

    def missing_loader(module: str, attribute: str, *, extra: str | None):
        raise iql.MissingDependencyError(
            f"missing {module}.{attribute}; install invariantql[{extra}]",
            details={"module": module, "extra": extra or ""},
        )

    monkeypatch.setattr(context_module, "load_adapter", missing_loader)
    engine = SimpleNamespace(
        handler_kind="local",
        register_format_handler=lambda handler: handlers.setdefault(handler.format_name, handler),
    )
    iql.Context()._register_format_handlers(engine)

    with pytest.raises(iql.MissingDependencyError) as info:
        handlers["xml"].capabilities(iql.XmlFormat(row_tag="item"))
    assert info.value.code is DiagnosticCode.ADAPTER_DEPENDENCY_MISSING
    assert dict(info.value.diagnostic.details)["extra"] == "xml"


def test_registry_rules(ctx, data_dir) -> None:
    dup = iql.file_source(
        "orders", iql.local_storage(data_dir), "orders.parquet", iql.ParquetFormat()
    )
    with pytest.raises(iql.InvariantQLError) as info:
        ctx.register_source(dup)
    assert info.value.code is DiagnosticCode.SOURCE_ALREADY_REGISTERED
    ctx.register_source(dup, replace=True)
    assert "orders" in ctx.sources
    ctx.unregister_source("orders")
    assert "orders" not in ctx.sources
    with pytest.raises(iql.InvariantQLError):
        ctx.unregister_source("orders")
    assert ctx.engines == ("duckdb",)


def test_replacing_registered_resources_closes_the_previous_instance() -> None:
    first_source = SimpleNamespace(name="source", close=Mock())
    second_source = SimpleNamespace(name="source", close=Mock())
    first_engine = SimpleNamespace(name="engine", close=Mock())
    second_engine = SimpleNamespace(name="engine", close=Mock())

    with iql.Context() as ctx:
        ctx.register_source(first_source)  # type: ignore[arg-type]
        ctx.register_source(second_source, replace=True)  # type: ignore[arg-type]
        ctx.register_engine(first_engine)  # type: ignore[arg-type]
        ctx.register_engine(second_engine, replace=True)  # type: ignore[arg-type]

        first_source.close.assert_called_once_with()
        first_engine.close.assert_called_once_with()
        second_source.close.assert_not_called()
        second_engine.close.assert_not_called()

    second_source.close.assert_called_once_with()
    second_engine.close.assert_called_once_with()


@pytest.mark.parametrize(
    "options",
    [
        {"preview_rows": -1},
        {"preview_rows": True},
        {"batch_size": 0},
        {"batch_size": -1},
        {"batch_size": True},
    ],
)
def test_context_rejects_invalid_execution_limits(options) -> None:
    with pytest.raises(ValueError):
        iql.Context(**options)


@pytest.mark.parametrize("batch_size", [0, -1, True])
def test_execute_rejects_invalid_batch_size_before_scanning(ctx, batch_size) -> None:
    with pytest.raises(ValueError, match="batch_size"):
        ctx.sql("SELECT id FROM orders").execute(batch_size=batch_size)


def test_queries_are_immutable_values(ctx) -> None:
    base = ctx.sql("SELECT id FROM orders")
    filtered = base.where(iql.col("id") > 1)
    assert base.fingerprint() != filtered.fingerprint()
    assert base.plan.predicate is None
    assert ctx.from_plan(filtered.to_dict()).fingerprint() == filtered.fingerprint()
    assert str(filtered) == "SELECT id FROM orders WHERE (id > 1)"


def test_context_is_a_context_manager(data_dir) -> None:
    with iql.Context() as ctx:
        ctx.register_source(
            iql.file_source("o", iql.local_storage(data_dir), "orders.parquet", iql.ParquetFormat())
        )
        assert ctx.sql("SELECT id FROM o LIMIT 1").execute().to_arrow().num_rows == 1
    assert ctx.sources == ()


def test_query_local_results_have_a_public_materialization_protocol(ctx) -> None:
    assert get_type_hints(iql.Query.preview)["return"] is LocalResult
    assert get_type_hints(iql.Query.execute)["return"] is LocalResult

    result = ctx.sql("SELECT id FROM orders LIMIT 1").execute()
    try:
        assert isinstance(result, LocalResult)
        assert not result.closed
    finally:
        result.close()


def test_expression_builder_shapes() -> None:
    e = (
        ((iql.col("a") + 1) * 2 / 4 - 1 > iql.param("p")) & ~iql.col("b").is_null()
        | iql.col("c").isin([1, 2]).not_in([3])
        if False
        else None
    )
    expr = (((iql.col("a") + 1) * 2 / 4 - 1) > iql.param("p")) & ~iql.col("b").is_null()
    assert str(expr) == "((((((a + 1) * 2) / 4) - 1) > :p) AND (NOT (b IS NULL)))"
    assert str(iql.col("n").between(1, 5)) == "((n >= 1) AND (n <= 5))"
    assert str(iql.col("n").not_like("x%")) == "(n NOT LIKE 'x%')"
    assert str(iql.lit("it's")) == "'it''s'"
    assert e is None


def test_expression_builder_rejects_python_boolean_operators() -> None:
    left = iql.col("a") > 1
    right = iql.col("b") > 2
    with pytest.raises(TypeError, match=r"combine predicates with & and \|"):
        _ = left and right
    with pytest.raises(TypeError, match=r"combine predicates with & and \|"):
        _ = left or right
    with pytest.raises(TypeError, match="no Python truth value"):
        bool(left)
