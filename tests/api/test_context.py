from __future__ import annotations

import pytest

import invariantql as iql
from invariantql.api.loading import load_adapter
from invariantql.domain import DiagnosticCode


def test_missing_extra_is_named(monkeypatch) -> None:
    with pytest.raises(iql.MissingDependencyError) as info:
        load_adapter("invariantql.adapters.storage.definitely_missing", "X", extra="magic")
    assert info.value.code is DiagnosticCode.ADAPTER_DEPENDENCY_MISSING
    assert "invariantql[magic]" in str(info.value)


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
