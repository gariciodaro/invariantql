from __future__ import annotations

import pytest

from invariantql.application import bind_parameters, bind_plan
from invariantql.domain import (
    Alias,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    ComparisonOp,
    DiagnosticCode,
    FloatType,
    IntegerType,
    Like,
    Literal,
    Parameter,
    ParameterError,
    PlanValidationError,
    QueryPlan,
    Schema,
    StringType,
)

SCHEMA = Schema.of(("Id", IntegerType()), ("name", StringType()), ("amount", FloatType()))


def test_unknown_column_is_named() -> None:
    with pytest.raises(PlanValidationError) as info:
        bind_plan(QueryPlan.scan("t").select("nope"), SCHEMA)
    assert info.value.code is DiagnosticCode.PLAN_UNKNOWN_COLUMN
    assert "nope" in str(info.value)


def test_case_insensitive_resolution_rewrites_to_canonical_name() -> None:
    bound = bind_plan(
        QueryPlan.scan("t")
        .select("id")
        .where(Comparison(ComparisonOp.GT, Column("ID"), Literal.of(1))),
        SCHEMA,
    )
    assert bound.plan.output_names == ("Id",)
    assert str(bound.plan.predicate) == "(Id > 1)"
    assert bound.output_schema.names == ("Id",)


def test_type_mismatches_are_rejected() -> None:
    with pytest.raises(PlanValidationError) as info:
        bind_plan(
            QueryPlan.scan("t").where(Comparison(ComparisonOp.EQ, Column("name"), Literal.of(1))),
            SCHEMA,
        )
    assert info.value.code is DiagnosticCode.PLAN_TYPE_MISMATCH
    with pytest.raises(PlanValidationError):
        bind_plan(QueryPlan.scan("t").where(Like(Column("Id"), Literal.of("1%"))), SCHEMA)
    with pytest.raises(PlanValidationError):
        bind_plan(
            QueryPlan.scan("t").select(
                Alias(Arithmetic(ArithmeticOp.ADD, Column("name"), Literal.of(1)), "x")
            ),
            SCHEMA,
        )
    with pytest.raises(PlanValidationError):
        bind_plan(QueryPlan.scan("t").where(Column("name")), SCHEMA)


def test_output_schema_types() -> None:
    plan = QueryPlan.scan("t").select(
        "Id",
        Alias(Arithmetic(ArithmeticOp.DIV, Column("Id"), Literal.of(2)), "half"),
        Alias(Arithmetic(ArithmeticOp.ADD, Column("Id"), Literal.of(2)), "plus"),
        Alias(Comparison(ComparisonOp.GT, Column("amount"), Parameter("p")), "big"),
    )
    schema = bind_plan(plan, SCHEMA).output_schema
    assert [str(f.data_type) for f in schema] == ["int64", "float64", "int64", "boolean"]


def test_parameter_binding_rules() -> None:
    plan = QueryPlan.scan("t").where(Comparison(ComparisonOp.GT, Column("Id"), Parameter("min")))
    assert bind_parameters(plan, {"min": 3}) == {"min": Literal.of(3)}
    with pytest.raises(ParameterError) as missing:
        bind_parameters(plan, {})
    assert missing.value.code is DiagnosticCode.PARAMETER_MISSING
    with pytest.raises(ParameterError) as extra:
        bind_parameters(plan, {"min": 1, "other": 2})
    assert extra.value.code is DiagnosticCode.PARAMETER_UNEXPECTED
    with pytest.raises(ParameterError) as invalid:
        bind_parameters(plan, {"min": object()})
    assert invalid.value.code is DiagnosticCode.PARAMETER_INVALID
