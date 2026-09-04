from __future__ import annotations

from decimal import Decimal

import pytest

from invariantql.application import bind_parameters, bind_plan
from invariantql.domain import (
    Alias,
    And,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    ComparisonOp,
    DecimalType,
    DiagnosticCode,
    FloatType,
    IntegerType,
    Like,
    Literal,
    Not,
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
    assert info.value.diagnostic.node_id == "1-project"
    assert "nope" in str(info.value)

    with pytest.raises(PlanValidationError) as filtered:
        bind_plan(
            QueryPlan.scan("t")
            .where(Comparison(ComparisonOp.GT, Column("Id"), Literal.of(0)))
            .select("nope"),
            SCHEMA,
        )
    assert filtered.value.diagnostic.node_id == "2-project"


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


def test_case_resolution_cannot_create_duplicate_output_columns() -> None:
    with pytest.raises(PlanValidationError) as info:
        bind_plan(QueryPlan.scan("t").select("id", "Id"), SCHEMA)
    assert info.value.code is DiagnosticCode.PLAN_INVALID_SHAPE
    assert "duplicate output column" in str(info.value)


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
    with pytest.raises(PlanValidationError):
        bind_plan(
            QueryPlan.scan("t").where(
                And(
                    (
                        Comparison(ComparisonOp.GT, Column("Id"), Literal.of(1)),
                        Column("amount"),
                    )
                )
            ),
            SCHEMA,
        )
    with pytest.raises(PlanValidationError):
        bind_plan(QueryPlan.scan("t").where(Not(Column("amount"))), SCHEMA)
    with pytest.raises(PlanValidationError):
        bind_plan(QueryPlan.scan("t").where(Like(Column("name"), Literal.of(1))), SCHEMA)


def test_aliases_are_only_allowed_at_the_top_of_a_projection() -> None:
    predicate = Comparison(ComparisonOp.GT, Column("Id"), Literal.of(1))
    with pytest.raises(PlanValidationError) as info:
        bind_plan(QueryPlan.scan("t").where(Alias(predicate, "p")), SCHEMA)
    assert info.value.code is DiagnosticCode.PLAN_INVALID_SHAPE

    nested = Alias(
        Arithmetic(ArithmeticOp.ADD, Alias(Column("Id"), "inner"), Literal.of(1)), "outer"
    )
    with pytest.raises(PlanValidationError) as info:
        bind_plan(QueryPlan.scan("t").select(nested), SCHEMA)
    assert info.value.code is DiagnosticCode.PLAN_INVALID_SHAPE


def test_output_schema_types() -> None:
    plan = QueryPlan.scan("t").select(
        "Id",
        Alias(Arithmetic(ArithmeticOp.DIV, Column("Id"), Literal.of(2)), "half"),
        Alias(Arithmetic(ArithmeticOp.ADD, Column("Id"), Literal.of(2)), "plus"),
        Alias(Comparison(ComparisonOp.GT, Column("amount"), Parameter("p")), "big"),
    )
    schema = bind_plan(plan, SCHEMA).output_schema
    assert [str(f.data_type) for f in schema] == ["int64", "float64", "int64", "boolean"]


def test_numeric_result_types_are_symmetric_and_operation_aware() -> None:
    schema = Schema.of(
        ("tiny", IntegerType(8)),
        ("wide", IntegerType(64)),
        ("single", FloatType(32)),
        ("double", FloatType(64)),
        ("money", DecimalType(5, 2)),
        ("ratio", DecimalType(18, 6)),
    )
    expressions = (
        Alias(Arithmetic(ArithmeticOp.ADD, Column("tiny"), Column("wide")), "ints_lr"),
        Alias(Arithmetic(ArithmeticOp.ADD, Column("wide"), Column("tiny")), "ints_rl"),
        Alias(Arithmetic(ArithmeticOp.ADD, Column("single"), Column("double")), "floats_lr"),
        Alias(Arithmetic(ArithmeticOp.ADD, Column("double"), Column("single")), "floats_rl"),
        Alias(Arithmetic(ArithmeticOp.ADD, Column("money"), Column("ratio")), "decimal_add"),
        Alias(Arithmetic(ArithmeticOp.MUL, Column("money"), Column("ratio")), "decimal_mul"),
        Alias(Arithmetic(ArithmeticOp.ADD, Column("wide"), Column("money")), "mixed_add"),
        Alias(Arithmetic(ArithmeticOp.DIV, Column("money"), Column("ratio")), "division"),
    )

    output = bind_plan(QueryPlan.scan("t").select(*expressions), schema).output_schema

    assert [str(field.data_type) for field in output] == [
        "int64",
        "int64",
        "float64",
        "float64",
        "decimal(19,6)",
        "decimal(24,8)",
        "decimal(22,2)",
        "float64",
    ]

    with pytest.raises(PlanValidationError) as overflow:
        bind_plan(
            QueryPlan.scan("t").select(
                Alias(
                    Arithmetic(ArithmeticOp.ADD, Column("huge"), Column("huge")),
                    "overflow",
                )
            ),
            Schema.of(("huge", DecimalType(76, 0))),
        )
    assert overflow.value.code is DiagnosticCode.PLAN_TYPE_MISMATCH


def test_division_result_remains_float_with_one_unknown_or_null_operand() -> None:
    schema = Schema.of(("number", IntegerType()))
    plan = QueryPlan.scan("t").select(
        Alias(Arithmetic(ArithmeticOp.DIV, Parameter("p"), Column("number")), "parameter"),
        Alias(Arithmetic(ArithmeticOp.DIV, Literal.of(None), Column("number")), "null"),
        Alias(Arithmetic(ArithmeticOp.DIV, Parameter("p"), Parameter("q")), "unknowns"),
    )
    output = bind_plan(plan, schema).output_schema
    assert [field.data_type for field in output] == [
        FloatType(64),
        FloatType(64),
        FloatType(64),
    ]


def test_decimal_literal_shape_counts_positive_exponents_and_leading_fractional_zeros() -> None:
    assert Literal.of(Decimal("1E+5")).data_type == DecimalType(6, 0)
    assert Literal.of(Decimal("0.0012")).data_type == DecimalType(4, 4)


def test_decimal256_is_descriptive_but_not_in_the_executable_profile() -> None:
    schema = Schema.of(("id", IntegerType()), ("huge", DecimalType(65, 30)))
    assert bind_plan(QueryPlan.scan("t").select("id"), schema).output_schema.names == ("id",)

    for plan in (
        QueryPlan.scan("t"),
        QueryPlan.scan("t").select("huge"),
        QueryPlan.scan("t").select(Alias(Literal.of(Decimal("1E+38")), "too_precise")),
    ):
        with pytest.raises(PlanValidationError) as info:
            bind_plan(plan, schema)
        assert info.value.code is DiagnosticCode.PLAN_TYPE_MISMATCH
        assert "precision limit of 38" in str(info.value)


def test_parameter_types_refine_the_bound_output_schema() -> None:
    direct = QueryPlan.scan("t").select(Alias(Parameter("value"), "value"))
    assert str(bind_plan(direct, SCHEMA).output_schema.field("value").data_type) == "unknown"
    bound = bind_plan(direct, SCHEMA, {"value": Literal.of(7)})
    assert str(bound.output_schema.field("value").data_type) == "int64"

    arithmetic = QueryPlan.scan("t").select(
        Alias(Arithmetic(ArithmeticOp.ADD, Parameter("value"), Column("Id")), "value")
    )
    assert str(bind_plan(arithmetic, SCHEMA).output_schema.field("value").data_type) == "unknown"
    refined = bind_plan(arithmetic, SCHEMA, {"value": Literal.of(2.5)})
    assert str(refined.output_schema.field("value").data_type) == "float64"


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
    with pytest.raises(ParameterError) as out_of_range:
        bind_parameters(plan, {"min": 2**63})
    assert out_of_range.value.code is DiagnosticCode.PARAMETER_INVALID
    with pytest.raises(ParameterError) as excessive_decimal:
        bind_parameters(plan, {"min": Decimal("1E+38")})
    assert excessive_decimal.value.code is DiagnosticCode.PARAMETER_INVALID
