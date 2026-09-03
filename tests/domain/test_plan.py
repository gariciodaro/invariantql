"""FF-04: deterministic, immutable plans."""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
from decimal import Decimal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from invariantql.domain import (
    Alias,
    And,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    ComparisonOp,
    Filter,
    In,
    IsNull,
    Like,
    Limit,
    Literal,
    Not,
    Or,
    Parameter,
    Project,
    QueryPlan,
    Scan,
    SourceRef,
)
from invariantql.domain.expressions import (
    expression_from_dict,
    referenced_columns,
    referenced_parameters,
)

pytestmark = pytest.mark.architecture


def _values():
    return st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-(2**63), max_value=2**63 - 1),
        st.floats(allow_nan=False, allow_infinity=False),
        st.text(max_size=12),
        st.decimals(allow_nan=False, allow_infinity=False, places=3),
        st.dates(),
        st.datetimes(),
    )


names = st.sampled_from(["a", "b", "c", "Amount", "weird name"])
leaf = st.one_of(
    names.map(Column),
    _values().map(Literal.of),
    st.sampled_from(["p", "q"]).map(Parameter),
)


def _compose(children):
    return st.one_of(
        st.tuples(st.sampled_from(list(ComparisonOp)), children, children).map(
            lambda t: Comparison(*t)
        ),
        st.lists(children, min_size=2, max_size=3).map(lambda ops: And(tuple(ops))),
        st.lists(children, min_size=2, max_size=3).map(lambda ops: Or(tuple(ops))),
        children.map(Not),
        st.tuples(children, st.booleans()).map(lambda t: IsNull(*t)),
        st.tuples(
            children, st.lists(_values().map(Literal.of), min_size=1, max_size=3), st.booleans()
        ).map(lambda t: In(t[0], tuple(t[1]), t[2])),
        st.tuples(children, st.text(max_size=5).map(Literal.of), st.booleans()).map(
            lambda t: Like(*t)
        ),
        st.tuples(st.sampled_from(list(ArithmeticOp)), children, children).map(
            lambda t: Arithmetic(*t)
        ),
    )


expressions = st.recursive(leaf, _compose, max_leaves=12)


@st.composite
def plans(draw):
    plan = QueryPlan.scan(draw(st.sampled_from(["orders", "t"])))
    if draw(st.booleans()):
        plan = plan.where(draw(expressions))
    if draw(st.booleans()):
        cols = draw(st.lists(names, min_size=1, max_size=3, unique=True))
        exprs = [
            Column(c) if draw(st.booleans()) else Alias(draw(expressions), f"{c}_x") for c in cols
        ]
        plan = plan.select(*exprs)
    if draw(st.booleans()):
        plan = plan.limit(draw(st.integers(min_value=0, max_value=10_000)))
    return plan


@settings(max_examples=150, deadline=None)
@given(plans())
def test_roundtrip_and_fingerprint_are_deterministic(plan: QueryPlan) -> None:
    data = plan.to_dict()
    again = QueryPlan.from_dict(json.loads(json.dumps(data, default=str)))
    assert again == plan
    assert again.fingerprint() == plan.fingerprint()
    assert QueryPlan.from_dict(data).canonical_json() == plan.canonical_json()


@settings(max_examples=100, deadline=None)
@given(expressions)
def test_expression_serialisation_roundtrip(expression) -> None:
    assert expression_from_dict(expression.to_dict()) == expression


def test_plans_are_immutable() -> None:
    plan = QueryPlan.scan("orders").where(Comparison(ComparisonOp.GT, Column("a"), Literal.of(1)))
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.root = Scan(SourceRef("x"))  # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.root.predicate = Literal.of(True)  # type: ignore[attr-defined]
    assert isinstance(plan.select("a", "b").projection, tuple)


def test_construction_order_does_not_change_identity() -> None:
    pred = Comparison(ComparisonOp.GT, Column("a"), Literal.of(1))
    one = QueryPlan.scan("orders").where(pred).select("a").limit(5)
    two = QueryPlan.scan("orders").limit(5).select("a").where(pred)
    assert one == two
    assert one.fingerprint() == two.fingerprint()


def test_where_conjoins_and_limit_keeps_the_smaller_bound() -> None:
    plan = (
        QueryPlan.scan("orders")
        .where(Comparison(ComparisonOp.GT, Column("a"), Literal.of(1)))
        .where(IsNull(Column("b")))
        .limit(100)
        .limit(10)
        .limit(50)
    )
    assert isinstance(plan.predicate, And)
    assert len(plan.predicate.operands) == 2
    assert plan.limit_count == 10


def test_invalid_shapes_are_rejected() -> None:
    scan = Scan(SourceRef("t"))
    with pytest.raises(ValueError):
        QueryPlan(Filter(Limit(scan, 1), Literal.of(True)))
    with pytest.raises(ValueError):
        Project(scan, (Column("a"), Alias(Column("b"), "a")))
    with pytest.raises(ValueError):
        Project(scan, (Comparison(ComparisonOp.EQ, Column("a"), Literal.of(1)),))
    with pytest.raises(ValueError):
        Limit(scan, -1)
    with pytest.raises(ValueError):
        In(Column("a"), (Column("b"),))
    with pytest.raises(ValueError):
        SourceRef("")


def test_plan_reports_columns_and_parameters() -> None:
    plan = (
        QueryPlan.scan("orders")
        .where(
            And(
                (
                    Comparison(ComparisonOp.GT, Column("a"), Parameter("min")),
                    Like(Column("n"), Parameter("pat")),
                )
            )
        )
        .select("a", Alias(Arithmetic(ArithmeticOp.ADD, Column("b"), Literal.of(1)), "b1"))
    )
    assert plan.referenced_columns == ("a", "b", "n")
    assert plan.parameters == ("min", "pat")
    assert plan.output_names == ("a", "b1")
    assert referenced_columns(plan.predicate) == ("a", "n")
    assert referenced_parameters(plan.predicate) == ("min", "pat")


def test_serialised_plan_contains_only_logical_names() -> None:
    plan = QueryPlan.scan("orders").where(Comparison(ComparisonOp.EQ, Column("k"), Literal.of("v")))
    text = plan.canonical_json()
    assert "orders" in text
    for forbidden in ("password", "token", "sig=", "Storage", "Connection"):
        assert forbidden not in text


def test_literal_types_and_values_survive_serialisation() -> None:
    values = [
        Decimal("1.250"),
        dt.date(2024, 1, 2),
        dt.datetime(2024, 1, 2, 3, 4, 5),
        b"\x00\xff",
        None,
        True,
        1,
        1.5,
        "s",
    ]
    for value in values:
        lit = Literal.of(value)
        assert expression_from_dict(lit.to_dict()) == lit


def test_node_ids_are_stable() -> None:
    plan = QueryPlan.scan("t").where(IsNull(Column("a"))).select("a").limit(1)
    assert [node_id for node_id, _ in plan.node_ids()] == [
        "0-scan",
        "1-filter",
        "2-project",
        "3-limit",
    ]
    assert [node_id for node_id, _ in QueryPlan.scan("t").limit(1).node_ids()] == [
        "0-scan",
        "1-limit",
    ]
