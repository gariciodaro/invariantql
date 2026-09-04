"""FF-05: the pushdown completeness invariant, plus disposition rules."""

from __future__ import annotations

from dataclasses import replace

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from invariantql.application import CapabilityPlanner, PlanningTarget, bind_plan
from invariantql.domain import (
    Alias,
    And,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    ComparisonOp,
    DiagnosticCode,
    Disposition,
    EngineCapabilities,
    ExpressionKind,
    IntegerType,
    IsNull,
    Like,
    Literal,
    Parameter,
    PlanValidationError,
    PushdownCapabilities,
    QueryPlan,
    Schema,
    StringType,
    Support,
    check_completeness,
)
from invariantql.domain.expressions import ALL_EXPRESSION_KINDS, conjuncts

pytestmark = pytest.mark.architecture

SCHEMA = Schema.of(("a", IntegerType()), ("b", IntegerType()), ("n", StringType()))
PLANNER = CapabilityPlanner()

kinds = st.frozensets(st.sampled_from(sorted(ALL_EXPRESSION_KINDS, key=lambda k: k.value)))
supports = st.sampled_from(list(Support))


@st.composite
def scan_caps(draw):
    return PushdownCapabilities(
        projection=draw(supports),
        predicate=draw(supports),
        limit=draw(supports),
        expressions=draw(kinds),
        parameters=draw(st.booleans()),
        evidence=("fuzz",),
    )


@st.composite
def engine_caps(draw):
    return EngineCapabilities(
        "fuzz-engine",
        residual_expressions=draw(kinds) | {ExpressionKind.COLUMN, ExpressionKind.LITERAL},
    )


leaf = st.one_of(
    st.sampled_from(["a", "b"]).map(Column),
    st.integers(-5, 5).map(Literal.of),
    st.sampled_from(["p", "q"]).map(Parameter),
)


def _compose(children):
    return st.one_of(
        st.tuples(st.sampled_from(list(ComparisonOp)), children, children).map(
            lambda t: Comparison(*t)
        ),
        st.lists(children, min_size=2, max_size=3).map(lambda ops: And(tuple(ops))),
        st.tuples(children, st.booleans()).map(lambda t: IsNull(*t)),
        st.tuples(st.sampled_from(list(ArithmeticOp)), children, children).map(
            lambda t: Arithmetic(*t)
        ),
    )


predicates = st.one_of(
    st.recursive(leaf, _compose, max_leaves=8).filter(
        lambda e: not isinstance(e, (Column, Literal, Parameter, Arithmetic))
    ),
    st.tuples(st.sampled_from(["n"]).map(Column), st.text(max_size=3).map(Literal.of)).map(
        lambda t: Like(*t)
    ),
)


@st.composite
def plans(draw):
    plan = QueryPlan.scan("t")
    if draw(st.booleans()):
        plan = plan.where(draw(predicates))
        if draw(st.booleans()):
            plan = plan.where(draw(predicates))
    if draw(st.booleans()):
        choice = draw(st.integers(0, 2))
        if choice == 0:
            plan = plan.select("a")
        elif choice == 1:
            plan = plan.select("b", "a")
        else:
            plan = plan.select(
                Alias(Arithmetic(ArithmeticOp.ADD, Column("a"), Literal.of(1)), "a1"), "n"
            )
    if draw(st.booleans()):
        plan = plan.limit(draw(st.integers(0, 100)))
    return plan


@settings(max_examples=300, deadline=None)
@given(plans(), scan_caps(), engine_caps(), st.booleans())
def test_every_operation_is_accounted_for_exactly_once(plan, scan, engine, reachable) -> None:
    try:
        bound = bind_plan(plan, SCHEMA)
    except PlanValidationError:
        assume(
            False
        )  # semantically invalid plan (type mismatch); binding rejects it before planning
        return
    execution = PLANNER.plan(
        bound, PlanningTarget("fuzz-engine", engine, scan, reachable, "unreachable")
    )
    assert check_completeness(execution) == ()
    explain = execution.explain
    assert len(explain.nodes) == len(plan.nodes)
    assert all(n.reason_code for n in explain.nodes)
    if not reachable:
        assert explain.staging_required and not explain.executable
        assert explain.nodes[0].disposition is Disposition.REJECTED
    if execution.executable:
        # Everything pushed must be within declared capabilities, conjunct by conjunct
        # (top-level conjunctions are always split and re-joined by the planner).
        if execution.pushed.predicate is not None:
            assert scan.predicate is not Support.NONE
            assert all(scan.supports_expression(c) for c in conjuncts(execution.pushed.predicate))
        if execution.pushed.limit is not None:
            assert scan.limit is not Support.NONE and execution.residual.predicate is None
        if execution.pushed.projection is not None:
            assert scan.projection is not Support.NONE
        if execution.residual.predicate is not None:
            assert all(
                engine.supports_expression(c) for c in conjuncts(execution.residual.predicate)
            )


def _target(
    scan: PushdownCapabilities, engine: EngineCapabilities | None = None, reachable: bool = True
) -> PlanningTarget:
    return PlanningTarget("e", engine or EngineCapabilities("e"), scan, reachable, "no route")


def test_full_pushdown() -> None:
    plan = (
        QueryPlan.scan("t")
        .where(Comparison(ComparisonOp.GT, Column("a"), Literal.of(1)))
        .select("a", "b")
        .limit(5)
    )
    ep = PLANNER.plan(bind_plan(plan, SCHEMA), _target(PushdownCapabilities.full("x")))
    assert [n.disposition for n in ep.explain.nodes] == [Disposition.PUSHED] * 4
    assert ep.residual.is_empty
    assert ep.pushed.projection == ("a", "b") and ep.pushed.limit == 5


def test_partial_predicate_pushdown_keeps_limit_residual() -> None:
    plan = (
        QueryPlan.scan("t")
        .where(
            And(
                (
                    Comparison(ComparisonOp.GT, Column("a"), Literal.of(1)),
                    Like(Column("n"), Literal.of("a%")),
                )
            )
        )
        .limit(5)
    )
    caps = PushdownCapabilities(
        projection=Support.FULL,
        predicate=Support.FULL,
        limit=Support.FULL,
        expressions=frozenset(
            {ExpressionKind.COLUMN, ExpressionKind.LITERAL, ExpressionKind.COMPARISON}
        ),
    )
    ep = PLANNER.plan(bind_plan(plan, SCHEMA), _target(caps))
    f, limit = ep.explain.node("1-filter"), ep.explain.node("2-limit")
    assert f.disposition is Disposition.PARTIAL and f.reason_code is DiagnosticCode.PUSHDOWN_PARTIAL
    assert str(ep.pushed.predicate) == "(a > 1)"
    assert str(ep.residual.predicate) == "(n LIKE 'a%')"
    assert limit.disposition is Disposition.RESIDUAL
    assert limit.reason_code is DiagnosticCode.RESIDUAL_LIMIT_AFTER_RESIDUAL_FILTER
    assert ep.pushed.limit is None and ep.residual.limit == 5


def test_parameters_are_not_pushed_without_parameter_support() -> None:
    plan = QueryPlan.scan("t").where(Comparison(ComparisonOp.GT, Column("a"), Parameter("x")))
    caps = PushdownCapabilities(
        predicate=Support.FULL, expressions=ALL_EXPRESSION_KINDS, parameters=False
    )
    ep = PLANNER.plan(bind_plan(plan, SCHEMA), _target(caps))
    assert ep.explain.node("1-filter").disposition is Disposition.RESIDUAL
    assert ep.explain.node("1-filter").reason_code is DiagnosticCode.RESIDUAL_UNSUPPORTED_EXPRESSION


def test_partial_predicate_is_rechecked_by_the_engine() -> None:
    predicate = Comparison(ComparisonOp.GT, Column("a"), Literal.of(1))
    caps = PushdownCapabilities(
        predicate=Support.PARTIAL,
        expressions=ALL_EXPRESSION_KINDS,
        parameters=True,
        evidence=("safe relaxation",),
    )
    ep = PLANNER.plan(bind_plan(QueryPlan.scan("t").where(predicate), SCHEMA), _target(caps))
    assert ep.pushed.predicate == predicate
    assert ep.residual.predicate == predicate
    assert ep.explain.node("1-filter").disposition is Disposition.PARTIAL
    assert check_completeness(ep) == ()

    missing_recheck = replace(ep, residual=replace(ep.residual, predicate=None))
    assert "partial predicate coverage differs" in check_completeness(missing_recheck)[0]


def test_engine_can_implicitly_conjoin_top_level_residual_clauses() -> None:
    expression_kinds = frozenset(
        {
            ExpressionKind.COLUMN,
            ExpressionKind.LITERAL,
            ExpressionKind.COMPARISON,
        }
    )
    predicate = And(
        (
            Comparison(ComparisonOp.GT, Column("a"), Literal.of(1)),
            Comparison(ComparisonOp.LT, Column("b"), Literal.of(5)),
        )
    )
    engine = EngineCapabilities("conjunctive", residual_expressions=expression_kinds)
    assert engine.supports_expression(predicate)
    ep = PLANNER.plan(
        bind_plan(QueryPlan.scan("t").where(predicate), SCHEMA),
        _target(PushdownCapabilities.none(), engine),
    )
    assert ep.executable


def test_completeness_rejects_duplicate_full_predicate_evaluation() -> None:
    predicate = Comparison(ComparisonOp.GT, Column("a"), Literal.of(1))
    ep = PLANNER.plan(
        bind_plan(QueryPlan.scan("t").where(predicate), SCHEMA),
        _target(PushdownCapabilities.full()),
    )
    duplicate = replace(ep, residual=replace(ep.residual, predicate=predicate))
    assert "predicate conjuncts differ" in check_completeness(duplicate)[0]


def test_completeness_rejects_predicates_pushed_beyond_scan_capabilities() -> None:
    predicate = Comparison(ComparisonOp.GT, Column("a"), Literal.of(1))
    none = PLANNER.plan(
        bind_plan(QueryPlan.scan("t").where(predicate), SCHEMA),
        _target(PushdownCapabilities.none()),
    )
    forged_none = replace(
        none,
        pushed=replace(none.pushed, predicate=predicate),
        residual=replace(none.residual, predicate=None),
    )
    assert any("exceeds scan capabilities" in issue for issue in check_completeness(forged_none))

    partial_caps = PushdownCapabilities(
        predicate=Support.PARTIAL,
        expressions=frozenset({ExpressionKind.COLUMN, ExpressionKind.LITERAL}),
    )
    partial = PLANNER.plan(
        bind_plan(QueryPlan.scan("t").where(predicate), SCHEMA),
        _target(partial_caps),
    )
    forged_partial = replace(partial, pushed=replace(partial.pushed, predicate=predicate))
    assert any("exceeds scan capabilities" in issue for issue in check_completeness(forged_partial))


def test_completeness_rejects_residual_work_beyond_engine_capabilities() -> None:
    predicate = Comparison(ComparisonOp.GT, Column("a"), Literal.of(1))
    weak = EngineCapabilities(
        "weak",
        residual_expressions=ALL_EXPRESSION_KINDS - {ExpressionKind.COMPARISON},
    )
    pushed = PLANNER.plan(
        bind_plan(QueryPlan.scan("t").where(predicate), SCHEMA),
        _target(PushdownCapabilities.full(), weak),
    )
    forged_predicate = replace(
        pushed,
        pushed=replace(pushed.pushed, predicate=None),
        residual=replace(pushed.residual, predicate=predicate),
    )
    assert any(
        "residual predicate exceeds" in issue for issue in check_completeness(forged_predicate)
    )

    computed = PLANNER.plan(
        bind_plan(
            QueryPlan.scan("t").select(
                Alias(Arithmetic(ArithmeticOp.ADD, Column("a"), Literal.of(1)), "a1")
            ),
            SCHEMA,
        ),
        _target(PushdownCapabilities.full()),
    )
    no_projection = replace(
        computed.explain,
        engine_capabilities={
            **(computed.explain.engine_capabilities or {}),
            "residual_projection": False,
        },
    )
    assert "residual projection exceeds engine capabilities" in check_completeness(
        replace(computed, explain=no_projection)
    )

    limited = PLANNER.plan(
        bind_plan(QueryPlan.scan("t").limit(1), SCHEMA),
        _target(PushdownCapabilities.none()),
    )
    no_limit = replace(
        limited.explain,
        engine_capabilities={
            **(limited.explain.engine_capabilities or {}),
            "residual_limit": False,
        },
    )
    assert "residual limit exceeds engine capabilities" in check_completeness(
        replace(limited, explain=no_limit)
    )


def test_partial_predicate_is_rejected_when_engine_cannot_recheck_it() -> None:
    predicate = Comparison(ComparisonOp.GT, Column("a"), Literal.of(1))
    caps = PushdownCapabilities(
        predicate=Support.PARTIAL,
        expressions=ALL_EXPRESSION_KINDS,
        parameters=True,
    )
    engine = EngineCapabilities(
        "weak", residual_expressions=ALL_EXPRESSION_KINDS - {ExpressionKind.COMPARISON}
    )
    ep = PLANNER.plan(
        bind_plan(QueryPlan.scan("t").where(predicate), SCHEMA), _target(caps, engine)
    )
    assert not ep.executable
    assert ep.explain.node("1-filter").disposition is Disposition.REJECTED


def test_computed_projection_prunes_columns_and_stays_residual() -> None:
    plan = (
        QueryPlan.scan("t")
        .where(IsNull(Column("n")))
        .select(Alias(Arithmetic(ArithmeticOp.MUL, Column("a"), Literal.of(2)), "a2"))
    )
    caps = PushdownCapabilities(projection=Support.FULL, predicate=Support.NONE)
    ep = PLANNER.plan(bind_plan(plan, SCHEMA), _target(caps))
    project = ep.explain.node("2-project")
    assert project.disposition is Disposition.PARTIAL
    assert project.reason_code is DiagnosticCode.RESIDUAL_COMPUTED_PROJECTION
    assert ep.pushed.projection == ("a", "n")  # n is needed by the residual predicate
    assert ep.residual.projection == plan.projection
    assert ep.output_schema.names == ("a2",)


def test_completeness_rejects_missing_residual_input_and_projection_on_select_star() -> None:
    plan = QueryPlan.scan("t").select(
        Alias(Arithmetic(ArithmeticOp.ADD, Column("a"), Column("b")), "total")
    )
    ep = PLANNER.plan(bind_plan(plan, SCHEMA), _target(PushdownCapabilities.full()))
    missing_input = replace(ep, pushed=replace(ep.pushed, projection=("a",)))
    assert "projection placement differs" in check_completeness(missing_input)[0]

    select_all = PLANNER.plan(
        bind_plan(QueryPlan.scan("t"), SCHEMA), _target(PushdownCapabilities.full())
    )
    silent_pruning = replace(select_all, pushed=replace(select_all.pushed, projection=("a",)))
    assert "without a logical projection" in check_completeness(silent_pruning)[0]


def test_constant_projection_reads_rows_instead_of_zero_columns() -> None:
    plan = QueryPlan.scan("t").select(Alias(Literal.of(1), "one"))
    ep = PLANNER.plan(bind_plan(plan, SCHEMA), _target(PushdownCapabilities.full("x")))
    project = ep.explain.node("1-project")
    assert project.disposition is Disposition.RESIDUAL
    assert ep.pushed.projection is None
    assert ep.residual.projection == plan.projection
    assert "preserve row cardinality" in project.detail

    zero_columns = replace(ep, pushed=replace(ep.pushed, projection=()))
    assert "projection placement differs" in check_completeness(zero_columns)[0]


def test_partial_projection_is_trimmed_by_the_engine() -> None:
    plan = QueryPlan.scan("t").select("a")
    caps = PushdownCapabilities(projection=Support.PARTIAL)
    ep = PLANNER.plan(bind_plan(plan, SCHEMA), _target(caps))
    assert ep.pushed.projection == ("a",)
    assert ep.residual.projection == plan.projection
    assert ep.explain.node("1-project").disposition is Disposition.PARTIAL

    missing_enforcement = replace(ep, residual=replace(ep.residual, projection=None))
    assert "projection placement differs" in check_completeness(missing_enforcement)[0]


def test_projection_is_rejected_when_the_engine_lacks_the_expression() -> None:
    plan = QueryPlan.scan("t").select(
        Alias(Arithmetic(ArithmeticOp.ADD, Column("a"), Literal.of(1)), "a1")
    )
    engine = EngineCapabilities(
        "weak", residual_expressions=ALL_EXPRESSION_KINDS - {ExpressionKind.ARITHMETIC}
    )
    ep = PLANNER.plan(bind_plan(plan, SCHEMA), _target(PushdownCapabilities.full(), engine))
    assert not ep.executable
    assert (
        ep.explain.node("1-project").reason_code
        is DiagnosticCode.REJECTED_ENGINE_UNSUPPORTED_EXPRESSION
    )


def test_projection_is_rejected_when_engine_cannot_project_residuals() -> None:
    plan = QueryPlan.scan("t").select(
        Alias(Arithmetic(ArithmeticOp.ADD, Column("a"), Literal.of(1)), "a1")
    )
    engine = EngineCapabilities("weak", residual_projection=False)
    ep = PLANNER.plan(bind_plan(plan, SCHEMA), _target(PushdownCapabilities.full(), engine))
    assert not ep.executable
    assert (
        ep.explain.node("1-project").reason_code
        is DiagnosticCode.REJECTED_ENGINE_UNSUPPORTED_OPERATION
    )


@pytest.mark.parametrize("support", [Support.NONE, Support.PARTIAL])
def test_limit_is_rejected_when_the_engine_cannot_enforce_a_residual(
    support: Support,
) -> None:
    plan = QueryPlan.scan("t").limit(5)
    caps = PushdownCapabilities(limit=support)
    engine = EngineCapabilities("weak", residual_limit=False)
    ep = PLANNER.plan(bind_plan(plan, SCHEMA), _target(caps, engine))
    assert not ep.executable
    assert (
        ep.explain.node("1-limit").reason_code
        is DiagnosticCode.REJECTED_ENGINE_UNSUPPORTED_OPERATION
    )


def test_completeness_enforces_partial_and_post_filter_limit_placement() -> None:
    partial = PLANNER.plan(
        bind_plan(QueryPlan.scan("t").limit(5), SCHEMA),
        _target(PushdownCapabilities(limit=Support.PARTIAL)),
    )
    missing_recheck = replace(partial, residual=replace(partial.residual, limit=None))
    assert "limit placement differs" in check_completeness(missing_recheck)[0]

    plan = QueryPlan.scan("t").where(Like(Column("n"), Literal.of("x%"))).limit(5)
    caps = PushdownCapabilities(
        predicate=Support.FULL,
        limit=Support.FULL,
        expressions=frozenset({ExpressionKind.COLUMN, ExpressionKind.LITERAL}),
    )
    filtered = PLANNER.plan(bind_plan(plan, SCHEMA), _target(caps))
    premature = replace(filtered, pushed=replace(filtered.pushed, limit=5))
    assert "limit placement differs" in check_completeness(premature)[0]


def test_rejected_when_neither_side_can_evaluate() -> None:
    plan = QueryPlan.scan("t").where(Like(Column("n"), Literal.of("x%")))
    engine = EngineCapabilities(
        "weak", residual_expressions=ALL_EXPRESSION_KINDS - {ExpressionKind.LIKE}
    )
    ep = PLANNER.plan(bind_plan(plan, SCHEMA), _target(PushdownCapabilities.none(), engine))
    assert not ep.executable
    assert (
        ep.explain.node("1-filter").reason_code
        is DiagnosticCode.REJECTED_ENGINE_UNSUPPORTED_EXPRESSION
    )
    assert ep.explain.diagnostics[0].code is DiagnosticCode.REJECTED_ENGINE_UNSUPPORTED_EXPRESSION
    assert check_completeness(ep) == ()


def test_unreachable_source_requires_staging() -> None:
    plan = QueryPlan.scan("t").limit(1)
    ep = PLANNER.plan(
        bind_plan(plan, SCHEMA), _target(PushdownCapabilities.full(), reachable=False)
    )
    assert ep.staging_required and not ep.executable
    assert ep.explain.diagnostics[0].code is DiagnosticCode.STAGING_REQUIRED
    assert ep.explain.to_dict()["nodes"][0]["reason_code"] == "REJECTED_SOURCE_UNREACHABLE"


def test_explain_structure_is_stable() -> None:
    plan = QueryPlan.scan("t").where(IsNull(Column("a"))).select("a").limit(2)
    ep = PLANNER.plan(bind_plan(plan, SCHEMA), _target(PushdownCapabilities.full("ev")))
    data = ep.explain.to_dict()
    assert set(data) == {
        "version",
        "engine",
        "source",
        "fingerprint",
        "executable",
        "staging_required",
        "nodes",
        "diagnostics",
        "scan_capabilities",
        "engine_capabilities",
    }
    assert set(data["nodes"][0]) == {
        "node_id",
        "operation",
        "disposition",
        "location",
        "reason_code",
        "detail",
        "pushed",
        "residual",
        "evidence",
    }
    assert data["fingerprint"] == plan.fingerprint()
    assert "ev" in data["nodes"][0]["evidence"]
