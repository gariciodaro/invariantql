"""Semantic validation of a plan against a source schema.

Binding resolves column names to schema fields (exact match first, then an
unambiguous case-insensitive match), checks that comparisons are between
comparable types, and derives the output schema. It performs no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass

from invariantql.domain.diagnostics import DiagnosticCode, PlanValidationError
from invariantql.domain.expressions import (
    Alias,
    And,
    Arithmetic,
    ArithmeticOp,
    Column,
    Comparison,
    Expression,
    In,
    IsNull,
    Like,
    Literal,
    Not,
    Or,
    Parameter,
    output_name,
)
from invariantql.domain.plan import QueryPlan
from invariantql.domain.schema import Field, Schema
from invariantql.domain.types import (
    BooleanType,
    DataType,
    FloatType,
    NullType,
    UnknownType,
    is_comparable,
    is_numeric,
    unify,
)


@dataclass(frozen=True, slots=True)
class BoundPlan:
    plan: QueryPlan
    schema: Schema
    output_schema: Schema


def bind_plan(plan: QueryPlan, schema: Schema) -> BoundPlan:
    binder = _Binder(schema)
    predicate = plan.predicate
    projection = plan.projection

    bound_predicate = None
    if predicate is not None:
        bound_predicate = binder.bind(predicate, node_id="1-filter")
        ptype = binder.type_of(bound_predicate)
        if ptype.kind not in ("boolean", "unknown", "null"):
            raise PlanValidationError(
                f"WHERE predicate must be boolean, got {ptype}",
                code=DiagnosticCode.PLAN_TYPE_MISMATCH,
                node_id="1-filter",
            )

    bound_projection: tuple[Expression, ...] | None = None
    output_fields: list[Field]
    if projection is not None:
        exprs = []
        output_fields = []
        for expression in projection:
            bound = binder.bind(expression, node_id="2-project")
            exprs.append(bound)
            inner = bound.expression if isinstance(bound, Alias) else bound
            output_fields.append(
                Field(output_name(bound), binder.type_of(inner), binder.nullable_of(inner))
            )
        bound_projection = tuple(exprs)
    else:
        output_fields = list(schema.fields)

    rebuilt = QueryPlan.scan(plan.source)
    if bound_predicate is not None:
        rebuilt = rebuilt.where(bound_predicate)
    if bound_projection is not None:
        rebuilt = rebuilt.select(*bound_projection)
    if plan.limit_count is not None:
        rebuilt = rebuilt.limit(plan.limit_count)
    return BoundPlan(rebuilt, schema, Schema(tuple(output_fields)))


class _Binder:
    def __init__(self, schema: Schema) -> None:
        self.schema = schema

    def bind(self, expression: Expression, *, node_id: str) -> Expression:
        if isinstance(expression, Column):
            field = self.schema.resolve(expression.name)
            if field is None:
                raise PlanValidationError(
                    f"unknown column {expression.name!r}; available: {', '.join(self.schema.names)}",
                    code=DiagnosticCode.PLAN_UNKNOWN_COLUMN,
                    node_id=node_id,
                    details={"column": expression.name},
                )
            return Column(field.name)
        if isinstance(expression, (Literal, Parameter)):
            return expression
        if isinstance(expression, Alias):
            return Alias(self.bind(expression.expression, node_id=node_id), expression.name)
        if isinstance(expression, Comparison):
            left = self.bind(expression.left, node_id=node_id)
            right = self.bind(expression.right, node_id=node_id)
            self._check_comparable(left, right, node_id, str(expression))
            return Comparison(expression.op, left, right)
        if isinstance(expression, And):
            return And(tuple(self.bind(o, node_id=node_id) for o in expression.operands))
        if isinstance(expression, Or):
            return Or(tuple(self.bind(o, node_id=node_id) for o in expression.operands))
        if isinstance(expression, Not):
            return Not(self.bind(expression.operand, node_id=node_id))
        if isinstance(expression, IsNull):
            return IsNull(self.bind(expression.operand, node_id=node_id), expression.negated)
        if isinstance(expression, In):
            operand = self.bind(expression.operand, node_id=node_id)
            values = tuple(self.bind(v, node_id=node_id) for v in expression.values)
            for value in values:
                self._check_comparable(operand, value, node_id, str(expression))
            return In(operand, values, expression.negated)
        if isinstance(expression, Like):
            operand = self.bind(expression.operand, node_id=node_id)
            otype = self.type_of(operand)
            if otype.kind not in ("string", "unknown", "null"):
                raise PlanValidationError(
                    f"LIKE requires a string operand, got {otype} in {expression}",
                    code=DiagnosticCode.PLAN_TYPE_MISMATCH,
                    node_id=node_id,
                )
            return Like(operand, self.bind(expression.pattern, node_id=node_id), expression.negated)
        if isinstance(expression, Arithmetic):
            left = self.bind(expression.left, node_id=node_id)
            right = self.bind(expression.right, node_id=node_id)
            for side in (left, right):
                stype = self.type_of(side)
                if stype.kind not in ("unknown", "null") and not is_numeric(stype):
                    raise PlanValidationError(
                        f"arithmetic requires numeric operands, got {stype} in {expression}",
                        code=DiagnosticCode.PLAN_TYPE_MISMATCH,
                        node_id=node_id,
                    )
            return Arithmetic(expression.op, left, right)
        raise PlanValidationError(
            f"unsupported expression node {type(expression).__name__}",
            code=DiagnosticCode.PLAN_INVALID_SHAPE,
            node_id=node_id,
        )

    def _check_comparable(
        self, left: Expression, right: Expression, node_id: str, text: str
    ) -> None:
        ltype, rtype = self.type_of(left), self.type_of(right)
        if not is_comparable(ltype, rtype):
            raise PlanValidationError(
                f"cannot compare {ltype} with {rtype} in {text}",
                code=DiagnosticCode.PLAN_TYPE_MISMATCH,
                node_id=node_id,
            )

    def type_of(self, expression: Expression) -> DataType:
        if isinstance(expression, Column):
            field = self.schema.resolve(expression.name)
            return field.data_type if field is not None else UnknownType()
        if isinstance(expression, Literal):
            return expression.data_type
        if isinstance(expression, Parameter):
            return UnknownType()
        if isinstance(expression, Alias):
            return self.type_of(expression.expression)
        if isinstance(expression, (Comparison, And, Or, Not, IsNull, In, Like)):
            return BooleanType()
        if isinstance(expression, Arithmetic):
            if expression.op is ArithmeticOp.DIV:
                return FloatType(64)
            left, right = self.type_of(expression.left), self.type_of(expression.right)
            if isinstance(left, (UnknownType, NullType)) and isinstance(
                right, (UnknownType, NullType)
            ):
                return UnknownType()
            return unify(left, right)
        return UnknownType()

    def nullable_of(self, expression: Expression) -> bool:
        if isinstance(expression, Column):
            field = self.schema.resolve(expression.name)
            return field.nullable if field is not None else True
        if isinstance(expression, Literal):
            return expression.value is None
        if isinstance(expression, Arithmetic):
            return self.nullable_of(expression.left) or self.nullable_of(expression.right)
        return True


__all__ = ["BoundPlan", "bind_plan"]
