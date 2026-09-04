"""Semantic validation of a plan against a source schema.

Binding resolves column names to schema fields (exact match first, then an
unambiguous case-insensitive match), checks that comparisons are between
comparable types, and derives the output schema. It performs no I/O.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from invariantql.domain.diagnostics import DiagnosticCode, PlanValidationError
from invariantql.domain.expressions import (
    Alias,
    And,
    Arithmetic,
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
    substitute_parameters,
)
from invariantql.domain.plan import QueryPlan
from invariantql.domain.schema import Field, Schema
from invariantql.domain.semantics import expression_type
from invariantql.domain.types import (
    PORTABLE_DECIMAL_PRECISION,
    DataType,
    is_comparable,
    is_numeric,
    is_portable_type,
    normalise_portable_type,
)


@dataclass(frozen=True, slots=True)
class BoundPlan:
    plan: QueryPlan
    schema: Schema
    output_schema: Schema


def bind_plan(
    plan: QueryPlan,
    schema: Schema,
    parameters: Mapping[str, Literal] | None = None,
) -> BoundPlan:
    binder = _Binder(schema, parameters)
    predicate = plan.predicate
    projection = plan.projection
    node_ids = {node.operation: node_id for node_id, node in plan.node_ids()}

    bound_predicate = None
    if predicate is not None:
        predicate_node_id = node_ids["filter"]
        bound_predicate = binder.bind(predicate, node_id=predicate_node_id)
        ptype = binder.type_of(bound_predicate)
        if ptype.kind not in ("boolean", "unknown", "null"):
            raise PlanValidationError(
                f"WHERE predicate must be boolean, got {ptype}",
                code=DiagnosticCode.PLAN_TYPE_MISMATCH,
                node_id=predicate_node_id,
            )

    bound_projection: tuple[Expression, ...] | None = None
    output_fields: list[Field]
    if projection is not None:
        projection_node_id = node_ids["project"]
        exprs = []
        output_fields = []
        output_names: set[str] = set()
        for expression in projection:
            bound = binder.bind(expression, node_id=projection_node_id, allow_alias=True)
            name = output_name(bound)
            if name in output_names:
                raise PlanValidationError(
                    f"duplicate output column after name resolution: {name!r}",
                    code=DiagnosticCode.PLAN_INVALID_SHAPE,
                    node_id=projection_node_id,
                    details={"column": name},
                )
            output_names.add(name)
            exprs.append(bound)
            inner = bound.expression if isinstance(bound, Alias) else bound
            output_fields.append(
                Field(name, normalise_portable_type(binder.type_of(inner)), nullable=True)
            )
        bound_projection = tuple(exprs)
    else:
        output_fields = [
            Field(field.name, normalise_portable_type(field.data_type), nullable=True)
            for field in schema
        ]

    output_node_id = node_ids["project"] if projection is not None else node_ids["scan"]
    for field in output_fields:
        binder.require_portable_type(
            field.data_type,
            node_id=output_node_id,
            context=f"output column {field.name!r}",
        )

    rebuilt = QueryPlan.scan(plan.source)
    if bound_predicate is not None:
        rebuilt = rebuilt.where(bound_predicate)
    if bound_projection is not None:
        rebuilt = rebuilt.select(*bound_projection)
    if plan.limit_count is not None:
        rebuilt = rebuilt.limit(plan.limit_count)
    return BoundPlan(rebuilt, schema, Schema(tuple(output_fields)))


class _Binder:
    def __init__(
        self,
        schema: Schema,
        parameters: Mapping[str, Literal] | None = None,
    ) -> None:
        self.schema = schema
        self.parameters = dict(parameters or {})

    def bind(
        self, expression: Expression, *, node_id: str, allow_alias: bool = False
    ) -> Expression:
        if isinstance(expression, Column):
            field = self.schema.resolve(expression.name)
            if field is None:
                raise PlanValidationError(
                    f"unknown column {expression.name!r}; available: {', '.join(self.schema.names)}",
                    code=DiagnosticCode.PLAN_UNKNOWN_COLUMN,
                    node_id=node_id,
                    details={"column": expression.name},
                )
            self.require_portable_type(
                field.data_type,
                node_id=node_id,
                context=f"column {field.name!r}",
            )
            return Column(field.name)
        if isinstance(expression, Literal):
            self.require_portable_type(
                expression.data_type,
                node_id=node_id,
                context="literal",
            )
            return expression
        if isinstance(expression, Parameter):
            literal = self.parameters.get(expression.name)
            if literal is not None:
                self.require_portable_type(
                    literal.data_type,
                    node_id=node_id,
                    context=f"parameter {expression.name!r}",
                )
            return expression
        if isinstance(expression, Alias):
            if not allow_alias:
                raise PlanValidationError(
                    "aliases are only valid at the top level of a projection",
                    code=DiagnosticCode.PLAN_INVALID_SHAPE,
                    node_id=node_id,
                )
            return Alias(self.bind(expression.expression, node_id=node_id), expression.name)
        if isinstance(expression, Comparison):
            left = self.bind(expression.left, node_id=node_id)
            right = self.bind(expression.right, node_id=node_id)
            self._check_comparable(left, right, node_id, str(expression))
            return Comparison(expression.op, left, right)
        if isinstance(expression, And):
            operands = tuple(self.bind(o, node_id=node_id) for o in expression.operands)
            for operand in operands:
                self._check_boolean(operand, node_id, str(expression))
            return And(operands)
        if isinstance(expression, Or):
            operands = tuple(self.bind(o, node_id=node_id) for o in expression.operands)
            for operand in operands:
                self._check_boolean(operand, node_id, str(expression))
            return Or(operands)
        if isinstance(expression, Not):
            operand = self.bind(expression.operand, node_id=node_id)
            self._check_boolean(operand, node_id, str(expression))
            return Not(operand)
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
            pattern = self.bind(expression.pattern, node_id=node_id)
            ptype = self.type_of(pattern)
            if ptype.kind not in ("string", "unknown", "null"):
                raise PlanValidationError(
                    f"LIKE requires a string pattern, got {ptype} in {expression}",
                    code=DiagnosticCode.PLAN_TYPE_MISMATCH,
                    node_id=node_id,
                )
            return Like(operand, pattern, expression.negated)
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
            bound = Arithmetic(expression.op, left, right)
            try:
                self.type_of(bound)
            except ValueError as exc:
                raise PlanValidationError(
                    f"arithmetic result is outside the portable type range in {expression}: {exc}",
                    code=DiagnosticCode.PLAN_TYPE_MISMATCH,
                    node_id=node_id,
                ) from None
            return bound
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

    def _check_boolean(self, expression: Expression, node_id: str, text: str) -> None:
        data_type = self.type_of(expression)
        if data_type.kind not in ("boolean", "unknown", "null"):
            raise PlanValidationError(
                f"boolean operator requires boolean operands, got {data_type} in {text}",
                code=DiagnosticCode.PLAN_TYPE_MISMATCH,
                node_id=node_id,
            )

    def type_of(self, expression: Expression) -> DataType:
        if self.parameters:
            expression = substitute_parameters(expression, self.parameters)
        return expression_type(expression, self.schema)

    def nullable_of(self, expression: Expression) -> bool:
        if isinstance(expression, Column):
            field = self.schema.resolve(expression.name)
            return field.nullable if field is not None else True
        if isinstance(expression, Literal):
            return expression.value is None
        if isinstance(expression, Arithmetic):
            return self.nullable_of(expression.left) or self.nullable_of(expression.right)
        return True

    @staticmethod
    def require_portable_type(data_type: DataType, *, node_id: str, context: str) -> None:
        if is_portable_type(data_type):
            return
        raise PlanValidationError(
            f"{context} has type {data_type}, which exceeds the Local+Spark "
            f"decimal precision limit of {PORTABLE_DECIMAL_PRECISION}",
            code=DiagnosticCode.PLAN_TYPE_MISMATCH,
            node_id=node_id,
            details={"type": str(data_type)},
        )


__all__ = ["BoundPlan", "bind_plan"]
