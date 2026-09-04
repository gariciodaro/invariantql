"""Pure type semantics shared by binders and engine translators."""

from __future__ import annotations

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
)
from invariantql.domain.schema import Schema
from invariantql.domain.types import BooleanType, DataType, UnknownType, unify


def expression_type(expression: Expression, schema: Schema) -> DataType:
    """Derive an expression's logical type from a resolved source schema.

    Parameters remain unknown until substituted with their typed literals.
    Callers at an execution boundary should substitute parameters first.
    """

    if isinstance(expression, Column):
        field = schema.resolve(expression.name)
        return field.data_type if field is not None else UnknownType()
    if isinstance(expression, Literal):
        return expression.data_type
    if isinstance(expression, Parameter):
        return UnknownType()
    if isinstance(expression, Alias):
        return expression_type(expression.expression, schema)
    if isinstance(expression, (Comparison, And, Or, Not, IsNull, In, Like)):
        return BooleanType()
    if isinstance(expression, Arithmetic):
        return unify(
            expression_type(expression.left, schema),
            expression_type(expression.right, schema),
            operation=expression.op.value,
        )
    return UnknownType()


__all__ = ["expression_type"]
