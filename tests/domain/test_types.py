from __future__ import annotations

import pytest

from invariantql.domain import (
    DecimalType,
    FloatType,
    IntegerType,
    ListType,
    NullType,
    StructType,
    TimestampType,
    UnknownType,
)
from invariantql.domain.types import normalise_portable_type, unify


def test_numeric_unification_is_symmetric() -> None:
    pairs = (
        (IntegerType(8), IntegerType(64)),
        (FloatType(32), FloatType(64)),
        (IntegerType(16), FloatType(32)),
        (IntegerType(64), DecimalType(5, 2)),
        (DecimalType(5, 2), DecimalType(18, 6)),
    )

    for left, right in pairs:
        assert unify(left, right) == unify(right, left)

    assert unify(IntegerType(8), IntegerType(64)) == IntegerType(64)
    assert unify(FloatType(32), FloatType(64)) == FloatType(64)
    assert unify(IntegerType(16), FloatType(32)) == FloatType(64)
    assert unify(IntegerType(64), DecimalType(5, 2)) == DecimalType(21, 2)
    assert unify(DecimalType(5, 2), DecimalType(18, 6)) == DecimalType(18, 6)


def test_integer_arithmetic_uses_checked_int64_results() -> None:
    for operation in ("+", "-", "*"):
        assert unify(IntegerType(8), IntegerType(8), operation=operation) == IntegerType(64)


def test_absent_type_unification_is_symmetric() -> None:
    assert unify(UnknownType(), NullType()) == UnknownType()
    assert unify(NullType(), UnknownType()) == UnknownType()
    assert unify(NullType(), IntegerType()) == IntegerType()
    assert unify(IntegerType(), NullType()) == IntegerType()


def test_decimal_arithmetic_rejects_lossy_or_integral_overflow() -> None:
    with pytest.raises(ValueError, match="without scale reduction"):
        unify(DecimalType(38, 20), DecimalType(18, 10), operation="*")
    with pytest.raises(ValueError, match="integral digits"):
        unify(DecimalType(38, 0), DecimalType(38, 0), operation="+")


def test_aware_timestamps_normalise_to_utc_recursively() -> None:
    source = StructType(
        (
            ("when", TimestampType("Europe/Berlin")),
            ("history", ListType(TimestampType("America/New_York"))),
        )
    )
    assert normalise_portable_type(source) == StructType(
        (
            ("when", TimestampType("UTC")),
            ("history", ListType(TimestampType("UTC"))),
        )
    )
