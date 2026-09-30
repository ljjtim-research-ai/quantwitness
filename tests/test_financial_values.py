from __future__ import annotations

from decimal import Decimal

import pytest

from research_pipeline.domain import DomainContractError, Money, Price


def test_fixed_values_are_exact_and_hash_stable() -> None:
    left = Money.from_decimal("12.34", scale=2, currency="CNY")
    right = Money(66, 2, "CNY")
    assert left.decimal == Decimal("12.34")
    assert (left + right).decimal == Decimal("13.00")
    assert left.value_hash == Money(1234, 2, "CNY").value_hash
    assert Price(1001, 2, "CNY").notional(100) == Money(100100, 2, "CNY")


def test_fixed_values_reject_implicit_rounding_and_mixed_identity() -> None:
    with pytest.raises(DomainContractError, match="隐式舍入"):
        Money.from_decimal("1.001", scale=2, currency="CNY")
    with pytest.raises(DomainContractError, match="scale/currency"):
        Money(100, 2, "CNY") + Money(100, 2, "USD")
    with pytest.raises(DomainContractError, match="decimal128"):
        Money(10**38, 0, "CNY")
    with pytest.raises(DomainContractError, match="正数"):
        Price(0, 2, "CNY")
