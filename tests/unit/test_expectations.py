"""Expectation suite loading and predicate construction."""

from __future__ import annotations

import pytest

from lakehouse.quality.expectations import Action, Expectation, ExpectationSuite, load_suite


def test_every_shipped_suite_loads():
    for name in ("orders", "customers", "products"):
        suite = load_suite(name)
        assert suite.expectations, f"{name} suite is empty"
        for exp in suite.expectations:
            assert exp.expr.strip()
            assert 0.0 <= exp.tolerance <= 1.0


def test_orders_pk_rule_is_blocking():
    suite = load_suite("orders")
    blocking = {e.name for e in suite.blocking}
    assert "order_id_not_null" in blocking


def test_pass_expression_combines_with_and():
    suite = ExpectationSuite(
        name="t",
        expectations=[
            Expectation("a", "x > 0", Action.QUARANTINE),
            Expectation("b", "y IS NOT NULL", Action.FAIL),
            Expectation("c", "z < 10", Action.WARN),
        ],
    )
    expr = suite.pass_expression((Action.QUARANTINE, Action.FAIL))
    assert "(x > 0)" in expr and "(y IS NOT NULL)" in expr
    assert "z < 10" not in expr  # warn rules never quarantine
    assert " AND " in expr


def test_empty_suite_passes_everything():
    assert ExpectationSuite("empty").pass_expression((Action.FAIL,)) == "true"


def test_missing_suite_raises():
    with pytest.raises(FileNotFoundError):
        load_suite("does_not_exist")
