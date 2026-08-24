"""Data quality engine: pass/fail, quarantine split, blocking rules."""

from __future__ import annotations

import pytest

from lakehouse.quality.expectations import Action, Expectation, ExpectationSuite
from lakehouse.quality.runner import DataQualityError, DataQualityRunner


@pytest.mark.spark
def test_quarantine_splits_valid_and_invalid(spark, orders_df):
    suite = ExpectationSuite(
        name="t",
        expectations=[
            Expectation("qty_positive", "quantity > 0", Action.QUARANTINE),
            Expectation("price_non_negative", "unit_price >= 0", Action.QUARANTINE),
        ],
    )
    valid, bad, report = DataQualityRunner(spark).evaluate(orders_df, suite, "orders")
    assert report.total_rows == 5
    assert valid.count() == 3  # two defective rows removed
    assert bad.count() == 2
    assert report.quarantined_rows == 2
    assert 0.5 < report.pass_rate < 1.0


@pytest.mark.spark
def test_blocking_rule_raises(spark, orders_df):
    suite = ExpectationSuite(
        name="t",
        expectations=[Expectation("id_format", "order_id RLIKE '^ORD-[0-9]{8}$'", Action.FAIL)],
    )
    with pytest.raises(DataQualityError):
        DataQualityRunner(spark).evaluate(orders_df, suite, "orders")


@pytest.mark.spark
def test_tolerance_absorbs_small_failure_rate(spark, orders_df):
    suite = ExpectationSuite(
        name="t",
        expectations=[
            Expectation("id_format", "order_id RLIKE '^ORD-[0-9]{8}$'", Action.FAIL, tolerance=0.5)
        ],
    )
    _, _, report = DataQualityRunner(spark).evaluate(orders_df, suite, "orders")
    assert report.results[0].passed  # 1/5 = 20% <= 50% tolerance


@pytest.mark.spark
def test_warn_rule_does_not_quarantine(spark, orders_df):
    suite = ExpectationSuite(
        name="t", expectations=[Expectation("always_false", "1 = 0", Action.WARN)]
    )
    valid, bad, report = DataQualityRunner(spark).evaluate(orders_df, suite, "orders")
    assert valid.count() == 5
    assert bad.count() == 0
    assert report.results[0].failed_rows == 5


@pytest.mark.spark
def test_unresolvable_rule_is_skipped_not_fatal(spark, orders_df):
    """A rule on an absent optional column must not crash the pipeline."""
    suite = ExpectationSuite(
        name="t",
        expectations=[
            Expectation("no_corrupt", "_corrupt_record IS NULL", Action.QUARANTINE),
            Expectation("qty_positive", "quantity > 0", Action.QUARANTINE),
        ],
    )
    valid, _, report = DataQualityRunner(spark).evaluate(orders_df, suite, "orders")
    actions = {r.name: r.action for r in report.results}
    assert actions["no_corrupt"] == "skipped"
    assert valid.count() == 4  # only the quantity rule applied
    # A skipped rule must never be counted as a failure.
    skipped = next(r for r in report.results if r.name == "no_corrupt")
    assert skipped.passed


@pytest.mark.spark
def test_null_predicate_counts_as_failure(spark):
    df = spark.createDataFrame([(None,), ("ok",)], "v string")
    suite = ExpectationSuite(
        name="t", expectations=[Expectation("len_ok", "length(v) > 1", Action.QUARANTINE)]
    )
    valid, bad, _ = DataQualityRunner(spark).evaluate(df, suite, "t")
    assert valid.count() == 1 and bad.count() == 1
