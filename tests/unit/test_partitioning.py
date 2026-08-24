"""Partitioning advisor rules."""

from __future__ import annotations

from lakehouse.optimize.partitioning import recommend_partitions

GB = 1024**3


def test_small_table_should_not_be_partitioned():
    recs = recommend_partitions(1000, 5 * 1024**2, {"order_date": 30})
    assert len(recs) == 1
    assert recs[0].column is None
    assert recs[0].severity == "warn"


def test_high_cardinality_is_critical():
    recs = recommend_partitions(10_000_000, 100 * GB, {"order_id": 10_000_000})
    assert recs[0].severity == "critical"
    assert "Z-ORDER" in recs[0].reason


def test_too_many_partitions_is_critical():
    recs = recommend_partitions(50_000_000, 200 * GB, {"event_ts_day": 50_000})
    assert recs[0].severity == "critical"


def test_good_candidate_is_info():
    recs = recommend_partitions(100_000_000, 60 * GB, {"country": 40})
    assert recs[0].severity == "info"
    assert "Good candidate" in recs[0].reason


def test_undersized_partitions_warn():
    recs = recommend_partitions(10_000_000, 2 * GB, {"order_date": 400})
    assert recs[0].severity == "warn"
    assert "too small" in recs[0].reason
