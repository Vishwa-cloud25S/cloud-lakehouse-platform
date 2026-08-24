"""Schema-diff logic - the policy engine, tested without Spark where possible."""

from __future__ import annotations

import pytest

from lakehouse.io.schema_registry import SAFE_WIDENING, SchemaDiff, diff_schemas


def _struct(fields: dict[str, str]):
    from pyspark.sql.types import StructType

    return StructType.fromJson(
        {
            "type": "struct",
            "fields": [
                {"name": n, "type": t, "nullable": True, "metadata": {}} for n, t in fields.items()
            ],
        }
    )


def test_empty_diff_is_empty():
    assert SchemaDiff().is_empty


def test_added_column_is_additive():
    diff = SchemaDiff(added={"promo": "string"})
    assert diff.is_additive
    assert not diff.is_empty
    assert "promo" in diff.summary()


def test_removed_column_is_not_additive():
    assert not SchemaDiff(removed={"price": "double"}).is_additive


def test_safe_widening_is_additive():
    assert SchemaDiff(changed={"qty": ("int", "bigint")}).is_additive


def test_narrowing_is_not_additive():
    assert not SchemaDiff(changed={"qty": ("bigint", "int")}).is_additive


def test_widening_table_is_transitively_sane():
    assert "double" in SAFE_WIDENING["float"]
    assert "timestamp" in SAFE_WIDENING["date"]


@pytest.mark.spark
def test_diff_schemas_detects_all_change_types(spark):
    old = _struct({"id": "string", "qty": "integer", "dropped": "string"})
    new = _struct({"id": "string", "qty": "long", "added": "string"})
    diff = diff_schemas(old, new)
    assert diff.added == {"added": "string"}
    assert diff.removed == {"dropped": "string"}
    assert diff.changed == {"qty": ("int", "bigint")}
    assert not diff.is_additive  # because of the drop
