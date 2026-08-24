"""Configuration loading, env-var expansion and naming."""

from __future__ import annotations

import os

import pytest

from lakehouse.config import load_dataset, load_datasets, load_env


def test_local_env_is_local():
    cfg = load_env("local")
    assert cfg.is_local
    assert cfg.catalog == "lakehouse_local"
    assert cfg.shuffle_partitions == 4


def test_dev_env_builds_abfss_root():
    os.environ["STORAGE_ACCOUNT"] = "sttest"
    load_env.cache_clear()
    cfg = load_env("dev")
    assert cfg.root_path.startswith("abfss://lakehouse@sttest.dfs.core.windows.net")
    assert not cfg.is_local
    load_env.cache_clear()


def test_three_level_namespace():
    cfg = load_env("dev")
    assert cfg.fqn("gold", "fact_orders").count(".") == 2
    assert cfg.fqn("gold", "fact_orders").startswith(cfg.catalog)


def test_unknown_layer_raises():
    with pytest.raises(KeyError):
        load_env("local").schema("platinum")


def test_datasets_have_required_contract():
    datasets = load_datasets()
    assert {"orders", "customers", "products"} <= set(datasets)
    for name, ds in datasets.items():
        assert ds.primary_keys, f"{name} must declare primary keys for MERGE"
        assert ds.watermark_column, f"{name} must declare a watermark column"
        assert ds.schema_evolution in ("additive", "strict", "none")
        assert ds.scd in ("type1", "type2")


def test_customers_is_scd2_with_pii():
    ds = load_dataset("customers")
    assert ds.scd == "type2"
    assert "email" in ds.pii_columns


def test_unknown_dataset_raises():
    with pytest.raises(KeyError):
        load_dataset("nope")
