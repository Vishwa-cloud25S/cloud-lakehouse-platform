"""Environment + dataset configuration.

Config is data, not code: `conf/<env>.yaml` describes *where* the lakehouse lives,
`conf/datasets.yaml` describes *what* flows through it. Jobs receive only `--env`
and `--dataset`, so promoting dev -> prod never edits Python.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Any

import yaml

CONF_DIR = Path(os.environ.get("LAKEHOUSE_CONF_DIR", Path(__file__).resolve().parents[2] / "conf"))
_ENV_VAR = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


def _expand(value: Any) -> Any:
    """Recursively expand ${VAR} / ${VAR:-default} in loaded YAML."""
    if isinstance(value, str):
        return _ENV_VAR.sub(lambda m: os.environ.get(m.group(1), m.group(2) or ""), value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        return _expand(yaml.safe_load(fh) or {})


@dataclass(frozen=True)
class DatasetConfig:
    """Declarative contract for one dataset as it moves through the medallion."""

    name: str
    source_format: str = "csv"
    source_path: str = ""
    primary_keys: list[str] = field(default_factory=list)
    watermark_column: str = "updated_at"
    partition_by: list[str] = field(default_factory=list)
    zorder_by: list[str] = field(default_factory=list)
    scd: str = "type1"  # type1 | type2
    write_mode: str = "merge"  # append | merge | overwrite
    schema_evolution: str = "additive"  # additive | strict | none
    quality_suite: str = ""
    quarantine_on_fail: bool = True
    pii_columns: list[str] = field(default_factory=list)
    expected_freshness_hours: int = 24
    tags: dict[str, str] = field(default_factory=dict)

    @property
    def bronze_table(self) -> str:
        return f"bronze_{self.name}"

    @property
    def silver_table(self) -> str:
        return f"silver_{self.name}"


@dataclass(frozen=True)
class EnvConfig:
    """Everything environment-specific: storage, catalog, cluster sizing, alerting."""

    env: str
    catalog: str
    storage_account: str
    container: str
    schemas: dict[str, str]
    root_path: str
    checkpoint_path: str
    quarantine_path: str
    shuffle_partitions: int = 200
    auto_optimize: bool = True
    vacuum_retention_hours: int = 168
    delta_retention_check: bool = True
    alert_emails: list[str] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)

    def schema(self, layer: str) -> str:
        try:
            return self.schemas[layer]
        except KeyError as exc:  # pragma: no cover - defensive
            raise KeyError(
                f"unknown layer '{layer}', expected one of {list(self.schemas)}"
            ) from exc

    def fqn(self, layer: str, table: str) -> str:
        """Three-level Unity Catalog name: catalog.schema.table."""
        return f"{self.catalog}.{self.schema(layer)}.{table}"

    def layer_path(self, layer: str, table: str = "") -> str:
        base = f"{self.root_path.rstrip('/')}/{layer}"
        return f"{base}/{table}" if table else base

    @property
    def is_local(self) -> bool:
        return self.env == "local" or self.root_path.startswith(("/", "./", "file:"))


@cache
def load_env(env: str = "dev") -> EnvConfig:
    """Load and validate `conf/<env>.yaml`."""
    data = _load_yaml(CONF_DIR / f"{env}.yaml")
    storage = data.get("storage", {})
    account = storage.get("account", "")
    container = storage.get("container", "lakehouse")
    root = storage.get("root_path") or (
        f"abfss://{container}@{account}.dfs.core.windows.net" if account else "./data/lake"
    )
    return EnvConfig(
        env=data.get("env", env),
        catalog=data["catalog"],
        storage_account=account,
        container=container,
        schemas=data.get("schemas", {"bronze": "bronze", "silver": "silver", "gold": "gold"}),
        root_path=root,
        checkpoint_path=storage.get("checkpoint_path", f"{root.rstrip('/')}/_checkpoints"),
        quarantine_path=storage.get("quarantine_path", f"{root.rstrip('/')}/_quarantine"),
        shuffle_partitions=int(data.get("spark", {}).get("shuffle_partitions", 200)),
        auto_optimize=bool(data.get("spark", {}).get("auto_optimize", True)),
        vacuum_retention_hours=int(data.get("maintenance", {}).get("vacuum_retention_hours", 168)),
        delta_retention_check=bool(data.get("maintenance", {}).get("retention_check", True)),
        alert_emails=data.get("monitoring", {}).get("alert_emails", []),
        tags=data.get("tags", {}),
        raw=data,
    )


@cache
def load_datasets() -> dict[str, DatasetConfig]:
    """Load every dataset contract from `conf/datasets.yaml`."""
    data = _load_yaml(CONF_DIR / "datasets.yaml")
    out: dict[str, DatasetConfig] = {}
    for name, spec in (data.get("datasets") or {}).items():
        spec = dict(spec or {})
        spec.pop("name", None)
        out[name] = DatasetConfig(name=name, **spec)
    return out


def load_dataset(name: str) -> DatasetConfig:
    datasets = load_datasets()
    if name not in datasets:
        raise KeyError(f"unknown dataset '{name}'. Known: {sorted(datasets)}")
    return datasets[name]
