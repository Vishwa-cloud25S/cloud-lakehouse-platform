"""Unity Catalog object management.

The three-level namespace (`catalog.schema.table`) is the backbone of governance:
grants, lineage, tags and row/column policies all attach to these objects. This
module creates them idempotently and applies the tag + grant model from
`conf/governance.yaml`, so the catalog state is reproducible from source control.

Everything degrades gracefully on a non-UC (local/OSS) Spark: `TAG` and `GRANT`
statements are skipped with a warning rather than failing the job, which is what
makes the same code path testable in CI.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import yaml

from lakehouse.config import CONF_DIR, EnvConfig
from lakehouse.logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

log = get_logger(__name__)


@dataclass
class CatalogManager:
    """Create and govern catalog objects."""

    spark: SparkSession
    cfg: EnvConfig
    unity_enabled: bool = True

    def __post_init__(self) -> None:
        if self.cfg.is_local:
            self.unity_enabled = False

    # -------------------------------------------------------------- internals
    def _try(self, sql: str, what: str) -> bool:
        """Run governance DDL, tolerating engines that do not support it."""
        try:
            self.spark.sql(sql)
            log.info("governance.applied", extra={"what": what})
            return True
        except Exception as exc:  # noqa: BLE001 - deliberate: governance is best-effort
            log.warning(
                "governance.skipped",
                extra={"what": what, "reason": str(exc).split("\n", 1)[0][:200]},
            )
            return False

    # ---------------------------------------------------------------- objects
    def create_catalog(self, managed_location: str | None = None) -> None:
        """Create the catalog. On UC a managed location makes it a governed root."""
        if not self.unity_enabled:
            log.info("governance.local_mode", extra={"catalog": self.cfg.catalog})
            return
        sql = f"CREATE CATALOG IF NOT EXISTS {self.cfg.catalog}"
        if managed_location:
            sql += f"\nMANAGED LOCATION '{managed_location}'"
        sql += f"\nCOMMENT 'Lakehouse catalog for the {self.cfg.env} environment.'"
        self._try(sql, f"create catalog {self.cfg.catalog}")

    def create_schemas(self) -> None:
        """Create bronze/silver/gold (+ ops) schemas with descriptive comments."""
        comments = {
            "bronze": "Raw, append-only landing zone. Source fidelity preserved; no business logic.",
            "silver": "Cleansed, deduplicated, quality-enforced business entities.",
            "gold": "Dimensional model and serving aggregates for BI consumption.",
            "ops": "Pipeline control plane: watermarks, schema registry, DQ results, metrics.",
        }
        for layer, schema in {
            **self.cfg.schemas,
            "ops": self.cfg.schemas.get("ops", "ops"),
        }.items():
            full = f"{self.cfg.catalog}.{schema}" if self.unity_enabled else schema
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full}")
            comment = comments.get(layer, "").replace("'", "''")
            if comment:
                self._try(f"COMMENT ON SCHEMA {full} IS '{comment}'", f"comment schema {full}")
            log.info("governance.schema_ready", extra={"schema": full})

    # ------------------------------------------------------------------- tags
    def apply_tags(self, obj: str, tags: dict[str, str], obj_type: str = "TABLE") -> None:
        """Attach UC tags - the searchable metadata layer for discovery + policy."""
        if not tags or not self.unity_enabled:
            return
        pairs = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
        self._try(f"ALTER {obj_type} {obj} SET TAGS ({pairs})", f"tag {obj_type.lower()} {obj}")

    def tag_columns(self, table: str, column_tags: dict[str, dict[str, str]]) -> None:
        """Tag individual columns, e.g. `pii = true`, `classification = confidential`."""
        if not self.unity_enabled:
            return
        for column, tags in column_tags.items():
            pairs = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
            self._try(
                f"ALTER TABLE {table} ALTER COLUMN {column} SET TAGS ({pairs})",
                f"tag column {table}.{column}",
            )

    def set_column_comments(self, table: str, comments: dict[str, str]) -> None:
        """Column-level documentation surfaces directly in Databricks Catalog Explorer."""
        for column, comment in comments.items():
            safe = comment.replace("'", "''")
            self._try(
                f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{safe}'",
                f"comment column {table}.{column}",
            )

    # ----------------------------------------------------------------- grants
    def grant(
        self, privileges: list[str], obj: str, principal: str, obj_type: str = "TABLE"
    ) -> None:
        """Apply a grant. Privileges inherit down the namespace in UC."""
        if not self.unity_enabled:
            return
        privs = ", ".join(privileges)
        self._try(
            f"GRANT {privs} ON {obj_type} {obj} TO `{principal}`",
            f"grant {privs} on {obj} to {principal}",
        )

    def apply_governance_file(self, path: str | None = None) -> dict[str, Any]:
        """Apply the declarative model in `conf/governance.yaml`."""
        conf_path = path or (CONF_DIR / "governance.yaml")
        with open(conf_path, encoding="utf-8") as fh:
            model = yaml.safe_load(fh) or {}

        for schema_name, spec in (model.get("schemas") or {}).items():
            full = f"{self.cfg.catalog}.{schema_name}"
            for principal, privs in (spec.get("grants") or {}).items():
                self.grant(privs, full, principal, obj_type="SCHEMA")
            self.apply_tags(full, spec.get("tags", {}), obj_type="SCHEMA")

        for table_name, spec in (model.get("tables") or {}).items():
            full = f"{self.cfg.catalog}.{table_name}"
            self.apply_tags(full, spec.get("tags", {}))
            self.tag_columns(full, spec.get("column_tags", {}))
            self.set_column_comments(full, spec.get("column_comments", {}))
            for principal, privs in (spec.get("grants") or {}).items():
                self.grant(privs, full, principal)

        log.info(
            "governance.model_applied",
            extra={
                "schemas": len(model.get("schemas") or {}),
                "tables": len(model.get("tables") or {}),
            },
        )
        return model

    # ------------------------------------------------------------- discovery
    def describe_lineage_ready(self, table: str) -> dict[str, Any]:
        """Return the catalog metadata UC exposes for a table (used in docs/tests)."""
        try:
            detail = self.spark.sql(f"DESCRIBE DETAIL {table}").collect()[0].asDict()
        except Exception:  # pragma: no cover
            return {}
        return {
            "format": detail.get("format"),
            "location": detail.get("location"),
            "num_files": detail.get("numFiles"),
            "size_bytes": detail.get("sizeInBytes"),
            "partition_columns": detail.get("partitionColumns"),
            "properties": detail.get("properties"),
        }
