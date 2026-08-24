"""Schema evolution with an explicit contract.

Three policies, chosen per dataset in `conf/datasets.yaml`:

* ``additive``  - new nullable columns are allowed and merged into the target.
                  Drops, renames and type narrowing are rejected.
* ``strict``    - the incoming schema must match the registered one exactly.
* ``none``      - no checking (escape hatch for throwaway exploration).

A JSON snapshot of every accepted schema version is persisted, so a breaking change
shows up as a diff in the run log *and* in the registry table rather than as a
mysterious downstream NULL column three days later.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from lakehouse.logging_utils import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

log = get_logger(__name__)

# Widening casts Delta/Spark can perform without data loss.
SAFE_WIDENING: dict[str, set[str]] = {
    "byte": {"short", "int", "bigint", "float", "double", "decimal"},
    "short": {"int", "bigint", "float", "double", "decimal"},
    "int": {"bigint", "float", "double", "decimal"},
    "bigint": {"double", "decimal"},
    "float": {"double"},
    "date": {"timestamp"},
}


class SchemaEvolutionError(RuntimeError):
    """Raised when an incoming schema breaks the dataset's evolution contract."""


@dataclass
class SchemaDiff:
    """The structural delta between a registered schema and an incoming one."""

    added: dict[str, str] = field(default_factory=dict)
    removed: dict[str, str] = field(default_factory=dict)
    changed: dict[str, tuple[str, str]] = field(default_factory=dict)

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.removed or self.changed)

    @property
    def is_additive(self) -> bool:
        """Additive = only new columns, plus type changes that widen safely."""
        if self.removed:
            return False
        return all(new in SAFE_WIDENING.get(old, set()) for old, new in self.changed.values())

    def as_dict(self) -> dict[str, Any]:
        return {
            "added": self.added,
            "removed": self.removed,
            "changed": {k: {"from": v[0], "to": v[1]} for k, v in self.changed.items()},
        }

    def summary(self) -> str:
        if self.is_empty:
            return "no schema change"
        bits = []
        if self.added:
            bits.append(f"+{len(self.added)} added ({', '.join(sorted(self.added))})")
        if self.removed:
            bits.append(f"-{len(self.removed)} removed ({', '.join(sorted(self.removed))})")
        if self.changed:
            bits.append(
                "~"
                + str(len(self.changed))
                + " retyped ("
                + ", ".join(f"{c}: {a}->{b}" for c, (a, b) in sorted(self.changed.items()))
                + ")"
            )
        return "; ".join(bits)


def _flatten(schema: StructType, prefix: str = "") -> dict[str, str]:
    """Flatten a (possibly nested) StructType to {dotted_name: simpleString}."""
    from pyspark.sql.types import StructType as ST

    out: dict[str, str] = {}
    for f in schema.fields:
        name = f"{prefix}{f.name}"
        if isinstance(f.dataType, ST):
            out.update(_flatten(f.dataType, prefix=f"{name}."))
        else:
            out[name] = f.dataType.simpleString()
    return out


def diff_schemas(current: StructType, incoming: StructType) -> SchemaDiff:
    """Compare two Spark schemas, ignoring column order."""
    cur, inc = _flatten(current), _flatten(incoming)
    diff = SchemaDiff()
    for col, dtype in inc.items():
        if col not in cur:
            diff.added[col] = dtype
        elif cur[col] != dtype:
            diff.changed[col] = (cur[col], dtype)
    for col, dtype in cur.items():
        if col not in inc:
            diff.removed[col] = dtype
    return diff


_REGISTRY_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    dataset     STRING NOT NULL,
    layer       STRING NOT NULL,
    version     INT,
    schema_json STRING,
    diff_json   STRING,
    policy      STRING,
    run_id      STRING,
    created_at  TIMESTAMP
) USING DELTA
COMMENT 'Append-only history of accepted schema versions per dataset/layer.'
"""


@dataclass
class SchemaRegistry:
    """Persisted schema history + policy enforcement."""

    spark: SparkSession
    table: str
    location: str | None = None

    def ensure(self) -> None:
        ddl = _REGISTRY_DDL.format(table=self.table)
        if self.location:
            ddl += f"\nLOCATION '{self.location}'"
        self.spark.sql(ddl)

    def latest(self, dataset: str, layer: str) -> tuple[int, StructType | None]:
        """Most recent registered (version, schema) or (0, None) if unseen."""
        from pyspark.sql.types import StructType

        self.ensure()
        rows = (
            self.spark.table(self.table)
            .where(f"dataset = '{dataset}' AND layer = '{layer}'")
            .orderBy("version", ascending=False)
            .limit(1)
            .collect()
        )
        if not rows:
            return 0, None
        return rows[0]["version"], StructType.fromJson(
            __import__("json").loads(rows[0]["schema_json"])
        )

    def register(
        self,
        dataset: str,
        layer: str,
        schema: StructType,
        diff: SchemaDiff,
        policy: str,
        run_id: str,
    ) -> int:
        """Append a new schema version and return its number."""
        import json
        from datetime import datetime, timezone

        version, _ = self.latest(dataset, layer)
        new_version = version + 1
        df = self.spark.createDataFrame(
            [
                (
                    dataset,
                    layer,
                    new_version,
                    json.dumps(schema.jsonValue()),
                    json.dumps(diff.as_dict()),
                    policy,
                    run_id,
                    datetime.now(timezone.utc),
                )
            ],
            schema="dataset string, layer string, version int, schema_json string, "
            "diff_json string, policy string, run_id string, created_at timestamp",
        )
        df.write.format("delta").mode("append").saveAsTable(self.table)
        log.info(
            "schema.registered",
            extra={
                "dataset": dataset,
                "layer": layer,
                "version": new_version,
                "diff": diff.summary(),
                "policy": policy,
            },
        )
        return new_version

    def enforce(
        self,
        df: DataFrame,
        dataset: str,
        layer: str,
        policy: str = "additive",
        run_id: str = "",
    ) -> SchemaDiff:
        """Validate `df`'s schema against the registry and record the outcome.

        Raises SchemaEvolutionError when the change violates the policy.
        """
        _, previous = self.latest(dataset, layer)
        if previous is None:
            diff = SchemaDiff(added=_flatten(df.schema))
            self.register(dataset, layer, df.schema, SchemaDiff(), policy, run_id)
            log.info(
                "schema.baseline",
                extra={"dataset": dataset, "layer": layer, "columns": len(diff.added)},
            )
            return SchemaDiff()

        diff = diff_schemas(previous, df.schema)
        if diff.is_empty:
            return diff

        if policy == "none":
            log.warning("schema.unchecked", extra={"dataset": dataset, "diff": diff.summary()})
        elif policy == "strict":
            raise SchemaEvolutionError(
                f"[{dataset}/{layer}] strict policy violated: {diff.summary()}"
            )
        elif policy == "additive":
            if not diff.is_additive:
                raise SchemaEvolutionError(
                    f"[{dataset}/{layer}] non-additive change rejected: {diff.summary()}. "
                    "Dropped columns or narrowing casts require an explicit migration "
                    "(see docs/schema_evolution.md)."
                )
        else:  # pragma: no cover - guarded by config validation
            raise ValueError(f"unknown schema_evolution policy: {policy}")

        self.register(dataset, layer, df.schema, diff, policy, run_id)
        return diff
