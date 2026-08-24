# Schema evolution

Upstream teams add columns without telling you. The question is whether that is a
Tuesday-morning outage or a log line.

## Policies

Set per dataset in `conf/datasets.yaml`:

| Policy | Allows | Rejects |
|---|---|---|
| `additive` (default) | New columns; safe widening (`int`→`bigint`, `date`→`timestamp`) | Dropped columns, renames, narrowing casts |
| `strict` | Nothing — the schema must match exactly | Any change |
| `none` | Everything, logs a warning | Nothing (exploration only) |

## Safe widening

```python
SAFE_WIDENING = {
    "byte":  {"short", "int", "bigint", "float", "double", "decimal"},
    "int":   {"bigint", "float", "double", "decimal"},
    "float": {"double"},
    "date":  {"timestamp"},
    ...
}
```

`int → bigint` is lossless and allowed. `bigint → int` is not: it would silently
truncate values above 2³¹, so it is rejected and requires an explicit migration.

## The registry

Every accepted schema is appended to `ops.schema_registry` with the diff that
produced it:

```
dataset  layer   version  policy    diff_json
orders   bronze  1        additive  {"added": {}, "removed": {}, "changed": {}}
orders   bronze  2        additive  {"added": {"promotion_code": "string",
                                               "fulfilment_center": "string",
                                               "gift_wrap_flag": "string"}, ...}
```

A breaking change is a rejected job with a precise message, not a mysterious NULL
column discovered three days later:

```
SchemaEvolutionError: [orders/bronze] non-additive change rejected:
  -1 removed (discount_pct). Dropped columns or narrowing casts require an
  explicit migration (see docs/schema_evolution.md).
```

## Verified

Landing an orders file with three new columns produced, with no manual step:

```json
{"msg": "schema.registered", "dataset": "orders", "layer": "bronze", "version": 2,
 "diff": "+3 added (fulfilment_center, gift_wrap_flag, promotion_code)", "policy": "additive"}
{"msg": "bronze.schema_evolved", "dataset": "orders",
 "diff": "+3 added (fulfilment_center, gift_wrap_flag, promotion_code)"}
```

The columns flowed through to Silver (`5,998` rows, `80` non-null
`promotion_code`) in the same run.

Reproduce it:

```bash
python scripts/generate_sample_data.py --out data/samples --days 7 --evolve
python scripts/run_local_pipeline.py --env local --clean
```

## Enabling mechanics

- `mergeSchema` on writes, and `autoMerge` scoped tightly around MERGE (enabled
  immediately before, disabled immediately after — a global `autoMerge` would let
  a typo'd column name silently create a new column)
- `delta.columnMapping.mode = name` on every table, which makes future renames and
  drops metadata-only operations instead of full rewrites
- Auto Loader uses `schemaEvolutionMode = addNewColumns` plus `rescuedDataColumn`,
  so unexpected fields are captured rather than discarded

## Handling a genuinely breaking change

1. Set the dataset to `schema_evolution: none` temporarily
2. Migrate the Silver table (`ALTER TABLE ... RENAME COLUMN`, cheap under column mapping)
3. Update `conf/expectations/<dataset>.yaml` for the new shape
4. Restore `additive` and let the registry record the new baseline
