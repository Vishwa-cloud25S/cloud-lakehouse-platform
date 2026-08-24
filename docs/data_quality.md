# Data quality

Quality rules are configuration, not code. An analyst who spots a bad pattern
opens a PR against a YAML file; nobody edits Spark.

## The model

An expectation is a named SQL predicate plus an action:

| Action | Behaviour | Use for |
|---|---|---|
| `warn` | Count the failures, let rows through | Soft signals — a malformed email should not block revenue reporting |
| `quarantine` | Route failing rows aside, continue | Recoverable row-level problems |
| `fail` | Abort the job | Anything that would corrupt Gold — a NULL primary key breaks MERGE idempotency |

`tolerance` allows a fraction of failures before the action fires, so a rule that
is 99% right does not page someone at 3am.

```yaml
quantity_positive:
  expr: "quantity > 0"
  action: quarantine
  description: Zero/negative quantity indicates a cancelled line sent in error.

unit_price_sane:
  expr: "unit_price < 1000000"
  action: warn
  tolerance: 0.01
  description: Catches unit-of-currency mistakes (paise vs rupees).
```

## Quarantine, don't drop

Failing rows land in `ops.quarantine` with the rules that rejected them, the run
id and the original payload as JSON:

```sql
SELECT _dataset, explode(_dq_failed_rules) AS rule, count(*) AS rows_rejected
FROM ops.quarantine
WHERE _quarantined_at >= current_timestamp() - INTERVAL 7 DAYS
GROUP BY ALL ORDER BY rows_rejected DESC;
```

"Why is yesterday's revenue short?" becomes a query instead of an investigation.
Once the upstream bug is fixed, quarantined rows can be replayed.

## Single-pass evaluation

All rules become boolean columns in one projection, then a single `agg` collects
every failure count. Twenty rules cost **one scan**, not twenty.

## Two correctness details that matter

**NULL-safe by construction.** In SQL, `length(NULL) > 1` is `NULL`, not `false` —
so a naive `WHERE <predicate>` split lets NULL rows slip through *both* sides.
Each rule is wrapped in `coalesce(<expr>, false)` and the valid/quarantine split
uses those flag columns, so a NULL predicate counts as a failure. There is a
regression test for exactly this (`test_null_predicate_counts_as_failure`).

**Unresolvable rules are skipped, not fatal.** `_corrupt_record` only exists when
Spark's CSV parser actually hits a malformed row. A rule referencing it must not
crash a clean batch. Rules are probed against the real schema first; unresolvable
ones are reported with `action = "skipped"` so the gap is visible in the dashboard
rather than silently ignored. (This was a genuine bug caught while building — the
first end-to-end run died on it.)

## Shipped suites

| Suite | Rules | Blocking | Notable |
|---|---|---|---|
| `orders` | 11 | `order_id_not_null` | ID format, status enum, ISO currency, date sanity |
| `customers` | 7 | `customer_id_not_null`, `updated_at_present` | Email format, ISO-3166 country, no future signup |
| `products` | 6 | `product_id_not_null` | Non-negative price, cost-vs-price sanity |

Measured on the sample data: orders 96.68% pass (201 quarantined of 6,064),
customers 99.40%, products 98.50%. The sample generator injects those defects
deliberately so the quality path is exercised on every run.

## Alerting

`AlertManager` covers the three failure modes that actually matter:

- **freshness** — the table stopped updating
- **volume** — row count deviates >50% from the trailing median (catches a
  truncated source file, which a freshness check alone would miss)
- **quality** — pass rate fell below the contract
