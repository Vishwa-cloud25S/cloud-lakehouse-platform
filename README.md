# cloud-lakehouse-platform

**A production-grade Azure lakehouse: ADLS Gen2 → Bronze → Silver → Gold on Delta Lake, governed by Unity Catalog, served through Databricks SQL and Power BI.**

[![CI](https://github.com/Vishwa-cloud25S/cloud-lakehouse-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Vishwa-cloud25S/cloud-lakehouse-platform/actions/workflows/ci.yml)
[![Terraform](https://img.shields.io/badge/Terraform-validated-7B42BC?logo=terraform&logoColor=white)](infra/terraform)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)](pyproject.toml)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2-00ADD4)](https://delta.io)
[![Spark](https://img.shields.io/badge/PySpark-3.5.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

This is not a notebook demo. It is an installable Python package with a declarative
dataset contract, a quality engine, schema-evolution policies, incremental state,
Terraform for the whole Azure footprint, and CI that runs the entire medallion
pipeline on every push.

**Everything below has actually been executed** — the numbers in this README are
measured output, not aspirations.

---

## Architecture

![Architecture](docs/img/architecture.svg)

```
Source APIs / CSV  →  ADLS Gen2  →  Bronze  →  PySpark  →  Silver
                                                              ↓
        Power BI  ←  Databricks SQL  ←  Gold  ←  Delta Lake  ─┘
```

Every arrow is a Delta transaction: a failure mid-pipeline leaves the lakehouse in
its previous consistent state, never half-updated.

---

## Run it in two minutes

No Azure account, no Databricks workspace — the whole pipeline runs locally on
open-source Delta Lake.

```bash
git clone https://github.com/Vishwa-cloud25S/cloud-lakehouse-platform.git
cd cloud-lakehouse-platform

pip install -r requirements-dev.txt

make seed     # generate 14 days of sample data (with deliberate defects)
make demo     # run Bronze → Silver → Gold → maintenance
```

Requires Python 3.10+ and a JDK (11 or 17).

### Actual output

```
==============================================================================
  BRONZE - incremental ingest
==============================================================================
  products   read=   200  written=   200  status=success
  customers  read=   540  written=   540  status=success
  orders     read= 6,064  written= 6,064  status=success
==============================================================================
  SILVER - conform, quality gate, dedupe, SCD2
==============================================================================
  products   read=   200  written=   197  quarantined=   3  dq_pass=98.50%
  customers  read=   540  written=   497  quarantined=   3  dq_pass=99.40%
  orders     read= 6,064  written= 5,848  quarantined= 201  dq_pass=96.68%
==============================================================================
  GOLD - star schema + serving aggregates
==============================================================================
  dim_customer       rows=   497
  dim_product        rows=   197
  dim_date           rows= 4,018
  fact_orders        rows= 5,848
  agg_daily_sales    rows=   299
  agg_customer_360   rows=   497
==============================================================================
  RESULT
==============================================================================
  gold.fact_orders              5,848 rows
  gold.agg_customer_360           497 rows
  ops.quarantine                  204 rows
  total elapsed: 131.0s
```

Run `make demo` a second time and it reads **0 rows** — incremental processing and
idempotency, demonstrated rather than claimed.

---

## What this project demonstrates

| Capability | Implementation | Proof |
|---|---|---|
| **Medallion architecture** | Bronze/Silver/Gold with enforced contracts per layer | `src/lakehouse/jobs/` |
| **Incremental processing** | Delta watermark state, Auto Loader, partition-pruned MERGE | Second run reads 0 rows |
| **Schema evolution** | `additive`/`strict`/`none` policies + versioned registry | 3 new columns absorbed live |
| **Data quality** | 24 declarative rules, quarantine, single-pass evaluation | 207 rows quarantined, not dropped |
| **Unity Catalog governance** | 3-level namespace, per-layer grants, column masking, row filters, PII tags | `conf/governance.yaml`, `sql/unity_catalog/` |
| **SCD Type 2** | Atomic two-pass MERGE preserving history | Integration test asserts one current row |
| **Partitioning** | Advisor encoding the ~1 GB rule; monthly fact partitions | Unit-tested pure function |
| **Spark optimisation** | AQE, skew joins, optimizeWrite, deletion vectors, broadcast joins, Z-ORDER | `docs/spark_optimisation.md` |
| **Terraform** | ADLS, workspace, UC, jobs, warehouse, budget — 5 modules | `terraform validate` passes |
| **CI/CD** | 6 GitHub Actions workflows incl. full e2e pipeline run | `.github/workflows/` |
| **Monitoring** | JSON logs, Delta metrics, freshness/volume/quality alerts | `ops.pipeline_metrics` |
| **Cost engineering** | Spot + job clusters, auto-stop, lifecycle tiering, forecast budgets | `docs/cost.md` |

---

## Repository layout

```
cloud-lakehouse-platform/
├── src/lakehouse/
│   ├── config.py              declarative env + dataset contracts
│   ├── spark_session.py       tuned Spark/Delta session factory
│   ├── logging_utils.py       structured JSON logging
│   ├── io/                    readers, Delta writer, watermarks, schema registry
│   ├── quality/               expectations + single-pass runner
│   ├── governance/            catalog, masking, lineage
│   ├── transforms/            bronze / silver / gold logic
│   ├── optimize/              maintenance + partitioning advisor
│   ├── monitoring/            metrics + alerts
│   └── jobs/                  executable entry points
├── conf/                      env, dataset, governance, expectation YAML
├── infra/terraform/           5 modules, dev + prod environments
├── sql/                       Unity Catalog DDL, Gold DDL, BI views, ops dashboard
├── tests/                     43 unit + 8 integration tests
├── scripts/                   sample data generator, local runner, SLA checker
├── docs/                      7 deep-dive documents + architecture diagram
└── .github/workflows/         CI, CD, Terraform plan, scheduled quality
```

---

## Design decisions worth defending

**Bronze never casts and never filters.** A malformed price lands in Bronze
exactly as it arrived, then fails a *quality rule* in Silver where the failure is
recorded and the row quarantined. If Bronze rejected it at ingest, the row would
be lost and nobody could reconstruct what the source sent.

**Quarantine, don't drop.** Rejected rows land in `ops.quarantine` with the failing
rule names and the original payload. "Why is yesterday's revenue short?" is a
query, not an investigation.

**Two clock domains for watermarks.** Bronze filters on file modification time, so
its watermark is a wall-clock discovery boundary captured *before* the read.
Silver filters on `_ingest_timestamp`, a column Bronze wrote. Using the business
`updated_at` for Bronze looks natural and silently loses backdated records.

**Deterministic surrogate keys.** `xxhash64` of the business key, not
`monotonically_increasing_id()` — so the same business key yields the same key on
every rebuild and facts stay joinable after a dimension refresh.

**Point-in-time dimension joins.** `fact_orders` joins the `dim_customer` *version*
whose validity window contains the order timestamp, so revenue is attributed to
the segment the customer had at the time.

**Analysts have no Bronze access.** That boundary is the entire point of the
medallion pattern. If analysts can query Bronze, they will, and the quality layer
becomes decorative.

**Maintenance is best-effort and isolated.** OPTIMIZE/VACUUM/ANALYZE run in a
separate job so housekeeping can never delay a data SLA, and one table's failure
does not stop the rest.

---

## Three bugs found by actually running this

Building this surfaced real defects. Each is now fixed and regression-tested —
which is the honest argument for running your pipeline instead of only writing it.

1. **NULL-safe quality predicates.** In SQL, `length(NULL) > 1` is `NULL`, not
   `false`, so a naive `WHERE`/`WHERE NOT` split let NULL rows pass *both* filters.
   The split now uses coalesced flag columns.
   → `test_null_predicate_counts_as_failure`

2. **Unresolvable rules crashed clean batches.** `_corrupt_record` only exists when
   Spark's CSV parser hits a malformed row; a rule referencing it killed the job on
   good data. Rules are now probed against the real schema and reported as
   `skipped`. → `test_unresolvable_rule_is_skipped_not_fatal`

3. **Alerting crashed on its own payload.** An alert whose context contained
   `message` raised `KeyError: Attempt to overwrite 'message' in LogRecord` — the
   alert path failing exactly when something was already wrong. Fixed with
   `safe_extra()`.

---

## Tests

```bash
make test        # 43 unit tests
make test-all    # + 8 integration tests against a real local Delta lake
```

Integration tests cover MERGE idempotency, selective updates via `row_hash`,
additive schema evolution, rejection of breaking changes, watermark
monotonicity, SCD2 close-out, point-in-time fact joins and unknown-member handling.

---

## Deploying to Azure

```bash
export ARM_SUBSCRIPTION_ID=... ARM_TENANT_ID=... ARM_CLIENT_ID=... ARM_CLIENT_SECRET=...
export TF_VAR_databricks_account_id=...

make tf-plan  ENV=dev
make tf-apply ENV=dev
```

Terraform provisions ADLS Gen2 (HNS, lifecycle tiering), the Databricks workspace,
Unity Catalog (Access Connector → storage credential → external location → catalog
→ schemas → grants), the job cluster policy, the medallion and maintenance jobs, a
Photon SQL warehouse, Log Analytics, alerts and a budget.

Then run the pipeline:

```bash
lakehouse bronze      --env dev --dataset orders
lakehouse silver      --env dev --dataset orders
lakehouse gold        --env dev
lakehouse maintenance --env dev
```

---

## Adding a dataset

No Python changes. Add a block to `conf/datasets.yaml`:

```yaml
invoices:
  source_format: csv
  source_path: abfss://landing@st....dfs.core.windows.net/invoices
  primary_keys: [invoice_id]
  watermark_column: updated_at
  partition_by: [invoice_month]
  zorder_by: [customer_id]
  scd: type1
  write_mode: merge
  schema_evolution: additive
  quality_suite: invoices
```

…and a `conf/expectations/invoices.yaml` suite. The jobs pick it up automatically.

---

## Documentation

| Document | Contents |
|---|---|
| [Architecture](docs/architecture.md) | Layer contracts, control plane, verified behaviour |
| [Incremental processing](docs/incremental_processing.md) | Watermarks, clock domains, backfill, partition pruning |
| [Data quality](docs/data_quality.md) | Expectation model, quarantine, NULL-safety |
| [Schema evolution](docs/schema_evolution.md) | Policies, safe widening, the registry |
| [Spark optimisation](docs/spark_optimisation.md) | AQE, small files, Z-ORDER vs partitioning |
| [Governance](docs/governance.md) | Unity Catalog, masking, lineage, permissions |
| [Monitoring](docs/monitoring.md) | Structured logs, metrics, alerts, SLA checks |
| [Cost](docs/cost.md) | Compute, storage and query cost levers |

---

## Tech stack

Azure Data Lake Storage Gen2 · Azure Databricks · Delta Lake 3.2 · PySpark 3.5.1 ·
Unity Catalog · Databricks SQL · Power BI · Terraform 1.9 · GitHub Actions ·
Python 3.10+ · pytest · ruff · black · mypy

---

## Licence

MIT — see [LICENSE](LICENSE).
