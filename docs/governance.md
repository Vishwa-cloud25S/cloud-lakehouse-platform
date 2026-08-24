# Governance

Unity Catalog is the control point: identity, permissions, lineage, masking and
discovery all attach to the three-level namespace `catalog.schema.table`.

## Namespace

```
lakehouse_dev                     ← catalog (one per environment)
├── bronze                        ← raw landing
├── silver                        ← validated entities
├── gold                          ← dimensional model
├── ops                           ← pipeline control plane
└── governance                    ← masking functions
```

One catalog per environment means dev and prod are separated by permissions, not
by naming convention — a query cannot accidentally cross the boundary.

## Permission model

| Group | Bronze | Silver | Gold | Ops |
|---|---|---|---|---|
| `data_platform_admins` | ALL | ALL | ALL | ALL |
| `data_engineers` | read/write | read/write | read/write | read/write |
| `data_analysts` | **none** | read | read | none |
| `bi_service_principal` | none | none | read | none |

Analysts having **no Bronze access** is the point of the medallion boundary:
unvalidated data must not reach a dashboard. If analysts can query Bronze, they
will, and the quality layer becomes decorative.

Defined declaratively in `conf/governance.yaml` and mirrored in Terraform
(`modules/unity_catalog`) for objects that must exist before any job runs.

## Storage access without secrets

Databricks reaches ADLS through an **Access Connector** — an Azure managed
identity granted `Storage Blob Data Contributor`:

```
Access Connector (managed identity)
  → databricks_storage_credential
    → databricks_external_location (abfss://lakehouse@...)
      → catalog / schemas / tables
```

No account keys, no service principal secret in Spark conf, nothing to rotate or
leak. `shared_access_key_enabled = false` on the storage account enforces it.

## Column masking

Masking functions live in the `governance` schema and are attached to columns:

```sql
CREATE OR REPLACE FUNCTION governance.mask_email(email STRING)
  RETURN CASE
    WHEN is_account_group_member('data_pii_readers') THEN email
    WHEN email IS NULL THEN NULL
    ELSE concat(left(email, 1), '****@', split_part(email, '@', 2))
  END;

ALTER TABLE silver.silver_customers ALTER COLUMN email SET MASK governance.mask_email;
```

Same table, same query, different results by group. Analysts get broad access
without ever seeing raw PII, and there is no second "masked copy" table to keep
in sync.

Row-level security works the same way — `dim_customer` carries a row filter on
`country`, so a regional analyst sees only their region.

## Tags

Tags make the catalog searchable and drive policy:

```sql
ALTER TABLE silver.silver_customers
  SET TAGS ('domain' = 'crm', 'contains_pii' = 'true', 'classification' = 'confidential');
ALTER TABLE silver.silver_customers ALTER COLUMN email
  SET TAGS ('pii' = 'true', 'pii_type' = 'email');
```

"Which tables contain PII?" becomes a query against `system.information_schema`
rather than an audit meeting — which is what makes a GDPR/DPDP request tractable.

## Lineage

Two complementary layers:

1. **Unity Catalog** captures table and column lineage automatically from query
   plans — `system.access.table_lineage` and `system.access.column_lineage`. This
   answers *what* depends on what, including through views.
2. **`ops.lineage`** records job-level edges with run metrics — *why* a table
   changed, which run did it, how many rows moved.

```sql
-- What breaks if I change this column?
SELECT * FROM system.access.column_lineage
WHERE source_table_full_name = 'lakehouse_prod.silver.silver_orders'
  AND source_column_name = 'net_amount';
```

## Graceful degradation

Every governance call is wrapped so an engine without UC support logs a warning
instead of failing:

```json
{"level":"WARNING","msg":"governance.skipped","what":"tag table silver.silver_customers",
 "reason":"ALTER TABLE SET TAGS is not supported"}
```

That is what lets the identical code path run in CI on OSS Delta and in
production on Unity Catalog.
