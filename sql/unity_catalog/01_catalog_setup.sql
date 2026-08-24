-- Unity Catalog bootstrap.
-- Terraform creates the catalog, schemas and grants; this script is the
-- equivalent DDL for a manual/exploratory setup and documents the intent.
-- Every statement is idempotent.

-- :param env  dev | prod

CREATE CATALOG IF NOT EXISTS lakehouse_dev
  MANAGED LOCATION 'abfss://metastore@stlakehousedev.dfs.core.windows.net/'
  COMMENT 'Lakehouse catalog for the dev environment.';

USE CATALOG lakehouse_dev;

CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Raw append-only landing zone. Source fidelity preserved; no business logic.';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Cleansed, deduplicated, quality-enforced business entities.';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Dimensional model and serving aggregates for BI consumption.';

CREATE SCHEMA IF NOT EXISTS ops
  COMMENT 'Pipeline control plane: watermarks, schema registry, DQ results, metrics, lineage.';

CREATE SCHEMA IF NOT EXISTS governance
  COMMENT 'Masking functions and row-filter UDFs.';

-- ---------------------------------------------------------------- grants
GRANT USE CATALOG ON CATALOG lakehouse_dev TO `data_engineers`;
GRANT USE CATALOG ON CATALOG lakehouse_dev TO `data_analysts`;
GRANT ALL PRIVILEGES ON CATALOG lakehouse_dev TO `data_platform_admins`;

-- Bronze is engineering-only on purpose: unvalidated data must never reach a
-- dashboard. This boundary is the reason the medallion architecture exists.
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA bronze TO `data_engineers`;

GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA silver TO `data_engineers`;
GRANT USE SCHEMA, SELECT                       ON SCHEMA silver TO `data_analysts`;

GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA gold   TO `data_engineers`;
GRANT USE SCHEMA, SELECT                       ON SCHEMA gold   TO `data_analysts`;
GRANT USE SCHEMA, SELECT                       ON SCHEMA gold   TO `bi_service_principal`;

GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA ops    TO `data_engineers`;
