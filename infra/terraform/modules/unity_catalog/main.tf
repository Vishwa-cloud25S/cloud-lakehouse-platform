# Unity Catalog: identity, storage credential, external location, catalog, schemas
# and grants. This is the governance backbone - every table lives in
# catalog.schema.table and inherits permissions from it.

terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.116" }
    databricks = {
      source                = "databricks/databricks"
      version               = "~> 1.50"
      configuration_aliases = [databricks, databricks.account]
    }
  }
}

# The Access Connector is a managed identity Databricks uses to reach ADLS.
# This is strictly better than a service principal secret: nothing to rotate,
# nothing to leak.
resource "azurerm_databricks_access_connector" "this" {
  name                = "dbac-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

resource "azurerm_role_assignment" "storage_contributor" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}

resource "databricks_storage_credential" "this" {
  name = "sc-${var.environment}"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.this.id
  }

  comment = "Managed identity credential for ${var.environment} lakehouse storage."

  depends_on = [azurerm_role_assignment.storage_contributor]
}

resource "databricks_external_location" "lakehouse" {
  name            = "el-${var.environment}-lakehouse"
  url             = "abfss://${var.lakehouse_container}@${var.storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.this.name
  comment         = "Root external location for the ${var.environment} medallion layers."

  depends_on = [databricks_storage_credential.this]
}

resource "databricks_catalog" "this" {
  name         = var.catalog_name
  storage_root = "abfss://${var.metastore_container}@${var.storage_account_name}.dfs.core.windows.net/"
  comment      = "Lakehouse catalog for ${var.environment}."

  properties = {
    purpose     = "medallion-lakehouse"
    environment = var.environment
  }

  depends_on = [databricks_external_location.lakehouse]
}

# Medallion schemas. `ops` holds the control plane (watermarks, DQ, metrics).
resource "databricks_schema" "layers" {
  for_each = {
    bronze = "Raw append-only landing zone. Source fidelity preserved; no business logic."
    silver = "Cleansed, deduplicated, quality-enforced business entities."
    gold   = "Dimensional model and serving aggregates for BI consumption."
    ops    = "Pipeline control plane: watermarks, schema registry, DQ results, metrics, lineage."
  }

  catalog_name = databricks_catalog.this.name
  name         = each.key
  comment      = each.value

  properties = {
    layer = each.key
  }
}

# ---------------------------------------------------------------- groups
resource "databricks_group" "roles" {
  for_each = toset([
    "data_platform_admins",
    "data_engineers",
    "data_analysts",
    "data_pii_readers",
  ])

  display_name = each.value
  lifecycle {
    # Groups are often managed centrally in the account console; do not fight it.
    ignore_changes = [display_name]
  }
}

# ---------------------------------------------------------------- grants
resource "databricks_grants" "catalog" {
  catalog = databricks_catalog.this.name

  grant {
    principal  = "data_platform_admins"
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = "data_engineers"
    privileges = ["USE_CATALOG", "USE_SCHEMA", "CREATE_SCHEMA", "CREATE_TABLE", "MODIFY", "SELECT"]
  }
  grant {
    principal  = "data_analysts"
    privileges = ["USE_CATALOG"]
  }

  depends_on = [databricks_group.roles]
}

# Analysts deliberately get no Bronze access: unvalidated data must never reach
# a dashboard. This is the whole point of the medallion boundary.
resource "databricks_grants" "bronze" {
  schema = "${databricks_catalog.this.name}.${databricks_schema.layers["bronze"].name}"

  grant {
    principal  = "data_engineers"
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
  grant {
    principal  = "data_platform_admins"
    privileges = ["ALL_PRIVILEGES"]
  }
}

resource "databricks_grants" "silver" {
  schema = "${databricks_catalog.this.name}.${databricks_schema.layers["silver"].name}"

  grant {
    principal  = "data_engineers"
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
  grant {
    principal  = "data_analysts"
    privileges = ["USE_SCHEMA", "SELECT"]
  }
  grant {
    principal  = "data_platform_admins"
    privileges = ["ALL_PRIVILEGES"]
  }
}

resource "databricks_grants" "gold" {
  schema = "${databricks_catalog.this.name}.${databricks_schema.layers["gold"].name}"

  grant {
    principal  = "data_engineers"
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
  grant {
    principal  = "data_analysts"
    privileges = ["USE_SCHEMA", "SELECT"]
  }
  grant {
    principal  = "data_platform_admins"
    privileges = ["ALL_PRIVILEGES"]
  }
}

resource "databricks_grants" "ops" {
  schema = "${databricks_catalog.this.name}.${databricks_schema.layers["ops"].name}"

  grant {
    principal  = "data_engineers"
    privileges = ["USE_SCHEMA", "SELECT", "MODIFY", "CREATE_TABLE"]
  }
  grant {
    principal  = "data_platform_admins"
    privileges = ["ALL_PRIVILEGES"]
  }
}
