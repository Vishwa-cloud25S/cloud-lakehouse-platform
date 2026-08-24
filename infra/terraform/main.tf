resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name_prefix}"
  location = var.location
  tags     = local.common_tags
}

module "adls" {
  source = "./modules/adls"

  resource_group_name           = azurerm_resource_group.this.name
  location                      = azurerm_resource_group.this.location
  storage_account_name          = local.storage_account_name
  replication_type              = var.storage_replication
  enable_hierarchical_namespace = var.enable_hierarchical_namespace
  containers                    = local.containers
  bronze_cool_after_days        = var.lifecycle_bronze_cool_days
  bronze_archive_after_days     = var.lifecycle_bronze_archive_days
  tags                          = local.common_tags
}

module "databricks_workspace" {
  source = "./modules/databricks_workspace"

  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  workspace_name      = "dbw-${local.name_prefix}"
  sku                 = var.environment == "prod" ? "premium" : "standard"
  tags                = local.common_tags
}

module "unity_catalog" {
  source = "./modules/unity_catalog"

  providers = {
    databricks         = databricks
    databricks.account = databricks.account
  }

  environment          = var.environment
  catalog_name         = local.catalog_name
  storage_account_id   = module.adls.storage_account_id
  storage_account_name = module.adls.storage_account_name
  metastore_container  = "metastore"
  lakehouse_container  = "lakehouse"
  resource_group_name  = azurerm_resource_group.this.name
  location             = azurerm_resource_group.this.location
  workspace_id         = module.databricks_workspace.workspace_id
  tags                 = local.common_tags

  depends_on = [module.adls, module.databricks_workspace]
}

module "jobs" {
  source = "./modules/jobs"

  providers = { databricks = databricks }

  environment                     = var.environment
  catalog_name                    = local.catalog_name
  storage_account_name            = module.adls.storage_account_name
  max_workers                     = var.job_max_workers
  use_spot_instances              = var.use_spot_instances
  sql_warehouse_size              = var.sql_warehouse_size
  sql_warehouse_auto_stop_minutes = var.sql_warehouse_auto_stop_minutes
  alert_emails                    = var.alert_emails
  tags                            = local.common_tags

  depends_on = [module.unity_catalog]
}

module "monitoring" {
  source = "./modules/monitoring"

  resource_group_name   = azurerm_resource_group.this.name
  location              = azurerm_resource_group.this.location
  name_prefix           = local.name_prefix
  workspace_id          = module.databricks_workspace.workspace_id
  storage_account_id    = module.adls.storage_account_id
  alert_emails          = var.alert_emails
  monthly_budget_amount = var.monthly_budget_amount
  subscription_id       = data.azurerm_subscription.current.id
  tags                  = local.common_tags
}

data "azurerm_subscription" "current" {}
data "azurerm_client_config" "current" {}
