output "resource_group" {
  description = "Resource group holding every environment resource."
  value       = azurerm_resource_group.this.name
}

output "storage_account" {
  value = module.adls.storage_account_name
}

output "adls_abfss_root" {
  description = "Root path used by conf/<env>.yaml."
  value       = "abfss://lakehouse@${module.adls.storage_account_name}.dfs.core.windows.net"
}

output "databricks_workspace_url" {
  value = module.databricks_workspace.workspace_url
}

output "catalog_name" {
  value = module.unity_catalog.catalog_name
}

output "sql_warehouse_id" {
  description = "Warehouse id for Power BI / DBSQL connections."
  value       = module.jobs.sql_warehouse_id
}

output "sql_warehouse_jdbc" {
  value = module.jobs.sql_warehouse_jdbc_url
}

output "pipeline_job_id" {
  value = module.jobs.pipeline_job_id
}
