output "catalog_name" { value = databricks_catalog.this.name }
output "access_connector_id" { value = azurerm_databricks_access_connector.this.id }
output "storage_credential_name" { value = databricks_storage_credential.this.name }
output "external_location_url" { value = databricks_external_location.lakehouse.url }
output "schemas" { value = [for s in databricks_schema.layers : s.name] }
