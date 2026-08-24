output "storage_account_id" { value = azurerm_storage_account.this.id }
output "storage_account_name" { value = azurerm_storage_account.this.name }
output "dfs_endpoint" { value = azurerm_storage_account.this.primary_dfs_endpoint }
output "principal_id" { value = azurerm_storage_account.this.identity[0].principal_id }
