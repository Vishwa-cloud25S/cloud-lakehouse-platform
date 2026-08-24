terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.116" }
  }
}

resource "azurerm_databricks_workspace" "this" {
  name                = var.workspace_name
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = var.sku

  # Premium is required for Unity Catalog table ACLs, cluster policies and
  # SQL warehouse access control - the governance features this project relies on.

  managed_resource_group_name = "${var.resource_group_name}-dbw-managed"

  tags = var.tags
}
