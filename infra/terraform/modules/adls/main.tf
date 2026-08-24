# ADLS Gen2: the lakehouse storage layer.
# Hierarchical namespace is what turns a blob account into a real filesystem with
# atomic directory rename - which is what makes Delta's transaction log safe.

terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.116" }
  }
}

resource "azurerm_storage_account" "this" {
  name                = var.storage_account_name
  resource_group_name = var.resource_group_name
  location            = var.location

  account_tier             = "Standard"
  account_kind             = "StorageV2"
  account_replication_type = var.replication_type
  is_hns_enabled           = var.enable_hierarchical_namespace

  # Security posture
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false # force Entra ID / managed identity auth

  blob_properties {
    # Soft delete is the cheap insurance policy against an accidental VACUUM or
    # a bad overwrite; 7 days matches the Delta retention default.
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
    versioning_enabled  = false # Delta already versions; blob versioning doubles cost
    change_feed_enabled = false
  }

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

resource "azurerm_storage_container" "this" {
  for_each = toset(var.containers)

  name                  = each.value
  storage_account_name  = azurerm_storage_account.this.name
  container_access_type = "private"
}

# Cost control: Bronze is write-once/read-rarely after a few weeks, so tiering it
# down is the single largest storage saving available. Silver/Gold stay Hot because
# they back interactive BI queries.
resource "azurerm_storage_management_policy" "lifecycle" {
  storage_account_id = azurerm_storage_account.this.id

  rule {
    name    = "bronze-tiering"
    enabled = true

    filters {
      prefix_match = ["lakehouse/bronze"]
      blob_types   = ["blockBlob"]
    }

    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = var.bronze_cool_after_days
        tier_to_archive_after_days_since_modification_greater_than = var.bronze_archive_after_days
      }
    }
  }

  rule {
    name    = "cleanup-checkpoints-and-temp"
    enabled = true

    filters {
      prefix_match = ["lakehouse/_checkpoints/_tmp", "landing/_tmp"]
      blob_types   = ["blockBlob"]
    }

    actions {
      base_blob {
        delete_after_days_since_modification_greater_than = 7
      }
    }
  }

  # Delta's own VACUUM removes unreferenced data files; this catches orphaned
  # multipart uploads that VACUUM never sees.
  rule {
    name    = "abort-stale-uploads"
    enabled = true

    filters {
      prefix_match = ["lakehouse"]
      blob_types   = ["blockBlob"]
    }

    actions {
      base_blob {
        delete_after_days_since_last_access_time_greater_than = 365
      }
      version {
        delete_after_days_since_creation = 30
      }
    }
  }
}
