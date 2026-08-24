locals {
  # Deterministic naming: <project><env> with a short random suffix for globally
  # unique names (storage accounts must be globally unique and alphanumeric).
  name_prefix = "${var.project}${var.environment}"
  suffix      = random_string.suffix.result

  storage_account_name = substr(lower(replace("st${local.name_prefix}${local.suffix}", "-", "")), 0, 24)

  common_tags = merge(
    {
      project     = var.project
      environment = var.environment
      managed_by  = "terraform"
      owner       = "data-engineering"
      cost_center = "analytics"
    },
    var.tags
  )

  catalog_name = "${var.project}_${var.environment}"

  # Medallion containers + a dedicated metastore root for UC managed tables.
  containers = ["lakehouse", "metastore", "landing"]
}

resource "random_string" "suffix" {
  length  = 5
  special = false
  upper   = false
  numeric = true
}
