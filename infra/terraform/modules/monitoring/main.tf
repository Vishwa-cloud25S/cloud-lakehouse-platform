# Observability + cost guardrails.

terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.116" }
  }
}

resource "azurerm_log_analytics_workspace" "this" {
  name                = "law-${var.name_prefix}"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "PerGB2018"

  # 30 days is the free-ish tier sweet spot; long-term audit data belongs in the
  # lakehouse itself (ops.pipeline_metrics), not in Log Analytics at $2.30/GB.
  retention_in_days = 30

  tags = var.tags
}

resource "azurerm_monitor_diagnostic_setting" "storage" {
  name                       = "diag-storage"
  target_resource_id         = "${var.storage_account_id}/blobServices/default"
  log_analytics_workspace_id = azurerm_log_analytics_workspace.this.id

  enabled_log {
    category = "StorageRead"
  }
  enabled_log {
    category = "StorageWrite"
  }

  metric {
    category = "Transaction"
    enabled  = true
  }
}

resource "azurerm_monitor_action_group" "alerts" {
  name                = "ag-${var.name_prefix}"
  resource_group_name = var.resource_group_name
  short_name          = substr(var.name_prefix, 0, 12)

  dynamic "email_receiver" {
    for_each = var.alert_emails

    content {
      name                    = "email-${email_receiver.key}"
      email_address           = email_receiver.value
      use_common_alert_schema = true
    }
  }

  tags = var.tags
}

# Storage capacity alert - catches a runaway ingest or a VACUUM that never runs.
resource "azurerm_monitor_metric_alert" "storage_capacity" {
  name                = "alert-${var.name_prefix}-storage-capacity"
  resource_group_name = var.resource_group_name
  scopes              = [var.storage_account_id]
  description         = "ADLS capacity exceeded the expected envelope."
  severity            = 2
  frequency           = "PT1H"
  window_size         = "PT1H"

  criteria {
    metric_namespace = "Microsoft.Storage/storageAccounts"
    metric_name      = "UsedCapacity"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = 5497558138880 # 5 TiB
  }

  action {
    action_group_id = azurerm_monitor_action_group.alerts.id
  }

  tags = var.tags
}

# Budget: the backstop that turns a surprise bill into an email on day 3.
resource "azurerm_consumption_budget_resource_group" "this" {
  name              = "budget-${var.name_prefix}"
  resource_group_id = "/subscriptions/${split("/", var.subscription_id)[2]}/resourceGroups/${var.resource_group_name}"

  amount     = var.monthly_budget_amount
  time_grain = "Monthly"

  time_period {
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", timestamp())
  }

  # Forecast-based alerting fires before the money is spent, not after.
  notification {
    enabled        = true
    threshold      = 80
    operator       = "GreaterThan"
    threshold_type = "Actual"
    contact_emails = var.alert_emails
  }

  notification {
    enabled        = true
    threshold      = 100
    operator       = "GreaterThan"
    threshold_type = "Forecasted"
    contact_emails = var.alert_emails
  }

  lifecycle {
    ignore_changes = [time_period[0].start_date]
  }
}
