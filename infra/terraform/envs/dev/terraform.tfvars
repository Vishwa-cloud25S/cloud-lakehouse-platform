environment = "dev"
location    = "centralindia"
project     = "lakehouse"

# Dev keeps costs near zero: smallest warehouse, aggressive auto-stop, spot workers.
storage_replication             = "LRS"
sql_warehouse_size              = "2X-Small"
sql_warehouse_auto_stop_minutes = 5
job_max_workers                 = 2
use_spot_instances              = true
monthly_budget_amount           = 100

lifecycle_bronze_cool_days    = 14
lifecycle_bronze_archive_days = 45

alert_emails = ["data-eng-alerts@example.com"]

tags = {
  criticality   = "low"
  auto_shutdown = "enabled"
}
