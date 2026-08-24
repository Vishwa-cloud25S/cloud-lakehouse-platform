environment = "prod"
location    = "centralindia"
project     = "lakehouse"

# Prod trades some cost for resilience and concurrency.
storage_replication             = "ZRS"
sql_warehouse_size              = "Small"
sql_warehouse_auto_stop_minutes = 15
job_max_workers                 = 8
use_spot_instances              = true # driver stays on-demand (first_on_demand = 1)
monthly_budget_amount           = 1200

lifecycle_bronze_cool_days    = 30
lifecycle_bronze_archive_days = 120

alert_emails = [
  "data-eng-oncall@example.com",
  "analytics-leads@example.com",
]

tags = {
  criticality = "tier-1"
  compliance  = "gdpr"
}
