variable "environment" {
  description = "Environment name; suffixes every resource."
  type        = string
  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "environment must be one of: dev, test, prod."
  }
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "centralindia"
}

variable "project" {
  description = "Short project slug used in resource names."
  type        = string
  default     = "lakehouse"
}

variable "databricks_account_id" {
  description = "Databricks account id (for Unity Catalog account-level resources)."
  type        = string
  sensitive   = true
}

variable "storage_replication" {
  description = "ADLS replication: LRS is fine for dev, ZRS/GRS for prod."
  type        = string
  default     = "LRS"
}

variable "enable_hierarchical_namespace" {
  description = "HNS is what makes a storage account ADLS Gen2. Required."
  type        = bool
  default     = true
}

variable "lifecycle_bronze_cool_days" {
  description = "Days before Bronze blobs move to the Cool tier."
  type        = number
  default     = 30
}

variable "lifecycle_bronze_archive_days" {
  description = "Days before Bronze blobs move to Archive."
  type        = number
  default     = 90
}

variable "sql_warehouse_size" {
  description = "Databricks SQL warehouse t-shirt size."
  type        = string
  default     = "2X-Small"
}

variable "sql_warehouse_auto_stop_minutes" {
  description = "Idle minutes before the warehouse stops. The single biggest DBSQL cost lever."
  type        = number
  default     = 10
}

variable "job_max_workers" {
  description = "Autoscale ceiling for the pipeline job cluster."
  type        = number
  default     = 4
}

variable "use_spot_instances" {
  description = "Use spot/low-priority workers (60-80% cheaper, interruptible)."
  type        = bool
  default     = true
}

variable "alert_emails" {
  description = "Recipients for job failure and budget alerts."
  type        = list(string)
  default     = []
}

variable "monthly_budget_amount" {
  description = "Azure budget threshold in USD."
  type        = number
  default     = 300
}

variable "tags" {
  description = "Tags applied to every resource; drives cost allocation."
  type        = map(string)
  default     = {}
}
