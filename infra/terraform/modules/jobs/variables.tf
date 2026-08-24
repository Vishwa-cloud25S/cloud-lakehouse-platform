variable "environment" { type = string }
variable "catalog_name" { type = string }
variable "storage_account_name" { type = string }
variable "max_workers" {
  type    = number
  default = 4
}
variable "use_spot_instances" {
  type    = bool
  default = true
}
variable "sql_warehouse_size" {
  type    = string
  default = "2X-Small"
}
variable "sql_warehouse_auto_stop_minutes" {
  type    = number
  default = 10
}
variable "alert_emails" {
  type    = list(string)
  default = []
}
variable "tags" {
  type    = map(string)
  default = {}
}
