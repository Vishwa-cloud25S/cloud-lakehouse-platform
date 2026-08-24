variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "storage_account_name" { type = string }
variable "replication_type" {
  type    = string
  default = "LRS"
}
variable "enable_hierarchical_namespace" {
  type    = bool
  default = true
}
variable "containers" {
  type    = list(string)
  default = ["lakehouse"]
}
variable "bronze_cool_after_days" {
  type    = number
  default = 30
}
variable "bronze_archive_after_days" {
  type    = number
  default = 90
}
variable "tags" {
  type    = map(string)
  default = {}
}
