variable "environment" { type = string }
variable "catalog_name" { type = string }
variable "storage_account_id" { type = string }
variable "storage_account_name" { type = string }
variable "metastore_container" {
  type    = string
  default = "metastore"
}
variable "lakehouse_container" {
  type    = string
  default = "lakehouse"
}
variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "workspace_id" { type = string }
variable "tags" {
  type    = map(string)
  default = {}
}
