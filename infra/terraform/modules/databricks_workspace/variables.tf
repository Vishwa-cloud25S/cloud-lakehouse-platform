variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "workspace_name" { type = string }
variable "sku" {
  type    = string
  default = "premium"
}
variable "tags" {
  type    = map(string)
  default = {}
}
