variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "name_prefix" { type = string }
variable "workspace_id" { type = string }
variable "storage_account_id" { type = string }
variable "subscription_id" { type = string }
variable "alert_emails" {
  type    = list(string)
  default = []
}
variable "monthly_budget_amount" {
  type    = number
  default = 300
}
variable "tags" {
  type    = map(string)
  default = {}
}
