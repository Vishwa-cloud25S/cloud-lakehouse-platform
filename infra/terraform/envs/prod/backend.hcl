resource_group_name  = "rg-terraform-state"
storage_account_name = "sttfstatelakehouse"
container_name       = "tfstate"
key                  = "lakehouse/prod.terraform.tfstate"
use_azuread_auth     = true
