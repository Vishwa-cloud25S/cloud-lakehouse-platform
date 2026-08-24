# Remote state. The storage account below must exist before `terraform init`
# (bootstrap it once manually or with scripts/bootstrap_backend.sh).
resource_group_name  = "rg-terraform-state"
storage_account_name = "sttfstatelakehouse"
container_name       = "tfstate"
key                  = "lakehouse/dev.terraform.tfstate"
use_azuread_auth     = true
