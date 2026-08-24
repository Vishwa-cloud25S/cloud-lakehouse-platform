# Terraform

Provisions the whole platform: ADLS Gen2, the Databricks workspace, Unity Catalog
(credential, external location, catalog, schemas, grants), jobs, the SQL warehouse
and the monitoring/budget guardrails.

## Layout

```
infra/terraform
├── main.tf              module composition
├── variables.tf         inputs + validation
├── locals.tf            naming convention
├── versions.tf          provider pinning + remote state
├── modules/
│   ├── adls/                storage, containers, lifecycle tiering
│   ├── databricks_workspace/
│   ├── unity_catalog/       access connector, credential, catalog, grants
│   ├── jobs/                cluster policy, medallion job, maintenance job, warehouse
│   └── monitoring/          Log Analytics, alerts, action group, budget
└── envs/{dev,prod}/     tfvars + backend config
```

## Usage

```bash
export ARM_SUBSCRIPTION_ID=... ARM_TENANT_ID=... ARM_CLIENT_ID=... ARM_CLIENT_SECRET=...
export TF_VAR_databricks_account_id=...

cd infra/terraform
terraform init -backend-config=envs/dev/backend.hcl
terraform plan  -var-file=envs/dev/terraform.tfvars
terraform apply -var-file=envs/dev/terraform.tfvars
```

`make tf-plan ENV=dev` wraps the same commands.

## Why these choices

| Decision | Reason |
|---|---|
| Access Connector (managed identity) instead of a service-principal secret | Nothing to rotate, nothing to leak; UC gets storage access without credentials in Spark conf. |
| Separate `metastore` and `lakehouse` containers | UC managed tables and externally-located data have different lifecycle and access policies. |
| Cluster **policy** rather than trust | Caps worker count, forces auto-termination and mandates cost tags — the guardrail that keeps a test cluster from becoming a bill. |
| Job clusters, not all-purpose | Billed at the (much cheaper) Jobs DBU rate and terminated the moment the run ends. |
| `first_on_demand = 1` with spot workers | 60–80% cheaper compute, but a spot eviction can never kill the driver and lose the run. |
| `auto_stop_mins` on the warehouse | An idle SQL warehouse is the most common source of a surprise Databricks bill. |
| Lifecycle tiering on Bronze only | Bronze is write-once/read-rarely; Silver and Gold back interactive queries and must stay Hot. |
| Forecast-based budget alert | Fires *before* the money is spent, not after the invoice. |
