output "pipeline_job_id" { value = databricks_job.medallion.id }
output "maintenance_job_id" { value = databricks_job.maintenance.id }
output "sql_warehouse_id" { value = databricks_sql_endpoint.bi.id }
output "sql_warehouse_jdbc_url" { value = databricks_sql_endpoint.bi.jdbc_url }
output "cluster_policy_id" { value = databricks_cluster_policy.pipeline.id }
