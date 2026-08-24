# Databricks compute + orchestration.
# Cost decisions live here: job clusters (not all-purpose), spot workers,
# autoscaling floors, Photon only where it pays, and an aggressive warehouse
# auto-stop.

terraform {
  required_providers {
    databricks = { source = "databricks/databricks", version = "~> 1.50" }
  }
}

data "databricks_spark_version" "lts" {
  long_term_support = true
  spark_version     = "3.5"
}

data "databricks_node_type" "smallest" {
  local_disk    = true
  min_cores     = 4
  min_memory_gb = 16
  category      = "General Purpose"
}

# A cluster policy stops anyone from spinning up a 64-node all-purpose cluster
# "just to test something" - the classic way a Databricks bill triples.
resource "databricks_cluster_policy" "pipeline" {
  name = "lakehouse-${var.environment}-pipeline"

  definition = jsonencode({
    "spark_version" : {
      "type" : "fixed",
      "value" : data.databricks_spark_version.lts.id
    },
    "autotermination_minutes" : {
      "type" : "fixed",
      "value" : 20
    },
    "num_workers" : {
      "type" : "range",
      "minValue" : 1,
      "maxValue" : var.max_workers
    },
    "azure_attributes.availability" : {
      "type" : "fixed",
      "value" : var.use_spot_instances ? "SPOT_WITH_FALLBACK_AZURE" : "ON_DEMAND_AZURE"
    },
    "azure_attributes.spot_bid_max_price" : {
      "type" : "fixed",
      "value" : -1 # -1 = pay up to the on-demand price; never pay a premium
    },
    "custom_tags.cost_center" : {
      "type" : "fixed",
      "value" : "analytics"
    },
    "custom_tags.environment" : {
      "type" : "fixed",
      "value" : var.environment
    }
  })
}

locals {
  job_cluster = {
    spark_version = data.databricks_spark_version.lts.id
    node_type_id  = data.databricks_node_type.smallest.id

    # Autoscaling with a floor of 1: small incremental batches should not pay for
    # idle workers, but the ceiling absorbs a backfill.
    autoscale = {
      min_workers = 1
      max_workers = var.max_workers
    }

    azure_attributes = {
      availability       = var.use_spot_instances ? "SPOT_WITH_FALLBACK_AZURE" : "ON_DEMAND_AZURE"
      first_on_demand    = 1 # keep the driver on-demand so a spot eviction cannot kill the run
      spot_bid_max_price = -1
    }

    spark_conf = {
      "spark.databricks.delta.optimizeWrite.enabled" = "true"
      "spark.databricks.delta.autoCompact.enabled"   = "true"
      "spark.sql.adaptive.enabled"                   = "true"
      "spark.sql.adaptive.skewJoin.enabled"          = "true"
      "spark.sql.shuffle.partitions"                 = "auto"
    }

    custom_tags = merge(var.tags, { pipeline = "medallion" })
  }
}

resource "databricks_job" "medallion" {
  name        = "lakehouse-${var.environment}-medallion"
  description = "Bronze -> Silver -> Gold with quality gates, run incrementally."

  # One shared job cluster for the whole DAG: starting a cluster costs ~3 minutes,
  # so reusing it across tasks is both faster and cheaper than a cluster per task.
  job_cluster {
    job_cluster_key = "medallion"

    new_cluster {
      spark_version      = local.job_cluster.spark_version
      node_type_id       = local.job_cluster.node_type_id
      policy_id          = databricks_cluster_policy.pipeline.id
      data_security_mode = "SINGLE_USER" # required for Unity Catalog write workloads

      autoscale {
        min_workers = local.job_cluster.autoscale.min_workers
        max_workers = local.job_cluster.autoscale.max_workers
      }

      azure_attributes {
        availability       = local.job_cluster.azure_attributes.availability
        first_on_demand    = local.job_cluster.azure_attributes.first_on_demand
        spot_bid_max_price = local.job_cluster.azure_attributes.spot_bid_max_price
      }

      spark_conf  = local.job_cluster.spark_conf
      custom_tags = local.job_cluster.custom_tags
    }
  }

  dynamic "task" {
    for_each = ["orders", "customers", "products"]

    content {
      task_key        = "bronze_${task.value}"
      job_cluster_key = "medallion"

      python_wheel_task {
        package_name = "lakehouse"
        entry_point  = "lakehouse"
        parameters   = ["bronze", "--env", var.environment, "--dataset", task.value]
      }

      library {
        whl = "dbfs:/FileStore/wheels/lakehouse-latest-py3-none-any.whl"
      }
    }
  }

  dynamic "task" {
    for_each = ["orders", "customers", "products"]

    content {
      task_key        = "silver_${task.value}"
      job_cluster_key = "medallion"

      depends_on {
        task_key = "bronze_${task.value}"
      }

      python_wheel_task {
        package_name = "lakehouse"
        entry_point  = "lakehouse"
        parameters   = ["silver", "--env", var.environment, "--dataset", task.value]
      }

      library {
        whl = "dbfs:/FileStore/wheels/lakehouse-latest-py3-none-any.whl"
      }
    }
  }

  task {
    task_key        = "gold"
    job_cluster_key = "medallion"

    # Gold fans in from all three Silver tables.
    depends_on { task_key = "silver_orders" }
    depends_on { task_key = "silver_customers" }
    depends_on { task_key = "silver_products" }

    python_wheel_task {
      package_name = "lakehouse"
      entry_point  = "lakehouse"
      parameters   = ["gold", "--env", var.environment]
    }

    library {
      whl = "dbfs:/FileStore/wheels/lakehouse-latest-py3-none-any.whl"
    }
  }

  schedule {
    quartz_cron_expression = "0 0 2 * * ?" # 02:00 UTC daily
    timezone_id            = "UTC"
    pause_status           = var.environment == "prod" ? "UNPAUSED" : "PAUSED"
  }

  # Retries absorb transient ADLS throttling; a hard failure still pages someone.
  max_concurrent_runs = 1

  email_notifications {
    on_failure                = var.alert_emails
    no_alert_for_skipped_runs = true
  }

  health {
    rules {
      metric = "RUN_DURATION_SECONDS"
      op     = "GREATER_THAN"
      value  = 3600
    }
  }

  tags = var.tags
}

# Maintenance runs separately so housekeeping can never delay a data SLA.
resource "databricks_job" "maintenance" {
  name        = "lakehouse-${var.environment}-maintenance"
  description = "Nightly OPTIMIZE / ZORDER / VACUUM / ANALYZE."

  job_cluster {
    job_cluster_key = "maintenance"

    new_cluster {
      spark_version      = local.job_cluster.spark_version
      node_type_id       = local.job_cluster.node_type_id
      policy_id          = databricks_cluster_policy.pipeline.id
      data_security_mode = "SINGLE_USER"

      autoscale {
        min_workers = 1
        max_workers = 2
      }

      azure_attributes {
        availability       = "SPOT_WITH_FALLBACK_AZURE"
        first_on_demand    = 1
        spot_bid_max_price = -1
      }

      custom_tags = merge(var.tags, { pipeline = "maintenance" })
    }
  }

  task {
    task_key        = "maintenance"
    job_cluster_key = "maintenance"

    python_wheel_task {
      package_name = "lakehouse"
      entry_point  = "lakehouse"
      parameters   = ["maintenance", "--env", var.environment]
    }

    library {
      whl = "dbfs:/FileStore/wheels/lakehouse-latest-py3-none-any.whl"
    }
  }

  schedule {
    quartz_cron_expression = "0 0 4 * * ?" # 04:00 UTC, after the medallion run
    timezone_id            = "UTC"
    pause_status           = var.environment == "prod" ? "UNPAUSED" : "PAUSED"
  }

  email_notifications {
    on_failure = var.alert_emails
  }

  tags = var.tags
}

# Serverless SQL warehouse for Databricks SQL + Power BI.
resource "databricks_sql_endpoint" "bi" {
  name             = "wh-${var.environment}-bi"
  cluster_size     = var.sql_warehouse_size
  max_num_clusters = var.environment == "prod" ? 3 : 1

  # The single most important cost setting in DBSQL. An idle warehouse left
  # running overnight can cost more than the entire pipeline.
  auto_stop_mins = var.sql_warehouse_auto_stop_minutes

  enable_photon             = true # vectorised engine; large win on aggregation scans
  enable_serverless_compute = true
  spot_instance_policy      = "COST_OPTIMIZED"
  warehouse_type            = "PRO"

  tags {
    dynamic "custom_tags" {
      for_each = var.tags

      content {
        key   = custom_tags.key
        value = custom_tags.value
      }
    }
  }
}

resource "databricks_sql_global_config" "this" {
  security_policy = "DATA_ACCESS_CONTROL"

  sql_config_params = {
    ANSI_MODE = "true"
  }
}
