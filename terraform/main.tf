# ── ENABLE GCP APIs ──────────────────────────────────
resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "dataproc.googleapis.com",
    "secretmanager.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "iam.googleapis.com",
  ])
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# ── SERVICE ACCOUNT ──────────────────────────────────
resource "google_service_account" "pipeline_sa" {
  project      = var.project_id
  account_id   = "smard-pipeline-sa"
  display_name = "SMARD Pipeline Service Account"
  depends_on   = [google_project_service.apis]
}

resource "google_project_iam_member" "pipeline_roles" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/dataproc.worker",
    "roles/monitoring.metricWriter",
    "roles/logging.logWriter",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ── GCS BUCKETS ───────────────────────────────────────
resource "google_storage_bucket" "raw_data" {
  project                     = var.project_id
  name                        = var.gcs_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = var.labels

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "dbt_artifacts" {
  project                     = var.project_id
  name                        = "${var.gcs_bucket_name}-dbt"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = var.labels
}

resource "google_storage_bucket" "ge_docs" {
  project                     = var.project_id
  name                        = "${var.gcs_bucket_name}-ge-docs"
  location                    = var.region
  uniform_bucket_level_access = false
  force_destroy               = true
  labels                      = var.labels
}

# ── GCS FOLDER STRUCTURE ──────────────────────────────
resource "google_storage_bucket_object" "folders" {
  for_each = toset([
    "smard/historical/energy/.keep",
    "smard/historical/prices/.keep",
    "smard/historical/crossborder/.keep",
    "smard/live/.keep",
    "weather/historical/.keep",
    "weather/live/.keep",
    "ecb/exchange_rates/.keep",
    "eurostat/energy_prices/.keep",
    "holidays/german/.keep",
    "snowflake_staging/.keep",
    "checkpoints/energy_stream/.keep",
    "checkpoints/weather_stream/.keep",
    "spark/jobs/.keep",
  ])
  bucket  = google_storage_bucket.raw_data.name
  name    = each.value
  content = "placeholder"
}

# ── BIGQUERY DATASETS ─────────────────────────────────
resource "google_bigquery_dataset" "bronze" {
  project       = var.project_id
  dataset_id    = "bronze"
  friendly_name = "Bronze — Raw Landing Zone"
  location      = var.bq_location
  labels        = var.labels
  depends_on    = [google_project_service.apis]
}

resource "google_bigquery_dataset" "quarantine" {
  project       = var.project_id
  dataset_id    = "quarantine"
  friendly_name = "Quarantine — Failed Records"
  location      = var.bq_location
  labels        = var.labels
  depends_on    = [google_project_service.apis]
}

resource "google_bigquery_dataset" "monitoring" {
  project       = var.project_id
  dataset_id    = "monitoring"
  friendly_name = "Monitoring — Pipeline Audit"
  location      = var.bq_location
  labels        = var.labels
  depends_on    = [google_project_service.apis]
}

# ── BIGQUERY TABLES ───────────────────────────────────
resource "google_bigquery_table" "quarantine_failed_records" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.quarantine.dataset_id
  table_id            = "failed_records"
  labels              = var.labels
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "failed_at"
  }

  schema = jsonencode([
    {name="record_id",       type="STRING",    mode="REQUIRED"},
    {name="source_table",    type="STRING",    mode="REQUIRED"},
    {name="failed_at",       type="TIMESTAMP", mode="REQUIRED"},
    {name="failure_reason",  type="STRING",    mode="NULLABLE"},
    {name="raw_record",      type="JSON",      mode="NULLABLE"},
    {name="ge_suite_name",   type="STRING",    mode="NULLABLE"},
    {name="pipeline_run_id", type="STRING",    mode="NULLABLE"},
  ])
}

resource "google_bigquery_table" "pipeline_runs" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.monitoring.dataset_id
  table_id            = "pipeline_runs"
  labels              = var.labels
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "run_date"
  }

  schema = jsonencode([
    {name="run_id",           type="STRING",    mode="REQUIRED"},
    {name="dag_id",           type="STRING",    mode="REQUIRED"},
    {name="task_id",          type="STRING",    mode="REQUIRED"},
    {name="started_at",       type="TIMESTAMP", mode="REQUIRED"},
    {name="completed_at",     type="TIMESTAMP", mode="NULLABLE"},
    {name="status",           type="STRING",    mode="REQUIRED"},
    {name="rows_extracted",   type="INT64",     mode="NULLABLE"},
    {name="rows_loaded",      type="INT64",     mode="NULLABLE"},
    {name="rows_rejected",    type="INT64",     mode="NULLABLE"},
    {name="source_table",     type="STRING",    mode="NULLABLE"},
    {name="target_table",     type="STRING",    mode="NULLABLE"},
    {name="error_message",    type="STRING",    mode="NULLABLE"},
    {name="duration_seconds", type="FLOAT64",   mode="NULLABLE"},
    {name="run_date",         type="DATE",      mode="REQUIRED"},
    {name="environment",      type="STRING",    mode="NULLABLE"},
  ])
}

# ── PUB/SUB ───────────────────────────────────────────
resource "google_pubsub_topic" "smard_energy_live" {
  project                    = var.project_id
  name                       = "smard-energy-live"
  labels                     = var.labels
  message_retention_duration = "86400s"
  depends_on                 = [google_project_service.apis]
}

resource "google_pubsub_topic" "smard_energy_dlq" {
  project                    = var.project_id
  name                       = "smard-energy-live-dlq"
  labels                     = var.labels
  message_retention_duration = "604800s"
  depends_on                 = [google_project_service.apis]
}

resource "google_pubsub_subscription" "smard_energy_sub" {
  project                    = var.project_id
  name                       = "smard-energy-live-sub"
  topic                      = google_pubsub_topic.smard_energy_live.name
  labels                     = var.labels
  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.smard_energy_dlq.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

resource "google_pubsub_topic" "weather_live" {
  project                    = var.project_id
  name                       = "weather-live"
  labels                     = var.labels
  message_retention_duration = "86400s"
  depends_on                 = [google_project_service.apis]
}

resource "google_pubsub_topic" "weather_dlq" {
  project                    = var.project_id
  name                       = "weather-live-dlq"
  labels                     = var.labels
  message_retention_duration = "604800s"
  depends_on                 = [google_project_service.apis]
}

resource "google_pubsub_subscription" "weather_sub" {
  project                    = var.project_id
  name                       = "weather-live-sub"
  topic                      = google_pubsub_topic.weather_live.name
  labels                     = var.labels
  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.weather_dlq.id
    max_delivery_attempts = 5
  }
}

# ── MONITORING ────────────────────────────────────────
resource "google_monitoring_notification_channel" "email" {
  project      = var.project_id
  display_name = "SMARD Pipeline Alerts"
  type         = "email"
  labels = {
    email_address = var.alert_email
  }
  depends_on = [google_project_service.apis]
}
