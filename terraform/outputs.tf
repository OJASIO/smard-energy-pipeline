output "gcs_raw_bucket" {
  value = google_storage_bucket.raw_data.name
}
output "gcs_dbt_bucket" {
  value = google_storage_bucket.dbt_artifacts.name
}
output "bq_bronze_dataset" {
  value = google_bigquery_dataset.bronze.dataset_id
}
output "bq_quarantine_dataset" {
  value = google_bigquery_dataset.quarantine.dataset_id
}
output "bq_monitoring_dataset" {
  value = google_bigquery_dataset.monitoring.dataset_id
}
output "pubsub_energy_topic" {
  value = google_pubsub_topic.smard_energy_live.name
}
output "pubsub_weather_topic" {
  value = google_pubsub_topic.weather_live.name
}
output "pipeline_sa_email" {
  value = google_service_account.pipeline_sa.email
}

output "snowflake_account" {
  value = var.snowflake_account
}

output "snowflake_dev_database" {
  value = snowflake_database.smard_dev.name
}

output "snowflake_prod_database" {
  value = snowflake_database.smard_prod.name
}

output "snowflake_warehouse" {
  value = snowflake_warehouse.compute_wh.name
}
