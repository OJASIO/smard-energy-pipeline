project_id      = "data-management-2-498012"
region          = "europe-west3"
environment     = "prod"
bq_location     = "EU"
gcs_bucket_name = "data-management-2-smard-raw"
alert_email     = "100004636@srh-heidelberg.org"

labels = {
  project    = "smard-energy-pipeline"
  managed_by = "terraform"
  team       = "data-engineering"
}

snowflake_account  = "qg17675.europe-west3.gcp"
snowflake_username = "OJASINDULKAR"
snowflake_password = "SmardPipeline2026!"
