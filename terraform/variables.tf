variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "europe-west3"
}

variable "environment" {
  description = "Environment: dev, staging, prod"
  type        = string
  default     = "prod"
}

variable "gcs_bucket_name" {
  description = "GCS bucket name — must be globally unique"
  type        = string
}

variable "bq_location" {
  description = "BigQuery location"
  type        = string
  default     = "EU"
}

variable "alert_email" {
  description = "Email for pipeline alerts"
  type        = string
}

variable "labels" {
  description = "Labels for all resources"
  type        = map(string)
  default = {
    project    = "smard-energy-pipeline"
    managed_by = "terraform"
  }
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_username" {
  description = "Snowflake username"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}

variable "snowflake_user" {
  description = "Snowflake username"
  type        = string
  default     = "OJASINDULKAR"
}
