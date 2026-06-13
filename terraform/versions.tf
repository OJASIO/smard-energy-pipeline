terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.89"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "snowflake" {
  account       = "qg17675.europe-west3.gcp"
  username      = "OJASINDULKAR"
  password      = var.snowflake_password
  role          = "ACCOUNTADMIN"
  authenticator = "snowflake"
}
