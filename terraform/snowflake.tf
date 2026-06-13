# ── WAREHOUSE ────────────────────────────────────────
resource "snowflake_warehouse" "compute_wh" {
  name           = "COMPUTE_WH"
  warehouse_size = "X-SMALL"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "SMARD pipeline compute warehouse"
}

# ── DATABASES ─────────────────────────────────────────
resource "snowflake_database" "smard_dev" {
  name    = "SMARD_DEV"
  comment = "Development database"
}

resource "snowflake_database" "smard_prod" {
  name    = "SMARD_PROD"
  comment = "Production database"
}

# ── SCHEMAS — DEV ─────────────────────────────────────
resource "snowflake_schema" "dev_raw" {
  database = snowflake_database.smard_dev.name
  name     = "RAW"
  comment  = "Raw data from BigQuery Bronze"
}

resource "snowflake_schema" "dev_silver" {
  database = snowflake_database.smard_dev.name
  name     = "SILVER"
  comment  = "PySpark cleaned data"
}

resource "snowflake_schema" "dev_gold" {
  database = snowflake_database.smard_dev.name
  name     = "GOLD"
  comment  = "dbt transformed data"
}

# ── SCHEMAS — PROD ────────────────────────────────────
resource "snowflake_schema" "prod_raw" {
  database = snowflake_database.smard_prod.name
  name     = "RAW"
  comment  = "Raw data from BigQuery Bronze"
}

resource "snowflake_schema" "prod_silver" {
  database = snowflake_database.smard_prod.name
  name     = "SILVER"
  comment  = "PySpark cleaned data"
}

resource "snowflake_schema" "prod_gold" {
  database = snowflake_database.smard_prod.name
  name     = "GOLD"
  comment  = "dbt transformed data"
}

# ── ROLE ──────────────────────────────────────────────
resource "snowflake_account_role" "transformer" {
  name    = "TRANSFORMER"
  comment = "Role for dbt + PySpark pipeline"
}

# ── GRANTS ────────────────────────────────────────────
resource "snowflake_grant_privileges_to_account_role" "transformer_warehouse" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.compute_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_dev_db" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.smard_dev.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_prod_db" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.smard_prod.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_dev_silver" {
  account_role_name = snowflake_account_role.transformer.name
  all_privileges    = true
  on_schema {
    schema_name = "\"SMARD_DEV\".\"SILVER\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_dev_gold" {
  account_role_name = snowflake_account_role.transformer.name
  all_privileges    = true
  on_schema {
    schema_name = "\"SMARD_DEV\".\"GOLD\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_prod_silver" {
  account_role_name = snowflake_account_role.transformer.name
  all_privileges    = true
  on_schema {
    schema_name = "\"SMARD_PROD\".\"SILVER\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_prod_gold" {
  account_role_name = snowflake_account_role.transformer.name
  all_privileges    = true
  on_schema {
    schema_name = "\"SMARD_PROD\".\"GOLD\""
  }
}

# ── GRANT ROLE TO USER ────────────────────────────────
resource "snowflake_grant_account_role" "transformer_to_user" {
  role_name = snowflake_account_role.transformer.name
  user_name = "OJASINDULKAR"
}
