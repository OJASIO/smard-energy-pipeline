from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

BASE = "/home/usr_100004636_srh_heidelberg_org/smard-energy-pipeline"
VENV = "/home/usr_100004636_srh_heidelberg_org/airflow_venv/bin/python3"
DBT  = "/home/usr_100004636_srh_heidelberg_org/airflow_venv/bin/dbt"
ENV  = "export GOOGLE_CLOUD_PROJECT=data-management-2-498012 && export PIPELINE_ENV=prod"
DBT_CMD = DBT + " --project-dir " + BASE + "/dbt/smard_pipeline --profiles-dir ~/.dbt --target prod"

default_args = {
    "owner": "smard_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="smard_daily_pipeline",
    default_args=default_args,
    description="Daily quality gate: validate Bronze freshness, run dbt tests, refresh Gold",
    schedule_interval="0 1 * * *",
    start_date=datetime(2026, 6, 14),
    catchup=False,
    tags=["smard", "batch", "daily"],
) as dag:

    # Ingest supplementary data (ECB rates, Eurostat)
    # These don't exist in the stream monitor so stay here
    ingest_ecb = BashOperator(
        task_id="ingest_ecb_rates",
        bash_command=ENV + " && " + VENV + " " + BASE + "/ingestion/batch/ecb_ingestion.py",
    )

    ingest_eurostat = BashOperator(
        task_id="ingest_eurostat",
        bash_command=ENV + " && " + VENV + " " + BASE + "/ingestion/batch/eurostat_ingestion.py",
    )

    # Validate Bronze data quality
    ge_validate = BashOperator(
        task_id="great_expectations_validate",
        bash_command=ENV + " && cd " + BASE + " && " + VENV + " great_expectations/validate_bronze.py",
    )

    # Check Silver freshness — fail fast if Silver is stale
    # Prevents dbt from building Gold on top of stale Silver data
    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=ENV + " && " + DBT_CMD + " source freshness",
    )

    # Rebuild Silver from all sources (incremental)
    dbt_silver = BashOperator(
        task_id="dbt_silver_models",
        bash_command=ENV + " && " + DBT_CMD + " run --select silver",
    )

    # Rebuild Gold from Silver (incremental)
    dbt_gold = BashOperator(
        task_id="dbt_gold_models",
        bash_command=ENV + " && " + DBT_CMD + " run --select gold",
    )

    # Run data tests to validate Gold output
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=ENV + " && " + DBT_CMD.replace("run", "test"),
    )

    # Task chain:
    # ingest ECB + Eurostat in parallel
    # → validate Bronze (GE)
    # → check Silver freshness (fail fast if stale)
    # → rebuild Silver → rebuild Gold → test Gold
    [ingest_ecb, ingest_eurostat] >> ge_validate >> dbt_freshness >> dbt_silver >> dbt_gold >> dbt_test
