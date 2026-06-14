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
    description="Daily batch pipeline at 02:00 CET",
    schedule_interval="0 1 * * *",
    start_date=datetime(2026, 6, 14),
    catchup=False,
    tags=["smard", "batch", "daily"],
) as dag:

    ingest_ecb = BashOperator(
        task_id="ingest_ecb_rates",
        bash_command=ENV + " && " + VENV + " " + BASE + "/ingestion/batch/ecb_ingestion.py",
    )

    ingest_eurostat = BashOperator(
        task_id="ingest_eurostat",
        bash_command=ENV + " && " + VENV + " " + BASE + "/ingestion/batch/eurostat_ingestion.py",
    )

    dbt_silver = BashOperator(
        task_id="dbt_silver_models",
        bash_command=ENV + " && " + DBT_CMD + " run --select silver",
    )

    dbt_gold = BashOperator(
        task_id="dbt_gold_models",
        bash_command=ENV + " && " + DBT_CMD + " run --select gold",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=ENV + " && " + DBT_CMD + " test",
    )

    [ingest_ecb, ingest_eurostat] >> dbt_silver >> dbt_gold >> dbt_test
