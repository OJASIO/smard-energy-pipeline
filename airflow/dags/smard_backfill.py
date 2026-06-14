from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

BASE = "/home/usr_100004636_srh_heidelberg_org/smard-energy-pipeline"
VENV = "/home/usr_100004636_srh_heidelberg_org/airflow_venv/bin/python3"
DBT  = "/home/usr_100004636_srh_heidelberg_org/airflow_venv/bin/dbt"
JAVA = "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 && export PATH=$JAVA_HOME/bin:$PATH"
ENV  = JAVA + " && export SPARK_LOCAL_IP=127.0.0.1 && export GOOGLE_CLOUD_PROJECT=data-management-2-498012 && export PIPELINE_ENV=prod"
DBT_CMD = DBT + " --project-dir " + BASE + "/dbt/smard_pipeline --profiles-dir ~/.dbt --target prod"

default_args = {
    "owner": "smard_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
}

with DAG(
    dag_id="smard_backfill",
    default_args=default_args,
    description="Manual backfill for historical data",
    schedule_interval=None,
    start_date=datetime(2026, 6, 14),
    catchup=False,
    tags=["smard", "backfill", "historical"],
) as dag:

    ingest_energy = BashOperator(
        task_id="ingest_historical_energy",
        bash_command=ENV + " && " + VENV + " " + BASE + "/ingestion/batch/smard_historical_ingestion.py",
    )

    ingest_weather = BashOperator(
        task_id="ingest_historical_weather",
        bash_command=ENV + " && " + VENV + " " + BASE + "/ingestion/batch/historical_weather_ingestion.py",
    )

    ingest_crossborder = BashOperator(
        task_id="ingest_cross_border",
        bash_command=ENV + " && " + VENV + " " + BASE + "/ingestion/batch/cross_border_ingestion.py",
    )

    spark_energy = BashOperator(
        task_id="spark_process_energy",
        bash_command=ENV + " && " + VENV + " " + BASE + "/spark/batch/spark_batch_processor.py",
    )

    spark_weather = BashOperator(
        task_id="spark_process_weather",
        bash_command=ENV + " && " + VENV + " " + BASE + "/spark/batch/spark_weather_batch.py",
    )

    spark_crossborder = BashOperator(
        task_id="spark_process_crossborder",
        bash_command=ENV + " && " + VENV + " " + BASE + "/spark/batch/spark_crossborder_batch.py",
    )

    dbt_refresh = BashOperator(
        task_id="dbt_full_refresh",
        bash_command=ENV + " && " + DBT_CMD + " run --full-refresh --select silver gold",
    )

    [ingest_energy, ingest_weather, ingest_crossborder] >> spark_energy
    spark_energy >> spark_weather >> spark_crossborder >> dbt_refresh
