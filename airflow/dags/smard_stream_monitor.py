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
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="smard_stream_monitor",
    default_args=default_args,
    description="Real-time stream pipeline every 15 minutes: SMARD → Bronze → Silver → Gold",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 6, 14),
    catchup=False,
    tags=["smard", "stream", "realtime"],
) as dag:

    poll_energy = BashOperator(
        task_id="poll_smard_energy",
        bash_command=ENV + " && " + VENV + " " + BASE + "/ingestion/streaming/smard_poller.py --once",
    )

    poll_weather = BashOperator(
        task_id="poll_weather",
        bash_command=ENV + " && " + VENV + " " + BASE + "/ingestion/streaming/weather_poller.py --once",
    )

    spark_bronze = BashOperator(
        task_id="spark_stream_to_bronze",
        bash_command=ENV + " && " + VENV + " " + BASE + "/spark/streaming/spark_stream_processor.py --once",
    )

    spark_silver = BashOperator(
        task_id="spark_bronze_to_silver",
        bash_command=ENV + " && " + VENV + " " + BASE + "/spark/streaming/spark_silver_processor.py --once",
    )

    # NEW: update Gold incrementally after every Silver write
    # This ensures Gold is never more than 15 minutes behind Silver
    dbt_gold = BashOperator(
        task_id="dbt_update_gold",
        bash_command=ENV + " && " + DBT_CMD + " run --select gold",
    )

    poll_energy >> poll_weather >> spark_bronze >> spark_silver >> dbt_gold
