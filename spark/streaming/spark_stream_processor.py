
import os, json, time, sys
from datetime import datetime, timezone, date
import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, IntegerType
from google.cloud import pubsub_v1, bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/home/jovyan/smard-energy-pipeline/config/service_account.json"
)
os.environ["JAVA_HOME"]            = "/opt/conda"
os.environ["SPARK_LOCAL_IP"]       = "127.0.0.1"
os.environ["GOOGLE_CLOUD_PROJECT"] = "data-management-2-498012"

PROJECT_ID   = "data-management-2-498012"
ENERGY_SUB   = "smard-energy-live-sub"
WEATHER_SUB  = "weather-live-sub"
BQ_DATASET   = "bronze"
POLL_SECONDS = 60

JAR_DIR  = "/home/jovyan/smard-energy-pipeline/spark/jars"
BQ_JAR   = f"{JAR_DIR}/spark-bigquery-with-dependencies_2.12-0.34.0.jar"
GCS_JAR  = f"{JAR_DIR}/gcs-connector-hadoop3-latest.jar"
ALL_JARS = f"{BQ_JAR},{GCS_JAR}"

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")

def create_spark():
    spark = SparkSession.builder \
        .appName("smard-stream-processor") \
        .master("local[*]") \
        .config("spark.driver.host",             "127.0.0.1") \
        .config("spark.driver.bindAddress",      "127.0.0.1") \
        .config("spark.ui.enabled",               "false") \
        .config("spark.driver.extraClassPath",    ALL_JARS) \
        .config("spark.executor.extraClassPath",  ALL_JARS) \
        .config("spark.sql.shuffle.partitions",   "4") \
        .config("spark.driver.memory",            "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    log("Spark session created")
    return spark

def pull_messages(subscription_id, max_messages=100):
    subscriber = pubsub_v1.SubscriberClient()
    sub_path   = subscriber.subscription_path(PROJECT_ID, subscription_id)
    response   = subscriber.pull(
        request={"subscription": sub_path, "max_messages": max_messages}
    )
    messages = []
    ack_ids  = []
    for msg in response.received_messages:
        data = json.loads(msg.message.data.decode("utf-8"))
        messages.append(data)
        ack_ids.append(msg.ack_id)
    if ack_ids:
        subscriber.acknowledge(
            request={"subscription": sub_path, "ack_ids": ack_ids}
        )
    log(f"Pulled {len(messages)} messages from {subscription_id}")
    return messages

def write_to_bigquery(spark_df, table_name):
    if spark_df is None or spark_df.count() == 0:
        log(f"No data for {table_name}")
        return 0
    safe_df = spark_df
    for field in spark_df.schema.fields:
        col_name = field.name
        if isinstance(field.dataType, DoubleType):
            safe_df = safe_df.withColumn(col_name, F.col(col_name).cast("double"))
        elif isinstance(field.dataType, LongType):
            safe_df = safe_df.withColumn(col_name, F.col(col_name).cast("long"))
        elif isinstance(field.dataType, IntegerType):
            safe_df = safe_df.withColumn(col_name, F.col(col_name).cast("integer"))
        else:
            safe_df = safe_df.withColumn(col_name, F.col(col_name).cast("string"))
    pdf        = safe_df.toPandas()
    bq_client  = bigquery.Client(project=PROJECT_ID)
    table_id   = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    job = bq_client.load_table_from_dataframe(pdf, table_id, job_config=job_config)
    job.result()
    log(f"Written {len(pdf)} rows -> {BQ_DATASET}.{table_name}")
    return len(pdf)

def log_audit(task_id, status, rows_extracted, rows_loaded, error=None):
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        row = [{
            "run_id":           str(uuid.uuid4()),
            "dag_id":           "smard_stream_pipeline",
            "task_id":          task_id,
            "started_at":       datetime.now(tz=timezone.utc).isoformat(),
            "completed_at":     datetime.now(tz=timezone.utc).isoformat(),
            "status":           status,
            "rows_extracted":   rows_extracted,
            "rows_loaded":      rows_loaded,
            "rows_rejected":    0,
            "source_table":     "pubsub",
            "target_table":     "bronze",
            "error_message":    error,
            "duration_seconds": 0.0,
            "run_date":         date.today().isoformat(),
            "environment":      "hpc",
        }]
        bq_client.insert_rows_json(
            f"{PROJECT_ID}.monitoring.pipeline_runs", row)
        log(f"Audit: {task_id} -> {status}")
    except Exception as e:
        log(f"Audit failed (non-critical): {e}")

def process_cycle(spark):
    total_energy = total_weather = 0
    try:
        energy_msgs = pull_messages(ENERGY_SUB, 100)
        if energy_msgs:
            now = datetime.now(tz=timezone.utc).isoformat()
            for m in energy_msgs:
                m["_batch_ingested_at"] = now
                m["ingestion_date"]     = now[:10]
            df = spark.createDataFrame(energy_msgs)
            df = df.withColumn("value_mw",    F.col("value_mw").cast("double")) \
                   .withColumn("timestamp_ms", F.col("timestamp_ms").cast("long")) \
                   .withColumn("filter_id",    F.col("filter_id").cast("integer"))
            total_energy = write_to_bigquery(df, "raw_energy_stream")
            log_audit("spark_energy_to_bronze", "success",
                      len(energy_msgs), total_energy)
    except Exception as e:
        log(f"Energy error: {e}")
        log_audit("spark_energy_to_bronze", "failed", 0, 0, str(e))

    try:
        weather_msgs = pull_messages(WEATHER_SUB, 20)
        if weather_msgs:
            now = datetime.now(tz=timezone.utc).isoformat()
            for m in weather_msgs:
                m["_batch_ingested_at"] = now
                m["ingestion_date"]     = now[:10]
            df = spark.createDataFrame(weather_msgs)
            numeric_cols = ["wind_speed_ms","wind_direction","wind_gusts_ms",
                           "solar_direct_wm2","solar_diffuse_wm2","temperature_c",
                           "cloud_cover_pct","precipitation_mm","latitude","longitude"]
            for col in numeric_cols:
                if col in df.columns:
                    df = df.withColumn(col, F.col(col).cast("double"))
            total_weather = write_to_bigquery(df, "raw_weather_stream")
            log_audit("spark_weather_to_bronze", "success",
                      len(weather_msgs), total_weather)
    except Exception as e:
        log(f"Weather error: {e}")
        log_audit("spark_weather_to_bronze", "failed", 0, 0, str(e))

    log(f"Cycle done -> Energy:{total_energy} Weather:{total_weather}")
    return total_energy, total_weather

def main():
    run_once = "--once" in sys.argv
    log("="*50)
    log("SMARD Spark Stream Processor")
    log(f"Mode: {'single' if run_once else 'continuous'}")
    log("="*50)
    spark = create_spark()
    if run_once:
        process_cycle(spark)
        log("Single run complete")
        spark.stop()
        return
    log(f"Running every {POLL_SECONDS}s — Ctrl+C to stop")
    while True:
        try:
            process_cycle(spark)
            time.sleep(POLL_SECONDS)
        except KeyboardInterrupt:
            log("Stopped")
            break
        except Exception as e:
            log(f"Error: {e} — retrying in 60s")
            time.sleep(60)
    spark.stop()

if __name__ == "__main__":
    main()
