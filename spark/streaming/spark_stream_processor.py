
import os, json, time, sys, uuid
from datetime import datetime, timezone, date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType, IntegerType
from google.cloud import pubsub_v1, bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/jovyan/smard-energy-pipeline/config/service_account.json"
os.environ["JAVA_HOME"]            = "/opt/conda/lib/jvm"
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

RENEWABLE_SOURCES = [
    "wind_onshore", "wind_offshore",
    "solar", "biomass", "hydro",
    "pumped_storage", "other_renewables"
]

REGION_NAMES = {
    "DE":         "Germany",
    "50Hertz":    "North/East Germany",
    "Amprion":    "West Germany",
    "TenneT":     "South/Central Germany",
    "TransnetBW": "Baden-Wuerttemberg",
}

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

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

# ── PYSPARK CLEANING — ENERGY ─────────────────────────
def clean_energy(spark, messages):
    """
    Full PySpark preprocessing and cleaning for energy data.
    Professor requirement: PySpark does all cleaning.
    dbt does business transformations.
    """
    if not messages:
        return None, 0, 0

    now = datetime.now(tz=timezone.utc).isoformat()
    for m in messages:
        m["_batch_ingested_at"] = now
        m["ingestion_date"]     = now[:10]

    df = spark.createDataFrame(messages)
    raw_count = df.count()
    log(f"Raw energy records: {raw_count}")

    # ── Step 1: Type casting ──────────────────────────
    df = df         .withColumn("value_mw",
            F.col("value_mw").cast("double"))         .withColumn("timestamp_ms",
            F.col("timestamp_ms").cast("long"))         .withColumn("filter_id",
            F.col("filter_id").cast("integer"))

    # ── Step 2: Standardise timestamps ───────────────
    df = df         .withColumn("reading_ts",
            F.to_timestamp(F.col("timestamp_ms") / 1000))         .withColumn("reading_date",
            F.to_date(F.col("reading_ts")))         .withColumn("reading_hour",
            F.hour(F.col("reading_ts")))         .withColumn("reading_minute",
            F.minute(F.col("reading_ts")))

    # ── Step 3: Handle nulls ──────────────────────────
    df = df.dropna(subset=["energy_source", "value_mw", "timestamp_ms"])
    df = df.fillna({"region": "DE", "region_name": "Germany"})

    # ── Step 4: Range validation ──────────────────────
    # Remove physically impossible values
    df = df.filter(F.col("value_mw") >= 0)
    df = df.filter(F.col("value_mw") <= 100000)

    # ── Step 5: Remove duplicates ─────────────────────
    df = df.dropDuplicates(["timestamp_ms", "region", "energy_source"])

    # ── Step 6: Standardise region names ─────────────
    region_map = F.create_map(
        *[item for k, v in REGION_NAMES.items()
          for item in [F.lit(k), F.lit(v)]]
    )
    df = df.withColumn("region_full",
        F.coalesce(
            region_map[F.col("region")],
            F.col("region")
        ))

    # ── Step 7: Add is_renewable flag ────────────────
    df = df.withColumn("is_renewable",
        F.col("energy_source").isin(RENEWABLE_SOURCES))

    # ── Step 8: Add is_fossil flag ───────────────────
    df = df.withColumn("is_fossil",
        F.col("energy_source").isin([
            "lignite", "hard_coal",
            "natural_gas", "other_conventional"
        ]))

    # ── Step 9: Window functions ──────────────────────
    # Rolling 1-hour average (last 4 readings = 4 x 15min)
    window_rolling = Window         .partitionBy("energy_source", "region")         .orderBy("timestamp_ms")         .rowsBetween(-3, 0)

    df = df.withColumn("rolling_avg_1h_mw",
        F.round(F.avg("value_mw").over(window_rolling), 2))

    # ── Step 10: Lag function (previous reading) ──────
    window_lag = Window         .partitionBy("energy_source", "region")         .orderBy("timestamp_ms")

    df = df.withColumn("prev_value_mw",
        F.lag("value_mw", 1).over(window_lag))

    # ── Step 11: Change from previous reading ────────
    df = df.withColumn("change_mw",
        F.round(
            F.col("value_mw") - F.col("prev_value_mw"),
            2
        ))

    df = df.withColumn("change_pct",
        F.round(
            F.when(
                F.col("prev_value_mw") > 0,
                (F.col("value_mw") - F.col("prev_value_mw"))
                / F.col("prev_value_mw") * 100
            ).otherwise(None),
            2
        ))

    # ── Step 12: Anomaly detection ────────────────────
    # Flag readings that are unusually high
    df = df.withColumn("is_anomaly",
        F.when(
            (F.col("energy_source") == "solar") &
            (F.col("value_mw") > 60000), True
        ).when(
            (F.col("energy_source") == "wind_onshore") &
            (F.col("value_mw") > 50000), True
        ).when(
            F.col("value_mw") > 90000, True
        ).otherwise(False))

    # ── Step 13: Time of day classification ──────────
    df = df.withColumn("time_of_day",
        F.when(
            (F.col("reading_hour") >= 6) &
            (F.col("reading_hour") < 12), "morning"
        ).when(
            (F.col("reading_hour") >= 12) &
            (F.col("reading_hour") < 18), "afternoon"
        ).when(
            (F.col("reading_hour") >= 18) &
            (F.col("reading_hour") < 22), "evening"
        ).otherwise("night"))

    # ── Step 14: Cast all to safe types for BQ ────────
    for field in df.schema.fields:
        col_name = field.name
        if isinstance(field.dataType, DoubleType):
            df = df.withColumn(col_name, F.col(col_name).cast("double"))
        elif isinstance(field.dataType, LongType):
            df = df.withColumn(col_name, F.col(col_name).cast("long"))
        elif isinstance(field.dataType, IntegerType):
            df = df.withColumn(col_name, F.col(col_name).cast("integer"))
        else:
            df = df.withColumn(col_name, F.col(col_name).cast("string"))

    clean_count = df.count()
    rejected    = raw_count - clean_count
    log(f"Energy after cleaning: {clean_count} rows "
        f"({rejected} rejected)")

    return df, clean_count, rejected

# ── PYSPARK CLEANING — WEATHER ────────────────────────
def clean_weather(spark, messages):
    """
    Full PySpark preprocessing and cleaning for weather data.
    """
    if not messages:
        return None, 0, 0

    now = datetime.now(tz=timezone.utc).isoformat()
    for m in messages:
        m["_batch_ingested_at"] = now
        m["ingestion_date"]     = now[:10]

    df = spark.createDataFrame(messages)
    raw_count = df.count()
    log(f"Raw weather records: {raw_count}")

    # ── Step 1: Type casting ──────────────────────────
    numeric_cols = [
        "wind_speed_ms", "wind_direction", "wind_gusts_ms",
        "solar_direct_wm2", "solar_diffuse_wm2",
        "temperature_c", "cloud_cover_pct",
        "precipitation_mm", "latitude", "longitude",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast("double"))

    # ── Step 2: Handle nulls ──────────────────────────
    df = df.dropna(subset=["region", "temperature_c"])
    df = df.fillna({
        "wind_speed_ms":    0.0,
        "solar_direct_wm2": 0.0,
        "precipitation_mm": 0.0,
    })

    # ── Step 3: Range validation ──────────────────────
    df = df.filter(
        (F.col("temperature_c") >= -50) &
        (F.col("temperature_c") <= 60)
    )
    df = df.filter(
        (F.col("wind_speed_ms") >= 0) &
        (F.col("wind_speed_ms") <= 200)
    )
    df = df.filter(
        (F.col("solar_direct_wm2") >= 0) &
        (F.col("solar_direct_wm2") <= 1500)
    )

    # ── Step 4: Remove duplicates ─────────────────────
    df = df.dropDuplicates(["region", "_batch_ingested_at"])

    # ── Step 5: Add wind category ─────────────────────
    df = df.withColumn("wind_category",
        F.when(F.col("wind_speed_ms") < 5,   "calm")
         .when(F.col("wind_speed_ms") < 15,  "moderate")
         .when(F.col("wind_speed_ms") < 25,  "strong")
         .otherwise("storm"))

    # ── Step 6: Add solar category ────────────────────
    df = df.withColumn("solar_category",
        F.when(F.col("solar_direct_wm2") == 0,   "night")
         .when(F.col("solar_direct_wm2") < 100,  "low")
         .when(F.col("solar_direct_wm2") < 500,  "medium")
         .otherwise("high"))

    # ── Step 7: Add temperature category ─────────────
    df = df.withColumn("temp_category",
        F.when(F.col("temperature_c") < 0,   "freezing")
         .when(F.col("temperature_c") < 10,  "cold")
         .when(F.col("temperature_c") < 20,  "mild")
         .when(F.col("temperature_c") < 30,  "warm")
         .otherwise("hot"))

    # ── Step 8: Standardise region names ─────────────
    region_map = F.create_map(
        *[item for k, v in REGION_NAMES.items()
          for item in [F.lit(k), F.lit(v)]]
    )
    df = df.withColumn("region_full",
        F.coalesce(region_map[F.col("region")], F.col("region")))

    # ── Step 9: Cast all to safe types ────────────────
    for field in df.schema.fields:
        col_name = field.name
        if isinstance(field.dataType, DoubleType):
            df = df.withColumn(col_name, F.col(col_name).cast("double"))
        else:
            df = df.withColumn(col_name, F.col(col_name).cast("string"))

    clean_count = df.count()
    rejected    = raw_count - clean_count
    log(f"Weather after cleaning: {clean_count} rows "
        f"({rejected} rejected)")

    return df, clean_count, rejected

# ── WRITE TO BIGQUERY ─────────────────────────────────
def write_to_bigquery(spark_df, table_name):
    if spark_df is None or spark_df.count() == 0:
        log(f"No data for {table_name}")
        return 0
    pdf        = spark_df.toPandas()
    bq_client  = bigquery.Client(project=PROJECT_ID)
    table_id   = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    job = bq_client.load_table_from_dataframe(
        pdf, table_id, job_config=job_config)
    job.result()
    log(f"Written {len(pdf)} rows -> {BQ_DATASET}.{table_name}")
    return len(pdf)

# ── WRITE TO QUARANTINE ───────────────────────────────
def write_to_quarantine(rejected_count, source_table, reason):
    """Write rejected record counts to quarantine table"""
    if rejected_count == 0:
        return
    try:
        bq_client = bigquery.Client(project=PROJECT_ID)
        row = [{
            "record_id":       str(uuid.uuid4()),
            "source_table":    source_table,
            "failed_at":       datetime.now(tz=timezone.utc).isoformat(),
            "failure_reason":  reason,
            "raw_record":      None,
            "ge_suite_name":   "spark_cleaning",
            "pipeline_run_id": str(uuid.uuid4()),
        }]
        bq_client.insert_rows_json(
            f"{PROJECT_ID}.quarantine.failed_records", row)
        log(f"Quarantine: {rejected_count} records from {source_table}")
    except Exception as e:
        log(f"Quarantine write failed: {e}")

# ── AUDIT LOG ─────────────────────────────────────────
def log_audit(task_id, status,
              rows_extracted, rows_loaded,
              rows_rejected=0, error=None):
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
            "rows_rejected":    rows_rejected,
            "source_table":     "pubsub",
            "target_table":     f"bronze.{task_id}",
            "error_message":    error,
            "duration_seconds": 0.0,
            "run_date":         date.today().isoformat(),
            "environment":      "hpc",
        }]
        bq_client.insert_rows_json(
            f"{PROJECT_ID}.monitoring.pipeline_runs", row)
        log(f"Audit: {task_id} -> {status} "
            f"(extracted:{rows_extracted} "
            f"loaded:{rows_loaded} "
            f"rejected:{rows_rejected})")
    except Exception as e:
        log(f"Audit failed: {e}")

# ── ONE PROCESSING CYCLE ──────────────────────────────
def process_cycle(spark):
    total_energy = total_weather = 0

    # ── Energy ────────────────────────────────────────
    try:
        energy_msgs = pull_messages(ENERGY_SUB, 100)
        if energy_msgs:
            df_energy, clean_count, rejected = clean_energy(
                spark, energy_msgs)
            if df_energy:
                total_energy = write_to_bigquery(
                    df_energy, "raw_energy_stream")
                if rejected > 0:
                    write_to_quarantine(
                        rejected,
                        "raw_energy_stream",
                        "Failed PySpark cleaning validation"
                    )
                log_audit(
                    "spark_energy_to_bronze", "success",
                    len(energy_msgs), total_energy, rejected
                )
    except Exception as e:
        log(f"Energy error: {e}")
        log_audit("spark_energy_to_bronze", "failed",
                  0, 0, 0, str(e))

    # ── Weather ───────────────────────────────────────
    try:
        weather_msgs = pull_messages(WEATHER_SUB, 20)
        if weather_msgs:
            df_weather, clean_count, rejected = clean_weather(
                spark, weather_msgs)
            if df_weather:
                total_weather = write_to_bigquery(
                    df_weather, "raw_weather_stream")
                if rejected > 0:
                    write_to_quarantine(
                        rejected,
                        "raw_weather_stream",
                        "Failed PySpark cleaning validation"
                    )
                log_audit(
                    "spark_weather_to_bronze", "success",
                    len(weather_msgs), total_weather, rejected
                )
    except Exception as e:
        log(f"Weather error: {e}")
        log_audit("spark_weather_to_bronze", "failed",
                  0, 0, 0, str(e))

    log(f"Cycle complete -> "
        f"Energy:{total_energy} Weather:{total_weather}")
    return total_energy, total_weather

# ── MAIN ──────────────────────────────────────────────
def main():
    run_once = "--once" in sys.argv
    log("="*55)
    log("SMARD Spark Stream Processor")
    log("PySpark cleaning: nulls + dedup + validation")
    log("           + window functions + anomaly detection")
    log(f"Mode: {'single' if run_once else 'continuous'}")
    log("="*55)

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
