
"""
spark_silver_processor.py
─────────────────────────
Step 2: PySpark full cleaning + preprocessing
Reads RAW data from BigQuery Bronze
Applies ALL cleaning and preprocessing
Writes CLEAN data to Snowflake Silver
This is where professor sees PySpark work
"""

import os, sys, uuid
from datetime import datetime, timezone, date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, LongType, IntegerType
from google.cloud import bigquery
import snowflake.connector
import pandas as pd

os.environ["JAVA_HOME"]            = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"] = "/usr/lib/jvm/java-11-openjdk-amd64/bin:" + os.environ.get("PATH", "")
os.environ["SPARK_LOCAL_IP"]       = "127.0.0.1"
os.environ["GOOGLE_CLOUD_PROJECT"] = "data-management-2-498012"

PROJECT_ID   = "data-management-2-498012"
BQ_DATASET   = "bronze"

# Snowflake connection config
SF_CONFIG = {
    "account":   "qg17675.europe-west3.gcp",
    "user":      "OJASINDULKAR",
    "password":  "SmardPipeline2026!",
    "role":      "TRANSFORMER",
    "warehouse": "COMPUTE_WH",
    "database":  os.environ.get("PIPELINE_ENV", "dev").upper() == "PROD" and "SMARD_PROD" or "SMARD_DEV",
    "schema":    "SILVER",
}

JAR_DIR  = "/home/usr_100004636_srh_heidelberg_org/smard-energy-pipeline/spark/jars"
BQ_JAR   = f"{JAR_DIR}/spark-bigquery-with-dependencies_2.12-0.34.0.jar"
GCS_JAR  = f"{JAR_DIR}/gcs-connector-hadoop3-latest.jar"
ALL_JARS = f"{BQ_JAR},{GCS_JAR}"

RENEWABLE_SOURCES = [
    "wind_onshore", "wind_offshore", "solar",
    "biomass", "hydro", "pumped_storage", "other_renewables"
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
        .appName("smard-silver-processor") \
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

def read_from_bronze(table_name, last_processed_ts=None):
    """Read RAW data from BigQuery Bronze."""
    bq_client = bigquery.Client(project=PROJECT_ID)

    if last_processed_ts:
        query = f"""
            SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.{table_name}`
            WHERE _raw_ingested_at > '{last_processed_ts}'
            ORDER BY _raw_ingested_at
        """
    else:
        query = f"""
            SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.{table_name}`
            ORDER BY _raw_ingested_at
            LIMIT 10000
        """

    df = bq_client.query(query).to_dataframe()
    log(f"Read {len(df)} rows from {BQ_DATASET}.{table_name}")
    return df

def write_to_snowflake(pdf, table_name):
    """Write cleaned pandas DataFrame to Snowflake Silver."""
    if pdf.empty:
        log(f"No data to write to Snowflake {table_name}")
        return 0

    conn = snowflake.connector.connect(**SF_CONFIG)

    try:
        from snowflake.connector.pandas_tools import write_pandas
        success, nchunks, nrows, _ = write_pandas(
            conn, pdf, table_name.upper(),
            auto_create_table=True,
            overwrite=False,
        )
        log(f"Written {nrows} rows -> Snowflake SILVER.{table_name}")
        return nrows
    finally:
        conn.close()

# ── PYSPARK CLEANING — ENERGY ─────────────────────────
def clean_energy(spark, pdf_raw):
    """
    Full PySpark preprocessing and cleaning for energy data.
    Professor requirement: ALL cleaning done here in PySpark.
    Input:  raw pandas DataFrame from BigQuery Bronze
    Output: clean pandas DataFrame for Snowflake Silver
    """
    if pdf_raw.empty:
        return pd.DataFrame(), 0, 0

    df = spark.createDataFrame(pdf_raw)
    raw_count = df.count()
    log(f"Energy raw records: {raw_count}")

    # ── Step 1: Type casting ──────────────────────────
    df = df \
        .withColumn("value_mw",
            F.col("value_mw").cast("double")) \
        .withColumn("timestamp_ms",
            F.col("timestamp_ms").cast("long")) \
        .withColumn("filter_id",
            F.col("filter_id").cast("integer"))

    # ── Step 2: Standardise timestamps ───────────────
    df = df \
        .withColumn("reading_ts",
            F.to_timestamp(F.col("timestamp_ms") / 1000)) \
        .withColumn("reading_date",
            F.to_date(F.col("reading_ts"))) \
        .withColumn("reading_hour",
            F.hour(F.col("reading_ts"))) \
        .withColumn("reading_minute",
            F.minute(F.col("reading_ts"))) \
        .withColumn("reading_ts_15min",
            F.date_trunc("minute",
                F.from_unixtime(
                    (F.unix_timestamp("reading_ts") / 900)
                    .cast("long") * 900)))

    # ── Step 3: Null handling ─────────────────────────
    df = df.dropna(
        subset=["energy_source", "value_mw", "timestamp_ms"])
    df = df.fillna({"region": "DE", "region_name": "Germany"})

    # ── Step 4: Range validation ──────────────────────
    df = df.filter(F.col("value_mw") >= 0)
    df = df.filter(F.col("value_mw") <= 100000)

    # ── Step 5: Deduplication ─────────────────────────
    df = df.dropDuplicates(
        ["timestamp_ms", "region", "energy_source"])

    # ── Step 6: Region name standardisation ──────────
    region_map = F.create_map(
        *[item for k, v in REGION_NAMES.items()
          for item in [F.lit(k), F.lit(v)]])
    df = df.withColumn("region_full",
        F.coalesce(region_map[F.col("region")], F.col("region")))

    # ── Step 7: is_renewable flag ─────────────────────
    df = df.withColumn("is_renewable",
        F.col("energy_source").isin(RENEWABLE_SOURCES))

    # ── Step 8: is_fossil flag ────────────────────────
    df = df.withColumn("is_fossil",
        F.col("energy_source").isin([
            "lignite", "hard_coal",
            "natural_gas", "other_conventional"]))

    # ── Step 9: Rolling 1h average (window function) ──
    window_rolling = Window \
        .partitionBy("energy_source", "region") \
        .orderBy("timestamp_ms") \
        .rowsBetween(-3, 0)
    df = df.withColumn("rolling_avg_1h_mw",
        F.round(F.avg("value_mw").over(window_rolling), 2))

    # ── Step 10: Lag function ─────────────────────────
    window_lag = Window \
        .partitionBy("energy_source", "region") \
        .orderBy("timestamp_ms")
    df = df.withColumn("prev_value_mw",
        F.lag("value_mw", 1).over(window_lag))

    # ── Step 11: Change from previous reading ─────────
    df = df.withColumn("change_mw",
        F.round(F.col("value_mw") - F.col("prev_value_mw"), 2))
    df = df.withColumn("change_pct",
        F.round(
            F.when(F.col("prev_value_mw") > 0,
                (F.col("value_mw") - F.col("prev_value_mw"))
                / F.col("prev_value_mw") * 100
            ).otherwise(None), 2))

    # ── Step 12: Anomaly detection ────────────────────
    df = df.withColumn("is_anomaly",
        F.when(
            (F.col("energy_source") == "solar") &
            (F.col("value_mw") > 60000), True
        ).when(
            (F.col("energy_source") == "wind_onshore") &
            (F.col("value_mw") > 50000), True
        ).when(F.col("value_mw") > 90000, True
        ).otherwise(False))

    # ── Step 13: Time of day ──────────────────────────
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

    # ── Step 14: Add surrogate key ────────────────────
    df = df.withColumn("reading_id",
        F.md5(F.concat_ws("|",
            F.col("timestamp_ms").cast("string"),
            F.col("region"),
            F.col("energy_source")
        )))

    # ── Step 15: Add metadata ─────────────────────────
    now = datetime.now(tz=timezone.utc).isoformat()
    df = df.withColumn("_silver_processed_at", F.lit(now))
    df = df.withColumn("data_source", F.lit("stream"))

    # Cast all to safe types
    for field in df.schema.fields:
        col_name = field.name
        if isinstance(field.dataType, DoubleType):
            df = df.withColumn(col_name,
                F.col(col_name).cast("double"))
        elif isinstance(field.dataType, LongType):
            df = df.withColumn(col_name,
                F.col(col_name).cast("long"))
        elif isinstance(field.dataType, IntegerType):
            df = df.withColumn(col_name,
                F.col(col_name).cast("integer"))
        else:
            df = df.withColumn(col_name,
                F.col(col_name).cast("string"))

    clean_count = df.count()
    rejected    = raw_count - clean_count
    log(f"Energy cleaned: {clean_count} rows ({rejected} rejected)")

    return df.toPandas(), clean_count, rejected

# ── PYSPARK CLEANING — WEATHER ────────────────────────
def clean_weather(spark, pdf_raw):
    """
    Full PySpark cleaning for weather data.
    """
    if pdf_raw.empty:
        return pd.DataFrame(), 0, 0

    df = spark.createDataFrame(pdf_raw)
    raw_count = df.count()

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

    # ── Step 2: Null handling ─────────────────────────
    df = df.dropna(subset=["region", "temperature_c"])
    df = df.fillna({
        "wind_speed_ms":    0.0,
        "solar_direct_wm2": 0.0,
        "precipitation_mm": 0.0,
    })

    # ── Step 3: Range validation ──────────────────────
    df = df.filter(
        (F.col("temperature_c") >= -50) &
        (F.col("temperature_c") <= 60))
    df = df.filter(
        (F.col("wind_speed_ms") >= 0) &
        (F.col("wind_speed_ms") <= 200))
    df = df.filter(
        (F.col("solar_direct_wm2") >= 0) &
        (F.col("solar_direct_wm2") <= 1500))

    # ── Step 4: Deduplication ─────────────────────────
    df = df.dropDuplicates(["region", "_raw_ingested_at"])

    # ── Step 5: Wind category ─────────────────────────
    df = df.withColumn("wind_category",
        F.when(F.col("wind_speed_ms") < 5,   "calm")
         .when(F.col("wind_speed_ms") < 15,  "moderate")
         .when(F.col("wind_speed_ms") < 25,  "strong")
         .otherwise("storm"))

    # ── Step 6: Solar category ────────────────────────
    df = df.withColumn("solar_category",
        F.when(F.col("solar_direct_wm2") == 0,  "night")
         .when(F.col("solar_direct_wm2") < 100, "low")
         .when(F.col("solar_direct_wm2") < 500, "medium")
         .otherwise("high"))

    # ── Step 7: Temperature category ─────────────────
    df = df.withColumn("temp_category",
        F.when(F.col("temperature_c") < 0,   "freezing")
         .when(F.col("temperature_c") < 10,  "cold")
         .when(F.col("temperature_c") < 20,  "mild")
         .when(F.col("temperature_c") < 30,  "warm")
         .otherwise("hot"))

    # ── Step 8: Region full name ──────────────────────
    region_map = F.create_map(
        *[item for k, v in REGION_NAMES.items()
          for item in [F.lit(k), F.lit(v)]])
    df = df.withColumn("region_full",
        F.coalesce(region_map[F.col("region")], F.col("region")))

    # ── Step 9: reading_ts_15min join key ─────────────
    df = df.withColumn("reading_ts",
        F.to_timestamp(F.col("reading_ts")))
    df = df.withColumn("reading_ts_15min",
        F.date_trunc("minute",
            F.from_unixtime(
                (F.unix_timestamp("reading_ts") / 900)
                .cast("long") * 900)))

    # ── Step 10: Surrogate key ────────────────────────
    df = df.withColumn("weather_id",
        F.md5(F.concat_ws("|",
            F.col("reading_ts_15min").cast("string"),
            F.col("region")
        )))

    # ── Step 11: Metadata ─────────────────────────────
    now = datetime.now(tz=timezone.utc).isoformat()
    df = df.withColumn("_silver_processed_at", F.lit(now))

    # Cast all to safe types
    for field in df.schema.fields:
        col_name = field.name
        if isinstance(field.dataType, DoubleType):
            df = df.withColumn(col_name, F.col(col_name).cast("double"))
        else:
            df = df.withColumn(col_name, F.col(col_name).cast("string"))

    clean_count = df.count()
    rejected    = raw_count - clean_count
    log(f"Weather cleaned: {clean_count} rows ({rejected} rejected)")

    return df.toPandas(), clean_count, rejected

# ── MAIN ──────────────────────────────────────────────
def main():
    run_once = "--once" in sys.argv
    log("="*55)
    log("SMARD Silver Processor")
    log("BigQuery Bronze (RAW) -> PySpark cleaning")
    log("-> Snowflake Silver (CLEAN)")
    log(f"Mode: {'single' if run_once else 'continuous'}")
    log("="*55)

    spark = create_spark()

    def process_once():
        # ── Energy ────────────────────────────────────
        try:
            log("Reading energy RAW from Bronze...")
            pdf_raw = read_from_bronze("raw_energy_stream")
            if not pdf_raw.empty:
                pdf_clean, clean_count, rejected = clean_energy(
                    spark, pdf_raw)
                if not pdf_clean.empty:
                    write_to_snowflake(
                        pdf_clean, "stg_energy_stream_clean")
                    log(f"Energy: {clean_count} clean, "
                        f"{rejected} rejected")
        except Exception as e:
            log(f"Energy error: {e}")

        # ── Weather ───────────────────────────────────
        try:
            log("Reading weather RAW from Bronze...")
            pdf_raw = read_from_bronze("raw_weather_stream")
            if not pdf_raw.empty:
                pdf_clean, clean_count, rejected = clean_weather(
                    spark, pdf_raw)
                if not pdf_clean.empty:
                    write_to_snowflake(
                        pdf_clean, "stg_weather_stream_clean")
                    log(f"Weather: {clean_count} clean, "
                        f"{rejected} rejected")
        except Exception as e:
            log(f"Weather error: {e}")

    if run_once:
        process_once()
        log("Single run complete")
        spark.stop()
        return

    import time
    log("Running every 60s — Ctrl+C to stop")
    while True:
        try:
            process_once()
            time.sleep(60)
        except KeyboardInterrupt:
            log("Stopped")
            break
        except Exception as e:
            log(f"Error: {e} — retrying in 60s")
            time.sleep(60)

    spark.stop()

if __name__ == "__main__":
    main()
