
import os, sys
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from google.cloud import bigquery
import snowflake.connector
import pandas as pd

os.environ["JAVA_HOME"]      = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"]           = "/usr/lib/jvm/java-11-openjdk-amd64/bin:" + os.environ.get("PATH", "")
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["GOOGLE_CLOUD_PROJECT"] = "data-management-2-498012"

PROJECT_ID = "data-management-2-498012"
BQ_DATASET = "bronze"

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
BQ_JAR   = JAR_DIR + "/spark-bigquery-with-dependencies_2.12-0.34.0.jar"
GCS_JAR  = JAR_DIR + "/gcs-connector-hadoop3-latest.jar"
ALL_JARS = BQ_JAR + "," + GCS_JAR

REGION_NAMES = {
    "DE":         "Germany",
    "50Hertz":    "North/East Germany",
    "Amprion":    "West Germany",
    "TenneT":     "South/Central Germany",
    "TransnetBW": "Baden-Wuerttemberg",
}

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[" + ts + "] " + msg)

def create_spark():
    spark = SparkSession.builder \
        .appName("smard-weather-batch") \
        .master("local[*]") \
        .config("spark.driver.host",            "127.0.0.1") \
        .config("spark.driver.bindAddress",     "127.0.0.1") \
        .config("spark.ui.enabled",              "false") \
        .config("spark.driver.extraClassPath",   ALL_JARS) \
        .config("spark.executor.extraClassPath", ALL_JARS) \
        .config("spark.sql.shuffle.partitions",  "4") \
        .config("spark.driver.memory",           "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    log("Spark session created")
    return spark

def read_weather_from_bronze(chunk_size=50000, offset=0):
    bq_client = bigquery.Client(project=PROJECT_ID)
    query = (
        "SELECT * FROM `" + PROJECT_ID
        + ".bronze.raw_weather_historical` "
        + "ORDER BY observation_time, region "
        + "LIMIT " + str(chunk_size)
        + " OFFSET " + str(offset)
    )
    rows = list(bq_client.query(query).result())
    if not rows:
        return pd.DataFrame()
    pdf = pd.DataFrame([dict(row) for row in rows])
    log("Read " + str(len(pdf)) + " weather rows (offset " + str(offset) + ")")
    return pdf

def clean_weather(spark, pdf_raw):
    if pdf_raw.empty:
        return pd.DataFrame(), 0, 0

    df = spark.createDataFrame(pdf_raw)
    raw_count = df.count()

    numeric_cols = [
        "wind_speed_ms", "wind_direction", "wind_gusts_ms",
        "solar_direct_wm2", "solar_diffuse_wm2",
        "temperature_c", "cloud_cover_pct",
        "precipitation_mm", "latitude", "longitude",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast("double"))

    df = df.dropna(subset=["region", "temperature_c"])
    df = df.fillna({"wind_speed_ms": 0.0,
                    "solar_direct_wm2": 0.0,
                    "precipitation_mm": 0.0})

    df = df.filter((F.col("temperature_c") >= -50) &
                   (F.col("temperature_c") <= 60))
    df = df.filter((F.col("wind_speed_ms") >= 0) &
                   (F.col("wind_speed_ms") <= 200))
    df = df.filter((F.col("solar_direct_wm2") >= 0) &
                   (F.col("solar_direct_wm2") <= 1500))

    df = df.dropDuplicates(["region", "observation_time"])

    df = df.withColumn("wind_category",
        F.when(F.col("wind_speed_ms") < 5,   "calm")
         .when(F.col("wind_speed_ms") < 15,  "moderate")
         .when(F.col("wind_speed_ms") < 25,  "strong")
         .otherwise("storm"))

    df = df.withColumn("solar_category",
        F.when(F.col("solar_direct_wm2") == 0,  "night")
         .when(F.col("solar_direct_wm2") < 100, "low")
         .when(F.col("solar_direct_wm2") < 500, "medium")
         .otherwise("high"))

    df = df.withColumn("temp_category",
        F.when(F.col("temperature_c") < 0,   "freezing")
         .when(F.col("temperature_c") < 10,  "cold")
         .when(F.col("temperature_c") < 20,  "mild")
         .when(F.col("temperature_c") < 30,  "warm")
         .otherwise("hot"))

    region_map = F.create_map(
        *[item for k, v in REGION_NAMES.items()
          for item in [F.lit(k), F.lit(v)]])
    df = df.withColumn("region_full",
        F.coalesce(region_map[F.col("region")], F.col("region")))

    df = df.withColumn("reading_ts",
        F.to_timestamp(F.col("reading_ts")))
    df = df.withColumn("reading_ts_15min",
        F.date_trunc("minute",
            F.from_unixtime(
                (F.unix_timestamp("reading_ts") / 900)
                .cast("long") * 900)))

    df = df.withColumn("weather_id",
        F.md5(F.concat_ws("|",
            F.col("reading_ts_15min").cast("string"),
            F.col("region"))))

    now = datetime.now(tz=timezone.utc).isoformat()
    df = df.withColumn("_silver_processed_at", F.lit(now))

    for field in df.schema.fields:
        col_name = field.name
        if isinstance(field.dataType, DoubleType):
            df = df.withColumn(col_name,
                F.col(col_name).cast("double"))
        else:
            df = df.withColumn(col_name,
                F.col(col_name).cast("string"))

    clean_count = df.count()
    rejected    = raw_count - clean_count
    log("Cleaned: " + str(clean_count)
        + " rows (" + str(rejected) + " rejected)")
    return df.toPandas(), clean_count, rejected

def write_to_snowflake(pdf, table_name):
    if pdf.empty:
        return 0
    conn = snowflake.connector.connect(**SF_CONFIG)
    try:
        from snowflake.connector.pandas_tools import write_pandas
        success, nchunks, nrows, _ = write_pandas(
            conn, pdf, table_name.upper(),
            auto_create_table=True,
            overwrite=False,
        )
        log("Written " + str(nrows) + " rows -> Snowflake SILVER." + table_name)
        return nrows
    finally:
        conn.close()

def main():
    log("=" * 55)
    log("Weather Batch Processor")
    log("BigQuery Bronze -> PySpark -> Snowflake Silver")
    log("=" * 55)

    spark        = create_spark()
    total_rows   = 0
    chunk_size   = 50000
    offset       = 0

    while True:
        pdf_raw = read_weather_from_bronze(
            chunk_size=chunk_size, offset=offset)

        if pdf_raw.empty:
            log("No more weather data")
            break

        pdf_clean, clean_count, rejected = clean_weather(
            spark, pdf_raw)

        if not pdf_clean.empty:
            rows = write_to_snowflake(
                pdf_clean, "stg_weather_historical_clean")
            total_rows += rows

        offset += chunk_size
        log("Progress: " + str(offset)
            + " processed, " + str(total_rows) + " written")

        if len(pdf_raw) < chunk_size:
            log("Last chunk processed")
            break

    log("Weather batch complete: " + str(total_rows) + " rows")
    spark.stop()

if __name__ == "__main__":
    main()
