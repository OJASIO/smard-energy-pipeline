
import os
import sys
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from google.cloud import bigquery
import snowflake.connector
import pandas as pd

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/home/jovyan/smard-energy-pipeline/config/service_account.json"
)
os.environ["JAVA_HOME"]      = "/opt/conda/lib/jvm"
os.environ["PATH"]           = "/opt/conda/lib/jvm/bin:" + os.environ.get("PATH", "")
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["GOOGLE_CLOUD_PROJECT"] = "data-management-2-498012"

PROJECT_ID = "data-management-2-498012"
BQ_DATASET = "bronze"
BQ_TABLE   = "raw_cross_border_flows"

SF_CONFIG = {
    "account":   "qg17675.europe-west3.gcp",
    "user":      "OJASINDULKAR",
    "password":  "SmardPipeline2026!",
    "role":      "TRANSFORMER",
    "warehouse": "COMPUTE_WH",
    "database":  "SMARD_DEV",
    "schema":    "SILVER",
}

JAR_DIR  = "/home/jovyan/smard-energy-pipeline/spark/jars"
BQ_JAR   = JAR_DIR + "/spark-bigquery-with-dependencies_2.12-0.34.0.jar"
GCS_JAR  = JAR_DIR + "/gcs-connector-hadoop3-latest.jar"
ALL_JARS = BQ_JAR + "," + GCS_JAR

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[" + ts + "] " + msg)

def create_spark():
    spark = SparkSession.builder \
        .appName("smard-crossborder-batch") \
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

def read_from_bronze(chunk_size=50000, offset=0):
    bq_client = bigquery.Client(project=PROJECT_ID)
    query = (
        "SELECT * FROM " + "`" + PROJECT_ID + "." + BQ_DATASET
        + "." + BQ_TABLE + "`"
        + " ORDER BY timestamp_ms, flow_name"
        + " LIMIT " + str(chunk_size)
        + " OFFSET " + str(offset)
    )
    rows = list(bq_client.query(query).result())
    if not rows:
        return pd.DataFrame()
    pdf = pd.DataFrame([dict(row) for row in rows])
    log("Read " + str(len(pdf)) + " rows (offset " + str(offset) + ")")
    return pdf

def clean_cross_border(spark, pdf_raw):
    if pdf_raw.empty:
        return pd.DataFrame(), 0, 0

    df        = spark.createDataFrame(pdf_raw)
    raw_count = df.count()

    # Type casting
    df = df.withColumn("value_mw",
        F.col("value_mw").cast("double"))
    df = df.withColumn("timestamp_ms",
        F.col("timestamp_ms").cast("long"))
    df = df.withColumn("reading_hour",
        F.col("reading_hour").cast("integer"))

    # Drop nulls
    df = df.dropna(subset=["flow_name", "value_mw", "timestamp_ms"])

    # Timestamps
    df = df.withColumn("reading_ts",
        F.to_timestamp(F.col("timestamp_ms") / 1000))
    df = df.withColumn("reading_date",
        F.to_date(F.col("reading_ts")))
    df = df.withColumn("reading_ts_15min",
        F.date_trunc("minute",
            F.from_unixtime(
                (F.unix_timestamp("reading_ts") / 900)
                .cast("long") * 900)))

    # Deduplication
    df = df.dropDuplicates(["timestamp_ms", "flow_name"])

    # Flow type classification
    df = df.withColumn("flow_type",
        F.when(F.col("flow_name").contains("commercial"),
               "commercial_exchange")
         .when(F.col("flow_name").contains("physical"),
               "physical_flow")
         .otherwise("other"))

    # Is total flag
    df = df.withColumn("is_total",
        F.col("flow_name").contains("total"))

    # Surrogate key
    df = df.withColumn("flow_id",
        F.md5(F.concat_ws("|",
            F.col("timestamp_ms").cast("string"),
            F.col("flow_name"))))

    now = datetime.now(tz=timezone.utc).isoformat()
    df  = df.withColumn("_silver_processed_at", F.lit(now))

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
        log("Written " + str(nrows)
            + " rows -> Snowflake SILVER." + table_name)
        return nrows
    finally:
        conn.close()

def main():
    log("=" * 55)
    log("Cross-Border Flow Batch Processor")
    log("BigQuery Bronze -> PySpark -> Snowflake Silver")
    log("=" * 55)

    spark      = create_spark()
    total_rows = 0
    chunk_size = 50000
    offset     = 0

    while True:
        pdf_raw = read_from_bronze(
            chunk_size=chunk_size, offset=offset)

        if pdf_raw.empty:
            log("No more data")
            break

        pdf_clean, clean_count, rejected = clean_cross_border(
            spark, pdf_raw)

        if not pdf_clean.empty:
            rows = write_to_snowflake(
                pdf_clean, "stg_cross_border_clean")
            total_rows += rows

        offset += chunk_size
        log("Progress: " + str(offset)
            + " processed, " + str(total_rows) + " written")

        if len(pdf_raw) < chunk_size:
            log("Last chunk processed")
            break

    log("Cross-border batch complete: " + str(total_rows) + " rows")
    spark.stop()

if __name__ == "__main__":
    main()
