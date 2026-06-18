"""
spark_cross_border_processor.py
Reads RAW cross-border flow data from BigQuery Bronze
Applies PySpark cleaning (matches stg_cross_border_clean schema)
Writes CLEAN data to Snowflake Silver
Run after cross_border_ingestion.py
Usage: python3 spark_cross_border_processor.py [--year YYYY]
       PIPELINE_ENV=prod|dev controls target database
"""
import os, sys, hashlib
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
    "database":  "SMARD_PROD" if os.environ.get("PIPELINE_ENV", "dev").upper() == "PROD" else "SMARD_DEV",
    "schema":    "SILVER",
}

JAR_DIR  = "/home/usr_100004636_srh_heidelberg_org/smard-energy-pipeline/spark/jars"
BQ_JAR   = f"{JAR_DIR}/spark-bigquery-with-dependencies_2.12-0.34.0.jar"
GCS_JAR  = f"{JAR_DIR}/gcs-connector-hadoop3-latest.jar"
ALL_JARS = f"{BQ_JAR},{GCS_JAR}"

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def create_spark():
    spark = SparkSession.builder \
        .appName("SMARD_CrossBorder_Processor") \
        .config("spark.jars", ALL_JARS) \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    log("Spark session created")
    return spark

def read_from_bronze(table_name, chunk_size=50000, offset=0, where_clause=""):
    bq_client = bigquery.Client(project=PROJECT_ID)
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    query = f"""
        SELECT * FROM `{PROJECT_ID}.{BQ_DATASET}.{table_name}`
        {where_sql}
        ORDER BY timestamp_ms
        LIMIT {chunk_size} OFFSET {offset}
    """
    rows = list(bq_client.query(query).result())
    if not rows:
        return pd.DataFrame()
    pdf = pd.DataFrame([dict(row) for row in rows])
    log(f"Read {len(pdf)} rows from {table_name} (offset {offset})")
    return pdf

FLOW_TYPE_MAP = {
    "commercial_exchange_A": "commercial_exchange",
    "commercial_exchange_B": "commercial_exchange",
    "commercial_total":      "commercial_exchange",
    "physical_flow_A":       "physical_flow",
    "physical_flow_B":       "physical_flow",
    "physical_total":        "physical_flow",
    "border_small":          "other",
}

def classify_flow_type(flow_name):
    return FLOW_TYPE_MAP.get(flow_name, "other")

def make_flow_id(row):
    raw = f"{row['timestamp_ms']}_{row['filter_id']}_{row['region']}"
    return hashlib.md5(raw.encode()).hexdigest()

def clean_cross_border_batch(pdf_raw):
    if pdf_raw.empty:
        return pd.DataFrame(), 0, 0

    df = pdf_raw.copy()
    before = len(df)

    # Type casting
    df["timestamp_ms"] = df["timestamp_ms"].astype("int64")
    df["value_mw"] = pd.to_numeric(df["value_mw"], errors="coerce")
    df["reading_hour"] = pd.to_numeric(df["reading_hour"], errors="coerce").astype("Int64")
    df["filter_id"] = pd.to_numeric(df["filter_id"], errors="coerce").astype("Int64")

    # Null handling - drop rows with critical nulls
    df = df.dropna(subset=["timestamp_ms", "value_mw", "filter_id", "region"])

    # Range validation
    df = df[(df["value_mw"] >= -50000) & (df["value_mw"] <= 50000)]

    # reading_ts_15min - cross-border is hourly, so same as reading_ts but normalized
    df["reading_ts"] = pd.to_datetime(df["reading_ts"])
    df["reading_ts_15min"] = df["reading_ts"].dt.floor("15min")

    # flow_type classification
    df["flow_type"] = df["flow_name"].apply(classify_flow_type)

    # is_total flag - flows named with "total" or aggregate filter ids
    df["is_total"] = df["flow_name"].str.contains("total", case=False, na=False)

    # flow_id surrogate key
    df["flow_id"] = df.apply(make_flow_id, axis=1)

    # silver processed timestamp
    df["_silver_processed_at"] = datetime.now().isoformat()

    after = len(df)
    rejected = before - after
    return df, after, rejected

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
        log(f"Written {nrows} rows -> Snowflake SILVER.{table_name}")
        return nrows
    finally:
        conn.close()

def main():
    log("=" * 55)
    log("SMARD Cross-Border Processor")
    log("BigQuery Bronze -> PySpark -> Snowflake Silver")
    log(f"Target database: {SF_CONFIG['database']}")
    log("=" * 55)

    spark = create_spark()
    chunk_size = 50000

    year_filter = None
    if "--year" in sys.argv:
        idx = sys.argv.index("--year")
        year_filter = int(sys.argv[idx+1])

    where_clause = ""
    if year_filter:
        where_clause = f"EXTRACT(YEAR FROM TIMESTAMP_MILLIS(CAST(timestamp_ms AS INT64))) = {year_filter}"
        log(f"Processing ONLY year {year_filter}")

    total_rows = 0
    offset = 0
    while True:
        pdf_raw = read_from_bronze(
            "raw_cross_border_flows", chunk_size=chunk_size,
            offset=offset, where_clause=where_clause)
        if pdf_raw.empty:
            break
        pdf_clean, clean_count, rejected = clean_cross_border_batch(pdf_raw)
        if not pdf_clean.empty:
            rows = write_to_snowflake(pdf_clean, "stg_cross_border_clean")
            total_rows += rows
        offset += chunk_size
        log(f"Progress: {offset} processed, {total_rows} written, {rejected} rejected this batch")
        if len(pdf_raw) < chunk_size:
            break

    log(f"Cross-border processing complete: {total_rows} rows")
    spark.stop()

if __name__ == "__main__":
    main()
