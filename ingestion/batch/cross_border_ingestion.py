
import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery, storage


PROJECT_ID = "data-management-2-498012"
BQ_DATASET = "bronze"
BQ_TABLE   = "raw_cross_border_flows"
GCS_BUCKET = "data-management-2-smard-raw"
SMARD_BASE = "https://www.smard.de/app/chart_data"
REGION     = "DE"
RESOLUTION = "hour"
HEADERS    = {"User-Agent": "Mozilla/5.0 (SMARD Pipeline)"}

# Cross-border flow filter IDs confirmed working
# Labels based on SMARD API value analysis
# Total commercial exchange and physical flows
CROSS_BORDER_FILTERS = {
    "commercial_total":      5097,
    "physical_total":        5140,
    "physical_flow_A":       5129,
    "physical_flow_B":       5138,
    "commercial_exchange_A": 5104,
    "commercial_exchange_B": 5105,
    "border_small":          5078,
}

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[" + ts + "] " + msg)

def get_timestamps(filter_id):
    url = (SMARD_BASE + "/" + str(filter_id)
           + "/" + REGION
           + "/index_" + RESOLUTION + ".json")
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        if r.status_code == 200:
            return r.json().get("timestamps", [])
    except Exception as e:
        log("Error getting timestamps: " + str(e))
    return []

def get_series(filter_id, timestamp_ms):
    url = (SMARD_BASE + "/" + str(filter_id)
           + "/" + REGION + "/" + str(filter_id)
           + "_" + REGION + "_" + RESOLUTION
           + "_" + str(timestamp_ms) + ".json")
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        if r.status_code == 200:
            return r.json().get("series", [])
    except Exception as e:
        log("Error getting series: " + str(e))
    return []

def upload_to_gcs(pdf, gcs_path):
    gcs_client = storage.Client(project=PROJECT_ID)
    bucket     = gcs_client.bucket(GCS_BUCKET)
    blob       = bucket.blob(gcs_path)
    blob.upload_from_string(
        pdf.to_csv(index=False),
        content_type="text/csv"
    )
    gcs_uri = "gs://" + GCS_BUCKET + "/" + gcs_path
    log("Uploaded to " + gcs_uri)
    return gcs_uri

def load_gcs_to_bigquery(gcs_uri):
    bq_client  = bigquery.Client(project=PROJECT_ID)
    table_id   = PROJECT_ID + "." + BQ_DATASET + "." + BQ_TABLE
    job_config = bigquery.LoadJobConfig(
        source_format     = bigquery.SourceFormat.CSV,
        skip_leading_rows = 1,
        autodetect        = True,
        write_disposition = "WRITE_APPEND",
    )
    # Deterministic job_id derived from the GCS path: a client-side retry
    # on a transient network error re-submits the SAME job_id instead of
    # creating a new one, which raises Conflict if the original already
    # succeeded -- preventing the double-load bug found on 2026-06-26.
    safe_job_id = "load_" + re.sub(r"[^a-zA-Z0-9_]", "_", gcs_uri.replace("gs://", ""))
    try:
        load_job = bq_client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config, job_id=safe_job_id)
        load_job.result()
        log("Loaded " + gcs_uri + " -> " + table_id)
    except Exception as e:
        if "Already Exists" in str(e) or "duplicate" in str(e).lower():
            log("Load job " + safe_job_id + " already completed previously - skipping (no duplicate write)")
        else:
            raise

def download_filter(flow_name, filter_id, start_year, end_year):
    log("Downloading " + flow_name + " (filter " + str(filter_id) + ")...")

    all_timestamps = get_timestamps(filter_id)
    if not all_timestamps:
        log("No timestamps for " + flow_name)
        return 0

    start_ms = int(datetime(start_year, 1, 1).timestamp() * 1000)
    end_ms   = int(datetime(end_year, 12, 31).timestamp() * 1000)
    filtered = [ts for ts in all_timestamps
                if start_ms <= ts <= end_ms]

    log("Timestamps in range: " + str(len(filtered)))

    now        = datetime.now(tz=timezone.utc).isoformat()
    total_rows = 0
    batch      = []
    batch_num  = 0

    for i, ts_ms in enumerate(filtered):
        series = get_series(filter_id, ts_ms)

        for reading_ts_ms, value in series:
            if value is None:
                continue
            reading_dt = datetime.fromtimestamp(
                reading_ts_ms / 1000, tz=timezone.utc)
            if not (start_year <= reading_dt.year <= end_year):
                continue

            batch.append({
                "timestamp_ms":     reading_ts_ms,
                "reading_ts":       reading_dt.isoformat(),
                "reading_date":     reading_dt.strftime("%Y-%m-%d"),
                "reading_hour":     reading_dt.hour,
                "filter_id":        filter_id,
                "flow_name":        flow_name,
                "region":           REGION,
                "value_mw":         float(value),
                "data_source":      "smard_cross_border",
                "_raw_ingested_at": now,
                "ingestion_date":   now[:10],
            })

        if len(batch) >= 5000:
            batch_num += 1
            pdf      = pd.DataFrame(batch)
            gcs_path = (
                "smard/historical/crossborder/"
                + str(start_year) + "/"
                + flow_name + "_batch"
                + str(batch_num) + ".csv"
            )
            gcs_uri = upload_to_gcs(pdf, gcs_path)
            load_gcs_to_bigquery(gcs_uri)
            total_rows += len(batch)
            log("Batch " + str(batch_num) + ": "
                + str(len(batch)) + " rows via GCS")
            batch = []

        if i % 5 == 0:
            time.sleep(0.3)

    if batch:
        batch_num += 1
        pdf      = pd.DataFrame(batch)
        gcs_path = (
            "smard/historical/crossborder/"
            + str(start_year) + "/"
            + flow_name + "_batch"
            + str(batch_num) + ".csv"
        )
        gcs_uri = upload_to_gcs(pdf, gcs_path)
        load_gcs_to_bigquery(gcs_uri)
        total_rows += len(batch)

    log("Done " + flow_name + ": " + str(total_rows) + " rows")
    return total_rows

def main():
    test_mode  = "--test" in sys.argv
    start_year = 2023 if test_mode else 2017
    end_year   = 2023 if test_mode else 2024
    if "--year" in sys.argv:
        idx = sys.argv.index("--year")
        custom_year = int(sys.argv[idx+1])
        start_year = custom_year
        end_year = custom_year

    log("=" * 55)
    log("Cross-Border Flow Ingestion via GCS")
    log("Flow: SMARD API -> GCS CSV -> BigQuery Bronze")
    log("Years: " + str(start_year) + " to " + str(end_year))
    log("Filters: " + str(len(CROSS_BORDER_FILTERS)))
    log("Mode: " + ("TEST 2023 only" if test_mode else "FULL"))
    log("=" * 55)

    grand_total = 0
    for flow_name, filter_id in CROSS_BORDER_FILTERS.items():
        try:
            rows = download_filter(
                flow_name, filter_id,
                start_year, end_year)
            grand_total += rows
        except Exception as e:
            log("Error on " + flow_name + ": " + str(e))

    log("Grand total: " + str(grand_total) + " rows")
    log("All CSVs in gs://" + GCS_BUCKET + "/smard/historical/crossborder/")
    log("Cross-border ingestion complete!")

if __name__ == "__main__":
    main()
