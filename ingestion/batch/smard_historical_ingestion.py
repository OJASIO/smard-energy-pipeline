
import os
import sys
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery, storage


PROJECT_ID  = "data-management-2-498012"
BQ_DATASET  = "bronze"
BQ_TABLE    = "raw_energy_historical"
GCS_BUCKET  = "data-management-2-smard-raw"
SMARD_BASE  = "https://www.smard.de/app/chart_data"
REGION      = "DE"
RESOLUTION  = "quarterhour"
HEADERS     = {"User-Agent": "Mozilla/5.0 (SMARD Pipeline)"}

SMARD_FILTERS = {
    "wind_onshore":       4067,
    "wind_offshore":      1225,
    "solar":              4068,
    "biomass":            4066,
    "hydro":              1226,
    "pumped_storage":     4070,
    "other_renewables":   1228,
    "other_conventional": 1227,
    "lignite":            1223,
    "nuclear":            1224,
    "hard_coal":          4069,
    "natural_gas":        4071,
    "consumption":        410,
    "price_de_lu":        4169,
}

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[" + ts + "] " + msg)

def get_timestamps(filter_id):
    url = (SMARD_BASE + "/" + str(filter_id) + "/" + REGION
           + "/index_" + RESOLUTION + ".json")
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        if r.status_code == 200:
            return r.json().get("timestamps", [])
    except Exception as e:
        log("Error getting timestamps: " + str(e))
    return []

def get_series(filter_id, timestamp_ms):
    url = (SMARD_BASE + "/" + str(filter_id) + "/" + REGION
           + "/" + str(filter_id) + "_" + REGION
           + "_" + RESOLUTION + "_" + str(timestamp_ms) + ".json")
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
    csv_data   = pdf.to_csv(index=False)
    blob.upload_from_string(csv_data, content_type="text/csv")
    log("Uploaded to gs://" + GCS_BUCKET + "/" + gcs_path)
    return "gs://" + GCS_BUCKET + "/" + gcs_path

def load_gcs_to_bigquery(gcs_uri, table_name):
    bq_client  = bigquery.Client(project=PROJECT_ID)
    table_id   = PROJECT_ID + "." + BQ_DATASET + "." + table_name
    job_config = bigquery.LoadJobConfig(
        source_format     = bigquery.SourceFormat.CSV,
        skip_leading_rows = 1,
        autodetect        = True,
        write_disposition = "WRITE_APPEND",
    )
    load_job = bq_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config)
    load_job.result()
    log("Loaded " + gcs_uri + " -> " + table_id)

def download_source(source_name, filter_id,
                    start_year=2023, end_year=2023):
    log("Downloading " + source_name + "...")

    all_timestamps = get_timestamps(filter_id)
    if not all_timestamps:
        log("  No timestamps for " + source_name)
        return 0

    start_ms = int(datetime(start_year, 1, 1).timestamp() * 1000)
    end_ms   = int(datetime(end_year, 12, 31).timestamp() * 1000)
    filtered = [ts for ts in all_timestamps
                if start_ms <= ts <= end_ms]

    log("  Timestamps in range: " + str(len(filtered)))

    now         = datetime.now(tz=timezone.utc).isoformat()
    total_rows  = 0
    batch       = []
    batch_num   = 0

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
                "filter_id":        filter_id,
                "energy_source":    source_name,
                "region":           REGION,
                "region_name":      "Germany",
                "value_mw":         float(value),
                "data_source":      "historical",
                "_raw_ingested_at": now,
                "ingestion_date":   now[:10],
            })

        if len(batch) >= 5000:
            batch_num += 1
            pdf = pd.DataFrame(batch)

            # Step 1: Save to GCS
            gcs_path = (
                "smard/historical/energy/"
                + str(start_year) + "/"
                + source_name + "_batch"
                + str(batch_num) + ".csv"
            )
            gcs_uri = upload_to_gcs(pdf, gcs_path)

            # Step 2: Load GCS → BigQuery
            load_gcs_to_bigquery(gcs_uri, BQ_TABLE)

            total_rows += len(batch)
            log("  Batch " + str(batch_num)
                + ": " + str(len(batch)) + " rows via GCS")
            batch = []

        if i % 10 == 0:
            time.sleep(0.5)

    # Write remaining batch
    if batch:
        batch_num += 1
        pdf     = pd.DataFrame(batch)
        gcs_path = (
            "smard/historical/energy/"
            + str(start_year) + "/"
            + source_name + "_batch"
            + str(batch_num) + ".csv"
        )
        gcs_uri = upload_to_gcs(pdf, gcs_path)
        load_gcs_to_bigquery(gcs_uri, BQ_TABLE)
        total_rows += len(batch)

    log("Done " + source_name + ": " + str(total_rows) + " rows")
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
    log("SMARD Historical Ingestion via GCS")
    log("Flow: SMARD API -> GCS CSV -> BigQuery Bronze")
    log("Years: " + str(start_year) + " to " + str(end_year))
    log("Sources: " + str(len(SMARD_FILTERS)))
    log("=" * 55)

    grand_total = 0
    for source_name, filter_id in SMARD_FILTERS.items():
        try:
            rows = download_source(
                source_name, filter_id,
                start_year, end_year)
            grand_total += rows
        except Exception as e:
            log("Error on " + source_name + ": " + str(e))

    log("Grand total: " + str(grand_total) + " rows")
    log("All CSVs saved to gs://" + GCS_BUCKET + "/smard/historical/")
    log("Historical ingestion complete!")

if __name__ == "__main__":
    main()
