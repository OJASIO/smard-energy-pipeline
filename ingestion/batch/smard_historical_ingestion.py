
"""
smard_historical_ingestion.py
Downloads SMARD historical data 2017-2024
Writes RAW to BigQuery Bronze
Run once for backfill
"""

import os
import sys
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/home/jovyan/smard-energy-pipeline/config/service_account.json"
)

PROJECT_ID  = "data-management-2-498012"
BQ_DATASET  = "bronze"
BQ_TABLE    = "raw_energy_historical"
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
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def get_timestamps(filter_id):
    url = (f"{SMARD_BASE}/{filter_id}/{REGION}"
           f"/index_{RESOLUTION}.json")
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        if r.status_code == 200:
            return r.json().get("timestamps", [])
    except Exception as e:
        log(f"Error getting timestamps for {filter_id}: {e}")
    return []

def get_series(filter_id, timestamp_ms):
    url = (f"{SMARD_BASE}/{filter_id}/{REGION}"
           f"/{filter_id}_{REGION}_{RESOLUTION}_{timestamp_ms}.json")
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        if r.status_code == 200:
            return r.json().get("series", [])
    except Exception as e:
        log(f"Error getting series: {e}")
    return []

def write_to_bigquery(records):
    if not records:
        return 0
    bq_client  = bigquery.Client(project=PROJECT_ID)
    table_id   = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    pdf = pd.DataFrame(records)
    job = bq_client.load_table_from_dataframe(
        pdf, table_id, job_config=job_config)
    job.result()
    return len(pdf)

def download_source(source_name, filter_id,
                    start_year=2017, end_year=2024):
    log(f"Downloading {source_name} ({filter_id})...")

    # Get all available timestamps
    all_timestamps = get_timestamps(filter_id)
    if not all_timestamps:
        log(f"  No timestamps for {source_name}")
        return 0

    # Filter to requested year range
    start_ms = int(datetime(start_year, 1, 1).timestamp() * 1000)
    end_ms   = int(datetime(end_year, 12, 31).timestamp() * 1000)

    filtered = [ts for ts in all_timestamps
                if start_ms <= ts <= end_ms]
    log(f"  Timestamps in range: {len(filtered)}")

    total_rows  = 0
    batch       = []
    now         = datetime.now(tz=timezone.utc).isoformat()

    for i, ts_ms in enumerate(filtered):
        series = get_series(filter_id, ts_ms)

        for reading_ts_ms, value in series:
            if value is None:
                continue

            reading_dt = datetime.fromtimestamp(
                reading_ts_ms / 1000, tz=timezone.utc)

            # Only include requested year range
            if not (start_year <= reading_dt.year <= end_year):
                continue

            batch.append({
                "timestamp_ms":  reading_ts_ms,
                "reading_ts":    reading_dt.isoformat(),
                "filter_id":     filter_id,
                "energy_source": source_name,
                "region":        REGION,
                "region_name":   "Germany",
                "value_mw":      float(value),
                "data_source":   "historical",
                "_raw_ingested_at": now,
                "ingestion_date": now[:10],
            })

        # Write in batches of 5000
        if len(batch) >= 5000:
            written = write_to_bigquery(batch)
            total_rows += written
            log(f"  Written batch: {written} rows "
                f"(total: {total_rows})")
            batch = []

        # Small delay every 10 chunks
        if i % 10 == 0:
            time.sleep(0.5)

    # Write remaining
    if batch:
        written = write_to_bigquery(batch)
        total_rows += written
        log(f"  Written final batch: {written} rows")

    log(f"  Done {source_name}: {total_rows} total rows")
    return total_rows

def main():
    # For testing use 2023 only
    # For full backfill use 2017 to 2024
    test_mode = "--test" in sys.argv
    start_year = 2023 if test_mode else 2017
    end_year   = 2023 if test_mode else 2024

    log("=" * 55)
    log("SMARD Historical Ingestion")
    log(f"Years: {start_year} to {end_year}")
    log(f"Sources: {len(SMARD_FILTERS)}")
    log(f"Mode: {'TEST 2023 only' if test_mode else 'FULL 2017-2024'}")
    log("=" * 55)

    grand_total = 0
    for source_name, filter_id in SMARD_FILTERS.items():
        try:
            rows = download_source(
                source_name, filter_id,
                start_year, end_year)
            grand_total += rows
        except Exception as e:
            log(f"Error on {source_name}: {e}")

    log(f"Grand total rows: {grand_total}")
    log("Historical ingestion complete!")

if __name__ == "__main__":
    main()
