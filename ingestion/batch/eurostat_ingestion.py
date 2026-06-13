
"""
eurostat_ingestion.py
Downloads Eurostat energy price index for Germany
Writes to BigQuery Bronze raw_eurostat_energy
"""

import os
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/home/jovyan/smard-energy-pipeline/config/service_account.json"
)

PROJECT_ID = "data-management-2-498012"
BQ_DATASET = "bronze"
BQ_TABLE   = "raw_eurostat_energy"
API_BASE   = (
    "https://ec.europa.eu/eurostat/api/dissemination"
    "/statistics/1.0/data"
)

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def fetch_eurostat(dataset, params):
    url = f"{API_BASE}/{dataset}"
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        log(f"Eurostat error {r.status_code}")
    except Exception as e:
        log(f"Eurostat fetch error: {e}")
    return None

def parse_eurostat(data, indicator_name):
    if not data:
        return []
    try:
        values    = data.get("value", {})
        dims      = data.get("dimension", {})
        time_dim  = dims.get("time", {}).get(
                        "category", {}).get("index", {})
        time_vals = {v: k for k, v in time_dim.items()}
        records   = []
        now       = datetime.now(tz=timezone.utc).isoformat()

        for idx_str, value in values.items():
            idx  = int(idx_str)
            period = time_vals.get(idx, "")
            records.append({
                "reference_period":  period,
                "indicator":         indicator_name,
                "value":             float(value),
                "geo":               "DE",
                "unit":              "INDEX",
                "data_source":       "eurostat",
                "_raw_ingested_at":  now,
                "ingestion_date":    now[:10],
            })
        return records
    except Exception as e:
        log(f"Parse error: {e}")
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

def main():
    log("=" * 55)
    log("Eurostat Energy Price Index Ingestion")
    log("=" * 55)

    all_records = []

    # Energy HICP for Germany
    log("Fetching energy price index...")
    params = {
        "geo":             "DE",
        "coicop":          "CP045",
        "format":          "JSON",
        "sinceTimePeriod": "2017-01",
    }
    data    = fetch_eurostat("prc_hicp_midx", params)
    records = parse_eurostat(data, "HICP_ENERGY_DE")
    all_records.extend(records)
    log(f"  Energy HICP: {len(records)} records")

    total = write_to_bigquery(all_records)
    log(f"Eurostat ingestion complete: {total} rows")

if __name__ == "__main__":
    main()
