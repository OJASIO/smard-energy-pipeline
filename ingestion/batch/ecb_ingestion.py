
"""
ecb_ingestion.py
Downloads ECB exchange rates and interest rates
Writes to BigQuery Bronze raw_ecb_rates
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
BQ_TABLE   = "raw_ecb_rates"
ECB_BASE   = "https://data-api.ecb.europa.eu/service/data"

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def fetch_ecb_series(series_key, rate_type, start="2017-01-01"):
    url = (f"{ECB_BASE}/{series_key}"
           f"?format=jsondata&startPeriod={start}")
    try:
        r = requests.get(url, timeout=30,
                        headers={"Accept": "application/json"})
        if r.status_code != 200:
            log(f"ECB API error {r.status_code} for {series_key}")
            return []

        data       = r.json()
        series     = data.get("dataSets", [{}])[0].get(
                         "series", {})
        dates      = data.get("structure", {}).get(
                         "dimensions", {}).get(
                         "observation", [{}])[0].get("values", [])
        records    = []
        now        = datetime.now(tz=timezone.utc).isoformat()

        for series_key_inner, series_data in series.items():
            observations = series_data.get("observations", {})
            for date_idx, obs in observations.items():
                if obs and obs[0] is not None:
                    records.append({
                        "rate_date":        dates[int(date_idx)]["id"],
                        "rate_type":        rate_type,
                        "rate_value":       float(obs[0]),
                        "series_key":       series_key,
                        "data_source":      "ecb",
                        "_raw_ingested_at": now,
                        "ingestion_date":   now[:10],
                    })
        return records
    except Exception as e:
        log(f"Error fetching ECB {series_key}: {e}")
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
    log(f"Written {len(pdf)} rows to {BQ_TABLE}")
    return len(pdf)

def main():
    log("=" * 55)
    log("ECB Rates Ingestion")
    log("=" * 55)

    all_records = []

    # EUR/USD exchange rate
    log("Fetching EUR/USD exchange rate...")
    records = fetch_ecb_series(
        "EXR/D.USD.EUR.SP00.A", "EUR_USD_RATE")
    all_records.extend(records)
    log(f"  EUR/USD: {len(records)} records")

    # ECB deposit facility rate
    log("Fetching ECB deposit rate...")
    records = fetch_ecb_series(
        "FM/B.U2.EUR.RT.MM.EURIBOR3MD_.HSTA",
        "EURIBOR_3M")
    all_records.extend(records)
    log(f"  EURIBOR 3M: {len(records)} records")

    total = write_to_bigquery(all_records)
    log(f"ECB ingestion complete: {total} rows")

if __name__ == "__main__":
    main()
