
"""
holidays_ingestion.py
Downloads German public holidays from nager.date API
Writes to BigQuery Bronze raw_german_holidays
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
BQ_TABLE   = "raw_german_holidays"
API_BASE   = "https://date.nager.at/api/v3/PublicHolidays"

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def fetch_holidays(year):
    url = f"{API_BASE}/{year}/DE"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        log(f"Error fetching holidays {year}: {e}")
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
    log("German Holidays Ingestion 2017-2025")
    log("=" * 55)

    all_records = []
    now         = datetime.now(tz=timezone.utc).isoformat()

    for year in range(2017, 2026):
        holidays = fetch_holidays(year)
        for h in holidays:
            all_records.append({
                "holiday_date":     h.get("date"),
                "holiday_name":     h.get("localName"),
                "holiday_name_en":  h.get("name"),
                "state_code":       "DE",
                "is_national":      h.get("global", True),
                "counties":         str(h.get("counties", [])),
                "data_source":      "nager_date",
                "_raw_ingested_at": now,
                "ingestion_date":   now[:10],
            })
        log(f"  {year}: {len(holidays)} holidays")

    total = write_to_bigquery(all_records)
    log(f"Holidays ingestion complete: {total} rows")

if __name__ == "__main__":
    main()
