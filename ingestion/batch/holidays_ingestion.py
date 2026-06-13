
import os
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery, storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/home/jovyan/smard-energy-pipeline/config/service_account.json"
)

PROJECT_ID = "data-management-2-498012"
BQ_DATASET = "bronze"
BQ_TABLE   = "raw_german_holidays"
GCS_BUCKET = "data-management-2-smard-raw"
API_BASE   = "https://date.nager.at/api/v3/PublicHolidays"

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[" + ts + "] " + msg)

def fetch_holidays(year):
    url = API_BASE + "/" + str(year) + "/DE"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        log("Error fetching holidays " + str(year) + ": " + str(e))
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
    load_job = bq_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config)
    load_job.result()
    log("Loaded " + gcs_uri + " -> " + table_id)

def main():
    log("=" * 55)
    log("German Holidays Ingestion via GCS")
    log("Flow: nager.date API -> GCS CSV -> BigQuery Bronze")
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
        log(str(year) + ": " + str(len(holidays)) + " holidays")

    if all_records:
        pdf      = pd.DataFrame(all_records)
        now_str  = datetime.now().strftime("%Y%m%d_%H%M%S")
        gcs_path = "holidays/german_holidays_" + now_str + ".csv"
        gcs_uri  = upload_to_gcs(pdf, gcs_path)
        load_gcs_to_bigquery(gcs_uri)
        log("Holidays complete: " + str(len(all_records)) + " rows")

if __name__ == "__main__":
    main()
