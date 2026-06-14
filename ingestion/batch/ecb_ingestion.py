
import os
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery, storage


PROJECT_ID = "data-management-2-498012"
BQ_DATASET = "bronze"
BQ_TABLE   = "raw_ecb_rates"
GCS_BUCKET = "data-management-2-smard-raw"
ECB_BASE   = "https://data-api.ecb.europa.eu/service/data"

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[" + ts + "] " + msg)

def fetch_ecb_series(series_key, rate_type, start="2017-01-01"):
    url = ECB_BASE + "/" + series_key + "?format=jsondata&startPeriod=" + start
    try:
        r = requests.get(url, timeout=30,
                        headers={"Accept": "application/json"})
        if r.status_code != 200:
            log("ECB API error " + str(r.status_code)
                + " for " + series_key)
            return []

        data       = r.json()
        series     = data.get("dataSets", [{}])[0].get("series", {})
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
        log("Error fetching ECB " + series_key + ": " + str(e))
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
    log("ECB Rates Ingestion via GCS")
    log("Flow: ECB API -> GCS CSV -> BigQuery Bronze")
    log("=" * 55)

    all_records = []

    log("Fetching EUR/USD exchange rate...")
    records = fetch_ecb_series(
        "EXR/D.USD.EUR.SP00.A", "EUR_USD_RATE")
    all_records.extend(records)
    log("EUR/USD: " + str(len(records)) + " records")

    if all_records:
        pdf      = pd.DataFrame(all_records)
        now      = datetime.now().strftime("%Y%m%d_%H%M%S")
        gcs_path = "ecb/ecb_rates_" + now + ".csv"
        gcs_uri  = upload_to_gcs(pdf, gcs_path)
        load_gcs_to_bigquery(gcs_uri)
        log("ECB ingestion complete: " + str(len(all_records)) + " rows")

if __name__ == "__main__":
    main()
