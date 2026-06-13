
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
BQ_TABLE   = "raw_eurostat_energy"
GCS_BUCKET = "data-management-2-smard-raw"
API_BASE   = (
    "https://ec.europa.eu/eurostat/api/dissemination"
    "/statistics/1.0/data"
)

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[" + ts + "] " + msg)

def fetch_eurostat(dataset, params):
    url = API_BASE + "/" + dataset
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        log("Eurostat error " + str(r.status_code))
    except Exception as e:
        log("Eurostat fetch error: " + str(e))
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
            idx    = int(idx_str)
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
        log("Parse error: " + str(e))
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
    log("Eurostat Energy Price Index via GCS")
    log("Flow: Eurostat API -> GCS CSV -> BigQuery Bronze")
    log("=" * 55)

    all_records = []

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
    log("Energy HICP: " + str(len(records)) + " records")

    if all_records:
        pdf      = pd.DataFrame(all_records)
        now_str  = datetime.now().strftime("%Y%m%d_%H%M%S")
        gcs_path = "eurostat/eurostat_energy_" + now_str + ".csv"
        gcs_uri  = upload_to_gcs(pdf, gcs_path)
        load_gcs_to_bigquery(gcs_uri)
        log("Eurostat complete: " + str(len(all_records)) + " rows")

if __name__ == "__main__":
    main()
