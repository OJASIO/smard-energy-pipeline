
import os
import sys
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery, storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/home/jovyan/smard-energy-pipeline/config/service_account.json"
)

PROJECT_ID = "data-management-2-498012"
BQ_DATASET = "bronze"
BQ_TABLE   = "raw_weather_historical"
GCS_BUCKET = "data-management-2-smard-raw"
API_BASE   = "https://archive-api.open-meteo.com/v1/archive"
HEADERS    = {"User-Agent": "Mozilla/5.0 (SMARD Pipeline)"}

REGIONS = {
    "DE":         {"lat": 51.1657, "lon": 10.4515,
                   "name": "Germany"},
    "50Hertz":    {"lat": 52.5200, "lon": 13.4050,
                   "name": "North/East Germany"},
    "Amprion":    {"lat": 51.5136, "lon":  7.4653,
                   "name": "West Germany"},
    "TenneT":     {"lat": 48.1351, "lon": 11.5820,
                   "name": "South/Central Germany"},
    "TransnetBW": {"lat": 48.7758, "lon":  9.1829,
                   "name": "Baden-Wuerttemberg"},
}

WEATHER_VARIABLES = [
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
    "direct_radiation", "diffuse_radiation",
    "temperature_2m", "cloud_cover", "precipitation",
]

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[" + ts + "] " + msg)

def fetch_historical_weather(region_code, coords,
                              start_date, end_date):
    params = {
        "latitude":   coords["lat"],
        "longitude":  coords["lon"],
        "start_date": start_date,
        "end_date":   end_date,
        "hourly":     ",".join(WEATHER_VARIABLES),
        "timezone":   "Europe/Berlin",
    }
    try:
        r = requests.get(API_BASE, params=params,
                         timeout=60, headers=HEADERS)
        if r.status_code != 200:
            log("Error " + str(r.status_code)
                + " for " + region_code)
            return []

        data   = r.json()
        hourly = data.get("hourly", {})
        times  = hourly.get("time", [])
        now    = datetime.now(tz=timezone.utc).isoformat()

        records = []
        for i, time_str in enumerate(times):
            records.append({
                "observation_time":  time_str,
                "reading_ts":        time_str + ":00+00:00",
                "region":            region_code,
                "region_name":       coords["name"],
                "latitude":          coords["lat"],
                "longitude":         coords["lon"],
                "wind_speed_ms":     hourly.get("wind_speed_10m",    [None]*len(times))[i],
                "wind_direction":    hourly.get("wind_direction_10m", [None]*len(times))[i],
                "wind_gusts_ms":     hourly.get("wind_gusts_10m",    [None]*len(times))[i],
                "solar_direct_wm2":  hourly.get("direct_radiation",  [None]*len(times))[i],
                "solar_diffuse_wm2": hourly.get("diffuse_radiation", [None]*len(times))[i],
                "temperature_c":     hourly.get("temperature_2m",    [None]*len(times))[i],
                "cloud_cover_pct":   hourly.get("cloud_cover",       [None]*len(times))[i],
                "precipitation_mm":  hourly.get("precipitation",     [None]*len(times))[i],
                "data_source":       "historical",
                "_raw_ingested_at":  now,
                "ingestion_date":    now[:10],
            })
        return records

    except Exception as e:
        log("Error fetching " + region_code + ": " + str(e))
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

def main():
    test_mode  = "--test" in sys.argv
    start_year = 2023 if test_mode else 2017
    end_year   = 2023 if test_mode else 2024

    log("=" * 55)
    log("Historical Weather Ingestion via GCS")
    log("Flow: Open-Meteo API -> GCS CSV -> BigQuery Bronze")
    log("Years: " + str(start_year) + " to " + str(end_year))
    log("Regions: " + str(len(REGIONS)))
    log("=" * 55)

    grand_total = 0

    for year in range(start_year, end_year + 1):
        start_date = str(year) + "-01-01"
        end_date   = str(year) + "-12-31"
        log("Downloading year " + str(year) + "...")

        for region_code, coords in REGIONS.items():
            log("  Fetching " + region_code + "...")
            records = fetch_historical_weather(
                region_code, coords, start_date, end_date)

            if records:
                pdf = pd.DataFrame(records)

                # Step 1: Save to GCS
                gcs_path = (
                    "weather/historical/"
                    + str(year) + "/"
                    + region_code + ".csv"
                )
                gcs_uri = upload_to_gcs(pdf, gcs_path)

                # Step 2: Load GCS → BigQuery
                load_gcs_to_bigquery(gcs_uri, BQ_TABLE)

                grand_total += len(records)
                log("  Written " + str(len(records))
                    + " rows for " + region_code
                    + " " + str(year) + " via GCS")
            else:
                log("  No data for " + region_code
                    + " " + str(year))

    log("Grand total: " + str(grand_total) + " rows")
    log("All CSVs saved to gs://" + GCS_BUCKET + "/weather/historical/")
    log("Historical weather ingestion complete!")

if __name__ == "__main__":
    main()
