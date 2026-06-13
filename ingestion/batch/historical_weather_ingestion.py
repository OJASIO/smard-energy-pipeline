
import os
import sys
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/home/jovyan/smard-energy-pipeline/config/service_account.json"
)

PROJECT_ID = "data-management-2-498012"
BQ_DATASET = "bronze"
BQ_TABLE   = "raw_weather_historical"
API_BASE   = "https://archive-api.open-meteo.com/v1/archive"
HEADERS    = {"User-Agent": "Mozilla/5.0 (SMARD Pipeline)"}

REGIONS = {
    "DE":         {"lat": 51.1657, "lon": 10.4515, "name": "Germany"},
    "50Hertz":    {"lat": 52.5200, "lon": 13.4050, "name": "North/East Germany"},
    "Amprion":    {"lat": 51.5136, "lon":  7.4653, "name": "West Germany"},
    "TenneT":     {"lat": 48.1351, "lon": 11.5820, "name": "South/Central Germany"},
    "TransnetBW": {"lat": 48.7758, "lon":  9.1829, "name": "Baden-Wuerttemberg"},
}

WEATHER_VARIABLES = [
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "direct_radiation",
    "diffuse_radiation",
    "temperature_2m",
    "cloud_cover",
    "precipitation",
]

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("[" + ts + "] " + msg)

def fetch_historical_weather(region_code, coords, start_date, end_date):
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
            log("Error " + str(r.status_code) + " for " + region_code)
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
                "wind_speed_ms":     hourly.get("wind_speed_10m",     [None]*len(times))[i],
                "wind_direction":    hourly.get("wind_direction_10m",  [None]*len(times))[i],
                "wind_gusts_ms":     hourly.get("wind_gusts_10m",     [None]*len(times))[i],
                "solar_direct_wm2":  hourly.get("direct_radiation",   [None]*len(times))[i],
                "solar_diffuse_wm2": hourly.get("diffuse_radiation",  [None]*len(times))[i],
                "temperature_c":     hourly.get("temperature_2m",     [None]*len(times))[i],
                "cloud_cover_pct":   hourly.get("cloud_cover",        [None]*len(times))[i],
                "precipitation_mm":  hourly.get("precipitation",      [None]*len(times))[i],
                "data_source":       "historical",
                "_raw_ingested_at":  now,
                "ingestion_date":    now[:10],
            })
        return records

    except Exception as e:
        log("Error fetching " + region_code + ": " + str(e))
        return []

def write_to_bigquery(records):
    if not records:
        return 0
    bq_client  = bigquery.Client(project=PROJECT_ID)
    table_id   = PROJECT_ID + "." + BQ_DATASET + "." + BQ_TABLE
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
    test_mode  = "--test" in sys.argv
    start_year = 2023 if test_mode else 2017
    end_year   = 2023 if test_mode else 2024

    log("=" * 55)
    log("Historical Weather Ingestion")
    log("Source: Open-Meteo Archive API")
    log("Years:  " + str(start_year) + " to " + str(end_year))
    log("Regions: " + str(len(REGIONS)))
    log("Mode: " + ("TEST 2023 only" if test_mode else "FULL"))
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
                written = write_to_bigquery(records)
                grand_total += written
                log("  Written " + str(written) + " rows for "
                    + region_code + " " + str(year))
            else:
                log("  No data for " + region_code + " " + str(year))

    log("Grand total: " + str(grand_total) + " rows")
    log("Historical weather ingestion complete!")

if __name__ == "__main__":
    main()
