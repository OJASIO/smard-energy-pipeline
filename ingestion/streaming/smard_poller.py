
import os, json, time, requests, sys
from datetime import datetime, timezone
from google.cloud import pubsub_v1

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/home/jovyan/smard-energy-pipeline/config/service_account.json"
)

PROJECT_ID    = "data-management-2-498012"
TOPIC_ID      = "smard-energy-live"
SMARD_BASE    = "https://www.smard.de/app/chart_data"
REGION        = "DE"
RESOLUTION    = "quarterhour"
POLL_INTERVAL = 900
HEADERS       = {"User-Agent": "Mozilla/5.0 (SMARD Pipeline)"}

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

def get_latest_timestamp(filter_id):
    url = f"{SMARD_BASE}/{filter_id}/{REGION}/index_{RESOLUTION}.json"
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        if r.status_code == 200:
            timestamps = r.json().get("timestamps", [])
            return timestamps[-1] if timestamps else None
    except Exception as e:
        print(f"  Warning: {e}")
    return None

def get_series(filter_id, timestamp_ms):
    url = (f"{SMARD_BASE}/{filter_id}/{REGION}/"
           f"{filter_id}_{REGION}_{RESOLUTION}_{timestamp_ms}.json")
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        if r.status_code == 200:
            return r.json().get("series", [])
    except Exception as e:
        print(f"  Warning: {e}")
    return []

def get_latest_reading(filter_id, source_name):
    latest_ts = get_latest_timestamp(filter_id)
    if not latest_ts:
        return None
    series = get_series(filter_id, latest_ts)
    for ts_ms, value in reversed(series):
        if value is not None:
            return {
                "timestamp_ms":  ts_ms,
                "reading_ts":    datetime.fromtimestamp(
                                   ts_ms/1000, tz=timezone.utc
                                 ).isoformat(),
                "filter_id":     filter_id,
                "energy_source": source_name,
                "region":        REGION,
                "region_name":   "Germany",
                "value_mw":      float(value),
                "data_source":   "stream",
                "_ingested_at":  datetime.now(tz=timezone.utc).isoformat(),
                "_pipeline_env": "hpc",
            }
    return None

def poll_and_publish(publisher, topic_path):
    published = 0
    failed    = 0
    print(f"\n{chr(45)*55}")
    print(f"  Poll: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{chr(45)*55}")
    for source_name, filter_id in SMARD_FILTERS.items():
        try:
            reading = get_latest_reading(filter_id, source_name)
            if reading:
                future = publisher.publish(
                    topic_path,
                    data=json.dumps(reading).encode("utf-8"),
                    energy_source=source_name,
                    region=REGION,
                )
                future.result()
                published += 1
                print(f"  OK {source_name:20} -> {reading['value_mw']:8.1f} MW")
            else:
                failed += 1
                print(f"  SKIP {source_name:20} -> no data")
            time.sleep(0.3)
        except Exception as e:
            failed += 1
            print(f"  FAIL {source_name:20} -> {e}")
    print(f"{chr(45)*55}")
    print(f"  Published: {published} | Failed: {failed}")
    return published

def main(run_once=False):
    print("="*55)
    print("  SMARD Energy Poller")
    print(f"  Sources: {len(SMARD_FILTERS)}")
    print("="*55)
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    print(f"\nConnected: {topic_path}")
    if run_once:
        poll_and_publish(publisher, topic_path)
        print("\nSingle poll complete")
        return
    print(f"\nPolling every {POLL_INTERVAL//60} min — Ctrl+C to stop")
    while True:
        try:
            poll_and_publish(publisher, topic_path)
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("\nStopped")
            break
        except Exception as e:
            print(f"\nError: {e} — retrying in 60s...")
            time.sleep(60)

if __name__ == "__main__":
    run_once = "--once" in sys.argv
    main(run_once=run_once)
