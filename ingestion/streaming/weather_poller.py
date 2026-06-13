
import os, json, time, requests, sys
from datetime import datetime, timezone
from google.cloud import pubsub_v1

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/home/jovyan/smard-energy-pipeline/config/service_account.json"
)

PROJECT_ID    = "data-management-2-498012"
TOPIC_ID      = "weather-live"
POLL_INTERVAL = 900
HEADERS       = {"User-Agent": "Mozilla/5.0"}
API_BASE      = "https://api.open-meteo.com/v1/forecast"

REGIONS = {
    "DE":         {"lat": 51.1657, "lon": 10.4515, "name": "Germany"},
    "50Hertz":    {"lat": 52.5200, "lon": 13.4050, "name": "North/East Germany"},
    "Amprion":    {"lat": 51.5136, "lon":  7.4653, "name": "West Germany"},
    "TenneT":     {"lat": 48.1351, "lon": 11.5820, "name": "South/Central Germany"},
    "TransnetBW": {"lat": 48.7758, "lon":  9.1829, "name": "Baden-Wuerttemberg"},
}

WEATHER_VARIABLES = [
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
    "direct_radiation", "diffuse_radiation",
    "temperature_2m", "cloud_cover", "precipitation",
]

def fetch_weather(region_code, coords):
    params = {
        "latitude":  coords["lat"],
        "longitude": coords["lon"],
        "current":   ",".join(WEATHER_VARIABLES),
        "timezone":  "Europe/Berlin",
    }
    try:
        r = requests.get(API_BASE, params=params, timeout=15, headers=HEADERS)
        if r.status_code == 200:
            current = r.json().get("current", {})
            now_utc = datetime.now(tz=timezone.utc)
            return {
                "observation_time":  current.get("time", ""),
                "reading_ts":        now_utc.isoformat(),
                "region":            region_code,
                "region_name":       coords["name"],
                "latitude":          coords["lat"],
                "longitude":         coords["lon"],
                "wind_speed_ms":     current.get("wind_speed_10m"),
                "wind_direction":    current.get("wind_direction_10m"),
                "wind_gusts_ms":     current.get("wind_gusts_10m"),
                "solar_direct_wm2":  current.get("direct_radiation"),
                "solar_diffuse_wm2": current.get("diffuse_radiation"),
                "temperature_c":     current.get("temperature_2m"),
                "cloud_cover_pct":   current.get("cloud_cover"),
                "precipitation_mm":  current.get("precipitation"),
                "data_source":       "stream",
                "_ingested_at":      now_utc.isoformat(),
                "_pipeline_env":     "hpc",
            }
    except Exception as e:
        print(f"  Warning: {region_code}: {e}")
    return None

def poll_and_publish(publisher, topic_path):
    published = 0
    print(f"\n{chr(45)*55}")
    print(f"  Weather Poll: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{chr(45)*55}")
    for region_code, coords in REGIONS.items():
        try:
            reading = fetch_weather(region_code, coords)
            if reading:
                publisher.publish(
                    topic_path,
                    data=json.dumps(reading).encode("utf-8"),
                    region=region_code,
                ).result()
                published += 1
                print(f"  OK {region_code:12} Temp:{reading['temperature_c']:5.1f}C "
                      f"Wind:{reading['wind_speed_ms']:5.1f}km/h")
            time.sleep(0.2)
        except Exception as e:
            print(f"  FAIL {region_code}: {e}")
    print(f"{chr(45)*55}")
    print(f"  Published: {published}")
    return published

def main(run_once=False):
    print("="*55)
    print("  Open-Meteo Weather Poller")
    print(f"  Regions: {len(REGIONS)}")
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
