"""
Purpose:
    Fetch raw weather data from the Open-Meteo API for all configured locations
    and store it in the Bronze layer of the data lake.

What This Script Does:
    • Loads API and location settings from /config  
    • Sends REST requests to Open-Meteo (with retry + caching)  
    • Validates data completeness before saving  
    • Saves each valid response as a timestamped file in /data_lake/bronze  

Output:
    data_lake/bronze/<location>_raw_<YYYYMMDD_HHMMSS>.json

Usage:
    python scripts/test_api_call.py
"""

import json
import logging
import re
import requests_cache
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path
from datetime import datetime

# =============================
# Setup paths
# =============================
REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = REPO_ROOT / "data_lake" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

# =============================
# Logging
# =============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# =============================
# Retry + Cache Setup
# =============================
session = requests_cache.CachedSession(
    cache_name=".cache",
    expire_after=3600  # 1 hour cache
)

retry_strategy = Retry(
    total=5,  # max retries
    backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504]
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

BASE_URL = "https://api.open-meteo.com/v1/forecast"

# =============================
# Load configuration files
# =============================
try:
    CONFIG_API = json.load(open(REPO_ROOT / "config" / "api_settings.json"))
    CONFIG_LOCATIONS = json.load(open(REPO_ROOT / "config" / "locations.json"))
except Exception:
    logging.exception("Failed to load configuration files.")
    raise


# =============================
# Helper: sanitize filenames
# =============================
def sanitize_filename(name: str) -> str:
    name = name.lower().replace(" ", "_")
    return re.sub(r"[^a-z0-9_\-]", "", name)


# =============================
# API Fetch Function
# =============================
def fetch_raw_weather(location):
    """Fetches raw JSON response from Open-Meteo API (with retry + cache)."""

    params = {
        "latitude": location["lat"],
        "longitude": location["lon"],
        "timezone": location["timezone"],
        "hourly": ",".join(CONFIG_API["hourly"]),
        "daily": ",".join(CONFIG_API["daily"]),
        "current": ",".join(CONFIG_API["current"]),
        "past_days": 1
    }

    logging.info(f"Fetching RAW weather data for: {location['name']}")

    response = session.get(BASE_URL, params=params)

    logging.info(f"Cache Hit: {response.from_cache}")
    logging.info(f"Status Code: {response.status_code}")

    if response.status_code != 200:
        raise RuntimeError(
            f"API Error for {location['name']} — Status {response.status_code}"
        )

    return response.json()


# =============================
# Bronze Validation Functions
# =============================

def validate_top_level_keys(data):
    required = ["latitude", "longitude", "timezone", "hourly", "daily", "current"]
    missing = [k for k in required if k not in data]

    if missing:
        raise ValueError(f"Missing top-level keys: {missing}")

    logging.info("Top-level key validation passed.")


def validate_sections(data):
    if not data.get("hourly"):
        raise ValueError("Missing or empty 'hourly' section.")
    if not data.get("daily"):
        raise ValueError("Missing or empty 'daily' section.")
    if not data.get("current"):
        raise ValueError("Missing or empty 'current' section.")

    logging.info("Section completeness validation passed.")


def validate_non_empty_arrays(data):
    hourly_times = data.get("hourly", {}).get("time", [])
    daily_times = data.get("daily", {}).get("time", [])

    if not hourly_times:
        raise ValueError("Hourly time array is empty.")
    if not daily_times:
        raise ValueError("Daily time array is empty.")

    logging.info("Non-empty array validation passed.")


# =============================
# Save Raw JSON
# =============================
def save_raw_json(location_name, raw_json):
    safe = sanitize_filename(location_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    file_path = BRONZE_DIR / f"{safe}_raw_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(raw_json, f, indent=2)

    logging.info(f"Saved RAW Bronze JSON: {file_path}")


# =============================
# Main Runner
# =============================
def main():
    for location in CONFIG_LOCATIONS:
        data = fetch_raw_weather(location)

        # Validation phase
        validate_top_level_keys(data)
        validate_sections(data)
        validate_non_empty_arrays(data)

        # Store only valid data
        save_raw_json(location["name"], data)
        print("\n")


if __name__ == "__main__":
    main()
