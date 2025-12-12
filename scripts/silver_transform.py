"""
Silver Layer Transformation Script
----------------------------------

Purpose:
    Convert Bronze raw JSON weather files into cleaned, structured,
    analytics-ready Silver datasets.

What This Script Does:
    • Loads raw Bronze JSON files
    • Extracts CURRENT, HOURLY, DAILY weather data
    • Converts arrays → tabular DataFrames
    • Normalizes data types (datetime, floats, integers)
    • Adds metadata columns: location, load_timestamp
    • Saves Silver datasets in Parquet format

Output:
    data_lake/silver/current/<location>_current_<timestamp>.parquet
    data_lake/silver/hourly/<location>_hourly_<timestamp>.parquet
    data_lake/silver/daily/<location>_daily_<timestamp>.parquet
"""

import json
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
import re

# ===============================================
# Path Setup (Correct for your project structure)
# ===============================================
REPO_ROOT = Path(__file__).resolve().parents[1]

BRONZE_DIR = REPO_ROOT / "data_lake" / "bronze"

SILVER_BASE = REPO_ROOT / "data_lake" / "silver"
SILVER_CURRENT = SILVER_BASE / "current"
SILVER_HOURLY = SILVER_BASE / "hourly"
SILVER_DAILY = SILVER_BASE / "daily"

# Create folders if missing
for folder in [SILVER_BASE, SILVER_CURRENT, SILVER_HOURLY, SILVER_DAILY]:
    folder.mkdir(parents=True, exist_ok=True)

# =============================
# Logging
# =============================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# =============================
# Helper Functions
# =============================
def _sanitize_filename(name: str) -> str:
    """Convert location names to safe lowercase filenames."""
    name = (name or "location").lower().replace(" ", "_")
    return re.sub(r"[^a-z0-9_\-]", "", name)


def _validate_list_lengths(section: dict) -> bool:
    """Ensure all arrays in hourly/daily section have the same length."""
    lengths = [len(v) for v in section.values() if isinstance(v, list)]
    return len(set(lengths)) <= 1  # all equal or only 1 array


def load_bronze_files():
    """Return all JSON files from Bronze layer."""
    return sorted(BRONZE_DIR.glob("*.json"))


# =============================
# CURRENT Transform
# =============================
def transform_current(data: dict, location: str) -> pd.DataFrame:
    current = data.get("current", {})
    if not current:
        return pd.DataFrame()

    df = pd.DataFrame([current])

    # Convert timestamp
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Convert numerics
    for col in df.columns:
        if col == "time":
            continue
        try:
            df[col] = pd.to_numeric(df[col])
        except Exception:
            pass

    # Add metadata
    df["location"] = location
    df["load_timestamp"] = datetime.now()

    return df


# =============================
# HOURLY Transform
# =============================
def transform_hourly(data: dict, location: str) -> pd.DataFrame:
    hourly = data.get("hourly", {})
    if not hourly:
        return pd.DataFrame()

    if not _validate_list_lengths(hourly):
        logging.warning("Hourly list lengths mismatch for %s", location)

    # Construct DataFrame safely
    df = pd.DataFrame({k: pd.Series(v) for k, v in hourly.items()})

    # Timestamp
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Numeric conversion
    for col in df.columns:
        if col != "time":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["location"] = location
    df["load_timestamp"] = datetime.now()

    return df


# =============================
# DAILY Transform
# =============================
def transform_daily(data: dict, location: str) -> pd.DataFrame:
    daily = data.get("daily", {})
    if not daily:
        return pd.DataFrame()

    if not _validate_list_lengths(daily):
        logging.warning("Daily list lengths mismatch for %s", location)

    df = pd.DataFrame({k: pd.Series(v) for k, v in daily.items()})

    # Rename columns to business-friendly names
    df = df.rename(columns={
        "temperature_2m_max": "temp_max",
        "temperature_2m_min": "temp_min",
        "wind_speed_10m_max": "wind_speed_max",
        "precipitation_sum": "precip_total",
    })

    # Timestamp
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Numeric conversion
    for col in df.columns:
        if col != "time":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["location"] = location
    df["load_timestamp"] = datetime.now()

    return df


# =============================
# Save as Parquet
# =============================
def save_parquet(df: pd.DataFrame, folder: Path, prefix: str, location: str):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_loc = _sanitize_filename(location)

    file_path = folder / f"{safe_loc}_{prefix}_{timestamp}.parquet"

    # import pyarrow  # ensure parquet engine installed

    df.to_parquet(file_path, index=False, compression="snappy")

    logging.info("Saved Silver file → %s", file_path)


# =============================
# Per-File Processor
# =============================
def process_file(file_path: Path):
    location = file_path.name.split("_raw_")[0]

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    logging.info("Processing Bronze → Silver: %s", file_path.name)

    # Transform + Save
    df_current = transform_current(data, location)
    if not df_current.empty:
        save_parquet(df_current, SILVER_CURRENT, "current", location)

    df_hourly = transform_hourly(data, location)
    if not df_hourly.empty:
        save_parquet(df_hourly, SILVER_HOURLY, "hourly", location)

    df_daily = transform_daily(data, location)
    if not df_daily.empty:
        save_parquet(df_daily, SILVER_DAILY, "daily", location)


# =============================
# Runner
# =============================
def main():
    bronze_files = load_bronze_files()

    if not bronze_files:
        logging.warning("No bronze files found. Nothing to process.")
        return

    for file in bronze_files:
        process_file(file)


if __name__ == "__main__":
    main()
