"""
Gold Layer Transformation Script
----------------------------------

Purpose:
    Produce business-ready analytical datasets from Silver weather data.

Gold Outputs:
    • daily_summary  → aggregated weather metrics per location/day
    • alerts         → rule-based weather risk classification
    • features       → ML-ready engineered features

Output files:
    data_lake/gold/daily_summary/<location>_daily_summary_<timestamp>.parquet
    data_lake/gold/alerts/<location>_alerts_<timestamp>.parquet
    data_lake/gold/features/<location>_features_<timestamp>.parquet
"""

import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------
# Path Setup
# ---------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]

SILVER_HOURLY = REPO_ROOT / "data_lake" / "silver" / "hourly"
SILVER_DAILY = REPO_ROOT / "data_lake" / "silver" / "daily"

GOLD_BASE = REPO_ROOT / "data_lake" / "gold"
GOLD_DAILY = GOLD_BASE / "daily_summary"
GOLD_ALERTS = GOLD_BASE / "alerts"
GOLD_FEATURES = GOLD_BASE / "features"

for f in [GOLD_BASE, GOLD_DAILY, GOLD_ALERTS, GOLD_FEATURES]:
    f.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ---------------------------------------------------------
# Helper
# ---------------------------------------------------------
def load_silver(folder: Path):
    return sorted(folder.glob("*.parquet"))

def save_gold(df, folder: Path, prefix: str, location: str):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = folder / f"{location}_{prefix}_{timestamp}.parquet"
    df.to_parquet(file_path, index=False)
    logging.info(f"Saved Gold file → {file_path}")

# ---------------------------------------------------------
# 1. Daily Summary
# ---------------------------------------------------------
def build_daily_summary(df_daily: pd.DataFrame):
    df = df_daily.copy()

    summary = df.groupby(["time", "location"]).agg(
        avg_temp=("temp_max", "mean"),
        max_temp=("temp_max", "max"),
        min_temp=("temp_min", "min"),
        total_precip=("precip_total", "sum"),
        max_wind_speed=("wind_speed_max", "max"),
    ).reset_index()

    summary["weather_label"] = summary["avg_temp"].apply(
        lambda x: "hot" if x > 35 else ("cold" if x < 10 else "moderate")
    )
    summary["load_timestamp"] = datetime.now()

    return summary

# ---------------------------------------------------------
# 2. Weather Alerts Dataset
# ---------------------------------------------------------
def build_alerts(df_daily: pd.DataFrame):

    alerts = pd.DataFrame()
    alerts["date"] = df_daily["time"]
    alerts["location"] = df_daily["location"]

    alerts["heat_alert_flag"] = (df_daily["temp_max"] > 35).astype(int)
    alerts["storm_flag"] = (df_daily["wind_speed_max"] > 50).astype(int)
    alerts["rain_alert_flag"] = (df_daily["precip_total"] > 20).astype(int)

    alerts["risk_level"] = alerts.apply(
        lambda row: (
            "high" if row["storm_flag"] == 1 else
            "moderate" if row["rain_alert_flag"] == 1 else
            "low"
        ),
        axis=1
    )

    alerts["load_timestamp"] = datetime.now()
    return alerts

# ---------------------------------------------------------
# 3. ML Features Dataset
# ---------------------------------------------------------
def build_features(df_daily: pd.DataFrame):

    df = df_daily.copy()

    df["temperature_range"] = df["temp_max"] - df["temp_min"]
    df["humidity_index"] = df["temp_max"] * 0.1 + df["precip_total"] * 0.5
    df["wind_chill"] = df["temp_min"] - (df["wind_speed_max"] * 0.1)

    df["load_timestamp"] = datetime.now()

    return df

# ---------------------------------------------------------
# Main Processor
# ---------------------------------------------------------
def process_gold():

    daily_files = load_silver(SILVER_DAILY)

    if not daily_files:
        logging.warning("No Silver DAILY files found. Gold layer skipped.")
        return

    for file in daily_files:
        logging.info(f"Processing Gold for → {file.name}")

        df_daily = pd.read_parquet(file)
        location = df_daily["location"].iloc[0]

        # Build datasets
        daily_summary = build_daily_summary(df_daily)
        alerts = build_alerts(df_daily)
        features = build_features(df_daily)

        # Save to gold
        save_gold(daily_summary, GOLD_DAILY, "daily_summary", location)
        save_gold(alerts, GOLD_ALERTS, "alerts", location)
        save_gold(features, GOLD_FEATURES, "features", location)


if __name__ == "__main__":
    process_gold()
