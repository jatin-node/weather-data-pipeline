# Weather Data Pipeline

A simple project to collect and process weather data from the Open-Meteo API using Python and Apache Airflow. It organizes data into Bronze, Silver, and Gold layers for better analytics.

## What It Does

- **Bronze Layer**: Gets raw weather data (current, hourly, daily) from the internet and saves it as JSON files.
- **Silver Layer**: Cleans and organizes the data into Parquet files (efficient for analytics) with proper types and structure.
- **Gold Layer**: Creates summary reports and aggregated data for easy analysis.
- Uses Apache Airflow to schedule and run the pipeline automatically.

## Quick Start

1. **Install Python** (if not already): Download from python.org.

2. **Clone the project**:
   ```bash
   git clone https://github.com/jatin-node/weather-data-pipeline.git
   cd weather-data-pipeline
   ```

3. **Set up environment**:
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   source venv/bin/activate  # Mac/Linux
   pip install -r requirements.txt
   ```

4. **Run the pipeline**:
   ```bash
   python scripts/test_api_call.py  # Get data
   python scripts/silver_transform.py  # Clean data
   python scripts/gold_transform.py  # Make reports
   ```

## Project Structure

```
weather-data-pipeline/
├── README.md                 # This file
├── requirements.txt          # List of needed packages
├── config/
│   ├── api_settings.json     # What weather data to get
│   └── locations.json        # Cities to get weather for
├── scripts/
│   ├── test_api_call.py      # Script to get weather data
│   ├── silver_transform.py   # Script to clean data
│   └── gold_transform.py     # Script to make summaries
├── data_lake/                # Where data is stored
│   ├── bronze/               # Raw JSON files
│   ├── silver/               # Clean Parquet files
│   │   ├── current/
│   │   ├── hourly/
│   │   └── daily/
│   └── gold/                 # Summary Parquet files
└── airflow/                  # New Airflow folder
    └── dags/
        └── weather_data_pipeline.py
```

## Need Help?

- Check the config files to add your own cities.
- Run each script one by one to see what happens.
- If stuck, look at the code in scripts/ folder.
