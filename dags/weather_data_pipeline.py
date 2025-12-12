"""
Airflow DAG — Weather Data Pipeline (Bronze -> Silver -> Gold)

Modern TaskFlow API design (Airflow 2.7+ compatible).
This DAG imports and calls the Python functions already implemented
in your external project mounted at /opt/airflow/projects/weather-data-pipeline.

Place this file into: /home/jatin/AirFlow/dags/
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys
import logging
from pathlib import Path

# ---------------------------
# Configuration
# ---------------------------
PROJECT_ROOT_CONTAINER = "/opt/airflow/projects/weather-data-pipeline"
SCRIPTS_DIR = f"{PROJECT_ROOT_CONTAINER}/scripts"

# Add project to python path so we can import modules
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

# Import your scripts as modules (they must be import-safe i.e. not run heavy code at import)
try:
    import test_api_call   # expects test_api_call.main() available
    import silver_transform
    import gold_transform
except Exception as e:
    # If imports fail at DAG parse time, raise a clear error in logs
    logging.exception("Failed to import project scripts. Ensure project is mounted and path is correct.")
    raise

DEFAULT_ARGS = {
    "owner": "jatin",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    # set sensible execution timeout if desired:
    # "execution_timeout": timedelta(minutes=30),
}

@dag(
    dag_id="weather_data_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  # run daily; change as needed
    catchup=False,
    tags=["weather", "etl", "medallion"],
)
def weather_pipeline():
    """
    Single DAG with three dependent tasks:
      1) extract_bronze  -> calls test_api_call.main()
      2) transform_silver -> calls silver_transform.main()
      3) transform_gold   -> calls gold_transform.process_gold()
    """

    @task(task_id="extract_bronze")
    def extract_bronze_task():
        """
        Run Bronze extraction. Expected function: test_api_call.main()
        This function should raise on error so Airflow marks the task as failed.
        """
        if not Path(SCRIPTS_DIR, "test_api_call.py").exists():
            raise FileNotFoundError(f"Script missing inside container: {SCRIPTS_DIR}/test_api_call.py")

        # Call your existing main() — it does the fetching + save to bronze
        try:
            # prefer explicit main() call; adjust if your script exposes different API
            return test_api_call.main()
        except Exception as exc:
            logging.exception("extract_bronze failed")
            raise

    @task(task_id="transform_silver")
    def transform_silver_task():
        """
        Run Bronze -> Silver transform. Expected function: silver_transform.main()
        """
        if not Path(SCRIPTS_DIR, "silver_transform.py").exists():
            raise FileNotFoundError(f"Script missing inside container: {SCRIPTS_DIR}/silver_transform.py")
        try:
            return silver_transform.main()
        except Exception:
            logging.exception("transform_silver failed")
            raise

    @task(task_id="transform_gold")
    def transform_gold_task():
        """
        Run Silver -> Gold transform. Expected function: gold_transform.process_gold()
        """
        if not Path(SCRIPTS_DIR, "gold_transform.py").exists():
            raise FileNotFoundError(f"Script missing inside container: {SCRIPTS_DIR}/gold_transform.py")
        try:
            # gold_transform defines process_gold() — call it
            return gold_transform.process_gold()
        except Exception:
            logging.exception("transform_gold failed")
            raise

    # orchestration: bronze -> silver -> gold
    bronze_result = extract_bronze_task()
    silver_result = transform_silver_task()
    gold_result = transform_gold_task()

    bronze_result >> silver_result >> gold_result


dag = weather_pipeline()
