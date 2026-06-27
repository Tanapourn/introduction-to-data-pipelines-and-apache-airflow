from airflow import DAG
from airflow.utils import timezone
from airflow.providers.standard.operators.python import PythonOperator


CITIES = {
    "Tokyo": {
        "latitude": 35.6762,
        "longitude": 139.6503,
    },
    "Osaka": {
        "latitude": 34.6937,
        "longitude": 135.5023,
    },
    "Sapporo": {
        "latitude": 43.0618,
        "longitude": 141.3545,
    },
}


def _get_japan_weather_data(**context):
    ds = context["ds"]

    import requests
    import json
    import os

    all_weather_data = {}

    for city, location in CITIES.items():
        latitude = location["latitude"]
        longitude = location["longitude"]

        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={latitude}"
            f"&longitude={longitude}"
            "&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum"
            f"&start_date={ds}"
            f"&end_date={ds}"
            "&timezone=Asia%2FTokyo"
        )

        response = requests.get(url)
        data = response.json()

        all_weather_data[city] = data

    output_path = f"/opt/airflow/dags/{ds}-japan-weather-output.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_weather_data, f, ensure_ascii=False, indent=2)

    print(f"Saved weather data to {output_path}")


def _check_weather_key_exists(**context):
    ds = context["ds"]

    import json

    with open(f"/opt/airflow/dags/{ds}-japan-weather-output.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    for city, weather_data in data.items():
        assert "daily" in weather_data
        assert "time" in weather_data["daily"]
        assert "temperature_2m_max" in weather_data["daily"]
        assert "temperature_2m_min" in weather_data["daily"]
        assert "temperature_2m_mean" in weather_data["daily"]

    print("Weather data quality check passed")


def _transform_weather_to_csv(**context):
    import csv
    import json
    import glob
    import os

    pattern = os.path.join("/opt/airflow/dags", "*-japan-weather-output.json")
    files = sorted(glob.glob(pattern))

    csv_path = "/opt/airflow/dags/japan_weather_data.csv"

    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        writer.writerow([
            "date",
            "city",
            "temperature_max",
            "temperature_min",
            "temperature_mean",
            "precipitation_sum",
        ])

        for file in files:
            with open(file, "r", encoding="utf-8") as fh:
                data = json.load(fh)

            for city, weather_data in data.items():
                daily = weather_data["daily"]

                writer.writerow([
                    daily["time"][0],
                    city,
                    daily["temperature_2m_max"][0],
                    daily["temperature_2m_min"][0],
                    daily["temperature_2m_mean"][0],
                    daily["precipitation_sum"][0],
                ])

    print(f"Saved CSV file to {csv_path}")


def _calculate_data_quality_score(**context):
    import json
    import glob
    import os

    pattern = os.path.join("/opt/airflow/dags", "*-japan-weather-output.json")
    files = sorted(glob.glob(pattern))

    total_records = 0
    complete_records = 0

    for file in files:
        with open(file, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        for city, weather_data in data.items():
            total_records += 1
            daily = weather_data.get("daily", {})

            if (
                daily.get("time")
                and daily.get("temperature_2m_max")
                and daily.get("temperature_2m_min")
                and daily.get("temperature_2m_mean")
            ):
                complete_records += 1

    if total_records == 0:
        score = 0
    else:
        score = complete_records / total_records

    print(f"Data Quality Score: {score}")


def _find_average_temperature_in_duckdb(**context):
    import duckdb

    conn = duckdb.connect(database="/opt/airflow/dags/japan_weather.db")

    csv_file_path = "/opt/airflow/dags/japan_weather_data.csv"

    conn.execute(f"""
        CREATE OR REPLACE TABLE japan_weather_data AS
        SELECT * FROM read_csv_auto('{csv_file_path}')
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW avg_temperature_by_city AS
        SELECT
            city,
            AVG(temperature_mean) AS average_temperature
        FROM japan_weather_data
        GROUP BY city
    """)

    conn.execute("""
        CREATE OR REPLACE VIEW avg_temperature_by_month AS
        SELECT
            city,
            strftime(CAST(date AS DATE), '%Y-%m') AS month,
            AVG(temperature_mean) AS average_temperature
        FROM japan_weather_data
        GROUP BY city, month
    """)

    print("Created DuckDB table and views successfully")


with DAG(
    "japan_weather_dag",
    start_date=timezone.datetime(2026, 6, 6),
    schedule="@daily",
    catchup=True,
):

    get_japan_weather_data = PythonOperator(
        task_id="get_japan_weather_data",
        python_callable=_get_japan_weather_data,
    )

    check_weather_key_exists = PythonOperator(
        task_id="check_weather_key_exists",
        python_callable=_check_weather_key_exists,
    )

    transform_weather_to_csv = PythonOperator(
        task_id="transform_weather_to_csv",
        python_callable=_transform_weather_to_csv,
    )

    calculate_data_quality_score = PythonOperator(
        task_id="calculate_data_quality_score",
        python_callable=_calculate_data_quality_score,
    )

    find_average_temperature_in_duckdb = PythonOperator(
        task_id="find_average_temperature_in_duckdb",
        python_callable=_find_average_temperature_in_duckdb,
    )

    get_japan_weather_data >> check_weather_key_exists
    check_weather_key_exists >> calculate_data_quality_score
    check_weather_key_exists >> transform_weather_to_csv >> find_average_temperature_in_duckdb
