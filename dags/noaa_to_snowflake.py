from airflow.decorators import dag, task
from datetime import datetime
import requests
import pandas as pd
import logging

# Station IDs - San Diego (9410230) and San Francisco (9414290)
NOAA_STATIONS = ["9410230", "9414290"]

# testing dev deployments

# Data products to collect
NOAA_PRODUCTS = [
    "water_temperature",
    "wind",
    "water_level",
    "air_temperature",
    "conductivity"
]

# Base NOAA API URL
NOAA_BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False, tags=["noaa", "demo"])
def noaa_demo_dag():

    @task
    def fetch_data():
        all_data = []

        for station in NOAA_STATIONS:
            station_data = []
            for product in NOAA_PRODUCTS:
                api_url = f"{NOAA_BASE_URL}?product={product}&station={station}&range=24&interval=h&datum=MLLW&units=english&time_zone=gmt&format=json"
                response = requests.get(api_url)
                response.raise_for_status()
                data = response.json()

                if "data" in data:
                    for record in data["data"]:
                        record["station"] = station
                        record["product"] = product
                    station_data.extend(data["data"])

            all_data.extend(station_data)

        return all_data

    @task
    def transform_data(raw_data):
        df = pd.DataFrame(raw_data)
        df.rename(columns={"t": "timestamp", "v": "value"}, inplace=True)
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%S")

        def transform_values(row):
            product = row["product"]
            value = row["value"]

            if product == "water_temperature":
                row["water_temp"] = value
            elif product == "air_temperature":
                row["air_temp"] = value
            elif product == "water_level":
                row["water_level"] = value
            elif product == "conductivity":
                row["salinity"] = value
            elif product == "wind":
                row["wind_speed"] = row.get("s", None)
                row["wind_direction"] = row.get("d", None)

            return row

        df = df.apply(transform_values, axis=1)
        columns_to_keep = ["timestamp", "station", "water_temp", "air_temp", "water_level", "salinity", "wind_speed", "wind_direction"]
        df = df[[col for col in columns_to_keep if col in df.columns]]

        return df.to_dict(orient="records")

    @task
    def log_data_summary(clean_data):
        logger = logging.getLogger("airflow.task")
        total = len(clean_data)
        stations = set([rec["station"] for rec in clean_data if "station" in rec])
        logger.info(f"✅ NOAA data processing complete.")
        logger.info(f"Total records: {total}")
        logger.info(f"Stations: {', '.join(stations)}")
        sample = clean_data[0] if clean_data else {}
        logger.info(f"Sample record: {sample}")

    raw = fetch_data()
    clean = transform_data(raw)
    log_data_summary(clean)

dag_instance = noaa_demo_dag()