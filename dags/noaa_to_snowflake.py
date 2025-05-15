from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
import requests
import pandas as pd
import snowflake.connector

# Station IDs - San Diego (9410230) and San Francisco (9414290)
NOAA_STATIONS = ["9410230", "9414290"]

# Data products to collect
NOAA_PRODUCTS = [
    "water_temperature",
    "wind",
    "water_level",
    "air_temperature",
    "conductivity"  # For salinity data
]

# Base NOAA API URL
NOAA_BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False, tags=["noaa", "snowflake"])
def noaa_to_snowflake():

    @task
    def fetch_data():
        all_data = []
        
        for station in NOAA_STATIONS:
            station_data = []
            for product in NOAA_PRODUCTS:
                # Construct the API URL for each station and product
                api_url = f"{NOAA_BASE_URL}?product={product}&station={station}&range=24&interval=h&datum=MLLW&units=english&time_zone=gmt&format=json"

                response = requests.get(api_url)
                response.raise_for_status()
                data = response.json()
                
                if "data" in data:
                    # Add station and product information to each record
                    for record in data["data"]:
                        record["station"] = station
                        record["product"] = product
                    station_data.extend(data["data"])
            
            all_data.extend(station_data)
        
        return all_data

    @task
    def transform_data(raw_data):
        df = pd.DataFrame(raw_data)
        
        # Standardize column names
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
    def load_to_snowflake(clean_data):
        # Get Snowflake connection details from Astro UI connection
        conn_info = BaseHook.get_connection("snowflake")
        conn = snowflake.connector.connect(
            user=conn_info.login,
            password=conn_info.password,
            account=conn_info.extra_dejson.get("account"),
            warehouse=conn_info.extra_dejson.get("warehouse"),
            database=conn_info.schema,
            schema=conn_info.extra_dejson.get("schema"),
        )

        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS noaa_marine_data (
                timestamp TIMESTAMP,
                station VARCHAR(10),
                water_temp FLOAT,
                air_temp FLOAT,
                water_level FLOAT,
                salinity FLOAT,
                wind_speed FLOAT,
                wind_direction FLOAT
            )
        """)

        # Dynamic insert statement that handles NULL values for any missing fields
        insert_stmt = """
            INSERT INTO noaa_marine_data 
            (timestamp, station, water_temp, air_temp, water_level, salinity, wind_speed, wind_direction) 
            VALUES (%(timestamp)s, %(station)s, %(water_temp)s, %(air_temp)s, %(water_level)s, 
                    %(salinity)s, %(wind_speed)s, %(wind_direction)s)
        """
        cursor.executemany(insert_stmt, clean_data)
        conn.commit()
        cursor.close()
        conn.close()

    raw = fetch_data()
    clean = transform_data(raw)
    load_to_snowflake(clean)

dag_instance = noaa_to_snowflake()