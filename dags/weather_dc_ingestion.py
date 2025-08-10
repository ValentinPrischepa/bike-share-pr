from __future__ import annotations
import json
import logging
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import date, timedelta
from airflow.decorators import dag, task
from google.cloud import bigquery


import pendulum
import requests

from constants import BQ_DATASET_NAME, BQ_WEATHER_TABLE, GCP_CONN_ID, GCP_PROJECT_ID, LATITUDE_DC, LONGITUDE_DC, WEATHER_API_BASE_URL

WEATHER_START_YEAR = int(Variable.get("weather_start_year", default_var=2010))
WEATHER_END_YEAR = date.today().year

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

@dag(
    dag_id="weather_dc_ingestion",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bigquery", "weather", "api"],
)
def weather_dc_ingestion():
    """
    DAG to fetch historical daily weather data for Washington DC starting from 2010.
    Supports incremental loads and full reload via Airflow Variable.
    """
    
    @task
    def fetch_weather_data(full_reload: bool):
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = hook.get_client()

        if not full_reload:
            query = f"SELECT MAX(date) as last_date FROM `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_WEATHER_TABLE}`"
            result = list(client.query(query).result())
            last_date = result[0].last_date if result and result[0].last_date else date(2010, 1, 1)
            start_date = last_date + timedelta(days=1)
        else:
            start_date = date(Variable.get("weather_start_date", default_var=2010), 1, 1)

        end_date = date.today()
        all_weather = []

        current_date = start_date
        while current_date <= end_date:
            chunk_end = min(current_date.replace(month=12, day=31), end_date)
            params = {
                "latitude": LATITUDE_DC,
                "longitude": LONGITUDE_DC,
                "start_date": current_date.strftime("%Y-%m-%d"),
                "end_date": chunk_end.strftime("%Y-%m-%d"),
                "daily": [
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "precipitation_sum",
                    "windspeed_10m_max"
                ],
                "timezone": "America/New_York"
            }

            logging.info(f"Fetching weather data {current_date} to {chunk_end}...")
            response = requests.get(url=WEATHER_API_BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            dates = data.get("daily", {}).get("time", [])
            temps_max = data["daily"].get("temperature_2m_max", [])
            temps_min = data["daily"].get("temperature_2m_min", [])
            precip = data["daily"].get("precipitation_sum", [])
            wind = data["daily"].get("windspeed_10m_max", [])

            for i in range(len(dates)):
                all_weather.append({
                    "date": dates[i],
                    "temperature_max": temps_max[i],
                    "temperature_min": temps_min[i],
                    "precipitation_sum": precip[i],
                    "windspeed_max": wind[i]
                })
            current_date = chunk_end + timedelta(days=1)
        return all_weather
    
    @task
    def load_weather_data_to_bq(weather_data, full_reload: bool):
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = hook.get_client()

        schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("temperature_max", "FLOAT"),
            bigquery.SchemaField("temperature_min", "FLOAT"),
            bigquery.SchemaField("precipitation_sum", "FLOAT"),
            bigquery.SchemaField("windspeed_max", "FLOAT"),
        ]

        write_disposition = "WRITE_TRUNCATE" if full_reload else "WRITE_APPEND"

        job = client.load_table_from_json(
            weather_data,
            f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_WEATHER_TABLE}",
            job_config=bigquery.LoadJobConfig(schema=schema, write_disposition=write_disposition),
        )

        job.result()
    
    full_reload_flag = Variable.get("weather_full_reload", default_var="false").lower() == "true"
    weather_data = fetch_weather_data(full_reload_flag)
    load_weather_data_to_bq(weather_data, full_reload_flag)

weather_dc_ingestion()


