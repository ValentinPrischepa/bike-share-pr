from __future__ import annotations
from constants import (
    GCP_CONN_ID,
    GCP_PROJECT_ID,
    BQ_DATASET_NAME,
    BQ_HOLIDAYS_TABLE
)
import pendulum
from datetime import date
import requests
import json
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models.variable import Variable
import logging
from google.cloud import bigquery

holidays_start_year = int(Variable.get("holidays_start_year", default_var=2010))
holidays_end_year = int(Variable.get("holidays_end_year", default_var=date.today().year))

@dag(
    dag_id="holidays_dag",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bigquery", "holidays", "api"],
)
def create_holidays_and_weekends_table():
    """
    DAG to fetch holiday data for a range of years and create a BigQuery table
    with holiday and weekend flags.
    """
    @task
    def get_washington_dc_holidays_for_range(start_year: int, end_year: int):
        all_holidays = []
        for year in range(start_year, end_year + 1):
            holiday_api_url = f"https://date.nager.at/api/v3/publicholidays/{year}/US"
            logging.info(f"Fetching holidays for {year} in Washington US...") 
            response = requests.get(holiday_api_url)
            response.raise_for_status()
            holidays_data = response.json()

            holiday_list = [
                {"trip_date": h["date"], "holiday_name": h["name"]}
                for h in holidays_data
            ]
            all_holidays.extend(holiday_list)

        return json.dumps(all_holidays)
    
    @task
    def load_holidays_to_bigquery(holiday_data, start_year: int, end_year: int):
        """
        Upload holiday data to a temp table in BigQuery, then join with date array
        to create holidays + weekends table.
        """
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = hook.get_client()

        holidays = json.loads(holiday_data)

        # Define schema for the holidays temp table
        temp_table_id = f"{BQ_DATASET_NAME}._tmp_holidays"
        schema = [
            bigquery.SchemaField("trip_date", "DATE"),
            bigquery.SchemaField("holiday_name", "STRING"),
        ]

        # Upload JSON data to BigQuery temp table
        job = client.load_table_from_json(
            holidays,
            f"{GCP_PROJECT_ID}.{temp_table_id}",
            job_config=bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE"),
        )
        job.result()

        logging.info(f"Uploaded {len(holidays)} rows to {temp_table_id}")

        # Query to join temp table with generated date array
        insert_job_sql = f"""
            WITH all_dates AS (
                SELECT
                    date(d) AS trip_date
                FROM
                    UNNEST(GENERATE_DATE_ARRAY(DATE('{start_year}-01-01'), DATE('{end_year}-12-31'))) AS d
            )
            SELECT
                t1.trip_date,
                (EXTRACT(DAYOFWEEK FROM t1.trip_date) = 1 OR EXTRACT(DAYOFWEEK FROM t1.trip_date) = 7) as is_weekend,
                CASE WHEN t2.holiday_name IS NOT NULL THEN TRUE ELSE FALSE END as is_holiday,
                t2.holiday_name
            FROM
                all_dates AS t1
            LEFT JOIN
                `{GCP_PROJECT_ID}.{temp_table_id}` AS t2
            ON
                t1.trip_date = DATE(t2.trip_date)
            ORDER BY
                t1.trip_date
        """

        job_config = bigquery.QueryJobConfig(
            destination=f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_HOLIDAYS_TABLE}",
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )

        query_job = client.query(insert_job_sql, job_config=job_config)
        query_job.result()

        logging.info(f"Holidays and weekends table loaded: {BQ_HOLIDAYS_TABLE}")

    holidays_data_task = get_washington_dc_holidays_for_range(holidays_start_year, holidays_end_year)
    load_holidays_to_bigquery(holidays_data_task, holidays_start_year, holidays_end_year)

create_holidays_and_weekends_table()