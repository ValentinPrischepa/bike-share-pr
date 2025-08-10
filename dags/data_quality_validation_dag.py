from __future__ import annotations
import json
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.smtp.operators.smtp import EmailOperator
import logging
import pandas as pd
from sklearn.ensemble import IsolationForest

from constants import GCP_CONN_ID, GCP_PROJECT_ID, BQ_DATASET_NAME


EMAIL_SUBJECT = "Data Quality Validation Report"

TABLES_CONFIG = {
    "weather": {
        "table_name": "weather_daily_dc",
        "date_col": "date",
    },
    "bike_rentals": {
        "table_name": "daily_trip_counts",
        "date_col": "trip_date",
    },
    "holidays": {
        "table_name": "holidays_and_weekends",
        "date_col": "trip_date",
    },
}

@dag(
    dag_id="data_quality_validation_dag",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["validation", "bigquery", "ml"],
)
def data_quality_validation(
    run_missing_date_check: bool = True,
    run_null_check: bool = True,
    run_anomaly_check: bool = True,
):

    def check_missing_dates(df, date_col, table_name, warnings):
        min_date = df[date_col].min()
        max_date = df[date_col].max()
        all_dates = pd.date_range(start=min_date, end=max_date)
        missing_dates = all_dates.difference(pd.to_datetime(df[date_col]))
        if not missing_dates.empty:
            msg = f"[{table_name}] Missing dates: {missing_dates.tolist()}"
            logging.warning(msg)
            warnings.append(msg)

    def check_nulls(df, table_name, warnings):
        null_summary = df.isnull().sum()
        null_cols = null_summary[null_summary > 0]
        if not null_cols.empty:
            msg = f"[{table_name}] Null values in columns:\n{null_cols.to_dict()}"
            logging.warning(msg)
            warnings.append(msg)

    def run_anomaly_detection(df, table_name, warnings):
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if not numeric_cols:
            return
        iso_forest = IsolationForest(contamination=0.01, random_state=42)
        preds = iso_forest.fit_predict(df[numeric_cols].fillna(0))
        anomalies = df[preds == -1]
        if not anomalies.empty:
            msg = f"[{table_name}] Anomalies detected ({len(anomalies)} rows)"
            logging.warning(msg)
            warnings.append(msg)

    @task
    def validate_table(table_key: str):
        config = TABLES_CONFIG[table_key]
        hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client = hook.get_client()

        # Note: no project ID in the query string
        sql_query = f"SELECT * FROM `{BQ_DATASET_NAME}.{config['table_name']}`"
        logging.info(f"Running query: {sql_query}")
        query_job = client.query(sql_query)
        results = query_job.result()
        rows = [dict(row) for row in results]

        # Convert to pandas DataFrame
        df = pd.DataFrame(rows)

        warnings = []
        if run_missing_date_check:
            check_missing_dates(df, config["date_col"], config["table_name"], warnings)
        if run_null_check:
            check_nulls(df, config["table_name"], warnings)
        if run_anomaly_check:
            run_anomaly_detection(df, config["table_name"], warnings)

        return warnings
    
    @task
    def compile_warnings(*warning_lists):
        all_warnings = [w for lst in warning_lists for w in lst]
        if not all_warnings:
            return "No data quality issues found."
        return "\n".join(all_warnings)

    results = [validate_table(tbl) for tbl in TABLES_CONFIG.keys()]
    compile_warnings(*results)

data_quality_validation()