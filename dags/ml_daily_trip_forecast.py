from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ---- Constants (from your constants.py) ----
from constants import (
    GCP_CONN_ID,      # e.g., "google_cloud_default"
    GCP_PROJECT_ID,   # e.g., "coinproject-463620"
    BQ_DATASET_NAME,  # e.g., "ml_dataset"
    BQ_TRAIN_DATASET  # e.g., "features_dataset"
)

TABLE = "daily_features"
MODEL_NAME = "daily_trip_model"
EVAL_RESULTS_TABLE = "daily_trip_model_evaluation"
PREDICTIONS_TABLE = "daily_trip_predictions"

# ---- Configurable Airflow Variables ----
CUTOFF_DATE = Variable.get("model_cutoff_date", default_var="2025-05-01")
PREDICT_START = Variable.get("predict_start", default_var="2025-08-01")
PREDICT_END = Variable.get("predict_end", default_var="2026-08-01")


@dag(
    dag_id="ml_daily_trip_forecast",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bigquery", "ml"],
)
def bq_ml_pipeline():
    # ---- 1. Train model ----
    train_sql = f"""
    CREATE OR REPLACE MODEL `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{MODEL_NAME}`
    OPTIONS(
    model_type='linear_reg',
    input_label_cols=['daily_trip_count']
    ) AS
    SELECT
      temperature_max,
      temperature_min,
      precipitation_sum,
      windspeed_max,
      is_weekend AS is_weekend,
      is_holiday AS is_holiday,
      daily_trip_count
    FROM `{GCP_PROJECT_ID}.{BQ_TRAIN_DATASET}.{TABLE}`
    WHERE date < DATE('{CUTOFF_DATE}');
    """

    train_model = BigQueryInsertJobOperator(
        task_id="train_model",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": train_sql,
                "useLegacySql": False,
            }
        },
    )

    # ---- 2. Evaluate model ----
    eval_sql = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{EVAL_RESULTS_TABLE}` AS
    SELECT *
    FROM ML.EVALUATE(
      MODEL `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{MODEL_NAME}`,
      (
        SELECT
          temperature_max,
          temperature_min,
          precipitation_sum,
          windspeed_max,
          is_weekend AS is_weekend,
          is_holiday AS is_holiday,
          daily_trip_count
        FROM `{GCP_PROJECT_ID}.{BQ_TRAIN_DATASET}.{TABLE}`
        WHERE date >= DATE('{CUTOFF_DATE}')
      )
    );
    """

    evaluate_model = BigQueryInsertJobOperator(
        task_id="evaluate_model",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": eval_sql,
                "useLegacySql": False,
            }
        },
    )

    # ---- 3. Predict future ----
    predict_sql = f"""
    CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{PREDICTIONS_TABLE}` AS
    SELECT
      date,
      predicted_daily_trip_count
    FROM ML.PREDICT(
      MODEL `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{MODEL_NAME}`,
      (
        SELECT
          date,
          temperature_max,
          temperature_min,
          precipitation_sum,
          windspeed_max,
          is_weekend AS is_weekend,
          is_holiday AS is_holiday
        FROM `{GCP_PROJECT_ID}.{BQ_TRAIN_DATASET}.{TABLE}`
        WHERE date BETWEEN DATE('{PREDICT_START}') AND DATE('{PREDICT_END}')
      )
    );
    """

    predict_future = BigQueryInsertJobOperator(
        task_id="predict_future",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": predict_sql,
                "useLegacySql": False,
            }
        },
    )

    # ---- Task chaining ----
    train_model >> evaluate_model >> predict_future  # type: ignore


bq_ml_pipeline()
