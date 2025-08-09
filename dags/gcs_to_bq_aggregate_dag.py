from __future__ import annotations
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
)
import pendulum
from constants import (
    GCP_CONN_ID,
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    BQ_DATASET_NAME,
    BQ_DAILY_TRIP_COUNT_TABLE_NAME,
    BQ_EXTERNAL_TABLE_NAME
)

AGGREGATION_SQL = f"""
    SELECT
        DATE(start_at) as trip_date,
        COUNT(*) as daily_trip_count
    FROM
        `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_EXTERNAL_TABLE_NAME}`
    GROUP BY
        trip_date
    ORDER BY
        trip_date
"""

with DAG(
    dag_id="gcs_to_bq_aggregate_dag",
    schedule=None,  # Set a schedule, e.g., "@daily"
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["gcs", "bigquery", "data_aggregation"],
) as dag:
    
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id='create_external_table',
        gcp_conn_id=GCP_CONN_ID,
        table_resource={
            "tableReference": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": BQ_DATASET_NAME,
                "tableId": BQ_EXTERNAL_TABLE_NAME
            },
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
                "sourceUris": [
                    f"gs://{GCS_BUCKET_NAME}/*.csv.gz"
                ],
                "csvOptions": {
                    "skipLeadingRows": 1,
                    "allowJaggedRows": True,
                },
                "autodetect": True,
            },
        },

    )

    aggregate_and_load = BigQueryInsertJobOperator(
        task_id="aggregate_and_load",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {
                "query": AGGREGATION_SQL,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET_NAME,
                    "tableId": BQ_DAILY_TRIP_COUNT_TABLE_NAME,
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE", # This will overwrite the table each time
            }
        },
    )

    create_external_table >> aggregate_and_load # type: ignore