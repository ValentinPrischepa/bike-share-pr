from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocDeleteClusterOperator, DataprocSubmitJobOperator
from airflow.models import Variable
import pendulum

from constants import GCP_CONN_ID, GCP_PROJECT_ID

REGION = "us-east1"
CLUSTER_NAME = "dataproc-ml-cluster"
BUCKET_NAME = "airflow_pr"  # Replace with your GCS bucket
PYSPARK_URI = f"gs://{BUCKET_NAME}/spark/jobs/bikerent/daily_features_processing.py"
CLUSTER_CONFIG = {
    "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
    "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
}

PYSPARK_JOB = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

with DAG(
    "join_tables_dataproc",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dataproc", "spark", "bq", "ml"],
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id=GCP_CONN_ID,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id=GCP_PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB,
        gcp_conn_id=GCP_CONN_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=GCP_PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule="all_done",  # ensures cluster is deleted even if job fails
        gcp_conn_id=GCP_CONN_ID,
    )

    create_cluster >> submit_job >> delete_cluster # type: ignore
