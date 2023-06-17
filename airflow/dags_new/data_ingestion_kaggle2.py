import os
import logging
from concurrent.futures import ThreadPoolExecutor
import glob
import zipfile

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq


# Fetch project ID and bucket name from environment variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "airline-delay-analysis"
dataset_url = f"kaggle datasets download -d sherrytp/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'project1_data')


def format_to_parquet(src_file):
    # Check if source file is a zip file
    if src_file.endswith('.zip'):
        # Create a ZipFile object
        with zipfile.ZipFile(src_file, 'r') as zip_ref:
            # Extract all files to the same directory as the source file
            zip_ref.extractall(os.path.dirname(src_file))
            # Replace the .zip extension with .csv for the source file
            src_file = src_file.replace('.zip', '.csv')
    
    # Existing CSV to Parquet conversion logic
    if src_file.endswith('.csv'):
        table = pv.read_csv(src_file)
        pq.write_table(table, src_file.replace('.csv', '.parquet'))
    else:
        logging.error("Can only accept source files in CSV format, for the moment")


def upload_directory_to_gcs(bucket_name, source_directory):
    """
    Uploads all files from a local directory to a GCS bucket.
    """
    # Workaround to prevent timeout for files > 6 MB on 800 kbps upload speed
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # create a thread pool of 4 threads
    with ThreadPoolExecutor(max_workers=4) as executor:
        for filename in os.listdir(source_directory):
            local_file = os.path.join(source_directory, filename)
            executor.submit(upload_file, bucket, filename, local_file)


def upload_file(bucket, object_name, local_file):
    """
    Uploads a file to a GCS bucket.
    """
    object_name = f"raw/{object_name}"
    local_file = f""
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"{dataset_url} -p {path_to_local_home}/{dataset_file} && unzip {path_to_local_home}/{dataset_file}/{dataset_file}.zip -d {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_directory_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET,
            "source_directory": f"{path_to_local_home}/{dataset_file}",
        },
    )

    # Initialise a client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(BUCKET)

    # Get a list of blobs in the "raw" folder of the bucket
    blobs = bucket.list_blobs(prefix="raw/")

    # Filter out blobs based on whether they're a file
    files = [blob for blob in blobs if blob.size > 0]

    # Create a list of URIs
    source_uris = [f"gs://{BUCKET}/{file.name}" for file in files]

    # BigQuery operator
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": source_uris,
            },
        },
    )

    # Define dependencies
    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task
