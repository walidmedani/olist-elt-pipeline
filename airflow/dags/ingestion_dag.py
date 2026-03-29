import os
import kaggle
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

default_args = {
    "owner": "walid",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

GCP_BUCKET = os.environ.get("GCP_BUCKET_NAME", "olist-data-lake-491503")
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
DATA_DIR = "/tmp/olist_data"


def download_and_upload():
    """Download from Kaggle and upload CSVs directly to GCS."""
    # Download from Kaggle
    os.makedirs(DATA_DIR, exist_ok=True)
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(KAGGLE_DATASET, path=DATA_DIR, unzip=True)
    print(f"Downloaded files: {os.listdir(DATA_DIR)}")

    # Upload each CSV to GCS
    client = storage.Client()
    bucket = client.bucket(GCP_BUCKET)

    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".csv"):
            local_path = os.path.join(DATA_DIR, filename)
            gcs_path = f"raw/{filename}"
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            print(f"Uploaded {filename} to gs://{GCP_BUCKET}/{gcs_path}")

    print("All files uploaded to GCS successfully!")


def cleanup():
    """Remove local files after upload."""
    import shutil

    shutil.rmtree(DATA_DIR, ignore_errors=True)
    print("Cleanup done.")


with DAG(
    dag_id="olist_ingestion",
    default_args=default_args,
    description="Download Olist CSVs from Kaggle and upload to GCS",
    schedule_interval="@monthly",
    catchup=False,
    tags=["olist", "ingestion"],
) as dag:
    upload_task = PythonOperator(
        task_id="download_and_upload_to_gcs",
        python_callable=download_and_upload,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
    )

    upload_task >> cleanup_task
