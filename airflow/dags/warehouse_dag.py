import os
import subprocess
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "walid",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

GCP_BUCKET = os.environ.get("GCP_BUCKET_NAME", "olist-data-lake-491503")
GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "olist-pipeline-491503")
BQ_DATASET = os.environ.get("GCP_BQ_DATASET", "olist_dwh")

TABLES = [
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customers",
    "sellers",
    "products",
    "geolocation",
    "category_translation",
]


def run_spark():
    """Spark enforces schema on raw CSVs and writes Parquet to GCS."""
    import sys

    sys.path.insert(0, "/opt/airflow/spark/jobs")
    from process_olist import process_olist

    process_olist(GCP_BUCKET)


def load_to_bigquery():
    """Load Parquet files from GCS processed/ into BigQuery."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)

    for table in TABLES:
        gcs_uri = f"gs://{GCP_BUCKET}/processed/{table}/*.parquet"
        table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{table}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        if table == "orders":
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="order_purchase_timestamp",
            )
            job_config.clustering_fields = ["order_status"]

        print(f"Loading {table} into BigQuery...")
        load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()
        print(f"Loaded {table}")

    print("All tables loaded into BigQuery!")


def run_dbt():
    """Run dbt models and tests."""
    dbt_project_dir = "/opt/airflow/dbt/olist"

    print("Running dbt models...")
    result = subprocess.run(
        [
            "dbt",
            "run",
            "--project-dir",
            dbt_project_dir,
            "--profiles-dir",
            dbt_project_dir,
        ],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception("dbt run failed")

    print("Running dbt tests...")
    result = subprocess.run(
        [
            "dbt",
            "test",
            "--project-dir",
            dbt_project_dir,
            "--profiles-dir",
            dbt_project_dir,
        ],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception("dbt test failed")

    print("dbt completed successfully!")


with DAG(
    dag_id="olist_warehouse",
    default_args=default_args,
    description="Schema enforcement with Spark, load to BigQuery, transform with dbt",
    schedule_interval="@monthly",
    catchup=False,
    tags=["olist", "warehouse"],
) as dag:
    spark_task = PythonOperator(
        task_id="spark_schema_enforcement",
        python_callable=run_spark,
    )

    bq_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    dbt_task = PythonOperator(
        task_id="run_dbt",
        python_callable=run_dbt,
    )

    spark_task >> bq_task >> dbt_task
