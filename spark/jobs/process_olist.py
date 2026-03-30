import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)


def create_spark_session():
    return (
        SparkSession.builder.appName("OlistSchemaEnforcement")
        .master("local[*]")
        .getOrCreate()
    )


SCHEMAS = {
    "olist_orders_dataset.csv": StructType(
        [
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("order_status", StringType()),
            StructField("order_purchase_timestamp", TimestampType()),
            StructField("order_approved_at", TimestampType()),
            StructField("order_delivered_carrier_date", TimestampType()),
            StructField("order_delivered_customer_date", TimestampType()),
            StructField("order_estimated_delivery_date", TimestampType()),
        ]
    ),
    "olist_order_items_dataset.csv": StructType(
        [
            StructField("order_id", StringType()),
            StructField("order_item_id", IntegerType()),
            StructField("product_id", StringType()),
            StructField("seller_id", StringType()),
            StructField("shipping_limit_date", TimestampType()),
            StructField("price", DoubleType()),
            StructField("freight_value", DoubleType()),
        ]
    ),
    "olist_order_payments_dataset.csv": StructType(
        [
            StructField("order_id", StringType()),
            StructField("payment_sequential", IntegerType()),
            StructField("payment_type", StringType()),
            StructField("payment_installments", IntegerType()),
            StructField("payment_value", DoubleType()),
        ]
    ),
    "olist_order_reviews_dataset.csv": StructType(
        [
            StructField("review_id", StringType()),
            StructField("order_id", StringType()),
            StructField("review_score", IntegerType()),
            StructField("review_comment_title", StringType()),
            StructField("review_comment_message", StringType()),
            StructField("review_creation_date", TimestampType()),
            StructField("review_answer_timestamp", TimestampType()),
        ]
    ),
    "olist_customers_dataset.csv": StructType(
        [
            StructField("customer_id", StringType()),
            StructField("customer_unique_id", StringType()),
            StructField("customer_zip_code_prefix", StringType()),
            StructField("customer_city", StringType()),
            StructField("customer_state", StringType()),
        ]
    ),
    "olist_sellers_dataset.csv": StructType(
        [
            StructField("seller_id", StringType()),
            StructField("seller_zip_code_prefix", StringType()),
            StructField("seller_city", StringType()),
            StructField("seller_state", StringType()),
        ]
    ),
    "olist_products_dataset.csv": StructType(
        [
            StructField("product_id", StringType()),
            StructField("product_category_name", StringType()),
            StructField("product_name_lenght", IntegerType()),
            StructField("product_description_lenght", IntegerType()),
            StructField("product_photos_qty", IntegerType()),
            StructField("product_weight_g", DoubleType()),
            StructField("product_length_cm", DoubleType()),
            StructField("product_height_cm", DoubleType()),
            StructField("product_width_cm", DoubleType()),
        ]
    ),
    "olist_geolocation_dataset.csv": StructType(
        [
            StructField("geolocation_zip_code_prefix", StringType()),
            StructField("geolocation_lat", DoubleType()),
            StructField("geolocation_lng", DoubleType()),
            StructField("geolocation_city", StringType()),
            StructField("geolocation_state", StringType()),
        ]
    ),
    "product_category_name_translation.csv": StructType(
        [
            StructField("product_category_name", StringType()),
            StructField("product_category_name_english", StringType()),
        ]
    ),
}

TABLE_NAMES = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_customers_dataset.csv": "customers",
    "olist_sellers_dataset.csv": "sellers",
    "olist_products_dataset.csv": "products",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "category_translation",
}


def process_olist(bucket_name, local_dir="/tmp/olist_spark"):
    from google.cloud import storage

    # Step 1: Download CSVs from GCS
    print("Downloading CSVs from GCS...")
    os.makedirs(local_dir, exist_ok=True)
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for blob in bucket.list_blobs(prefix="raw/"):
        if blob.name.endswith(".csv"):
            filename = blob.name.split("/")[-1]
            blob.download_to_filename(os.path.join(local_dir, filename))
            print(f"Downloaded {filename}")

    # Step 2: Apply schema enforcement with Spark
    print("Applying schema enforcement with Spark...")
    spark = create_spark_session()
    output_dir = "/tmp/olist_parquet"

    for csv_file, schema in SCHEMAS.items():
        local_path = os.path.join(local_dir, csv_file)
        table_name = TABLE_NAMES[csv_file]
        print(f"Enforcing schema for {table_name}...")

        df = (
            spark.read.option("header", True)
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .schema(schema)
            .csv(local_path)
        )

        df.write.mode("overwrite").parquet(f"{output_dir}/{table_name}")

    spark.stop()
    print("Schema enforcement complete!")

    # Step 3: Upload Parquet files to GCS
    print("Uploading Parquet files to GCS processed/...")

    # Delete old parquet files before uploading to prevent duplicates on re-runs
    for table_name in TABLE_NAMES.values():
        old_blobs = list(bucket.list_blobs(prefix=f"processed/{table_name}/"))
        if old_blobs:
            bucket.delete_blobs(old_blobs)
            print(f"Cleared old GCS files for {table_name}")

    for table_name in TABLE_NAMES.values():
        table_path = os.path.join(output_dir, table_name)
        if os.path.isdir(table_path):
            for f in os.listdir(table_path):
                if f.endswith(".parquet"):
                    local_file = os.path.join(table_path, f)
                    blob = bucket.blob(f"processed/{table_name}/{f}")
                    blob.upload_from_filename(local_file)
            print(f"Uploaded {table_name}")

    shutil.rmtree(local_dir, ignore_errors=True)
    shutil.rmtree(output_dir, ignore_errors=True)
    print("Done!")
