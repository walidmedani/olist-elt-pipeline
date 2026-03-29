from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


def create_spark_session():
    return SparkSession.builder.appName("OlistTransformation").getOrCreate()


def process_olist(bucket_name):
    spark = create_spark_session()
    base_path = f"gs://{bucket_name}/raw"
    output_path = f"gs://{bucket_name}/processed"

    print("Reading CSVs from GCS...")

    orders = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{base_path}/olist_orders_dataset.csv")
    )

    order_items = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{base_path}/olist_order_items_dataset.csv")
    )

    order_payments = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{base_path}/olist_order_payments_dataset.csv")
    )

    order_reviews = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{base_path}/olist_order_reviews_dataset.csv")
    )

    customers = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{base_path}/olist_customers_dataset.csv")
    )

    sellers = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{base_path}/olist_sellers_dataset.csv")
    )

    products = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{base_path}/olist_products_dataset.csv")
    )

    geolocation = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{base_path}/olist_geolocation_dataset.csv")
    )

    translation = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{base_path}/product_category_name_translation.csv")
    )

    print("Transforming data...")

    # Cast timestamp columns properly
    orders = (
        orders.withColumn(
            "order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp")
        )
        .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
        .withColumn(
            "order_delivered_carrier_date",
            F.to_timestamp("order_delivered_carrier_date"),
        )
        .withColumn(
            "order_delivered_customer_date",
            F.to_timestamp("order_delivered_customer_date"),
        )
        .withColumn(
            "order_estimated_delivery_date",
            F.to_timestamp("order_estimated_delivery_date"),
        )
    )

    # Add date column for partitioning in BigQuery
    orders = orders.withColumn(
        "order_purchase_date", F.to_date("order_purchase_timestamp")
    )

    # Join products with English category names
    products = products.join(
        translation, on="product_category_name", how="left"
    ).withColumn(
        "product_category_name_english",
        F.coalesce(F.col("product_category_name_english"), F.lit("unknown")),
    )

    # Cast numeric columns
    order_items = order_items.withColumn(
        "price", F.col("price").cast("double")
    ).withColumn("freight_value", F.col("freight_value").cast("double"))

    order_payments = order_payments.withColumn(
        "payment_value", F.col("payment_value").cast("double")
    )

    order_reviews = order_reviews.withColumn(
        "review_score", F.col("review_score").cast("integer")
    )

    print("Writing Parquet files to GCS processed folder...")

    orders.write.mode("overwrite").parquet(f"{output_path}/orders")
    order_items.write.mode("overwrite").parquet(f"{output_path}/order_items")
    order_payments.write.mode("overwrite").parquet(f"{output_path}/order_payments")
    order_reviews.write.mode("overwrite").parquet(f"{output_path}/order_reviews")
    customers.write.mode("overwrite").parquet(f"{output_path}/customers")
    sellers.write.mode("overwrite").parquet(f"{output_path}/sellers")
    products.write.mode("overwrite").parquet(f"{output_path}/products")
    geolocation.write.mode("overwrite").parquet(f"{output_path}/geolocation")

    print("All done!")
    spark.stop()


if __name__ == "__main__":
    import sys

    process_olist(sys.argv[1])
