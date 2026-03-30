# Olist E-Commerce Data Engineering Pipeline

An end-to-end batch data pipeline for the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

Raw CSV data is ingested from Kaggle, schema-enforced with Spark, stored in a GCS data lake, loaded into BigQuery, transformed with dbt, and visualised in Looker.

[Reproduce This Project](#reproduce-this-project)

---

### Problem Statement

Olist is a Brazilian e-commerce marketplace that connects small and medium-sized businesses to customers across Brazil. The dataset contains 9 CSV files covering 100,000+ orders placed between 2016 and 2018, spanning orders, customers, products, sellers, payments, reviews, and geolocation data.

The challenge with this dataset is that the raw data lives across 9 separate files with no enforced types, Portuguese category names, and no joined view of the customer journey from purchase to delivery to review. A business analyst cannot answer basic questions from the raw data without significant preparation work. The goal of this project was to build a production-style end-to-end data pipeline that takes those 9 raw CSV files and transforms them into a clean and query-ready data warehouse to answer key business questions:

- Which product categories generate the most revenue?
> Understanding where revenue is concentrated helps prioritize seller acquisition and marketing spend.

- How does monthly revenue trend over time?
> Identifying growth patterns and seasonal spikes informs inventory and logistics planning.

- Which states place the most orders?
> Understanding the geographic distribution of demand reveals where Olist's customer base is strongest and where there is untapped growth potential.

- Which states have the longest average delivery times?
> Identifying geographic bottlenecks in the delivery network highlights where logistics infrastructure or seller fulfillment needs improvement.

[View Interactive Dashboard](https://lookerstudio.google.com/reporting/ca2b76a6-db5f-40ba-be9d-91dee4f52dd0)
<img width="1491" height="1116" alt="olist_dashboard" src="https://github.com/user-attachments/assets/ad211aa5-ec1a-4f61-a63a-9ae4fb143e77" />



### Pipeline Architecture
<img width="2394" height="1344" alt="olist_pipeline_architecture" src="https://github.com/user-attachments/assets/9066bfc3-6e68-49b1-b2bd-6c2818d56e65" />

### Technologies

| Tool | Purpose |
|---|---|
| **Terraform** | Provision GCS bucket and BigQuery dataset |
| **Docker / Docker Compose** | Containerise Airflow and all dependencies |
| **Apache Airflow** | Orchestrate ingestion and warehouse DAGs |
| **Apache Spark (PySpark)** | Schema enforcement and CSV to Parquet conversion |
| **Google Cloud Storage** | Data lake storing raw CSVs and processed Parquet files |
| **Google BigQuery** | Data warehouse with partitioning and clustering |
| **dbt** | Staging and mart transformations with data quality tests |
| **Looker** | Business intelligence dashboard |
| **GitHub Actions** | CI pipeline that runs dbt parse, compile, and test on every push |
| **Make** | Shortcuts for common development commands |




---

### Data Model & Dictionary

```
┌─────────────────────────────────────────────────────────────────┐
│                  RAW SOURCES (BigQuery: olist_dwh)              │
│                                                                 │
│  orders  order_items  order_payments  order_reviews  customers  │
│  sellers  products  geolocation  category_translation           │
└─────────────────────────────────────────────────────────────────┘
                              │
                    (dbt staging models)
                              │
┌─────────────────────────────────────────────────────────────────┐
│                  STAGING LAYER (olist_dwh_staging)              │
│                                                                 │
│  stg_orders        stg_order_payments    stg_products           │
│  stg_order_items   stg_order_reviews     stg_sellers            │
│  stg_customers                                                  │
└─────────────────────────────────────────────────────────────────┘
        │                                        │
        │           (dbt mart models)            │
        ▼                                        ▼
┌───────────────────────┐          ┌─────────────────────────────┐
│      fct_orders       │          │       fct_order_items       │
│ --------------------- │          │ --------------------------- │
│ order_id (PK)         │◄─────────│ order_id (FK -> fct_orders) │
│ customer_id           │          │ order_item_id               │
│ order_status          │          │ product_id                  │
│ order_purchase_date   │          │ seller_id                   │
│ order_delivered_date  │          │ price                       │
│ order_estimated_date  │          │ freight_value               │
│ customer_city         │          │ total_item_value            │
│ customer_state        │          │ product_category_name_eng   │
│ total_payment_value   │          │ seller_city                 │
│ payment_count         │          │ seller_state                │
│ avg_review_score      │          │ order_purchase_date         │
└───────────────────────┘          │ order_status                │
                                   └─────────────────────────────┘
```
#### Mart Tables (Analytics Ready)

#### `fct_orders`
> One row per order. Joins orders, customers, payments, and reviews into a single analytics-ready table.

| Column | Type | Description |
|---|---|---|
| `order_id` | STRING | Unique identifier for each order |
| `customer_id` | STRING | Foreign key to customers |
| `order_status` | STRING | Current status of the order |
| `order_purchase_timestamp` | TIMESTAMP | Date and time the order was placed |
| `order_purchase_date` | DATE | Date portion of purchase timestamp, used for partitioning |
| `order_delivered_customer_date` | TIMESTAMP | Date and time the customer received the order |
| `order_estimated_delivery_date` | TIMESTAMP | Estimated delivery date shown at time of purchase |
| `delivery_days` | INTEGER | Actual number of days from purchase to delivery, null if not yet delivered |
| `customer_city` | STRING | Customer's city |
| `customer_state` | STRING | Customer's state |
| `total_payment_value` | DOUBLE | Sum of all payment values for this order |
| `payment_count` | INTEGER | Number of payment transactions for this order |
| `avg_review_score` | DOUBLE | Average review score (1-5), null if no review submitted |

#### `fct_order_items`
> One row per item per order. Joins order items, products, sellers, and orders into a single analytics-ready table.

| Column | Type | Description |
|---|---|---|
| `order_id` | STRING | Foreign key to orders |
| `order_item_id` | INTEGER | Sequential item number within the order |
| `product_id` | STRING | Foreign key to products |
| `seller_id` | STRING | Foreign key to sellers |
| `price` | DOUBLE | Item price in BRL |
| `freight_value` | DOUBLE | Freight cost for this item in BRL |
| `total_item_value` | DOUBLE | Combined price and freight (price + freight_value) |
| `product_category_name_english` | STRING | English product category name |
| `seller_city` | STRING | City where the seller is located |
| `seller_state` | STRING | State where the seller is located |
| `order_purchase_date` | DATE | Date the order was placed |
| `order_status` | STRING | Current status of the order |

##### `Data Inventory`

`orders_dataset.csv`: Core table — order status, timestamps

`order_items_dataset.csv`: Items per order — price, freight, seller

`order_payments_dataset.csv`: Payment type and value per order

`order_reviews_dataset.csv`: Customer review scores and comments

`customers_dataset.csv`: Customer location and unique IDs

`sellers_dataset.csv`: Seller location data

`products_dataset.csv`: Product dimensions, category names

`geolocation_dataset.csv`: ZIP code → lat/lng mapping

`product_category_name_translation.csv`: Portuguese → English category names

---

# Reproduce This Project

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Terraform](https://developer.hashicorp.com/terraform/install)
- [Make](https://www.gnu.org/software/make/)
- A GCP project with billing enabled
- A GCP service account JSON key with roles: BigQuery Admin, Storage Admin
- A [Kaggle API token](https://www.kaggle.com/settings/account)

### Step 1 - Clone the repo

```bash
git clone https://github.com/<your-username>/olist-de-project.git
cd olist-de-project
```

### Step 2 - Set up credentials

Place your GCP service account key at:
```bash
mkdir -p ~/.gcp
cp /path/to/your/key.json ~/.gcp/olist-pipeline-sa.json
```

Copy and fill in the environment file:
```bash
cp .env.example .env
```

Edit `.env` with your values:
```
GCP_PROJECT_ID=your-project-id
GCP_BUCKET_NAME=your-bucket-name
GCP_BQ_DATASET=olist_dwh
KAGGLE_USERNAME=your-kaggle-username
KAGGLE_KEY=your-kaggle-api-key
```

### Step 3 - Provision cloud infrastructure

```bash
make infra-init
make infra-up
```

This creates a GCS bucket with `raw/` and `processed/` folders, and a BigQuery dataset `olist_dwh`.

### Step 4 - Create the dbt profiles file

This file is not committed to the repo as it contains credentials. Create it manually:

```bash
cat > dbt/olist/profiles.yml <<EOF
olist:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-project-id
      dataset: olist_dwh
      threads: 4
      timeout_seconds: 300
      location: US
      keyfile: /opt/airflow/credentials/olist-pipeline-sa.json
EOF
```

### Step 5 - Build and start Airflow

```bash
make build
make up
```

Wait about 30 seconds for the services to become healthy, then open [http://localhost:8080](http://localhost:8080) with username and password `admin`.

### Step 6 - Run the pipelines

In the Airflow UI:
1. Trigger **`olist_ingestion`** - downloads CSVs from Kaggle and uploads to GCS `raw/`
2. Once complete, trigger **`olist_warehouse`** - runs Spark, loads BigQuery, runs dbt

To verify dbt is working before triggering the full DAG:
```bash
make dbt-debug   # test BigQuery connection
make dbt-run     # run all models
make dbt-test    # run all tests
```

### Step 7 - Connect Looker

1. Open Looker and go to **Get Data > Google BigQuery**
2. Sign in with your Google account
3. Navigate to `your-project-id > olist_dwh_marts`
4. Load `fct_orders` and `fct_order_items`
5. Build your dashboard from the mart tables

### Tear down

```bash
make down        # stop Docker containers
make infra-down  # destroy GCS bucket and BigQuery dataset
```

---

## Make Commands

```bash
make help        # list all available commands

# Infrastructure
make infra-init  # terraform init
make infra-up    # terraform apply
make infra-down  # terraform destroy

# Docker
make build       # build Docker images
make up          # start all services
make down        # stop all services
make restart     # rebuild and restart
make logs        # tail logs

# dbt
make dbt-debug   # test BigQuery connection
make dbt-run     # run all models
make dbt-test    # run all tests
make dbt-build   # run models then tests
```

---

## CI/CD

A GitHub Actions workflow at `.github/workflows/dbt_ci.yml` runs on every push or pull request that touches the `dbt/` directory. It installs dbt, writes a `profiles.yml` from GitHub Secrets, then runs `dbt parse`, `dbt compile`, and `dbt test` against BigQuery.

To set it up, add these secrets to your GitHub repo under Settings > Secrets:

| Secret | Value |
|---|---|
| `GCP_PROJECT_ID` | Your GCP project ID |
| `GCP_SA_KEY_JSON` | Full contents of your service account JSON key |

---

## Project Structure

```
olist-de-project/
├── .github/
│   └── workflows/
│       └── dbt_ci.yml          # CI pipeline
├── airflow/
│   ├── dags/
│   │   ├── ingestion_dag.py    # DAG 1: Kaggle -> GCS
│   │   └── warehouse_dag.py    # DAG 2: Spark -> BigQuery -> dbt
│   └── Dockerfile
├── dbt/
│   └── olist/
│       ├── models/
│       │   ├── staging/        # stg_* views
│       │   └── marts/          # fct_* tables
│       └── tests/              # custom SQL tests
├── spark/
│   └── jobs/
│       └── process_olist.py    # Schema enforcement and GCS upload
├── terraform/
│   ├── main.tf
│   └── variables.tf
├── docker-compose.yml
├── Makefile
└── .env                        # not committed - see Step 2
```
