# Olist E-Commerce Data Engineering Pipeline

An end-to-end batch data pipeline for the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

Raw CSV data is ingested from Kaggle, schema-enforced with Spark, stored in a GCS data lake, loaded into BigQuery, transformed with dbt, and visualised in Looker.

---

## Problem Statement

The Olist dataset contains 9 CSV files covering orders, customers, products, sellers, payments, and reviews from a Brazilian e-commerce marketplace. The goal was to build a production-style data pipeline that answers key business questions:

- Which product categories generate the most revenue?
- How does monthly revenue trend over time?
- Which states have the lowest customer satisfaction?
- What percentage of orders are delivered late?

---

## [View Dashboard](https://lookerstudio.google.com/reporting/ca2b76a6-db5f-40ba-be9d-91dee4f52dd0)

[View Dashboard](https://lookerstudio.google.com/reporting/ca2b76a6-db5f-40ba-be9d-91dee4f52dd0)
<img width="1488" height="1121" alt="image" src="https://github.com/user-attachments/assets/cefc019d-917f-4bbd-befa-eac8bc560a3d" />


## Architecture
<img width="1920" height="1080" alt="pipeline_architecture" src="https://github.com/user-attachments/assets/c251a899-494a-4a40-9421-022529f32937" />



```
![Uploading Group 1216.svg…]()

┌──────────────┐     ┌─────────────────────────────────────────────────────┐
│   Kaggle API │────▶│                      GCS (Data Lake)                │
│  (9 CSV files)│     │   raw/          -->   processed/                    │
└──────────────┘     │   *.csv               *.parquet (Spark enforced)    │
                     └────────────────────────────┬────────────────────────┘
                                                  │
                                          Airflow DAG
                                       (olist_warehouse)
                                                  │
                     ┌────────────────────────────▼────────────────────────┐
                     │               BigQuery (Data Warehouse)             │
                     │                                                     │
                     │   olist_dwh  (raw tables, partitioned + clustered)  │
                     │       │                                             │
                     │   dbt staging  -->  dbt marts                      │
                     │   (views)           (tables)                        │
                     └────────────────────────────┬────────────────────────┘
                                                  │
                                             Looker
                                          (Dashboard)
```

- Orchestration: Apache Airflow with two DAGs (ingestion + warehouse)
- Infrastructure: Terraform provisions GCS bucket and BigQuery dataset
- Containerisation: Docker Compose running Airflow webserver, scheduler, and Postgres

---

## Data Model
<img width="762" height="729" alt="Group 1222" src="https://github.com/user-attachments/assets/8a94ff7a-864a-41c7-a936-2682f170a407" />


```
┌─────────────────────────────────────────────────────────────────┐
│                  RAW SOURCES (BigQuery: olist_dwh)              │
│                                                                  │
│  orders  order_items  order_payments  order_reviews  customers  │
│  sellers  products  geolocation  category_translation           │
└─────────────────────────────────────────────────────────────────┘
                              │
                    (dbt staging models)
                              │
┌─────────────────────────────────────────────────────────────────┐
│                  STAGING LAYER (olist_dwh_staging)              │
│                                                                  │
│  stg_orders        stg_order_payments    stg_products           │
│  stg_order_items   stg_order_reviews     stg_sellers            │
│  stg_customers                                                   │
└─────────────────────────────────────────────────────────────────┘
        │                                        │
        │           (dbt mart models)            │
        ▼                                        ▼
┌───────────────────────┐          ┌─────────────────────────────┐
│      fct_orders       │          │       fct_order_items       │
│ ------------------- │          │ --------------------------- │
│ order_id (PK)         │          │ order_id (FK -> fct_orders) │
│ customer_id           │◄─────────│ order_item_id               │
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

- `fct_orders`: one row per order, joins orders + customers + payments + reviews
- `fct_order_items`: one row per item, joins order items + products + sellers + orders

---

## Technologies

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

## Dashboard

> Add screenshots here after publishing the Looker report.

**Tile 1: Top 10 Product Categories by Revenue**
Bar chart from `fct_order_items` showing which categories drive the most sales.

**Tile 2: Monthly Revenue**
Line chart from `fct_orders` showing revenue trends over time.

**KPI Cards**
Total Orders | Total Revenue | Avg Review Score | Late Deliveries

---

## Reproduce This Project

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
