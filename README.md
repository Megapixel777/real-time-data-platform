# Real-Time Data Platform

End-to-end real-time data engineering platform built with Python, Apache Kafka, PySpark Structured Streaming, Google Cloud Storage, BigQuery, Terraform, Docker, and GitHub Actions.

The project simulates an e-commerce platform that generates events in real time, processes them through a Medallion Architecture, validates data quality, sends invalid records to a Quarantine Layer, creates order-level aggregations, and incrementally loads the Gold data into BigQuery.

## Architecture

```text
Event Generator
       │
       ▼
Apache Kafka
       │
       ▼
Bronze Layer
Raw events stored as Parquet
       │
       ▼
Silver Layer
Data quality validation
       │
       ├──────────────► Quarantine Layer
       │                Invalid events
       │
       ▼
Cleaned and enriched events
       │
       ▼
Gold Layer
Order-level aggregations
stored as Parquet
       │
       ▼
BigQuery
Incremental load
       │
       ▼
Analytics
```

The platform supports both local execution and Google Cloud Storage as the storage backend.

## Technologies

- Python 3.11
- Apache Kafka
- Docker
- PySpark Structured Streaming
- Google Cloud Storage
- BigQuery
- Terraform
- Parquet
- Pytest
- Coverage
- Ruff
- GitHub Actions

## Project Structure

```text
real-time-data-platform/

├── docker/
│   └── docker-compose.yml
│
├── jars/
│   └── gcs-connector-*.jar
│
├── src/
│   ├── bigquery/
│   │   └── load_gold_to_bigquery.py
│   │
│   ├── config/
│   │   ├── paths.py
│   │   └── settings.py
│   │
│   ├── producer/
│   │   ├── event_generator.py
│   │   └── send_invalid_event.py
│   │
│   ├── schemas/
│   │   └── event_schema.py
│   │
│   ├── streaming/
│   │   ├── bronze.py
│   │   ├── silver.py
│   │   └── gold.py
│   │
│   ├── transformations/
│   │   ├── data_quality.py
│   │   ├── silver.py
│   │   └── gold.py
│   │
│   └── utils/
│       ├── gcs_check.py
│       ├── read_bronze.py
│       ├── read_silver.py
│       ├── read_quarantine.py
│       └── read_gold.py
│
├── terraform/
│   ├── bigquery.tf
│   ├── main.tf
│   ├── outputs.tf
│   ├── providers.tf
│   ├── variables.tf
│   └── .terraform.lock.hcl
│
├── tests/
│   ├── test_event_generator.py
│   ├── test_event_schema.py
│   ├── test_data_quality.py
│   ├── test_silver.py
│   ├── test_gold.py
│   ├── test_bronze.py
│   ├── test_silver_streaming.py
│   ├── test_gold_streaming.py
│   └── test_send_invalid_event.py
│
├── .github/
│   └── workflows/
│       └── tests.yml
│
├── requirements.txt
├── .gitignore
└── README.md
```

## Data Flow

### 1. Event Generator

A Python Kafka producer generates simulated e-commerce events.

The following event types are generated:

- `order_created`
- `order_item_added`
- `payment_completed`
- `order_shipped`
- `order_delivered`

Events are published to the Kafka topic:

```text
ecommerce-events
```

Run the event generator:

```bash
python -m src.producer.event_generator
```

The project also includes a producer for testing invalid events:

```bash
python -m src.producer.send_invalid_event
```

## 2. Bronze Layer

The Bronze streaming job consumes events from Kafka.

Kafka JSON messages are parsed using a predefined PySpark schema and stored as raw Parquet files.

```text
Kafka
  ↓
PySpark Structured Streaming
  ↓
Bronze Parquet
```

Local output:

```text
data/bronze/events
```

Local checkpoint:

```text
data/checkpoints/bronze
```

When running with the GCP environment, the Bronze data and checkpoint are stored in Google Cloud Storage.

Run the Bronze streaming job:

```bash
python -m src.streaming.bronze
```

## 3. Silver Layer

The Silver streaming job reads events from the Bronze Layer.

Each micro-batch is processed using `foreachBatch`.

The pipeline separates valid and invalid records.

### Data Quality Validation

Events are validated using data quality rules including:

- Required `event_id`
- Valid `event_type`
- Required `order_id`
- Required `customer_id`
- Required `event_timestamp`
- Required product information for `order_item_added`
- Positive quantity
- Positive unit price

Invalid events are separated from valid events.

Duplicate invalid events are removed using:

```text
event_id
```

### Quarantine Layer

Invalid events are stored separately in the Quarantine Layer.

Local output:

```text
data/quarantine/events
```

When running with GCP, the Quarantine Layer is stored in Google Cloud Storage.

This prevents invalid records from entering the Silver and Gold layers while preserving them for investigation and debugging.

```text
Bronze Batch
     │
     ├── Valid Events
     │       │
     │       ▼
     │   Transform
     │       │
     │       ▼
     │   Silver Layer
     │
     └── Invalid Events
             │
             ▼
      Remove duplicates
             │
             ▼
      Quarantine Layer
```

### Silver Transformations

Valid events are transformed by:

- Removing duplicate events using `event_id`
- Converting `event_timestamp` to `TIMESTAMP`
- Calculating `line_amount`
- Adding `processing_timestamp`

Local output:

```text
data/silver/events
```

Local checkpoint:

```text
data/checkpoints/silver
```

Run the Silver streaming job:

```bash
python -m src.streaming.silver
```

## 4. Gold Layer

The Gold streaming job reads events from the Silver Layer.

The pipeline aggregates `order_item_added` events at order level.

For each order it calculates:

- `customer_id`
- `total_items`
- `total_amount`

```text
Silver Events
      │
      ▼
Order Aggregation
      │
      ▼
foreachBatch
      │
      ▼
Gold Parquet
```

Local output:

```text
data/gold/order_summary
```

Local checkpoint:

```text
data/checkpoints/gold_order_summary
```

When running with GCP, Gold data is stored in Google Cloud Storage.

Run the Gold streaming job:

```bash
python -m src.streaming.gold
```

The Gold layer uses Parquet as its storage format.

## 5. BigQuery

The Gold Parquet data can be incrementally loaded into BigQuery.

The loader:

1. Discovers finalized Gold Parquet files in Google Cloud Storage.
2. Loads them into a BigQuery staging table.
3. Identifies batches that have not previously been processed.
4. Merges each new batch into the target `order_summary` table.
5. Registers the processed batch.
6. Prevents the same batch from being processed again.

BigQuery tables:

```text
ecommerce.order_summary
ecommerce.order_summary_staging
ecommerce.order_summary_processed_batches
```

Run the incremental loader:

```bash
python -m src.bigquery.load_gold_to_bigquery
```

The loader is designed to be idempotent. Running it again after all batches have been processed results in:

```text
No new batches to process.
```

## Google Cloud Storage

The project supports Google Cloud Storage as the storage backend.

The storage paths are:

```text
gs://real-time-data-platform-thomasede/bronze/events
gs://real-time-data-platform-thomasede/silver/events
gs://real-time-data-platform-thomasede/quarantine/events
gs://real-time-data-platform-thomasede/gold/order_summary
```

Checkpoints are stored in:

```text
gs://real-time-data-platform-thomasede/checkpoints/bronze
gs://real-time-data-platform-thomasede/checkpoints/silver
gs://real-time-data-platform-thomasede/checkpoints/gold_order_summary
```

The environment is selected using:

```bash
ENVIRONMENT=gcp
```

For example:

```bash
ENVIRONMENT=gcp python -m src.streaming.bronze
```

The default environment is local:

```text
ENVIRONMENT=local
```

## Running the Project

### 1. Create the Python environment

The project is developed and tested with Python 3.11.

```bash
conda create -n real-time-data-platform-py311 python=3.11
conda activate real-time-data-platform-py311
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### 2. Start Kafka

Kafka runs locally using Docker Compose.

```bash
docker compose -f docker/docker-compose.yml up -d
```

Verify that Kafka is running:

```bash
docker ps
```

Verify the Kafka topic:

```bash
MSYS_NO_PATHCONV=1 docker exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

The expected topic is:

```text
ecommerce-events
```

### 3. Start Bronze

In the first terminal:

```bash
python -m src.streaming.bronze
```

### 4. Start Silver

In another terminal:

```bash
python -m src.streaming.silver
```

### 5. Start Gold

In another terminal:

```bash
python -m src.streaming.gold
```

### 6. Start the Event Generator

In another terminal:

```bash
python -m src.producer.event_generator
```

The complete pipeline is:

```text
Event Generator
       ↓
Apache Kafka
       ↓
Bronze
       ↓
Silver
       ├──────► Quarantine
       ↓
Gold
       ↓
BigQuery
```

## Reading the Data

### Bronze

```bash
python -m src.utils.read_bronze
```

### Silver

```bash
python -m src.utils.read_silver
```

### Quarantine

```bash
python -m src.utils.read_quarantine
```

### Gold

```bash
python -m src.utils.read_gold
```

## Testing

Run the complete test suite:

```bash
pytest -q
```

The project currently contains:

```text
18 tests
```

The tests cover:

- Event generation
- Event schema
- Data quality validation
- Silver transformations
- Gold transformations
- Invalid event producer
- Bronze streaming configuration
- Silver streaming configuration
- Silver micro-batch processing
- Quarantine processing
- Gold streaming configuration
- Gold micro-batch processing

The streaming components are tested using mocks to validate:

- Spark configuration
- Structured Streaming configuration
- `foreachBatch`
- Checkpoints
- Parquet reads and writes
- Data transformations
- Micro-batch processing

## Code Coverage

Run the test suite with coverage:

```bash
pytest --cov=src --cov-report=term-missing
```

## Code Quality

Ruff is used for linting and code quality checks.

Run Ruff:

```bash
ruff check .
```

Automatically fix supported issues:

```bash
ruff check . --fix
```

Check formatting:

```bash
ruff format --check .
```

Format the project:

```bash
ruff format .
```

## Continuous Integration

GitHub Actions runs automatically on:

- Pushes to `main`
- Pull requests targeting `main`

The workflow is located at:

```text
.github/workflows/tests.yml
```

The CI pipeline performs:

```text
Checkout repository
        ↓
Set up Python 3.11
        ↓
Install dependencies
        ↓
Run Ruff
        ↓
Validate Docker Compose
        ↓
Run Pytest
```

Docker Compose is validated using:

```bash
docker compose -f docker/docker-compose.yml config
```

This ensures that the Python code, tests, linting, and Docker configuration are validated automatically.

## Infrastructure as Code

Google Cloud infrastructure is managed using Terraform.

Terraform provisions:

- Google Cloud Storage bucket
- BigQuery dataset
- BigQuery `order_summary` table
- BigQuery staging table
- BigQuery processed-batches table

Terraform configuration is located in:

```text
terraform/
```

Initialize Terraform:

```bash
cd terraform
terraform init
```

Validate the configuration:

```bash
terraform validate
```

Review infrastructure changes:

```bash
terraform plan
```

Apply infrastructure:

```bash
terraform apply
```

## GCS Connectivity Check

The project includes a manual utility to validate Spark connectivity with Google Cloud Storage:

```bash
python -m src.utils.gcs_check
```

The utility tests:

- Spark session creation
- GCS connector configuration
- Google Cloud authentication
- Writing Parquet to GCS
- Reading Parquet from GCS

This utility is intended as a manual integration check and is not part of the automated unit test suite.

## Key Concepts Demonstrated

This project demonstrates:

- Real-time event processing
- Apache Kafka
- Docker
- PySpark Structured Streaming
- Medallion Architecture
- Bronze, Silver, and Gold layers
- Data quality validation
- Quarantine Layer
- Parquet
- Google Cloud Storage
- BigQuery
- Incremental processing
- Idempotent batch processing
- `foreachBatch`
- Micro-batch processing
- Checkpointing
- Spark transformations
- Unit testing
- Mocking Spark components
- Code coverage
- Ruff
- CI/CD with GitHub Actions
- Infrastructure as Code with Terraform

## Future Improvements

Possible future improvements include:

- More advanced data quality rules
- Data quality metrics
- Monitoring and alerting
- Kafka multi-broker setup
- Kafka Schema Registry
- Airflow orchestration
- Dataproc deployment
- Production Spark deployment
- BigQuery optimization
- Production monitoring
- Centralized logging
- Secret management
- Automated cloud deployment
- GitHub Actions deployment pipeline

## Cloud Architecture

The current cloud architecture uses Google Cloud Storage and BigQuery:

```text
Kafka / Event Source
        ↓
PySpark Structured Streaming
        ↓
Google Cloud Storage
        │
        ├── Bronze
        ├── Silver
        ├── Quarantine
        └── Gold
               ↓
           BigQuery
               ↓
           Analytics
```

Terraform manages the cloud infrastructure.

The project can be evolved towards a fully managed cloud architecture using services such as Dataproc for Spark execution.

## Project Status

The core real-time data platform is implemented and tested.

Current implementation includes:

- Real-time Kafka ingestion
- Dockerized Kafka
- Bronze Layer
- Silver Layer
- Gold Layer
- Data Quality validation
- Quarantine Layer
- Parquet-based storage
- Google Cloud Storage integration
- BigQuery incremental loading
- Idempotent batch processing
- Terraform infrastructure
- Automated tests
- 18 passing tests
- Ruff code quality checks
- GitHub Actions CI
- Docker Compose validation

The next stage is to further develop the cloud deployment architecture and production-oriented operational capabilities.