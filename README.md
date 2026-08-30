# Real-Time Data Platform

End-to-end real-time data engineering platform built with Python, Apache Kafka, PySpark Structured Streaming, and Delta Lake.

The project simulates an e-commerce platform that generates events in real time, processes them through a Medallion Architecture, validates data quality, sends invalid records to a Quarantine Layer, and produces order-level aggregations.

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
stored in Delta Lake
```

## Technologies

- Python
- Apache Kafka
- Docker
- PySpark Structured Streaming
- Delta Lake
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
├── src/
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
│       ├── read_bronze.py
│       ├── read_silver.py
│       └── read_gold.py
│
├── tests/
│   ├── test_event_generator.py
│   ├── test_data_quality.py
│   ├── test_silver.py
│   ├── test_gold.py
│   ├── test_bronze.py
│   ├── test_silver_streaming.py
│   └── test_gold_streaming.py
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── requirements.txt
├── .gitignore
└── README.md
```

## Data Flow

### 1. Event Generator

A Python producer generates simulated e-commerce events.

The following event types are generated:

- order_created
- order_item_added
- payment_completed
- order_shipped
- order_delivered

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

### 2. Bronze Layer

The Bronze streaming job consumes events from Kafka.

Kafka JSON messages are parsed using a predefined PySpark schema and stored as raw Parquet files.

```text
Kafka
  ↓
PySpark Structured Streaming
  ↓
Bronze Parquet
```

Output:

```text
data/bronze/events
```

Checkpoint:

```text
data/checkpoints/bronze
```

Run the Bronze streaming job:

```bash
python -m src.streaming.bronze
```

### 3. Silver Layer

The Silver streaming job reads events from the Bronze Layer.

Each micro-batch is processed using foreachBatch.

The pipeline separates valid and invalid records.

#### Data Quality Validation

Valid events must meet the required data quality rules.

Invalid events are separated from valid events.

Duplicate invalid events are removed using:

- event_id

#### Quarantine Layer

Invalid events are stored separately in the Quarantine Layer.

```text
data/quarantine/events
```

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

#### Silver Transformations

Valid events are transformed by:

- Removing duplicate events using event_id
- Converting event_timestamp to TIMESTAMP
- Calculating line_amount
- Adding processing_timestamp

Output:

```text
data/silver/events
```

Checkpoint:

```text
data/checkpoints/silver
```

Run the Silver streaming job:

```bash
python -m src.streaming.silver
```

### 4. Gold Layer

The Gold streaming job reads events from the Silver Layer.

The pipeline aggregates order_item_added events at order level.

For each order it calculates:

- customer_id
- total_items
- total_amount

Output:

```text
data/gold/order_summary
```

The Gold Layer uses:

- foreachBatch
- Delta Lake
- Delta MERGE

#### Delta Table Creation

During the first batch, the pipeline checks whether the Delta table already exists.

If it does not exist, it creates the table.

#### Incremental Updates

If the Delta table already exists, new batches are merged using Delta MERGE.

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
Delta Table Exists?
      │
 ┌────┴─────┐
 │          │
No         Yes
 │          │
 ▼          ▼
CREATE     MERGE
DELTA      DELTA
TABLE      TABLE
```

Checkpoint:

```text
data/checkpoints/gold_order_summary
```

Run the Gold streaming job:

```bash
python -m src.streaming.gold
```

## Running the Project

### 1. Create the Python environment

```bash
conda create -n real-time-data-platform python=3.12
conda activate real-time-data-platform
```

Install dependencies:

```bash
pip install -r requirements.txt
```

### 2. Start Kafka

```bash
docker compose -f docker/docker-compose.yml up -d
```

Verify that Kafka is running:

```bash
docker ps
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

### Gold

```bash
python -m src.utils.read_gold
```

Example Gold output:

```text
+--------+-----------+-----------+------------+
|order_id|customer_id|total_items|total_amount|
+--------+-----------+-----------+------------+
|ORD-1732|CUST-628   |1          |380.17      |
|ORD-2125|CUST-988   |2          |971.54      |
|ORD-3800|CUST-908   |7          |2050.41     |
+--------+-----------+-----------+------------+
```

## Testing

Run the complete test suite:

```bash
pytest -v
```

The project includes unit tests for:

- Event generation
- Event schema
- Data quality validation
- Silver transformations
- Gold transformations
- Invalid event producer
- Bronze streaming configuration
- Silver micro-batch processing
- Quarantine Layer
- Gold Delta table creation
- Gold Delta MERGE
- Gold streaming configuration

### Code Coverage

Run the test suite with coverage:

```bash
pytest --cov=src --cov-report=term-missing
```

Current test coverage:

```text
94%
```

The streaming components are tested using mocks to validate:

- Spark configuration
- Structured Streaming configuration
- foreachBatch
- Checkpoints
- Parquet reads
- Delta table creation
- Delta MERGE

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

## Continuous Integration

GitHub Actions runs automatically on repository changes.

The CI pipeline performs the following steps:

```text
Checkout repository
        ↓
Set up Python
        ↓
Install dependencies
        ↓
Run Pytest
        ↓
Run Ruff
```

This ensures that tests must pass before code quality checks are executed.

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
- Delta Lake
- Delta MERGE
- foreachBatch
- Micro-batch processing
- Checkpointing
- Incremental processing
- Unit testing
- Mocking Spark and Delta components
- Code coverage
- Ruff
- CI/CD with GitHub Actions

## Future Improvements

Possible future improvements include:

- Schema validation
- More advanced data quality rules
- Data quality metrics
- Monitoring and alerting
- Kafka multi-broker setup
- Kafka Schema Registry
- Airflow orchestration
- Cloud deployment
- Google Cloud Storage
- Dataproc
- BigQuery
- Terraform infrastructure provisioning
- Production monitoring
- Centralized logging
- GitHub Actions deployment pipeline

## Future Cloud Architecture

The next phase of the project will move the local platform to Google Cloud.

```text
Kafka / Event Source
        ↓
Google Cloud Storage
        ↓
Dataproc + PySpark
        ↓
BigQuery
        ↓
Analytics
```

Infrastructure will be provisioned using Terraform.

Future CI/CD will automate:

```text
Code
 ↓
Tests
 ↓
Linting
 ↓
Build
 ↓
Cloud Deployment
```

## Project Status

The local version of the real-time data platform is complete.

Current implementation includes:

- Real-time Kafka ingestion
- Bronze, Silver, and Gold layers
- Data Quality validation
- Quarantine Layer
- Delta Lake incremental processing
- Automated tests
- 94% test coverage
- Ruff code quality checks
- GitHub Actions CI

The next stage is cloud deployment on Google Cloud Platform.