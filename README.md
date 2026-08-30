# Real-Time Data Platform

End-to-end real-time data engineering platform built with Python, Apache Kafka, PySpark, and Delta Lake.

## Architecture

The platform processes e-commerce events in real time using a Medallion Architecture with an additional Quarantine layer for invalid events:

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
Data Quality Validation
      │
      ├──────────────────► Quarantine Layer
      │                   Invalid events
      │
      ▼
Silver Layer
Cleaned and enriched events
      │
      ▼
Gold Layer
Order-level aggregations stored in Delta Lake
```

## Technologies

- Python
- Apache Kafka
- Docker
- PySpark Structured Streaming
- Delta Lake
- Parquet
- Pytest

## Project Structure

```text
real-time-data-platform/
│
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
│       ├── read_gold.py
│       └── read_quarantine.py
│
├── tests/
│   ├── test_event_generator.py
│   ├── test_data_quality.py
│   ├── test_silver.py
│   └── test_gold.py
│
├── requirements.txt
└── README.md
```

## Data Flow

### 1. Event Generator

A Python producer generates simulated e-commerce events:

- `order_created`
- `order_item_added`
- `payment_completed`
- `order_shipped`
- `order_delivered`

Events are published to the Kafka topic:

```text
ecommerce-events
```

### 2. Bronze Layer

The Bronze streaming job consumes events from Kafka.

Kafka JSON messages are parsed using a predefined PySpark schema and written to Parquet:

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

### 3. Data Quality and Quarantine Layer

Before events are processed by the Silver layer, data quality validations are applied.

An event is considered valid when:

- `event_id` is not null.
- `order_id` is not null.
- `event_type` belongs to one of the supported event types.

Supported event types:

- `order_created`
- `order_item_added`
- `payment_completed`
- `order_shipped`
- `order_delivered`

Additionally, `order_item_added` events must contain:

- A non-null `product_id`.
- A `quantity` greater than zero.
- A non-null `unit_price`.
- A `unit_price` greater than zero.

Invalid events are separated from valid events and stored in the Quarantine layer:

```text
data/quarantine/events
```

This prevents invalid data from reaching the Silver and Gold layers while preserving the original records for inspection and troubleshooting.

The flow is:

```text
Bronze Events
      │
      ▼
Data Quality Validation
      │
      ├──────────────► Quarantine
      │                Invalid Events
      │
      ▼
Valid Events
      │
      ▼
Silver Layer
```

A dedicated producer is also included to generate an invalid event for testing:

```text
src/producer/send_invalid_event.py
```

### 4. Silver Layer

The Silver streaming job processes valid Bronze events and applies data transformations:

- Removes duplicate events using `event_id`.
- Converts `event_timestamp` to `TIMESTAMP`.
- Calculates `line_amount`.
- Adds a `processing_timestamp`.

Output:

```text
data/silver/events
```

### 5. Gold Layer

The Gold streaming job aggregates order item events.

For each order, it calculates:

- Customer ID.
- Total number of items.
- Total order amount.

The results are stored as a Delta Lake table:

```text
data/gold/order_summary
```

The Gold layer uses:

- `foreachBatch`
- Delta Lake
- Delta `MERGE`

This allows incremental updates to existing orders.

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

### 3. Start the Bronze streaming job

```bash
python -m src.streaming.bronze
```

### 4. Start the Silver streaming job

In another terminal:

```bash
python -m src.streaming.silver
```

### 5. Start the Gold streaming job

After the Silver layer has generated data, start Gold in another terminal:

```bash
python -m src.streaming.gold
```

### 6. Start the event generator

In another terminal:

```bash
python -m src.producer.event_generator
```

The complete pipeline is:

```text
Event Generator
      ↓
Kafka
      ↓
Bronze
      ↓
Data Quality Validation
      ├────────► Quarantine
      ↓
Silver
      ↓
Gold
```

### 7. Send an Invalid Event

To test the Quarantine layer:

```bash
python -m src.producer.send_invalid_event
```

The invalid event should be processed by Bronze and then stored in:

```text
data/quarantine/events
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

### Quarantine

```bash
python -m src.utils.read_quarantine
```

Example Quarantine output:

```text
+----------------+----------------+-----------+-----------+----------+--------+----------+-------------------------+
|event_id        |event_type      |order_id   |customer_id|product_id|quantity|unit_price|event_timestamp          |
+----------------+----------------+-----------+-----------+----------+--------+----------+-------------------------+
|invalid-test-001|order_item_added|ORD-INVALID|CUST-TEST  |PROD-TEST |-5      |100.00    |2026-08-30T12:00:00+00:00|
+----------------+----------------+-----------+-----------+----------+--------+----------+-------------------------+
```

Example Gold output:

```text
+--------+-----------+------------+-------------+
|order_id|customer_id|total_items |total_amount |
+--------+-----------+------------+-------------+
|ORD-2179|CUST-171  |8           |2771.43      |
|ORD-2184|CUST-852  |8           |3455.58      |
|ORD-5520|CUST-480  |17          |4268.79      |
+--------+-----------+------------+-------------+
```

## Testing

Run the test suite:

```bash
pytest -v
```

Current tests cover:

- Event generation.
- Data quality validation.
- Silver transformations.
- Gold aggregations.

## Key Concepts Demonstrated

This project demonstrates:

- Real-time event processing.
- Apache Kafka.
- Docker.
- PySpark Structured Streaming.
- Medallion Architecture.
- Bronze, Silver, and Gold layers.
- Data quality validation.
- Quarantine layer for invalid events.
- Parquet.
- Delta Lake.
- Delta `MERGE`.
- `foreachBatch`.
- Checkpointing.
- Data transformations.
- Unit testing with Pytest.

## Future Improvements

Possible future improvements include:

- More advanced schema validation.
- Detailed error reasons in the Quarantine layer.
- Kafka multi-broker setup.
- Airflow orchestration.
- CI/CD with GitHub Actions.
- Cloud deployment using Google Cloud Platform.
- Google Cloud Storage.
- Dataproc.
- BigQuery.
- Terraform infrastructure provisioning.
- Monitoring and alerting.