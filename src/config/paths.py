import os

GCS_BUCKET = os.getenv(
    "GCS_BUCKET",
    "real-time-data-platform-thomasede",
)

# Local data paths
LOCAL_BRONZE_PATH = "data/bronze/events"
LOCAL_SILVER_PATH = "data/silver/events"
LOCAL_GOLD_PATH = "data/gold/order_summary"
LOCAL_QUARANTINE_PATH = "data/quarantine/events"

# Local checkpoint paths
LOCAL_BRONZE_CHECKPOINT = "data/checkpoints/bronze"
LOCAL_SILVER_CHECKPOINT = "data/checkpoints/silver"
LOCAL_GOLD_CHECKPOINT = "data/checkpoints/gold_order_summary"

# GCS data paths
GCS_BRONZE_PATH = f"gs://{GCS_BUCKET}/bronze/events"
GCS_SILVER_PATH = f"gs://{GCS_BUCKET}/silver/events"
GCS_GOLD_PATH = f"gs://{GCS_BUCKET}/gold/order_summary"
GCS_QUARANTINE_PATH = f"gs://{GCS_BUCKET}/quarantine/events"

# GCS checkpoint paths
GCS_BRONZE_CHECKPOINT = f"gs://{GCS_BUCKET}/checkpoints/bronze"
GCS_SILVER_CHECKPOINT = f"gs://{GCS_BUCKET}/checkpoints/silver"
GCS_GOLD_CHECKPOINT = f"gs://{GCS_BUCKET}/checkpoints/gold_order_summary"
