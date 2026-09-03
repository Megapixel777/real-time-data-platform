from pyspark.sql import DataFrame
from pyspark.sql.functions import col

VALID_EVENT_TYPES = [
    "order_created",
    "order_item_added",
    "payment_completed",
    "order_shipped",
    "order_delivered",
]


def _quality_condition(df: DataFrame):
    """Return the data quality condition for valid events."""

    base_validations = (
        col("event_id").isNotNull()
        & col("event_type").isin(VALID_EVENT_TYPES)
        & col("order_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("event_timestamp").isNotNull()
    )

    item_validations = (
        (col("event_type") != "order_item_added")
        | (
            col("product_id").isNotNull()
            & col("quantity").isNotNull()
            & (col("quantity") > 0)
            & col("unit_price").isNotNull()
            & (col("unit_price") > 0)
        )
    )

    return base_validations & item_validations


def get_valid_events(df: DataFrame) -> DataFrame:
    """Return events that pass data quality validation."""

    return df.filter(_quality_condition(df))


def get_invalid_events(df: DataFrame) -> DataFrame:
    """Return events that fail data quality validation."""

    return df.filter(~_quality_condition(df))