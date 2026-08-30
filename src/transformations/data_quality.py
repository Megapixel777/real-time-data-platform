from pyspark.sql import DataFrame
from pyspark.sql.functions import col

VALID_EVENT_TYPES = [
    "order_created",
    "order_item_added",
    "payment_completed",
    "order_shipped",
    "order_delivered",
]


def get_valid_events(df: DataFrame) -> DataFrame:
    """Return events that pass data quality validation."""

    base_validations = (
        col("event_id").isNotNull()
        & col("order_id").isNotNull()
        & col("event_type").isin(VALID_EVENT_TYPES)
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

    return df.filter(
        base_validations & item_validations
    )


def get_invalid_events(df: DataFrame) -> DataFrame:
    """Return events that fail data quality validation."""

    base_validations = (
        col("event_id").isNotNull()
        & col("order_id").isNotNull()
        & col("event_type").isin(VALID_EVENT_TYPES)
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

    return df.filter(
        ~(base_validations & item_validations)
    )