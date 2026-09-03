from pyspark.sql import DataFrame
from pyspark.sql.functions import col, first, sum


def transform_to_order_summary(
    batch_df: DataFrame,
) -> DataFrame:
    """
    Transform Silver events into an order-level summary.
    """

    return (
        batch_df.filter(col("event_type") == "order_item_added")
        .groupBy("order_id")
        .agg(
            first("customer_id").alias("customer_id"),
            sum("quantity").alias("total_items"),
            sum("line_amount").alias("total_amount"),
        )
    )
