from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
)
from pyspark.sql.types import DecimalType


def transform_silver(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["event_id"])
        .withColumn(
            "event_timestamp",
            to_timestamp(col("event_timestamp")),
        )
        .withColumn(
            "line_amount",
            (
                col("quantity") * col("unit_price")
            ).cast(DecimalType(12, 2)),
        )
        .withColumn(
            "processing_timestamp",
            current_timestamp(),
        )
    )