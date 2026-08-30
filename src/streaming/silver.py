from pyspark.sql import SparkSession

from src.transformations.data_quality import (
    get_invalid_events,
    get_valid_events,
)
from src.transformations.silver import transform_silver

SILVER_PATH = "data/silver/events"
QUARANTINE_PATH = "data/quarantine/events"


def process_batch(batch_df, batch_id) -> None:
    """Process one Bronze micro-batch."""

    valid_df = get_valid_events(batch_df)

    invalid_df = (
        get_invalid_events(batch_df)
        .dropDuplicates(["event_id"])
    )

    # Write invalid records to Quarantine
    if not invalid_df.isEmpty():

        (
            invalid_df
            .write
            .mode("append")
            .parquet(QUARANTINE_PATH)
        )

        print(
            f"Silver batch {batch_id}: "
            f"invalid events sent to Quarantine"
        )

    # Transform and write valid records to Silver
    if not valid_df.isEmpty():

        silver_df = transform_silver(valid_df)

        (
            silver_df
            .write
            .mode("append")
            .parquet(SILVER_PATH)
        )

        print(
            f"Silver batch {batch_id}: "
            f"valid events written to Silver"
        )


def main() -> None:

    spark = (
        SparkSession.builder
        .appName("silver-layer")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    bronze_df = (
        spark.readStream
        .format("parquet")
        .schema(
            """
            event_id STRING,
            event_type STRING,
            order_id STRING,
            customer_id STRING,
            product_id STRING,
            quantity INT,
            unit_price DECIMAL(10, 2),
            event_timestamp STRING
            """
        )
        .load("data/bronze/events")
    )

    query = (
        bronze_df
        .writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            "data/checkpoints/silver",
        )
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()