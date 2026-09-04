from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from src.config.settings import (
    BRONZE_CHECKPOINT,
    BRONZE_PATH,
)
from src.config.spark import create_spark_session
from src.schemas.event_schema import event_schema


def create_bronze_spark_session() -> SparkSession:
    return create_spark_session(
        app_name="real-time-data-platform",
        include_kafka=True,
    )

def main() -> None:
    spark = create_bronze_spark_session()

    try:
        print("Starting Bronze streaming...")
        print(f"BRONZE_PATH: {BRONZE_PATH}")
        print(f"BRONZE_CHECKPOINT: {BRONZE_CHECKPOINT}")

        kafka_df = (
            spark.readStream.format("kafka")
            .option(
                "kafka.bootstrap.servers",
                "localhost:9092",
            )
            .option(
                "subscribe",
                "ecommerce-events",
            )
            .option(
                "startingOffsets",
                "earliest",
            )
            .load()
        )

        events_df = (
            kafka_df.select(
                from_json(
                    col("value").cast("string"),
                    event_schema,
                ).alias("event")
            )
            .select("event.*")
        )

        query = (
            events_df.writeStream.format("parquet")
            .outputMode("append")
            .option(
                "path",
                BRONZE_PATH,
            )
            .option(
                "checkpointLocation",
                BRONZE_CHECKPOINT,
            )
            .start()
        )

        print("Bronze streaming started.")
        print("Waiting for Kafka events...")

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()