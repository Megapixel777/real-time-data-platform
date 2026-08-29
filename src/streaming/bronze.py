from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from src.schemas.event_schema import event_schema


KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("real-time-data-platform")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .getOrCreate()
    )

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "ecommerce-events")
        .load()
    )

    events_df = (
        kafka_df
        .select(
            from_json(
                col("value").cast("string"),
                event_schema,
            ).alias("event")
        )
        .select("event.*")
    )

    query = (
        events_df
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", "data/bronze/events")
        .option(
            "checkpointLocation",
            "data/checkpoints/bronze",
        )
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()