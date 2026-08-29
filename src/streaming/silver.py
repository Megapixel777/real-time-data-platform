from pyspark.sql import SparkSession

from src.transformations.silver import transform_silver


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

    silver_df = transform_silver(bronze_df)

    query = (
        silver_df
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", "data/silver/events")
        .option(
            "checkpointLocation",
            "data/checkpoints/silver",
        )
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()