from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("read-gold")
        .master("local[*]")
        .getOrCreate()
    )

    df = spark.read.parquet("data/gold/order_summary")

    df.orderBy("order_id").show(
        100,
        truncate=False,
    )

    spark.stop()


if __name__ == "__main__":
    main()