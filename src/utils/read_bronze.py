from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("read-bronze")
        .master("local[*]")
        .getOrCreate()
    )

    df = spark.read.parquet("data/bronze/events")

    df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()