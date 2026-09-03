from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("read-silver").master("local[*]").getOrCreate()

    df = spark.read.parquet("data/silver/events")

    df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
