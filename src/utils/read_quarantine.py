from pyspark.sql import SparkSession

QUARANTINE_PATH = "data/quarantine/events"


def main() -> None:

    spark = (
        SparkSession.builder.appName("read-quarantine").master("local[*]").getOrCreate()
    )

    df = spark.read.parquet(QUARANTINE_PATH)

    df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
