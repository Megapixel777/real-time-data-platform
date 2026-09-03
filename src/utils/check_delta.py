from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def main() -> None:
    builder = SparkSession.builder.appName("delta-test").master("local[*]")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    print("Delta Lake configured successfully!")

    spark.stop()


if __name__ == "__main__":
    main()
