from pyspark.sql import SparkSession

from src.config.paths import GCS_BUCKET
from src.config.settings import GCP_CREDENTIALS, GCP_PROJECT_ID
from src.config.spark import create_spark_session


def stop_spark_session(
    spark: SparkSession | None,
) -> None:
    """Stop the Spark session."""
    if spark is not None:
        spark.stop()


def test_gcs(spark: SparkSession) -> None:
    """Test writing and reading a Parquet DataFrame to/from GCS."""
    test_path = f"gs://{GCS_BUCKET}/test/spark"

    print()
    print("Creating test DataFrame...")

    df = spark.createDataFrame(
        [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie"),
        ],
        ["id", "name"],
    )

    print()
    print("Test DataFrame:")
    df.show()

    print()
    print("Writing DataFrame to GCS:")
    print(test_path)
    print()

    df.write.mode("overwrite").parquet(test_path)

    print("SUCCESS: DataFrame written to GCS")

    print()
    print("Reading DataFrame from GCS...")

    result_df = spark.read.parquet(test_path)

    print()
    print("Data read from GCS:")
    result_df.show()

    print()
    print("SUCCESS: GCS READ/WRITE TEST PASSED")


def main() -> None:
    print()
    print("=" * 60)
    print("SPARK + GCS TEST")
    print("=" * 60)

    print()
    print("GCP PROJECT:")
    print(GCP_PROJECT_ID)

    print()
    print("SERVICE ACCOUNT:")
    print(GCP_CREDENTIALS)

    print()
    print("GCS BUCKET:")
    print(GCS_BUCKET)

    print()
    print("Creating Spark session...")
    print()

    spark = None

    try:
        spark = create_spark_session(
            app_name="spark-gcs-check",
        )

        print("Spark session created successfully.")

        print()
        print("Spark version:")
        print(spark.version)

        print()
        print("Testing Spark...")

        test_df = spark.createDataFrame(
            [
                (1, "Alice"),
                (2, "Bob"),
                (3, "Charlie"),
            ],
            ["id", "name"],
        )

        test_df.show()

        print()
        print("SUCCESS: SPARK TEST PASSED")

        test_gcs(spark)

    finally:
        if spark is not None:
            print()
            print("Stopping Spark...")

            stop_spark_session(spark)

            print("Spark stopped.")


if __name__ == "__main__":
    main()