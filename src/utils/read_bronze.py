from src.config.settings import BRONZE_PATH
from src.config.spark import create_spark_session


def main() -> None:
    spark = create_spark_session(
        app_name="read-bronze",
    )

    try:
        df = spark.read.parquet(BRONZE_PATH)

        print("=" * 70)
        print("BRONZE DATA")
        print("=" * 70)
        print(f"Rows: {df.count()}")
        print()

        df.printSchema()
        print()

        print("Sample events:")
        print()

        df.show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()