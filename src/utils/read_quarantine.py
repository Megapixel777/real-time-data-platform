from src.config.settings import QUARANTINE_PATH
from src.config.spark import create_spark_session


def main() -> None:
    spark = create_spark_session(
        app_name="read-quarantine",
    )

    try:
        df = spark.read.parquet(QUARANTINE_PATH)

        df.show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()