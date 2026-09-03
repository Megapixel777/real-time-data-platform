from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from src.config.settings import BRONZE_CHECKPOINT, BRONZE_PATH
from src.schemas.event_schema import event_schema


KAFKA_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
)

GCS_CONNECTOR_JAR = (
    r"C:\Users\thoma\Proyectos\real-time-data-platform"
    r"\jars\gcs-connector-3.1.18-shaded.jar"
)

GCP_PROJECT_ID = "real-time-data-platform-507417"

GCP_CREDENTIALS = (
    r"C:\Users\thoma\.gcp\real-time-data-platform-sa.json"
)


def create_spark_session() -> SparkSession:

    spark = (
        SparkSession.builder
        .appName("real-time-data-platform")
        .master("local[*]")

        # Kafka
        .config(
            "spark.jars.packages",
            KAFKA_PACKAGE,
        )

        # GCS connector
        .config(
            "spark.jars",
            GCS_CONNECTOR_JAR,
        )

        # GCS filesystem
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )

        # GCP project
        .config(
            "spark.hadoop.fs.gs.project.id",
            GCP_PROJECT_ID,
        )

        # Authentication
        .config(
            "spark.hadoop.fs.gs.auth.type",
            "SERVICE_ACCOUNT_JSON_KEYFILE",
        )
        .config(
            "spark.hadoop.fs.gs.auth.service.account.json.keyfile",
            GCP_CREDENTIALS,
        )

        # Legacy authentication keys
        .config(
            "spark.hadoop.google.cloud.auth.type",
            "SERVICE_ACCOUNT_JSON_KEYFILE",
        )
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            GCP_CREDENTIALS,
        )

        # GCS
        .config(
            "spark.hadoop.fs.gs.block.size",
            "67108864",
        )

        # Python
        .config(
            "spark.pyspark.python",
            "python",
        )
        .config(
            "spark.pyspark.driver.python",
            "python",
        )

        # Parquet
        .config(
            "spark.sql.parquet.enableVectorizedReader",
            "false",
        )

        .config(
            "spark.sql.shuffle.partitions",
            "4",
        )

        .getOrCreate()
    )

    return spark


def main() -> None:

    spark = create_spark_session()

    try:

        print("Starting Bronze streaming...")
        print(f"BRONZE_PATH: {BRONZE_PATH}")
        print(f"BRONZE_CHECKPOINT: {BRONZE_CHECKPOINT}")

        kafka_df = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                "localhost:9092",
            )
            .option(
                "subscribe",
                "ecommerce-events",
            )
            .option(
                "startingOffsets",
                "earliest",
            )
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
            .option(
                "path",
                BRONZE_PATH,
            )
            .option(
                "checkpointLocation",
                BRONZE_CHECKPOINT,
            )
            .start()
        )

        print("Bronze streaming started.")
        print("Waiting for Kafka events...")

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()