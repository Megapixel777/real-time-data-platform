from pyspark.sql import SparkSession

from src.config.settings import (
    BRONZE_PATH,
    QUARANTINE_PATH,
    SILVER_CHECKPOINT,
    SILVER_PATH,
)
from src.transformations.data_quality import (
    get_invalid_events,
    get_valid_events,
)
from src.transformations.silver import transform_silver

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

GCS_CONNECTOR_JAR = (
    r"C:\Users\thoma\Proyectos\real-time-data-platform"
    r"\jars\gcs-connector-3.1.18-shaded.jar"
)

GCP_PROJECT_ID = "real-time-data-platform-507417"

GCP_CREDENTIALS = (
    r"C:\Users\thoma\.gcp\real-time-data-platform-sa.json"
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("silver-layer")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.jars", GCS_CONNECTOR_JAR)
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .config(
            "spark.hadoop.fs.gs.project.id",
            GCP_PROJECT_ID,
        )
        .config(
            "spark.hadoop.fs.gs.auth.type",
            "SERVICE_ACCOUNT_JSON_KEYFILE",
        )
        .config(
            "spark.hadoop.fs.gs.auth.service.account.json.keyfile",
            GCP_CREDENTIALS,
        )
        .config(
            "spark.hadoop.google.cloud.auth.type",
            "SERVICE_ACCOUNT_JSON_KEYFILE",
        )
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            GCP_CREDENTIALS,
        )
        .config(
            "spark.hadoop.fs.gs.block.size",
            "67108864",
        )
        .config(
            "spark.pyspark.python",
            "python",
        )
        .config(
            "spark.pyspark.driver.python",
            "python",
        )
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


def process_batch(batch_df, batch_id) -> None:
    valid_df = get_valid_events(batch_df)

    invalid_df = (
        get_invalid_events(batch_df)
        .dropDuplicates(["event_id"])
    )

    if not invalid_df.isEmpty():
        invalid_df.write.mode("append").parquet(
            QUARANTINE_PATH
        )

        print(
            f"Silver batch {batch_id}: "
            "invalid events sent to Quarantine"
        )

    if not valid_df.isEmpty():
        silver_df = transform_silver(valid_df)

        silver_df.write.mode("append").parquet(
            SILVER_PATH
        )

        print(
            f"Silver batch {batch_id}: "
            "valid events written to Silver"
        )


def main() -> None:
    spark = create_spark_session()

    try:
        print("Starting Silver streaming...")
        print(f"BRONZE_PATH: {BRONZE_PATH}")
        print(f"SILVER_PATH: {SILVER_PATH}")
        print(f"QUARANTINE_PATH: {QUARANTINE_PATH}")
        print(f"SILVER_CHECKPOINT: {SILVER_CHECKPOINT}")

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
            .load(BRONZE_PATH)
        )

        query = (
            bronze_df.writeStream
            .foreachBatch(process_batch)
            .option(
                "checkpointLocation",
                SILVER_CHECKPOINT,
            )
            .start()
        )

        print("Silver streaming started.")
        print("Waiting for Bronze events...")

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()