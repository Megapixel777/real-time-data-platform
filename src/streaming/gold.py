from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from src.config.settings import (
    GOLD_CHECKPOINT,
    GOLD_PATH,
    SILVER_PATH,
)
from src.transformations.gold import transform_to_order_summary

GCS_CONNECTOR_JAR = (
    r"C:\Users\thoma\Proyectos\real-time-data-platform"
    r"\jars\gcs-connector-3.1.18-shaded.jar"
)

GCP_PROJECT_ID = "real-time-data-platform-507417"

GCP_CREDENTIALS = r"C:\Users\thoma\.gcp\real-time-data-platform-sa.json"


def create_spark_session() -> SparkSession:
    """Create Spark session configured for GCS."""

    return (
        SparkSession.builder.appName("gold-layer")
        .master("local[*]")
        .config(
            "spark.jars",
            GCS_CONNECTOR_JAR,
        )
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
    """Process one Silver micro-batch."""

    order_summary_df = transform_to_order_summary(batch_df).withColumn(
        "batch_id", lit(batch_id)
    )

    if order_summary_df.isEmpty():
        print(f"Gold batch {batch_id}: no order items")
        return

    (order_summary_df.write.mode("append").parquet(GOLD_PATH))

    print(f"Gold batch {batch_id}: order summary written to Gold")


def main() -> None:
    """Start the Gold streaming pipeline."""

    spark = create_spark_session()

    try:
        print("Starting Gold streaming...")
        print(f"SILVER_PATH: {SILVER_PATH}")
        print(f"GOLD_PATH: {GOLD_PATH}")
        print(f"GOLD_CHECKPOINT: {GOLD_CHECKPOINT}")

        silver_df = (
            spark.readStream.format("parquet")
            .schema(
                """
                event_id STRING,
                event_type STRING,
                order_id STRING,
                customer_id STRING,
                product_id STRING,
                quantity INT,
                unit_price DECIMAL(10, 2),
                event_timestamp TIMESTAMP,
                line_amount DECIMAL(12, 2),
                processing_timestamp TIMESTAMP
                """
            )
            .load(SILVER_PATH)
        )

        query = (
            silver_df.writeStream.foreachBatch(process_batch)
            .option(
                "checkpointLocation",
                GOLD_CHECKPOINT,
            )
            .start()
        )

        print("Gold streaming started.")
        print("Waiting for Silver events...")

        query.awaitTermination()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
