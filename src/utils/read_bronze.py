from pyspark.sql import SparkSession


BRONZE_PATH = (
    "gs://real-time-data-platform-thomasede/bronze/events"
)

GCS_CONNECTOR_JAR = (
    r"C:\Users\thoma\Proyectos\real-time-data-platform"
    r"\jars\gcs-connector-3.1.18-shaded.jar"
)

GCP_PROJECT_ID = "real-time-data-platform-507417"

GCP_CREDENTIALS = (
    r"C:\Users\thoma\.gcp\real-time-data-platform-sa.json"
)


def main() -> None:

    spark = (
        SparkSession.builder
        .appName("read-bronze")
        .master("local[*]")
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
        .getOrCreate()
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