import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from src.config.settings import (
    GCP_CREDENTIALS,
    GCP_PROJECT_ID,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

GCS_CONNECTOR_JAR = (
    PROJECT_ROOT / "jars" / "gcs-connector-3.1.18-shaded.jar"
)

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

PYTHON_EXECUTABLE = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_EXECUTABLE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXECUTABLE
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS


def create_spark_session(
    app_name: str = "real-time-data-platform",
    master: str = "local[*]",
    include_kafka: bool = False,
) -> SparkSession:
    """
    Create a Spark session configured for local execution
    and Google Cloud Storage.
    """

    if not GCS_CONNECTOR_JAR.exists():
        raise FileNotFoundError(
            f"GCS Connector JAR not found: {GCS_CONNECTOR_JAR}"
        )

    credentials_path = Path(GCP_CREDENTIALS)

    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Google Service Account file not found: {credentials_path}"
        )

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config(
            "spark.jars",
            str(GCS_CONNECTOR_JAR),
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
            "spark.hadoop.google.cloud.auth.type",
            "SERVICE_ACCOUNT_JSON_KEYFILE",
        )
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            GCP_CREDENTIALS,
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
            "spark.hadoop.fs.gs.block.size",
            "67108864",
        )
        .config(
            "spark.hadoop.fs.gs.inputstream.support.enable",
            "false",
        )
        .config(
            "spark.pyspark.python",
            PYTHON_EXECUTABLE,
        )
        .config(
            "spark.executorEnv.PYSPARK_PYTHON",
            PYTHON_EXECUTABLE,
        )
        .config(
            "spark.sql.parquet.enableVectorizedReader",
            "false",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "4",
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )
    )

    if include_kafka:
        builder = builder.config(
            "spark.jars.packages",
            KAFKA_PACKAGE,
        )

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark