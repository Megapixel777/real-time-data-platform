import os
import sys
import shutil
from pathlib import Path

from pyspark.sql import SparkSession


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

JARS_DIR = PROJECT_ROOT / "jars"

GCS_CONNECTOR_JAR = (
    JARS_DIR
    / "gcs-connector-3.1.18-shaded.jar"
)


# ============================================================
# GCP CONFIGURATION
# ============================================================

GCP_PROJECT_ID = "real-time-data-platform-507417"

GCS_BUCKET = "real-time-data-platform-thomasede"

GCS_SERVICE_ACCOUNT_FILE = (
    Path.home()
    / ".gcp"
    / "real-time-data-platform-sa.json"
)


# ============================================================
# PYTHON CONFIGURATION
# ============================================================

PYTHON_EXECUTABLE = sys.executable


# ============================================================
# EXPORT ENVIRONMENT VARIABLES
# ============================================================

# Force Spark Python workers to use the same Python
# interpreter as the driver.

os.environ["PYSPARK_PYTHON"] = PYTHON_EXECUTABLE

os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXECUTABLE


# Google credentials available to the local environment.

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(
    GCS_SERVICE_ACCOUNT_FILE
)


# ============================================================
# CREATE SPARK SESSION
# ============================================================

def get_spark_session(
    app_name: str = "real-time-data-platform",
    master: str = "local[*]",
) -> SparkSession:
    """
    Create a local Spark session configured for:

    - PySpark
    - Google Cloud Storage
    - Service Account authentication
    - Consistent Python environment
    - Parquet compatibility
    """

    # --------------------------------------------------------
    # VALIDATION
    # --------------------------------------------------------

    if not GCS_CONNECTOR_JAR.exists():

        raise FileNotFoundError(
            f"""
GCS Connector JAR not found.

Expected path:

{GCS_CONNECTOR_JAR}
"""
        )

    if not GCS_SERVICE_ACCOUNT_FILE.exists():

        raise FileNotFoundError(
            f"""
Google Service Account file not found.

Expected path:

{GCS_SERVICE_ACCOUNT_FILE}
"""
        )

    # --------------------------------------------------------
    # CREATE SESSION
    # --------------------------------------------------------

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)

        # ====================================================
        # GCS CONNECTOR
        # ====================================================

        .config(
            "spark.jars",
            str(GCS_CONNECTOR_JAR),
        )

        # ====================================================
        # GOOGLE CLOUD STORAGE FILESYSTEM
        # ====================================================

        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )

        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )

        # ====================================================
        # GOOGLE CLOUD PROJECT
        # ====================================================

        .config(
            "spark.hadoop.fs.gs.project.id",
            GCP_PROJECT_ID,
        )

        # ====================================================
        # SERVICE ACCOUNT AUTHENTICATION
        # ====================================================

        .config(
            "spark.hadoop.google.cloud.auth.type",
            "SERVICE_ACCOUNT_JSON_KEYFILE",
        )

        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            str(GCS_SERVICE_ACCOUNT_FILE),
        )

        .config(
            "spark.hadoop.fs.gs.auth.type",
            "SERVICE_ACCOUNT_JSON_KEYFILE",
        )

        .config(
            "spark.hadoop.fs.gs.auth.service.account.json.keyfile",
            str(GCS_SERVICE_ACCOUNT_FILE),
        )

        # ====================================================
        # GCS PERFORMANCE
        # ====================================================

        .config(
            "spark.hadoop.fs.gs.block.size",
            "67108864",
        )

        # ====================================================
        # DISABLE GCS VECTORED READS
        # ====================================================

        # Avoid compatibility problems between the GCS
        # connector and Hadoop/Spark versions.

        .config(
            "spark.hadoop.fs.gs.inputstream.support.enable",
            "false",
        )

        # ====================================================
        # PYTHON CONFIGURATION
        # ====================================================

        .config(
            "spark.pyspark.python",
            PYTHON_EXECUTABLE,
        )

        .config(
            "spark.executorEnv.PYSPARK_PYTHON",
            PYTHON_EXECUTABLE,
        )

        # ====================================================
        # PARQUET COMPATIBILITY
        # ====================================================

        # Disable Spark vectorized Parquet reader.

        .config(
            "spark.sql.parquet.enableVectorizedReader",
            "false",
        )

        # ====================================================
        # GENERAL SPARK CONFIGURATION
        # ====================================================

        .config(
            "spark.sql.shuffle.partitions",
            "4",
        )

        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# STOP SPARK SESSION
# ============================================================

def stop_spark_session(
    spark: SparkSession | None,
) -> None:
    """
    Stop the Spark session safely.
    """

    if spark is not None:

        try:

            spark.stop()

        except Exception:

            pass


# ============================================================
# TEST GCS
# ============================================================

def test_gcs(
    spark: SparkSession,
) -> None:
    """
    Test writing and reading a Parquet DataFrame
    to/from Google Cloud Storage.
    """

    test_path = (
        f"gs://{GCS_BUCKET}/test/spark"
    )

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

    (
        df.write
        .mode("overwrite")
        .parquet(test_path)
    )

    print("SUCCESS: DataFrame written to GCS")

    print()

    print("Reading DataFrame from GCS...")

    result_df = (
        spark.read
        .parquet(test_path)
    )

    print()

    print("Data read from GCS:")

    result_df.show()

    print()

    print("SUCCESS: GCS READ/WRITE TEST PASSED")


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    print()

    print("=" * 60)

    print("SPARK + GCS TEST")

    print("=" * 60)

    print()

    print("PROJECT ROOT:")

    print(PROJECT_ROOT)

    print()

    print("GCS CONNECTOR:")

    print(GCS_CONNECTOR_JAR)

    print()

    print("SERVICE ACCOUNT:")

    print(GCS_SERVICE_ACCOUNT_FILE)

    print()

    print("GCS BUCKET:")

    print(GCS_BUCKET)

    print()

    print("PYTHON EXECUTABLE:")

    print(PYTHON_EXECUTABLE)

    print()

    print("=" * 60)

    print()

    spark = None

    try:

        print("Creating Spark session...")

        spark = get_spark_session()

        print()

        print(
            "Spark session created successfully."
        )

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

        # ====================================================
        # TEST GCS
        # ====================================================

        test_gcs(spark)

    finally:

        if spark is not None:

            print()

            print("Stopping Spark...")

            stop_spark_session(spark)

            print("Spark stopped.")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()