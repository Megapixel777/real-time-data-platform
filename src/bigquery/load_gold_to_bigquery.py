from google.cloud import bigquery
from google.cloud import storage


PROJECT_ID = "real-time-data-platform-507417"
DATASET_ID = "ecommerce"

TARGET_TABLE = "order_summary"
STAGING_TABLE = "order_summary_staging"
PROCESSED_TABLE = "order_summary_processed_batches"

BUCKET_NAME = "real-time-data-platform-thomasede"
GOLD_PREFIX = "gold/order_summary/"


def get_table_name(table_id: str) -> str:
    return f"{PROJECT_ID}.{DATASET_ID}.{table_id}"


def get_gold_files() -> list[str]:
    """Return only finalized Parquet files from Gold."""

    storage_client = storage.Client(project=PROJECT_ID)

    bucket = storage_client.bucket(BUCKET_NAME)

    blobs = bucket.list_blobs(prefix=GOLD_PREFIX)

    files = []

    for blob in blobs:
        name = blob.name

        if (
            name.endswith(".parquet")
            and "/_temporary/" not in name
            and not name.startswith(f"{GOLD_PREFIX}_")
        ):
            files.append(
                f"gs://{BUCKET_NAME}/{name}"
            )

    files.sort()

    return files


def create_tables(client: bigquery.Client) -> None:
    target_table = get_table_name(TARGET_TABLE)
    processed_table = get_table_name(PROCESSED_TABLE)

    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{target_table}` (
            order_id STRING,
            customer_id STRING,
            total_items INT64,
            total_amount NUMERIC
        )
        """
    ).result()

    client.query(
        f"""
        CREATE TABLE IF NOT EXISTS `{processed_table}` (
            batch_id INT64
        )
        """
    ).result()


def load_staging_table(
    client: bigquery.Client,
    gold_files: list[str],
) -> None:
    staging_table = get_table_name(STAGING_TABLE)

    if not gold_files:
        print("No finalized Gold Parquet files found.")
        return

    print(
        f"Loading {len(gold_files)} finalized Gold files..."
    )

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    job = client.load_table_from_uri(
        gold_files,
        staging_table,
        job_config=job_config,
    )

    job.result()

    print(
        "Gold data loaded into BigQuery staging."
    )


def get_new_batches(
    client: bigquery.Client,
) -> list[int]:
    staging_table = get_table_name(STAGING_TABLE)
    processed_table = get_table_name(PROCESSED_TABLE)

    query = f"""
        SELECT DISTINCT staging.batch_id
        FROM `{staging_table}` AS staging
        LEFT JOIN `{processed_table}` AS processed
            ON staging.batch_id = processed.batch_id
        WHERE processed.batch_id IS NULL
        ORDER BY staging.batch_id
    """

    rows = client.query(query).result()

    return [row.batch_id for row in rows]


def merge_batch(
    client: bigquery.Client,
    batch_id: int,
) -> None:
    target_table = get_table_name(TARGET_TABLE)
    staging_table = get_table_name(STAGING_TABLE)

    query = f"""
        MERGE `{target_table}` AS target

        USING (
            SELECT
                order_id,
                ANY_VALUE(customer_id) AS customer_id,
                SUM(total_items) AS total_items,
                SUM(total_amount) AS total_amount
            FROM `{staging_table}`
            WHERE batch_id = {batch_id}
            GROUP BY order_id
        ) AS source

        ON target.order_id = source.order_id

        WHEN MATCHED THEN
            UPDATE SET
                target.customer_id = source.customer_id,
                target.total_items =
                    target.total_items + source.total_items,
                target.total_amount =
                    target.total_amount + source.total_amount

        WHEN NOT MATCHED THEN
            INSERT (
                order_id,
                customer_id,
                total_items,
                total_amount
            )
            VALUES (
                source.order_id,
                source.customer_id,
                source.total_items,
                source.total_amount
            )
    """

    client.query(query).result()


def register_batch(
    client: bigquery.Client,
    batch_id: int,
) -> None:
    processed_table = get_table_name(PROCESSED_TABLE)

    query = f"""
        INSERT INTO `{processed_table}` (batch_id)
        SELECT {batch_id}
        FROM (
            SELECT 1 AS dummy
        )
        WHERE NOT EXISTS (
            SELECT 1
            FROM `{processed_table}`
            WHERE batch_id = {batch_id}
        )
    """

    client.query(query).result()

    print(f"Batch {batch_id} registered.")


def main() -> None:
    client = bigquery.Client(
        project=PROJECT_ID
    )

    print(
        "Starting Gold → BigQuery incremental load..."
    )

    create_tables(client)

    gold_files = get_gold_files()

    print(
        f"Found {len(gold_files)} finalized Gold Parquet files."
    )

    load_staging_table(
        client,
        gold_files,
    )

    new_batches = get_new_batches(client)

    if not new_batches:
        print("No new batches to process.")
        return

    print(
        f"New batches found: {new_batches}"
    )

    for batch_id in new_batches:
        print(
            f"Processing batch {batch_id}..."
        )

        merge_batch(
            client,
            batch_id,
        )

        register_batch(
            client,
            batch_id,
        )

        print(
            f"Batch {batch_id} processed successfully."
        )

    print(
        "Incremental load completed successfully."
    )


if __name__ == "__main__":
    main()