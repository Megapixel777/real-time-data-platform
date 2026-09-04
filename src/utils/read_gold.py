from google.cloud import bigquery

from src.config.settings import (
    BIGQUERY_DATASET,
    BIGQUERY_ORDER_SUMMARY_TABLE,
    GCP_PROJECT_ID,
)


def main() -> None:
    client = bigquery.Client(project=GCP_PROJECT_ID)

    table_ref = (
        f"{GCP_PROJECT_ID}."
        f"{BIGQUERY_DATASET}."
        f"{BIGQUERY_ORDER_SUMMARY_TABLE}"
    )

    query = f"""
        SELECT
            order_id,
            customer_id,
            total_items,
            total_amount
        FROM `{table_ref}`
        ORDER BY order_id
    """

    print(f"Reading BigQuery table: {table_ref}")

    query_job = client.query(query)
    rows = query_job.result()

    for row in rows:
        print(
            f"{row.order_id} | "
            f"{row.customer_id} | "
            f"{row.total_items} | "
            f"{row.total_amount}"
        )


if __name__ == "__main__":
    main()