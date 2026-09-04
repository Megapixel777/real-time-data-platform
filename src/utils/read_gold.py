from google.cloud import bigquery

PROJECT_ID = "real-time-data-platform-507417"
DATASET_ID = "ecommerce"
TABLE_ID = "order_summary"


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

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
