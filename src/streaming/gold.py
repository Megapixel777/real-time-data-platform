from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from src.transformations.gold import transform_to_order_summary

GOLD_PATH = "data/gold/order_summary"


def process_batch(batch_df, batch_id) -> None:

    order_summary_df = transform_to_order_summary(
        batch_df
    )

    # Si no hay eventos order_item_added, no hacemos nada
    if order_summary_df.isEmpty():
        print(f"Gold batch {batch_id}: no order items")
        return

    # Primera ejecución: crear la tabla Delta
    if not DeltaTable.isDeltaTable(
        batch_df.sparkSession,
        GOLD_PATH,
    ):

        (
            order_summary_df
            .write
            .format("delta")
            .mode("overwrite")
            .save(GOLD_PATH)
        )

        print(f"Created Gold Delta table - batch {batch_id}")

    else:

        gold_table = DeltaTable.forPath(
            batch_df.sparkSession,
            GOLD_PATH,
        )

        (
            gold_table.alias("gold")
            .merge(
                order_summary_df.alias("updates"),
                "gold.order_id = updates.order_id",
            )
            .whenMatchedUpdate(
                set={
                    "customer_id": "updates.customer_id",
                    "total_items": (
                        "gold.total_items + updates.total_items"
                    ),
                    "total_amount": (
                        "gold.total_amount + updates.total_amount"
                    ),
                }
            )
            .whenNotMatchedInsert(
                values={
                    "order_id": "updates.order_id",
                    "customer_id": "updates.customer_id",
                    "total_items": "updates.total_items",
                    "total_amount": "updates.total_amount",
                }
            )
            .execute()
        )

        print(f"Merged Gold batch: {batch_id}")


def main() -> None:

    builder = (
        SparkSession.builder
        .appName("gold-layer")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = (
        configure_spark_with_delta_pip(builder)
        .getOrCreate()
    )

    silver_df = (
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
            event_timestamp TIMESTAMP,
            line_amount DECIMAL(12, 2),
            processing_timestamp TIMESTAMP
            """
        )
        .load("data/silver/events")
    )

    query = (
        silver_df
        .writeStream
        .foreachBatch(process_batch)
        .option(
            "checkpointLocation",
            "data/checkpoints/gold_order_summary",
        )
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()