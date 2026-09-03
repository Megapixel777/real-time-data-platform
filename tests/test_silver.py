from pyspark.sql import SparkSession

from src.transformations.silver import transform_silver


def test_silver_transformation():

    spark = SparkSession.builder.master("local[2]").appName("test-silver").getOrCreate()

    data = [
        (
            "event-1",
            "order_item_added",
            "ORD-1000",
            "CUST-100",
            "PROD-1",
            2,
            10.50,
            "2026-08-29T10:00:00",
        )
    ]

    columns = [
        "event_id",
        "event_type",
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "event_timestamp",
    ]

    df = spark.createDataFrame(
        data,
        columns,
    )

    silver_df = transform_silver(df)

    result = silver_df.collect()[0]

    assert result["event_id"] == "event-1"
    assert result["order_id"] == "ORD-1000"
    assert result["quantity"] == 2
    assert float(result["line_amount"]) == 21.00
    assert result["processing_timestamp"] is not None

    spark.stop()
