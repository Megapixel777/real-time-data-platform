from decimal import Decimal

from pyspark.sql import SparkSession

from src.transformations.gold import transform_to_order_summary


def test_gold_transformation():

    spark = SparkSession.builder.master("local[2]").appName("test-gold").getOrCreate()

    data = [
        (
            "event-1",
            "order_item_added",
            "ORD-1000",
            "CUST-100",
            "PROD-1",
            2,
            Decimal("10.50"),
        ),
        (
            "event-2",
            "order_item_added",
            "ORD-1000",
            "CUST-100",
            "PROD-2",
            3,
            Decimal("20.00"),
        ),
        (
            "event-3",
            "order_created",
            "ORD-1000",
            "CUST-100",
            None,
            None,
            None,
        ),
    ]

    columns = [
        "event_id",
        "event_type",
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "line_amount",
    ]

    df = spark.createDataFrame(
        data,
        columns,
    )

    gold_df = transform_to_order_summary(df)

    result = gold_df.collect()[0]

    assert result["order_id"] == "ORD-1000"
    assert result["customer_id"] == "CUST-100"
    assert result["total_items"] == 5
    assert result["total_amount"] == Decimal("30.50")

    spark.stop()
