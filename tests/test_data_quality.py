from decimal import Decimal

from pyspark.sql import SparkSession

from src.transformations.data_quality import (
    get_invalid_events,
    get_valid_events,
)


def test_data_quality():

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-data-quality")
        .getOrCreate()
    )

    data = [
        (
            "event-1",
            "order_item_added",
            "ORD-1000",
            "CUST-100",
            "PROD-1",
            2,
            10.50,
        ),
        (
            "event-2",
            "order_item_added",
            "ORD-1001",
            "CUST-101",
            "PROD-2",
            -1,
            20.00,
        ),
        (
            "event-3",
            "invalid_event",
            "ORD-1002",
            "CUST-102",
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
        "unit_price",
    ]

    df = spark.createDataFrame(data, columns)

    valid_df = get_valid_events(df)
    invalid_df = get_invalid_events(df)

    assert valid_df.count() == 1
    assert invalid_df.count() == 2

    assert valid_df.collect()[0]["event_id"] == "event-1"

    spark.stop()


def test_invalid_events_are_deduplicated():

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-quarantine-deduplication")
        .getOrCreate()
    )

    data = [
        (
            "invalid-001",
            "order_item_added",
            "ORD-001",
            "CUST-001",
            "PROD-001",
            -5,
            Decimal("100.00"),
        ),
        (
            "invalid-001",
            "order_item_added",
            "ORD-001",
            "CUST-001",
            "PROD-001",
            -5,
            Decimal("100.00"),
        ),
    ]

    columns = [
        "event_id",
        "event_type",
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
    ]

    df = spark.createDataFrame(data, columns)

    invalid_df = get_invalid_events(df).dropDuplicates(["event_id"])

    assert invalid_df.count() == 1

    result = invalid_df.collect()[0]

    assert result["event_id"] == "invalid-001"
    assert result["quantity"] == -5

    spark.stop()
