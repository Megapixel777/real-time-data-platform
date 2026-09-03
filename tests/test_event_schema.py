from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
)

from src.schemas.event_schema import event_schema


def test_event_schema_fields():

    assert len(event_schema.fields) == 8

    assert event_schema["event_id"].dataType == StringType()
    assert event_schema["event_id"].nullable is False

    assert event_schema["event_type"].dataType == StringType()
    assert event_schema["event_type"].nullable is False

    assert event_schema["order_id"].dataType == StringType()
    assert event_schema["order_id"].nullable is False

    assert event_schema["customer_id"].dataType == StringType()
    assert event_schema["customer_id"].nullable is True

    assert event_schema["product_id"].dataType == StringType()
    assert event_schema["product_id"].nullable is True

    assert event_schema["quantity"].dataType == IntegerType()
    assert event_schema["quantity"].nullable is True

    assert event_schema["unit_price"].dataType == DecimalType(10, 2)
    assert event_schema["unit_price"].nullable is True

    assert event_schema["event_timestamp"].dataType == StringType()
    assert event_schema["event_timestamp"].nullable is False
