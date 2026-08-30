from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

event_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("event_timestamp", StringType(), False),
    ]
)