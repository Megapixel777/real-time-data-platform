from unittest.mock import Mock, patch

from src.streaming import bronze


@patch("src.streaming.bronze.from_json")
@patch("src.streaming.bronze.col")
@patch("src.streaming.bronze.SparkSession")
def test_bronze_main(
    mock_spark_session,
    mock_col,
    mock_from_json,
):
    mock_spark = Mock()

    # Todos los métodos del builder devuelven el propio builder
    mock_builder = Mock()
    mock_spark_session.builder = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_spark

    # PySpark functions
    mock_column = Mock()
    mock_col.return_value = mock_column

    mock_cast_column = Mock()
    mock_column.cast.return_value = mock_cast_column

    mock_json_column = Mock()
    mock_from_json.return_value = mock_json_column
    mock_json_column.alias.return_value = Mock()

    # Kafka DataFrame
    mock_kafka_df = Mock()

    (
        mock_spark.readStream.format.return_value
        .option.return_value
        .option.return_value
        .option.return_value
        .load.return_value
    ) = mock_kafka_df

    # Events DataFrame
    mock_events_df = Mock()
    mock_kafka_df.select.return_value.select.return_value = mock_events_df

    # Streaming query
    mock_query = Mock()

    (
        mock_events_df.writeStream.format.return_value
        .outputMode.return_value
        .option.return_value
        .option.return_value
        .start.return_value
    ) = mock_query

    bronze.main()

    # Spark
    mock_builder.appName.assert_called_once_with(
        "real-time-data-platform"
    )

    mock_builder.master.assert_called_once_with("local[*]")

    mock_builder.config.assert_any_call(
        "spark.jars.packages",
        bronze.KAFKA_PACKAGE,
    )

    mock_builder.getOrCreate.assert_called_once()

    # Kafka
    mock_spark.readStream.format.assert_called_once_with("kafka")

    mock_spark.readStream.format.return_value.option.assert_any_call(
        "kafka.bootstrap.servers",
        "localhost:9092",
    )

    # JSON
    mock_col.assert_called_once_with("value")
    mock_column.cast.assert_called_once_with("string")

    mock_from_json.assert_called_once_with(
        mock_cast_column,
        bronze.event_schema,
    )

    mock_json_column.alias.assert_called_once_with("event")

    # Streaming
    mock_events_df.writeStream.format.assert_called_once_with("parquet")

    mock_events_df.writeStream.format.return_value.outputMode.assert_called_once_with(
        "append"
    )

    mock_query.awaitTermination.assert_called_once()