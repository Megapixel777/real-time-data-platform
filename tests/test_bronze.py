from unittest.mock import Mock, patch

from src.streaming import bronze


@patch("src.streaming.bronze.from_json")
@patch("src.streaming.bronze.col")
@patch("src.streaming.bronze.create_spark_session")
def test_bronze_main(
    mock_create_spark_session,
    mock_col,
    mock_from_json,
):
    mock_spark = Mock()
    mock_create_spark_session.return_value = mock_spark

    # PySpark functions
    mock_column = Mock()
    mock_col.return_value = mock_column

    mock_cast_column = Mock()
    mock_column.cast.return_value = mock_cast_column

    mock_json_column = Mock()
    mock_from_json.return_value = mock_json_column

    mock_alias_column = Mock()
    mock_json_column.alias.return_value = mock_alias_column

    # Kafka DataFrame
    mock_kafka_df = Mock()

    (
        mock_spark.readStream
        .format.return_value
        .option.return_value
        .option.return_value
        .option.return_value
        .load.return_value
    ) = mock_kafka_df

    # Events DataFrame
    mock_events_df = Mock()

    (
        mock_kafka_df.select.return_value.select.return_value
    ) = mock_events_df

    # Streaming query
    mock_query = Mock()

    (
        mock_events_df.writeStream
        .format.return_value
        .outputMode.return_value
        .option.return_value
        .option.return_value
        .start.return_value
    ) = mock_query

    bronze.main()

    mock_create_spark_session.assert_called_once_with(
        app_name="real-time-data-platform",
        include_kafka=True,
    )

    mock_spark.readStream.format.assert_called_once_with("kafka")

    mock_query.awaitTermination.assert_called_once()

    mock_spark.stop.assert_called_once()