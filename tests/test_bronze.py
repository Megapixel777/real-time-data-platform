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
    # SparkSession mock
    mock_spark = Mock()

    (
        mock_spark_session.builder.appName.return_value.master.return_value.config.return_value.getOrCreate.return_value
    ) = mock_spark

    # Mock de las funciones PySpark
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
        mock_spark.readStream.format.return_value.option.return_value.option.return_value.load.return_value
    ) = mock_kafka_df

    # Events DataFrame
    mock_events_df = Mock()

    mock_kafka_df.select.return_value.select.return_value = mock_events_df

    # Streaming query
    mock_query = Mock()

    (
        mock_events_df.writeStream.format.return_value.outputMode.return_value.option.return_value.option.return_value.start.return_value
    ) = mock_query

    # Ejecutamos el código real
    bronze.main()

    # --------------------------------------------------
    # Verificamos SparkSession
    # --------------------------------------------------

    mock_spark_session.builder.appName.assert_called_once_with(
        "real-time-data-platform"
    )

    (
        mock_spark_session.builder.appName.return_value.master.assert_called_once_with(
            "local[*]"
        )
    )

    (
        mock_spark_session.builder.appName.return_value.master.return_value.config.assert_called_once_with(
            "spark.jars.packages",
            bronze.KAFKA_PACKAGE,
        )
    )

    (
        mock_spark_session.builder.appName.return_value.master.return_value.config.return_value.getOrCreate.assert_called_once()
    )

    # --------------------------------------------------
    # Verificamos Kafka
    # --------------------------------------------------

    mock_spark.readStream.format.assert_called_once_with("kafka")

    mock_spark.readStream.format.return_value.option.assert_any_call(
        "kafka.bootstrap.servers",
        "localhost:9092",
    )

    (
        mock_spark.readStream.format.return_value.option.return_value.option.assert_called_once_with(
            "subscribe",
            "ecommerce-events",
        )
    )

    # --------------------------------------------------
    # Verificamos transformación JSON
    # --------------------------------------------------

    mock_col.assert_called_once_with("value")

    mock_column.cast.assert_called_once_with("string")

    mock_from_json.assert_called_once_with(
        mock_cast_column,
        bronze.event_schema,
    )

    mock_json_column.alias.assert_called_once_with("event")

    # --------------------------------------------------
    # Verificamos salida Parquet
    # --------------------------------------------------

    mock_events_df.writeStream.format.assert_called_once_with("parquet")

    (
        mock_events_df.writeStream.format.return_value.outputMode.assert_called_once_with(
            "append"
        )
    )

    # --------------------------------------------------
    # Verificamos que inicia el streaming
    # --------------------------------------------------

    mock_query.awaitTermination.assert_called_once()
