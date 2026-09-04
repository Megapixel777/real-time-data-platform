from unittest.mock import Mock, patch

from src.streaming import silver


@patch("src.streaming.silver.transform_silver")
@patch("src.streaming.silver.get_invalid_events")
@patch("src.streaming.silver.get_valid_events")
def test_process_batch_with_valid_and_invalid_events(
    mock_get_valid_events,
    mock_get_invalid_events,
    mock_transform_silver,
):
    batch_df = Mock()

    # There are valid events
    mock_valid_df = Mock()
    mock_valid_df.isEmpty.return_value = False
    mock_get_valid_events.return_value = mock_valid_df

    # There are invalid events
    mock_invalid_df = Mock()
    mock_invalid_df.dropDuplicates.return_value = mock_invalid_df
    mock_invalid_df.isEmpty.return_value = False
    mock_get_invalid_events.return_value = mock_invalid_df

    # Transformed DataFrame for Silver
    mock_silver_df = Mock()
    mock_transform_silver.return_value = mock_silver_df

    # Execute
    silver.process_batch(batch_df, 1)

    # Verify validation
    mock_get_valid_events.assert_called_once_with(batch_df)
    mock_get_invalid_events.assert_called_once_with(batch_df)

    # Verify duplicate removal
    mock_invalid_df.dropDuplicates.assert_called_once_with(["event_id"])

    # Verify writing to Quarantine
    mock_invalid_df.write.mode.assert_called_once_with("append")

    mock_invalid_df.write.mode.return_value.parquet.assert_called_once_with(
        silver.QUARANTINE_PATH
    )

    # Verify Silver transformation
    mock_transform_silver.assert_called_once_with(mock_valid_df)

    # Verify writing to Silver
    mock_silver_df.write.mode.assert_called_once_with("append")

    mock_silver_df.write.mode.return_value.parquet.assert_called_once_with(
        silver.SILVER_PATH
    )


@patch("src.streaming.silver.transform_silver")
@patch("src.streaming.silver.get_invalid_events")
@patch("src.streaming.silver.get_valid_events")
def test_process_batch_with_only_valid_events(
    mock_get_valid_events,
    mock_get_invalid_events,
    mock_transform_silver,
):
    batch_df = Mock()

    # There are valid events
    mock_valid_df = Mock()
    mock_valid_df.isEmpty.return_value = False
    mock_get_valid_events.return_value = mock_valid_df

    # There are no invalid events
    mock_invalid_df = Mock()
    mock_invalid_df.dropDuplicates.return_value = mock_invalid_df
    mock_invalid_df.isEmpty.return_value = True
    mock_get_invalid_events.return_value = mock_invalid_df

    # Result of the Silver transformation
    mock_silver_df = Mock()
    mock_transform_silver.return_value = mock_silver_df

    # Execute
    silver.process_batch(batch_df, 2)

    # Quarantine should not be written
    mock_invalid_df.write.mode.assert_not_called()

    # Valid events should be transformed
    mock_transform_silver.assert_called_once_with(mock_valid_df)

    # Silver should be written
    mock_silver_df.write.mode.assert_called_once_with("append")

    mock_silver_df.write.mode.return_value.parquet.assert_called_once_with(
        silver.SILVER_PATH
    )


@patch("src.streaming.silver.transform_silver")
@patch("src.streaming.silver.get_invalid_events")
@patch("src.streaming.silver.get_valid_events")
def test_process_batch_with_only_invalid_events(
    mock_get_valid_events,
    mock_get_invalid_events,
    mock_transform_silver,
):
    batch_df = Mock()

    # There are no valid events
    mock_valid_df = Mock()
    mock_valid_df.isEmpty.return_value = True
    mock_get_valid_events.return_value = mock_valid_df

    # There are invalid events
    mock_invalid_df = Mock()
    mock_invalid_df.dropDuplicates.return_value = mock_invalid_df
    mock_invalid_df.isEmpty.return_value = False
    mock_get_invalid_events.return_value = mock_invalid_df

    # Execute
    silver.process_batch(batch_df, 3)

    # Invalid events should be written to Quarantine
    mock_invalid_df.write.mode.assert_called_once_with("append")

    mock_invalid_df.write.mode.return_value.parquet.assert_called_once_with(
        silver.QUARANTINE_PATH
    )

    # Valid events should not be transformed
    mock_transform_silver.assert_not_called()


@patch("src.streaming.silver.create_silver_spark_session")
def test_silver_main(mock_create_silver_spark_session):
    # --------------------------------------------------
    # Mock SparkSession
    # --------------------------------------------------

    mock_spark = Mock()
    mock_create_silver_spark_session.return_value = mock_spark

    # --------------------------------------------------
    # Mock reading from Bronze
    # --------------------------------------------------

    mock_bronze_df = Mock()

    (
        mock_spark.readStream.format.return_value
        .schema.return_value
        .load.return_value
    ) = mock_bronze_df

    # --------------------------------------------------
    # Mock Streaming Query
    # --------------------------------------------------

    mock_query = Mock()

    (
        mock_bronze_df.writeStream.foreachBatch.return_value
        .option.return_value
        .start.return_value
    ) = mock_query

    # --------------------------------------------------
    # Execute the real main function
    # --------------------------------------------------

    silver.main()

    # ==================================================
    # Verify Spark session creation
    # ==================================================

    mock_create_silver_spark_session.assert_called_once_with()

    # ==================================================
    # Verify Bronze reading
    # ==================================================

    mock_spark.readStream.format.assert_called_once_with("parquet")

    mock_spark.readStream.format.return_value.schema.assert_called_once()

    (
        mock_spark.readStream
        .format.return_value
        .schema.return_value
        .load
    ).assert_called_once_with(
        silver.BRONZE_PATH
    )

    # ==================================================
    # Verify streaming write
    # ==================================================

    mock_bronze_df.writeStream.foreachBatch.assert_called_once_with(
        silver.process_batch
    )

    (
        mock_bronze_df.writeStream
        .foreachBatch.return_value
        .option
    ).assert_called_once_with(
        "checkpointLocation",
        silver.SILVER_CHECKPOINT,
    )

    (
        mock_bronze_df.writeStream
        .foreachBatch.return_value
        .option.return_value
        .start
    ).assert_called_once()

    # ==================================================
    # Verify streaming remains active
    # ==================================================

    mock_query.awaitTermination.assert_called_once()

    # ==================================================
    # Verify Spark is stopped
    # ==================================================

    mock_spark.stop.assert_called_once()