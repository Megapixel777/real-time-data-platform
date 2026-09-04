from unittest.mock import Mock, patch

from src.streaming import gold


@patch("src.streaming.gold.lit")
@patch("src.streaming.gold.transform_to_order_summary")
def test_process_batch_with_empty_order_summary(
    mock_transform,
    mock_lit,
):
    batch_df = Mock()

    # Result of the transformation
    mock_order_summary_df = Mock()
    mock_transform.return_value = mock_order_summary_df

    # Result after adding batch_id
    mock_order_summary_with_batch = Mock()
    mock_order_summary_with_batch.isEmpty.return_value = True

    mock_order_summary_df.withColumn.return_value = (
        mock_order_summary_with_batch
    )

    gold.process_batch(batch_df, 1)

    # Verify transformation
    mock_transform.assert_called_once_with(batch_df)

    # Verify batch_id creation
    mock_order_summary_df.withColumn.assert_called_once()

    # Verify that the DataFrame is checked for emptiness
    mock_order_summary_with_batch.isEmpty.assert_called_once()

    # Nothing should be written to Gold
    mock_order_summary_with_batch.write.mode.assert_not_called()


@patch("src.streaming.gold.lit")
@patch("src.streaming.gold.transform_to_order_summary")
def test_process_batch_writes_gold(
    mock_transform,
    mock_lit,
):
    batch_df = Mock()

    # Result of the transformation
    mock_order_summary_df = Mock()
    mock_transform.return_value = mock_order_summary_df

    # Result after adding batch_id
    mock_order_summary_with_batch = Mock()
    mock_order_summary_with_batch.isEmpty.return_value = False

    mock_order_summary_df.withColumn.return_value = (
        mock_order_summary_with_batch
    )

    gold.process_batch(batch_df, 2)

    # Verify transformation
    mock_transform.assert_called_once_with(batch_df)

    # Verify batch_id creation
    mock_order_summary_df.withColumn.assert_called_once()

    # Verify that the DataFrame is checked for emptiness
    mock_order_summary_with_batch.isEmpty.assert_called_once()

    # Verify writing to Gold
    mock_order_summary_with_batch.write.mode.assert_called_once_with(
        "append"
    )

    (
        mock_order_summary_with_batch.write.mode
        .return_value.parquet.assert_called_once_with(
            gold.GOLD_PATH
        )
    )


@patch("src.streaming.gold.create_gold_spark_session")
def test_gold_main(mock_create_gold_spark_session):
    # --------------------------------------------------
    # Mock SparkSession
    # --------------------------------------------------

    mock_spark = Mock()
    mock_create_gold_spark_session.return_value = mock_spark

    # --------------------------------------------------
    # Mock reading from Silver
    # --------------------------------------------------

    mock_silver_df = Mock()

    (
        mock_spark.readStream.format.return_value
        .schema.return_value
        .load.return_value
    ) = mock_silver_df

    # --------------------------------------------------
    # Mock Streaming Query
    # --------------------------------------------------

    mock_query = Mock()

    (
        mock_silver_df.writeStream.foreachBatch.return_value
        .option.return_value
        .start.return_value
    ) = mock_query

    # --------------------------------------------------
    # Execute the real main function
    # --------------------------------------------------

    gold.main()

    # ==================================================
    # Verify Spark session creation
    # ==================================================

    mock_create_gold_spark_session.assert_called_once_with()

    # ==================================================
    # Verify Silver reading
    # ==================================================

    mock_spark.readStream.format.assert_called_once_with("parquet")

    mock_spark.readStream.format.return_value.schema.assert_called_once()

    (
        mock_spark.readStream
        .format.return_value
        .schema.return_value
        .load
    ).assert_called_once_with(
        gold.SILVER_PATH
    )

    # ==================================================
    # Verify streaming write
    # ==================================================

    mock_silver_df.writeStream.foreachBatch.assert_called_once_with(
        gold.process_batch
    )

    (
        mock_silver_df.writeStream
        .foreachBatch.return_value
        .option
    ).assert_called_once_with(
        "checkpointLocation",
        gold.GOLD_CHECKPOINT,
    )

    (
        mock_silver_df.writeStream
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