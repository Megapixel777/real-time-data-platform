from unittest.mock import Mock, patch

from src.streaming import gold


@patch("src.streaming.gold.lit")
@patch("src.streaming.gold.transform_to_order_summary")
def test_process_batch_with_empty_order_summary(
    mock_transform,
    mock_lit,
):
    batch_df = Mock()

    # Resultado de la transformación
    mock_order_summary_df = Mock()
    mock_transform.return_value = mock_order_summary_df

    # Resultado después de añadir batch_id
    mock_order_summary_with_batch = Mock()
    mock_order_summary_with_batch.isEmpty.return_value = True

    mock_order_summary_df.withColumn.return_value = mock_order_summary_with_batch

    gold.process_batch(batch_df, 1)

    # Comprobamos transformación
    mock_transform.assert_called_once_with(batch_df)

    # Comprobamos creación de batch_id
    mock_order_summary_df.withColumn.assert_called_once()

    # Comprobamos que se comprueba si está vacío
    mock_order_summary_with_batch.isEmpty.assert_called_once()

    # No debe escribir nada en Gold
    mock_order_summary_with_batch.write.mode.assert_not_called()


@patch("src.streaming.gold.lit")
@patch("src.streaming.gold.transform_to_order_summary")
def test_process_batch_writes_gold(
    mock_transform,
    mock_lit,
):
    batch_df = Mock()

    # Resultado de la transformación
    mock_order_summary_df = Mock()
    mock_transform.return_value = mock_order_summary_df

    # Resultado después de añadir batch_id
    mock_order_summary_with_batch = Mock()
    mock_order_summary_with_batch.isEmpty.return_value = False

    mock_order_summary_df.withColumn.return_value = mock_order_summary_with_batch

    gold.process_batch(batch_df, 2)

    # Comprobamos transformación
    mock_transform.assert_called_once_with(batch_df)

    # Comprobamos creación de batch_id
    mock_order_summary_df.withColumn.assert_called_once()

    # Comprobamos que se comprueba si está vacío
    mock_order_summary_with_batch.isEmpty.assert_called_once()

    # Comprobamos escritura en Gold
    mock_order_summary_with_batch.write.mode.assert_called_once_with("append")

    (
        mock_order_summary_with_batch.write.mode.return_value.parquet.assert_called_once_with(
            gold.GOLD_PATH
        )
    )


@patch("src.streaming.gold.SparkSession")
def test_gold_main(mock_spark_session):
    mock_builder = Mock()

    mock_spark_session.builder = mock_builder

    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder

    mock_spark = Mock()
    mock_builder.getOrCreate.return_value = mock_spark

    # DataFrame leído desde Silver
    mock_silver_df = Mock()

    (
        mock_spark.readStream.format.return_value.schema.return_value.load.return_value
    ) = mock_silver_df

    # Streaming query
    mock_query = Mock()

    (
        mock_silver_df.writeStream.foreachBatch.return_value.option.return_value.start.return_value
    ) = mock_query

    gold.main()

    # ------------------------------------------
    # Verificamos SparkSession
    # ------------------------------------------

    mock_builder.appName.assert_called_once_with("gold-layer")

    mock_builder.master.assert_called_once_with("local[*]")

    mock_builder.config.assert_any_call(
        "spark.sql.shuffle.partitions",
        "4",
    )

    mock_builder.getOrCreate.assert_called_once()

    # ------------------------------------------
    # Verificamos lectura Silver
    # ------------------------------------------

    mock_spark.readStream.format.assert_called_once_with("parquet")

    (mock_spark.readStream.format.return_value.schema.assert_called_once())

    (
        mock_spark.readStream.format.return_value.schema.return_value.load.assert_called_once_with(
            gold.SILVER_PATH
        )
    )

    # ------------------------------------------
    # Verificamos escritura streaming
    # ------------------------------------------

    mock_silver_df.writeStream.foreachBatch.assert_called_once_with(gold.process_batch)

    (
        mock_silver_df.writeStream.foreachBatch.return_value.option.assert_called_once_with(
            "checkpointLocation",
            gold.GOLD_CHECKPOINT,
        )
    )

    (
        mock_silver_df.writeStream.foreachBatch.return_value.option.return_value.start.assert_called_once()
    )

    # ------------------------------------------
    # Verificamos que el streaming queda activo
    # ------------------------------------------

    mock_query.awaitTermination.assert_called_once()

    # ------------------------------------------
    # Verificamos parada de Spark
    # ------------------------------------------

    mock_spark.stop.assert_called_once()
