from unittest.mock import Mock, patch

from src.streaming import gold


@patch("src.streaming.gold.transform_to_order_summary")
def test_process_batch_with_empty_order_summary(
    mock_transform,
):
    batch_df = Mock()

    mock_order_summary_df = Mock()
    mock_order_summary_df.isEmpty.return_value = True

    mock_transform.return_value = mock_order_summary_df

    gold.process_batch(batch_df, 1)

    mock_transform.assert_called_once_with(batch_df)
    mock_order_summary_df.isEmpty.assert_called_once()


@patch("src.streaming.gold.DeltaTable")
@patch("src.streaming.gold.transform_to_order_summary")
def test_process_batch_creates_delta_table(
    mock_transform,
    mock_delta_table,
):
    batch_df = Mock()
    batch_df.sparkSession = Mock()

    mock_order_summary_df = Mock()
    mock_order_summary_df.isEmpty.return_value = False

    mock_transform.return_value = mock_order_summary_df

    # La tabla Delta todavía no existe
    mock_delta_table.isDeltaTable.return_value = False

    gold.process_batch(batch_df, 2)

    # Se transforma el batch
    mock_transform.assert_called_once_with(batch_df)

    # Se comprueba si existe la tabla Delta
    mock_delta_table.isDeltaTable.assert_called_once_with(
        batch_df.sparkSession,
        gold.GOLD_PATH,
    )

    # Se crea la tabla Delta
    mock_order_summary_df.write.format.assert_called_once_with("delta")

    (
        mock_order_summary_df.write.format.return_value.mode.assert_called_once_with(
            "overwrite"
        )
    )

    (
        mock_order_summary_df.write.format.return_value.mode.return_value.save.assert_called_once_with(
            gold.GOLD_PATH
        )
    )


@patch("src.streaming.gold.DeltaTable")
@patch("src.streaming.gold.transform_to_order_summary")
def test_process_batch_merges_existing_delta_table(
    mock_transform,
    mock_delta_table,
):
    batch_df = Mock()
    batch_df.sparkSession = Mock()

    mock_order_summary_df = Mock()
    mock_order_summary_df.isEmpty.return_value = False

    mock_transform.return_value = mock_order_summary_df

    # La tabla Delta ya existe
    mock_delta_table.isDeltaTable.return_value = True

    # Mock de la tabla Gold existente
    mock_gold_table = Mock()

    mock_delta_table.forPath.return_value = mock_gold_table

    gold.process_batch(batch_df, 3)

    # Se obtiene la tabla Delta existente
    mock_delta_table.forPath.assert_called_once_with(
        batch_df.sparkSession,
        gold.GOLD_PATH,
    )

    # Alias de la tabla Gold
    mock_gold_table.alias.assert_called_once_with("gold")

    # Alias de los nuevos datos
    mock_order_summary_df.alias.assert_called_once_with("updates")

    # MERGE
    mock_gold_table.alias.return_value.merge.assert_called_once_with(
        mock_order_summary_df.alias.return_value,
        "gold.order_id = updates.order_id",
    )

    # Ejecuta el MERGE
    (
        mock_gold_table.alias.return_value.merge.return_value.whenMatchedUpdate.return_value.whenNotMatchedInsert.return_value.execute.assert_called_once()
    )


@patch("src.streaming.gold.configure_spark_with_delta_pip")
@patch("src.streaming.gold.SparkSession")
def test_gold_main(
    mock_spark_session,
    mock_configure_delta,
):
    # Builder de Spark
    mock_builder = Mock()

    (
        mock_spark_session.builder.appName.return_value.master.return_value.config.return_value.config.return_value.config.return_value
    ) = mock_builder

    # Spark creado mediante Delta
    mock_spark = Mock()

    (mock_configure_delta.return_value.getOrCreate.return_value) = mock_spark

    # DataFrame leído desde Silver
    mock_silver_df = Mock()

    (
        mock_spark.readStream.format.return_value.schema.return_value.load.return_value
    ) = mock_silver_df

    # Query de streaming
    mock_query = Mock()

    (
        mock_silver_df.writeStream.foreachBatch.return_value.option.return_value.start.return_value
    ) = mock_query

    # Ejecutamos main()
    gold.main()

    # ------------------------------------------
    # Verificamos Spark builder
    # ------------------------------------------

    mock_spark_session.builder.appName.assert_called_once_with("gold-layer")

    (
        mock_spark_session.builder.appName.return_value.master.assert_called_once_with(
            "local[*]"
        )
    )

    # Verificamos las configuraciones
    (
        mock_spark_session.builder.appName.return_value.master.return_value.config.assert_called_once_with(
            "spark.sql.shuffle.partitions",
            "4",
        )
    )

    # Delta recibe el builder final
    mock_configure_delta.assert_called_once_with(mock_builder)

    # Se crea Spark
    (mock_configure_delta.return_value.getOrCreate.assert_called_once())

    # ------------------------------------------
    # Verificamos lectura streaming de Silver
    # ------------------------------------------

    mock_spark.readStream.format.assert_called_once_with("parquet")

    (mock_spark.readStream.format.return_value.schema.assert_called_once())

    (
        mock_spark.readStream.format.return_value.schema.return_value.load.assert_called_once_with(
            "data/silver/events"
        )
    )

    # ------------------------------------------
    # Verificamos escritura streaming Gold
    # ------------------------------------------

    mock_silver_df.writeStream.foreachBatch.assert_called_once_with(gold.process_batch)

    (
        mock_silver_df.writeStream.foreachBatch.return_value.option.assert_called_once_with(
            "checkpointLocation",
            "data/checkpoints/gold_order_summary",
        )
    )

    (
        mock_silver_df.writeStream.foreachBatch.return_value.option.return_value.start.assert_called_once()
    )

    # Verificamos que el streaming queda activo
    mock_query.awaitTermination.assert_called_once()
