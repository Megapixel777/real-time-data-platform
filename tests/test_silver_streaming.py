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

    # Hay eventos válidos
    mock_valid_df = Mock()
    mock_valid_df.isEmpty.return_value = False
    mock_get_valid_events.return_value = mock_valid_df

    # Hay eventos inválidos
    mock_invalid_df = Mock()
    mock_invalid_df.dropDuplicates.return_value = mock_invalid_df
    mock_invalid_df.isEmpty.return_value = False
    mock_get_invalid_events.return_value = mock_invalid_df

    # DataFrame transformado para Silver
    mock_silver_df = Mock()
    mock_transform_silver.return_value = mock_silver_df

    # Ejecutamos
    silver.process_batch(batch_df, 1)

    # Comprobamos validación
    mock_get_valid_events.assert_called_once_with(batch_df)
    mock_get_invalid_events.assert_called_once_with(batch_df)

    # Comprobamos eliminación de duplicados
    mock_invalid_df.dropDuplicates.assert_called_once_with(["event_id"])

    # Comprobamos escritura en Quarantine
    mock_invalid_df.write.mode.assert_called_once_with("append")

    mock_invalid_df.write.mode.return_value.parquet.assert_called_once_with(
        silver.QUARANTINE_PATH
    )

    # Comprobamos transformación Silver
    mock_transform_silver.assert_called_once_with(mock_valid_df)

    # Comprobamos escritura en Silver
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

    # Hay eventos válidos
    mock_valid_df = Mock()
    mock_valid_df.isEmpty.return_value = False
    mock_get_valid_events.return_value = mock_valid_df

    # No hay eventos inválidos
    mock_invalid_df = Mock()
    mock_invalid_df.dropDuplicates.return_value = mock_invalid_df
    mock_invalid_df.isEmpty.return_value = True
    mock_get_invalid_events.return_value = mock_invalid_df

    # Resultado de la transformación Silver
    mock_silver_df = Mock()
    mock_transform_silver.return_value = mock_silver_df

    # Ejecutamos
    silver.process_batch(batch_df, 2)

    # No debe escribir en Quarantine
    mock_invalid_df.write.mode.assert_not_called()

    # Sí debe transformar los eventos válidos
    mock_transform_silver.assert_called_once_with(mock_valid_df)

    # Sí debe escribir en Silver
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

    # No hay eventos válidos
    mock_valid_df = Mock()
    mock_valid_df.isEmpty.return_value = True
    mock_get_valid_events.return_value = mock_valid_df

    # Hay eventos inválidos
    mock_invalid_df = Mock()
    mock_invalid_df.dropDuplicates.return_value = mock_invalid_df
    mock_invalid_df.isEmpty.return_value = False
    mock_get_invalid_events.return_value = mock_invalid_df

    # Ejecutamos
    silver.process_batch(batch_df, 3)

    # Se escribe en Quarantine
    mock_invalid_df.write.mode.assert_called_once_with("append")

    mock_invalid_df.write.mode.return_value.parquet.assert_called_once_with(
        silver.QUARANTINE_PATH
    )

    # No debe transformar eventos para Silver
    mock_transform_silver.assert_not_called()


@patch("src.streaming.silver.SparkSession")
def test_silver_main(mock_spark_session):
    # --------------------------------------------------
    # Mock de SparkSession.builder
    # --------------------------------------------------

    mock_spark = Mock()
    mock_builder = Mock()

    mock_spark_session.builder = mock_builder

    # Todos los métodos del builder devuelven el propio builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder

    # getOrCreate devuelve nuestro Spark mock
    mock_builder.getOrCreate.return_value = mock_spark

    # --------------------------------------------------
    # Mock de lectura desde Bronze
    # --------------------------------------------------

    mock_bronze_df = Mock()

    (
        mock_spark.readStream.format.return_value.schema.return_value.load.return_value
    ) = mock_bronze_df

    # --------------------------------------------------
    # Mock de Streaming Query
    # --------------------------------------------------

    mock_query = Mock()

    (
        mock_bronze_df.writeStream.foreachBatch.return_value.option.return_value.start.return_value
    ) = mock_query

    # --------------------------------------------------
    # Ejecutamos el código real
    # --------------------------------------------------

    silver.main()

    # ==================================================
    # Verificamos SparkSession
    # ==================================================

    mock_builder.appName.assert_called_once_with("silver-layer")

    mock_builder.master.assert_called_once_with("local[*]")

    # Comprobamos las configuraciones importantes
    mock_builder.config.assert_any_call(
        "spark.jars.packages",
        silver.KAFKA_PACKAGE,
    )

    mock_builder.config.assert_any_call(
        "spark.jars",
        silver.GCS_CONNECTOR_JAR,
    )

    mock_builder.config.assert_any_call(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )

    mock_builder.config.assert_any_call(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    )

    mock_builder.config.assert_any_call(
        "spark.hadoop.fs.gs.project.id",
        silver.GCP_PROJECT_ID,
    )

    mock_builder.config.assert_any_call(
        "spark.hadoop.fs.gs.auth.type",
        "SERVICE_ACCOUNT_JSON_KEYFILE",
    )

    mock_builder.config.assert_any_call(
        "spark.hadoop.fs.gs.auth.service.account.json.keyfile",
        silver.GCP_CREDENTIALS,
    )

    mock_builder.config.assert_any_call(
        "spark.hadoop.google.cloud.auth.type",
        "SERVICE_ACCOUNT_JSON_KEYFILE",
    )

    mock_builder.config.assert_any_call(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        silver.GCP_CREDENTIALS,
    )

    mock_builder.config.assert_any_call(
        "spark.hadoop.fs.gs.block.size",
        "67108864",
    )

    mock_builder.config.assert_any_call(
        "spark.pyspark.python",
        "python",
    )

    mock_builder.config.assert_any_call(
        "spark.pyspark.driver.python",
        "python",
    )

    mock_builder.config.assert_any_call(
        "spark.sql.parquet.enableVectorizedReader",
        "false",
    )

    mock_builder.config.assert_any_call(
        "spark.sql.shuffle.partitions",
        "4",
    )

    mock_builder.getOrCreate.assert_called_once()

    # ==================================================
    # Verificamos lectura Bronze
    # ==================================================

    mock_spark.readStream.format.assert_called_once_with("parquet")

    mock_spark.readStream.format.return_value.schema.assert_called_once()

    mock_spark.readStream.format.return_value.schema.return_value.load.assert_called_once_with(
        silver.BRONZE_PATH
    )

    # ==================================================
    # Verificamos escritura streaming
    # ==================================================

    mock_bronze_df.writeStream.foreachBatch.assert_called_once_with(
        silver.process_batch
    )

    (
        mock_bronze_df.writeStream.foreachBatch.return_value.option.assert_called_once_with(
            "checkpointLocation",
            silver.SILVER_CHECKPOINT,
        )
    )

    (
        mock_bronze_df.writeStream.foreachBatch.return_value.option.return_value.start.assert_called_once()
    )

    # ==================================================
    # Verificamos que el streaming queda activo
    # ==================================================

    mock_query.awaitTermination.assert_called_once()

    # ==================================================
    # Verificamos parada de Spark
    # ==================================================

    mock_spark.stop.assert_called_once()
