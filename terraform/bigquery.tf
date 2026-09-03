resource "google_bigquery_dataset" "ecommerce" {
  dataset_id = "ecommerce"
  location   = "EU"
}

resource "google_bigquery_table" "order_summary" {
  dataset_id = google_bigquery_dataset.ecommerce.dataset_id
  table_id   = "order_summary"

  schema = jsonencode([
    {
      name = "order_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "customer_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "total_items"
      type = "INT64"
      mode = "NULLABLE"
    },
    {
      name = "total_amount"
      type = "NUMERIC"
      mode = "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "order_summary_staging" {
  dataset_id = google_bigquery_dataset.ecommerce.dataset_id
  table_id   = "order_summary_staging"

  schema = jsonencode([
    {
      name = "order_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "customer_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "total_items"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "total_amount"
      type = "NUMERIC"
      mode = "NULLABLE"
    },
    {
      name = "batch_id"
      type = "INTEGER"
      mode = "REQUIRED"
    }
  ])
}

resource "google_bigquery_table" "order_summary_processed_batches" {
  dataset_id = google_bigquery_dataset.ecommerce.dataset_id
  table_id   = "order_summary_processed_batches"

  schema = jsonencode([
    {
      name = "batch_id"
      type = "INT64"
      mode = "NULLABLE"
    }
  ])
}
