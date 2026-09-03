output "bucket_name" {
  description = "Name of the GCS bucket"

  value = google_storage_bucket.data_lake.name
}

output "bucket_url" {
  description = "GCS bucket URL"

  value = "gs://${google_storage_bucket.data_lake.name}"
}