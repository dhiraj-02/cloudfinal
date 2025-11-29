output "dataproc_cluster_name" {
  value = google_dataproc_cluster.flink_cluster.name
}

output "dataproc_master_endpoint" {
  value = google_dataproc_cluster.flink_cluster.cluster_config[0].gce_cluster_config[0].internal_ip_only
}

output "gcs_bucket" {
  value = google_storage_bucket.flink_bucket.name
}

output "service_account_email" {
  value = google_service_account.dataproc_sa.email
}
