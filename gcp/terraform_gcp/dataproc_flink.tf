resource "google_storage_bucket" "flink_bucket" {
  name     = "flink-bucket-cloud-assignment-479111"
  location = var.gcp_region

  # required by org policy: uniform bucket-level access
  uniform_bucket_level_access = true

  # optional: labels, lifecycle, versioning, etc.
  force_destroy = true
}

resource "google_dataproc_cluster" "flink_cluster" {
  name   = var.dataproc_cluster_name
  region = var.gcp_region

  cluster_config {
    # use the bucket name for staging; this is valid and common
    staging_bucket = google_storage_bucket.flink_bucket.name

    master_config {
      num_instances = 1
      machine_type  = var.dataproc_machine_type
      disk_config {
        boot_disk_size_gb = 50
      }
    }

    worker_config {
      num_instances = var.dataproc_worker_count
      machine_type  = var.dataproc_machine_type
      disk_config {
        boot_disk_size_gb = 50
      }
    }

    software_config {
      optional_components = ["FLINK"]
      # changed to a supported image version (choose 2.3-debian12 or 3.0-debian12)
      image_version       = "2.3-debian12"
    }

    gce_cluster_config {
      service_account   = google_service_account.dataproc_sa.email
      subnetwork        = google_compute_subnetwork.subnet.name
      internal_ip_only  = false
    }
  }

  depends_on = [
    google_project_iam_member.dataproc_role,
    google_project_iam_member.storage_role,
    google_project_iam_member.logging_role
  ]
}
