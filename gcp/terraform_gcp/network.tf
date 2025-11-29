resource "google_compute_network" "vpc" {
  name                    = var.gcp_vpc_name
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = var.gcp_subnet_name
  region        = var.gcp_region
  network       = google_compute_network.vpc.id
  ip_cidr_range = var.gcp_subnet_cidr
}
