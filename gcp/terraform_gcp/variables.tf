######################################################
# GLOBAL
######################################################
variable "gcp_project" {
  type        = string
  description = "GCP Project ID"
  default = "cloud-assignment"
}

variable "gcp_region" {
  type    = string
  default = "asia-south1"
}

variable "gcp_zone" {
  type    = string
  default = "asia-south1-a"
}

######################################################
# NETWORK
######################################################
variable "gcp_vpc_name" {
  type    = string
  default = "gcp-flink-vpc"
}

variable "gcp_subnet_name" {
  type    = string
  default = "gcp-flink-subnet"
}

variable "gcp_subnet_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

######################################################
# DATAPROC + FLINK
######################################################
variable "dataproc_cluster_name" {
  type    = string
  default = "flink-analytics-cluster"
}

variable "dataproc_machine_type" {
  type    = string
  default = "n1-standard-4"
}

variable "dataproc_worker_count" {
  type    = number
  default = 2
}

variable "gcs_bucket_name" {
  type    = string
  default = "gcp-flink-bucket-01"
}
