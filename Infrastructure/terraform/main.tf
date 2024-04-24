terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project_id
  region      = var.region
}

# Virtual Machine
resource "google_compute_instance" "vm_instance" {
  name         = var.instance_name
  machine_type = "c2-standard-8"
  zone         = "${var.region}-a"
  tags         = ["http-server", "https-server"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
      size  = 100
      type  = "pd-balanced"
    }
    auto_delete = true
    device_name = "test-instance-dez"
    mode        = "READ_WRITE"
  }

  network_interface {
    network = "default"
    access_config {
      // Use the default network-tier and stack-type
    }
  }

  labels = {
    "goog-ec-src" = "vm_add-gcloud"
  }
}

# Cloud Storage Bucket
resource "google_storage_bucket" "bucket_instance" {
  name                        = var.bucket_name
  location                    = "us"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  retention_policy {
    retention_period = 8
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "my_dataset" {
  dataset_id = var.bq_dataset_name
  location   = "US"
}
