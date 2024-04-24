provider "google" {
  credentials = file("path/to/your/service-account-key.json")
  project     = "your-existing-project-id"
  region      = "us-central1"
}

# Virtual Machine
resource "google_compute_instance" "vm_instance" {
  name         = "my-vm-instance"
  machine_type = "c2-standard-8"
  zone         = "us-central1-a"

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
      size  = 100
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }
}

# Cloud Storage Bucket
resource "google_storage_bucket" "my_bucket" {
  name     = "my-bucket-name"
  location = "us"
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
  retention_policy {
    retention_period = 8
  }
}

# BigQuery Dataset
resource "google_bigquery_dataset" "my_dataset" {
  dataset_id = "my_dataset"
  location   = "US"
}
