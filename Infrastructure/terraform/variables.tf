variable "credentials" {
  description = "JSON credentials file"
  default     = "D://prjs//data_engineering_zoomcamp_datatalks//dez_2024_project//Infrastructure//keys//gcp_key.json"
  sensitive   = true
}

variable "project_id" {
  description = "Project name as in GCP"
  type        = string
  default     = "dez-2024-project"
}

variable "region" {
  description = "region described in documentation"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Bucket name"
  type        = string
  default     = "this-is-a-test-bucket"
}

variable "instance_name" {
  description = "Instance name"
  type        = string
  default     = "this-is-an-instance-test"
}

variable "dataset_name" {
  description = "Dataset name"
  type        = string
  default     = "my_dataset_test"
}

variable "bq_dataset_name" {
  description = "Dataset name"
  type        = string
  default     = "my_dataset_test_v1_0"
}
