variable "project" {
  default = "f1-analytics-zc"
}

variable "location" {
  description = "Location to be used when creating resources"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset Name"
  default     = "f1_analytics_zc_dataset"
}

variable "gcp_bucket_name" {
  description = "My Storage bucket name"
  default     = "f1_analytics_zc_bucket"
}

variable "gcp_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

variable "credentials" {
  description = "My credentials file"
  default = "./service_account.json"
}