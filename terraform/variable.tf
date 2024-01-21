
variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
  sensitive = True
}


variable "location" {
  description = "Project Location"
  default     = "asia-southeast1"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "terraform-demo-411906-terra-bucket"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "project" {
  description = "Project Name"
  default     = "terraform-demo-411906"
}

variable "region" {
  description = "Region"
  default     = "asia-southeast1-a"
}