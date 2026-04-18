variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "location" {
  description = "GCP region/location"
  type        = string
}

variable "credentials" {
  description = "Path to service account JSON key"
  type        = string
  sensitive   = true
}

variable "bucket_name" {
  description = "Bucket to store the data related to this project"
  type        = string
}

variable "services" {
  type = list(string)
}

variable "service_account_email" {
  description = "Existing service account email to bind IAM roles to"
  type        = string
}

variable "region" {
  description = "GCP region for provider"
  type        = string
}
