provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials)
}

resource "google_storage_bucket" "project-bucket" {
  name          = var.bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}