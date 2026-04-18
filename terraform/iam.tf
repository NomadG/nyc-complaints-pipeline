locals {
  sa_roles = [
    "roles/storage.admin",
    "roles/bigquery.admin",
    "roles/dataproc.editor",
  ]
}

resource "google_project_iam_member" "sa_roles" {
  for_each = toset(local.sa_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${var.service_account_email}"
}
