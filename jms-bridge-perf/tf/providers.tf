// Configure the Google Cloud provider
provider "google" {
 project     = var.project
 region      = var.zone
}

resource "google_service_account" "prometheus-sd-user" {
  account_id   = "prometheus-sd-user"
  display_name = "Prometheus Service Discovery"
}

resource "google_project_iam_member" "prometheus-sd-user-compute-viewer" {
  role    = "roles/compute.viewer"
  member  = "serviceAccount:${google_service_account.prometheus-sd-user.email}"
}
