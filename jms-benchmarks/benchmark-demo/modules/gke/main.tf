terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~>5.38.0"
    }
  }
}
resource "google_compute_network" "default" {
  name = "default-network"

  auto_create_subnetworks  = false
  enable_ula_internal_ipv6 = true
}

resource "google_compute_subnetwork" "default" {
  name = "default-subnetwork"

  ip_cidr_range = "10.0.0.0/16"
  region        = var.gke_region

  stack_type       = "IPV4_IPV6"
  ipv6_access_type = var.gke_ipv6_access_type

  network = google_compute_network.default.id
  secondary_ip_range {
    range_name    = "services-range"
    ip_cidr_range = "192.168.0.0/20"
  }

  secondary_ip_range {
    range_name    = "pod-ranges"
    ip_cidr_range = "192.168.16.0/20"
  }
}

// random_id is used to generate a unique name for the cluster
resource "random_id" "cluster_suffix" {
  byte_length = 4
}

resource "google_container_cluster" "default" {
  name = "jms-bridge-benchmarks-${random_id.cluster_suffix.hex}"

  location                 = var.gke_region
  enable_autopilot         = true
  enable_l4_ilb_subsetting = true

  network    = google_compute_network.default.id
  subnetwork = google_compute_subnetwork.default.id

  ip_allocation_policy {
    stack_type                    = "IPV4_IPV6"
    services_secondary_range_name = google_compute_subnetwork.default.secondary_ip_range[0].range_name
    cluster_secondary_range_name  = google_compute_subnetwork.default.secondary_ip_range[1].range_name
  }

  deletion_protection = false
}