variable "gke_ipv6_access_type" {
  type    = string
  default = "INTERNAL"
  validation {
    condition = contains(["INTERNAL", "EXTERNAL"], var.gke_ipv6_access_type)
    error_message = "Invalid IPv6 access type. Must be one of INTERNAL or EXTERNAL"
  }
}

variable "gke_region" {
  type        = string
  description = "The region where the GKE cluster is running"
}

variable "gke_project_id" {
  type        = string
  description = "The GCP project ID where the GKE cluster is running"
}