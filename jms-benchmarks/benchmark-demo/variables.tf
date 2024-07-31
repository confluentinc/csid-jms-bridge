variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key (also referred as Cloud API ID)"
  type        = string
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret"
  type        = string
  sensitive   = true
}

variable "gcp_region" {
  description = "The GCP region"
  type        = string
}

variable "gcp_project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "gke_ipv6_access_type" {
  type    = string
  default = "INTERNAL"
  validation {
    condition     = contains(["INTERNAL", "EXTERNAL"], var.gke_ipv6_access_type)
    error_message = "Invalid IPv6 access type. Must be one of INTERNAL or EXTERNAL"
  }
}

variable "gke_jmsbridge_apps" {
  description = "List of jms bridge applicationsapplications to deploy on GKE"
  type = list(object({
    name        = string
    image       = string
    port        = list(number)
    replicas    = optional(number)
    access_type = optional(string)
    env         = optional(map(string))
    command     = optional(list(string))
    args        = optional(list(string))
    file_config = optional(list(object({
      file_name    = string
      file_content = string
      mount_path   = string
    })))
    cpu     = optional(string)
    memory  = optional(string)
    storage = optional(string)
  }))
}

variable "gke_benchmark_workers" {
  description = "List of benchmark workers to deploy on GKE"
  type = list(object({
    name        = string
    image       = string
    port        = list(number)
    replicas    = optional(number)
    access_type = optional(string)
    env         = optional(map(string))
    command     = optional(list(string))
    args        = optional(list(string))
    file_config = optional(list(object({
      file_name    = string
      file_content = string
      mount_path   = string
    })))
    cpu     = optional(string)
    memory  = optional(string)
    storage = optional(string)
  }))

}

variable "gke_benchmark_drivers" {
  description = "List of driver applications to deploy on GKE"
  type = list(object({
    name        = string
    image       = string
    port        = list(number)
    replicas    = optional(number)
    access_type = optional(string)
    env         = optional(map(string))
    command     = optional(list(string))
    args        = optional(list(string))
    file_config = optional(list(object({
      file_name    = string
      file_content = string
      mount_path   = string
    })))
    cpu      = optional(string)
    memory   = optional(string)
    storage  = optional(string)
    workload = optional(string)
  }))
}

variable "gke_prometheus_app" {
  description = "Prometheus application to deploy on GKE"
  type = object({
    name        = string
    image       = string
    port        = list(number)
    replicas    = optional(number)
    access_type = optional(string)
    env         = optional(map(string))
    command     = optional(list(string))
    args        = optional(list(string))
    file_config = optional(list(object({
      file_name    = string
      file_content = string
      mount_path   = string
    })))
    cpu     = optional(string)
    memory  = optional(string)
    storage = optional(string)
  })
  default = null
}

variable "gke_grafana_app" {
  description = "Grafana application to deploy on GKE"
  type = object({
    name        = string
    image       = string
    port        = list(number)
    replicas    = optional(number)
    access_type = optional(string)
    env         = optional(map(string))
    command     = optional(list(string))
    args        = optional(list(string))
    file_config = optional(list(object({
      file_name    = string
      file_content = string
      mount_path   = string
    })))
    cpu     = optional(string)
    memory  = optional(string)
    storage = optional(string)
  })
  default = null
}