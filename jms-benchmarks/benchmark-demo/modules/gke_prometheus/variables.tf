variable "apps" {
  type = list(object({
    name     = string
    image    = string
    port = list(number)
    replicas = number
    access_type = optional(string)
    env = optional(map(string))
    command = optional(list(string))
    args = optional(list(string))
    file_config = optional(list(object({
      file_name    = string
      file_content = string
      mount_path   = string
    })))
    cpu = optional(string)
    memory = optional(string)
    storage = optional(string)
  }))

  validation {
    condition = alltrue([
      for app in var.apps :
      (app.access_type == null ? true :
        contains(["INTERNAL", "EXTERNAL"], app.access_type))
    ])
    error_message = "Access type. Must be one of INTERNAL or EXTERNAL"
  }
  default = []
}

variable "gke_ipv6_access_type" {
  type    = string
  default = "INTERNAL"
  validation {
    condition = contains(["INTERNAL", "EXTERNAL"], var.gke_ipv6_access_type)
    error_message = "Invalid IPv6 access type. Must be one of INTERNAL or EXTERNAL"
  }
}

variable "prometheus_scrape_configs" {
  type = list(object({
    job_name        = string
    scrape_interval = string
    static_configs = list(object({
      targets = list(string)
    }))
  }))
  default = [
    {
      job_name        = "prometheus"
      scrape_interval = "15s"
      static_configs = [
        {
          targets = ["localhost:9090"]
        }
      ]
    }
  ]
}