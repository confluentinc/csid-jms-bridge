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

variable "prometheus_url" {
  type = string
  validation {
    condition = can(regex("http(s)?://[a-zA-Z0-9.-]+(:[0-9]+)?", var.prometheus_url))
    error_message = "Invalid Prometheus URL"
  }
}