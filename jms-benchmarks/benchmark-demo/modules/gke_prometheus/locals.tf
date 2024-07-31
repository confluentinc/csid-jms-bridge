locals {
  adjusted_apps = [
    for app in var.apps : {
      name        = app.name
      image       = app.image
      port        = app.port
      replicas    = app.replicas != "" ? app.replicas : 1
      env         = app.env != "" ? app.env : {}
      command     = length(coalesce(app.command, []))> 0 ? app.command : []
      args        = length(coalesce(app.args, []))> 0 ? app.args : []
      access_type = app.access_type != "" ? app.access_type : var.gke_ipv6_access_type
      file_config = concat(
        app.file_config != null ? app.file_config : [],
        [{
          file_name  = "prometheus.yml"
          file_content = yamlencode(local.prometheus_config)
          mount_path = "/etc/prometheus"
        }]
      )
      cpu     = app.cpu != "" ? app.cpu : null
      memory  = app.memory != "" ? app.memory : null
      storage = app.storage != "" ? app.storage : null
    }
  ]
  prometheus_config = {
    global = {
      scrape_interval     = "15s"
      evaluation_interval = "15s"
    }
    scrape_configs = var.prometheus_scrape_configs
  }
}