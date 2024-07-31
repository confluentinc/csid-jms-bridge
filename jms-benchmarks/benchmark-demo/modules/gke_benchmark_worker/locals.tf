locals {
  adjusted_apps = [
    for app in var.apps : {
      name        = app.name
      image       = app.image
      port        = app.port
      replicas    = app.replicas != "" ? app.replicas : 1
      env         = app.env != "" ? app.env : {}
      command     = length(coalesce(app.command, [])) > 0 ? app.command : ["/bin/bash"]
      args        = length(coalesce(app.args, [])) > 0 ? app.args : ["-c", "/benchmark/bin/benchmark-worker"]
      access_type = app.access_type != "" ? app.access_type : var.gke_ipv6_access_type
      file_config = app.file_config != null ? app.file_config : []
      cpu         = app.cpu != "" ? app.cpu : null
      memory      = app.memory != "" ? app.memory : null
      storage     = app.storage != "" ? app.storage : null
    }
  ]
}