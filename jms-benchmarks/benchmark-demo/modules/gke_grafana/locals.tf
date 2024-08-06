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
        [
          {
            file_name  = "datasource.yml"
            file_content = yamlencode(local.datasource_config)
            mount_path = "/etc/grafana/provisioning/datasources"
          },
          {
            file_name  = "dashboard.yml"
            file_content = yamlencode(local.dashboard_config)
            mount_path = "/etc/grafana/provisioning/dashboards"
          },
          {
            file_name  = "bridge-dashboard.json"
            file_content = file("${path.module}/dashboard.json")
            mount_path = "/etc/grafana/provisioning/my-dashboards"
          }

        ]
      )
      cpu     = app.cpu != "" ? app.cpu : null
      memory  = app.memory != "" ? app.memory : null
      storage = app.storage != "" ? app.storage : null
    }
  ]
  datasource_config = {
    apiVersion = 1
    datasources = [
      {
        name      = "Prometheus"
        type      = "prometheus"
        access    = "proxy"
        url       = var.prometheus_url
        isDefault = true
        editable  = true
      }
    ]
  }
  dashboard_config = {
    apiVersion = 1
    providers = [
      {
        name            = "default"
        orgId           = 1
        folder          = ""
        type            = "file"
        disableDeletion = false
        allowUiUpdates  = true
        editable        = true
        options = {
          path = "/etc/grafana/provisioning/my-dashboards"
        }
      }
    ]
  }
}