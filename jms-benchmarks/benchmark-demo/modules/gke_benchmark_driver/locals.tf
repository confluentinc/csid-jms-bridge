locals {
  workers = join(",", var.workers)
  broker_address = join(",", var.broker_address)
  adjusted_apps = [
    for app in var.apps : {
      name        = app.name
      image       = app.image
      port        = app.port
      replicas    = app.replicas != "" ? app.replicas : 1
      env         = app.env != "" ? app.env : {}
      command     = length(coalesce(app.command, [])) > 0 ? app.command : ["/bin/bash"]
      args        = length(coalesce(app.args, [])) > 0 ? app.args : ["-c", "/benchmark/bin/benchmark --drivers /benchmark/driver-artemis/jms-bridge.yaml --workers ${local.workers} ${app.workload != null ? app.workload : "/benchmark/workloads/1-topic-1-partition-100b.yaml"}"]
      access_type = app.access_type != "" ? app.access_type : var.gke_ipv6_access_type
      file_config = concat(
          app.file_config != null ? app.file_config : [],
        [
          {
            file_name = "jms-bridge.yaml"
            file_content = templatefile("${path.module}/jms-bridge.yaml.tftpl", {
              broker_address = local.broker_address
            })
            mount_path = "/benchmark/driver-artemis"
          }
        ]
      )
      cpu     = app.cpu != "" ? app.cpu : null
      memory  = app.memory != "" ? app.memory : null
      storage = app.storage != "" ? app.storage : null
    }
  ]
}