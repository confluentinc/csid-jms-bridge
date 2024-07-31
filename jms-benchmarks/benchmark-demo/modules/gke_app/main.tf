terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~>2.31.0"
    }
  }
}

resource "kubernetes_config_map" "app" {
  for_each = {for app in var.apps : app.name => app if app.file_config != null}

  metadata {
    name = "${each.key}-configmap"
  }

  data = {
    for file in (each.value.file_config != null ? each.value.file_config : []) :
    file.file_name => file.file_content
  }
}

resource "kubernetes_deployment" "app" {
  for_each = {for app in var.apps : app.name => app}

  metadata {
    name = each.value.name
  }

  spec {
    replicas = each.value.replicas
    selector {
      match_labels = {
        app = each.value.name
      }
    }

    template {
      metadata {
        labels = {
          app = each.value.name
        }
      }

      spec {
        container {
          image = each.value.image
          image_pull_policy = "Always"
          name  = each.value.name

          resources {
            limits = {
              cpu               = each.value.cpu != null ? each.value.cpu : "500m"
              memory            = each.value.memory != null ? each.value.memory : "2Gi"
            }
            requests = {
              cpu               = each.value.cpu != null ? each.value.cpu : "500m"
              memory            = each.value.memory != null ? each.value.memory : "2Gi"
              ephemeral-storage = each.value.storage != null ? each.value.storage : "1Gi"
            }
          }

          // dynamic port determined by a list of numbers in each.value.port
          dynamic "port" {
            for_each = each.value.port
            content {
              container_port = port.value
              name = "port-${port.key}"
            }
          }

          dynamic "volume_mount" {
            for_each = each.value.file_config != null ? each.value.file_config : []
            content {
              name       = "${replace(volume_mount.value.file_name, ".", "-")}"
              mount_path = volume_mount.value.mount_path
            }
          }


          dynamic "env" {
            for_each = each.value.env != null ? each.value.env : {}

            content {
              name  = env.key
              value = env.value
            }
          }

          command = each.value.command
          args    = each.value.args
        }
        dynamic "volume" {
          for_each = each.value.file_config != null ? each.value.file_config : []
          content {
            name = "${replace(volume.value.file_name, ".", "-")}"

            config_map {
              name = kubernetes_config_map.app[each.value.name].metadata[0].name
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "default" {
  for_each = {for app in var.apps : app.name => app}

  metadata {
    name        = each.value.name
    annotations = each.value.access_type == "INTERNAL" ? {
      "networking.gke.io/load-balancer-type" = "Internal"
    } : {}
  }

  spec {
    selector = {
      app = kubernetes_deployment.app[each.key].spec[0].selector[0].match_labels.app
    }

    dynamic "port" {
      for_each = each.value.port
      content {
        name = "port-${port.key}"
        port        = port.value
        target_port = port.value
      }
    }

    type = "LoadBalancer"
  }
}
