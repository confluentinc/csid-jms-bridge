terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~>2.31.0"
    }
  }
}

resource "kubernetes_storage_class" "app" {
  // only add a storage class if storage variable is set. should be 0 or 1
  count = alltrue([for app in var.apps : app.storage != null]) ? 1 : 0
  metadata {
    name = "ssd"
  }
  storage_provisioner = "kubernetes.io/gce-pd"
  parameters = {
    type = "pd-ssd"
  }
}

resource "kubernetes_persistent_volume_claim" "app" {
  for_each = {for app in var.apps : app.name => app if app.storage != null}

  metadata {
    name = "${each.key}-pvc"
  }

  spec {
    access_modes = ["ReadWriteOnce"]
    resources {
      requests = {
        storage = each.value.storage
      }
    }
    storage_class_name = kubernetes_storage_class.app[0].metadata[0].name
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

  lifecycle {
    ignore_changes = [
      data,
    ]
  }
}

locals {
  config_map_checksums = {
    for name, app in var.apps : name => base64sha256(join("", [
      for file in (app.file_config != null ? app.file_config : []) :
      file.file_content
    ])) if app.file_config != null
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
        annotations = {
          configmap-checksum = lookup(local.config_map_checksums, each.key, "")
        }
      }

      spec {
        container {
          image             = each.value.image
          image_pull_policy = "Always"
          name              = each.value.name

          resources {
            limits = {
              cpu    = each.value.cpu != null ? each.value.cpu : "500m"
              memory = each.value.memory != null ? each.value.memory : "2Gi"
            }
            requests = {
              cpu    = each.value.cpu != null ? each.value.cpu : "500m"
              memory = each.value.memory != null ? each.value.memory : "2Gi"
            }
          }

          // dynamic port determined by a list of numbers in each.value.port
          dynamic "port" {
            for_each = each.value.port
            content {
              container_port = port.value
              name           = "port-${port.key}"
            }
          }

          dynamic "volume_mount" {
            for_each = each.value.file_config != null ? each.value.file_config : []
            content {
              name       = "${replace(volume_mount.value.file_name, ".", "-")}"
              mount_path = volume_mount.value.mount_path
            }
          }

          // Add volume mount for PVC
          dynamic "volume_mount" {
            for_each = each.value.storage != null ? [each.key] : []
            content {
              name = "${each.key}-pvc"
              // TODO: This should be configurable
              mount_path = "/var/cache/jms-bridge"
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

        // Add config map volumes
        dynamic "volume" {
          for_each = each.value.file_config != null ? each.value.file_config : []
          content {
            name = "${replace(volume.value.file_name, ".", "-")}"

            config_map {
              name = kubernetes_config_map.app[each.value.name].metadata[0].name
            }
          }
        }

        // Add PVC volume
        dynamic "volume" {
          for_each = each.value.storage != null ? [each.key] : []
          content {
            name = "${each.key}-pvc"

            persistent_volume_claim {
              claim_name = kubernetes_persistent_volume_claim.app[each.key].metadata[0].name
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
        name        = "port-${port.key}"
        port        = port.value
        target_port = port.value
      }
    }

    type = "LoadBalancer"
  }
}
