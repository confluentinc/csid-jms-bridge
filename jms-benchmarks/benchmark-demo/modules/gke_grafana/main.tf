terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~>2.31.0"
    }
  }
}

module "gke_grafana" {
  source               = "../gke_app"
  apps                 = local.adjusted_apps
  gke_ipv6_access_type = var.gke_ipv6_access_type
  providers = {
    kubernetes = kubernetes
  }
}