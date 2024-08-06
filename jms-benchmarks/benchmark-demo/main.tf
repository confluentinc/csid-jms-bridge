terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~>1.80.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~>5.38.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~>2.31.0"
    }
  }
  backend "gcs" {
    bucket = "jms-bridge-benchmarks"
    prefix = "terraform/state"
  }
}

provider "confluent" {
  cloud_api_key    = var.confluent_cloud_api_key
  cloud_api_secret = var.confluent_cloud_api_secret
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

data "google_client_config" "default" {}

data "google_container_cluster" "default" {
  name       = module.gke.google_container_cluster.name
  location   = module.gke.google_container_cluster.location
  depends_on = [module.gke]
}

provider "kubernetes" {
  // set provider based on the GKE provider
  host                   = "https://${data.google_container_cluster.default.endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(data.google_container_cluster.default.master_auth[0].cluster_ca_certificate)
}

module "confluent" {
  source = "./modules/confluent"
}

module "gke" {
  source               = "./modules/gke"
  gke_project_id       = var.gcp_project_id
  gke_region           = var.gcp_region
  gke_ipv6_access_type = var.gke_ipv6_access_type
  # wait for the Confluent Cloud environment to be ready
  depends_on = [module.confluent]
}

module "gke_jmsbridge" {
  source     = "./modules/gke_jmsbridge"
  depends_on = [module.gke]
  providers = {
    kubernetes = kubernetes
  }
  apps                           = var.gke_jmsbridge_apps
  gke_ipv6_access_type           = var.gke_ipv6_access_type
  confluent_app_kafka_api_key    = module.confluent.app_kafka_api_key
  confluent_app_kafka_api_secret = module.confluent.app_kafka_api_secret
  confluent_bootstrap_endpoint   = module.confluent.bootstrap_endpoint
}

module "gke_benchmark_worker" {
  source     = "./modules/gke_benchmark_worker"
  depends_on = [module.gke_jmsbridge]
  providers = {
    kubernetes = kubernetes
  }
  apps                 = var.gke_benchmark_workers
  gke_ipv6_access_type = var.gke_ipv6_access_type
}

module "gke_benchmark_driver" {
  source     = "./modules/gke_benchmark_driver"
  depends_on = [module.gke, module.gke_benchmark_worker]
  providers = {
    kubernetes = kubernetes
  }
  apps                 = var.gke_benchmark_drivers
  gke_ipv6_access_type = var.gke_ipv6_access_type
  workers              = module.gke_benchmark_worker.service_external_endpoint
  broker_address       = module.gke_jmsbridge.service_external_endpoint
}

module "prometheus" {
  source     = "./modules/gke_prometheus"
  depends_on = [module.gke, module.gke_benchmark_worker, module.gke_jmsbridge]
  providers = {
    kubernetes = kubernetes
  }
  apps                 = var.gke_prometheus_app != null ? [var.gke_prometheus_app] : []
  gke_ipv6_access_type = var.gke_ipv6_access_type
  // create list of scrape configs for every service external point
  prometheus_scrape_configs = flatten([
    [
      for index, ip in module.gke_benchmark_worker.service_external_ip : {
        job_name        = "benchmark-worker-${index}"
        scrape_interval = "15s"
        static_configs  = [{ targets = ["${ip}:8081"] }]
      }
    ],
    [
      for index, ip in module.gke_jmsbridge.service_external_jmx_endpoint : {
        job_name        = "jmsbridge-${index}"
        scrape_interval = "15s"
        static_configs  = [{ targets = [ip] }]
      }
    ],
    [
      {
        job_name        = "prometheus"
        scrape_interval = "15s"
        static_configs  = [{ targets = ["localhost:9090"] }]
      }
    ]
  ])
}

module "grafana" {
  source     = "./modules/gke_grafana"
  depends_on = [module.gke, module.prometheus]
  providers = {
    kubernetes = kubernetes
  }
  apps                 = var.gke_grafana_app != null ? [var.gke_grafana_app] : []
  gke_ipv6_access_type = var.gke_ipv6_access_type
  prometheus_url       = module.prometheus.service_external_endpoint[0]
}
