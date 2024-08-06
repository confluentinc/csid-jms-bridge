output "service_external_endpoint" {
  // modify the value to reference the service_external_endpoint output from the gke_benchmark_worker module to include http:// in front of the IP address
  value = [for endpoint in module.gke_grafana.service_external_endpoint : "http://${endpoint}"]
}
