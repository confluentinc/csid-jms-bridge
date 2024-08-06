output "service_external_endpoint" {
  // modify the value to reference the service_external_endpoint output from the gke_benchmark_worker module to include http:// in front of the IP address
  value = [for ip in module.gke_app_benchmark_worker.service_external_endpoint : "http://${ip}"]
}

output "service_external_ip" {
  value = module.gke_app_benchmark_worker.service_external_ip
}
