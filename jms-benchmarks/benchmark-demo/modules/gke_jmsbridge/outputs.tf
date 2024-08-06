output "service_external_endpoint" {
  // modify the value to reference the service_external_endpoint output from the gke_benchmark_worker module to include http:// in front of the IP address
  value = [for endpoint in module.gke_app_jmsbridge.service_external_endpoint : "tcp://${endpoint}"]
}

output "service_external_jmx_endpoint" {
  value = [for ip in module.gke_app_jmsbridge.service_external_ip : "${ip}:61617"]
}
