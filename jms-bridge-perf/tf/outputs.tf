output "tester" {
  value = google_compute_instance.tester[*].network_interface.0.access_config.0.nat_ip
}

output "controller" {
  value = google_compute_instance.controller.network_interface.0.access_config.0.nat_ip
}

output "bridge" {
  value = google_compute_instance.bridge.network_interface.0.access_config.0.nat_ip
}

