output "service_external_endpoint" {
  value = [
    for value in kubernetes_service.default :
    "${value.status[0].load_balancer[0].ingress[0].ip}:${value.spec[0].port[0].port}"
  ]
}

output "service_external_ip" {
  value = [
    for value in kubernetes_service.default :
    value.status[0].load_balancer[0].ingress[0].ip
  ]
}