output "resource-ids" {
  value = <<EOT
  Bootstrap: ${confluent_kafka_cluster.cluster.bootstrap_endpoint}
  Environment ID:   ${confluent_environment.environment.id}
  Kafka Cluster ID: ${confluent_kafka_cluster.cluster.id}
%{ for topic in confluent_kafka_topic.topics ~}
  Topic: ${topic.topic_name}
%{ endfor ~}


  Service Accounts and their Kafka API Keys (API Keys inherit the permissions granted to the owner):
  ${confluent_service_account.app.display_name}:                     ${confluent_service_account.app.id}
  ${confluent_service_account.app.display_name}'s Kafka API Key:     "${confluent_api_key.app-kafka-api-key.id}"
  ${confluent_service_account.app.display_name}'s Kafka API Secret:  "${confluent_api_key.app-kafka-api-key.secret}"
  EOT

  sensitive = true
}

output "bootstrap_endpoint" {
  value = confluent_kafka_cluster.cluster.bootstrap_endpoint
}

output "app_kafka_api_key" {
  value = confluent_api_key.app-kafka-api-key.id
}

output "app_kafka_api_secret" {
  value     = confluent_api_key.app-kafka-api-key.secret
  sensitive = true
}