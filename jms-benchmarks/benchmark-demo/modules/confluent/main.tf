terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~>1.80.0"
    }
  }
}

resource "confluent_environment" "environment" {
  display_name = var.confluent_environment_name

  stream_governance {
    package = "ESSENTIALS"
  }
}

resource "confluent_kafka_cluster" "cluster" {
  availability = "SINGLE_ZONE"
  cloud        = var.confluent_cluster_cloud_provider
  display_name = var.confluent_cluster_name
  region       = var.confluent_cluster_region
  environment {
    id = confluent_environment.environment.id
  }

  dynamic "basic" {
    for_each = var.confluent_cluster_type == "basic" ? [1] : []
    content {}
  }

  dynamic "standard" {
    for_each = var.confluent_cluster_type == "standard" ? [1] : []
    content {}
  }

  dynamic "enterprise" {
    for_each = var.confluent_cluster_type == "enterprise" ? [1] : []
    content {}
  }

  dynamic "freight" {
    for_each = var.confluent_cluster_type == "freight" ? [1] : []
    content {}
  }

  dynamic "dedicated" {
    for_each = var.confluent_cluster_type == "dedicated" ? [1] : []
    content {
      cku = local.cluster_type_configs.dedicated.cku
    }
  }

}

resource "confluent_kafka_topic" "topics" {
  count = length(var.confluent_topics)

  kafka_cluster {
    id = confluent_kafka_cluster.cluster.id
  }

  topic_name = trimspace(var.confluent_topics[count.index].name)
  partitions_count = var.confluent_topics[count.index].partitions
  rest_endpoint    = confluent_kafka_cluster.cluster.rest_endpoint

  config = var.confluent_topics[count.index].config

  credentials {
    key    = confluent_api_key.app-kafka-api-key.id
    secret = confluent_api_key.app-kafka-api-key.secret
  }
}

resource "random_string" "suffix" {
  length  = 4
  special = false
  numeric = false
  upper   = false
}

resource "confluent_service_account" "app" {
  display_name = "app-${random_string.suffix.result}"
  description  = "Service account for the app to manage the kafka cluster"
}


resource "confluent_role_binding" "app-role-cluster-admin" {
  crn_pattern = confluent_kafka_cluster.cluster.rbac_crn
  principal   = "User:${confluent_service_account.app.id}"
  role_name   = "CloudClusterAdmin"
}

resource "confluent_api_key" "app-kafka-api-key" {
  display_name = "app-kafka-api-key"
  description  = "Kafka API Key that is owned by 'app' service account"
  owner {
    id          = confluent_service_account.app.id
    api_version = confluent_service_account.app.api_version
    kind        = confluent_service_account.app.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.cluster.id
    api_version = confluent_kafka_cluster.cluster.api_version
    kind        = confluent_kafka_cluster.cluster.kind

    environment {
      id = confluent_environment.environment.id
    }
  }

  depends_on = [
    confluent_role_binding.app-role-cluster-admin
  ]
}

resource "confluent_kafka_acl" "app-acls-cluster-create" {
  kafka_cluster {
    id = confluent_kafka_cluster.cluster.id
  }
  host          = "*"
  operation     = "CREATE"
  pattern_type  = "LITERAL"
  permission    = "ALLOW"
  principal     = "User:${confluent_service_account.app.id}"
  resource_name = "kafka-cluster"
  resource_type = "CLUSTER"
  rest_endpoint = confluent_kafka_cluster.cluster.rest_endpoint

  credentials {
    key    = confluent_api_key.app-kafka-api-key.id
    secret = confluent_api_key.app-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-acls-describe-on-cluster" {
  kafka_cluster {
    id = confluent_kafka_cluster.cluster.id
  }
  host          = "*"
  operation     = "DESCRIBE"
  pattern_type  = "LITERAL"
  permission    = "ALLOW"
  principal     = "User:${confluent_service_account.app.id}"
  resource_name = "kafka-cluster"
  resource_type = "CLUSTER"
  rest_endpoint = confluent_kafka_cluster.cluster.rest_endpoint

  credentials {
    key    = confluent_api_key.app-kafka-api-key.id
    secret = confluent_api_key.app-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-acls-read-all-consumer-groups" {
  kafka_cluster {
    id = confluent_kafka_cluster.cluster.id
  }

  resource_type = "GROUP"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.cluster.rest_endpoint

  credentials {
    key    = confluent_api_key.app-kafka-api-key.id
    secret = confluent_api_key.app-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-consumer-read-on-all-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.cluster.id
  }
  resource_type = "TOPIC"
  resource_name = "*"
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.app.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.app-kafka-api-key.id
    secret = confluent_api_key.app-kafka-api-key.secret
  }
}

resource "confluent_kafka_acl" "app-producer-write-on-all-topic" {
  kafka_cluster {
    id = confluent_kafka_cluster.cluster.id
  }
  host          = "*"
  operation     = "WRITE"
  pattern_type  = "LITERAL"
  permission    = "ALLOW"
  principal     = "User:${confluent_service_account.app.id}"
  resource_name = "*"
  resource_type = "TOPIC"
  rest_endpoint = confluent_kafka_cluster.cluster.rest_endpoint
  credentials {
    key    = confluent_api_key.app-kafka-api-key.id
    secret = confluent_api_key.app-kafka-api-key.secret
  }
}