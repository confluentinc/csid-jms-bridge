locals {
  adjusted_apps = [
    for app in var.apps : {
      name     = app.name
      image    = app.image
      port     = app.port
      replicas = app.replicas != "" ? app.replicas : 1
      env = merge(
          app.env != "" ? app.env : {},
        {
          "JMSBRIDGE_KAFKA_BOOTSTRAP_SERVERS" = var.confluent_bootstrap_endpoint
          "JMSBRIDGE_KAFKA_SECURITY_PROTOCOL" = "SASL_SSL"
          "JMSBRIDGE_KAFKA_SASL_MECHANISM"    = "PLAIN"
          "JMSBRIDGE_KAFKA_SASL_JAAS_CONFIG"  = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${var.confluent_app_kafka_api_key}\" password=\"${var.confluent_app_kafka_api_secret}\";"
          "JMS_BRIDGE_JMX_OPTS"               = "-javaagent:/usr/share/java/jmx_prometheus_agent/jmx_prometheus_javaagent-0.15.0.jar=61617:/etc/jms-bridge/jmx-prometheus-config.yml"
        }
      )
      command     = length(coalesce(app.command, []))> 0 ? app.command : []
      args        = length(coalesce(app.args, []))> 0 ? app.args : []
      access_type = app.access_type != "" ? app.access_type : var.gke_ipv6_access_type
      file_config = app.file_config != null ? app.file_config : []
      cpu         = app.cpu != "" ? app.cpu : null
      memory      = app.memory != "" ? app.memory : null
      storage     = app.storage != "" ? app.storage : null
    }
  ]
}