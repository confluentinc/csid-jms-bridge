#confluent_cloud_api_key    = "api-key-here"
#confluent_cloud_api_secret = "api-secret-here"
gcp_project_id             = "csid-281116"
gcp_region                 = "northamerica-northeast2"
gke_ipv6_access_type       = "EXTERNAL"

gke_jmsbridge_apps = [
  {
    name  = "jms-bridge-docker"
    image = "us-docker.pkg.dev/csid-281116/csid-docker-repo-us/jms-bridge-docker:3.3.4-SNAPSHOT"
    port = [61616, 61617]
    env = {
      "JMS_BRIDGE_HEAP_OPTS"             = "-Xmx16g"
      "LOG_IO_CONFLUENT_AMQ"             = "INFO"
      "LOG_ORG_APACHE_ACTIVEMQ"          = "INFO"
      "JMSBRIDGE_ID"                     = "benchmark"
      "JMSBRIDGE_KAFKA_COMPRESSION_TYPE" = "zstd"
      "JMSBRIDGE_KAFKA_BATCH_SIZE"       = "1048576"
      "JMSBRIDGE_KAFKA_ACKS"             = "all"
      "JMSBRIDGE_KAFKA_MAX_REQUEST_SIZE" = "2097152"
      "JMSBRIDGE_KAFKA_BUFFER_MEMORY"    = "67108860"
      "JMSBRIDGE_KAFKA_LINGER_MS"        = "100"
      "JMSBRIDGE_KAFKA_MAX_POLL_RECORDS" = "5000"
      "JMSBRIDGE_JOURNALS_READY_TIMEOUT" = "5m"
      "JMSBRIDGE_JOURNALS_TOPIC_REPLICATION" : 3
      "JMSBRIDGE_JOURNALS_TOPIC_PARTITIONS" : 6
      "JMSBRIDGE_STREAMS_REPLICATION_FACTOR" : 3
      "JMSBRIDGE_STREAMS_ACKS" : "all"
      "JMSBRIDGE_STREAMS_CACHE_MAX_BYTES_BUFFERING" : "0"
      "JMSBRIDGE_STREAMS_NUM_STANDBY_REPLICAS" : "0"
      "JMSBRIDGE_STREAMS_NUM_STREAM_THREADS" : "7"
      "JMSBRIDGE_STREAMS_COMMIT_INTERVAL_MS" : "5000"
      "JMSBRIDGE_STREAMS_PRODUCER_COMPRESSION_TYPE" : "zstd"
      "JMSBRIDGE_STREAMS_PRODUCER_BATCH_SIZE" : "1048576"
      "JMSBRIDGE_STREAMS_PRODUCER_LINGER_MS" : "100"
      "JMSBRIDGE_STREAMS_PRODUCER_MAX_REQUEST_SIZE" : "2097152"
      "JMSBRIDGE_ROUTING_METADATA_REFRESH_MS" : "60000"
      "JMSBRIDGE_ROUTING_TOPICS_0_MATCH" : "^test-.*"
      "JMSBRIDGE_ROUTING_TOPICS_0_MESSAGE_TYPE" : "BYTES"
    }
    # 1 cpu core per 4gi of memory
    cpu     = "8000m"
    memory  = "32Gi"
    storage = "10Gi"
  },
]

gke_benchmark_workers = [
  {
    name   = "openmessaging-benchmark-worker1"
    image  = "us-docker.pkg.dev/csid-281116/csid-docker-repo-us/benchmarks:latest"
    port = [8080, 8081]
    cpu    = "1500m"
    memory = "6Gi"
  },
  {
    name   = "openmessaging-benchmark-worker2"
    image  = "us-docker.pkg.dev/csid-281116/csid-docker-repo-us/benchmarks:latest"
    port = [8080, 8081]
    cpu    = "1500m"
    memory = "6Gi"
  },
  {
    name   = "openmessaging-benchmark-worker3"
    image  = "us-docker.pkg.dev/csid-281116/csid-docker-repo-us/benchmarks:latest"
    port = [8080, 8081]
    cpu    = "1500m"
    memory = "6Gi"
  },
  {
    name   = "openmessaging-benchmark-worker4"
    image  = "us-docker.pkg.dev/csid-281116/csid-docker-repo-us/benchmarks:latest"
    port = [8080, 8081]
    cpu    = "1500m"
    memory = "6Gi"
  },
]

gke_benchmark_drivers = [
  {
    name   = "openmessaging-benchmark-driver"
    image  = "us-docker.pkg.dev/csid-281116/csid-docker-repo-us/benchmarks:latest"
    port = [8080]
    cpu    = "625m"
    memory = "2.5Gi"
    file_config = [
      {
        file_name    = "my-workload.yaml"
        file_content = <<-EOT
name: max-rate-10-topics-1-partition-1kb

topics: 10
partitionsPerTopic: 1
messageSize: 1024

useRandomizedPayloads: true
randomBytesRatio: 0.5
randomizedPayloadPoolSize: 10000

subscriptionsPerTopic: 65
consumerPerSubscription: 1
producersPerTopic: 1

# Discover max-sustainable rate
producerRate: 10000000

consumerBacklogSizeGB: 0
testDurationMinutes: 30
EOT
        mount_path   = "/benchmark/workloads"
      }
    ]
    workload = "/benchmark/workloads/my-workload.yaml"
  }
]

gke_prometheus_app = {
  name  = "prometheus"
  image = "prom/prometheus:v2.46.0"
  port = [9090]
  args = ["--config.file=/etc/prometheus/prometheus.yml"]
}

gke_grafana_app = {
  name  = "grafana"
  image = "grafana/grafana:10.0.3"
  port = [3000]
  env = {
    "GF_SECURITY_ADMIN_USER"     = "admin"
    "GF_SECURITY_ADMIN_PASSWORD" = "admin"
  }
}