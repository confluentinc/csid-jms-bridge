variable "confluent_environment_name" {
  description = "Name of the Confluent environment"
  type        = string
  default     = "jms-benchmarks"
}

variable "confluent_cluster_cloud_provider" {
  description = "Cloud provider for the Confluent Kafka cluster"
  // AWS, AZURE, OR GCP
  type = string
  validation {
    condition = contains(["AWS", "AZURE", "GCP"], var.confluent_cluster_cloud_provider)
    error_message = "Invalid cloud provider. Must be one of AWS, AZURE, or GCP"
  }
  default = "GCP"
}

variable "confluent_cluster_name" {
  description = "Name of the Confluent Kafka cluster"
  type        = string
  default     = "jms-benchmarks"
}

variable "confluent_cluster_region" {
  description = "The cloud service provider region where the Kafka cluster is running, for example, us-west-2. See https://docs.confluent.io/cloud/current/clusters/regions.html#cloud-providers-and-regions for a full list of options for AWS, Azure, and GCP."
  type        = string
  default     = "northamerica-northeast2"
}

variable "confluent_cluster_type" {
  description = "The type of the Confluent Kafka cluster"
  type        = string
  validation {
    condition = contains(["basic", "standard", "enterprise", "freight", "dedicated"], var.confluent_cluster_type)
    error_message = "Invalid cluster type. Must be one of basic, standard, enterprise, freight, or dedicated"
  }
  default = "basic"
}

variable "confluent_topics" {
  type = list(object({
    name       = string
    partitions = number
    config = map(string)
  }))
  description = "List of topics to create in the Confluent Kafka cluster"
  default = []
}