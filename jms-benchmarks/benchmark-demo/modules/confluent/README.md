## Requirements

| Name                                                                      | Version  |
|---------------------------------------------------------------------------|----------|
| <a name="requirement_confluent"></a> [confluent](#requirement\_confluent) | ~>1.80.0 |

## Providers

| Name                                                                | Version  |
|---------------------------------------------------------------------|----------|
| <a name="provider_confluent"></a> [confluent](#provider\_confluent) | ~>1.80.0 |
| <a name="provider_random"></a> [random](#provider\_random)          | n/a      |

## Modules

No modules.

## Resources

| Name                                                                                                                                                    | Type     |
|---------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| [confluent_api_key.app-kafka-api-key](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/api_key)                     | resource |
| [confluent_environment.environment](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/environment)                   | resource |
| [confluent_kafka_acl.app-acls-cluster-create](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/kafka_acl)           | resource |
| [confluent_kafka_acl.app-acls-describe-on-cluster](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/kafka_acl)      | resource |
| [confluent_kafka_acl.app-acls-read-all-consumer-groups](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/kafka_acl) | resource |
| [confluent_kafka_acl.app-consumer-read-on-all-topic](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/kafka_acl)    | resource |
| [confluent_kafka_acl.app-producer-write-on-all-topic](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/kafka_acl)   | resource |
| [confluent_kafka_cluster.cluster](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/kafka_cluster)                   | resource |
| [confluent_kafka_topic.topics](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/kafka_topic)                        | resource |
| [confluent_role_binding.app-role-cluster-admin](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/role_binding)      | resource |
| [confluent_service_account.app](https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/service_account)                   | resource |
| [random_string.suffix](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/string)                                           | resource |

## Inputs

| Name                                                                                                                                     | Description                                                                                                                                                                                                                                     | Type                                                                                                                | Default                     | Required |
|------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|-----------------------------|:--------:|
| <a name="input_confluent_cluster_cloud_provider"></a> [confluent\_cluster\_cloud\_provider](#input\_confluent\_cluster\_cloud\_provider) | Cloud provider for the Confluent Kafka cluster                                                                                                                                                                                                  | `string`                                                                                                            | `"GCP"`                     |    no    |
| <a name="input_confluent_cluster_name"></a> [confluent\_cluster\_name](#input\_confluent\_cluster\_name)                                 | Name of the Confluent Kafka cluster                                                                                                                                                                                                             | `string`                                                                                                            | `"jms-benchmarks"`          |    no    |
| <a name="input_confluent_cluster_region"></a> [confluent\_cluster\_region](#input\_confluent\_cluster\_region)                           | The cloud service provider region where the Kafka cluster is running, for example, us-west-2. See https://docs.confluent.io/cloud/current/clusters/regions.html#cloud-providers-and-regions for a full list of options for AWS, Azure, and GCP. | `string`                                                                                                            | `"northamerica-northeast2"` |    no    |
| <a name="input_confluent_cluster_type"></a> [confluent\_cluster\_type](#input\_confluent\_cluster\_type)                                 | The type of the Confluent Kafka cluster                                                                                                                                                                                                         | `string`                                                                                                            | `"basic"`                   |    no    |
| <a name="input_confluent_environment_name"></a> [confluent\_environment\_name](#input\_confluent\_environment\_name)                     | Name of the Confluent environment                                                                                                                                                                                                               | `string`                                                                                                            | `"jms-benchmarks"`          |    no    |
| <a name="input_confluent_topics"></a> [confluent\_topics](#input\_confluent\_topics)                                                     | List of topics to create in the Confluent Kafka cluster                                                                                                                                                                                         | <pre>list(object({<br>    name       = string<br>    partitions = number<br>    config = map(string)<br>  }))</pre> | `[]`                        |    no    |

## Outputs

| Name                                                                                                   | Description |
|--------------------------------------------------------------------------------------------------------|-------------|
| <a name="output_app_kafka_api_key"></a> [app\_kafka\_api\_key](#output\_app\_kafka\_api\_key)          | n/a         |
| <a name="output_app_kafka_api_secret"></a> [app\_kafka\_api\_secret](#output\_app\_kafka\_api\_secret) | n/a         |
| <a name="output_bootstrap_endpoint"></a> [bootstrap\_endpoint](#output\_bootstrap\_endpoint)           | n/a         |
| <a name="output_resource-ids"></a> [resource-ids](#output\_resource-ids)                               | n/a         |
