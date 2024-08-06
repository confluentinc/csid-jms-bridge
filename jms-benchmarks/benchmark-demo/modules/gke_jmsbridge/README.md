## Requirements

| Name                                                                         | Version  |
|------------------------------------------------------------------------------|----------|
| <a name="requirement_kubernetes"></a> [kubernetes](#requirement\_kubernetes) | ~>2.31.0 |

## Providers

No providers.

## Modules

| Name                                                                                        | Source     | Version |
|---------------------------------------------------------------------------------------------|------------|---------|
| <a name="module_gke_app_jmsbridge"></a> [gke\_app\_jmsbridge](#module\_gke\_app\_jmsbridge) | ../gke_app | n/a     |

## Resources

No resources.

## Inputs

| Name                                                                                                                                 | Description                        | Type                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Default      | Required |
|--------------------------------------------------------------------------------------------------------------------------------------|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|:--------:|
| <a name="input_apps"></a> [apps](#input\_apps)                                                                                       | n/a                                | <pre>list(object({<br>    name     = string<br>    image    = string<br>    port = list(number)<br>    replicas = number<br>    access_type = optional(string)<br>    env = optional(map(string))<br>    command = optional(list(string))<br>    args = optional(list(string))<br>    file_config = optional(list(object({<br>      file_name    = string<br>      file_content = string<br>      mount_path   = string<br>    })))<br>    cpu = optional(string)<br>    memory = optional(string)<br>    storage = optional(string)<br>  }))</pre> | `[]`         |    no    |
| <a name="input_confluent_app_kafka_api_key"></a> [confluent\_app\_kafka\_api\_key](#input\_confluent\_app\_kafka\_api\_key)          | Confluent Cloud Kafka API Key      | `string`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | n/a          |   yes    |
| <a name="input_confluent_app_kafka_api_secret"></a> [confluent\_app\_kafka\_api\_secret](#input\_confluent\_app\_kafka\_api\_secret) | Confluent Cloud Kafka API Secret   | `string`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | n/a          |   yes    |
| <a name="input_confluent_bootstrap_endpoint"></a> [confluent\_bootstrap\_endpoint](#input\_confluent\_bootstrap\_endpoint)           | Confluent Cloud bootstrap endpoint | `string`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | n/a          |   yes    |
| <a name="input_gke_ipv6_access_type"></a> [gke\_ipv6\_access\_type](#input\_gke\_ipv6\_access\_type)                                 | n/a                                | `string`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `"INTERNAL"` |    no    |

## Outputs

| Name                                                                                                                              | Description |
|-----------------------------------------------------------------------------------------------------------------------------------|-------------|
| <a name="output_service_external_endpoint"></a> [service\_external\_endpoint](#output\_service\_external\_endpoint)               | n/a         |
| <a name="output_service_external_jmx_endpoint"></a> [service\_external\_jmx\_endpoint](#output\_service\_external\_jmx\_endpoint) | n/a         |
