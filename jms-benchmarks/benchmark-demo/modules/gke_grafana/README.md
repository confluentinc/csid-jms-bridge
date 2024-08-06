## Requirements

| Name                                                                         | Version  |
|------------------------------------------------------------------------------|----------|
| <a name="requirement_kubernetes"></a> [kubernetes](#requirement\_kubernetes) | ~>2.31.0 |

## Providers

No providers.

## Modules

| Name                                                                    | Source     | Version |
|-------------------------------------------------------------------------|------------|---------|
| <a name="module_gke_grafana"></a> [gke\_grafana](#module\_gke\_grafana) | ../gke_app | n/a     |

## Resources

No resources.

## Inputs

| Name                                                                                                 | Description | Type                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | Default      | Required |
|------------------------------------------------------------------------------------------------------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|:--------:|
| <a name="input_apps"></a> [apps](#input\_apps)                                                       | n/a         | <pre>list(object({<br>    name     = string<br>    image    = string<br>    port = list(number)<br>    replicas = number<br>    access_type = optional(string)<br>    env = optional(map(string))<br>    command = optional(list(string))<br>    args = optional(list(string))<br>    file_config = optional(list(object({<br>      file_name    = string<br>      file_content = string<br>      mount_path   = string<br>    })))<br>    cpu = optional(string)<br>    memory = optional(string)<br>    storage = optional(string)<br>  }))</pre> | `[]`         |    no    |
| <a name="input_gke_ipv6_access_type"></a> [gke\_ipv6\_access\_type](#input\_gke\_ipv6\_access\_type) | n/a         | `string`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `"INTERNAL"` |    no    |
| <a name="input_prometheus_url"></a> [prometheus\_url](#input\_prometheus\_url)                       | n/a         | `string`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | n/a          |   yes    |

## Outputs

| Name                                                                                                                | Description |
|---------------------------------------------------------------------------------------------------------------------|-------------|
| <a name="output_service_external_endpoint"></a> [service\_external\_endpoint](#output\_service\_external\_endpoint) | n/a         |
