## Requirements

| Name                                                             | Version  |
|------------------------------------------------------------------|----------|
| <a name="requirement_google"></a> [google](#requirement\_google) | ~>5.38.0 |

## Providers

| Name                                                       | Version  |
|------------------------------------------------------------|----------|
| <a name="provider_google"></a> [google](#provider\_google) | ~>5.38.0 |
| <a name="provider_random"></a> [random](#provider\_random) | n/a      |

## Modules

No modules.

## Resources

| Name                                                                                                                                   | Type     |
|----------------------------------------------------------------------------------------------------------------------------------------|----------|
| [google_compute_network.default](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/compute_network)       | resource |
| [google_compute_subnetwork.default](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/compute_subnetwork) | resource |
| [google_container_cluster.default](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/container_cluster)   | resource |
| [random_id.cluster_suffix](https://registry.terraform.io/providers/hashicorp/random/latest/docs/resources/id)                          | resource |

## Inputs

| Name                                                                                                 | Description                                         | Type     | Default      | Required |
|------------------------------------------------------------------------------------------------------|-----------------------------------------------------|----------|--------------|:--------:|
| <a name="input_gke_ipv6_access_type"></a> [gke\_ipv6\_access\_type](#input\_gke\_ipv6\_access\_type) | n/a                                                 | `string` | `"INTERNAL"` |    no    |
| <a name="input_gke_project_id"></a> [gke\_project\_id](#input\_gke\_project\_id)                     | The GCP project ID where the GKE cluster is running | `string` | n/a          |   yes    |
| <a name="input_gke_region"></a> [gke\_region](#input\_gke\_region)                                   | The region where the GKE cluster is running         | `string` | n/a          |   yes    |

## Outputs

| Name                                                                                                             | Description     |
|------------------------------------------------------------------------------------------------------------------|-----------------|
| <a name="output_google_container_cluster"></a> [google\_container\_cluster](#output\_google\_container\_cluster) | The GKE cluster |
