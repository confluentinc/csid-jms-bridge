locals {
  cluster_type_configs = {
    "basic" = {},
    "standard" = {},
    "enterprise" = {},
    "freight" = {},
    "dedicated" = { cku = 1 }
  }
}