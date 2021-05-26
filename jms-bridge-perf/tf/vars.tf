variable "image" {
  type = string
  default = "ubuntu-os-cloud/ubuntu-2004-lts"
}

variable "zone" {
  type    = string
  default = "us-central1-a"
}

variable "ssh_key" {
  type    = string
  default = "~/.ssh/id_rsa.pub"
}

variable "project" {
  type    = string
  default = "csid-281116"
}

